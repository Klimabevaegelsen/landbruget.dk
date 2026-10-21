"""Dasymetric allocation of municipal population to residential buildings."""

import contextlib
import logging
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from common.crs_utils import DANISH_UTM, detect_crs_from_bounds, sql_transform_to_processing_crs
from loguru import logger
from pydantic import ConfigDict, Field
from tqdm import tqdm

from unified_pipeline.bronze.dst_population import fetch_folk1a_csv, parse_folk1a_csv
from unified_pipeline.common.base import BaseJobConfig, BaseSource, GoldJobInterface

DEFAULT_BANDS_M = [10, 30, 50, 100, 250, 500, 1000]
DEFAULT_MAX_DISTANCE_M = 1000.0
DEFAULT_BATCH_SIZE = 5000
NEAREST_KOMMUNE_WITHIN_M = 5000.0


def _validated_table_name(table_name: str) -> str:
    """Reject unexpected identifiers before inserting table names in SQL."""
    if not table_name or not table_name.replace("_", "").isalnum() or not table_name[0].isalpha():
        raise ValueError(f"Invalid DuckDB table name: {table_name!r}")
    return table_name


def _geometry_crs(conn, table_name: str) -> str:
    table_name = _validated_table_name(table_name)
    bounds = conn.execute(
        f"""SELECT MIN(ST_XMin(geometry)), MAX(ST_XMax(geometry)),
                   MIN(ST_YMin(geometry)), MAX(ST_YMax(geometry))
            FROM {table_name} WHERE geometry IS NOT NULL"""
    ).fetchone()
    if bounds and bounds[0] is not None:
        detected_crs, _ = detect_crs_from_bounds(*bounds)
    else:
        detected_crs = DANISH_UTM
    return detected_crs or DANISH_UTM


def _column_exists(conn, table_name: str, column_name: str) -> bool:
    return any(
        row[1].lower() == column_name.lower()
        for row in conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    )


def compute_building_field_distances(
    conn,
    buildings_table: str,
    fields_table: str,
    max_distance_m: float,
    batch_size: int,
) -> str:
    """Compute nearest field distances for residential buildings in spatial batches.

    ``fields_table`` must expose ``field_uuid``, ``geometry`` and
    ``is_organic``. It may also expose ``is_treated``; orchestration builds
    that flag from pesticide disaggregation before calling this function.
    """
    buildings_table = _validated_table_name(buildings_table)
    fields_table = _validated_table_name(fields_table)
    if max_distance_m <= 0 or batch_size <= 0:
        raise ValueError("max_distance_m and batch_size must be positive")

    building_crs = _geometry_crs(conn, buildings_table)
    field_crs = _geometry_crs(conn, fields_table)
    building_geom = (
        "geometry"
        if building_crs == DANISH_UTM
        else sql_transform_to_processing_crs("geometry", building_crs)
    )
    field_geom = (
        "f.geometry"
        if field_crs == DANISH_UTM
        else sql_transform_to_processing_crs("f.geometry", field_crs)
    )
    treated_expr = (
        "COALESCE(f.is_treated, false)"
        if _column_exists(conn, fields_table, "is_treated")
        else "false"
    )

    conn.execute(f"""
        CREATE OR REPLACE TABLE population_exposure_buildings_base AS
        WITH building_unions AS (
            SELECT
                building_uuid,
                MAX(TRY_CAST(inspire_dwellings AS INTEGER)) AS dwellings,
                ST_Union_Agg({building_geom}) AS building_geom_utm
            FROM {buildings_table}
            WHERE category_group = 'residential'
              AND TRY_CAST(inspire_dwellings AS INTEGER) > 0
              AND geometry IS NOT NULL
            GROUP BY building_uuid
        ), building_centroids AS (
            SELECT
                building_uuid,
                dwellings,
                building_geom_utm,
                ST_Centroid(building_geom_utm) AS geometry,
                ST_X(ST_Centroid(building_geom_utm)) AS utm_e,
                ST_Y(ST_Centroid(building_geom_utm)) AS utm_n
            FROM building_unions
        )
        SELECT
            *,
            ((ROW_NUMBER() OVER (
                ORDER BY ST_Hilbert(
                    utm_e,
                    utm_n,
                    ST_MakeBox2D(ST_Point(400000.0, 6000000.0), ST_Point(900000.0, 6450000.0))
                ), building_uuid
            ) - 1) // {batch_size})::BIGINT AS batch_id
        FROM building_centroids
    """)
    conn.execute(f"""
        CREATE OR REPLACE TABLE population_exposure_fields_base AS
        SELECT
            f.field_uuid,
            {field_geom} AS field_geom_utm,
            f.is_organic,
            {treated_expr} AS is_treated
        FROM {fields_table} f
        WHERE f.geometry IS NOT NULL
    """)
    with contextlib.suppress(Exception):
        conn.execute(
            """CREATE INDEX idx_population_fields_rtree
               ON population_exposure_fields_base USING RTREE(field_geom_utm)"""
        )

    conn.execute("""
        CREATE OR REPLACE TABLE population_exposure_building_distances (
            building_uuid VARCHAR,
            dwellings INTEGER,
            dist_any_field_m DOUBLE,
            dist_conventional_field_m DOUBLE,
            dist_treated_field_m DOUBLE,
            utm_e DOUBLE,
            utm_n DOUBLE,
            geometry GEOMETRY
        )
    """)
    building_count = conn.execute(
        "SELECT COUNT(*) FROM population_exposure_buildings_base"
    ).fetchone()[0]
    batch_count = (building_count + batch_size - 1) // batch_size
    progress = tqdm(
        total=building_count,
        desc="Computing population exposure distances",
        unit="buildings",
        disable=building_count == 0,
    )
    max_distance_sql = f"{float(max_distance_m):.8f}"

    for batch_index in range(batch_count):
        conn.execute(f"""
            CREATE OR REPLACE TABLE current_population_building_batch AS
            SELECT * FROM population_exposure_buildings_base
            WHERE batch_id = {batch_index}
        """)
        min_x, max_x, min_y, max_y = conn.execute("""
            SELECT MIN(ST_XMin(ST_Envelope(building_geom_utm))),
                   MAX(ST_XMax(ST_Envelope(building_geom_utm))),
                   MIN(ST_YMin(ST_Envelope(building_geom_utm))),
                   MAX(ST_YMax(ST_Envelope(building_geom_utm)))
            FROM current_population_building_batch
        """).fetchone()
        conn.execute(
            """
            CREATE OR REPLACE TABLE current_population_building_batch_bbox AS
            SELECT ST_MakeEnvelope(?, ?, ?, ?) AS bbox
        """,
            (
                min_x - max_distance_m,
                min_y - max_distance_m,
                max_x + max_distance_m,
                max_y + max_distance_m,
            ),
        )
        conn.execute("""
            CREATE OR REPLACE TABLE current_population_fields_batch AS
            SELECT f.*
            FROM population_exposure_fields_base f
            WHERE ST_Intersects(
                f.field_geom_utm,
                (SELECT bbox FROM current_population_building_batch_bbox)
            )
        """)
        # DuckDB Spatial 1.5.1 returns zero for disjoint polygons in the
        # collinear-edge degenerate case (axis-aligned polygons with collinear
        # edges). Boundary distance gives the polygon gap; intersecting pairs
        # are explicitly assigned zero.
        conn.execute(f"""
            INSERT INTO population_exposure_building_distances
            WITH candidate_distances AS MATERIALIZED (
                SELECT
                    b.building_uuid,
                    b.dwellings,
                    b.utm_e,
                    b.utm_n,
                    b.geometry,
                    f.is_organic,
                    f.is_treated,
                    CASE
                        WHEN ST_Intersects(b.building_geom_utm, f.field_geom_utm) THEN 0.0
                        ELSE ST_Distance(
                            ST_Boundary(b.building_geom_utm),
                            ST_Boundary(f.field_geom_utm)
                        )
                    END AS exact_distance_m
                FROM current_population_building_batch b
                LEFT JOIN current_population_fields_batch f
                  ON ST_DWithin(
                      b.building_geom_utm,
                      f.field_geom_utm,
                      {max_distance_sql}
                  )
            )
            SELECT
                building_uuid,
                dwellings,
                MIN(exact_distance_m)
                    FILTER (WHERE exact_distance_m <= {max_distance_sql}) AS dist_any_field_m,
                MIN(exact_distance_m)
                    FILTER (WHERE exact_distance_m <= {max_distance_sql}
                            AND is_organic IS DISTINCT FROM TRUE) AS dist_conventional_field_m,
                MIN(exact_distance_m)
                    FILTER (WHERE exact_distance_m <= {max_distance_sql}
                            AND is_treated) AS dist_treated_field_m,
                utm_e,
                utm_n,
                geometry
            FROM candidate_distances
            GROUP BY building_uuid, dwellings, utm_e, utm_n, geometry
        """)
        progress.update(min(batch_size, building_count - batch_index * batch_size))
        progress.set_postfix({"batch": f"{batch_index + 1}/{batch_count}"})
    progress.close()
    return "population_exposure_building_distances"


def assign_municipalities(
    conn,
    buildings_table: str,
    kommuner_table: str,
    nearest_within_m: float = NEAREST_KOMMUNE_WITHIN_M,
) -> str:
    """Assign each building centroid to its containing or nearest municipality."""
    buildings_table = _validated_table_name(buildings_table)
    kommuner_table = _validated_table_name(kommuner_table)
    if nearest_within_m < 0:
        raise ValueError("nearest_within_m cannot be negative")
    kommune_crs = _geometry_crs(conn, kommuner_table)
    kommune_geom = (
        "k.geometry"
        if kommune_crs == DANISH_UTM
        else sql_transform_to_processing_crs("k.geometry", kommune_crs)
    )
    conn.execute(f"""
        CREATE OR REPLACE TABLE population_exposure_kommuner_utm AS
        SELECT code, name, {kommune_geom} AS geometry
        FROM {kommuner_table} k
        WHERE k.geometry IS NOT NULL
    """)
    conn.execute(f"""
        CREATE OR REPLACE TABLE population_exposure_municipality_within AS
        SELECT building_uuid, kommune_code, kommune_name
        FROM (
            SELECT
                b.building_uuid,
                LPAD(CAST(k.code AS VARCHAR), 4, '0') AS kommune_code,
                k.name AS kommune_name,
                ROW_NUMBER() OVER (
                    PARTITION BY b.building_uuid
                    ORDER BY CAST(k.code AS VARCHAR)
                ) AS candidate_rank
            FROM {buildings_table} b
            INNER JOIN population_exposure_kommuner_utm k
              ON ST_Within(b.geometry, k.geometry)
        ) contained
        WHERE candidate_rank = 1
    """)
    conn.execute(f"""
        CREATE OR REPLACE TABLE population_exposure_unmatched_buildings AS
        SELECT b.*
        FROM {buildings_table} b
        LEFT JOIN population_exposure_municipality_within w
          ON b.building_uuid = w.building_uuid
        WHERE w.building_uuid IS NULL
    """)
    conn.execute(f"""
        CREATE OR REPLACE TABLE population_exposure_municipality_nearest AS
        SELECT building_uuid, kommune_code, kommune_name
        FROM (
            SELECT
                b.building_uuid,
                LPAD(CAST(k.code AS VARCHAR), 4, '0') AS kommune_code,
                k.name AS kommune_name,
                ROW_NUMBER() OVER (
                    PARTITION BY b.building_uuid
                    ORDER BY ST_Distance(b.geometry, k.geometry), CAST(k.code AS VARCHAR)
                ) AS candidate_rank
            FROM population_exposure_unmatched_buildings b
            INNER JOIN population_exposure_kommuner_utm k
              ON ST_DWithin(b.geometry, k.geometry, {float(nearest_within_m):.8f})
        ) nearby
        WHERE candidate_rank = 1
    """)
    conn.execute(f"""
        CREATE OR REPLACE TABLE population_exposure_assigned AS
        SELECT
            b.*,
            COALESCE(w.kommune_code, n.kommune_code) AS kommune_code,
            COALESCE(w.kommune_name, n.kommune_name) AS kommune_name
        FROM {buildings_table} b
        LEFT JOIN population_exposure_municipality_within w
          ON b.building_uuid = w.building_uuid
        LEFT JOIN population_exposure_municipality_nearest n
          ON b.building_uuid = n.building_uuid
    """)
    return "population_exposure_assigned"


def allocate_population(conn, table: str, population: dict[int, int]) -> dict[str, Any]:
    """Allocate municipality population to buildings by dwelling count."""
    table = _validated_table_name(table)
    conn.execute("DROP TABLE IF EXISTS population_exposure_population")
    conn.execute("""
        CREATE TABLE population_exposure_population (
            kommune_int INTEGER,
            population BIGINT
        )
    """)
    conn.executemany(
        "INSERT INTO population_exposure_population VALUES (?, ?)",
        [(int(code), int(value)) for code, value in population.items()],
    )
    if not _column_exists(conn, table, "est_persons"):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN est_persons DOUBLE")
    conn.execute(f"""
        CREATE OR REPLACE TABLE population_exposure_dwelling_totals AS
        SELECT TRY_CAST(kommune_code AS INTEGER) AS kommune_int,
               SUM(dwellings)::DOUBLE AS total_dwellings
        FROM {table}
        WHERE kommune_code IS NOT NULL
        GROUP BY TRY_CAST(kommune_code AS INTEGER)
    """)
    conn.execute(f"""
        UPDATE {table} AS b
        SET est_persons = CASE
            WHEN b.kommune_code IS NULL THEN NULL
            WHEN t.total_dwellings > 0 THEN
                b.dwellings * COALESCE(p.population, 0)::DOUBLE / t.total_dwellings
            ELSE 0.0
        END
        FROM population_exposure_dwelling_totals t
        LEFT JOIN population_exposure_population p USING (kommune_int)
        WHERE TRY_CAST(b.kommune_code AS INTEGER) = t.kommune_int
    """)
    # Any unmatched building should explicitly have no estimated persons.
    conn.execute(f"UPDATE {table} SET est_persons = NULL WHERE kommune_code IS NULL")

    allocated_population = conn.execute(
        f"SELECT COALESCE(SUM(est_persons), 0.0) FROM {table} WHERE kommune_code IS NOT NULL"
    ).fetchone()[0]
    building_counts = conn.execute(f"""
        SELECT COUNT(*)::BIGINT,
               COALESCE(SUM(dwellings), 0)::BIGINT,
               COUNT(*) FILTER (WHERE kommune_code IS NULL)::BIGINT,
               COALESCE(SUM(dwellings) FILTER (WHERE kommune_code IS NULL), 0)::BIGINT
        FROM {table}
    """).fetchone()
    allocated_codes = {
        int(row[0])
        for row in conn.execute(
            f"SELECT DISTINCT TRY_CAST(kommune_code AS INTEGER) FROM {table} WHERE kommune_code IS NOT NULL"
        ).fetchall()
        if row[0] is not None
    }
    population_unallocated = sum(
        int(value) for code, value in population.items() if int(code) not in allocated_codes
    )
    matched_population = sum(
        int(value) for code, value in population.items() if int(code) in allocated_codes
    )
    if (
        matched_population
        and abs(allocated_population - matched_population) / matched_population > 0.005
    ):
        logging.getLogger(__name__).error(
            "Population allocation differs from matched municipal population by more than 0.5%% "
            "(allocated=%s, matched=%s)",
            allocated_population,
            matched_population,
        )
    return {
        "population_unallocated": population_unallocated,
        "allocated_population": float(allocated_population),
        "matched_population": matched_population,
        "residential_buildings": int(building_counts[0]),
        "dwellings": int(building_counts[1]),
        "buildings_without_kommune": int(building_counts[2]),
        "dwellings_without_kommune": int(building_counts[3]),
    }


def _summary_rows(conn, table: str, distance_column: str, bands: list[int]):
    aggregates = ",\n".join(
        fragment
        for band in bands
        for fragment in (
            f"COALESCE(SUM(CASE WHEN {distance_column} <= {band} THEN dwellings ELSE 0 END), 0) "
            f"AS dwellings_{band}",
            f"COALESCE(SUM(CASE WHEN {distance_column} <= {band} THEN est_persons ELSE 0.0 END), 0.0) "
            f"AS persons_{band}",
        )
    )
    return conn.execute(f"""
        SELECT kommune_code, {aggregates}
        FROM {table}
        GROUP BY kommune_code
    """).fetchall()


def _summary_items(rows, bands: list[int], population: int) -> list[dict[str, Any]]:
    result = []
    for index, band in enumerate(bands):
        dwellings = sum(int(row[1 + index * 2] or 0) for row in rows)
        persons = round(sum(float(row[2 + index * 2] or 0.0) for row in rows))
        result.append(
            {
                "band_m": band,
                "dwellings": dwellings,
                "persons": int(persons),
                "share_of_population": round(persons / population, 6) if population else 0.0,
            }
        )
    return result


def build_population_summary(
    conn,
    table: str,
    *,
    bands: list[int],
    pesticide_year: int,
    population_meta: dict[str, Any],
    building_meta: dict[str, Any],
    kommune_names: dict[int | str, str],
    kommune_population: dict[int | str, int],
) -> dict[str, Any]:
    """Build the versioned public summary with cumulative distance bands."""
    table = _validated_table_name(table)
    bands = sorted(set(bands))
    if not bands or any(not isinstance(band, int) or band < 0 for band in bands):
        raise ValueError("bands must contain non-negative integers")
    national: dict[str, list[dict[str, Any]]] = {}
    municipality_rows: dict[int, dict[str, Any]] = {}
    names = {int(code): value for code, value in kommune_names.items()}
    populations = {int(code): int(value) for code, value in kommune_population.items()}

    for exposure_name, distance_column in (
        ("any_field", "dist_any_field_m"),
        ("conventional_field", "dist_conventional_field_m"),
        ("treated_field", "dist_treated_field_m"),
    ):
        rows = _summary_rows(conn, table, distance_column, bands)
        national[exposure_name] = _summary_items(
            rows, bands, int(population_meta["total_population"])
        )
        grouped: dict[int, list[tuple[Any, ...]]] = {}
        for row in rows:
            if row[0] is None:
                continue
            code = int(row[0])
            grouped[code] = [row]
        for code in sorted(set(names) | set(populations) | set(grouped)):
            record = municipality_rows.setdefault(
                code,
                {
                    "code": f"{code:04d}",
                    "name": names.get(code, ""),
                    "population": populations.get(code, 0),
                },
            )
            record[exposure_name] = _summary_items(
                grouped.get(code, []), bands, populations.get(code, 0)
            )

    for record in municipality_rows.values():
        for exposure_name in ("any_field", "conventional_field", "treated_field"):
            record.setdefault(
                exposure_name,
                [
                    {
                        "band_m": band,
                        "dwellings": 0,
                        "persons": 0,
                        "share_of_population": 0.0,
                    }
                    for band in bands
                ],
            )

    return {
        "schema_version": 1,
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "pesticide_year": pesticide_year,
        "field_year": pesticide_year + 1,
        "max_distance_m": DEFAULT_MAX_DISTANCE_M,
        "bands_m": bands,
        "population_source": {
            "provider": "Danmarks Statistik",
            "table": "FOLK1A",
            "period": population_meta["period"],
            "total_population": int(population_meta["total_population"]),
            "population_unallocated": int(population_meta["population_unallocated"]),
        },
        "buildings": {
            "residential_buildings": int(building_meta["residential_buildings"]),
            "dwellings": int(building_meta["dwellings"]),
            "buildings_without_kommune": int(building_meta["buildings_without_kommune"]),
            "dwellings_without_kommune": int(building_meta["dwellings_without_kommune"]),
        },
        "national": national,
        "municipalities": [municipality_rows[code] for code in sorted(municipality_rows)],
    }


class PopulationExposureGoldConfig(BaseJobConfig):
    """Configuration for the yearly population exposure gold job."""

    name: str = "Population Exposure Gold"
    dataset: str = "population_exposure"
    type: str = "gold"
    description: str = "Estimate people living near agricultural fields by distance band"
    frequency: str = "yearly"
    bucket: str = (
        os.getenv("STORAGE_BUCKET")
        or os.getenv("R2_BUCKET")
        or os.getenv("GCS_BUCKET", "landbruget-data")
    )
    pesticide_year: int | None = None
    max_distance_m: float = DEFAULT_MAX_DISTANCE_M
    bands_m: list[int] = Field(default_factory=lambda: DEFAULT_BANDS_M.copy())
    batch_size: int = DEFAULT_BATCH_SIZE
    population_csv_override: str | None = None

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    def apply_cli_filters(self, cli_config) -> None:
        year = getattr(cli_config, "pesticide_year", None)
        if year is not None:
            object.__setattr__(self, "pesticide_year", year)


class PopulationExposureGold(BaseSource[PopulationExposureGoldConfig], GoldJobInterface):
    """Compute people-weighted distances from homes to agricultural fields."""

    def __init__(self, config: PopulationExposureGoldConfig):
        super().__init__(config)
        self.log = logger

    async def run(self, silver_data: dict[str, Any] | None = None) -> None:
        self.log.info("Starting population exposure pipeline")
        await self._setup_duckdb()
        datasets = await self._load_datasets()
        if datasets is None or not datasets["years"]:
            self.log.warning("No matching population exposure inputs are available; skipping")
            return

        raw_csv = self._load_population_csv()
        period, total_population, population = parse_folk1a_csv(raw_csv)
        self._archive_population_csv(raw_csv)

        for year in datasets["years"]:
            self.log.info(f"Processing population exposure for pesticide year {year}")
            self.storage.create_table_from_storage(
                "current_population_disaggregation", datasets["disaggregation"][year]
            )
            field_table = datasets["field_tables"][year]
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE population_exposure_fields_input AS
                SELECT
                    f.field_uuid,
                    f.geometry,
                    f.is_organic,
                    (d.field_uuid IS NOT NULL) AS is_treated
                FROM {field_table} f
                LEFT JOIN (
                    SELECT DISTINCT field_uuid
                    FROM current_population_disaggregation
                    WHERE TRY_CAST(DosageQuantity AS DOUBLE) > 0
                ) d USING (field_uuid)
                WHERE f.geometry IS NOT NULL
            """)
            distance_table = compute_building_field_distances(
                self.conn,
                datasets["buildings"],
                "population_exposure_fields_input",
                self.config.max_distance_m,
                self.config.batch_size,
            )
            assigned_table = assign_municipalities(
                self.conn,
                distance_table,
                datasets["kommuner"],
            )
            allocation = allocate_population(self.conn, assigned_table, population)
            kommune_rows = self.conn.execute(
                f"SELECT code, name FROM {datasets['kommuner']}"
            ).fetchall()
            kommune_names = {}
            for code, name in kommune_rows:
                try:
                    kommune_names[int(code)] = name
                except (ValueError, TypeError):
                    continue
            summary = build_population_summary(
                self.conn,
                assigned_table,
                bands=self.config.bands_m,
                pesticide_year=year,
                population_meta={
                    "period": period,
                    "total_population": total_population,
                    "population_unallocated": allocation["population_unallocated"],
                },
                building_meta=allocation,
                kommune_names=kommune_names,
                kommune_population=population,
            )
            summary["max_distance_m"] = self.config.max_distance_m
            await self._save_year_results(year, assigned_table, summary)

    async def _setup_duckdb(self) -> None:
        self.conn.execute("INSTALL spatial")
        self.conn.execute("LOAD spatial")
        self.log.info("DuckDB spatial loaded")

    async def _load_datasets(self) -> dict[str, Any] | None:
        buildings = self._read_silver_data("bbr_buildings")
        kommuner = self._read_silver_data("dagi_kommuner")
        if not buildings or not kommuner:
            self.log.warning("Silver bbr_buildings or dagi_kommuner data is missing")
            return None

        pattern = f"{self.config.bucket}/gold/pesticide_disaggregation_*/*/*.parquet"
        year_files: dict[int, str] = {}
        for path in self.storage.list_files(pattern):
            for part in path.split("/"):
                if part.startswith("pesticide_disaggregation_"):
                    try:
                        year = int(part.replace("pesticide_disaggregation_", "").split("_")[0])
                        year_files[year] = path
                    except (ValueError, IndexError):
                        continue
        if self.config.pesticide_year is not None:
            candidates = (
                [self.config.pesticide_year] if self.config.pesticide_year in year_files else []
            )
        else:
            candidates = sorted(year_files, reverse=True)

        years = []
        field_tables = {}
        for year in candidates:
            field_table = self._read_silver_data(f"fvm_marker_{year + 1}")
            if field_table:
                years.append(year)
                field_tables[year] = field_table
                if self.config.pesticide_year is None:
                    break
        if not years:
            return {"years": [], "disaggregation": {}, "field_tables": {}}
        return {
            "years": years,
            "disaggregation": {year: year_files[year] for year in years},
            "field_tables": field_tables,
            "buildings": buildings,
            "kommuner": kommuner,
        }

    def _load_population_csv(self) -> str:
        if self.config.population_csv_override:
            return Path(self.config.population_csv_override).read_text(encoding="utf-8-sig")
        return fetch_folk1a_csv()

    def _archive_population_csv(self, csv_text: str) -> None:
        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        storage_path = f"{self.config.bucket}/bronze/dst_population/{timestamp}/FOLK1A.json"
        self.storage.upload_json({"csv": csv_text}, storage_path)

    async def _save_year_results(self, year: int, table: str, summary: dict[str, Any]) -> None:
        dataset_name = f"population_exposure_{year}_{year + 1}"
        parquet_filename = f"population_exposure_buildings_{year}_{year + 1}.parquet"
        summary_filename = f"summary_{year}_{year + 1}.json"
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE population_exposure_export AS
            SELECT
                building_uuid,
                kommune_code,
                dwellings,
                est_persons,
                dist_any_field_m,
                dist_conventional_field_m,
                dist_treated_field_m,
                utm_e,
                utm_n,
                ST_Transform(geometry, 'EPSG:25832', 'EPSG:4326', always_xy := true) AS geometry
            FROM {table}
        """)
        self._save_data(
            data="population_exposure_export",
            dataset=dataset_name,
            bucket=self.config.bucket,
            stage="gold",
            filename=parquet_filename,
            crs="EPSG:4326",
        )
        self._save_data(
            data=summary,
            dataset=dataset_name,
            bucket=self.config.bucket,
            stage="gold",
            filename=summary_filename,
        )
        self.log.info(f"Saved population exposure parquet and summary for {year}")

    def get_schema_info(self) -> dict[str, Any]:
        return {
            "output_columns": [
                "building_uuid: VARCHAR",
                "kommune_code: VARCHAR(4-digit)",
                "dwellings: INTEGER",
                "est_persons: DOUBLE",
                "dist_any_field_m: DOUBLE",
                "dist_conventional_field_m: DOUBLE",
                "dist_treated_field_m: DOUBLE",
                "utm_e: DOUBLE",
                "utm_n: DOUBLE",
                "geometry: GEOMETRY (centroid, EPSG:4326)",
            ],
            "methodology": {
                "population_source": "Danmarks Statistik FOLK1A latest quarter",
                "allocation": "Municipal population distributed over residential dwellings",
                "distance_crs": "EPSG:25832",
                "max_distance_m": self.config.max_distance_m,
                "bands_m": self.config.bands_m,
            },
        }
