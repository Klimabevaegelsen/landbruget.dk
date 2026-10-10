"""Yearly pesticide exposure metrics for child receptor sites."""

import contextlib
import os
from typing import Any

from common.crs_utils import (
    DANISH_UTM,
    detect_crs_from_bounds,
    sql_transform_to_processing_crs,
)
from loguru import logger
from pydantic import ConfigDict, Field
from tqdm import tqdm

from unified_pipeline.common.base import BaseJobConfig, BaseSource, GoldJobInterface
from unified_pipeline.gold.pesticide_drift_exposure import (
    MIN_DISTANCE_M,
    _build_direction_frequency_table,
    _load_data_file,
    rautmann_drift_pct,
    wind_weight_for_location,
)

DEFAULT_DISTANCE_BANDS_M = [10, 30, 50, 250, 500]
DEFAULT_SEARCH_RADIUS_M = 500.0
DEFAULT_MAX_SITE_AREA_M2 = 50_000.0


def _validated_table_name(table_name: str) -> str:
    """Reject unexpected table names before interpolating them into DuckDB SQL."""
    if not table_name or not table_name.replace("_", "").isalnum() or not table_name[0].isalpha():
        raise ValueError(f"Invalid DuckDB table name: {table_name!r}")
    return table_name


def _geometry_crs(conn, table_name: str) -> str:
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


def compute_child_receptor_exposure(
    conn,
    receptor_table: str,
    cadastral_table: str,
    disaggregation_table: str,
    field_table: str,
    pesticide_year: int,
    *,
    search_radius_m: float = DEFAULT_SEARCH_RADIUS_M,
    distance_bands_m: list[int] | None = None,
    max_site_area_m2: float = DEFAULT_MAX_SITE_AREA_M2,
    batch_size: int = 2000,
    enable_wind_weighting: bool = False,
    wind_freq_table: dict[int, list[float]] | None = None,
) -> int:
    """Build per-receptor metrics using the supplied DuckDB connection and tables.

    The function is intentionally storage-agnostic so its spatial calculations
    can be exercised using an in-memory DuckDB database.
    """
    receptor_table = _validated_table_name(receptor_table)
    cadastral_table = _validated_table_name(cadastral_table)
    disaggregation_table = _validated_table_name(disaggregation_table)
    field_table = _validated_table_name(field_table)

    bands = sorted(set(distance_bands_m or DEFAULT_DISTANCE_BANDS_M))
    if any(not isinstance(band, int) or band < 0 for band in bands):
        raise ValueError("distance_bands_m must contain non-negative integers")
    if search_radius_m <= 0 or batch_size <= 0:
        raise ValueError("search_radius_m and batch_size must be positive")

    # The function is called once per year on the same connection. Replacing
    # these UDFs keeps repeated year runs safe while reusing drift's functions.
    with contextlib.suppress(Exception):
        conn.remove_function("rautmann_drift_pct")
    conn.create_function("rautmann_drift_pct", rautmann_drift_pct, [float], float)

    apply_wind_weighting = bool(enable_wind_weighting and wind_freq_table)
    if apply_wind_weighting:
        freq_table = wind_freq_table or {}
        with contextlib.suppress(Exception):
            conn.remove_function("child_receptor_wind_weight")

        def child_receptor_wind_weight(bearing_deg: float, utm_e: float, utm_n: float) -> float:
            return wind_weight_for_location(bearing_deg, utm_e, utm_n, freq_table)

        conn.create_function(
            "child_receptor_wind_weight",
            child_receptor_wind_weight,
            [float, float, float],
            float,
        )

    field_crs = _geometry_crs(conn, field_table)
    if field_crs == DANISH_UTM:
        field_geom_expr = "f.geometry"
    else:
        field_geom_expr = sql_transform_to_processing_crs("f.geometry", field_crs)

    cadastral_crs = _geometry_crs(conn, cadastral_table)
    if cadastral_crs == DANISH_UTM:
        cadastral_geom_expr = "c.geometry"
    else:
        cadastral_geom_expr = sql_transform_to_processing_crs("c.geometry", cadastral_crs)

    conn.execute(f"""
        CREATE OR REPLACE TABLE drift_fields AS
        SELECT
            cd.field_uuid,
            cd.DosageQuantity,
            cd.AllocatedArea,
            cd.PesticideName,
            {field_geom_expr} AS field_geom_utm,
            ST_Centroid({field_geom_expr}) AS field_centroid_utm
        FROM {disaggregation_table} cd
        JOIN {field_table} f ON cd.field_uuid = f.field_uuid
        WHERE f.geometry IS NOT NULL
          AND cd.DosageQuantity IS NOT NULL
          AND cd.AllocatedArea > 0
    """)

    conn.execute("""
        CREATE OR REPLACE TABLE child_sprayed_fields AS
        SELECT
            field_uuid,
            ANY_VALUE(field_geom_utm) AS field_geom_utm,
            ANY_VALUE(field_centroid_utm) AS field_centroid_utm
        FROM drift_fields
        GROUP BY field_uuid
    """)
    with contextlib.suppress(Exception):
        conn.execute(
            """CREATE INDEX idx_child_sprayed_fields_rtree
               ON child_sprayed_fields USING RTREE(field_geom_utm)"""
        )

    conn.execute(f"""
        CREATE OR REPLACE TABLE child_receptors_base AS
        SELECT
            receptor_id,
            receptor_type,
            subtype,
            name,
            ownership,
            kommune_kode,
            address,
            cvr,
            source,
            matched_sources,
            utm_e,
            utm_n,
            lon,
            lat,
            geometry AS receptor_point_utm
        FROM {receptor_table}
        WHERE geometry IS NOT NULL
    """)

    conn.execute(f"""
        CREATE OR REPLACE TABLE child_receptor_cadastral AS
        SELECT
            c.bfe_number,
            {cadastral_geom_expr} AS parcel_geom_utm
        FROM {cadastral_table} c
        WHERE c.geometry IS NOT NULL
    """)
    conn.execute("""
        CREATE OR REPLACE TABLE child_receptor_parcel_candidates AS
        SELECT
            r.receptor_id,
            c.bfe_number,
            c.parcel_geom_utm,
            ST_Area(c.parcel_geom_utm) AS parcel_area_m2,
            ROW_NUMBER() OVER (
                PARTITION BY r.receptor_id
                ORDER BY ST_Area(c.parcel_geom_utm), c.bfe_number NULLS LAST
            ) AS parcel_rank
        FROM child_receptors_base r
        JOIN child_receptor_cadastral c
          ON ST_Intersects(c.parcel_geom_utm, r.receptor_point_utm)
    """)
    conn.execute(f"""
        CREATE OR REPLACE TABLE child_receptor_sites AS
        SELECT
            r.*,
            CASE
                WHEN r.receptor_type <> 'playground'
                 AND p.parcel_area_m2 <= {max_site_area_m2}
                    THEN p.parcel_geom_utm
                ELSE r.receptor_point_utm
            END AS site_geom_utm,
            CASE
                WHEN r.receptor_type = 'playground' THEN 'point'
                WHEN p.parcel_area_m2 <= {max_site_area_m2} THEN 'cadastral'
                WHEN p.parcel_area_m2 > {max_site_area_m2} THEN 'point_large_parcel'
                ELSE 'point'
            END AS site_geom_source,
            CASE
                WHEN r.receptor_type <> 'playground'
                 AND p.parcel_area_m2 <= {max_site_area_m2}
                    THEN p.bfe_number
                ELSE NULL
            END AS site_bfe_number,
            CASE
                WHEN r.receptor_type <> 'playground'
                 AND p.parcel_area_m2 <= {max_site_area_m2}
                    THEN p.parcel_area_m2
                ELSE NULL
            END AS site_area_m2
        FROM child_receptors_base r
        LEFT JOIN child_receptor_parcel_candidates p
          ON p.receptor_id = r.receptor_id AND p.parcel_rank = 1
    """)

    band_columns = ",\n".join(f"sprayed_fields_within_{band}m INTEGER" for band in bands)
    conn.execute(f"""
        CREATE OR REPLACE TABLE child_receptor_exposure_metrics (
            receptor_id VARCHAR,
            receptor_type VARCHAR,
            subtype VARCHAR,
            name VARCHAR,
            ownership VARCHAR,
            kommune_kode VARCHAR,
            address VARCHAR,
            cvr VARCHAR,
            source VARCHAR,
            matched_sources VARCHAR[],
            pesticide_year INTEGER,
            field_year INTEGER,
            site_geom_source VARCHAR,
            site_bfe_number BIGINT,
            site_area_m2 DOUBLE,
            nearest_sprayed_field_m DOUBLE,
            {band_columns},
            sprayed_area_ha_within_250m DOUBLE,
            sprayed_area_ha_within_500m DOUBLE,
            applied_kg_on_fields_within_500m DOUBLE,
            unique_pesticides_within_500m INTEGER,
            drift_dose_kg DOUBLE,
            max_single_drift_pct DOUBLE,
            wind_weighted BOOLEAN,
            top_pesticides VARCHAR,
            utm_e DOUBLE,
            utm_n DOUBLE,
            lon DOUBLE,
            lat DOUBLE,
            geometry GEOMETRY
        )
    """)

    receptor_count = conn.execute("SELECT COUNT(*) FROM child_receptor_sites").fetchone()[0]
    total_batches = (receptor_count + batch_size - 1) // batch_size
    pbar = tqdm(
        total=receptor_count,
        desc="Computing child receptor exposure",
        unit="receptors",
        disable=receptor_count == 0,
    )

    radius_sql = f"{float(search_radius_m):.8f}"
    band_count_sql = ",\n".join(
        f"COUNT(DISTINCT CASE WHEN distance_m <= {band} THEN field_uuid END)::INTEGER "
        f"AS sprayed_fields_within_{band}m"
        for band in bands
    )

    for batch_num, offset in enumerate(range(0, receptor_count, batch_size), 1):
        conn.execute(f"""
            CREATE OR REPLACE TABLE current_receptor_batch AS
            SELECT * FROM child_receptor_sites
            ORDER BY receptor_id
            LIMIT {batch_size} OFFSET {offset}
        """)
        bounds = conn.execute("""
            SELECT MIN(ST_XMin(ST_Envelope(site_geom_utm))),
                   MAX(ST_XMax(ST_Envelope(site_geom_utm))),
                   MIN(ST_YMin(ST_Envelope(site_geom_utm))),
                   MAX(ST_YMax(ST_Envelope(site_geom_utm)))
            FROM current_receptor_batch
        """).fetchone()
        min_x, max_x, min_y, max_y = bounds
        conn.execute(
            """
            CREATE OR REPLACE TABLE current_receptor_batch_bbox AS
            SELECT ST_MakeEnvelope(?, ?, ?, ?) AS bbox
        """,
            (
                min_x - search_radius_m,
                min_y - search_radius_m,
                max_x + search_radius_m,
                max_y + search_radius_m,
            ),
        )
        conn.execute("""
            CREATE OR REPLACE TABLE current_sprayed_fields_batch AS
            SELECT f.*
            FROM child_sprayed_fields f
            WHERE ST_Intersects(
                f.field_geom_utm,
                (SELECT bbox FROM current_receptor_batch_bbox)
            )
        """)
        conn.execute(f"""
            CREATE OR REPLACE TABLE batch_field_pairs AS
            SELECT
                r.receptor_id,
                r.utm_e,
                r.utm_n,
                f.field_uuid,
                f.field_geom_utm,
                ST_Distance(r.site_geom_utm, f.field_geom_utm) AS distance_m,
                DEGREES(ST_Azimuth(r.receptor_point_utm, f.field_centroid_utm)) AS bearing_deg,
                rautmann_drift_pct(GREATEST(
                    ST_Distance(r.site_geom_utm, f.field_geom_utm), {MIN_DISTANCE_M}
                )) AS drift_pct,
                COALESCE(TRY(ST_Area(ST_Intersection(
                    f.field_geom_utm, ST_Buffer(r.site_geom_utm, 250.0)
                ))), 0.0) / 10000.0 AS sprayed_area_ha_250m,
                COALESCE(TRY(ST_Area(ST_Intersection(
                    f.field_geom_utm, ST_Buffer(r.site_geom_utm, 500.0)
                ))), 0.0) / 10000.0 AS sprayed_area_ha_500m
            FROM current_receptor_batch r
            JOIN current_sprayed_fields_batch f
              ON ST_DWithin(r.site_geom_utm, f.field_geom_utm, {radius_sql})
        """)

        conn.execute(f"""
            CREATE OR REPLACE TABLE batch_field_metrics AS
            SELECT
                receptor_id,
                MIN(distance_m) AS nearest_sprayed_field_m,
                {band_count_sql},
                SUM(sprayed_area_ha_250m) AS sprayed_area_ha_within_250m,
                SUM(sprayed_area_ha_500m) AS sprayed_area_ha_within_500m,
                MAX(drift_pct) AS max_single_drift_pct
            FROM batch_field_pairs
            GROUP BY receptor_id
        """)

        wind_expression = (
            "child_receptor_wind_weight(p.bearing_deg, p.utm_e, p.utm_n)"
            if apply_wind_weighting
            else "1.0"
        )
        conn.execute(f"""
            CREATE OR REPLACE TABLE batch_pesticide_pairs AS
            SELECT
                p.receptor_id,
                d.PesticideName,
                d.DosageQuantity * d.AllocatedArea AS applied_kg,
                d.DosageQuantity * d.AllocatedArea * p.drift_pct / 100.0
                    * {wind_expression} AS drift_dose_kg
            FROM batch_field_pairs p
            JOIN drift_fields d ON p.field_uuid = d.field_uuid
        """)
        conn.execute("""
            CREATE OR REPLACE TABLE batch_pesticide_metrics AS
            SELECT
                receptor_id,
                SUM(applied_kg) AS applied_kg_on_fields_within_500m,
                COUNT(DISTINCT PesticideName)::INTEGER AS unique_pesticides_within_500m,
                SUM(drift_dose_kg) AS drift_dose_kg
            FROM batch_pesticide_pairs
            GROUP BY receptor_id
        """)
        conn.execute("""
            CREATE OR REPLACE TABLE batch_top_pesticides AS
            WITH pesticide_totals AS (
                SELECT receptor_id, PesticideName,
                       SUM(drift_dose_kg) AS pesticide_drift_dose_kg
                FROM batch_pesticide_pairs
                WHERE PesticideName IS NOT NULL
                GROUP BY receptor_id, PesticideName
            ), ranked AS (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY receptor_id
                    ORDER BY pesticide_drift_dose_kg DESC, PesticideName
                ) AS pesticide_rank
                FROM pesticide_totals
            )
            SELECT
                receptor_id,
                to_json(list(
                    struct_pack(
                        name := PesticideName,
                        drift_dose_kg := pesticide_drift_dose_kg
                    ) ORDER BY pesticide_drift_dose_kg DESC, PesticideName
                )) AS top_pesticides
            FROM ranked
            WHERE pesticide_rank <= 5
            GROUP BY receptor_id
        """)

        metric_band_columns = ",\n".join(
            f"COALESCE(fm.sprayed_fields_within_{band}m, 0)" for band in bands
        )
        conn.execute(f"""
            INSERT INTO child_receptor_exposure_metrics
            SELECT
                r.receptor_id,
                r.receptor_type,
                r.subtype,
                r.name,
                r.ownership,
                r.kommune_kode,
                r.address,
                r.cvr,
                r.source,
                r.matched_sources,
                {pesticide_year} AS pesticide_year,
                {pesticide_year + 1} AS field_year,
                r.site_geom_source,
                r.site_bfe_number,
                r.site_area_m2,
                fm.nearest_sprayed_field_m,
                {metric_band_columns},
                COALESCE(fm.sprayed_area_ha_within_250m, 0.0),
                COALESCE(fm.sprayed_area_ha_within_500m, 0.0),
                COALESCE(pm.applied_kg_on_fields_within_500m, 0.0),
                COALESCE(pm.unique_pesticides_within_500m, 0),
                COALESCE(pm.drift_dose_kg, 0.0),
                fm.max_single_drift_pct,
                {str(apply_wind_weighting).upper()} AS wind_weighted,
                COALESCE(tp.top_pesticides, '[]') AS top_pesticides,
                r.utm_e,
                r.utm_n,
                r.lon,
                r.lat,
                r.receptor_point_utm AS geometry
            FROM current_receptor_batch r
            LEFT JOIN batch_field_metrics fm USING (receptor_id)
            LEFT JOIN batch_pesticide_metrics pm USING (receptor_id)
            LEFT JOIN batch_top_pesticides tp USING (receptor_id)
        """)

        pbar.update(min(batch_size, receptor_count - offset))
        pbar.set_postfix({"batch": f"{batch_num}/{total_batches}"})

    pbar.close()

    ordered_columns = [
        "receptor_id",
        "receptor_type",
        "subtype",
        "name",
        "ownership",
        "kommune_kode",
        "address",
        "cvr",
        "source",
        "matched_sources",
        "pesticide_year",
        "field_year",
        "site_geom_source",
        "site_bfe_number",
        "site_area_m2",
        "nearest_sprayed_field_m",
        *(f"sprayed_fields_within_{band}m" for band in bands),
        "sprayed_area_ha_within_250m",
        "sprayed_area_ha_within_500m",
        "applied_kg_on_fields_within_500m",
        "unique_pesticides_within_500m",
        "drift_dose_kg",
        "max_single_drift_pct",
        "wind_weighted",
        "top_pesticides",
    ]
    projection_columns = ",\n".join(ordered_columns)
    conn.execute(f"""
        CREATE OR REPLACE TABLE child_receptor_exposure_final AS
        SELECT
            {projection_columns},
            ROUND(PERCENT_RANK() OVER (
                PARTITION BY receptor_type ORDER BY drift_dose_kg
            ) * 100.0, 2) AS drift_dose_percentile,
            utm_e,
            utm_n,
            lon,
            lat,
            geometry
        FROM child_receptor_exposure_metrics
    """)
    return receptor_count


def build_exposure_summary(conn, table_name: str, pesticide_year: int, bands: list[int]) -> dict:
    """Build receptor-type and municipality counts and distance shares."""
    table_name = _validated_table_name(table_name)
    bands = sorted(set(bands))
    aggregate_columns = ",\n".join(
        f"SUM(CASE WHEN nearest_sprayed_field_m <= {band} THEN 1 ELSE 0 END) AS within_{band}m"
        for band in bands
    )

    def summaries(rows):
        result = {}
        for row in rows:
            group_name, total, *within_counts = row
            total = int(total)
            result[str(group_name)] = {
                "total": total,
                "within_distance_bands": {
                    f"{band}m": {
                        "count": int(count),
                        "share": round(int(count) / total, 4) if total else 0.0,
                    }
                    for band, count in zip(bands, within_counts, strict=True)
                },
            }
        return result

    receptor_type_rows = conn.execute(f"""
        SELECT receptor_type, COUNT(*) AS total, {aggregate_columns}
        FROM {table_name}
        GROUP BY receptor_type
        ORDER BY receptor_type
    """).fetchall()
    kommune_rows = conn.execute(f"""
        SELECT COALESCE(kommune_kode, 'unknown') AS kommune_kode,
               receptor_type, COUNT(*) AS total, {aggregate_columns}
        FROM {table_name}
        WHERE receptor_type IN ('daycare', 'school')
        GROUP BY kommune_kode, receptor_type
        ORDER BY kommune_kode, receptor_type
    """).fetchall()

    by_kommune = {}
    for row in kommune_rows:
        kommune_kode, receptor_type, total, *within_counts = row
        kommune_summary = summaries([(receptor_type, total, *within_counts)])[receptor_type]
        by_kommune.setdefault(str(kommune_kode), {})[receptor_type] = kommune_summary

    return {
        "pesticide_year": pesticide_year,
        "field_year": pesticide_year + 1,
        "distance_bands_m": bands,
        "by_receptor_type": summaries(receptor_type_rows),
        "by_kommune_kode": by_kommune,
    }


class ChildReceptorExposureGoldConfig(BaseJobConfig):
    """Configuration for yearly exposure at children's receptor sites."""

    name: str = "Child Receptor Exposure Gold"
    dataset: str = "child_receptor_exposure"
    type: str = "gold"
    description: str = (
        "Nearest sprayed fields, sprayed area, and estimated pesticide drift dose "
        "per child receptor"
    )
    frequency: str = "yearly"
    bucket: str = (
        os.getenv("STORAGE_BUCKET")
        or os.getenv("R2_BUCKET")
        or os.getenv("GCS_BUCKET", "landbruget-data")
    )

    pesticide_disaggregation_dataset: str = "pesticide_disaggregation"
    agricultural_fields_dataset: str = "fvm_marker"
    child_receptors_dataset: str = "child_receptors"
    cadastral_dataset: str = "cadastral"

    pesticide_year: int | None = Field(
        default=None,
        description="Specific pesticide year to process (if None, processes all available years)",
    )
    search_radius_m: float = Field(
        default=DEFAULT_SEARCH_RADIUS_M,
        description="Maximum distance for nearby sprayed field metrics (meters)",
    )
    distance_bands_m: list[int] = Field(
        default_factory=lambda: DEFAULT_DISTANCE_BANDS_M.copy(),
        description="Distance thresholds for sprayed field counts (meters)",
    )
    max_site_area_m2: float = Field(
        default=DEFAULT_MAX_SITE_AREA_M2,
        description="Largest cadastral parcel area used as a receptor site (square meters)",
    )
    # Off by default: wind_direction_weight returns the raw sector frequency (~0.05-0.2),
    # not a weight normalised to 1, and pesticide_drift_exposure never applies it, so
    # enabling it would make doses ~10x lower and not comparable with building doses.
    enable_wind_weighting: bool = Field(
        default=False,
        description="Apply raw wind-sector frequency weighting (not comparable with building drift)",
    )
    batch_size: int = Field(default=2000, description="Number of receptors processed per batch")

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    def apply_cli_filters(self, cli_config) -> None:
        if cli_config.pesticide_year:
            object.__setattr__(self, "pesticide_year", cli_config.pesticide_year)


class ChildReceptorExposureGold(BaseSource[ChildReceptorExposureGoldConfig], GoldJobInterface):
    """Compute per-year sprayed-field and pesticide drift metrics for receptors."""

    def __init__(self, config: ChildReceptorExposureGoldConfig):
        super().__init__(config)
        self.log = logger
        self.wind_freq_table: dict[int, list[float]] = {}

    async def run(self, silver_data: dict[str, Any] | None = None) -> None:
        self.log.info("Starting child receptor pesticide exposure pipeline...")
        await self._setup_duckdb()
        self._load_wind_data()
        datasets = await self._load_datasets()
        if datasets is None:
            self.log.warning(
                "Silver child_receptors data is not available; skipping child receptor exposure"
            )
            return

        if self.config.pesticide_year:
            if self.config.pesticide_year not in datasets["disaggregation"]:
                self.log.warning(
                    "No disaggregation data found for year "
                    f"{self.config.pesticide_year}; skipping child receptor exposure"
                )
                return
            years = [self.config.pesticide_year]
            self.log.info(f"Matrix job mode: processing year {self.config.pesticide_year}")
        else:
            years = sorted(datasets["disaggregation"])
            self.log.info(f"Found {len(years)} years to process: {years}")

        for year in years:
            self.log.info(f"Processing child receptor exposure for year {year}...")
            try:
                await self._load_year_data(year, datasets)
                count = await self._compute_child_receptor_exposure(year)
                await self._save_year_results(year, count)
                self.log.info(f"Year {year} completed: {count:,} child receptor exposure records")
            except Exception as error:
                self.log.error(f"Year {year} failed: {error}")
                raise

        self.log.info("Child receptor pesticide exposure pipeline completed.")

    async def _setup_duckdb(self) -> None:
        self.conn.execute("INSTALL spatial")
        self.conn.execute("LOAD spatial")
        self.log.info("DuckDB spatial loaded")

    def _load_wind_data(self) -> None:
        if not self.config.enable_wind_weighting:
            self.log.info("Wind weighting disabled")
            return
        try:
            wind_rose = _load_data_file("wind_rose_oml_2008-2017.json")
            self.wind_freq_table = _build_direction_frequency_table(wind_rose)
            self.log.info(
                f"Loaded wind roses for {len(self.wind_freq_table)} regions "
                f"({wind_rose.get('period', 'unknown')} period)"
            )
        except FileNotFoundError:
            self.log.warning("Wind rose data not found, falling back to unweighted drift dose")
            self.config = self.config.model_copy(update={"enable_wind_weighting": False})

    async def _load_datasets(self) -> dict[str, Any] | None:
        datasets: dict[str, Any] = {}

        # _read_silver_data uses BaseSource's latest timestamped data.parquet lookup.
        receptor_table = self._read_silver_data(self.config.child_receptors_dataset)
        if not receptor_table:
            self.log.warning(
                "No silver child_receptors dataset found; skipping child receptor exposure"
            )
            return None
        datasets["receptors"] = receptor_table

        cadastral_table = self._read_silver_data(self.config.cadastral_dataset)
        if not cadastral_table:
            raise ValueError("No silver cadastral dataset found for child receptor exposure")
        datasets["cadastral"] = cadastral_table

        pattern = f"{self.config.bucket}/gold/pesticide_disaggregation_*/*/*.parquet"
        files = self.storage.list_files(pattern)
        year_files: dict[int, str] = {}
        for file_path in files:
            for part in file_path.split("/"):
                if part.startswith("pesticide_disaggregation_"):
                    try:
                        year = int(part.replace("pesticide_disaggregation_", "").split("_")[0])
                        year_files[year] = file_path
                    except (ValueError, IndexError):
                        continue
        if not year_files:
            raise ValueError("No pesticide disaggregation data found")
        datasets["disaggregation"] = year_files
        self.log.info(f"Found disaggregation data for years: {sorted(year_files)}")
        return datasets

    async def _load_year_data(self, year: int, datasets: dict[str, Any]) -> None:
        file_path = datasets["disaggregation"][year]
        self.storage.create_table_from_storage("current_disaggregation", file_path)
        count = self.conn.execute("SELECT COUNT(*) FROM current_disaggregation").fetchone()[0]
        self.log.info(f"Loaded {count:,} disaggregated records for year {year}")

        field_dataset = f"{self.config.agricultural_fields_dataset}_{year + 1}"
        field_table = self._read_silver_data(field_dataset)
        if not field_table:
            raise ValueError(f"No silver field data found for {field_dataset}")
        datasets["field_table"] = field_table
        field_count = self.conn.execute(f"SELECT COUNT(*) FROM {field_table}").fetchone()[0]
        self.log.info(f"Loaded {field_count:,} field records for field year {year + 1}")

    async def _compute_child_receptor_exposure(self, year: int) -> int:
        return compute_child_receptor_exposure(
            self.conn,
            receptor_table="data_child_receptors_silver",
            cadastral_table="data_cadastral_silver",
            disaggregation_table="current_disaggregation",
            field_table=f"data_{self.config.agricultural_fields_dataset}_{year + 1}_silver",
            pesticide_year=year,
            search_radius_m=self.config.search_radius_m,
            distance_bands_m=self.config.distance_bands_m,
            max_site_area_m2=self.config.max_site_area_m2,
            batch_size=self.config.batch_size,
            enable_wind_weighting=self.config.enable_wind_weighting,
            wind_freq_table=self.wind_freq_table,
        )

    async def _save_year_results(self, year: int, record_count: int) -> None:
        dataset_name = f"{self.config.dataset}_{year}_{year + 1}"
        parquet_filename = f"{dataset_name}.parquet"
        summary_filename = f"summary_{year}_{year + 1}.json"
        self.conn.execute("""
            CREATE OR REPLACE TABLE child_receptor_exposure_export AS
            SELECT
                * EXCLUDE (geometry),
                ST_Transform(geometry, 'EPSG:25832', 'EPSG:4326', always_xy := true) AS geometry
            FROM child_receptor_exposure_final
        """)
        summary = build_exposure_summary(
            self.conn,
            "child_receptor_exposure_final",
            year,
            self.config.distance_bands_m,
        )
        self._save_data(
            data="child_receptor_exposure_export",
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
        output_path = (
            f"{self.config.bucket}/gold/{dataset_name}/{self.date_pattern}/{parquet_filename}"
        )
        self.log.info(
            f"Saved {record_count:,} child receptor exposure records to {output_path} "
            f"and summary to summary_{year}_{year + 1}.json"
        )

    def get_schema_info(self) -> dict[str, Any]:
        return {
            "output_columns": [
                "receptor_id: VARCHAR",
                "receptor_type: VARCHAR",
                "pesticide_year: INTEGER",
                "field_year: INTEGER",
                "site_geom_source: VARCHAR",
                "site_bfe_number: BIGINT",
                "site_area_m2: DOUBLE",
                "nearest_sprayed_field_m: DOUBLE",
                *(
                    f"sprayed_fields_within_{band}m: INTEGER"
                    for band in self.config.distance_bands_m
                ),
                "sprayed_area_ha_within_250m: DOUBLE",
                "sprayed_area_ha_within_500m: DOUBLE",
                "applied_kg_on_fields_within_500m: DOUBLE",
                "unique_pesticides_within_500m: INTEGER",
                "drift_dose_kg: DOUBLE",
                "max_single_drift_pct: DOUBLE",
                "wind_weighted: BOOLEAN",
                "top_pesticides: JSON string",
                "drift_dose_percentile: DOUBLE (0-100, 2 decimals)",
                "geometry: GEOMETRY (EPSG:4326)",
            ],
            "methodology": {
                "drift_curve": "Rautmann power-law shared with pesticide_drift_exposure",
                "search_radius_m": self.config.search_radius_m,
                "distance_bands_m": self.config.distance_bands_m,
                "max_site_area_m2": self.config.max_site_area_m2,
                "coordinate_system": "EPSG:25832 (processing), EPSG:4326 (output)",
            },
        }
