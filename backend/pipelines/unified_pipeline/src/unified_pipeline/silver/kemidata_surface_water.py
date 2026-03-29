"""
Silver layer processing for Kemidata surface water pesticide data.

This module transforms raw CSV data from the Kemidata bronze layer into clean,
structured parquet format with standardized schema and Point geometries.

Data source:
- Input: CSV export from Kemidata API (kemidata_export.csv)
- Input: Station search results JSON (search_result.json)
- Output: Parquet with standardized schema and Point geometries in EPSG:25832

The CSV from Kemidata contains measurement records with station coordinates
embedded as "x, y" strings in the station data. The silver layer:
1. Reads the CSV via DuckDB
2. Joins station coordinates from search results
3. Creates Point geometries in EPSG:25832
4. Filters to surface water only (Vandløb, Sø)
5. Validates coordinates within Denmark bounds
6. Saves as parquet to cloud storage
"""

from datetime import datetime
from typing import Any

from common.crs_utils import DENMARK_BOUNDS_UTM

from unified_pipeline.bronze.kemidata_surface_water import SURFACE_WATER_MEDIA
from unified_pipeline.common.base import BaseJobConfig, BaseSource, SilverJobInterface
from unified_pipeline.util.timing import AsyncTimer, timed


class KemidataSurfaceWaterSilverConfig(BaseJobConfig):
    """
    Configuration for Kemidata surface water silver processing.

    Attributes:
        dataset: Name of the dataset in storage
        bucket: storage bucket name
        source_crs: Source coordinate reference system
    """

    dataset: str = "kemidata_surface_water_pesticides"
    bucket: str = "landbruget-data"
    source_crs: str = "EPSG:25832"


class KemidataSurfaceWaterSilver(BaseSource[KemidataSurfaceWaterSilverConfig], SilverJobInterface):
    """
    Silver layer processor for Kemidata surface water pesticide data.

    Transforms raw CSV and station JSON from bronze into clean parquet
    with standardized schema and spatial geometries.
    """

    def __init__(self, config: KemidataSurfaceWaterSilverConfig):
        super().__init__(config)

    def _load_stations_from_storage(self, stations_path: str) -> list[dict]:
        """
        Load station search results from storage and extract coordinate mapping.

        Returns list of station dicts with id, name, x, y, mediaName.
        """
        storage_uri = f"{self.config.bucket}/{stations_path}"
        self.log.info(f"Loading station data from {storage_uri}")

        search_result = self.storage.download_json(storage_uri)

        stations = search_result.get("stations", [])
        parsed = []

        for s in stations:
            location = s.get("location", "")
            if not location or "," not in location:
                continue
            parts = location.split(",")
            try:
                x = float(parts[0].strip())
                y = float(parts[1].strip())
            except (ValueError, IndexError):
                continue

            parsed.append(
                {
                    "station_id": s.get("id", ""),
                    "station_name": s.get("name", ""),
                    "media_name": s.get("mediaName", ""),
                    "x": x,
                    "y": y,
                }
            )

        self.log.info(f"Parsed {len(parsed)} stations with coordinates")
        return parsed

    @timed(name="Loading CSV into DuckDB")
    def _load_csv_from_storage(self, csv_path: str) -> str:
        """
        Load the Kemidata CSV export from storage directly into a DuckDB table.

        Uses DuckDB's registered cloud filesystem (configured by StorageAccess)
        to read the CSV without temp files or in-memory buffering.

        Returns the table name.
        """
        storage_uri = f"{self.config.bucket}/{csv_path}"
        self.log.info(f"Loading CSV from {storage_uri}")

        self.conn.execute(f"""
            CREATE OR REPLACE TABLE raw_kemidata AS
            SELECT * FROM read_csv_auto('{storage_uri}',
                header=true,
                all_varchar=true,
                ignore_errors=true
            )
        """)

        row_count = self.conn.execute("SELECT COUNT(*) FROM raw_kemidata").fetchone()[0]
        columns = [col[0] for col in self.conn.execute("DESCRIBE raw_kemidata").fetchall()]

        self.log.info(f"Loaded {row_count:,} rows with {len(columns)} columns")
        self.log.info(f"Columns: {columns}")

        return "raw_kemidata"

    @timed(name="Transforming Kemidata")
    def _transform_data(self, stations: list[dict]) -> str:
        """
        Transform raw CSV data with station coordinates into clean schema.

        The CSV columns vary by export but typically include:
        - Station/location identifiers
        - Parameter name and code
        - Sample date and result value
        - Unit, detection flag, etc.

        We join station coordinates from the search results to add geometry.
        """
        # Register stations as a DuckDB table
        self.conn.execute("""
            CREATE OR REPLACE TABLE stations (
                station_id VARCHAR,
                station_name VARCHAR,
                media_name VARCHAR,
                x DOUBLE,
                y DOUBLE
            )
        """)

        if stations:
            self.conn.executemany(
                "INSERT INTO stations VALUES (?, ?, ?, ?, ?)",
                [
                    (s["station_id"], s["station_name"], s["media_name"], s["x"], s["y"])
                    for s in stations
                ],
            )

        self.log.info(f"Registered {len(stations)} stations in DuckDB")

        # Inspect the raw CSV columns to build the right transform
        columns = [col[0] for col in self.conn.execute("DESCRIBE raw_kemidata").fetchall()]
        col_lower = {c.lower(): c for c in columns}

        self.log.info(f"CSV columns for mapping: {columns}")

        # Detect the right join column from the CSV
        station_col = None
        for candidate in [
            "Lokalitetsnavn",
            "StationsNavn",
            "Stationsnavn",
            "station_name",
            "Lokalitet",
            "Navn",
        ]:
            if candidate in columns or candidate.lower() in col_lower:
                station_col = col_lower.get(candidate.lower(), candidate)
                break

        station_id_col = None
        for candidate in [
            "Lokalitetsnummer",
            "StationsNummer",
            "Stationsnummer",
            "station_id",
            "LokalitetsId",
        ]:
            if candidate in columns or candidate.lower() in col_lower:
                station_id_col = col_lower.get(candidate.lower(), candidate)
                break

        self.log.info(f"Detected station column: {station_col}, ID column: {station_id_col}")

        # Build the actual transform with the detected columns
        if station_col:
            join_clause = f'r."{station_col}" = s.station_name'
        elif station_id_col:
            join_clause = f'r."{station_id_col}" = s.station_id'
        else:
            # No join possible — just include all data without geometry
            self.log.warning(
                "Could not detect station column for geometry join. "
                "Data will be saved without geometry."
            )
            join_clause = "1=0"  # No match, all NULLs for station columns

        self.conn.execute(f"""
            CREATE OR REPLACE TABLE kemidata_transformed AS
            SELECT
                r.*,
                s.x AS x_coord,
                s.y AS y_coord,
                s.media_name AS media_type,
                CASE
                    WHEN s.x IS NOT NULL AND s.y IS NOT NULL
                    THEN ST_Point(s.x, s.y)
                    ELSE NULL
                END AS geometry
            FROM raw_kemidata r
            LEFT JOIN stations s ON {join_clause}
        """)

        # Filter to surface water only where possible
        transformed_cols = [
            col[0] for col in self.conn.execute("DESCRIBE kemidata_transformed").fetchall()
        ]
        media_col = None
        for candidate in [
            "media_type",
            "Medietype",
            "Media",
            "MediaType",
            "MedieType",
            "Medie",
        ]:
            if candidate in transformed_cols:
                media_col = candidate
                break

        if media_col:
            media_values = ", ".join(f"'{m}'" for m in SURFACE_WATER_MEDIA)
            before = self.conn.execute("SELECT COUNT(*) FROM kemidata_transformed").fetchone()[0]

            self.conn.execute(f"""
                DELETE FROM kemidata_transformed
                WHERE "{media_col}" IS NOT NULL
                  AND "{media_col}" NOT IN ({media_values})
            """)

            after = self.conn.execute("SELECT COUNT(*) FROM kemidata_transformed").fetchone()[0]
            self.log.info(f"Filtered to surface water: {before:,} → {after:,} rows")

        # Log statistics
        total = self.conn.execute("SELECT COUNT(*) FROM kemidata_transformed").fetchone()[0]
        with_geom = self.conn.execute(
            "SELECT COUNT(*) FROM kemidata_transformed WHERE geometry IS NOT NULL"
        ).fetchone()[0]

        self.log.info(f"Transformed: {total:,} rows, {with_geom:,} with geometry")

        return "kemidata_transformed"

    @timed(name="Validating geometries")
    def _validate_geometries(self, table_name: str) -> None:
        """Validate geometries are within Denmark bounds (EPSG:25832)."""
        b = DENMARK_BOUNDS_UTM
        invalid_count = self.conn.execute(f"""
            SELECT COUNT(*)
            FROM {table_name}
            WHERE geometry IS NOT NULL
            AND NOT ST_Within(
                geometry,
                ST_MakeEnvelope({b["min_x"]}, {b["min_y"]}, {b["max_x"]}, {b["max_y"]})
            )
        """).fetchone()[0]

        total_with_geom = self.conn.execute(f"""
            SELECT COUNT(*) FROM {table_name} WHERE geometry IS NOT NULL
        """).fetchone()[0]

        if invalid_count > 0:
            self.log.warning(
                f"{invalid_count:,} of {total_with_geom:,} geometries outside Denmark bounds"
            )
        else:
            self.log.info(f"All {total_with_geom:,} geometries validated within Denmark bounds")

    async def run(self, bronze_data: Any | None = None) -> dict[str, Any] | None:
        """
        Run the silver layer processing pipeline.

        Args:
            bronze_data: Optional manifest dict from bronze stage.
                If None, looks for latest manifest in cloud storage.

        Returns:
            Success info with statistics, or None on failure.
        """
        async with AsyncTimer("Running Kemidata Surface Water silver job"):
            self.log.info("Running Kemidata Surface Water silver job")

            try:
                # Get paths from bronze manifest
                csv_path = None
                stations_path = None

                if bronze_data is not None:
                    if not isinstance(bronze_data, dict):
                        self.log.error(f"Expected dict from bronze, got {type(bronze_data)}")
                        return None
                    csv_path = bronze_data.get("csv_path")
                    stations_path = bronze_data.get("stations_path")
                else:
                    # Find latest manifest
                    self.log.info("No bronze data — reading from cloud storage")
                    manifest_pattern = (
                        f"{self.config.bucket}/bronze/{self.config.dataset}/*/manifest.json"
                    )
                    manifest_files = self.storage.list_files(manifest_pattern)
                    if not manifest_files:
                        self.log.error("No manifest files found. Run bronze stage first.")
                        return None

                    latest = sorted(manifest_files, reverse=True)[0]
                    self.log.info(f"Reading manifest from {latest}")
                    manifest = self.storage.download_json(latest)
                    csv_path = manifest.get("csv_path")
                    stations_path = manifest.get("stations_path")

                if not csv_path:
                    self.log.error("Missing csv_path in manifest")
                    return None

                # Load station coordinates
                stations = []
                if stations_path:
                    stations = self._load_stations_from_storage(stations_path)

                # Load CSV
                self._load_csv_from_storage(csv_path)

                # Transform
                table_name = self._transform_data(stations)

                # Validate
                self._validate_geometries(table_name)

                # Get statistics
                stats = self.conn.execute(f"""
                    SELECT
                        COUNT(*) as total_records,
                        COUNT(DISTINCT geometry) as unique_locations,
                        COUNT(CASE WHEN geometry IS NOT NULL THEN 1 END) as with_geometry
                    FROM {table_name}
                """).fetchone()

                # Save to cloud storage silver layer
                self._save_data_with_metadata(
                    data=table_name,
                    dataset=self.config.dataset,
                    source_key="kemidata_surface_water_pesticides",
                    bucket=self.config.bucket,
                    stage="silver",
                )

                self.log.info("Kemidata silver job completed successfully")

                return {
                    "dataset": self.config.dataset,
                    "processed_at": datetime.now().isoformat(),
                    "status": "completed",
                    "total_records": stats[0],
                    "unique_locations": stats[1],
                    "records_with_geometry": stats[2],
                }

            except Exception as e:
                self.log.error(f"Error in Kemidata silver processing: {e}")
                import traceback

                self.log.error(traceback.format_exc())
                return None
