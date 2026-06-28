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

import shutil
import struct
import tempfile
import zlib
from datetime import datetime
from pathlib import Path
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

        The bronze stage stores CSV exports via StorageAccess. Download to a
        temporary local file first because DuckDB cannot resolve the bare
        bucket/object path returned by the manifest.

        Returns the table name.
        """
        storage_uri = f"{self.config.bucket}/{csv_path}"
        self.log.info(f"Loading CSV from {storage_uri}")

        tmp_paths: list[str] = []
        try:
            with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
                tmp_paths.append(tmp.name)
                with self.storage.fs.open(storage_uri, "rb") as src:
                    shutil.copyfileobj(src, tmp)

            csv_read_path = tmp_paths[0]
            if self._is_zip_file(csv_read_path):
                csv_read_path = self._extract_first_zip_member(csv_read_path)
                tmp_paths.append(csv_read_path)

            escaped_tmp_path = csv_read_path.replace("'", "''")
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE raw_kemidata AS
                SELECT * FROM read_csv_auto('{escaped_tmp_path}',
                    header=true,
                    all_varchar=true,
                    delim=';',
                    strict_mode=false,
                    null_padding=true,
                    ignore_errors=true
                )
            """)
        finally:
            for tmp_path in tmp_paths:
                try:
                    Path(tmp_path).unlink(missing_ok=True)
                except OSError as e:
                    self.log.warning(f"Could not remove temporary CSV {tmp_path}: {e}")

        row_count = self.conn.execute("SELECT COUNT(*) FROM raw_kemidata").fetchone()[0]
        columns = [col[0] for col in self.conn.execute("DESCRIBE raw_kemidata").fetchall()]

        self.log.info(f"Loaded {row_count:,} rows with {len(columns)} columns")
        self.log.info(f"Columns: {columns}")

        return "raw_kemidata"

    @staticmethod
    def _is_zip_file(path: str) -> bool:
        with Path(path).open("rb") as f:
            return f.read(4) == b"PK\x03\x04"

    def _extract_first_zip_member(self, zip_path: str) -> str:
        """Extract the first local ZIP member without trusting the central directory."""
        with Path(zip_path).open("rb") as src:
            header = src.read(30)
            if len(header) != 30:
                raise ValueError("Kemidata ZIP payload is missing a local file header")

            (
                signature,
                _version,
                _flag_bits,
                compression_method,
                _modified_time,
                _modified_date,
                _crc32,
                compressed_size,
                _uncompressed_size,
                file_name_length,
                extra_field_length,
            ) = struct.unpack("<IHHHHHIIIHH", header)

            if signature != 0x04034B50:
                raise ValueError("Kemidata ZIP payload has an invalid local file header")

            member_name = src.read(file_name_length).decode("utf-8", errors="replace")
            src.read(extra_field_length)
            self.log.info(f"Extracting Kemidata ZIP member {member_name}")

            extracted_path = None
            try:
                with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
                    extracted_path = tmp.name
                    if compression_method == 8:  # deflate
                        decompressor = zlib.decompressobj(-zlib.MAX_WBITS)
                        while True:
                            chunk = src.read(1024 * 1024)
                            if not chunk:
                                break
                            data = decompressor.decompress(chunk)
                            if data:
                                tmp.write(data)
                            if decompressor.eof:
                                break
                        tail = decompressor.flush()
                        if tail:
                            tmp.write(tail)
                        if not decompressor.eof:
                            raise ValueError("Kemidata ZIP deflate stream ended before EOF")
                    elif compression_method == 0 and compressed_size > 0:  # stored
                        remaining = compressed_size
                        while remaining > 0:
                            chunk = src.read(min(1024 * 1024, remaining))
                            if not chunk:
                                raise ValueError("Kemidata ZIP stored member ended before EOF")
                            tmp.write(chunk)
                            remaining -= len(chunk)
                    else:
                        raise ValueError(
                            f"Unsupported Kemidata ZIP compression method {compression_method}"
                        )
            except Exception:
                if extracted_path is not None:
                    Path(extracted_path).unlink(missing_ok=True)
                raise

        return extracted_path

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
            "Stedtekst",
            "Målested navn",
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
            "StedID",
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

        x_col = self._find_column(
            columns,
            col_lower,
            ["x-koordinat", "Målested, x-koordinat", "x_coord", "x"],
        )
        y_col = self._find_column(
            columns,
            col_lower,
            ["y-koordinat", "Målested, y-koordinat", "y_coord", "y"],
        )
        media_col = self._find_column(columns, col_lower, ["Medie", "media_name", "Media"])

        # Build the actual transform with the detected columns.
        # The CSV carries coordinates, so prefer those to avoid duplicate rows
        # from non-unique station names in the metadata search result.
        if station_col:
            join_clause = f'r."{station_col}" = s.station_name'
        elif station_id_col:
            join_clause = f'r."{station_id_col}" = s.station_id'
        else:
            # No join possible — direct CSV coordinates may still provide geometry.
            self.log.warning(
                "Could not detect station column for geometry join. "
                "Falling back to CSV coordinates only."
            )
            join_clause = "1=0"  # No match, all NULLs for station columns

        if x_col and y_col:
            csv_x_expr = f"TRY_CAST(REPLACE(NULLIF(TRIM(r.\"{x_col}\"), ''), ',', '.') AS DOUBLE)"
            csv_y_expr = f"TRY_CAST(REPLACE(NULLIF(TRIM(r.\"{y_col}\"), ''), ',', '.') AS DOUBLE)"
            x_expr = f"COALESCE({csv_x_expr}, s.x)"
            y_expr = f"COALESCE({csv_y_expr}, s.y)"
        else:
            x_expr = "s.x"
            y_expr = "s.y"

        if media_col:
            media_expr = f"COALESCE(NULLIF(TRIM(r.\"{media_col}\"), ''), s.media_name)"
        else:
            media_expr = "s.media_name"

        if station_col:
            station_lookup_query = """
                CREATE OR REPLACE TABLE station_lookup AS
                SELECT
                    ANY_VALUE(station_id) AS station_id,
                    station_name,
                    ANY_VALUE(media_name) AS media_name,
                    ANY_VALUE(x) AS x,
                    ANY_VALUE(y) AS y
                FROM stations
                GROUP BY station_name
            """
        elif station_id_col:
            station_lookup_query = """
                CREATE OR REPLACE TABLE station_lookup AS
                SELECT
                    station_id,
                    ANY_VALUE(station_name) AS station_name,
                    ANY_VALUE(media_name) AS media_name,
                    ANY_VALUE(x) AS x,
                    ANY_VALUE(y) AS y
                FROM stations
                GROUP BY station_id
            """
        else:
            station_lookup_query = """
                CREATE OR REPLACE TABLE station_lookup AS
                SELECT
                    NULL::VARCHAR AS station_id,
                    NULL::VARCHAR AS station_name,
                    NULL::VARCHAR AS media_name,
                    NULL::DOUBLE AS x,
                    NULL::DOUBLE AS y
                WHERE FALSE
            """

        self.conn.execute(station_lookup_query)

        self.conn.execute(f"""
            CREATE OR REPLACE TABLE kemidata_transformed AS
            SELECT
                r.*,
                {x_expr} AS x_coord,
                {y_expr} AS y_coord,
                {media_expr} AS media_type,
                CASE
                    WHEN {x_expr} IS NOT NULL AND {y_expr} IS NOT NULL
                    THEN ST_Point({x_expr}, {y_expr})
                    ELSE NULL
                END AS geometry
            FROM raw_kemidata r
            LEFT JOIN station_lookup s ON {join_clause}
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

    @staticmethod
    def _find_column(
        columns: list[str], col_lower: dict[str, str], candidates: list[str]
    ) -> str | None:
        for candidate in candidates:
            if candidate in columns or candidate.lower() in col_lower:
                return col_lower.get(candidate.lower(), candidate)
        return None

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

                    for manifest_file in sorted(manifest_files, reverse=True):
                        self.log.info(f"Reading manifest from {manifest_file}")
                        manifest = self.storage.download_json(manifest_file)
                        candidate_csv_path = manifest.get("csv_path")
                        if not candidate_csv_path:
                            self.log.warning(f"Manifest {manifest_file} has no csv_path; skipping")
                            continue

                        candidate_storage_uri = f"{self.config.bucket}/{candidate_csv_path}"
                        if not self.storage.file_exists(candidate_storage_uri):
                            self.log.warning(
                                f"Manifest {manifest_file} points to missing CSV "
                                f"{candidate_storage_uri}; trying older manifest"
                            )
                            continue

                        csv_path = candidate_csv_path
                        stations_path = manifest.get("stations_path")
                        break
                    else:
                        self.log.error("No manifest with an existing CSV export found.")
                        return None

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
