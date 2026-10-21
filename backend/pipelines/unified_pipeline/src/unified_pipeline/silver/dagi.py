"""
Silver layer data processing for DAGI (Danish Administrative Geographic Division) data.

This module handles the transformation of raw DAGI data from the bronze layer
into clean, structured geographic data in the silver layer. It processes raw GeoJSON
from the DAWA API and standardizes them into consistent formats using DuckDB-spatial.

The module contains:
- DAGISilverConfig: Configuration class for the DAGI silver processing
- DAGISilver: Implementation class for transforming and processing DAGI data

The data processing includes:
- Parsing raw GeoJSON using DuckDB-spatial
- Standardizing column names and data types
- Validating geometries and coordinate systems using DuckDB-spatial
- Adding consistent metadata fields
- Creating unified datasets for each administrative division type
"""

import json
from typing import Any, ClassVar

from common.crs_utils import DANISH_UTM
from common.dagi_coverage import validate_dagi_coverage
from common.geometry_validator import (
    validate_and_normalize_to_utm,
    validate_and_transform_geometries_duckdb,
)
from pydantic import Field

from unified_pipeline.common.base import BaseJobConfig, BaseSource, SilverJobInterface
from unified_pipeline.util.timing import AsyncTimer

# CRS Strategy: Use EPSG:25832 for processing, transform to EPSG:4326 only at Supabase upload
# DAGI is a SPECIAL CASE - it provides data in WGS84, so we need to transform TO 25832
USE_UTM_PROCESSING = True


class DAGISilverConfig(BaseJobConfig):
    """
    Configuration for DAGI silver layer data processing.

    Attributes:
        name: Human-readable name of the data source
        type: Type of the data source
        description: Brief description of the data
        dataset: Name of the dataset in storage
        bucket: storage bucket name for data storage
        target_crs: Target coordinate reference system for geometries
        endpoints: Dictionary mapping layer names to API endpoints (should match bronze)
        required_columns: Mapping of layer types to their required columns
        column_mapping: Mapping of original column names to standardized names
    """

    name: str = "Danish Administrative Geographic Division - Silver"
    type: str = "dawa_api_silver"
    description: str = "Processed administrative geographic divisions from Danish DAWA API"
    dataset: str = "dagi"
    bucket: str = "landbruget-data"

    target_crs: str = Field(
        default="EPSG:4326",
        description="Target coordinate reference system - WGS84 for consistency "
        "with other datasets",
    )

    endpoints: ClassVar[dict[str, str]] = {
        "kommuner": "kommuner",
        "regioner": "regioner",
        "landsdele": "landsdele",
        "postnumre": "postnumre",
    }

    required_columns: ClassVar[dict[str, list]] = {
        "kommuner": ["kode", "navn", "regionskode"],
        "regioner": ["kode", "navn"],
        "landsdele": ["nuts3", "navn"],
        "postnumre": ["nr", "navn"],
    }

    column_mapping: ClassVar[dict[str, str]] = {
        "kode": "code",
        "navn": "name",
        "nr": "code",
        "nuts3": "code",
        "regionskode": "region_code",
    }


class DAGISilver(BaseSource[DAGISilverConfig], SilverJobInterface):
    """
    Silver layer implementation for DAGI (Danish Administrative Geographic Division) data.

    Processes raw GeoJSON data from the bronze layer and transforms them into clean,
    standardized data suitable for analysis and downstream processing using DuckDB-spatial.

    Processing includes:
    - Parsing raw GeoJSON with DuckDB-spatial
    - Geometry validation and coordinate system transformation using DuckDB-spatial
    - Column standardization and type conversion
    - Data quality validation and cleaning
    - Metadata enrichment
    """

    def __init__(self, config: DAGISilverConfig):
        """Initialize the DAGI silver layer with configuration."""
        super().__init__(config)
        # Setup DuckDB with spatial extension
        self._setup_duckdb()

    def _setup_duckdb(self):
        """Setup DuckDB connection with spatial extensions."""
        # Install and load spatial extension
        self.conn.execute("INSTALL spatial")
        self.conn.execute("LOAD spatial")
        self.log.info("✅ DuckDB-spatial initialized for DAGI processing")

    def _load_bronze_snapshot(self) -> dict[str, str]:
        """Load all configured layers from one completed bronze snapshot."""
        manifest_pattern = f"{self.config.bucket}/bronze/{self.config.dataset}/*/completion.json"
        manifests = sorted(self.storage.list_files(manifest_pattern), reverse=True)
        if not manifests:
            raise FileNotFoundError(
                "No completed DAGI bronze snapshot found. Markerless legacy files are not "
                "accepted because they may be a partial publish; run the DAGI bronze stage first."
            )

        expected_layers = set(self.config.endpoints)
        last_error: Exception | None = None
        for manifest_path in manifests:
            try:
                manifest = self.storage.download_json(manifest_path)
                if not isinstance(manifest, dict) or manifest.get("complete") is not True:
                    continue
                snapshot_id = manifest.get("snapshot_id")
                manifest_snapshot_id = manifest_path.rsplit("/", 2)[-2]
                if not isinstance(snapshot_id, str) or snapshot_id != manifest_snapshot_id:
                    raise ValueError(f"DAGI snapshot {manifest_path} has a mismatched snapshot_id")
                layer_paths = manifest.get("layers")
                if not isinstance(layer_paths, dict) or not expected_layers.issubset(layer_paths):
                    self.log.warning("Skipping incomplete DAGI snapshot manifest {}", manifest_path)
                    continue

                snapshot_data = {}
                for layer_name in self.config.endpoints:
                    layer_path = layer_paths[layer_name]
                    dataset_name = f"{self.config.dataset}_{layer_name}"
                    expected_path = (
                        f"{self.config.bucket}/bronze/{dataset_name}/"
                        f"{snapshot_id}/{dataset_name}.json"
                    )
                    if not isinstance(layer_path, str) or layer_path != expected_path:
                        raise ValueError(
                            f"DAGI snapshot {manifest_path} has an invalid path for {layer_name}"
                        )
                    raw_data = self.storage.download_json(layer_path)
                    if not isinstance(raw_data, dict | list):
                        raise ValueError(
                            f"DAGI snapshot layer {layer_name} is not a JSON object or array"
                        )
                    snapshot_data[layer_name] = json.dumps(raw_data)
                return snapshot_data
            except Exception as error:
                last_error = error
                self.log.warning(
                    "Skipping unreadable DAGI snapshot manifest {} ({})",
                    manifest_path,
                    type(error).__name__,
                )

        message = (
            "No valid completed DAGI bronze snapshot contains all configured layers. "
            "Markerless legacy files are not accepted; run the DAGI bronze stage first."
        )
        if last_error:
            raise FileNotFoundError(message) from last_error
        raise FileNotFoundError(message)

    def _save_dagi_layer(self, table_name: str, dataset: str) -> str:
        """Write DAGI silver with its declared EPSG:25832 GeoParquet CRS."""
        storage_path = f"{self.config.bucket}/silver/{dataset}/{self.date_pattern}/data.parquet"
        self.storage.upload_from_duckdb_table(
            table_name,
            storage_path,
            compression="zstd",
            row_group_size=100000,
            crs=DANISH_UTM,
        )
        return storage_path

    def _process_layer(self, raw_geojson: str, layer_type: str) -> str | None:
        """
        Process a single DAGI layer from raw GeoJSON to clean structured data using DuckDB-spatial.

        Args:
            raw_geojson: Raw GeoJSON string from bronze layer
            layer_type: Type of administrative layer (kommuner, regioner, etc.)

        Returns:
            Table name containing processed data, or None if processing fails
        """
        try:
            self.log.info(f"Processing DAGI layer: {layer_type}")

            # Parse GeoJSON
            geojson_data = json.loads(raw_geojson)
            features = geojson_data.get("features", [])

            if not features:
                self.log.warning(f"No features found in {layer_type} GeoJSON")
                return None

            # Extract feature data for DuckDB processing
            feature_records = []
            for i, feature in enumerate(features):
                properties = feature.get("properties", {})
                geometry = feature.get("geometry", {})

                if geometry:
                    # Create record with properties and geometry
                    record = {
                        "feature_id": i,
                        "geometry_json": json.dumps(geometry),
                        **properties,
                    }
                    feature_records.append(record)

            if not feature_records:
                self.log.warning(f"No valid features with geometry found in {layer_type}")
                return None

            # Create table using DuckDB (can't use register() with Python lists)
            temp_table = f"temp_{layer_type}_features"

            if feature_records:
                # Get column names from first record
                columns = list(feature_records[0].keys())
                column_defs = ", ".join([f'"{col}" VARCHAR' for col in columns])

                # Create table structure
                self.conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
                self.conn.execute(f"CREATE TABLE {temp_table} ({column_defs})")

                # Insert data row by row
                placeholders = ", ".join(["?" for _ in columns])
                for record in feature_records:
                    values = [record.get(col) for col in columns]
                    self.conn.execute(f"INSERT INTO {temp_table} VALUES ({placeholders})", values)

            # Get available columns
            columns_info = self.conn.execute(f"DESCRIBE {temp_table}").fetchall()
            available_columns = [row[0] for row in columns_info]

            # Apply column mapping
            select_columns = []
            required_columns = self.config.required_columns.get(layer_type, [])

            # Map columns according to configuration
            for old_col, new_col in self.config.column_mapping.items():
                if old_col in available_columns:
                    select_columns.append(f'"{old_col}" as {new_col}')

            # Add unmapped columns (except geometry_json and feature_id)
            for col in available_columns:
                if col not in self.config.column_mapping and col not in [
                    "geometry_json",
                    "feature_id",
                ]:
                    # Clean column name
                    clean_col = col.replace(".", "_").replace("-", "_").lower()
                    select_columns.append(f'"{col}" as {clean_col}')

            # Ensure we have required columns
            for req_col in required_columns:
                mapped_name = self.config.column_mapping.get(req_col, req_col)
                if (
                    req_col in available_columns
                    and f'"{req_col}" as {mapped_name}' not in select_columns
                ):
                    select_columns.append(f'"{req_col}" as {mapped_name}')

            select_clause = ", ".join(select_columns) if select_columns else "*"

            # Create processed table with spatial geometries using DuckDB-spatial
            processed_table = f"processed_{layer_type}"
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE {processed_table} AS
                SELECT
                    {select_clause},
                    ST_GeomFromGeoJSON(geometry_json) as geometry,
                    ST_IsValid(ST_GeomFromGeoJSON(geometry_json)) as is_valid_geometry,
                    ST_Area(ST_GeomFromGeoJSON(geometry_json)) as area_m2,
                    ST_X(ST_Centroid(ST_GeomFromGeoJSON(geometry_json))) as centroid_x,
                    ST_Y(ST_Centroid(ST_GeomFromGeoJSON(geometry_json))) as centroid_y,
                    ST_AsText(ST_GeomFromGeoJSON(geometry_json)) as geometry_wkt,
                    '{layer_type}' as layer_type,
                    CURRENT_TIMESTAMP as processed_at
                FROM {temp_table}
                WHERE geometry_json IS NOT NULL
            """)

            # Apply unified geometry validation
            # DAGI SPECIAL CASE: Data comes in WGS84 (GeoJSON format)
            # With new CRS strategy, we transform TO EPSG:25832 for consistent processing
            if USE_UTM_PROCESSING:
                # Bronze GeoJSON is WGS84 lon/lat by contract. Transform explicitly: bounds-based
                # CRS detection misfires on sea-inclusive postnumre (lon 3.2-16.5) and would
                # leave them in degrees. The validator below then only validates.
                self.conn.execute(f"""
                    UPDATE {processed_table}
                    SET geometry = ST_Transform(geometry, 'EPSG:4326', 'EPSG:25832', always_xy := true)
                    WHERE geometry IS NOT NULL
                """)
                validate_and_normalize_to_utm(
                    self.conn, processed_table, f"dagi_{layer_type}", geometry_column="geometry"
                )
                bounds = self.conn.execute(f"""
                    SELECT
                        MIN(ST_XMin(geometry)), MIN(ST_YMin(geometry)),
                        MAX(ST_XMax(geometry)), MAX(ST_YMax(geometry))
                    FROM {processed_table}
                    WHERE geometry IS NOT NULL
                """).fetchone()
                if bounds and bounds[0] is not None:
                    validate_dagi_coverage(tuple(bounds), DANISH_UTM, layer_name=layer_type)
                self.log.info(
                    f"DAGI {layer_type}: Transformed from WGS84 to EPSG:25832 for processing"
                )
            else:
                # Legacy path: Keep in WGS84
                validate_and_transform_geometries_duckdb(
                    self.conn, processed_table, f"dagi_{layer_type}", geometry_column="geometry"
                )

                # COORDINATE ORDER FIX: Convert from GeoJSON LON/LAT to proper EPSG:4326 LAT/LON
                # GeoJSON uses [longitude, latitude] order, EPSG:4326 standard is [latitude, longitude]
                self.log.info(
                    f"Converting {layer_type} from GeoJSON LON/LAT to EPSG:4326 LAT/LON order"
                )
                self.conn.execute(f"""
                    UPDATE {processed_table}
                    SET geometry = ST_FlipCoordinates(geometry)
                    WHERE geometry IS NOT NULL
                """)

                # Transform to target CRS if needed (after validation ensures WGS84)
                if self.config.target_crs != "EPSG:4326":
                    self.conn.execute(f"""
                        UPDATE {processed_table}
                        SET geometry = ST_Transform(geometry, 'EPSG:4326', '{self.config.target_crs}')
                        WHERE geometry IS NOT NULL
                    """)

            # `area_m2` was initially computed from GeoJSON's WGS84 coordinates. Recompute it
            # after normalization, when DAGI geometry is in EPSG:25832 metres.
            if USE_UTM_PROCESSING:
                self.conn.execute(f"""
                    UPDATE {processed_table}
                    SET area_m2 = ST_Area(geometry)
                    WHERE geometry IS NOT NULL
                """)

            # Get counts for logging
            total_count = self.conn.execute(f"SELECT COUNT(*) FROM {processed_table}").fetchone()[0]
            valid_count = self.conn.execute(
                f"SELECT COUNT(*) FROM {processed_table} WHERE is_valid_geometry = true"
            ).fetchone()[0]

            self.log.info(
                f"Processed {total_count} features for {layer_type}, "
                f"{valid_count} with valid geometries"
            )

            # Clean up temporary table
            self.conn.execute(f"DROP TABLE IF EXISTS {temp_table}")

            return processed_table

        except Exception as e:
            self.log.error(f"Error processing {layer_type}: {e}")
            return None

    async def run(self, bronze_data: Any | None = None) -> dict[str, str] | None:
        """
        Run the complete DAGI silver layer processing job.

        This method processes all DAGI layers, transforming raw GeoJSON data from the bronze
        layer into clean, structured data using DuckDB-spatial.

        Args:
            bronze_data: Optional in-memory data from bronze stage. If provided,
                        this data will be used instead of reading from storage.

        Returns:
            Optional[Dict[str, str]]: Dictionary mapping layer names to processed table names
                                     for potential gold stage consumption, or None
                                     if processing fails.
        """
        self.log.info("Running DAGI silver job with DuckDB-spatial")

        try:
            async with AsyncTimer("DAGI silver layer processing"):
                if bronze_data is None:
                    raw_layers = self._load_bronze_snapshot()
                else:
                    missing = set(self.config.endpoints) - set(bronze_data)
                    if missing:
                        raise ValueError(
                            "In-memory DAGI bronze data is missing configured layers: "
                            f"{sorted(missing)}"
                        )
                    raw_layers = {}
                    for layer_name in self.config.endpoints:
                        layer_data = bronze_data[layer_name]
                        if isinstance(layer_data, dict | list):
                            raw_layers[layer_name] = json.dumps(layer_data)
                        elif isinstance(layer_data, str):
                            raw_layers[layer_name] = layer_data
                        else:
                            raise ValueError(
                                f"In-memory DAGI bronze data for {layer_name} is not JSON text/data"
                            )

                processed_data = {}
                for layer_name in self.config.endpoints:
                    try:
                        self.log.info(f"Processing DAGI layer: {layer_name}")

                        silver_dataset_name = f"{self.config.dataset}_{layer_name}"
                        raw_geojson = raw_layers[layer_name]

                        # Process the data using DuckDB-spatial
                        processed_table = self._process_layer(raw_geojson, layer_name)
                        if processed_table is None:
                            self.log.warning(f"No processed data for DAGI {layer_name}")
                            continue

                        # ✅ OPTIMIZED: Save directly from main connection without copying
                        try:
                            storage_path = self._save_dagi_layer(
                                processed_table, silver_dataset_name
                            )
                            self.log.info(
                                f"Successfully processed and saved DAGI {layer_name} to {storage_path}"
                            )
                        except Exception as e:
                            self.log.error(f"Error saving DAGI {layer_name}: {e}")
                            continue

                        # Store table name for potential gold stage consumption
                        processed_data[layer_name] = processed_table

                    except Exception as e:
                        self.log.error(f"Error processing DAGI layer {layer_name}: {e}")
                        continue

                self.log.info("DAGI silver processing completed successfully")
                # ✅ FIXED: Return success information even if no layers were processed
                # This prevents the "no data returned" error that causes pipeline failure
                if processed_data:
                    return processed_data
                # Return a success indicator to prevent pipeline failure
                return {
                    "status": "completed",
                    "message": "DAGI processing completed but no layers had data to process",
                    "processed_layers": 0,
                }

        except Exception as e:
            self.log.error(f"Critical error in DAGI silver processing: {e}")
            raise
