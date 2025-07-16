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
from typing import Any, Dict, Optional

from pydantic import Field

from unified_pipeline.common.base import BaseJobConfig, BaseSource, SilverJobInterface
from unified_pipeline.util.timing import AsyncTimer


class DAGISilverConfig(BaseJobConfig):
    """
    Configuration for DAGI silver layer data processing.

    Attributes:
        name: Human-readable name of the data source
        type: Type of the data source
        description: Brief description of the data
        dataset: Name of the dataset in storage
        bucket: GCS bucket name for data storage
        target_crs: Target coordinate reference system for geometries
        endpoints: Dictionary mapping layer names to API endpoints (should match bronze)
        required_columns: Mapping of layer types to their required columns
        column_mapping: Mapping of original column names to standardized names
    """

    name: str = "Danish Administrative Geographic Division - Silver"
    type: str = "dawa_api_silver"
    description: str = "Processed administrative geographic divisions from Danish DAWA API"
    dataset: str = "dagi"
    bucket: str = "landbrugsdata-raw-data"

    target_crs: str = Field(
        default="EPSG:4326",
        description="Target coordinate reference system - WGS84 for consistency with other datasets",
    )

    endpoints: Dict[str, str] = Field(
        default={
            "kommuner": "kommuner",
            "regioner": "regioner",
            "landsdele": "landsdele",
            "postnumre": "postnumre",
        },
        description="Mapping of layer names to API endpoints (should match bronze config)",
    )

    required_columns: Dict[str, list] = Field(
        default={
            "kommuner": ["kode", "navn", "regionskode"],
            "regioner": ["kode", "navn"],
            "landsdele": ["nuts3", "navn"],
            "postnumre": ["nr", "navn"],
        },
        description="Required columns for each layer type",
    )

    column_mapping: Dict[str, str] = Field(
        default={
            "kode": "code",
            "navn": "name",
            "nr": "code",
            "nuts3": "code",
            "regionskode": "region_code",
        },
        description="Mapping of Danish column names to English standardized names",
    )


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

    def _process_layer(self, raw_geojson: str, layer_type: str) -> Optional[str]:
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

            # Transform to target CRS if needed (DAWA API returns EPSG:4326 by default)
            if self.config.target_crs != "EPSG:4326":
                self.conn.execute(f"""
                    UPDATE {processed_table} 
                    SET geometry = ST_Transform(geometry, 'EPSG:4326', '{self.config.target_crs}')
                    WHERE is_valid_geometry = true
                """)

            # Get counts for logging
            total_count = self.conn.execute(f"SELECT COUNT(*) FROM {processed_table}").fetchone()[0]
            valid_count = self.conn.execute(
                f"SELECT COUNT(*) FROM {processed_table} WHERE is_valid_geometry = true"
            ).fetchone()[0]

            self.log.info(
                f"Processed {total_count} features for {layer_type}, {valid_count} with valid geometries"
            )

            # Clean up temporary table
            self.conn.execute(f"DROP TABLE IF EXISTS {temp_table}")

            return processed_table

        except Exception as e:
            self.log.error(f"Error processing {layer_type}: {e}")
            return None

    async def run(self, bronze_data: Optional[Any] = None) -> Optional[Dict[str, str]]:
        """
        Run the complete DAGI silver layer processing job.

        This method processes all DAGI layers, transforming raw GeoJSON data from the bronze
        layer into clean, structured data using DuckDB-spatial.

        Args:
            bronze_data: Optional in-memory data from bronze stage. If provided,
                        this data will be used instead of reading from storage.

        Returns:
            Optional[Dict[str, str]]: Dictionary mapping layer names to processed table names
                                     for potential gold stage consumption, or None if processing fails.
        """
        self.log.info("Running DAGI silver job with DuckDB-spatial")

        processed_data = {}

        try:
            async with AsyncTimer("DAGI silver layer processing"):
                for layer_name in self.config.endpoints.keys():
                    try:
                        self.log.info(f"Processing DAGI layer: {layer_name}")

                        # Determine dataset names
                        bronze_dataset_name = f"{self.config.dataset}_{layer_name}"
                        silver_dataset_name = f"{self.config.dataset}_{layer_name}"

                        # Read data with support for in-memory passing
                        if bronze_data is not None and layer_name in bronze_data:
                            self.log.info(f"Using bronze data from memory for {layer_name}")
                            raw_geojson = bronze_data[layer_name]
                        else:
                            # Fallback to reading from storage
                            self.log.info(f"Reading bronze data from storage for {layer_name}")
                            bronze_df = self._read_bronze_data_from_storage(
                                bronze_dataset_name, self.config.bucket
                            )
                            if bronze_df is None:
                                self.log.warning(f"No bronze data found for {layer_name}")
                                continue
                            # Extract raw GeoJSON from bronze data
                            if hasattr(bronze_df, "iloc") and len(bronze_df) > 0:
                                # Assuming bronze data is stored as raw JSON string
                                raw_geojson = (
                                    bronze_df.iloc[0, 0] if len(bronze_df.columns) > 0 else None
                                )
                            else:
                                raw_geojson = bronze_df

                        if raw_geojson is None:
                            self.log.warning(f"No raw data available for {layer_name}")
                            continue

                        # Process the data using DuckDB-spatial
                        processed_table = self._process_layer(raw_geojson, layer_name)
                        if processed_table is None:
                            self.log.warning(f"No processed data for DAGI {layer_name}")
                            continue

                        # ✅ OPTIMIZED: Save directly from main connection without copying
                        try:
                            gcs_path = self.save_data_direct(
                                processed_table, silver_dataset_name, self.config.bucket, "silver"
                            )
                            self.log.info(
                                f"Successfully processed and saved DAGI {layer_name} to {gcs_path}"
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
                return processed_data if processed_data else None

        except Exception as e:
            self.log.error(f"Critical error in DAGI silver processing: {e}")
            raise
