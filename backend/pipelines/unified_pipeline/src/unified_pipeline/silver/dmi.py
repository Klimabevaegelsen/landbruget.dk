"""
DMI (Danish Meteorological Institute) Silver Layer Implementation

This module implements the silver layer data transformation for Danish climate data.
It processes raw bronze layer data from the DMI GovCloud API and transforms it into
clean, harmonized silver layer data following the medallion architecture.

The module contains:
- DMISilverConfig: Configuration class for the DMI silver transformation
- DMISilver: Implementation class for transforming DMI data using DuckDB

The data transformation includes geospatial processing, CRS transformation,
and statistical aggregation for multiple climate parameters.
"""

import json
from datetime import datetime
from typing import Any, Dict, Optional

from unified_pipeline.common.base import BaseJobConfig, BaseSource, SilverJobInterface
from unified_pipeline.util.timing import timed

class DMISilverConfig(BaseJobConfig):
    """
    Configuration for DMI Silver data processing.

    This configuration defines parameters for transforming DMI data
    from raw (bronze) to structured (silver) format, including dataset names,
    storage parameters, and geospatial configurations.

    Attributes:
        dataset (str): Primary dataset name for silver data collection
        bucket (str): GCS bucket name for storing processed data
        parameters (List[str]): List of climate parameters to process
        target_crs (str): Target coordinate reference system
        source_crs (str): Source coordinate reference system from DMI
    """

    dataset: str = "dmi"
    bucket: str = "landbrugsdata-raw-data"
    parameters: list[str] = ["pot_evaporation_makkink", "acc_precip"]
    target_crs: str = "EPSG:4326"  # Required target CRS
    source_crs: str = "EPSG:25832"  # DMI's native CRS

class DMISilver(BaseSource[DMISilverConfig], SilverJobInterface):
    """
    Silver layer processor for DMI climate data using DuckDB-spatial.

    This class transforms raw DMI climate data from the bronze layer into
    structured spatial data using DuckDB for all data operations and spatial
    extensions for geospatial transformations. It handles multiple climate
    parameters with CRS transformation and statistical aggregation.

    The processing includes:
    1. Reading raw data from GCS or in-memory bronze data
    2. Extracting GeoJSON features from API responses
    3. Transforming CRS using DuckDB spatial functions
    4. Calculating statistical aggregations
    5. Saving processed data to GCS
    """

    def __init__(self, config: DMISilverConfig):
        """
        Initialize the DMISilver processor.

        Args:
            config: Configuration for the silver processing job        """
        super().__init__(config)
        # Setup DuckDB with spatial extension
        self._setup_duckdb_spatial()

    def _setup_duckdb_spatial(self):
        """Setup DuckDB connection with spatial extensions."""
        try:
            # Install and load spatial extension
            self.conn.execute("INSTALL spatial")
            self.conn.execute("LOAD spatial")
            self.log.info("✅ DuckDB-spatial initialized for DMI climate data processing")
        except Exception as e:
            self.log.error(f"Failed to setup DuckDB spatial extension: {e}")
            raise

    async def _find_latest_bronze_data(
        self, parameter_id: str, bronze_data: Optional[Dict] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Find and load the most recent bronze data for a parameter.

        Args:
            parameter_id (str): Climate parameter identifier
            bronze_data (Optional[Dict]): In-memory bronze data from bronze stage

        Returns:
            Optional[Dict[str, Any]]: Parameter data and metadata, or None if not found
        """
        # If we have in-memory bronze data, use it directly
        if bronze_data and parameter_id in bronze_data:
            self.log.info(f"Using in-memory bronze data for parameter {parameter_id}")
            return bronze_data[parameter_id]

        # Fallback to storage if no in-memory data
        try:
            # List all bronze data files for this parameter
            pattern = f"gs://{self.config.bucket}/bronze/dmi/*/{parameter_id}_data.json"
            bronze_files = self.gcs_access.list_files(pattern)

            if not bronze_files:
                self.log.warning(f"No bronze data files found for parameter {parameter_id}")
                return None

            # Sort by path (which includes date) and get the most recent
            bronze_files.sort(reverse=True)
            latest_file = bronze_files[0]

            self.log.info(f"Loading latest bronze data from GCS: {latest_file}")

            # Load the data
            parameter_data = self.gcs_access.download_json(latest_file)

            # Try to load metadata
            metadata = None
            try:
                metadata_file = latest_file.replace("_data.json", "_metadata.json")
                metadata = self.gcs_access.download_json(metadata_file)
            except Exception as e:
                self.log.warning(f"Could not load metadata for {parameter_id}: {e}")

            return {
                "parameter_id": parameter_id,
                "data": parameter_data,
                "metadata": metadata,
            }

        except Exception as e:
            self.log.error(f"Failed to load bronze data from GCS for {parameter_id}: {e}")
            return None

    @timed(name="Transforming DMI climate data")
    def _transform_climate_data(self, raw_data: Dict, parameter_id: str) -> Optional[Any]:
        """
        Transform raw climate data into processed statistics using DuckDB-spatial.

        Args:
            raw_data (Dict): Raw climate data from DMI API
            parameter_id (str): Climate parameter identifier

        Returns:
            Optional[Any]: DuckDB relation with transformed data, or None if transformation fails
        """
        if not raw_data or "features" not in raw_data or not raw_data["features"]:
            self.log.warning(f"No features found in raw data for parameter {parameter_id}")
            return None

        try:
            # Create a list of dictionaries with extracted properties
            features = []
            for feature in raw_data["features"]:
                properties = feature.get("properties", {})
                geometry = feature.get("geometry", {})

                features.append(
                    {
                        "value": properties.get("value"),
                        "parameter_id": properties.get("parameterId", parameter_id),
                        "valid_time": properties.get("from"),
                        "created": properties.get("created"),
                        "geometry": json.dumps(geometry) if geometry else None,
                    }
                )

            # Create table directly from the list of dictionaries using DuckDB's native capabilities
            if not features:
                self.log.warning("No features to process")
                return None

            # Get column names from the first feature
            columns = list(features[0].keys())

            # Create the table schema
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE features_raw (
                    {", ".join([f"{col} VARCHAR" for col in columns])}
                )
            """)

            # Insert data in batches
            batch_size = 1000
            for i in range(0, len(features), batch_size):
                batch = features[i : i + batch_size]

                # Use parameterized queries instead of string concatenation
                for feature in batch:
                    values = [feature.get(col) for col in columns]
                    placeholders = ", ".join(["?" for _ in columns])

                    self.conn.execute(
                        f"""
                        INSERT INTO features_raw ({", ".join(columns)})
                        VALUES ({placeholders})
                    """,
                        values,
                    )

            # Create a table from the extracted features
            self.conn.execute("""
                CREATE OR REPLACE TABLE extracted_data AS
                SELECT
                    CAST(value AS DOUBLE) as value,
                    parameter_id,
                    valid_time,
                    created,
                    ST_GeomFromGeoJSON(geometry) as geometry
                FROM features_raw
                WHERE value IS NOT NULL AND geometry IS NOT NULL
            """)

            # Transform CRS using DuckDB's spatial functions
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE transformed_data AS
                SELECT
                    value,
                    parameter_id,
                    valid_time,
                    created,
                    ST_Transform(geometry, '{self.config.source_crs}', '{self.config.target_crs}') as geometry
                FROM extracted_data
            """)

            # Process data using DuckDB and calculate statistics
            result = self.conn.execute("""
                SELECT
                    parameter_id,
                    valid_time,
                    created,
                    AVG(value) as avg_value,
                    MIN(value) as min_value,
                    MAX(value) as max_value,
                    COUNT(*) as count,
                    STDDEV(value) as stddev_value,
                    ST_AsGeoJSON(ST_Centroid(ST_Union_Agg(geometry))) as centroid_geometry,
                    ST_AsGeoJSON(ST_Envelope(ST_Union_Agg(geometry))) as bbox_geometry
                FROM transformed_data
                GROUP BY parameter_id, valid_time, created
                ORDER BY valid_time DESC
            """)

            # Add processing metadata - create a new table instead of using result.query
            # Create a temporary table with the aggregation results
            self.conn.execute("""
                CREATE OR REPLACE TABLE aggregated_data AS
                SELECT
                    parameter_id,
                    valid_time,
                    created,
                    AVG(value) as avg_value,
                    MIN(value) as min_value,
                    MAX(value) as max_value,
                    COUNT(*) as count,
                    STDDEV(value) as stddev_value,
                    ST_AsGeoJSON(ST_Centroid(ST_Union_Agg(geometry))) as centroid_geometry,
                    ST_AsGeoJSON(ST_Envelope(ST_Union_Agg(geometry))) as bbox_geometry
                FROM transformed_data
                GROUP BY parameter_id, valid_time, created
                ORDER BY valid_time DESC
            """)

            # Now add the metadata columns
            processed_result = self.conn.execute(f"""
                SELECT *,
                    '{datetime.now().isoformat()}' as processing_time,
                    '{self.config.source_crs}' as source_crs,
                    '{self.config.target_crs}' as target_crs,
                    {len(raw_data["features"])} as original_feature_count
                FROM aggregated_data
            """)

            self.log.info(
                f"Successfully transformed {len(raw_data['features'])} records for parameter {parameter_id}"
            )
            return processed_result

        except Exception as e:
            self.log.error(f"Error transforming data for parameter {parameter_id}: {e}")
            return None

    async def run(self, bronze_data: Optional[Any] = None) -> Optional[Any]:
        """
        Run silver processing for DMI data.

        Args:
            bronze_data: Optional data from bronze stage. If None,
                        silver job will read from storage.

        Returns:
            Optional[Any]: Processed data that can be passed to gold stage,
                          or None if processing fails.
        """
        try:
            self.log.info("Starting DMI silver processing")

            all_processed_data = {}

            for parameter_id in self.config.parameters:
                self.log.info(f"Processing DMI parameter {parameter_id}")

                # Get bronze data for this parameter
                parameter_bronze_data = await self._find_latest_bronze_data(
                    parameter_id, bronze_data
                )
                if not parameter_bronze_data:
                    self.log.warning(f"No bronze data found for parameter {parameter_id}")
                    continue

                raw_data = parameter_bronze_data.get("data")
                metadata = parameter_bronze_data.get("metadata", {})

                if not raw_data:
                    self.log.warning(f"No raw data found for parameter {parameter_id}")
                    continue

                # Check if there was an error in bronze data
                if "error" in raw_data:
                    self.log.warning(
                        f"Bronze data contains error for parameter {parameter_id}: {raw_data['error']}"
                    )
                    continue

                # Transform the data
                processed_data = self._transform_climate_data(raw_data, parameter_id)

                if processed_data is not None:
                    # Check row count using the final table name instead of processed_data.query
                    try:
                        # Create a final table name for this parameter's processed data
                        final_table_name = f"dmi_{parameter_id}_final"

                        # Create the final table with the processed data
                        self.conn.execute(f"""
                            CREATE OR REPLACE TABLE {final_table_name} AS
                            SELECT *,
                                '{datetime.now().isoformat()}' as processing_time,
                                '{self.config.source_crs}' as source_crs,
                                '{self.config.target_crs}' as target_crs,
                                {len(raw_data["features"])} as original_feature_count
                            FROM aggregated_data
                        """)

                        # Get row count from the final table
                        row_count = self.conn.execute(
                            f"SELECT COUNT(*) FROM {final_table_name}"
                        ).fetchone()[0]
                    except Exception as e:
                        self.log.error(f"Error in row count check: {e}")
                        raise

                    if row_count > 0:
                        # ✅ MIGRATION: Save processed data directly from DuckDB relation
                        dataset_name = f"dmi_{parameter_id}"
                        try:
                            # Save using the table name instead of the processed_data object
                            self._save_data(
                                final_table_name,
                                dataset_name,
                                self.config.bucket,
                                "silver",
                                subdataset=dataset_name,
                                conn=self.conn,
                            )
                        except Exception as e:
                            self.log.error(f"Error saving data for {dataset_name}: {e}")
                            raise

                        all_processed_data[parameter_id] = final_table_name
                        self.log.info(
                            f"Successfully processed {row_count} records for parameter {parameter_id}"
                        )
                    else:
                        self.log.warning(
                            f"No records after processing for parameter {parameter_id}"
                        )
                else:
                    self.log.warning(f"Failed to process data for parameter {parameter_id}")

            if not all_processed_data:
                self.log.error("No DMI parameters were successfully processed")
                return None

            self.log.info(f"Successfully processed {len(all_processed_data)} DMI parameters")
            return all_processed_data

        except Exception as e:
            self.log.error(f"Error in DMI silver processing: {e}")
            return None
