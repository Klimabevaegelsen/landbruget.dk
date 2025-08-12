"""
Silver layer processing for Agricultural Fields data.

This module transforms raw data (from the bronze layer) into cleaner,
more structured data for analytical purposes. It handles the extraction
of GeoJSON features from API responses, converts them using DuckDB-spatial,
and applies transformations such as column renaming and geometry validation.

The module consists of two main components:
- AgriculturalFieldsSilverConfig: Configuration for Silver processing
- AgriculturalFieldsSilver: Implementation of Silver processing logic using DuckDB-spatial

The process reads in bronze layer data, transforms it using DuckDB-spatial,
validates geometries, and stores the processed data in GCS.
"""

import gc
import json
import os
from typing import Any, Optional

import psutil

# ✅ MIGRATION: Removed pandas import - using DuckDB for data operations
from unified_pipeline.common.base import BaseJobConfig, BaseSource, SilverJobInterface
from unified_pipeline.common.geometry_validator import validate_and_transform_geometries_duckdb
from unified_pipeline.util.timing import AsyncTimer


class AgriculturalFieldsSilverConfig(BaseJobConfig):
    """
    Configuration for Agricultural Fields Silver data processing.

    This configuration defines parameters for transforming agricultural fields
    data from raw (bronze) to structured (silver) format, including dataset names,
    storage parameters, and column mappings.

    Attributes:
        dataset (str): Primary dataset name for silver data collection
        fields_dataset (str): Name of the agricultural fields dataset
        blocks_dataset (str): Name of the agricultural blocks dataset
        bucket (str): GCS bucket name for storing processed data
        storage_batch_size (int): Batch size for storage operations
        column_mapping (dict): Dictionary mapping raw field names to standardized names
        debug_memory (bool): Whether to log memory usage for debugging
        processing_batch_size (int): Batch size for processing features to reduce memory usage
    """

    dataset: str = "agricultural_fields"  # Primary dataset name for app.py silver data collection
    fields_dataset: str = "agricultural_fields"
    blocks_dataset: str = "agricultural_blocks"
    bucket: str = "landbrugsdata-raw-data"
    storage_batch_size: int = 5000
    debug_memory: bool = True  # Enable memory logging by default
    processing_batch_size: int = 10000  # Process features in batches to reduce memory usage

    # Years to process (these should match what's available in bronze)
    # Note: Fields have 2020-2025, Blocks have 2020-2024
    available_years: list[int] = [2020, 2021, 2022, 2023, 2024, 2025]
    column_mapping: dict[str, str] = {
        "Marknr": "field_id",
        "IMK_areal": "area_ha",
        "Journalnr": "journal_number",
        "CVR": "cvr_number",
        "Afgkode": "crop_code",
        "Afgroede": "crop_name",
        "GB": "grundbetaling_eligible",  # GB is yes/no for "grundbetaling" (basic payment) eligibility, NOT organic farming
        "GBanmeldt": "grundbetaling_area_ha",  # Actual grundbetaling area reported
        "Markblok": "block_id",
        "MB_NR": "block_id",
        "BLOKAREAL": "block_area_ha",
        "MARKBLOKTY": "block_type",
    }


class AgriculturalFieldsSilver(BaseSource[AgriculturalFieldsSilverConfig], SilverJobInterface):
    """
    Silver layer processor for agricultural fields data using DuckDB-spatial.

    This class transforms raw agricultural fields data from the bronze layer into
    structured spatial data using DuckDB-spatial for all geometric operations.
    It handles extracting GeoJSON features from API responses, validates geometries,
    standardizes column names, and saves the processed data.

    The processing includes:
    1. Reading raw data from GCS
    2. Extracting GeoJSON from each payload using DuckDB-spatial
    3. Validating and transforming geometries using DuckDB-spatial
    4. Standardizing column names using the mapping from config
    5. Saving processed data to GCS
    """

    def __init__(self, config: AgriculturalFieldsSilverConfig):
        """
        Initialize the AgriculturalFieldsSilver processor.

        Args:
            config: Configuration for the silver processing job"""
        super().__init__(config)
        # Initialize DuckDB with spatial extension
        self._setup_duckdb()

    def _setup_duckdb(self):
        """Setup DuckDB connection with spatial extensions."""
        # Install and load spatial extension
        self.conn.execute("INSTALL spatial")
        self.conn.execute("LOAD spatial")
        self.log.info("✅ DuckDB-spatial initialized for agricultural fields processing")

    def _cleanup_after_year(self, dataset: str, year: int, table_name: str, temp_conn):
        """
        Comprehensive cleanup after processing each year to prevent memory accumulation.

        Args:
            dataset: Dataset name being processed
            year: Year being processed
            table_name: Name of the processed table
            temp_conn: Temporary connection used for processing
        """
        self.log.info(f"🧹 Starting comprehensive cleanup for {dataset} year {year}")

        try:
            # 1. Drop all temporary tables from main connection
            cleanup_tables = [
                f"bronze_raw_{dataset}_{year}",
                "temp_features",
                "features_raw",
                table_name,
                f"temp_{table_name}",
                f"raw_data_{dataset}_{year}",
                f"temp_bronze_{dataset}_{year}",
            ]

            for table in cleanup_tables:
                try:
                    self.conn.execute(f"DROP TABLE IF EXISTS {table}")
                except Exception:
                    # Ignore errors for tables that don't exist
                    pass

            # 2. Clean up temporary connection if it exists
            if hasattr(self, "_temp_raw_conn") and temp_conn != self.conn:
                try:
                    temp_conn.close()
                except Exception as e:
                    self.log.warning(f"Error closing temp connection: {e}")

            # 3. Force DuckDB to release memory by running VACUUM and CHECKPOINT
            try:
                self.conn.execute("VACUUM")
                self.conn.execute("CHECKPOINT")
            except Exception as e:
                self.log.warning(f"Error during DuckDB cleanup: {e}")

            # 4. Force Python garbage collection
            collected = gc.collect()
            self.log.info(f"🧹 Cleaned up {dataset} year {year} - collected {collected} objects")

        except Exception as e:
            self.log.error(f"Error during cleanup for {dataset} year {year}: {e}")
            # Continue processing even if cleanup fails

    def _cleanup_between_datasets(self, dataset: str):
        """
        Clean up memory between processing different datasets.

        Args:
            dataset: Dataset name being processed
        """
        self.log.info(f"🧹 Cleaning up before processing {dataset}")

        try:
            # Drop any remaining temporary tables
            temp_tables = [
                "temp_features",
                "features_raw",
                "temp_responses",
                "bronze_raw_*",
                "processed_features_*",
            ]

            for table_pattern in temp_tables:
                try:
                    if "*" in table_pattern:
                        # Get all tables matching pattern
                        tables = self.conn.execute(
                            f"SELECT table_name FROM information_schema.tables WHERE table_name LIKE '{table_pattern.replace('*', '%')}'"
                        ).fetchall()
                        for table_row in tables:
                            self.conn.execute(f"DROP TABLE IF EXISTS {table_row[0]}")
                    else:
                        self.conn.execute(f"DROP TABLE IF EXISTS {table_pattern}")
                except Exception:
                    pass

            # Force DuckDB memory cleanup
            self.conn.execute("VACUUM")
            self.conn.execute("CHECKPOINT")

            # Force garbage collection
            collected = gc.collect()
            self.log.info(f"🧹 Pre-{dataset} cleanup collected {collected} objects")

        except Exception as e:
            self.log.warning(f"Error during pre-dataset cleanup: {e}")

    def _final_cleanup(self):
        """
        Final comprehensive cleanup after all processing is complete.
        """
        self.log.info("🧹 Starting final comprehensive cleanup")

        try:
            # Get all table names and drop them
            all_tables = self.conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()

            for table_row in all_tables:
                table_name = table_row[0]
                try:
                    self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                except Exception:
                    pass

            # Final DuckDB cleanup
            self.conn.execute("VACUUM")
            self.conn.execute("CHECKPOINT")

            # Final garbage collection
            collected = gc.collect()
            self.log.info(f"🧹 Final cleanup collected {collected} objects")

        except Exception as e:
            self.log.warning(f"Error during final cleanup: {e}")

    def _log_memory_usage(self, context: str):
        """
        Log current memory usage to help monitor cleanup effectiveness.

        Args:
            context: Context description for the memory measurement
        """
        if not self.config.debug_memory:
            return

        try:
            process = psutil.Process(os.getpid())
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / 1024 / 1024  # Convert to MB
            self.log.info(f"📊 Memory usage {context}: {memory_mb:.1f} MB")
        except Exception as e:
            self.log.warning(f"Error getting memory usage: {e}")

    async def _process_payloads_with_duckdb(self, raw_data_input, dataset: str, year: int):
        """
        Process raw payloads using DuckDB-spatial for all geometric operations.

        This method uses DuckDB-spatial to parse GeoJSON features from ArcGIS API responses,
        handle coordinate transformations, and validate geometries.

        Args:
            raw_data_input: Table name containing raw payloads from the bronze layer
            dataset: Name of the dataset being processed
            year: Year of the data being processed

        Returns:
            A tuple (table_name, connection) containing all processed features with validated geometries,
            or (None, None) if processing fails
        """
        async with AsyncTimer("Processing data with DuckDB-spatial"):
            try:
                # ✅ MIGRATION: Only handle table names now
                if isinstance(raw_data_input, str):
                    # Input is a table name
                    table_name = raw_data_input
                    processing_conn = self.conn

                    # Get payloads from table
                    payloads = [
                        row[0]
                        for row in processing_conn.execute(
                            f"SELECT payload FROM {table_name}"
                        ).fetchall()
                    ]
                else:
                    self.log.error(f"Expected table name (string), got {type(raw_data_input)}")
                    return None, None

                all_features = []

                for i, payload_json in enumerate(payloads):
                    try:
                        payload = json.loads(payload_json)
                        features = payload.get("features", [])

                        for feature in features:
                            # Extract properties and geometry
                            properties = feature.get("attributes", {})
                            geometry = feature.get("geometry", {})

                            if geometry and "rings" in geometry:
                                # Convert ArcGIS geometry to GeoJSON format
                                geojson_geom = {"type": "Polygon", "coordinates": geometry["rings"]}

                                # Add properties with geometry
                                feature_record = {
                                    "payload_id": i,
                                    "geometry_json": json.dumps(geojson_geom),
                                    **properties,
                                }
                                all_features.append(feature_record)

                    except Exception as e:
                        self.log.warning(f"Error processing payload {i}: {e}")
                        continue

                if not all_features:
                    self.log.warning("No features found in payloads")
                    return None, None

                # ✅ MIGRATION: Create temporary table using DuckDB directly
                processing_conn.execute(
                    "CREATE OR REPLACE TABLE temp_features AS SELECT * FROM (VALUES (1)) t(dummy) WHERE false"
                )

                # Get column names from first feature
                if all_features:
                    first_feature = all_features[0]
                    columns = list(first_feature.keys())

                    # Create table with proper column types
                    column_defs = []
                    for col in columns:
                        if col == "geometry_json":
                            column_defs.append(f"{col} VARCHAR")
                        elif col == "payload_id":
                            column_defs.append(f"{col} INTEGER")
                        else:
                            column_defs.append(f"{col} VARCHAR")

                    processing_conn.execute("DROP TABLE IF EXISTS temp_features")
                    processing_conn.execute(
                        f"CREATE TABLE temp_features ({', '.join(column_defs)})"
                    )

                    # Insert data in batches to reduce memory usage
                    batch_size = self.config.processing_batch_size
                    total_features = len(all_features)

                    for batch_start in range(0, total_features, batch_size):
                        batch_end = min(batch_start + batch_size, total_features)
                        batch_features = all_features[batch_start:batch_end]

                        # Prepare batch insert
                        placeholders = ", ".join(["?" for _ in columns])
                        values = []
                        for feature in batch_features:
                            values.append([feature.get(col) for col in columns])

                        # Insert batch
                        processing_conn.executemany(
                            f"INSERT INTO temp_features ({', '.join(columns)}) VALUES ({placeholders})",
                            values,
                        )

                        # Clear batch from memory
                        del batch_features, values

                        # Periodic cleanup during processing
                        if batch_start > 0 and batch_start % (batch_size * 5) == 0:
                            gc.collect()

                    # Clear the full features list from memory
                    del all_features

                # First, create a mapping of problematic column names to cleaned names
                temp_columns_info = processing_conn.execute("DESCRIBE temp_features").fetchall()
                temp_column_names = [row[0] for row in temp_columns_info]

                # Build column mapping for renaming problematic columns
                column_renames = {}
                clean_select_parts = []

                for col in temp_column_names:
                    # Clean column name
                    clean_col = (
                        col.replace(".", "_").replace("()", "_").replace("(", "_").replace(")", "_")
                    )

                    if col != clean_col:  # Only rename if different
                        column_renames[col] = clean_col
                        clean_select_parts.append(f'"{col}" as {clean_col}')
                    else:
                        clean_select_parts.append(f'"{col}"')

                # Create features_raw with cleaned column names
                if clean_select_parts:
                    clean_select_clause = ", ".join(clean_select_parts)
                    processing_conn.execute(
                        f"CREATE OR REPLACE TABLE features_raw AS SELECT {clean_select_clause} FROM temp_features"
                    )
                else:
                    processing_conn.execute(
                        "CREATE OR REPLACE TABLE features_raw AS SELECT * FROM temp_features"
                    )

                # ✅ MIGRATION: Get column info from the cleaned table
                columns_info = processing_conn.execute("DESCRIBE features_raw").fetchall()
                available_columns = [row[0] for row in columns_info]

                # DEBUG: Log available columns to understand the data structure
                self.log.info(f"Available columns in features_raw: {available_columns}")
                self.log.info(f"Column mapping configuration: {self.config.column_mapping}")

                # Apply column mapping and create spatial geometries using DuckDB-spatial
                select_columns = []

                for old_col, new_col in self.config.column_mapping.items():
                    # Check if the original column exists, or if its cleaned version exists
                    actual_col = old_col
                    if old_col not in available_columns:
                        # Try the cleaned version of the column name
                        cleaned_old_col = (
                            old_col.replace(".", "_")
                            .replace("()", "_")
                            .replace("(", "_")
                            .replace(")", "_")
                        )
                        if cleaned_old_col in available_columns:
                            actual_col = cleaned_old_col
                        else:
                            continue  # Skip if neither version exists

                    # Apply proper type casting for area columns
                    if new_col in [
                        "area_ha",
                        "block_area_ha",
                        "applied_area_ha",
                        "reported_area_ha",
                    ]:
                        select_columns.append(f'CAST("{actual_col}" AS DOUBLE) as {new_col}')
                    else:
                        select_columns.append(f'"{actual_col}" as {new_col}')

                # Add unmapped columns (except geometry_json and payload_id)
                mapped_columns = set()
                for old_col, new_col in self.config.column_mapping.items():
                    # Add both original and cleaned versions to mapped set
                    mapped_columns.add(old_col)
                    cleaned_old_col = (
                        old_col.replace(".", "_")
                        .replace("()", "_")
                        .replace("(", "_")
                        .replace(")", "_")
                    )
                    mapped_columns.add(cleaned_old_col)

                for col in available_columns:
                    if col not in mapped_columns and col not in [
                        "geometry_json",
                        "payload_id",
                    ]:
                        # Column names are already cleaned, so use them as-is
                        select_columns.append(f'"{col}"')

                # DEBUG: Log what columns will be selected
                self.log.info(f"Select columns built: {select_columns}")

                # Ensure we have at least some columns to select, fallback to all columns except special ones
                if not select_columns:
                    self.log.warning(
                        "No columns mapped, using all available columns except geometry_json and payload_id"
                    )
                    for col in available_columns:
                        if col not in ["geometry_json", "payload_id"]:
                            # Column names are already cleaned, so use them as-is
                            select_columns.append(f'"{col}"')

                select_clause = ", ".join(select_columns) if select_columns else "payload_id"

                # DEBUG: Log the final select clause
                self.log.info(f"Final select clause: {select_clause}")

                # Create the final processed table with spatial geometries
                final_table_name = f"processed_features_{dataset}_{year}"

                # DEBUG: Show sample data to understand structure
                sample_data = processing_conn.execute(
                    "SELECT * FROM features_raw LIMIT 1"
                ).fetchall()
                self.log.info(f"Sample data from features_raw: {sample_data}")

                sql_query = f"""
                    CREATE OR REPLACE TABLE {final_table_name} AS
                    SELECT 
                        {select_clause},
                        {year} as year,
                        ST_GeomFromGeoJSON(geometry_json) as geometry,
                        ST_IsValid(ST_GeomFromGeoJSON(geometry_json)) as is_valid_geometry
                    FROM features_raw
                    WHERE geometry_json IS NOT NULL
                """

                # DEBUG: Log the SQL query being executed
                self.log.info(f"Executing SQL query: {sql_query}")

                processing_conn.execute(sql_query)

                # ✅ MIGRATION: Use unified geometry validator instead of manual coordinate transformation
                validate_and_transform_geometries_duckdb(
                    processing_conn,
                    final_table_name,
                    "agricultural_fields",
                    geometry_column="geometry",
                )

                # ✅ MIGRATION: Return table name instead of
                row_count = processing_conn.execute(
                    f"SELECT COUNT(*) FROM {final_table_name} WHERE is_valid_geometry = true"
                ).fetchone()[0]
                self.log.info(f"Processed {row_count} valid features using DuckDB-spatial")

                # Clean up intermediate tables within the processing method
                processing_conn.execute("DROP TABLE IF EXISTS temp_features")
                processing_conn.execute("DROP TABLE IF EXISTS features_raw")

                # Return table name and connection for further processing
                return final_table_name, processing_conn

            except Exception as e:
                self.log.error(f"Error processing data with DuckDB-spatial: {e}")
                return None, None

    async def run(self, bronze_data: Optional[Any] = None) -> Optional[Any]:
        """
        Execute the silver processing job for all available years using DuckDB-spatial.

        This method orchestrates the processing of raw multi-year data from the bronze
        layer into structured spatial data using DuckDB-spatial for all operations.

        Args:
            bronze_data: Optional in-memory data from bronze stage. If provided,
                        this data will be used instead of reading from storage.

        Returns:
            Optional[Any]: Dictionary containing processed data by dataset and year
                          for potential gold stage consumption, or None if processing fails.
        """
        self.log.info(
            "Running Agricultural Fields silver job with DuckDB-spatial for all available years"
        )

        # Collect processed data for potential gold stage consumption
        processed_data = {}

        async with AsyncTimer("Agricultural Fields Silver Job (DuckDB-spatial)"):
            # Log initial memory usage
            self._log_memory_usage("at start of processing")

            # Process agricultural fields for all years
            for dataset in [self.config.fields_dataset, self.config.blocks_dataset]:
                self.log.info(f"Processing {dataset} for all years using DuckDB-spatial")

                # Clean up before starting each dataset
                self._cleanup_between_datasets(dataset)
                self._log_memory_usage(f"after cleanup before {dataset}")

                for year in self.config.available_years:
                    try:
                        dataset_with_year = f"{dataset}_{year}"
                        self.log.info(f"Processing {dataset} for year {year}")

                        # Read data with support for in-memory passing
                        if bronze_data is not None:
                            self.log.info("Using bronze data from memory (in-memory data passing)")
                            # ✅ MIGRATION: Bronze data structure: {"fields": {year: raw_data_list}, "blocks": {year: raw_data_list}}
                            if isinstance(bronze_data, dict):
                                # Map dataset names to bronze data keys
                                bronze_key = "fields" if "fields" in dataset else "blocks"
                                if bronze_key in bronze_data and year in bronze_data[bronze_key]:
                                    # Bronze data should contain raw data lists now
                                    raw_data_table = bronze_data[bronze_key][year]
                                    if isinstance(raw_data_table, list):
                                        # List of JSON strings - create table in silver layer's connection
                                        table_name = f"bronze_raw_{dataset}_{year}"
                                        self.conn.execute(
                                            f"CREATE OR REPLACE TABLE {table_name} (payload VARCHAR)"
                                        )
                                        for json_str in raw_data_table:
                                            self.conn.execute(
                                                f"INSERT INTO {table_name} VALUES (?)", [json_str]
                                            )
                                        raw_data = table_name
                                    else:
                                        self.log.error(
                                            f"Expected list of JSON strings from bronze, got {type(raw_data_table)}"
                                        )
                                        continue
                                else:
                                    self.log.warning(
                                        f"No in-memory data found for {bronze_key} year {year}"
                                    )
                                    continue
                            else:
                                self.log.warning(
                                    f"Unexpected bronze data structure: {type(bronze_data)}"
                                )
                                continue
                        else:
                            # Fallback to reading from storage
                            self.log.info("Reading bronze data from storage (fallback)")
                            raw_data = self._read_bronze_data(dataset_with_year, self.config.bucket)
                            if raw_data is None:
                                self.log.warning(
                                    f"No raw data found for {dataset_with_year}, skipping"
                                )
                                continue

                        self.log.info(f"Read raw data successfully for {dataset_with_year}")
                        table_name, temp_conn = await self._process_payloads_with_duckdb(
                            raw_data, dataset, year
                        )

                        if table_name is None or temp_conn is None:
                            self.log.warning(f"No processed data for {dataset_with_year}, skipping")
                            continue

                        self.log.info(f"Processed raw data successfully for {dataset_with_year}")

                        # ✅ MIGRATION: Save directly from table (no  conversion)
                        self._save_data(
                            table_name,
                            dataset_with_year,
                            self.config.bucket,
                            stage="silver",
                            conn=temp_conn,
                        )
                        self.log.info(f"Saved processed data successfully for {dataset_with_year}")

                        # Store table name for potential gold stage consumption
                        processed_data[dataset_with_year] = table_name

                        # ✅ ENHANCED CLEANUP: Comprehensive memory cleanup after each year
                        self._cleanup_after_year(dataset, year, table_name, temp_conn)
                        self._log_memory_usage(f"after processing {dataset} year {year}")

                    except Exception as e:
                        self.log.error(f"Error processing {dataset} for year {year}: {e}")
                        continue

            self.log.info(
                "Agricultural Fields silver job completed for all available years using DuckDB-spatial"
            )

            # Final cleanup after all processing
            self._final_cleanup()

            # Log memory usage after final cleanup
            self._log_memory_usage("after final cleanup")

            # Return processed data for gold stage if any data was processed
            return processed_data if processed_data else None
