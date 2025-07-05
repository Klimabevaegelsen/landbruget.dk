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

import json
from typing import Any, Optional

# ✅ MIGRATION: Removed pandas import - using DuckDB for data operations
from unified_pipeline.common.base import BaseJobConfig, BaseSource, SilverJobInterface
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
    """

    dataset: str = "agricultural_fields"  # Primary dataset name for app.py silver data collection
    fields_dataset: str = "agricultural_fields"
    blocks_dataset: str = "agricultural_blocks"
    bucket: str = "landbrugsdata-raw-data"
    storage_batch_size: int = 5000

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
        "GB": "organic_farming",
        "GBanmeldt": "reported_area_ha",
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
                    self.log.warning("No valid features extracted from payloads")
                    return None, None

                # ✅ MIGRATION: Convert to table using DuckDB (no pandas conversion)
                # Create table directly from the list of dictionaries
                if not all_features:
                    self.log.warning("No features to process")
                    return None, None

                # Get column names from the first feature
                columns = list(all_features[0].keys())

                # Create the table schema with properly quoted column names
                quoted_column_definitions = [f'"{col}" VARCHAR' for col in columns]
                processing_conn.execute(f"""
                    CREATE OR REPLACE TABLE temp_features (
                        {", ".join(quoted_column_definitions)}
                    )
                """)

                # Insert data in batches
                batch_size = 1000
                for i in range(0, len(all_features), batch_size):
                    batch = all_features[i : i + batch_size]

                    # Use parameterized queries instead of string concatenation
                    for feature in batch:
                        values = [feature.get(col) for col in columns]
                        placeholders = ", ".join(["?" for _ in columns])
                        # Properly quote column names to handle special characters
                        quoted_columns = [f'"{col}"' for col in columns]

                        processing_conn.execute(
                            f"""
                            INSERT INTO temp_features ({", ".join(quoted_columns)})
                            VALUES ({placeholders})
                        """,
                            values,
                        )
                processing_conn.execute(
                    "CREATE OR REPLACE TABLE features_raw AS SELECT * FROM temp_features"
                )

                # ✅ MIGRATION: Get column info without  conversion
                columns_info = processing_conn.execute("DESCRIBE features_raw").fetchall()
                available_columns = [row[0] for row in columns_info]

                # Apply column mapping and create spatial geometries using DuckDB-spatial
                select_columns = []

                for old_col, new_col in self.config.column_mapping.items():
                    if old_col in available_columns:
                        # Apply proper type casting for area columns
                        if new_col in [
                            "area_ha",
                            "block_area_ha",
                            "applied_area_ha",
                            "reported_area_ha",
                        ]:
                            select_columns.append(f'CAST("{old_col}" AS DOUBLE) as {new_col}')
                        else:
                            select_columns.append(f'"{old_col}" as {new_col}')

                # Add unmapped columns (except geometry_json and payload_id)
                for col in available_columns:
                    if col not in self.config.column_mapping and col not in [
                        "geometry_json",
                        "payload_id",
                    ]:
                        # Clean column name
                        clean_col = (
                            col.replace(".", "_")
                            .replace("()", "_")
                            .replace("(", "_")
                            .replace(")", "_")
                        )
                        select_columns.append(f'"{col}" as {clean_col}')

                select_clause = ", ".join(select_columns) if select_columns else "*"

                # Create the final processed table with spatial geometries
                final_table_name = f"processed_features_{dataset}_{year}"
                processing_conn.execute(f"""
                    CREATE OR REPLACE TABLE {final_table_name} AS
                    SELECT 
                        {select_clause},
                        {year} as year,
                        ST_GeomFromGeoJSON(geometry_json) as geometry,
                        ST_IsValid(ST_GeomFromGeoJSON(geometry_json)) as is_valid_geometry
                    FROM features_raw
                    WHERE geometry_json IS NOT NULL
                """)

                # Transform geometries from EPSG:25832 to EPSG:4326 for consistency
                processing_conn.execute(f"""
                    UPDATE {final_table_name} 
                    SET geometry = ST_Transform(geometry, 'EPSG:25832', 'EPSG:4326')
                    WHERE is_valid_geometry = true
                """)

                # ✅ MIGRATION: Return table name instead of
                row_count = processing_conn.execute(
                    f"SELECT COUNT(*) FROM {final_table_name} WHERE is_valid_geometry = true"
                ).fetchone()[0]
                self.log.info(f"Processed {row_count} valid features using DuckDB-spatial")

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
            # Process agricultural fields for all years
            for dataset in [self.config.fields_dataset, self.config.blocks_dataset]:
                self.log.info(f"Processing {dataset} for all years using DuckDB-spatial")

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

                        # ✅ CLEANUP: Clean up temporary tables after processing each year
                        self.conn.execute(f"DROP TABLE IF EXISTS bronze_raw_{dataset}_{year}")
                        self.conn.execute("DROP TABLE IF EXISTS temp_features")
                        self.conn.execute("DROP TABLE IF EXISTS features_raw")
                        self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")

                        # Clean up temporary connection if it was created for bronze data processing
                        if hasattr(self, "_temp_raw_conn") and temp_conn != self.conn:
                            temp_conn.close()

                    except Exception as e:
                        self.log.error(f"Error processing {dataset} for year {year}: {e}")
                        continue

            self.log.info(
                "Agricultural Fields silver job completed for all available years using DuckDB-spatial"
            )

            # Return processed data for gold stage if any data was processed
            return processed_data if processed_data else None
