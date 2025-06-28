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

import pandas as pd

from unified_pipeline.common.base import BaseJobConfig, BaseSource, SilverJobInterface
from unified_pipeline.util.gcs_util import GCSUtil
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
        "Afgroede": "crop_type",
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

    def __init__(self, config: AgriculturalFieldsSilverConfig, gcs_util: GCSUtil):
        """
        Initialize the AgriculturalFieldsSilver processor.

        Args:
            config: Configuration for the silver processing job
            gcs_util: Utility for GCS operations
        """
        super().__init__(config, gcs_util)
        # Initialize DuckDB with spatial extension
        self._setup_duckdb()

    def _setup_duckdb(self):
        """Setup DuckDB connection with spatial extensions."""
        # Install and load spatial extension
        self.conn.execute("INSTALL spatial")
        self.conn.execute("LOAD spatial")
        self.log.info("✅ DuckDB-spatial initialized for agricultural fields processing")

    async def _process_payloads_with_duckdb(
        self, raw_df: pd.DataFrame, dataset: str, year: int
    ) -> Optional[pd.DataFrame]:
        """
        Process raw payloads using DuckDB-spatial for all geometric operations.

        This method uses DuckDB-spatial to parse GeoJSON features from ArcGIS API responses,
        handle coordinate transformations, and validate geometries.

        Args:
            raw_df: DataFrame containing raw payloads from the bronze layer
            dataset: Name of the dataset being processed
            year: Year of the data being processed

        Returns:
            A DataFrame containing all processed features with validated geometries,
            or None if processing fails
        """
        async with AsyncTimer("Processing data with DuckDB-spatial"):
            try:
                # Register the raw data with DuckDB
                self.conn.register("raw_payloads", raw_df)

                # Create a table to hold all extracted features
                self.conn.execute("DROP TABLE IF EXISTS extracted_features")
                self.conn.execute("""
                    CREATE TABLE extracted_features AS
                    SELECT 
                        payload,
                        ROW_NUMBER() OVER () as payload_id
                    FROM raw_payloads
                """)

                # Process each payload and extract GeoJSON features
                payloads = raw_df["payload"].tolist()
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
                    return None

                # Convert to DataFrame and register with DuckDB
                features_df = pd.DataFrame(all_features)
                self.conn.register("features_raw", features_df)

                # Apply column mapping and create spatial geometries using DuckDB-spatial
                column_mapping_sql = []
                for old_col, new_col in self.config.column_mapping.items():
                    column_mapping_sql.append(f'"{old_col}" as {new_col}')

                # Build the SELECT statement with available columns
                available_columns = features_df.columns.tolist()
                select_columns = []

                for old_col, new_col in self.config.column_mapping.items():
                    if old_col in available_columns:
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
                self.conn.execute(f"""
                    CREATE TABLE processed_features AS
                    SELECT 
                        {select_clause},
                        {year} as year,
                        ST_GeomFromGeoJSON(geometry_json) as geometry,
                        ST_IsValid(ST_GeomFromGeoJSON(geometry_json)) as is_valid_geometry
                    FROM features_raw
                    WHERE geometry_json IS NOT NULL
                """)

                # Transform geometries from EPSG:25832 to EPSG:4326 for consistency
                self.conn.execute("""
                    UPDATE processed_features 
                    SET geometry = ST_Transform(geometry, 'EPSG:25832', 'EPSG:4326')
                    WHERE is_valid_geometry = true
                """)

                # Get the final result as DataFrame with geometry as WKT
                result_df = self.conn.execute("""
                    SELECT 
                        * EXCLUDE (geometry, is_valid_geometry),
                        ST_AsText(geometry) as geometry
                    FROM processed_features 
                    WHERE is_valid_geometry = true
                """).df()

                self.log.info(f"Processed {len(result_df)} valid features using DuckDB-spatial")
                return result_df

            except Exception as e:
                self.log.error(f"Error processing data with DuckDB-spatial: {e}")
                return None

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
                            # Bronze data is expected to be a complex dict structure with multi-year data
                            if isinstance(bronze_data, dict) and dataset_with_year in bronze_data:
                                raw_data = bronze_data[dataset_with_year]
                                # Convert to DataFrame if it's not already
                                if not isinstance(raw_data, pd.DataFrame):
                                    raw_data = pd.DataFrame({"payload": raw_data})
                            else:
                                self.log.warning(f"No in-memory data found for {dataset_with_year}")
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
                        processed_df = await self._process_payloads_with_duckdb(
                            raw_data, dataset, year
                        )

                        if processed_df is None or processed_df.empty:
                            self.log.warning(f"No processed data for {dataset_with_year}, skipping")
                            continue

                        self.log.info(f"Processed raw data successfully for {dataset_with_year}")

                        # Store processed data for potential gold stage consumption
                        processed_data[dataset_with_year] = processed_df

                        # Save using DuckDB-spatial optimized format
                        self._save_data(
                            processed_df, dataset_with_year, self.config.bucket, stage="silver"
                        )
                        self.log.info(f"Saved processed data successfully for {dataset_with_year}")

                    except Exception as e:
                        self.log.error(f"Error processing {dataset} for year {year}: {e}")
                        continue

            self.log.info(
                "Agricultural Fields silver job completed for all available years using DuckDB-spatial"
            )

            # Return processed data for gold stage if any data was processed
            return processed_data if processed_data else None
