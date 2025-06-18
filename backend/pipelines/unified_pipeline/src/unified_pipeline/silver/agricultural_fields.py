"""
Silver layer processing for Agricultural Fields data.

This module transforms raw data (from the bronze layer) into cleaner,
more structured data for analytical purposes. It handles the extraction
of GeoJSON features from API responses, converts them to GeoDataFrames,
and applies transformations such as column renaming and geometry validation.

The module consists of two main components:
- AgriculturalFieldsSilverConfig: Configuration for Silver processing
- AgriculturalFieldsSilver: Implementation of Silver processing logic

The process reads in bronze layer data, transforms it into GeoDataFrames,
validates geometries, and stores the processed data in GCS.
"""

import asyncio
import json

import geopandas as gpd
import pandas as pd

from unified_pipeline.common.base import BaseJobConfig, BaseSource
from unified_pipeline.util.gcs_util import GCSUtil
from unified_pipeline.util.geometry_validator import validate_and_transform_geometries
from unified_pipeline.util.timing import AsyncTimer


class AgriculturalFieldsSilverConfig(BaseJobConfig):
    """
    Configuration for Agricultural Fields Silver data processing.

    This configuration defines parameters for transforming agricultural fields
    data from raw (bronze) to structured (silver) format, including dataset names,
    storage parameters, and column mappings.

    Attributes:
        fields_dataset (str): Name of the agricultural fields dataset
        blocks_dataset (str): Name of the agricultural blocks dataset
        bucket (str): GCS bucket name for storing processed data
        storage_batch_size (int): Batch size for storage operations
        column_mapping (dict): Dictionary mapping raw field names to standardized names
    """

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


class AgriculturalFieldsSilver(BaseSource[AgriculturalFieldsSilverConfig]):
    """
    Silver layer processor for agricultural fields data.

    This class transforms raw agricultural fields data from the bronze layer into
    structured GeoDataFrames. It handles extracting GeoJSON features from API responses,
    validates geometries, standardizes column names, and saves the processed data.

    The processing includes:
    1. Reading raw data from GCS
    2. Extracting GeoJSON from each payload and converting to GeoDataFrames
    3. Validating and transforming geometries
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

    async def extract_geojson_from_payload(
        self, payload_json: str, column_mapping: dict
    ) -> gpd.GeoDataFrame:
        """
        Extract GeoJSON features from a raw payload and convert to GeoDataFrame.

        This method parses a JSON string payload containing features from the ArcGIS API,
        converts them to proper GeoJSON format, and creates a GeoDataFrame with standardized
        column names.

        Args:
            payload_json: JSON string containing features from ArcGIS API response
            column_mapping: Dictionary mapping original column names to standardized names

        Returns:
            A GeoDataFrame containing the extracted features with standardized column names,
            or an empty GeoDataFrame if extraction fails or no features are found

        Note:
            The source data uses EPSG:25832 coordinate system (ETRS89 / UTM zone 32N)
        """
        try:
            payload = json.loads(payload_json)
            features = payload.get("features", [])

            # Convert features to GeoJSON format
            geojson_features = []
            for feature in features:
                geojson_feature = {
                    "type": "Feature",
                    "properties": feature["attributes"],
                    "geometry": {"type": "Polygon", "coordinates": feature["geometry"]["rings"]},
                }
                geojson_features.append(geojson_feature)

            if geojson_features:
                geo_df = gpd.GeoDataFrame.from_features(geojson_features, crs="EPSG:25832")
                geo_df = geo_df.rename(columns=column_mapping)
                return geo_df  # type: ignore[no-any-return]
            else:
                return gpd.GeoDataFrame()
        except Exception as e:
            self.log.error(f"Error parsing payload: {e}")
            return gpd.GeoDataFrame()

    async def _process_data(
        self, raw_df: pd.DataFrame, dataset: str, year: int
    ) -> gpd.GeoDataFrame:
        """
        Process raw data into a clean GeoDataFrame.

        This method takes raw data from the bronze layer, extracts GeoJSON features from each
        payload in parallel, and combines them into a single GeoDataFrame. It also handles
        column name cleaning and geometry validation.

        Args:
            raw_df: DataFrame containing raw payloads from the bronze layer
            dataset: Name of the dataset being processed (used for validation)
            year: Year of the data being processed

        Returns:
            A GeoDataFrame containing all processed features with validated geometries,
            or an empty GeoDataFrame if processing fails

        Steps:
        1. Extract GeoJSON features from each payload in parallel
        2. Combine all extracted features into a single GeoDataFrame
        3. Clean column names by replacing special characters with underscores
        4. Validate and transform geometries using the dataset name
        """
        async with AsyncTimer("Processing data"):
            payloads = raw_df["payload"].tolist()
            tasks = [
                self.extract_geojson_from_payload(payload, self.config.column_mapping)
                for payload in payloads
            ]
            geo_dfs_list = await asyncio.gather(*tasks)
            # Filter out empty dataframes
            geo_dfs_list = [gdf for gdf in geo_dfs_list if not gdf.empty]

            if not geo_dfs_list:
                return gpd.GeoDataFrame()
            geo_df = gpd.GeoDataFrame(pd.concat(geo_dfs_list, ignore_index=True))

            # Clean column names by replacing special characters with underscores
            geo_df.columns = [
                col.replace(".", "_").replace("()", "_").replace("(", "_").replace(")", "_")
                for col in geo_df.columns
            ]

            # Add year information
            if not geo_df.empty:
                geo_df["year"] = year

            # Validate and transform geometries
            dataset_with_year = f"{dataset}_{year}"
            geo_df = validate_and_transform_geometries(geo_df, dataset_with_year)

            return geo_df

    async def run(self) -> None:
        """
        Execute the silver processing job for all available years.

        This method orchestrates the processing of raw multi-year data from the bronze
        layer into structured GeoDataFrames. It reads raw data for both agricultural
        fields and blocks for all available years, processes each dataset separately,
        and saves the results to Google Cloud Storage.

        The processing workflow for each dataset and year:
        1. Read raw data from GCS using the configured bucket and year
        2. Process raw data into GeoDataFrames with standardized column names
        3. Add year information to the processed data
        4. Validate geometries and apply any needed transformations
        5. Save processed data back to GCS with year information

        Returns:
            None

        Note:
            If any step fails for a particular year, the method logs an error and
            continues with the next year to ensure maximum data availability.
        """
        self.log.info("Running Agricultural Fields silver job for all available years")
        async with AsyncTimer("Agricultural Fields Silver Job"):
            # Process agricultural fields for all years
            for dataset in [self.config.fields_dataset, self.config.blocks_dataset]:
                self.log.info(f"Processing {dataset} for all years")

                for year in self.config.available_years:
                    try:
                        dataset_with_year = f"{dataset}_{year}"
                        self.log.info(f"Processing {dataset} for year {year}")

                        raw_data = self._read_bronze_data(dataset_with_year, self.config.bucket)
                        if raw_data is None:
                            self.log.warning(f"No raw data found for {dataset_with_year}, skipping")
                            continue

                        self.log.info(f"Read raw data successfully for {dataset_with_year}")
                        geo_df = await self._process_data(raw_data, dataset, year)

                        if geo_df is None or geo_df.empty:
                            self.log.warning(f"No processed data for {dataset_with_year}, skipping")
                            continue

                        self.log.info(f"Processed raw data successfully for {dataset_with_year}")
                        self._save_data(geo_df, dataset_with_year, self.config.bucket)
                        self.log.info(f"Saved processed data successfully for {dataset_with_year}")

                    except Exception as e:
                        self.log.error(f"Error processing {dataset} for year {year}: {e}")
                        continue

            self.log.info("Agricultural Fields silver job completed for all available years")
