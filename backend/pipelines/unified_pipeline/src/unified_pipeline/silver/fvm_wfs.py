"""
Silver layer processing for FVM WFS Agricultural data.

This module transforms raw WFS data (from the bronze layer) into cleaner,
more structured GeoDataFrames for analytical purposes. It handles the extraction
of GeoJSON features from WFS responses, converts them to GeoDataFrames,
and applies transformations such as column renaming and geometry validation.

The module processes three types of FVM data:
- Markblokke (field blocks): Primary field boundary data 2005-2026
- Marker (field markers): Field usage/application data 2008-2025
- Smaabiotoper (small biotopes): Special biotope layers 2023-2025

The module consists of two main components:
- FVMWFSSilverConfig: Configuration for Silver processing
- FVMWFSSilver: Implementation of Silver processing logic
"""

import asyncio
import json
from typing import Any, Dict, List, Optional

import geopandas as gpd
import pandas as pd
from pydantic import ConfigDict

from unified_pipeline.common.base import BaseJobConfig, BaseSource, SilverJobInterface
from unified_pipeline.util.gcs_util import GCSUtil
from unified_pipeline.util.geometry_validator import validate_and_transform_geometries
from unified_pipeline.util.timing import AsyncTimer


class FVMWFSSilverConfig(BaseJobConfig):
    """
    Configuration for FVM WFS Silver data processing.

    This configuration defines parameters for transforming FVM WFS data
    from raw (bronze) to structured (silver) format, including dataset names,
    storage parameters, and column mappings.

    Attributes:
        name (str): Human-readable name of the data processing
        type (str): Type of the data processing
        dataset (str): Primary dataset name for silver data collection
        dataset_markblokke (str): Name of the markblokke dataset
        dataset_marker (str): Name of the marker dataset
        dataset_smaabiotoper (str): Name of the smaabiotoper dataset
        bucket (str): GCS bucket name for storing processed data
        storage_batch_size (int): Batch size for storage operations
        markblokke_years (List[int]): Years to process for Markblokke (2005-2026)
        marker_years (List[int]): Years to process for Marker (2008-2025)
        smaabiotoper_years (List[int]): Years to process for Smaabiotoper (2023-2025)
        column_mapping (Dict): Dictionary mapping raw field names to standardized names
    """

    name: str = "Danish FVM WFS Agricultural Data - Silver"
    type: str = "transformation"
    dataset: str = "fvm_wfs"  # Primary dataset name for app.py silver data collection

    # Bronze dataset names (for reading from bronze storage)
    bronze_dataset_markblokke: str = "fvm_markblokke"
    bronze_dataset_marker: str = "fvm_marker"

    # Silver dataset names (for saving to silver storage and test expectations)
    dataset_markblokke: str = "fvm_markblokke_silver"
    dataset_marker: str = "fvm_marker_silver"
    dataset_smaabiotoper: str = "fvm_smaabiotoper_silver"

    bucket: str = "landbrugsdata-raw-data"
    storage_batch_size: int = 5000

    # Year ranges based on FVM WFS capabilities
    markblokke_years: List[int] = list(range(2005, 2027))  # 2005-2026 (22 years)
    marker_years: List[int] = list(range(2008, 2026))  # 2008-2025 (18 years)
    smaabiotoper_years: List[int] = [2023, 2024, 2025]  # Special biotope layers

    # Column mapping for standardization
    # Markblokke fields
    markblokke_column_mapping: Dict[str, str] = {
        "MB_NR": "block_id",
        "BLOKAREAL": "block_area_ha",
        "MARKBLOKTY": "block_type",
        "STATUSOPL": "status_info",
        "NOTAT": "notes",
        "BRUGER_ID": "user_id",
        "OPRINDATO": "creation_date",
        "CVR": "cvr_number",
        "JOURNALNR": "journal_number",
    }

    # Marker fields
    marker_column_mapping: Dict[str, str] = {
        "Marknr": "field_id",
        "IMK_areal": "area_ha",
        "Journalnr": "journal_number",
        "CVR": "cvr_number",
        "Afgkode": "crop_code",
        "Afgroede": "crop_type",
        "GB": "organic_farming",
        "GBanmeldt": "reported_area_ha",
        "Markblok": "block_id",
        "MarkblokNr": "block_number",
        "BRUGER_ID": "user_id",
        "OPRINDATO": "creation_date",
        "NOTAT": "notes",
    }

    # Smaabiotoper fields (similar to Marker but with biotope-specific fields)
    smaabiotoper_column_mapping: Dict[str, str] = {
        "Marknr": "field_id",
        "IMK_areal": "area_ha",
        "Journalnr": "journal_number",
        "CVR": "cvr_number",
        "Afgkode": "biotope_code",
        "Afgroede": "biotope_type",
        "GB": "organic_farming",
        "GBanmeldt": "reported_area_ha",
        "Markblok": "block_id",
        "MarkblokNr": "block_number",
        "BRUGER_ID": "user_id",
        "OPRINDATO": "creation_date",
        "NOTAT": "notes",
    }

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class FVMWFSSilver(BaseSource[FVMWFSSilverConfig], SilverJobInterface):
    """
    Silver layer processor for FVM WFS agricultural data.

    This class transforms raw FVM WFS data from the bronze layer into
    structured GeoDataFrames. It handles extracting GeoJSON features from WFS responses,
    validates geometries, standardizes column names, and saves the processed data.

    The processing includes:
    1. Reading raw WFS data from GCS
    2. Extracting GeoJSON features from each payload and converting to GeoDataFrames
    3. Validating and transforming geometries
    4. Standardizing column names using the mapping from config
    5. Saving processed data to GCS for each year
    """

    def __init__(self, config: FVMWFSSilverConfig, gcs_util: GCSUtil):
        """
        Initialize the FVMWFSSilver processor.

        Args:
            config: Configuration for the silver processing job
            gcs_util: Utility for GCS operations
        """
        super().__init__(config, gcs_util)

    async def extract_geojson_from_wfs_payload(
        self, payload_json: str, column_mapping: Dict[str, str]
    ) -> gpd.GeoDataFrame:
        """
        Extract GeoJSON features from a raw WFS payload and convert to GeoDataFrame.

        This method parses a JSON string payload containing features from the FVM WFS response,
        converts them to proper GeoJSON format, and creates a GeoDataFrame with standardized
        column names.

        Args:
            payload_json: JSON string containing features from FVM WFS response
            column_mapping: Dictionary mapping original column names to standardized names

        Returns:
            A GeoDataFrame containing the extracted features with standardized column names,
            or an empty GeoDataFrame if extraction fails or no features are found

        Note:
            The source data uses EPSG:25832 coordinate system (UTM Zone 32N)
        """
        try:
            payload = json.loads(payload_json)
            features = payload.get("features", [])

            if not features:
                self.log.warning("No features found in payload")
                return gpd.GeoDataFrame()

            # Convert features to proper GeoDataFrame
            geo_df = gpd.GeoDataFrame.from_features(features, crs="EPSG:25832")

            # Apply column mapping if any columns match
            if column_mapping:
                geo_df = geo_df.rename(columns=column_mapping)

            return geo_df  # type: ignore[no-any-return]

        except json.JSONDecodeError as e:
            self.log.error(f"Error parsing JSON payload: {e}")
            return gpd.GeoDataFrame()
        except Exception as e:
            self.log.error(f"Error processing payload: {e}")
            return gpd.GeoDataFrame()

    async def _process_data(
        self, raw_df: pd.DataFrame, layer_type: str, year: int
    ) -> gpd.GeoDataFrame:
        """
        Process raw data into a clean GeoDataFrame.

        This method takes raw data from the bronze layer, extracts GeoJSON features from each
        payload in parallel, and combines them into a single GeoDataFrame. It also handles
        column name cleaning and geometry validation.

        Args:
            raw_df: DataFrame containing raw payloads from the bronze layer
            layer_type: Type of layer being processed (Markblokke, Marker, Smaabiotoper)
            year: Year of the data being processed

        Returns:
            A GeoDataFrame containing all processed features with validated geometries,
            or an empty GeoDataFrame if processing fails
        """
        async with AsyncTimer(f"Processing {layer_type} data for {year}"):
            payloads = raw_df["payload"].tolist()

            # Get appropriate column mapping based on layer type
            if layer_type == "Markblokke":
                column_mapping = self.config.markblokke_column_mapping
            elif layer_type == "Smaabiotoper":
                column_mapping = self.config.smaabiotoper_column_mapping
            else:  # Marker
                column_mapping = self.config.marker_column_mapping

            # Extract GeoJSON features from each payload
            tasks = [
                self.extract_geojson_from_wfs_payload(payload, column_mapping)
                for payload in payloads
            ]
            geo_dfs_list = await asyncio.gather(*tasks)

            # Filter out empty dataframes
            geo_dfs_list = [gdf for gdf in geo_dfs_list if not gdf.empty]

            if not geo_dfs_list:
                self.log.warning(f"No valid data extracted for {layer_type} {year}")
                return gpd.GeoDataFrame()

            # Combine all GeoDataFrames
            geo_df = gpd.GeoDataFrame(pd.concat(geo_dfs_list, ignore_index=True))

            # Clean column names by replacing special characters with underscores
            geo_df.columns = [
                col.replace(".", "_").replace("()", "_").replace("(", "_").replace(")", "_")
                for col in geo_df.columns
            ]

            # Add metadata
            if not geo_df.empty:
                geo_df["year"] = year
                geo_df["layer_type"] = layer_type
                geo_df["processed_at"] = pd.Timestamp.now()

            # Validate and transform geometries
            dataset_with_year = f"fvm_{layer_type.lower()}_{year}"
            geo_df = validate_and_transform_geometries(geo_df, dataset_with_year)

            return geo_df

    async def _process_layer_type(
        self,
        layer_type: str,
        years: List[int],
        bronze_dataset_name: str,
        silver_dataset_name: str,
        bronze_data: Optional[Any] = None,
    ) -> None:
        """
        Process all years for a specific layer type.

        Args:
            layer_type: Type of layer to process (Markblokke, Marker, Smaabiotoper)
            years: List of years to process
            bronze_dataset_name: Base dataset name for reading from bronze storage
            silver_dataset_name: Base dataset name for saving to silver storage
            bronze_data: Optional in-memory data from bronze stage
        """
        self.log.info(f"Processing {layer_type} silver data for {len(years)} years")

        for year in years:
            try:
                bronze_dataset_with_year = f"{bronze_dataset_name}_{year}"
                silver_dataset_with_year = f"{silver_dataset_name}_{year}"
                self.log.info(f"Processing {layer_type} for year {year}")

                # Read data with support for in-memory passing
                if bronze_data is not None:
                    self.log.info("Using bronze data from memory (in-memory data passing)")
                    # Bronze data structure: {layer_type: {year: raw_data}}
                    layer_data = bronze_data.get(layer_type.lower(), {})
                    if year in layer_data:
                        raw_data = layer_data[year]
                        # Convert to DataFrame if it's not already
                        if not isinstance(raw_data, pd.DataFrame):
                            raw_data = pd.DataFrame({"payload": [raw_data]})
                    else:
                        self.log.warning(f"No in-memory data found for {layer_type} {year}")
                        continue
                else:
                    # Fallback to reading from storage
                    self.log.info("Reading bronze data from storage (fallback)")
                    raw_data = self._read_bronze_data(bronze_dataset_with_year, self.config.bucket)
                    if raw_data is None:
                        self.log.warning(
                            f"No raw data found for {bronze_dataset_with_year}, skipping"
                        )
                        continue

                self.log.info(f"Read raw data successfully for {bronze_dataset_with_year}")

                # Process the data
                geo_df = await self._process_data(raw_data, layer_type, year)

                if geo_df is None or geo_df.empty:
                    self.log.warning(f"No processed data for {silver_dataset_with_year}, skipping")
                    continue

                self.log.info(f"Processed {len(geo_df):,} features for {silver_dataset_with_year}")

                # Save processed data
                self._save_data(
                    geo_df, silver_dataset_with_year, self.config.bucket, stage="silver"
                )
                self.log.info(f"Saved processed data successfully for {silver_dataset_with_year}")

            except Exception as e:
                self.log.error(f"Error processing {layer_type} for year {year}: {e}")
                continue

    async def run(self, bronze_data: Optional[Any] = None) -> Optional[Dict[str, Any]]:
        """
        Execute the silver processing job for all FVM WFS data.

        This method orchestrates the processing of raw multi-year data from the bronze
        layer into structured GeoDataFrames. It processes Markblokke, Marker, and
        Smaabiotoper data for all available years and saves the results to Google Cloud Storage.

        Args:
            bronze_data: Optional in-memory data from bronze stage. If provided,
                        this data will be used instead of reading from storage.

        The processing workflow for each layer type and year:
        1. Read raw data from GCS or use in-memory data
        2. Process raw WFS data into GeoDataFrames with standardized column names
        3. Add year and layer type information to the processed data
        4. Validate geometries and apply any needed transformations
        5. Save processed data back to GCS with year information

        Returns:
            Optional[Dict[str, Any]]: Summary information about processed datasets
                                    for potential gold layer usage, or None if processing fails
        """
        self.log.info("Running FVM WFS silver job for all available data")
        async with AsyncTimer("FVM WFS Silver Job"):
            # Process Markblokke data (field blocks) 2005-2026
            await self._process_layer_type(
                "Markblokke",
                self.config.markblokke_years,
                self.config.bronze_dataset_markblokke,
                self.config.dataset_markblokke,
                bronze_data,
            )

            # Process Marker data (field markers) 2008-2025
            await self._process_layer_type(
                "Marker",
                self.config.marker_years,
                self.config.bronze_dataset_marker,
                self.config.dataset_marker,
                bronze_data,
            )

            # Process Smaabiotoper data (small biotopes) 2023-2025
            await self._process_layer_type(
                "Smaabiotoper",
                self.config.smaabiotoper_years,
                f"{self.config.bronze_dataset_marker}_smaabiotoper",
                self.config.dataset_smaabiotoper,
                bronze_data,
            )

            self.log.info("FVM WFS silver job completed for all available data")

            # Return summary information for potential gold layer usage
            return {
                "dataset": self.config.dataset,
                "markblokke_years": self.config.markblokke_years,
                "marker_years": self.config.marker_years,
                "smaabiotoper_years": self.config.smaabiotoper_years,
                "processed_at": pd.Timestamp.now().isoformat(),
                "status": "completed",
            }
