import logging
import os
from typing import Any, Optional

import geopandas as gpd
import pandas as pd
from dotenv import load_dotenv
from pydantic import ConfigDict

from unified_pipeline.common.base import BaseJobConfig, BaseSource, SilverJobInterface
from unified_pipeline.common.geometry_validator import validate_and_transform_geometries
from unified_pipeline.util.gcs_util import GCSUtil

logger = logging.getLogger(__name__)


class CadastralSilverConfig(BaseJobConfig):
    """Configuration for the Cadastral Silver source."""

    name: str = "Danish Cadastral"
    dataset: str = "cadastral"
    type: str = "wfs"
    description: str = "Cadastral parcels from WFS"
    frequency: str = "weekly"
    bucket: str = os.getenv("GCS_BUCKET")

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)
    load_dotenv()
    save_local: bool = os.getenv("SAVE_LOCAL", False)


class CadastralSilver(BaseSource[CadastralSilverConfig], SilverJobInterface):
    """Cadastral Silver source."""

    def __init__(self, config: CadastralSilverConfig, gcs_util: GCSUtil) -> None:
        super().__init__(config, gcs_util)

    def _validate_and_transform(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Validate and transform the GeoDataFrame.

        This method validates the GeoDataFrame and transforms it into a valid format.
        Ensures the output is always in EPSG:4326, not OGC:CRS84.

        Args:
            gdf (gpd.GeoDataFrame): The GeoDataFrame to validate and transform.

        Returns:
            gpd.GeoDataFrame: The validated and transformed GeoDataFrame in EPSG:4326.
        """
        # First apply the standard validation and transformation
        processed_gdf = validate_and_transform_geometries(gdf, self.config.dataset)

        # Ensure the CRS is exactly EPSG:4326, not OGC:CRS84 or other equivalent forms
        if not processed_gdf.crs or processed_gdf.crs.to_epsg() != 4326:
            # If it's not EPSG:4326 equivalent, transform it
            logger.info(
                f"{self.config.dataset}: Converting CRS from {processed_gdf.crs} to EPSG:4326"
            )
            processed_gdf = processed_gdf.to_crs("EPSG:4326")

        # Final verification
        if not processed_gdf.crs or processed_gdf.crs.to_epsg() != 4326:
            logger.warning(
                f"{self.config.dataset}: Final CRS is not EPSG:4326: {processed_gdf.crs}"
            )
        else:
            logger.info(
                f"{self.config.dataset}: ✅ Final CRS confirmed as EPSG:4326 (code: {processed_gdf.crs.to_epsg()})"
            )

        return processed_gdf

    def _create_dissolved_gdf(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Create a dissolved version of the GeoDataFrame by dissolving all geometries.

        Args:
            gdf (gpd.GeoDataFrame): The input GeoDataFrame

        Returns:
            gpd.GeoDataFrame: A dissolved GeoDataFrame with all geometries merged
        """
        try:
            from shapely.ops import unary_union

            # Dissolve all geometries into one
            dissolved_geometry = unary_union(gdf.geometry.values)

            # Create a new GeoDataFrame with the dissolved geometry
            dissolved_gdf = gpd.GeoDataFrame(
                {
                    "geometry": [dissolved_geometry],
                    "feature_count": [len(gdf)],
                    "total_area": [gdf.geometry.area.sum()],
                    "dissolved_at": [pd.Timestamp.now()],
                },
                crs=gdf.crs,
            )

            logger.info(f"Dissolved {len(gdf):,} features into 1 geometry")
            return dissolved_gdf

        except Exception as e:
            logger.error(f"Error creating dissolved GeoDataFrame: {str(e)}")
            # Return original data if dissolve fails
            return gdf

    async def run(self, bronze_data: Optional[Any] = None) -> Optional[gpd.GeoDataFrame]:
        """
        Run the complete Cadastral silver layer processing job.

        This is the main entry point that orchestrates the entire process:
        1. Reads data from the bronze layer (either in-memory or from storage)
        2. Validates and transforms the data
        3. Creates a dissolved version of the GeoDataFrame
        4. Saves both the original and dissolved data to GCS

        Args:
            bronze_data: Optional in-memory data from bronze stage. If provided,
                        this data will be used instead of reading from storage.

        Returns:
            Optional[gpd.GeoDataFrame]: Processed cadastral data for gold layer,
                                       or None if processing fails.

        Raises:
            Exception: If there are issues at any step in the process.
        """
        self.log.info("Running Cadastral silver job")

        # Read data with support for in-memory passing
        if bronze_data is not None:
            self.log.info("Using bronze data from memory (in-memory data passing)")
            if isinstance(bronze_data, gpd.GeoDataFrame):
                raw_data = bronze_data
            else:
                self.log.error(f"Expected GeoDataFrame from bronze stage, got {type(bronze_data)}")
                return None
        else:
            # Fallback to reading from storage
            self.log.info("Reading bronze data from storage (fallback)")
            raw_data = self._read_bronze_data_from_storage(self.config.dataset, self.config.bucket)
            if raw_data is None:
                self.log.error("Failed to read raw data from storage")
                return None

        if raw_data is None or raw_data.empty:
            self.log.warning("No data found in bronze layer")
            return None

        self.log.info(f"Loaded {len(raw_data):,} records from bronze layer")

        # Validate and transform the data
        processed_gdf = self._validate_and_transform(raw_data)

        if processed_gdf is None or processed_gdf.empty:
            self.log.warning("No valid geometries found after processing")
            return None

        # Create dissolved version
        dissolved_gdf = self._create_dissolved_gdf(processed_gdf)

        # Save both versions using new unified method
        self._save_data(processed_gdf, self.config.dataset, self.config.bucket, "silver")
        self._save_data(
            dissolved_gdf, f"{self.config.dataset}_dissolved", self.config.bucket, "silver"
        )

        self.log.info("Cadastral silver job completed successfully")

        # Return processed data for gold layer
        return processed_gdf
