"""
Bronze layer data ingestion for Soil Types data.

This module handles the extraction of soil types data from a WFS (Web Feature Service) endpoint.
It fetches raw data from the Danish Environmental Portal's soil types layer and saves it to
Google Cloud Storage for further processing in the silver layer.

The module contains:
- SoilTypesBronzeConfig: Configuration class for the data source
- SoilTypesBronze: Implementation class for fetching and processing data

The data is fetched from the WFS endpoint using geopandas and saved as GeoParquet format
for efficient storage and processing.
"""

import asyncio
from typing import Optional

import geopandas as gpd
from pydantic import ConfigDict

from unified_pipeline.common.base import BaseJobConfig, BaseSource
from unified_pipeline.util.gcs_util import GCSUtil
from unified_pipeline.util.timing import AsyncTimer


class SoilTypesBronzeConfig(BaseJobConfig):
    """
    Configuration for the Soil Types Bronze source.

    This class defines all configuration parameters needed for fetching soil types
    data from the Danish Environmental Portal WFS endpoint. It includes the WFS URL,
    layer name, dataset configuration, and storage settings.

    Attributes:
        name (str): Human-readable name of the data source
        type (str): Type of the data source (wfs)
        description (str): Brief description of the data
        wfs_url (str): URL for the WFS endpoint
        layer_name (str): Name of the soil types layer
        dataset (str): Name of the dataset in storage
        frequency (str): How often the data is updated
        bucket (str): GCS bucket name for raw data storage
        crs (str): Coordinate reference system for the data
    """

    name: str = "Danish Soil Types"
    type: str = "wfs"
    description: str = "Soil types data from Danish Environmental Portal"
    wfs_url: str = "https://arld-extgeo.miljoeportal.dk/geoserver/wfs"
    layer_name: str = "landbrugsdrift:DJF_FGJOR"
    dataset: str = "soil_types"
    frequency: str = "monthly"
    bucket: str = "landbrugsdata-raw-data"
    crs: str = "EPSG:4326"  # WGS84 coordinate system (preferred per README)

    model_config = ConfigDict(frozen=True)


class SoilTypesBronze(BaseSource[SoilTypesBronzeConfig]):
    """
    Bronze layer processing for soil types data.

    This class is responsible for fetching raw soil types data from the WFS endpoint,
    processing it into a standardized format, and storing it in Google Cloud Storage
    for further processing in the silver layer.

    The class handles WFS requests using geopandas and implements proper error handling
    and logging for robustness.

    Processing flow:
    1. Connect to the WFS endpoint
    2. Fetch the soil types layer data
    3. Process and validate the data
    4. Save to Google Cloud Storage as GeoParquet
    """

    def __init__(self, config: SoilTypesBronzeConfig, gcs_util: GCSUtil):
        """
        Initialize the SoilTypesBronze source.

        Args:
            config (SoilTypesBronzeConfig): Configuration for the data source
            gcs_util (GCSUtil): Utility for Google Cloud Storage operations
        """
        super().__init__(config, gcs_util)

    async def _fetch_soil_types_data(self) -> Optional[gpd.GeoDataFrame]:
        """
        Fetch soil types data from the WFS endpoint.

        This method connects to the WFS endpoint and retrieves the soil types layer
        data using geopandas. It handles potential connection issues and data
        validation.

        Returns:
            Optional[gpd.GeoDataFrame]: GeoDataFrame containing the soil types data,
                                       or None if the fetch fails

        Raises:
            Exception: If the WFS request fails or returns invalid data
        """
        try:
            self.log.info(f"Fetching soil types data from WFS: {self.config.wfs_url}")

            # Construct WFS URL with parameters
            wfs_params = {
                "service": "WFS",
                "version": "2.0.0",
                "request": "GetFeature",
                "typeName": self.config.layer_name,
                "outputFormat": "application/json",
            }

            # Build the complete URL
            param_string = "&".join([f"{k}={v}" for k, v in wfs_params.items()])
            full_url = f"{self.config.wfs_url}?{param_string}"

            async with AsyncTimer("Fetch soil types data from WFS"):
                # Use geopandas to read from WFS
                # Note: geopandas.read_file is not async, so we run it in a thread pool
                loop = asyncio.get_event_loop()
                gdf = await loop.run_in_executor(None, lambda: gpd.read_file(full_url))

                if gdf is None or gdf.empty:
                    self.log.warning("No data returned from WFS endpoint")
                    return None

                self.log.info(f"Successfully fetched {len(gdf):,} soil type features")

                # Ensure the CRS is set correctly
                if gdf.crs is None:
                    self.log.info(f"Setting CRS to {self.config.crs}")
                    gdf = gdf.set_crs(self.config.crs)
                elif str(gdf.crs) != self.config.crs:
                    self.log.info(f"Reprojecting from {gdf.crs} to {self.config.crs}")
                    gdf = gdf.to_crs(self.config.crs)

                # Add metadata columns
                gdf["source"] = self.config.name
                gdf["created_at"] = gpd.pd.Timestamp.now()
                gdf["updated_at"] = gpd.pd.Timestamp.now()

                return gdf

        except Exception as e:
            self.log.error(f"Error fetching soil types data: {str(e)}")
            raise Exception(f"Failed to fetch soil types data from WFS: {str(e)}")

    async def run(self) -> None:
        """
        Run the soil types data processing pipeline.

        This method orchestrates the entire process of fetching soil types data
        from the WFS endpoint and saving it to Google Cloud Storage. It handles
        the complete workflow from data retrieval to storage.

        Returns:
            None

        Raises:
            Exception: If any step in the pipeline fails
        """
        try:
            self.log.info(f"Starting {self.config.name} bronze layer processing")

            # Fetch soil types data
            soil_types_gdf = await self._fetch_soil_types_data()

            if soil_types_gdf is None or soil_types_gdf.empty:
                self.log.warning("No soil types data to process")
                return

            # Log data summary
            self.log.info(f"Processing {len(soil_types_gdf):,} soil type features")
            self.log.info(f"Data columns: {list(soil_types_gdf.columns)}")
            self.log.info(
                f"Geometry type: {soil_types_gdf.geom_type.iloc[0] if len(soil_types_gdf) > 0 else 'Unknown'}"
            )

            # Save to GCS
            self._save_data(
                df=soil_types_gdf,
                dataset=self.config.dataset,
                bucket_name=self.config.bucket,
                stage="bronze",
            )

            self.log.info(f"Successfully completed {self.config.name} bronze layer processing")

        except Exception as e:
            self.log.error(f"Error in soil types bronze processing: {str(e)}")
            raise
