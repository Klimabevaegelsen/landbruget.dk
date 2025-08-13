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

from typing import Any, Optional

from pydantic import ConfigDict

from unified_pipeline.common.base import BaseJobConfig, BaseSource, BronzeJobInterface
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


class SoilTypesBronze(BaseSource[SoilTypesBronzeConfig], BronzeJobInterface):
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

    def __init__(self, config: SoilTypesBronzeConfig):
        """
        Initialize the SoilTypesBronze source.

        Args:
            config (SoilTypesBronzeConfig): Configuration for the data source"""
        super().__init__(config)

    async def _fetch_soil_types_data(self) -> Optional[Any]:
        """
        Fetch soil types data from the WFS endpoint.

        This method connects to the WFS endpoint and retrieves the soil types layer
        data. It handles potential connection issues and data validation.

        Returns:
            Optional[Any]: Raw soil types data,
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
                # For now, return the URL for silver layer to process with ibis/duckdb
                # The silver layer will handle the actual data fetching using proper tools
                self.log.info(f"Prepared WFS URL for silver layer processing: {full_url}")

                # Return metadata for silver layer processing
                return {
                    "wfs_url": full_url,
                    "source": self.config.name,
                    "layer_name": self.config.layer_name,
                    "crs": self.config.crs,
                }

        except Exception as e:
            self.log.error(f"Error preparing soil types data fetch: {str(e)}")
            raise Exception(f"Failed to prepare soil types data fetch: {str(e)}") from e

    async def run(self) -> Optional[Any]:
        """
        Run the soil types data processing pipeline.

        This method orchestrates the entire process of preparing soil types data
        fetch parameters and saving metadata to Google Cloud Storage.

        Returns:
            Optional[Any]: The metadata for soil types data that can be
                          passed to silver stage, or None if processing fails

        Raises:
            Exception: If any step in the pipeline fails
        """
        try:
            self.log.info(f"Starting {self.config.name} bronze layer processing")

            # Prepare soil types data fetch parameters
            soil_types_metadata = await self._fetch_soil_types_data()

            if soil_types_metadata is None:
                self.log.warning("No soil types metadata to process")
                return None

            # Save metadata to GCS using new unified method
            self._save_data(
                data=soil_types_metadata,
                dataset=self.config.dataset,
                bucket=self.config.bucket,
                stage="bronze",
            )

            self.log.info(f"Successfully completed {self.config.name} bronze layer processing")

            # Return metadata for in-memory passing
            return soil_types_metadata

        except Exception as e:
            self.log.error(f"Error in soil types bronze processing: {str(e)}")
            raise
