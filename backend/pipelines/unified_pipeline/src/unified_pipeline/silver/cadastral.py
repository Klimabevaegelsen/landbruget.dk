
from pydantic import ConfigDict
import logging

import geopandas as gpd
from unified_pipeline.common.geometry_validator import validate_and_transform_geometries
from unified_pipeline.common.base import BaseJobConfig, BaseSource
from unified_pipeline.util.gcs_util import GCSUtil
from unified_pipeline.common.geometry_validator import validate_and_transform_geometries
from dotenv import load_dotenv
import os

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
    
class CadastralSilver(BaseSource[CadastralSilverConfig]):
    """Cadastral Silver source."""
    
    def __init__(self, config: CadastralSilverConfig, gcs_util: GCSUtil) -> None:
        super().__init__(config, gcs_util)
        
    def _validate_and_transform(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Validate and transform the GeoDataFrame.

        This method validates the GeoDataFrame and transforms it into a valid format.

        Args:
            gdf (gpd.GeoDataFrame): The GeoDataFrame to validate and transform.

        Returns:
            gpd.GeoDataFrame: The validated and transformed GeoDataFrame.
        """
        return validate_and_transform_geometries(gdf, self.config.dataset)

    async def run(self):
        """
        Run the complete Cadastral silver layer processing job.

        This is the main entry point that orchestrates the entire process:
        1. Reads data from the bronze layer
        2. Processes XML data into a GeoDataFrame
        3. Creates a dissolved version of the GeoDataFrame
        4. Saves both the original and dissolved data to GCS

        Returns:
            None

        Raises:
            Exception: If there are issues at any step in the process.
        """
        self.log.info("Running Cadastral silver job")
        bronze_path = self._get_bronze_path(self.config.dataset, self.config.bucket)
        if bronze_path is None:
            self.log.error("Bronze data not found")
            return
        gdf = gpd.read_parquet(bronze_path)
        processed_data = self._validate_and_transform(gdf)
        self.log.info(processed_data)
        self._save_data(processed_data, self.config.dataset, self.config.bucket)
        
        self.log.info("Cadastral silver job completed successfully")