import logging
import geopandas as gpd
from src.sources.base import GeospatialSource
import os
import tempfile
import requests
import zipfile
from pathlib import Path
import shutil

logger = logging.getLogger(__name__)
# Set logging level to INFO to see debug messages
logging.basicConfig(level=logging.INFO)

class SoilTypeParser(GeospatialSource):
    """Parser for soil types from shapefile

    Downloads the soil type shapefile from landbrugsgeodata.fvm.dk and processes it into a GeoDataFrame.
    """

    source_id = "soil_types"
    SHAPEFILE_URL = "https://landbrugsgeodata.fvm.dk/Download/Jordbunds-%20og%20terraenforhold/Jordbundskort_2024.zip"
    TARGET_CRS = "EPSG:4326"  # The CRS we want the output to be in

    def __init__(self, config):
        super().__init__(config)
        # Use a consistent work directory name
        self.work_dir = os.path.join(tempfile.gettempdir(), 'soil_type_work')
        # Create work directory if it doesn't exist
        os.makedirs(self.work_dir, exist_ok=True)
        logger.info(f"Using work directory: {self.work_dir}")

    def _download_shapefile(self):
        """Download and extract the shapefile"""
        zip_path = os.path.join(self.work_dir, 'soil_types.zip')

        # Download the zip file
        logger.info("Downloading shapefile...")
        response = requests.get(self.SHAPEFILE_URL, stream=True)
        response.raise_for_status()

        with open(zip_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        # Extract the zip file
        logger.info("Extracting shapefile...")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(self.work_dir)

        # Find the shapefile directory
        shapefile_dir = None
        for root, dirs, files in os.walk(self.work_dir):
            if any(f.endswith('.shp') for f in files):
                shapefile_dir = root
                break

        if not shapefile_dir:
            raise Exception("Could not find shapefile in downloaded zip")

        return shapefile_dir

    def _cleanup(self):
        """Clean up temporary files"""
        try:
            shutil.rmtree(self.work_dir)
            logger.info("Cleaned up work directory")
        except Exception as e:
            logger.warning(f"Failed to clean up work directory: {str(e)}")

    async def fetch(self):
        """Fetch soil type data from shapefile

        Returns:
            GeoDataFrame: A GeoDataFrame containing soil type features with geometry and properties
                         or None if the fetch fails.
        """
        try:
            # Download and extract shapefile
            shapefile_dir = self._download_shapefile()

            # Find the shapefile
            shapefile = None
            for file in os.listdir(shapefile_dir):
                if file.endswith('.shp'):
                    shapefile = os.path.join(shapefile_dir, file)
                    break

            if not shapefile:
                raise Exception("Could not find .shp file in extracted directory")

            # Read shapefile into GeoDataFrame
            logger.info("Reading shapefile into GeoDataFrame...")
            gdf = gpd.read_file(shapefile)

            # Check and transform CRS if needed
            if gdf.crs is None:
                logger.warning("Shapefile has no CRS defined, assuming EPSG:4326")
                gdf.crs = self.TARGET_CRS
            elif gdf.crs != self.TARGET_CRS:
                logger.info(f"Transforming CRS from {gdf.crs} to {self.TARGET_CRS}")
                gdf = gdf.to_crs(self.TARGET_CRS)

            # Clean up temporary files
            self._cleanup()

            logger.info(f"Successfully loaded {len(gdf)} features")
            return gdf

        except Exception as e:
            logger.error(f"Failed to fetch soil types: {str(e)}")
            self._cleanup()
            return None
