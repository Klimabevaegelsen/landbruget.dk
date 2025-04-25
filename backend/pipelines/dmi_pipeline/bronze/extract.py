"""
DMI Climate Data Extraction Layer
Handles API communication and data retrieval from the Danish Meteorological Institute.
Converts raw API responses into geospatial dataframes with proper coordinate systems.
"""

import logging
import os
from datetime import datetime
from typing import Dict, Any
import aiohttp
from fastapi import HTTPException
import geopandas as gpd
from shapely.geometry import shape

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DMIConfig:
    """Configuration for DMI API access and data transformation settings"""
    def __init__(self):
        self.api_key = os.getenv('DMI_GOV_CLOUD_API_KEY')
        if not self.api_key:
            raise ValueError("DMI_GOV_CLOUD_API_KEY environment variable is required")

        self.base_url = "https://dmigw.govcloud.dk/v2/climateData"
        self.max_retries = int(os.getenv('MAX_RETRIES', 3))
        self.retry_delay = int(os.getenv('RETRY_DELAY', 5))
        # Define CRS constants
        self.SOURCE_CRS = "EPSG:25832"  # DMI's native CRS
        self.TARGET_CRS = "EPSG:4326"   # Required target CRS

class DMIApiClient:
    """Client for interacting with DMI's climate data API and processing geospatial data"""
    def __init__(self, config: DMIConfig):
        self.config = config
        self.headers = {
            "Accept": "application/geo+json",
            "X-Gravitee-Api-Key": self.config.api_key
        }

    async def _make_request(self, endpoint: str, params: Dict[str, Any] = None) -> Dict:
        """Make an authenticated request to the DMI API with error handling"""
        if params is None:
            params = {}

        url = f"{self.config.base_url}/{endpoint}"
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, headers=self.headers, params=params) as response:
                    if response.status == 429:
                        logger.warning("Rate limit exceeded")
                        raise HTTPException(status_code=429, detail="Rate limit exceeded")

                    text = await response.text()
                    if not text:
                        raise HTTPException(status_code=500, detail="Empty response from DMI API")

                    return await response.json()
            except Exception as e:
                logger.error(f"Error making request to DMI API: {str(e)}")
                raise

    async def fetch_grid_data(self, parameter_id: str, start_time: datetime, end_time: datetime) -> gpd.GeoDataFrame:
        """Fetch and process climate grid data into a geospatial dataframe"""
        params = {
            "parameterId": parameter_id,
            "limit": 1000,
            "datetime": f"{start_time.strftime('%Y-%m-%dT%H:%M:%SZ')}/{end_time.strftime('%Y-%m-%dT%H:%M:%SZ')}"
        }

        try:
            data = await self._make_request("collections/10kmGridValue/items", params)
            if not data or "features" not in data:
                return gpd.GeoDataFrame()

            # Process features into GeoDataFrame
            features = []
            for feature in data["features"]:
                properties = feature.get("properties", {})
                geometry = feature.get("geometry", {})

                if geometry:
                    # Convert GeoJSON geometry to Shapely geometry
                    shapely_geometry = shape(geometry)
                    features.append({
                        "geometry": shapely_geometry,
                        "value": properties.get("value"),
                        "parameter_id": properties.get("parameterId"),
                        "valid_time": properties.get("validTime"),
                        "created": properties.get("created"),
                        "geo_crs_source": self.config.SOURCE_CRS
                    })

            if not features:
                return gpd.GeoDataFrame()

            # Create GeoDataFrame with explicit geometry column
            gdf = gpd.GeoDataFrame(features, geometry="geometry")

            # Set initial CRS to match DMI's native CRS
            gdf.set_crs(self.config.SOURCE_CRS, inplace=True)

            # Log CRS information before conversion
            logger.info(f"Created GeoDataFrame with source CRS: {self.config.SOURCE_CRS}")

            # Convert to target CRS
            gdf = gdf.to_crs(self.config.TARGET_CRS)
            logger.info(f"Converted GeoDataFrame to target CRS: {self.config.TARGET_CRS}")

            return gdf

        except Exception as e:
            logger.error(f"Error fetching grid data: {str(e)}")
            return gpd.GeoDataFrame()