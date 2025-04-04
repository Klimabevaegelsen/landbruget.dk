import aiohttp
import pandas as pd
import geopandas as gpd
from shapely.geometry import box
from typing import Dict, Any, Optional, List
import logging
from datetime import datetime, timedelta
import os
from fastapi import HTTPException
from ...sources.base import GeospatialSource
import numpy as np

logger = logging.getLogger(__name__)

class DMIParser(GeospatialSource):
    """Parser for the DMI Climate Data API"""

    BASE_URL = "https://dmigw.govcloud.dk/v2/climateData"

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        # Try different possible environment variable names
        possible_env_vars = [
            'DMI_GOV_CLOUD_API_KEY',
            'dmi_govcloud_api_key',
            'dmi-govcloud-api-key'
        ]

        self.api_key = None
        for env_var in possible_env_vars:
            if os.getenv(env_var):
                self.api_key = os.getenv(env_var)
                logger.info(f"Using API key from environment variable: {env_var}")
                break

        if not self.api_key:
            raise ValueError(f"No DMI API key found. Please set one of these environment variables: {', '.join(possible_env_vars)}")

    @property
    def source_id(self) -> str:
        return "dmi_climate"

    async def _make_request(self, endpoint: str, params: Dict[str, Any] = None, headers: Dict[str, str] = None) -> Dict:
        """Make an authenticated request to the DMI API"""
        # Use header authentication (preferred method)
        if headers is None:
            headers = {
                "Accept": "application/geo+json",
                "X-Gravitee-Api-Key": self.api_key
            }

        # Initialize params if None
        if params is None:
            params = {}

        # Add api-key as query parameter as well (belt and braces)
        params['api-key'] = self.api_key

        async with aiohttp.ClientSession() as session:
            url = f"{self.BASE_URL}/{endpoint}"
            try:
                logger.info(f"Making request to: {url} with params: {params}")
                async with session.get(url, headers=headers, params=params) as response:
                    if response.status == 429:
                        logger.warning("Rate limit exceeded (500 requests per 5 seconds)")
                        raise HTTPException(status_code=429, detail="Rate limit exceeded")

                    text = await response.text()
                    if not text:
                        raise HTTPException(status_code=500, detail="Empty response from DMI API")

                    try:
                        return await response.json()
                    except Exception as e:
                        logger.error(f"Failed to parse JSON response: {text}")
                        raise HTTPException(status_code=500, detail=f"Invalid JSON response: {str(e)}")

            except aiohttp.ClientResponseError as e:
                logger.error(f"Error making request to DMI API: {str(e)}")
                if e.status == 404:
                    logger.error(f"Endpoint not found: {url}")
                raise HTTPException(status_code=e.status, detail=str(e))
            except Exception as e:
                logger.error(f"Unexpected error in DMI API request: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))

    async def get_grid_data(
        self,
        parameter_id: str,
        start_time: datetime = None,
        end_time: datetime = None,
        bbox: List[float] = None,  # [minx, miny, maxx, maxy] in EPSG:4326
        bbox_crs: str = "https://www.opengis.net/def/crs/OGC/1.3/CRS84"  # Updated to match API expectation
    ) -> gpd.GeoDataFrame:
        """
        Fetch 10x10km grid data for Denmark

        Args:
            parameter_id: The parameter ID to fetch data for (e.g. 'pot_evaporation_makkink')
            start_time: Start time for the data (optional)
            end_time: End time for the data (optional)
            bbox: Bounding box coordinates [minx, miny, maxx, maxy] in EPSG:4326 (optional)

        Returns:
            GeoDataFrame containing the grid data with geometry in EPSG:4326
        """
        params = {
            "parameterId": parameter_id,
            "limit": 1000
        }

        if start_time:
            if end_time:
                params["datetime"] = f"{start_time.strftime('%Y-%m-%dT%H:%M:%SZ')}/{end_time.strftime('%Y-%m-%dT%H:%M:%SZ')}"
            else:
                # If no end time specified, request from start time onwards
                params["datetime"] = f"{start_time.strftime('%Y-%m-%dT%H:%M:%SZ')}/.."

        if bbox:
            params["bbox"] = ",".join(map(str, bbox))
            params["bbox-crs"] = bbox_crs

        try:
            # Make request with detailed logging
            logger.info(f"Making DMI API request with parameters: {params}")
            data = await self._make_request("collections/10kmGridValue/items", params)

            if not data:
                logger.error("Empty response from DMI API")
                return gpd.GeoDataFrame()

            if "features" not in data:
                logger.error(f"Invalid response format from DMI API: {data}")
                return gpd.GeoDataFrame()

            # Extract grid values and metadata
            grid_data = []
            for feature in data.get("features", []):
                properties = feature.get("properties", {})
                geometry = feature.get("geometry", {})

                # Create grid cell geometry
                coords = geometry.get("coordinates", [])[0] if geometry else None
                if coords:
                    minx, miny = coords[0]
                    maxx, maxy = coords[2]
                    grid_cell = box(minx, miny, maxx, maxy)

                    grid_data.append({
                        "geometry": grid_cell,
                        "value": properties.get("value"),
                        "parameter_id": properties.get("parameterId"),
                        "valid_time": properties.get("validTime"),
                        "created": properties.get("created")
                    })

            # Create GeoDataFrame with original CRS
            gdf = gpd.GeoDataFrame(grid_data)
            if not gdf.empty:
                # Set the original CRS and transform to WGS84
                gdf.set_crs(epsg=25832, inplace=True)  # ETRS89 / UTM zone 32N
                gdf = gdf.to_crs(epsg=4326)  # Transform to WGS84
            return gdf

        except Exception as e:
            logger.error(f"Error fetching grid data: {str(e)}")
            return gpd.GeoDataFrame()

    async def get_stations(self) -> pd.DataFrame:
        """Fetch all climate stations"""
        try:
            data = await self._make_request("collections/station/items", {"limit": 1000})
            if not data or "features" not in data:
                return pd.DataFrame()
            return pd.DataFrame(data.get("features", []))
        except Exception as e:
            logger.error(f"Error fetching stations: {str(e)}")
            return pd.DataFrame()

    async def get_parameters(self) -> pd.DataFrame:
        """Fetch all available climate parameters"""
        try:
            data = await self._make_request("collections/parameter/items", {"limit": 1000})
            if not data or "features" not in data:
                return pd.DataFrame()
            return pd.DataFrame(data.get("features", []))
        except Exception as e:
            logger.error(f"Error fetching parameters: {str(e)}")
            return pd.DataFrame()

    async def get_observations(
        self,
        parameter_id: str,
        station_id: str = None,
        start_time: datetime = None,
        end_time: datetime = None,
        limit: int = 1000
    ) -> pd.DataFrame:
        """Fetch climate observations for a specific parameter"""
        params = {
            "parameterId": parameter_id,
            "limit": limit
        }

        if station_id:
            params["stationId"] = station_id
        if start_time:
            params["datetime_from"] = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        if end_time:
            params["datetime_to"] = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")

        try:
            data = await self._make_request("collections/observation/items", params)
            if not data or "features" not in data:
                return pd.DataFrame()
            return pd.DataFrame(data.get("features", []))
        except Exception as e:
            logger.error(f"Error fetching observations: {str(e)}")
            return pd.DataFrame()

    def _sanitize_for_json(self, df):
        """
        Helper function to make DataFrame values JSON compatible
        - Replace NaN/inf values with None
        - Convert datetimes to ISO format strings
        """
        # Create a copy to avoid modifying the original
        df_clean = df.copy()

        # Replace NaN/inf with None (null in JSON)
        for col in df_clean.select_dtypes(include=['float64']).columns:
            df_clean[col] = df_clean[col].apply(lambda x: None if pd.isna(x) or pd.api.types.is_float(x) and not np.isfinite(x) else x)

        # Convert datetimes to strings
        for col in df_clean.select_dtypes(include=['datetime64']).columns:
            df_clean[col] = df_clean[col].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

        return df_clean

    async def fetch_evaporation_data(self, start_time=None, end_time=None, bbox=None):
        """
        Fetch potential evaporation data (pot_evaporation_makkink)

        Args:
            start_time: Start time for the data (optional, defaults to 30 days ago)
            end_time: End time for the data (optional, defaults to now)
            bbox: Bounding box for the data [minx, miny, maxx, maxy] (optional, defaults to Denmark)

        Returns:
            GeoDataFrame with evaporation data
        """
        if start_time is None:
            start_time = datetime.utcnow() - timedelta(days=30)
        if end_time is None:
            end_time = datetime.utcnow()
        if bbox is None:
            bbox = [8.0, 54.5, 15.5, 57.8]  # Denmark bounding box

        try:
            # Fetch potential evaporation data
            logger.info(f"Fetching evaporation data from {start_time} to {end_time}")
            gdf = await self.get_grid_data(
                parameter_id="pot_evaporation_makkink",
                start_time=start_time,
                end_time=end_time,
                bbox=bbox
            )

            return gdf
        except Exception as e:
            logger.error(f"Error fetching evaporation data: {str(e)}")
            return gpd.GeoDataFrame()

    async def fetch_precipitation_data(self, start_time=None, end_time=None, bbox=None):
        """
        Fetch accumulated precipitation data (acc_precip)

        Args:
            start_time: Start time for the data (optional, defaults to 30 days ago)
            end_time: End time for the data (optional, defaults to now)
            bbox: Bounding box for the data [minx, miny, maxx, maxy] (optional, defaults to Denmark)

        Returns:
            GeoDataFrame with precipitation data
        """
        if start_time is None:
            start_time = datetime.utcnow() - timedelta(days=30)
        if end_time is None:
            end_time = datetime.utcnow()
        if bbox is None:
            bbox = [8.0, 54.5, 15.5, 57.8]  # Denmark bounding box

        try:
            # Fetch accumulated precipitation data
            logger.info(f"Fetching precipitation data from {start_time} to {end_time}")
            gdf = await self.get_grid_data(
                parameter_id="accumulated_precipitation",
                start_time=start_time,
                end_time=end_time,
                bbox=bbox
            )

            return gdf
        except Exception as e:
            logger.error(f"Error fetching precipitation data: {str(e)}")
            return gpd.GeoDataFrame()

    async def load_latest_data(self, parameter_id):
        """
        Load the latest data from GCS storage

        Args:
            parameter_id: Parameter ID to load (e.g., 'pot_evaporation_makkink', 'accumulated_precipitation')

        Returns:
            GeoDataFrame with the data
        """
        try:
            # Check if data exists in storage
            blob = self.bucket.blob(f'raw/{self.source_id}_{parameter_id}/current.parquet')
            if not blob.exists():
                logger.warning(f"No cached data found for {parameter_id}, fetching fresh data")
                if parameter_id == 'pot_evaporation_makkink':
                    return await self.fetch_evaporation_data()
                elif parameter_id == 'accumulated_precipitation':
                    return await self.fetch_precipitation_data()
                else:
                    logger.error(f"Unknown parameter: {parameter_id}")
                    return gpd.GeoDataFrame()

            # Download to temp file
            import tempfile
            with tempfile.NamedTemporaryFile(delete=False) as temp:
                blob.download_to_filename(temp.name)
                # Load as GeoDataFrame
                gdf = gpd.read_parquet(temp.name)
                os.unlink(temp.name)
                return gdf

        except Exception as e:
            logger.error(f"Error loading data for {parameter_id}: {str(e)}")
            return gpd.GeoDataFrame()

    async def save_data(self, gdf, parameter_id):
        """
        Save data to GCS storage

        Args:
            gdf: GeoDataFrame to save
            parameter_id: Parameter ID (used in the storage path)

        Returns:
            bool: True if successful, False otherwise
        """
        if gdf.empty:
            logger.warning(f"Empty GeoDataFrame for {parameter_id}, skipping upload")
            return False

        try:
            # Store with a parameter-specific dataset name
            await self.store(gdf, f"{self.source_id}_{parameter_id}")
            logger.info(f"Successfully stored data for {parameter_id}")
            return True
        except Exception as e:
            logger.error(f"Error storing data for {parameter_id}: {str(e)}")
            return False

    async def get_evaporation_data(self, refresh=False):
        """
        Get potential evaporation data (pot_evaporation_makkink)
        Will load from cache if available, or fetch fresh data if requested

        Args:
            refresh: If True, fetch fresh data regardless of cache

        Returns:
            GeoDataFrame with evaporation data
        """
        if refresh:
            gdf = await self.fetch_evaporation_data()
            await self.save_data(gdf, 'pot_evaporation_makkink')
            return gdf
        else:
            gdf = await self.load_latest_data('pot_evaporation_makkink')
            if gdf.empty:
                gdf = await self.fetch_evaporation_data()
                await self.save_data(gdf, 'pot_evaporation_makkink')
            return gdf

    async def get_precipitation_data(self, refresh=False):
        """
        Get accumulated precipitation data (acc_precip)
        Will load from cache if available, or fetch fresh data if requested

        Args:
            refresh: If True, fetch fresh data regardless of cache

        Returns:
            GeoDataFrame with precipitation data
        """
        if refresh:
            gdf = await self.fetch_precipitation_data()
            await self.save_data(gdf, 'accumulated_precipitation')
            return gdf
        else:
            gdf = await self.load_latest_data('accumulated_precipitation')
            if gdf.empty:
                gdf = await self.fetch_precipitation_data()
                await self.save_data(gdf, 'accumulated_precipitation')
            return gdf

    async def fetch(self) -> gpd.GeoDataFrame:
        """Fetch climate data for potential evaporation"""
        # Get current time in UTC
        end_time = datetime.utcnow()
        start_time = datetime(2018, 2, 12)  # Starting from a known date with data

        try:
            # Fetch potential evaporation data
            logger.info(f"Fetching DMI climate data from {start_time} to {end_time}")
            gdf = await self.get_grid_data(
                parameter_id="pot_evaporation_makkink",
                start_time=start_time,
                end_time=end_time,
                bbox=[8.0, 54.5, 15.5, 57.8]  # Denmark bounding box
            )

            if gdf.empty:
                logger.warning("No grid data returned from DMI API")
                return gpd.GeoDataFrame()

            # Replace NaN values with None for JSON compatibility
            gdf = self._sanitize_for_json(gdf)

            # Add GeoJSON representation of the geometry for easier serialization
            gdf['geojson'] = gdf.geometry.apply(lambda g: g.__geo_interface__ if g else None)

            return gdf
        except Exception as e:
            logger.error(f"Error in fetch: {str(e)}")
            return gpd.GeoDataFrame()

    async def sync(self) -> Optional[int]:
        """Sync climate data"""
        try:
            df = await self.fetch()
            if df.empty:
                logger.warning("No data to store")
                return None

            if await self.store(df):
                return len(df)
            return None
        except Exception as e:
            logger.error(f"Sync failed: {str(e)}")
            return None

    async def store(self, df: pd.DataFrame, dataset: str = None) -> bool:
        """Store raw data in GCS"""
        try:
            if df.empty:
                logger.warning(f"Empty DataFrame for {self.source_id}, skipping upload")
                return False

            # Save as geoparquet if it's a GeoDataFrame
            with self.get_temp_file() as temp:
                if isinstance(df, gpd.GeoDataFrame):
                    logger.info(f"Storing {self.source_id} as geoparquet")
                    # Ensure datetime columns are properly formatted
                    for col in df.select_dtypes(include=['datetime64']).columns:
                        df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                    # Save with geometry preserved
                    df.to_parquet(temp.name, index=False)
                else:
                    # For regular DataFrames, convert datetime columns
                    if 'valid_time' in df.columns:
                        df['valid_time'] = pd.to_datetime(df['valid_time']).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                    if 'created' in df.columns:
                        df['created'] = pd.to_datetime(df['created']).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                    df.to_parquet(temp.name, index=False)

                # Upload to GCS
                self.bucket.blob(f'raw/{dataset or self.source_id}/current.parquet').upload_from_filename(temp.name)
                logger.info(f"Successfully stored {self.source_id} data in GCS bucket")

            return True
        except Exception as e:
            logger.error(f"Error storing data for {self.source_id}: {str(e)}")
            return False
