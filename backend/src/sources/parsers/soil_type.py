import logging
import aiohttp
import xml.etree.ElementTree as ET
import geopandas as gpd
from shapely.geometry import Polygon
from src.sources.base import GeospatialSource
import asyncio
from urllib.parse import urlencode
import json

logger = logging.getLogger(__name__)
# Set logging level to INFO to see debug messages
logging.basicConfig(level=logging.INFO)

class SoilTypeParser(GeospatialSource):
    """Parser for soil types from WFS"""

    source_id = "soil_types"

    def __init__(self, config):
        super().__init__(config)
        self.batch_size = config.get('batch_size', 10000)  # Default to 10000 if not specified
        self.max_concurrent = 5
        self.request_timeout = 300

        self.namespaces = {
            'wfs': 'http://www.opengis.net/wfs/2.0',
            'gml': 'http://www.opengis.net/gml/3.2',
            'jord': 'http://www.fvm.dk/jordbunds_og_terraenforhold'
        }

        self.request_semaphore = asyncio.Semaphore(self.max_concurrent)

    async def fetch(self):
        """Required method from GeospatialSource. Fetches soil type data.

        Returns:
            GeoDataFrame: A GeoDataFrame containing soil type features with geometry and properties
                         or None if the fetch fails.
        """
        return await self.fetch_soil_types()

    async def fetch_soil_types(self):
        """Fetch soil types from the WFS service

        Returns:
            GeoDataFrame: A GeoDataFrame containing soil type features with geometry and properties
                         or None if the fetch fails.
        """
        base_url = 'https://geodata.fvm.dk/geoserver/Jordbunds_og_terraenforhold/wfs'
        all_features = []
        start_index = 0
        batch_number = 1

        while True:
            params = {
                'SERVICE': 'WFS',
                'REQUEST': 'GetFeature',
                'VERSION': '2.0.0',
                'TYPENAMES': 'Jordbunds_og_terraenforhold:Jordbundskort_2024',
                'SRSNAME': 'EPSG:4326',
                'count': str(self.batch_size),
                'startIndex': str(start_index),
                'outputFormat': 'application/json'
            }

            url = f"{base_url}?{urlencode(params)}"
            end_index = start_index + self.batch_size - 1
            logger.info(f"Fetching batch {batch_number} of features (indexes {start_index}-{end_index})")

            try:
                async with aiohttp.ClientSession() as session:
                    async with self.request_semaphore:
                        async with session.get(url, timeout=self.request_timeout) as response:
                            if response.status == 200:
                                try:
                                    text = await response.text()

                                    if text.strip().startswith('<?xml'):
                                        logger.error("Received XML error response")
                                        logger.error(f"Error response: {text}")
                                        return None

                                    try:
                                        data = json.loads(text)
                                        if not isinstance(data, dict):
                                            logger.error(f"Unexpected JSON response type: {type(data)}")
                                            return None

                                    if 'features' not in data:
                                        logger.error("No features found in response")
                                        return None

                                    features = data['features']
                                    if not features:
                                        # No more features to fetch
                                        break

                                    logger.info(f"Found {len(features)} features in batch {batch_number} (indexes {start_index}-{start_index + len(features) - 1})")
                                    all_features.extend(features)
                                    start_index += len(features)
                                    batch_number += 1

                                except json.JSONDecodeError as e:
                                    logger.error(f"Failed to parse JSON response: {str(e)}")
                                    logger.error(f"Response text: {text[:1000]}...")
                                    return None

                            except Exception as e:
                                logger.error(f"Error processing response: {str(e)}")
                                return None
                            else:
                                logger.error(f"Failed to fetch data: {response.status}")
                                error_text = await response.text()
                                logger.error(f"Error response: {error_text}")
                                return None
            except Exception as e:
                logger.error(f"Request failed: {str(e)}")
                return None

        if not all_features:
            logger.error("No features were fetched")
            return None

        logger.info(f"Total features fetched: {len(all_features)}")

        # Create GeoDataFrame from all features
        gdf = gpd.GeoDataFrame.from_features(all_features)
        if 'geometry' not in gdf.columns:
            logger.error("No geometry column in GeoDataFrame")
            return None

        gdf = gdf.set_geometry('geometry')
        gdf.crs = 'EPSG:4326'
        return gdf

    def _parse_features(self, root):
        """Parse features from XML root"""
        features = []
        for feature in root.findall('.//gml:featureMember', self.namespaces):
            geom_elem = feature.find('.//gml:Polygon', self.namespaces)
            if geom_elem is not None:
                geom = self._parse_geometry(geom_elem)
                if geom:
                    # Get soil type properties
                    soil_code = feature.find('.//jord:JB_kode', self.namespaces)
                    soil_type = feature.find('.//jord:Jordtype', self.namespaces)

                    properties = {
                        'soil_code': soil_code.text if soil_code is not None else None,
                        'soil_type': soil_type.text if soil_type is not None else None
                    }

                    features.append({
                        'type': 'Feature',
                        'geometry': geom.__geo_interface__,
                        'properties': properties
                    })
        return features

    def _parse_geometry(self, geom_elem):
        """Parse GML geometry into Shapely geometry"""
        try:
            coords = geom_elem.find('.//gml:posList', self.namespaces).text.split()
            coords = [(float(coords[i]), float(coords[i + 1])) for i in range(0, len(coords), 2)]
            poly = Polygon(coords)
            return poly if poly.is_valid else poly.buffer(0)
        except Exception as e:
            logger.error(f"Error parsing geometry: {str(e)}")
            return None
