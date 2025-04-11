import logging
import aiohttp
import xml.etree.ElementTree as ET
import geopandas as gpd
from shapely.geometry import Polygon
from src.sources.base import GeospatialSource
import asyncio
from urllib.parse import urlencode
import json
import os
import tempfile
import pandas as pd
import time
from pathlib import Path

logger = logging.getLogger(__name__)
# Set logging level to INFO to see debug messages
logging.basicConfig(level=logging.INFO)

class SoilTypeParser(GeospatialSource):
    """Parser for soil types from WFS"""

    source_id = "soil_types"

    def __init__(self, config):
        super().__init__(config)
        self.batch_size = config.get('batch_size', 10000)
        self.max_concurrent = 5
        self.request_timeout = 300
        self.max_retries = 3
        self.retry_delay = 5  # seconds

        # Use a consistent work directory name
        self.work_dir = os.path.join(tempfile.gettempdir(), 'soil_type_work')

        # Create work directory if it doesn't exist
        os.makedirs(self.work_dir, exist_ok=True)
        logger.info(f"Using work directory: {self.work_dir}")

        self.checkpoint_file = os.path.join(self.work_dir, 'checkpoint.json')
        self.namespaces = {
            'wfs': 'http://www.opengis.net/wfs/2.0',
            'gml': 'http://www.opengis.net/gml/3.2',
            'jord': 'http://www.fvm.dk/jordbunds_og_terraenforhold'
        }
        self.request_semaphore = asyncio.Semaphore(self.max_concurrent)

    def _save_checkpoint(self, start_index, batch_number, batch_files):
        """Save the current progress to a checkpoint file"""
        checkpoint = {
            'start_index': start_index,
            'batch_number': batch_number,
            'batch_files': batch_files
        }
        with open(self.checkpoint_file, 'w') as f:
            json.dump(checkpoint, f)
        logger.info(f"Saved checkpoint: batch {batch_number}, index {start_index}")

    def _load_checkpoint(self):
        """Load the last checkpoint if it exists and find existing batch files"""
        if not os.path.exists(self.checkpoint_file):
            # If no checkpoint exists, look for existing batch files
            batch_files = []
            start_index = 0
            batch_number = 1

            # Find all existing batch files
            for file in os.listdir(self.work_dir):
                if file.startswith('batch_') and file.endswith('.parquet'):
                    try:
                        batch_num = int(file.split('_')[1].split('.')[0])
                        batch_files.append(os.path.join(self.work_dir, file))
                        # Update start_index based on the number of features in this batch
                        gdf = gpd.read_parquet(os.path.join(self.work_dir, file))
                        start_index += len(gdf)
                        batch_number = max(batch_number, batch_num + 1)
                    except Exception as e:
                        logger.warning(f"Failed to process existing batch file {file}: {str(e)}")

            if batch_files:
                logger.info(f"Found {len(batch_files)} existing batch files, continuing from index {start_index}")
                return start_index, batch_number, batch_files

            return 0, 1, []

        try:
            with open(self.checkpoint_file, 'r') as f:
                checkpoint = json.load(f)

            # Verify that all batch files in the checkpoint still exist
            valid_batch_files = []
            for batch_file in checkpoint['batch_files']:
                if os.path.exists(batch_file):
                    valid_batch_files.append(batch_file)
                else:
                    logger.warning(f"Batch file from checkpoint not found: {batch_file}")

            if len(valid_batch_files) != len(checkpoint['batch_files']):
                logger.warning("Some batch files from checkpoint are missing, adjusting start index")
                # Recalculate start_index based on existing batch files
                start_index = 0
                for batch_file in valid_batch_files:
                    gdf = gpd.read_parquet(batch_file)
                    start_index += len(gdf)
            else:
                start_index = checkpoint['start_index']

            logger.info(f"Loaded checkpoint: batch {checkpoint['batch_number']}, index {start_index}")
            return start_index, checkpoint['batch_number'], valid_batch_files
        except Exception as e:
            logger.warning(f"Failed to load checkpoint: {str(e)}")
            return 0, 1, []

    def _save_batch(self, features, batch_number):
        """Save a batch of features to a GeoParquet file"""
        if not features:
            return None

        batch_file = os.path.join(self.work_dir, f'batch_{batch_number}.parquet')

        # Create GeoDataFrame from features
        gdf = gpd.GeoDataFrame.from_features(features)
        gdf = gdf.set_geometry('geometry')
        gdf.crs = 'EPSG:4326'

        # Save to GeoParquet
        gdf.to_parquet(batch_file)
        logger.info(f"Saved batch {batch_number} to {batch_file}")
        return batch_file

    async def _fetch_batch_with_retry(self, url, batch_number, start_index):
        """Fetch a batch with retry logic"""
        for attempt in range(self.max_retries):
            try:
                async with aiohttp.ClientSession() as session:
                    async with self.request_semaphore:
                        async with session.get(url, timeout=self.request_timeout) as response:
                            if response.status == 200:
                                text = await response.text()

                                if text.strip().startswith('<?xml'):
                                    logger.error("Received XML error response")
                                    logger.error(f"Error response: {text}")
                                    raise Exception("XML error response received")

                                data = json.loads(text)
                                if not isinstance(data, dict):
                                    raise Exception(f"Unexpected JSON response type: {type(data)}")

                                if 'features' not in data:
                                    raise Exception("No features found in response")

                                features = data['features']
                                if not features:
                                    logger.info(f"No more features found at index {start_index}")
                                    return None

                                logger.info(f"Found {len(features)} features in batch {batch_number} (indexes {start_index}-{start_index + len(features) - 1})")
                                return features

                            elif response.status == 404:
                                logger.info(f"No more features found at index {start_index} (404 response)")
                                return None
                            else:
                                error_text = await response.text()
                                raise Exception(f"HTTP {response.status}: {error_text}")

            except Exception as e:
                if attempt < self.max_retries - 1:
                    logger.warning(f"Attempt {attempt + 1} failed for batch {batch_number}: {str(e)}")
                    logger.info(f"Retrying in {self.retry_delay} seconds...")
                    await asyncio.sleep(self.retry_delay)
                else:
                    logger.error(f"All retry attempts failed for batch {batch_number}: {str(e)}")
                    raise

    async def fetch_soil_types(self):
        """Fetch soil types from the WFS service with error recovery

        Returns:
            GeoDataFrame: A GeoDataFrame containing soil type features with geometry and properties
                         or None if the fetch fails.
        """
        base_url = 'https://geodata.fvm.dk/geoserver/Jordbunds_og_terraenforhold/wfs'

        # Load checkpoint or start fresh
        start_index, batch_number, batch_files = self._load_checkpoint()

        try:
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
                    features = await self._fetch_batch_with_retry(url, batch_number, start_index)
                    if features is None:
                        break

                    # Save batch to file
                    batch_file = self._save_batch(features, batch_number)
                    if batch_file:
                        batch_files.append(batch_file)
                        # Update checkpoint after successful save
                        self._save_checkpoint(start_index + len(features), batch_number + 1, batch_files)

                    start_index += len(features)
                    batch_number += 1

                except Exception as e:
                    logger.error(f"Failed to process batch {batch_number}: {str(e)}")
                    # Don't raise the exception, allow the process to continue with the next batch
                    start_index += self.batch_size
                    batch_number += 1
                    continue

        except Exception as e:
            logger.error(f"Fatal error during fetch: {str(e)}")
            return None

        if not batch_files:
            logger.error("No features were fetched")
            return None

        logger.info(f"Total batches saved: {len(batch_files)}")

        # Read and merge all batch files
        logger.info("Merging batch files...")
        try:
            gdfs = [gpd.read_parquet(f) for f in batch_files]
            gdf = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True))

            # Clean up work directory
            for f in batch_files:
                try:
                    os.remove(f)
                except Exception as e:
                    logger.warning(f"Failed to remove batch file {f}: {str(e)}")

            try:
                os.remove(self.checkpoint_file)
                os.rmdir(self.work_dir)
            except Exception as e:
                logger.warning(f"Failed to clean up work directory: {str(e)}")

            logger.info(f"Final GeoDataFrame contains {len(gdf)} features")
            return gdf

        except Exception as e:
            logger.error(f"Failed to merge batch files: {str(e)}")
            return None

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

    async def fetch(self):
        """Required method from GeospatialSource. Fetches soil type data.

        Returns:
            GeoDataFrame: A GeoDataFrame containing soil type features with geometry and properties
                         or None if the fetch fails.
        """
        return await self.fetch_soil_types()
