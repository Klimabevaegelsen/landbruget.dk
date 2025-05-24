import asyncio
import xml.etree.ElementTree as ET
from asyncio import Semaphore

import aiohttp
from pydantic import ConfigDict
from dotenv import load_dotenv
from unified_pipeline.common.base import BaseJobConfig, BaseSource
from unified_pipeline.util.gcs_util import GCSUtil
import os
import logging
import time
from datetime import datetime
from shapely.geometry import Polygon, MultiPolygon
from shapely import wkt
import geopandas as gpd
import pandas as pd

logger = logging.getLogger(__name__)

def clean_value(value):
    """Clean string values"""
    if not isinstance(value, str):
        return value
    value = value.strip()
    return value if value else None

class CadastralBronzeConfig(BaseJobConfig):
    """Configuration for the Cadastral Bronze source."""
    name: str = "Danish Cadastral"
    dataset: str = "cadastral"
    type: str = "wfs"
    description: str = "Cadastral parcels from WFS"
    frequency: str = "weekly"
    bucket: str = os.getenv("GCS_BUCKET")

    batch_size: int = 10000
    max_concurrent: int = 5
    request_timeout: int = 300
    storage_batch_size: int = 5000
    request_timeout_config: aiohttp.ClientTimeout = aiohttp.ClientTimeout(
        total=request_timeout, connect=60, sock_read=300
    )
    headers: dict[str, str] = {"User-Agent": "Mozilla/5.0 QGIS/33603/macOS 15.1"}
    request_semaphore: Semaphore = Semaphore(max_concurrent)
    type: str = "wfs"
    url: str = "https://wfs.datafordeler.dk/MATRIKLEN2/MatGaeldendeOgForeloebigWFS/1.0.0/WFS"
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)
    load_dotenv()
    save_local: bool = os.getenv("SAVE_LOCAL", False)
    
class CadastralBronze(BaseSource[CadastralBronzeConfig]):
    
    def __init__(self, config: CadastralBronzeConfig, gcs_util: GCSUtil) -> None:
        super().__init__(config, gcs_util)
        self.last_request_time = {}
        self.requests_per_second = int(os.getenv('CADASTRAL_REQUESTS_PER_SECOND', '2'))

        self.field_mapping = {
            'BFEnummer': ('bfe_number', int),
            'forretningshaendelse': ('business_event', str),
            'forretningsproces': ('business_process', str),
            'senesteSagLokalId': ('latest_case_id', str),
            'id_lokalId': ('id_local', str),
            'id_namespace': ('id_namespace', str),
            'registreringFra': ('registration_from', lambda x: datetime.fromisoformat(x.replace('Z', '+00:00'))),
            'virkningFra': ('effect_from', lambda x: datetime.fromisoformat(x.replace('Z', '+00:00'))),
            'virkningsaktoer': ('authority', str),
            'arbejderbolig': ('is_worker_housing', lambda x: x.lower() == 'true'),
            'erFaelleslod': ('is_common_lot', lambda x: x.lower() == 'true'),
            'hovedejendomOpdeltIEjerlejligheder': ('has_owner_apartments', lambda x: x.lower() == 'true'),
            'udskiltVej': ('is_separated_road', lambda x: x.lower() == 'true'),
            'landbrugsnotering': ('agricultural_notation', str)
        }
        self.page_size = self.config.batch_size
        self.namespaces = {
            'wfs': 'http://www.opengis.net/wfs/2.0',
            'mat': 'http://data.gov.dk/schemas/matrikel/1',
            'gml': 'http://www.opengis.net/gml/3.2'
        }
        # Map credentials for increasingly varied sources
        user_env = os.getenv("DATAFORDELER_USERNAME") or os.getenv("WFS_USERNAME")
        pass_env = os.getenv("DATAFORDELER_PASSWORD") or os.getenv("WFS_PASSWORD")
        if not user_env or not pass_env:
            raise ValueError(
                "Missing credentials: set DATAFORDELER_USERNAME/PASSWORD or WFS_USERNAME/PASSWORD"
            )
        self.username = user_env
        self.password = pass_env
        self.total_timeout_config = aiohttp.ClientTimeout(
            total=self.config.request_timeout,
            connect=60,
            sock_read=self.config.request_timeout
        )

    def _get_base_params(self):
        """Get base WFS request parameters without pagination"""
        return {
            'username': self.username,
            'password': self.password,
            'SERVICE': 'WFS',
            'REQUEST': 'GetFeature',
            'VERSION': '2.0.0',
            'TYPENAMES': 'mat:SamletFastEjendom_Gaeldende',
            'SRSNAME': 'EPSG:25832'
        }
    
    
    def _get_params(self, start_index=0):
        """Get WFS request parameters with pagination"""
        params = self._get_base_params()
        params.update({
            'startIndex': str(start_index),
            'count': str(self.page_size)
        })
        return params

    def _parse_geometry(self, geom_elem):
        """Parse GML geometry to WKT"""
        try:
            pos_lists = geom_elem.findall('.//gml:posList', self.namespaces)
            if not pos_lists:
                return None

            polygons = []
            for pos_list in pos_lists:
                if not pos_list.text:
                    continue

                coords = [float(x) for x in pos_list.text.strip().split()]
                # Keep the original 3D coordinate handling - take x,y and skip z
                pairs = [(coords[i], coords[i+1]) 
                        for i in range(0, len(coords), 3)]

                if len(pairs) < 4:
                    logger.warning(f"Not enough coordinate pairs ({len(pairs)}) to form a polygon")
                    continue

                try:
                    # Check if the polygon is closed (first point equals last point)
                    if pairs[0] != pairs[-1]:
                        pairs.append(pairs[0])  # Close the polygon
                    
                    polygon = Polygon(pairs)
                    if polygon.is_valid:
                        polygons.append(polygon)
                    else:
                        # Try to fix invalid polygon
                        from shapely.ops import make_valid
                        fixed_polygon = make_valid(polygon)
                        if fixed_polygon.geom_type in ('Polygon', 'MultiPolygon'):
                            polygons.append(fixed_polygon)
                        else:
                            logger.warning(f"Could not create valid polygon, got {fixed_polygon.geom_type}")
                except Exception as e:
                    logger.warning(f"Error creating polygon: {str(e)}")
                    continue

            if not polygons:
                return None

            if len(polygons) == 1:
                final_geom = polygons[0]
            else:
                try:
                    final_geom = MultiPolygon(polygons)
                except Exception as e:
                    logger.warning(f"Error creating MultiPolygon: {str(e)}, falling back to first valid polygon")
                    final_geom = polygons[0]

            return wkt.dumps(final_geom)

        except Exception as e:
            logger.error(f"Error parsing geometry: {str(e)}")
            return None

    def _parse_feature(self, feature_elem):
        """Parse a single feature"""
        try:
            feature = {}
            
            # Add validation of the feature element
            if feature_elem is None:
                logger.warning("Received None feature element")
                return None
            
            # Parse all mapped fields
            for xml_field, (db_field, converter) in self.field_mapping.items():
                elem = feature_elem.find(f'.//mat:{xml_field}', self.namespaces)
                if elem is not None and elem.text:
                    try:
                        value = clean_value(elem.text)
                        if value is not None:
                            feature[db_field] = converter(value)
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Error converting field {xml_field}: {str(e)}")
                        continue

            # Parse geometry
            geom_elem = feature_elem.find('.//mat:geometri/gml:MultiSurface', self.namespaces)
            if geom_elem is not None:
                geometry_wkt = self._parse_geometry(geom_elem)
                if geometry_wkt:
                    feature['geometry'] = geometry_wkt
                else:
                    logger.warning("Failed to parse geometry for feature")

            # Add validation of required fields
            if not feature.get('bfe_number'):
                logger.warning("Missing required field: bfe_number")
            if not feature.get('geometry'):
                logger.warning("Missing required field: geometry")

            return feature if feature.get('bfe_number') and feature.get('geometry') else None

        except Exception as e:
            logger.error(f"Error parsing feature: {str(e)}")
            return None

    async def _wait_for_rate_limit(self):
        """Ensure we don't exceed requests_per_second"""
        worker_id = id(asyncio.current_task())
        if worker_id in self.last_request_time:
            elapsed = time.time() - self.last_request_time[worker_id]
            if elapsed < 1.0 / self.requests_per_second:
                await asyncio.sleep(1.0 / self.requests_per_second - elapsed)
        self.last_request_time[worker_id] = time.time()

    async def _fetch_chunk(self, session, start_index, timeout=None):
        """Fetch a chunk of features with rate limiting and retries"""
        async with self.config.request_semaphore:
            await self._wait_for_rate_limit()
            
            params = self._get_params(start_index)
            
            try:
                self.log.info(f"Fetching chunk at index {start_index}")
                async with session.get(
                    self.config.url, 
                    params=params,
                    timeout=timeout or self.config.request_timeout_config
                ) as response:
                    if response.status == 429:  # Too Many Requests
                        retry_after = int(response.headers.get('Retry-After', 5))
                        self.log.warning(f"Rate limited, waiting {retry_after} seconds")
                        await asyncio.sleep(retry_after)
                        raise ClientError("Rate limited")
                    
                    response.raise_for_status()
                    content = await response.text()
                    root = ET.fromstring(content)
                    
                    # Add validation of returned features count
                    number_returned = root.get('numberReturned', '0')
                    self.log.info(f"WFS reports {number_returned} features returned in this chunk")
                    
                    features = []
                    feature_elements = root.findall('.//mat:SamletFastEjendom_Gaeldende', self.namespaces)
                    self.log.info(f"Found {len(feature_elements)} feature elements in XML")
                    
                    for feature_elem in feature_elements:
                        feature = self._parse_feature(feature_elem)
                        if feature:
                            features.append(feature)
                    
                    valid_count = len(features)
                    self.log.info(f"Chunk {start_index}: parsed {valid_count} valid features out of {len(feature_elements)} elements")
                    
                    # Validate that we're getting reasonable numbers
                    if valid_count == 0 and len(feature_elements) > 0:
                        self.log.warning(f"No valid features parsed from {len(feature_elements)} elements - possible parsing issue")
                    elif valid_count < len(feature_elements) * 0.5:  # If we're losing more than 50% of features
                        self.log.warning(f"Low feature parsing success rate: {valid_count}/{len(feature_elements)}")
                    
                    return features
                    
            except Exception as e:
                self.log.error(f"Error fetching chunk at index {start_index}: {str(e)}")
                raise

        
    async def _parse_features(self):
        try:
            async with aiohttp.ClientSession(timeout=self.total_timeout_config) as session:
                total_features = await self._get_total_count(session)
                self.log.info(f"Found {total_features:,} total features")
                
                features_batch = []
                total_processed = 0
                failed_chunks = []
                
                for start_index in range(0, total_features, self.page_size):
                    try:
                        chunk = await self._fetch_chunk(session, start_index)
                        if chunk:
                            features_batch.extend(chunk)
                            total_processed += len(chunk)
                            
                            # Log progress every 10,000 features
                            if total_processed % 10000 == 0:
                                logger.info(f"Progress: {total_processed:,}/{total_features:,} features ({(total_processed/total_features)*100:.1f}%)")
    
                                
                    except Exception as e:
                        logger.error(f"Error processing batch at {start_index}: {str(e)}")
                        failed_chunks.append(start_index)
                        continue
                
                if failed_chunks:
                    logger.error(f"Failed to process chunks starting at indices: {failed_chunks}")
                df = pd.DataFrame([{k:v for k,v in f.items() if k != 'geometry'} for f in features_batch])
                geometries = [wkt.loads(f['geometry']) for f in features_batch]
                gdf = gpd.GeoDataFrame(df, geometry=geometries, crs="EPSG:25832")
                logger.info(f"Sync completed. Total processed: {total_processed:,} features")
                return total_processed, gdf
                
        except Exception as e:
            self.is_sync_complete = False
            logger.error(f"Error in sync: {str(e)}")
            raise

    async def _get_total_count(self, session):
        """Get total number of features from first page metadata"""
        params = self._get_base_params()
        params.update({
            'startIndex': '0',
            'count': '1'  # Just get one feature to check metadata
        })
        self.log.info(self.config.url)
        try:
            self.log.info("Getting total count from first page metadata...")
            async with session.get(self.config.url, params=params) as response:
                response.raise_for_status()
                text = await response.text()
                root = ET.fromstring(text)
                
                # Handle case where numberMatched might be '*'
                number_matched = root.get('numberMatched', '0')
                number_returned = root.get('numberReturned', '0')
                
                self.log.info(f"WFS response metadata - numberMatched: {number_matched}, numberReturned: {number_returned}")
                
                if number_matched == '*':
                    # If server doesn't provide exact count, fetch a larger page to estimate
                    self.log.warning("Server returned '*' for numberMatched, fetching sample to estimate...")
                    params['count'] = '1000'
                    async with session.get(self.config['url'], params=params) as sample_response:
                        sample_text = await sample_response.text()
                        sample_root = ET.fromstring(sample_text)
                        feature_count = len(sample_root.findall('.//mat:SamletFastEjendom_Gaeldende', self.namespaces))
                        # Estimate conservatively
                        return feature_count * 2000  # Adjust multiplier based on expected data size
                
                if not number_matched.isdigit():
                    raise ValueError(f"Invalid numberMatched value: {number_matched}")
                    
                total_available = int(number_matched)
                
                # Add sanity check for unreasonable numbers
                if total_available > 5000000:  # Adjust threshold as needed
                    self.log.warning(f"Unusually high feature count: {total_available:,}. This may indicate an issue.")
                self.log.info(f"Total available features: {total_available:,}")
                return total_available
                
        except Exception as e:
            self.log.error(f"Error getting total count: {str(e)}")
            raise

    async def run(self) -> None:
        """
        Run the complete Cadastral bronze layer job.

        This is the main entry point that orchestrates the entire process:
        1. Fetches raw data from the WFS service
        2. Saves the raw data to Google Cloud Storage

        Returns:
            None

        Raises:
            Exception: If there are issues at any step in the process.

        Note:
            This method is typically called by the pipeline orchestrator.
        """
        self.log.info(os.getenv("SAVE_LOCAL"))
        self.log.info(self.config.save_local)
        self.log.info("Running Cadastral bronze layer job")
        _, gdf = await self._parse_features() 
        if gdf is None:
            self.log.error("Failed to fetch raw data")
            return
        self.log.info("Fetched raw data successfully")
        self._save_data(gdf, self.config.dataset, self.config.bucket, 'bronze')
        self.log.info("Saved raw data successfully")