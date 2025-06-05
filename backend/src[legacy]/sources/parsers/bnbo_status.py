import asyncio
import logging
import os
import ssl
import xml.etree.ElementTree as ET

import aiohttp
import backoff
import geopandas as gpd
import pandas as pd
from aiohttp import ClientError, ClientTimeout
from shapely import difference, wkt
from shapely.geometry import MultiPolygon, Polygon
from shapely.ops import unary_union

from ..base import GeospatialSource
from ..utils.geometry_validator import validate_and_transform_geometries

logger = logging.getLogger(__name__)


def clean_value(value):
    """Clean string values"""
    if not isinstance(value, str):
        return value
    value = value.strip()
    return value if value else None


class BNBOStatus(GeospatialSource):
    source_id = "bnbo_status"

    def __init__(self, config):
        super().__init__(config)
        self.batch_size = 100
        self.max_concurrent = 3
        self.request_timeout = 300
        self.storage_batch_size = 5000

        self.request_timeout_config = ClientTimeout(total=self.request_timeout, connect=60, sock_read=300)

        self.headers = {"User-Agent": "Mozilla/5.0 QGIS/33603/macOS 15.1"}

        self.request_semaphore = asyncio.Semaphore(self.max_concurrent)

        # Define status mappings
        self.status_mapping = {
            "Frivillig aftale tilbudt (UDGÅET)": "Action Required",
            "Gennemgået, indsats nødvendig": "Action Required",
            "Ikke gennemgået (default værdi)": "Action Required",
            "Gennemgået, indsats ikke nødvendig": "Completed",
            "Indsats gennemført": "Completed",
            "Ingen erhvervsmæssig anvendelse af pesticider": "Completed",
        }

    def _get_params(self, start_index=0):
        """Get WFS request parameters"""
        return {
            "SERVICE": "WFS",
            "REQUEST": "GetFeature",
            "VERSION": "2.0.0",
            "TYPENAMES": "dai:status_bnbo",
            "STARTINDEX": str(start_index),
            "COUNT": str(self.batch_size),
            "SRSNAME": "urn:ogc:def:crs:EPSG::25832",
        }

    def _parse_geometry(self, geom_elem):
        """Parse GML geometry into WKT and calculate area"""
        try:
            gml_ns = "{http://www.opengis.net/gml/3.2}"

            multi_surface = geom_elem.find(f".//{gml_ns}MultiSurface")
            if multi_surface is None:
                logger.error("No MultiSurface element found")
                return None

            polygons = []
            for surface_member in multi_surface.findall(f".//{gml_ns}surfaceMember"):
                polygon = surface_member.find(f".//{gml_ns}Polygon")
                if polygon is None:
                    continue

                pos_list = polygon.find(f".//{gml_ns}posList")
                if pos_list is None or not pos_list.text:
                    continue

                try:
                    coords = [float(x) for x in pos_list.text.strip().split()]
                    coords = [(coords[i], coords[i + 1]) for i in range(0, len(coords), 2)]
                    if len(coords) >= 4:
                        polygons.append(Polygon(coords))
                except Exception as e:
                    logger.error(f"Failed to parse coordinates: {str(e)}")
                    continue

            if not polygons:
                return None

            geom = MultiPolygon(polygons) if len(polygons) > 1 else polygons[0]
            area_ha = geom.area / 10000  # Convert square meters to hectares

            return {"wkt": geom.wkt, "area_ha": area_ha}

        except Exception as e:
            logger.error(f"Error parsing geometry: {str(e)}")
            return None

    def _parse_feature(self, feature):
        """Parse a single feature into a dictionary"""
        try:
            namespace = feature.tag.split("}")[0].strip("{")

            geom_elem = feature.find("{%s}Shape" % namespace)
            if geom_elem is None:
                logger.warning("No geometry found in feature")
                return None

            geometry_data = self._parse_geometry(geom_elem)
            if geometry_data is None:
                logger.warning("Failed to parse geometry")
                return None

            data = {"geometry": geometry_data["wkt"], "area_ha": geometry_data["area_ha"]}

            for elem in feature:
                if not elem.tag.endswith("Shape"):
                    key = elem.tag.split("}")[-1].lower()
                    if elem.text:
                        value = clean_value(elem.text)
                        if value is not None:
                            data[key] = value

            # Map the status to simplified categories
            if "status_bnbo" in data:
                data["status_category"] = self.status_mapping.get(data["status_bnbo"], "Unknown")

            return data

        except Exception as e:
            logger.error(f"Error parsing feature: {str(e)}", exc_info=True)
            return None

    @backoff.on_exception(backoff.expo, (ClientError, asyncio.TimeoutError), max_tries=3)
    async def _fetch_chunk(self, session, start_index):
        """Fetch a chunk of features with retries"""
        async with self.request_semaphore:
            params = self._get_params(start_index)

            try:
                async with session.get(
                    "https://arealeditering-dist-geo.miljoeportal.dk/geoserver/wfs",
                    params=params,
                    timeout=self.request_timeout_config,
                ) as response:
                    if response.status != 200:
                        logger.error(f"Error fetching chunk. Status: {response.status}")
                        error_text = await response.text()
                        logger.error(f"Error response: {error_text[:500]}")
                        return None

                    text = await response.text()
                    root = ET.fromstring(text)

                    features = []
                    namespaces = {}
                    for elem in root.iter():
                        if "}" in elem.tag:
                            ns_url = elem.tag.split("}")[0].strip("{")
                            namespaces["ns"] = ns_url
                            break

                    for member in root.findall(".//ns:member", namespaces=namespaces):
                        for feature in member:
                            parsed = self._parse_feature(feature)
                            if parsed and parsed.get("geometry"):
                                features.append(parsed)

                    return features

            except Exception as e:
                logger.error(f"Error fetching chunk: {str(e)}")
                raise

    async def write_to_storage(self, features, dataset):
        """Write features to GeoParquet in Cloud Storage with special dissolve logic"""
        if not features:
            return

        try:
            # Create DataFrame from features
            df = pd.DataFrame(features)
            geometries = [wkt.loads(f["geometry"]) for f in features]
            gdf = gpd.GeoDataFrame(df, geometry=geometries, crs="EPSG:25832")

            # Handle working/final files
            temp_working = f"/tmp/{dataset}_working.parquet"
            working_blob = self.bucket.blob(f"raw/{dataset}/working.parquet")

            if working_blob.exists():
                working_blob.download_to_filename(temp_working)
                existing_gdf = gpd.read_parquet(temp_working)
                logger.info(f"Appending {len(gdf):,} features to existing {len(existing_gdf):,}")
                combined_gdf = pd.concat([existing_gdf, gdf], ignore_index=True)
            else:
                combined_gdf = gdf

            # Write working file
            combined_gdf.to_parquet(temp_working)
            working_blob.upload_from_filename(temp_working)
            logger.info(f"Updated working file now has {len(combined_gdf):,} features")

            # If sync complete, create final files
            if hasattr(self, "is_sync_complete") and self.is_sync_complete:
                logger.info("Sync complete - writing final files")

                # Write regular final file
                final_blob = self.bucket.blob(f"raw/{dataset}/current.parquet")
                final_blob.upload_from_filename(temp_working)

                # Create dissolved version with special handling
                logger.info("Creating dissolved version...")
                try:
                    # Convert to WGS84 before processing
                    if combined_gdf.crs.to_epsg() != 4326:
                        combined_gdf = combined_gdf.to_crs("EPSG:4326")

                    # Split into two categories
                    action_required = combined_gdf[combined_gdf["status_category"] == "Action Required"]
                    completed = combined_gdf[combined_gdf["status_category"] == "Completed"]

                    # Dissolve each category
                    action_required_dissolved = None
                    if not action_required.empty:
                        action_required_dissolved = unary_union(action_required.geometry.values).buffer(0)

                    completed_dissolved = None
                    if not completed.empty:
                        completed_dissolved = unary_union(completed.geometry.values).buffer(0)

                    # Handle overlaps - remove completed areas that overlap with action required
                    if action_required_dissolved is not None and completed_dissolved is not None:
                        completed_dissolved = difference(completed_dissolved, action_required_dissolved)

                    # Create final dissolved GeoDataFrame
                    dissolved_geometries = []
                    categories = []

                    if action_required_dissolved is not None:
                        if action_required_dissolved.geom_type == "MultiPolygon":
                            dissolved_geometries.extend(list(action_required_dissolved.geoms))
                            categories.extend(["Action Required"] * len(action_required_dissolved.geoms))
                        else:
                            dissolved_geometries.append(action_required_dissolved)
                            categories.append("Action Required")

                    if completed_dissolved is not None:
                        if completed_dissolved.geom_type == "MultiPolygon":
                            dissolved_geometries.extend(list(completed_dissolved.geoms))
                            categories.extend(["Completed"] * len(completed_dissolved.geoms))
                        else:
                            dissolved_geometries.append(completed_dissolved)
                            categories.append("Completed")

                    dissolved_gdf = gpd.GeoDataFrame(
                        {"status_category": categories, "geometry": dissolved_geometries}, crs="EPSG:4326"
                    )

                    # Final validation
                    dissolved_gdf = validate_and_transform_geometries(dissolved_gdf, f"{dataset}_dissolved")

                    # Write dissolved version
                    temp_dissolved = f"/tmp/{dataset}_dissolved.parquet"
                    dissolved_gdf.to_parquet(temp_dissolved)
                    dissolved_blob = self.bucket.blob(f"raw/{dataset}/dissolved_current.parquet")
                    dissolved_blob.upload_from_filename(temp_dissolved)
                    logger.info("Dissolved version created and saved")

                except Exception as e:
                    logger.error(f"Error during dissolve operation: {str(e)}")
                    raise

                # Cleanup
                working_blob.delete()
                os.remove(temp_dissolved)

            # Cleanup working file
            if os.path.exists(temp_working):
                os.remove(temp_working)

        except Exception as e:
            logger.error(f"Error writing to storage: {str(e)}")
            raise

    async def sync(self):
        """Sync BNBO status data"""
        logger.info("Starting BNBO status sync...")
        self.is_sync_complete = False
        total_processed = 0
        features_batch = []

        try:
            # Create SSL context that doesn't verify certificates
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            connector = aiohttp.TCPConnector(ssl=ssl_context)
            async with aiohttp.ClientSession(headers=self.headers, connector=connector) as session:
                # Get total count
                params = self._get_params(0)
                async with session.get(
                    "https://arealeditering-dist-geo.miljoeportal.dk/geoserver/wfs", params=params
                ) as response:
                    if response.status != 200:
                        logger.error(f"Failed initial request. Status: {response.status}")
                        return 0

                    text = await response.text()
                    root = ET.fromstring(text)
                    total_features = int(root.get("numberMatched", "0"))
                    logger.info(f"Found {total_features:,} total features")

                    # Process first batch
                    features = []
                    namespaces = {}
                    for elem in root.iter():
                        if "}" in elem.tag:
                            ns_url = elem.tag.split("}")[0].strip("{")
                            namespaces["ns"] = ns_url
                            break

                    for member in root.findall(".//ns:member", namespaces=namespaces):
                        for feature in member:
                            parsed = self._parse_feature(feature)
                            if parsed and parsed.get("geometry"):
                                features.append(parsed)

                    if features:
                        features_batch.extend(features)
                        total_processed += len(features)
                        logger.info(f"Processed {len(features):,} features")

                    # Process remaining batches
                    for start_index in range(self.batch_size, total_features, self.batch_size):
                        logger.info(
                            f"Fetching features {start_index:,}-{min(start_index + self.batch_size, total_features):,} of {total_features:,}"
                        )
                        chunk = await self._fetch_chunk(session, start_index)
                        if chunk:
                            features_batch.extend(chunk)
                            total_processed += len(chunk)
                            logger.info(f"Processed {len(chunk):,} features")

                        # Write batch if it's large enough
                        if len(features_batch) >= self.storage_batch_size:
                            logger.info(f"Writing batch of {len(features_batch):,} features")
                            await self.write_to_storage(features_batch, "bnbo_status")
                            features_batch = []

                # Write any remaining features as final batch
                if features_batch:
                    logger.info(f"Writing final batch of {len(features_batch):,} features")
                    self.is_sync_complete = True
                    await self.write_to_storage(features_batch, "bnbo_status")

                logger.info(f"Sync completed. Total processed: {total_processed:,}")
                return total_processed

        except Exception as e:
            self.is_sync_complete = False
            logger.error(f"Error in sync: {str(e)}", exc_info=True)
            return total_processed

    async def fetch(self):
        """Implement abstract method - using sync() instead"""
        return await self.sync()
