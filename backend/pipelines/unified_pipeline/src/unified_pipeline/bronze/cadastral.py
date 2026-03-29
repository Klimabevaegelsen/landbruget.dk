import asyncio
import json
import logging
import os
import time
import xml.etree.ElementTree as ET
from asyncio import Semaphore
from datetime import datetime
from typing import ClassVar

import aiohttp
from dotenv import load_dotenv
from pydantic import ConfigDict

# ✅ MIGRATION: Removed shapely imports - using pure coordinate-based WKT generation
from unified_pipeline.common.base import BaseJobConfig, BaseSource, BronzeJobInterface

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
    bucket: str = (
        os.getenv("STORAGE_BUCKET")
        or os.getenv("R2_BUCKET")
        or os.getenv("GCS_BUCKET", "landbruget-data")
    )

    batch_size: int = 25000  # Increased from 10k to 25k for fewer API calls
    max_concurrent: int = 8  # Increased from 5 to 8 for better parallelism
    request_timeout: int = 450  # Increased timeout for larger batches
    storage_batch_size: int = 25000  # Match batch_size for consistency
    request_timeout_config: aiohttp.ClientTimeout = aiohttp.ClientTimeout(
        total=request_timeout, connect=60, sock_read=300
    )
    headers: ClassVar[dict[str, str]] = {"User-Agent": "Mozilla/5.0 QGIS/33603/macOS 15.1"}
    request_semaphore: Semaphore = Semaphore(max_concurrent)
    type: str = "wfs"
    url: str = "https://wfs.datafordeler.dk/MATRIKLEN2/MatGaeldendeOgForeloebigWFS/1.0.0/WFS"
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)
    load_dotenv()
    save_local: bool = os.getenv("SAVE_LOCAL", "False").lower() == "true"


class CadastralBronze(BaseSource[CadastralBronzeConfig], BronzeJobInterface):
    def __init__(self, config: CadastralBronzeConfig) -> None:
        super().__init__(config)
        self.last_request_time = {}
        self.requests_per_second = int(
            os.getenv("CADASTRAL_REQUESTS_PER_SECOND", "4")
        )  # Increased from 2 to 4 req/sec

        self.field_mapping = {
            "BFEnummer": ("bfe_number", int),
            "forretningshaendelse": ("business_event", str),
            "forretningsproces": ("business_process", str),
            "senesteSagLokalId": ("latest_case_id", str),
            "id_lokalId": ("id_local", str),
            "id_namespace": ("id_namespace", str),
            "registreringFra": (
                "registration_from",
                lambda x: datetime.fromisoformat(x.replace("Z", "+00:00")),
            ),
            "virkningFra": (
                "effect_from",
                lambda x: datetime.fromisoformat(x.replace("Z", "+00:00")),
            ),
            "virkningsaktoer": ("authority", str),
            "arbejderbolig": ("is_worker_housing", lambda x: x.lower() == "true"),
            "erFaelleslod": ("is_common_lot", lambda x: x.lower() == "true"),
            "hovedejendomOpdeltIEjerlejligheder": (
                "has_owner_apartments",
                lambda x: x.lower() == "true",
            ),
            "udskiltVej": ("is_separated_road", lambda x: x.lower() == "true"),
            "landbrugsnotering": ("agricultural_notation", str),
        }
        self.page_size = self.config.batch_size
        self.namespaces = {
            "wfs": "http://www.opengis.net/wfs/2.0",
            "mat": "http://data.gov.dk/schemas/matrikel/1",
            "gml": "http://www.opengis.net/gml/3.2",
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
            total=self.config.request_timeout, connect=60, sock_read=self.config.request_timeout
        )

        # Checkpoint/resume functionality
        self.checkpoint_enabled = (
            os.getenv("CADASTRAL_CHECKPOINT_ENABLED", "true").lower() == "true"
        )
        self.checkpoint_file = f"/tmp/cadastral_checkpoint_{int(time.time())}.json"
        self.checkpoint_interval = int(
            os.getenv("CADASTRAL_CHECKPOINT_INTERVAL", "100000")
        )  # Every 100k features

    def _save_checkpoint(self, start_index: int, total_processed: int, features_batch: list):
        """Save processing checkpoint to enable resume functionality"""
        if not self.checkpoint_enabled:
            return

        try:
            checkpoint_data = {
                "start_index": start_index,
                "total_processed": total_processed,
                "timestamp": time.time(),
                "features_count": len(features_batch),
            }

            with open(self.checkpoint_file, "w") as f:
                json.dump(checkpoint_data, f)

            self.log.info(f"Checkpoint saved: {total_processed:,} features processed")

        except Exception as e:
            self.log.warning(f"Failed to save checkpoint: {e}")

    def _load_checkpoint(self):
        """Load processing checkpoint to resume from last position"""
        if not self.checkpoint_enabled:
            return None

        try:
            if os.path.exists(self.checkpoint_file):
                with open(self.checkpoint_file) as f:
                    checkpoint_data = json.load(f)

                # Check if checkpoint is recent (within 6 hours)
                if time.time() - checkpoint_data.get("timestamp", 0) < 21600:
                    self.log.info(
                        f"Resuming from checkpoint: {checkpoint_data['total_processed']:,} features"
                    )
                    return checkpoint_data
                self.log.info("Checkpoint too old, starting fresh")
                os.remove(self.checkpoint_file)

        except Exception as e:
            self.log.warning(f"Failed to load checkpoint: {e}")

        return None

    def _cleanup_checkpoint(self):
        """Clean up checkpoint file after successful completion"""
        try:
            if os.path.exists(self.checkpoint_file):
                os.remove(self.checkpoint_file)
                self.log.info("Checkpoint file cleaned up")
        except Exception as e:
            self.log.warning(f"Failed to cleanup checkpoint: {e}")

    def _get_base_params(self):
        """Get base WFS request parameters without pagination"""
        return {
            "username": self.username,
            "password": self.password,
            "SERVICE": "WFS",
            "REQUEST": "GetFeature",
            "VERSION": "2.0.0",
            "TYPENAMES": "mat:SamletFastEjendom_Gaeldende",
            "SRSNAME": "EPSG:25832",
        }

    def _get_params(self, start_index=0):
        """Get WFS request parameters with pagination"""
        params = self._get_base_params()
        params.update({"startIndex": str(start_index), "count": str(self.page_size)})
        return params

    def _parse_geometry(self, geom_elem):
        """Parse GML geometry to WKT using pure coordinate-based approach.

        ✅ OPTIMIZED: This method now creates WKT directly from coordinates without
        using shapely, providing better performance for large datasets.
        """
        try:
            pos_lists = geom_elem.findall(".//gml:posList", self.namespaces)
            if not pos_lists:
                return None

            polygon_wkts = []
            for pos_list in pos_lists:
                if not pos_list.text:
                    continue

                coords = [float(x) for x in pos_list.text.strip().split()]
                # Handle 2D coordinates - take x,y pairs
                pairs = [(coords[i], coords[i + 1]) for i in range(0, len(coords), 2)]

                if len(pairs) < 4:
                    logger.warning(f"Not enough coordinate pairs ({len(pairs)}) to form a polygon")
                    continue

                try:
                    # Check if the polygon is closed (first point equals last point)
                    if pairs[0] != pairs[-1]:
                        pairs.append(pairs[0])  # Close the polygon

                    # Create WKT polygon directly from coordinate pairs
                    coord_pairs = [f"{x} {y}" for x, y in pairs]
                    polygon_wkt = f"POLYGON(({', '.join(coord_pairs)}))"

                    # Basic validation: check if we have enough points and it's closed
                    if len(pairs) >= 4 and pairs[0] == pairs[-1]:
                        polygon_wkts.append(polygon_wkt)
                    else:
                        logger.warning("Invalid polygon: insufficient points or not closed")
                        continue

                except Exception as e:
                    logger.warning(f"Error creating polygon WKT: {e!s}")
                    continue

            if not polygon_wkts:
                return None

            # Create final WKT (MultiPolygon if multiple, single Polygon otherwise)
            if len(polygon_wkts) == 1:
                final_wkt = polygon_wkts[0]
            else:
                # Create MultiPolygon WKT
                polygon_parts = [wkt.replace("POLYGON", "").strip() for wkt in polygon_wkts]
                final_wkt = f"MULTIPOLYGON({', '.join(polygon_parts)})"

            return final_wkt

        except Exception as e:
            logger.error(f"Error parsing geometry: {e!s}")
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
                elem = feature_elem.find(f".//mat:{xml_field}", self.namespaces)
                if elem is not None and elem.text:
                    try:
                        value = clean_value(elem.text)
                        if value is not None:
                            feature[db_field] = converter(value)
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Error converting field {xml_field}: {e!s}")
                        continue

            # Parse geometry
            geom_elem = feature_elem.find(".//mat:geometri/gml:MultiSurface", self.namespaces)
            if geom_elem is not None:
                geometry_wkt = self._parse_geometry(geom_elem)
                if geometry_wkt:
                    feature["geometry"] = geometry_wkt
                else:
                    logger.warning("Failed to parse geometry for feature")

            # Add validation of required fields
            if not feature.get("bfe_number"):
                logger.warning("Missing required field: bfe_number")
            if not feature.get("geometry"):
                logger.warning("Missing required field: geometry")

            return feature if feature.get("bfe_number") and feature.get("geometry") else None

        except Exception as e:
            logger.error(f"Error parsing feature: {e!s}")
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
                    timeout=timeout or self.config.request_timeout_config,
                ) as response:
                    if response.status == 429:  # Too Many Requests
                        retry_after = int(response.headers.get("Retry-After", 5))
                        self.log.warning(f"Rate limited, waiting {retry_after} seconds")
                        await asyncio.sleep(retry_after)
                        raise aiohttp.ClientError("Rate limited")

                    response.raise_for_status()
                    content = await response.text()
                    root = ET.fromstring(content)

                    # Add validation of returned features count
                    number_returned = root.get("numberReturned", "0")
                    self.log.info(f"WFS reports {number_returned} features returned in this chunk")

                    features = []
                    feature_elements = root.findall(
                        ".//mat:SamletFastEjendom_Gaeldende", self.namespaces
                    )
                    self.log.info(f"Found {len(feature_elements)} feature elements in XML")

                    for feature_elem in feature_elements:
                        feature = self._parse_feature(feature_elem)
                        if feature:
                            features.append(feature)

                    valid_count = len(features)
                    self.log.info(
                        f"Chunk {start_index}: parsed {valid_count} valid features "
                        f"out of {len(feature_elements)} elements"
                    )

                    # Validate that we're getting reasonable numbers
                    if valid_count == 0 and len(feature_elements) > 0:
                        self.log.warning(
                            f"No valid features parsed from {len(feature_elements)} elements - "
                            "possible parsing issue"
                        )
                    elif (
                        valid_count < len(feature_elements) * 0.5
                    ):  # If we're losing more than 50% of features
                        self.log.warning(
                            f"Low feature parsing success rate: "
                            f"{valid_count}/{len(feature_elements)}"
                        )

                    return features

            except Exception as e:
                self.log.error(f"Error fetching chunk at index {start_index}: {e!s}")
                raise

    async def _parse_features(self):
        try:
            async with aiohttp.ClientSession(timeout=self.total_timeout_config) as session:
                total_features = await self._get_total_count(session)
                self.log.info(f"Found {total_features:,} total features")

                # Performance tracking variables
                start_time = time.time()
                last_progress_time = start_time
                features_batch = []
                total_processed = 0
                failed_chunks = []

                # Check for existing checkpoint
                checkpoint = self._load_checkpoint()
                if checkpoint:
                    # Resume from checkpoint
                    start_from = checkpoint["start_index"]
                    total_processed = checkpoint["total_processed"]
                    self.log.info(
                        f"Resuming from checkpoint at index {start_from:,} "
                        f"with {total_processed:,} features already processed"
                    )
                else:
                    start_from = 0

                # Calculate expected processing time based on current performance
                remaining_features = total_features - total_processed
                expected_features_per_minute = 8000  # Target minimum performance
                expected_duration_minutes = remaining_features / expected_features_per_minute
                self.log.info(
                    f"Expected processing duration: {expected_duration_minutes:.1f} minutes "
                    f"for {remaining_features:,} remaining features"
                )

                for start_index in range(start_from, total_features, self.page_size):
                    chunk_start_time = time.time()

                    try:
                        chunk = await self._fetch_chunk(session, start_index)
                        if chunk:
                            features_batch.extend(chunk)
                            total_processed += len(chunk)

                            # Enhanced progress monitoring every 15 minutes or 50k features
                            current_time = time.time()
                            if (
                                current_time - last_progress_time >= 900  # 15 minutes
                                or total_processed % 50000 == 0
                            ):
                                elapsed_minutes = (current_time - start_time) / 60
                                features_per_minute = (
                                    total_processed / elapsed_minutes if elapsed_minutes > 0 else 0
                                )
                                remaining_features = total_features - total_processed
                                eta_minutes = (
                                    remaining_features / features_per_minute
                                    if features_per_minute > 0
                                    else 0
                                )

                                progress_pct = (total_processed / total_features) * 100

                                self.log.info(
                                    f"🚀 PROGRESS: {total_processed:,}/{total_features:,} features "
                                    f"({progress_pct:.1f}%) | "
                                    f"⚡ Speed: {features_per_minute:,.0f} features/min | "
                                    f"⏱️  ETA: {eta_minutes:.1f} minutes | "
                                    f"⌛ Elapsed: {elapsed_minutes:.1f} minutes"
                                )

                                # Early termination if processing is too slow
                                if (
                                    features_per_minute < 6000 and elapsed_minutes > 60
                                ):  # Below 6k/min after 1 hour
                                    self.log.warning(
                                        f"⚠️  PERFORMANCE WARNING: Processing speed "
                                        f"({features_per_minute:,.0f}/min) below target (8000/min)."
                                    )

                                last_progress_time = current_time

                            # Save checkpoint periodically
                            if (
                                total_processed % self.checkpoint_interval == 0
                                and total_processed > 0
                            ):
                                self._save_checkpoint(
                                    start_index + self.page_size, total_processed, features_batch
                                )

                            # Log chunk performance for debugging
                            chunk_duration = time.time() - chunk_start_time
                            if chunk_duration > 60:  # Log slow chunks (>1 minute)
                                self.log.warning(
                                    f"Slow chunk at {start_index}: {chunk_duration:.1f}s "
                                    f"for {len(chunk)} features"
                                )

                    except Exception as e:
                        logger.error(f"Error processing batch at {start_index}: {e!s}")
                        failed_chunks.append(start_index)
                        continue

                # Final performance summary
                total_duration = time.time() - start_time
                final_features_per_minute = (
                    total_processed / (total_duration / 60) if total_duration > 0 else 0
                )

                if failed_chunks:
                    logger.error(f"Failed to process chunks starting at indices: {failed_chunks}")

                self.log.info(
                    f"✅ SYNC COMPLETED: {total_processed:,} features processed | "
                    f"⚡ Final speed: {final_features_per_minute:,.0f} features/min | "
                    f"⏱️  Total time: {total_duration / 60:.1f} minutes"
                )

                # Clean up checkpoint file on successful completion
                self._cleanup_checkpoint()

                # Return the raw features batch for processing by silver layer
                return total_processed, features_batch

        except Exception as e:
            self.is_sync_complete = False
            logger.error(f"Error in sync: {e!s}")
            raise

    async def _get_total_count(self, session):
        """Get total number of features from first page metadata"""
        params = self._get_base_params()
        params.update(
            {
                "startIndex": "0",
                "count": "1",  # Just get one feature to check metadata
            }
        )
        self.log.info(self.config.url)
        try:
            self.log.info("Getting total count from first page metadata...")
            async with session.get(self.config.url, params=params) as response:
                response.raise_for_status()
                text = await response.text()
                root = ET.fromstring(text)

                # Handle case where numberMatched might be '*'
                number_matched = root.get("numberMatched", "0")
                number_returned = root.get("numberReturned", "0")

                self.log.info(
                    f"WFS response metadata - numberMatched: {number_matched}, "
                    f"numberReturned: {number_returned}"
                )

                if number_matched == "*":
                    # If server doesn't provide exact count, fetch a larger page to estimate
                    self.log.warning(
                        "Server returned '*' for numberMatched, fetching sample to estimate..."
                    )
                    params["count"] = "1000"
                    async with session.get(self.config["url"], params=params) as sample_response:
                        sample_text = await sample_response.text()
                        sample_root = ET.fromstring(sample_text)
                        feature_count = len(
                            sample_root.findall(
                                ".//mat:SamletFastEjendom_Gaeldende", self.namespaces
                            )
                        )
                        # Estimate conservatively
                        return feature_count * 2000  # Adjust multiplier based on expected data size

                if not number_matched.isdigit():
                    raise ValueError(f"Invalid numberMatched value: {number_matched}")

                total_available = int(number_matched)

                # Add sanity check for unreasonable numbers
                if total_available > 5000000:  # Adjust threshold as needed
                    self.log.warning(
                        f"Unusually high feature count: {total_available:,}. "
                        "This may indicate an issue."
                    )
                self.log.info(f"Total available features: {total_available:,}")
                return total_available

        except Exception as e:
            self.log.error(f"Error getting total count: {e!s}")
            raise

    async def run(self) -> list | None:
        """
        Run the complete Cadastral bronze layer job.

        This is the main entry point that orchestrates the entire process:
        1. Fetches raw data from the WFS service
        2. Saves the raw data to cloud storage
        3. Returns the processed data for in-memory passing to silver stage

        Returns:
            Optional[list]: The raw cadastral features data that can be
                           passed to silver stage, or None if processing fails

        Raises:
            Exception: If there are issues at any step in the process.

        Note:
            This method is typically called by the pipeline orchestrator.
        """
        self.log.info(os.getenv("SAVE_LOCAL"))
        self.log.info(self.config.save_local)
        self.log.info("Running Cadastral bronze layer job")
        _, features_data = await self._parse_features()
        if features_data is None:
            self.log.error("Failed to fetch raw data")
            return None
        self.log.info("Fetched raw data successfully")

        # Save raw JSON data to bronze layer with explicit JSON filename
        # Using the new metadata-enhanced save method for complete data tracing
        self._save_data_with_metadata(
            data=features_data,
            dataset=self.config.dataset,
            source_key="cadastral",  # From DATA_SOURCE_REGISTRY
            bucket=self.config.bucket,
            stage="bronze",
            filename="data.json",
        )
        self.log.info("Saved raw data successfully with pipeline metadata")

        # Return data for in-memory passing
        return features_data
