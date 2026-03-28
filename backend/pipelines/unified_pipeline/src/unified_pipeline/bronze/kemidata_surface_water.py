"""
Bronze layer data ingestion for Kemidata surface water pesticide data.

This module downloads pesticide detection data from the Kemidata REST API
(Danmarks Miljøportal) for surface water monitoring stations (rivers and lakes).

Data source:
- API: https://kemidata.miljoeportal.dk/api/
- Endpoints: /search (station discovery), /download (CSV export), /metadata (parameters)
- Coverage: 2,900+ chemical parameters, 70M+ observations, 10,000+ stations
- Media types: Vandløb (rivers), Sø (lakes) — filtered from all media types

The pipeline is designed to start with pesticides but can expand to all parameters
by modifying the search configuration.
"""

import json
import ssl
import time
import uuid
from datetime import datetime
from typing import Any

import aiohttp
import certifi
from common.crs_utils import DENMARK_BOUNDS_UTM
from pydantic import ConfigDict
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from unified_pipeline.common.base import BaseJobConfig, BaseSource, BronzeJobInterface
from unified_pipeline.util.timing import AsyncTimer


def _build_area_geojson(bbox: dict) -> str:
    """Build GeoJSON FeatureCollection string for the Kemidata area parameter."""
    coords = [
        [bbox["min_x"], bbox["min_y"]],
        [bbox["min_x"], bbox["max_y"]],
        [bbox["max_x"], bbox["max_y"]],
        [bbox["max_x"], bbox["min_y"]],
        [bbox["min_x"], bbox["min_y"]],
    ]
    geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Polygon", "coordinates": [coords]},
                "properties": None,
            }
        ],
    }
    return json.dumps(geojson, separators=(",", ":"))


# Surface water media types to include
SURFACE_WATER_MEDIA = {"Vandløb", "Sø"}


class KemidataSurfaceWaterBronzeConfig(BaseJobConfig):
    """
    Configuration for Kemidata surface water pesticide bronze ingestion.

    Attributes:
        name: Human-readable name of the data source
        dataset: Name of the dataset in storage
        type: Type of the data source
        description: Brief description of the data
        search_url: Kemidata search API endpoint
        download_url: Kemidata CSV download endpoint
        metadata_url: Kemidata metadata endpoint
        source_crs: Coordinate reference system of source data (EPSG:25832)
        bucket: Storage bucket name
        request_timeout: Timeout for API requests in seconds
    """

    name: str = "Kemidata Surface Water Pesticides"
    dataset: str = "kemidata_surface_water_pesticides"
    type: str = "rest_api"
    description: str = (
        "Pesticide detections in Danish surface water (rivers, lakes) "
        "from Kemidata/Danmarks Miljøportal discrete sampling"
    )
    search_url: str = "https://kemidata.miljoeportal.dk/api/search"
    download_url: str = "https://kemidata.miljoeportal.dk/api/download"
    metadata_url: str = "https://kemidata.miljoeportal.dk/api/metadata?language=da"
    source_crs: str = "EPSG:25832"
    bucket: str = "landbruget-data"
    request_timeout: int = 600  # 10 minutes — CSV downloads can be large
    frequency: str = "quarterly"

    model_config = ConfigDict(frozen=True)


class KemidataSurfaceWaterBronze(BaseSource[KemidataSurfaceWaterBronzeConfig], BronzeJobInterface):
    """
    Bronze layer processing for Kemidata surface water pesticide data.

    Downloads raw CSV data and station metadata from the Kemidata API
    and stores them in GCS for silver layer processing.

    Processing flow:
    1. Fetch metadata (parameter catalogue)
    2. POST /search to discover stations with pesticide data
    3. POST /download to get full CSV export of measurements
    4. Save raw CSV + station JSON + metadata to GCS bronze layer
    5. Return manifest for silver layer
    """

    def __init__(self, config: KemidataSurfaceWaterBronzeConfig):
        super().__init__(config)

    def _build_search_body(self, session_id: str) -> dict:
        """
        Build the request body for the Kemidata search API.

        Uses no searchParameters filter to get ALL chemistry data,
        then filters by surface water media type in post-processing.
        This makes the pipeline easily expandable to non-pesticide parameters.
        """
        return {
            "language": "da",
            "searchBy": "Chemistry",
            "searchParameters": [],
            "period": {"showLastResult": False, "fromDate": None, "toDate": None},
            "area": {
                "type": "Rectangle",
                "geoJsonString": _build_area_geojson(DENMARK_BOUNDS_UTM),
            },
            "mediaTypes": list(SURFACE_WATER_MEDIA),
            "dataResponsibles": None,
            "ownersCvr": None,
            "operatorsCvr": None,
            "measuredParameters": None,
            "searchValues": None,
            "sessionId": session_id,
        }

    def _build_download_body(self, session_id: str) -> dict:
        """Build the request body for the Kemidata download (CSV) API."""
        body = self._build_search_body(session_id)
        body["isDownloadAll"] = True
        return body

    @retry(
        retry=retry_if_exception_type((aiohttp.ClientError, TimeoutError)),
        wait=wait_exponential(multiplier=2, min=5, max=60),
        stop=stop_after_attempt(3),
    )
    async def _fetch_metadata(self, session: aiohttp.ClientSession) -> dict:
        """Fetch the Kemidata parameter metadata catalogue."""
        self.log.info(f"Fetching metadata from {self.config.metadata_url}")
        async with session.get(self.config.metadata_url) as resp:
            if resp.status != 200:
                raise Exception(f"Metadata fetch failed: HTTP {resp.status}")
            data = await resp.json()
            self.log.info(f"Fetched metadata with {len(data)} top-level keys")
            return data

    @retry(
        retry=retry_if_exception_type((aiohttp.ClientError, TimeoutError)),
        wait=wait_exponential(multiplier=2, min=5, max=60),
        stop=stop_after_attempt(3),
    )
    async def _search_stations(self, session: aiohttp.ClientSession, session_id: str) -> dict:
        """
        Search for stations with chemistry data in surface water.

        Returns the full search response including station list and result counts.
        """
        body = self._build_search_body(session_id)
        self.log.info("Searching Kemidata for surface water chemistry stations...")

        async with session.post(
            self.config.search_url,
            json=body,
            headers={"Content-Type": "application/json"},
        ) as resp:
            if resp.status != 200:
                err_text = await resp.text()
                raise Exception(f"Search failed: HTTP {resp.status} — {err_text[:500]}")
            data = await resp.json()

        station_count = len(data.get("stations", []))
        result_summary = data.get("results", [])
        total_results = sum(r.get("resultCount", 0) for r in result_summary)

        self.log.info(f"Search returned {station_count} stations, {total_results:,} total results")

        return data, total_results

    @retry(
        retry=retry_if_exception_type((aiohttp.ClientError, TimeoutError)),
        wait=wait_exponential(multiplier=2, min=10, max=120),
        stop=stop_after_attempt(3),
    )
    async def _download_csv_to_gcs(
        self, session: aiohttp.ClientSession, session_id: str
    ) -> tuple[str, int]:
        """
        Download the full CSV export from Kemidata and stream directly to GCS.

        Streams the response in chunks to avoid holding 100MB+ in memory.

        Returns:
            Tuple of (relative GCS path, total bytes written).
        """
        body = self._build_download_body(session_id)
        self.log.info("Downloading CSV from Kemidata (this may take a while)...")

        run_timestamp = self.date_pattern
        relative_path = f"bronze/{self.config.dataset}/{run_timestamp}/kemidata_export.csv"
        storage_path = f"{self.config.bucket}/{relative_path}"

        async with session.post(
            self.config.download_url,
            json=body,
            headers={"Content-Type": "application/json"},
        ) as resp:
            if resp.status != 200:
                err_text = await resp.text()
                raise Exception(f"Download failed: HTTP {resp.status} — {err_text[:500]}")

            total_bytes = 0
            with self.gcs_access.fs.open(storage_path, "wb") as gcs_file:
                async for chunk in resp.content.iter_chunked(8 * 1024 * 1024):
                    gcs_file.write(chunk)
                    total_bytes += len(chunk)

        size_mb = total_bytes / (1024 * 1024)
        self.log.info(f"Streamed CSV to GCS: {size_mb:.1f} MB → {storage_path}")
        return relative_path, total_bytes

    def _gcs_path(self, filename: str) -> str:
        """Build a relative GCS path for a bronze artifact."""
        return f"bronze/{self.config.dataset}/{self.date_pattern}/{filename}"

    def _save_json_to_gcs(self, data: Any, filename: str) -> str:
        """Save JSON data to GCS bronze layer using the high-level upload_json."""
        relative_path = self._gcs_path(filename)
        gcs_uri = f"gs://{self.config.bucket}/{relative_path}"
        self.gcs_access.upload_json(data, gcs_uri, ensure_ascii=False)
        self.log.info(f"Saved {filename} to {gcs_uri}")
        return relative_path

    async def run(self) -> dict[str, Any] | None:
        """
        Run the bronze layer ingestion pipeline.

        1. Fetch metadata catalogue
        2. Search for surface water stations
        3. Download full CSV export
        4. Save everything to GCS
        5. Return manifest for silver layer
        """
        async with AsyncTimer("Running Kemidata Surface Water bronze job"):
            self.log.info("Running Kemidata Surface Water Pesticides bronze job")

            self.set_source_crs(self.config.source_crs)

            session_id = str(uuid.uuid4())

            ssl_context = ssl.create_default_context(cafile=certifi.where())
            connector = aiohttp.TCPConnector(ssl=ssl_context)
            timeout = aiohttp.ClientTimeout(total=self.config.request_timeout)

            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                try:
                    # 1. Fetch metadata
                    metadata = await self._fetch_metadata(session)

                    # 2. Search for stations
                    search_result, total_result_count = await self._search_stations(
                        session, session_id
                    )

                    stations = search_result.get("stations", [])
                    surface_stations = [
                        s for s in stations if s.get("mediaName") in SURFACE_WATER_MEDIA
                    ]

                    self.log.info(
                        f"Filtered to {len(surface_stations)} surface water stations "
                        f"(from {len(stations)} total)"
                    )

                    # 3. Download CSV (streamed directly to GCS)
                    csv_path, csv_size_bytes = await self._download_csv_to_gcs(session, session_id)

                    # 4. Save metadata to GCS
                    stations_path = self._save_json_to_gcs(search_result, "search_result.json")
                    metadata_path = self._save_json_to_gcs(metadata, "metadata.json")

                    # 5. Build manifest
                    run_timestamp = self.date_pattern
                    manifest = {
                        "dataset": self.config.dataset,
                        "bucket": self.config.bucket,
                        "source_crs": self.config.source_crs,
                        "run_timestamp": run_timestamp,
                        "downloaded_at": datetime.now().isoformat(),
                        "session_id": session_id,
                        # Paths
                        "csv_path": csv_path,
                        "stations_path": stations_path,
                        "metadata_path": metadata_path,
                        # Statistics
                        "csv_size_bytes": csv_size_bytes,
                        "total_stations": len(stations),
                        "surface_water_stations": len(surface_stations),
                        "search_result_count": total_result_count,
                    }

                    # Save manifest
                    manifest_gcs_path = (
                        f"gs://{self.config.bucket}/bronze/{self.config.dataset}/"
                        f"{run_timestamp}/manifest.json"
                    )
                    self.gcs_access.upload_json(manifest, manifest_gcs_path)

                    # Pipeline metadata
                    if self.pipeline_metadata_manager and self.processing_start_time:
                        try:
                            processing_duration = time.time() - self.processing_start_time
                            pipeline_metadata = self.pipeline_metadata_manager.create_metadata(
                                source_key="kemidata_surface_water_pesticides",
                                record_count=len(surface_stations),
                                processing_duration=processing_duration,
                            )
                            pm_path = (
                                f"gs://{self.config.bucket}/bronze/"
                                f"{self.config.dataset}/{run_timestamp}/"
                                f"pipeline_metadata.json"
                            )
                            self.gcs_access.upload_json(pipeline_metadata.model_dump(), pm_path)
                            self.log.info(f"Pipeline metadata saved to {pm_path}")
                        except Exception as e:
                            self.log.warning(f"Failed to create pipeline metadata: {e}")

                    self.log.info(
                        f"Kemidata bronze job completed. "
                        f"CSV: {csv_size_bytes / (1024 * 1024):.1f} MB, "
                        f"{len(surface_stations)} surface water stations"
                    )

                    return manifest

                except Exception as e:
                    self.log.error(f"Error in Kemidata bronze job: {e}")
                    raise
