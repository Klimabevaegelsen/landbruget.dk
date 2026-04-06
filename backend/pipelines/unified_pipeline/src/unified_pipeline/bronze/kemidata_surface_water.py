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

import asyncio
import json
import os
import shutil
import ssl
import tempfile
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
    and stores them in cloud storage for silver layer processing.

    Processing flow:
    1. Fetch metadata (parameter catalogue)
    2. POST /search to discover stations with pesticide data
    3. POST /download to get full CSV export of measurements
    4. Save raw CSV + station JSON + metadata to cloud storage bronze layer
    5. Return manifest for silver layer
    """

    def __init__(self, config: KemidataSurfaceWaterBronzeConfig):
        super().__init__(config)

    def _build_search_body(self, session_id: str, search_parameters: list[dict]) -> dict:
        """
        Build the request body for the Kemidata search API.

        Passes all substance parameters from the metadata catalogue to get ALL
        chemistry data, then filters by surface water media type in post-processing.
        The API requires at least one searchParameter (empty list returns HTTP 500).
        """
        return {
            "language": "da",
            "searchBy": "Chemistry",
            "searchParameters": search_parameters,
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

    def _build_download_body(self, session_id: str, search_parameters: list[dict]) -> dict:
        """Build the request body for the Kemidata download (CSV) API."""
        body = self._build_search_body(session_id, search_parameters)
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

    async def _search_batch(
        self, session: aiohttp.ClientSession, session_id: str, params_batch: list[dict]
    ) -> dict:
        """Search a single batch of parameters. Retries on transient errors."""
        body = self._build_search_body(session_id, params_batch)
        async with session.post(
            self.config.search_url,
            json=body,
            headers={"Content-Type": "application/json"},
        ) as resp:
            if resp.status != 200:
                err_text = await resp.text()
                raise Exception(f"Search failed: HTTP {resp.status} — {err_text[:500]}")
            return await resp.json()

    async def _search_stations(
        self, session: aiohttp.ClientSession, session_id: str, search_parameters: list[dict]
    ) -> dict:
        """
        Search for stations with chemistry data in surface water.

        Batches parameters in chunks of 200 to avoid server-side timeouts,
        then merges the deduplicated station results.

        Returns the full search response including station list and result counts.
        """
        batch_size = 200
        all_stations: dict[str, dict] = {}
        total_results = 0

        batches = [
            search_parameters[i : i + batch_size]
            for i in range(0, len(search_parameters), batch_size)
        ]
        self.log.info(
            f"Searching Kemidata in {len(batches)} batches "
            f"({len(search_parameters)} parameters, batch size {batch_size})..."
        )

        for idx, batch in enumerate(batches, 1):
            self.log.info(f"  Batch {idx}/{len(batches)} ({len(batch)} parameters)")
            data = await self._search_batch(session, session_id, batch)

            for station in data.get("stations", []):
                sid = station.get("id", "")
                if sid and sid not in all_stations:
                    all_stations[sid] = station

            batch_results = sum(r.get("resultCount", 0) for r in data.get("results", []))
            total_results += batch_results

        merged = {
            "stations": list(all_stations.values()),
            "results": [{"resultCount": total_results}],
            "exceedMaxStation": False,
            "canDownloadAll": True,
            "errors": [],
        }

        self.log.info(
            f"Search returned {len(all_stations)} unique stations, "
            f"{total_results:,} total results across all batches"
        )

        return merged, total_results

    async def _download_single_batch(
        self,
        session: aiohttp.ClientSession,
        body: dict,
        cloud_file: Any,
        batch_idx: int,
        attempt: int,
    ) -> int:
        """Download a single batch of parameters as CSV, appending to cloud_file."""
        async with session.post(
            self.config.download_url,
            json=body,
            headers={"Content-Type": "application/json"},
        ) as resp:
            if resp.status != 200:
                err_text = await resp.text()
                raise Exception(
                    f"Download batch {batch_idx} failed (attempt {attempt}): "
                    f"HTTP {resp.status} — {err_text[:500]}"
                )

            batch_bytes = 0
            is_first_line = True
            async for chunk in resp.content.iter_chunked(8 * 1024 * 1024):
                # Skip CSV header line for batches after the first
                if batch_idx > 1 and is_first_line:
                    newline_pos = chunk.find(b"\n")
                    if newline_pos >= 0:
                        chunk = chunk[newline_pos + 1 :]
                    is_first_line = False
                else:
                    is_first_line = False

                if chunk:
                    cloud_file.write(chunk)
                    batch_bytes += len(chunk)

        return batch_bytes

    async def _download_batch_with_retry(
        self,
        session: aiohttp.ClientSession,
        body: dict,
        cloud_file: Any,
        batch_counter: int,
        max_retries: int,
    ) -> int:
        """Try downloading a batch with retries on transient errors."""
        for attempt in range(1, max_retries + 1):
            try:
                return await self._download_single_batch(
                    session, body, cloud_file, batch_counter, attempt
                )
            except Exception:
                if attempt == max_retries:
                    raise
                wait = 10 * attempt
                self.log.warning(
                    f"  Download batch {batch_counter} attempt {attempt} failed, "
                    f"retrying in {wait}s..."
                )
                await asyncio.sleep(wait)
        return 0  # unreachable, but satisfies type checker

    async def _download_by_date_range(
        self,
        session: aiohttp.ClientSession,
        session_id: str,
        params: list[dict],
        cloud_file: Any,
        batch_counter: int,
    ) -> int:
        """
        Download a parameter that exceeds 200k records by splitting into yearly date ranges.

        Kemidata has data from ~1990 to present. We split into 5-year windows to stay
        under the 200k record limit per download.
        """
        current_year = datetime.now().year
        # 5-year windows from 1980 to present
        windows = []
        start = 1980
        while start <= current_year:
            end = min(start + 4, current_year)
            windows.append((f"{start}-01-01", f"{end}-12-31"))
            start = end + 1

        total_bytes = 0
        for win_idx, (from_date, to_date) in enumerate(windows, 1):
            self.log.info(f"      Date range {win_idx}/{len(windows)}: {from_date} to {to_date}")
            body = self._build_download_body(session_id, params)
            body["period"] = {
                "showLastResult": False,
                "fromDate": from_date,
                "toDate": to_date,
            }
            try:
                win_bytes = await self._download_batch_with_retry(
                    session, body, cloud_file, batch_counter + win_idx, 3
                )
                total_bytes += win_bytes
            except Exception as win_exc:
                if "200000 records" not in str(win_exc):
                    raise
                # Even 5-year window too big — split into individual years
                self.log.warning(
                    f"      Window {from_date}–{to_date} still exceeds 200k, "
                    f"splitting into individual years..."
                )
                y_start = int(from_date[:4])
                y_end = int(to_date[:4])
                for year in range(y_start, y_end + 1):
                    yr_body = self._build_download_body(session_id, params)
                    yr_body["period"] = {
                        "showLastResult": False,
                        "fromDate": f"{year}-01-01",
                        "toDate": f"{year}-12-31",
                    }
                    self.log.info(f"        Year {year}")
                    yr_bytes = await self._download_batch_with_retry(
                        session, yr_body, cloud_file, batch_counter + year, 3
                    )
                    total_bytes += yr_bytes

        return total_bytes

    async def _download_csv_to_storage(
        self, session: aiohttp.ClientSession, session_id: str, search_parameters: list[dict]
    ) -> tuple[str, int]:
        """
        Download the full CSV export from Kemidata and upload to storage.

        Downloads to a local temp file first, then uploads to R2.
        This avoids R2's multipart upload constraint (all non-trailing parts
        must have the same size) which is violated when streaming irregular
        chunks directly via s3fs.

        Batches parameters in small chunks to stay under the API's 200k record
        download limit. Uses adaptive sizing: starts at 10 params per batch,
        falls back to 1 param if a batch exceeds the limit.
        Concatenates CSV output, skipping duplicate headers after the first batch.

        Returns:
            Tuple of (relative storage path, total bytes written).
        """
        batch_size = 10
        batches = [
            search_parameters[i : i + batch_size]
            for i in range(0, len(search_parameters), batch_size)
        ]

        run_timestamp = self.date_pattern
        relative_path = f"bronze/{self.config.dataset}/{run_timestamp}/kemidata_export.csv"
        storage_path = f"{self.config.bucket}/{relative_path}"

        self.log.info(
            f"Downloading CSV from Kemidata in {len(batches)} batches (this may take a while)..."
        )

        total_bytes = 0
        max_retries = 3
        batch_counter = 0
        tmp_path = None

        try:
            with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
                tmp_path = tmp.name
                for idx, batch in enumerate(batches, 1):
                    batch_counter += 1
                    self.log.info(
                        f"  Download batch {idx}/{len(batches)} ({len(batch)} parameters)"
                    )
                    body = self._build_download_body(session_id, batch)

                    try:
                        batch_bytes = await self._download_batch_with_retry(
                            session, body, tmp, batch_counter, max_retries
                        )
                        total_bytes += batch_bytes
                    except Exception as exc:
                        if "200000 records" not in str(exc):
                            raise
                        if len(batch) <= 1:
                            # Single param exceeds 200k — split by date range
                            self.log.warning(
                                "  Single parameter exceeds 200k limit, splitting by date range..."
                            )
                            sub_bytes = await self._download_by_date_range(
                                session, session_id, batch, tmp, batch_counter
                            )
                            total_bytes += sub_bytes
                        else:
                            # Batch exceeds 200k — split into individual params
                            self.log.warning(
                                f"  Batch {idx} exceeds 200k record limit, "
                                f"splitting {len(batch)} params into individual downloads..."
                            )
                            for sub_idx, param in enumerate(batch, 1):
                                batch_counter += 1
                                sub_body = self._build_download_body(session_id, [param])
                                self.log.info(
                                    f"    Sub-batch {sub_idx}/{len(batch)}: "
                                    f"{param.get('name', 'unknown')}"
                                )
                                try:
                                    sub_bytes = await self._download_batch_with_retry(
                                        session, sub_body, tmp, batch_counter, max_retries
                                    )
                                    total_bytes += sub_bytes
                                except Exception as sub_exc:
                                    if "200000 records" not in str(sub_exc):
                                        raise
                                    self.log.warning(
                                        f"    Single param {param.get('name', 'unknown')} "
                                        f"exceeds 200k, splitting by date range..."
                                    )
                                    dr_bytes = await self._download_by_date_range(
                                        session, session_id, [param], tmp, batch_counter
                                    )
                                    total_bytes += dr_bytes

            # Upload completed temp file to R2
            size_mb = total_bytes / (1024 * 1024)
            self.log.info(f"Uploading {size_mb:.1f} MB CSV to cloud storage...")
            with open(tmp_path, "rb") as src, self.storage.fs.open(storage_path, "wb") as dst:
                shutil.copyfileobj(src, dst)

        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.unlink(tmp_path)

        self.log.info(f"Uploaded CSV to storage: {size_mb:.1f} MB → {storage_path}")
        return relative_path, total_bytes

    def _storage_path(self, filename: str) -> str:
        """Build a relative storage path for a bronze artifact."""
        return f"bronze/{self.config.dataset}/{self.date_pattern}/{filename}"

    def _save_json_to_storage(self, data: Any, filename: str) -> str:
        """Save JSON data to storage bronze layer using the high-level upload_json."""
        relative_path = self._storage_path(filename)
        storage_uri = f"{self.config.bucket}/{relative_path}"
        self.storage.upload_json(data, storage_uri, ensure_ascii=False)
        self.log.info(f"Saved {filename} to {storage_uri}")
        return relative_path

    async def run(self) -> dict[str, Any] | None:
        """
        Run the bronze layer ingestion pipeline.

        1. Fetch metadata catalogue
        2. Search for surface water stations
        3. Download full CSV export
        4. Save everything to cloud storage
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
                    # 1. Fetch metadata (needed for search parameters)
                    metadata = await self._fetch_metadata(session)
                    search_parameters = metadata.get("substanceParameters", [])
                    self.log.info(
                        f"Loaded {len(search_parameters)} substance parameters from metadata"
                    )

                    # 2. Search for stations
                    search_result, total_result_count = await self._search_stations(
                        session, session_id, search_parameters
                    )

                    stations = search_result.get("stations", [])
                    surface_stations = [
                        s for s in stations if s.get("mediaName") in SURFACE_WATER_MEDIA
                    ]

                    self.log.info(
                        f"Filtered to {len(surface_stations)} surface water stations "
                        f"(from {len(stations)} total)"
                    )

                    # 3. Download CSV (streamed directly to storage)
                    csv_path, csv_size_bytes = await self._download_csv_to_storage(
                        session, session_id, search_parameters
                    )

                    # 4. Save metadata to storage
                    stations_path = self._save_json_to_storage(search_result, "search_result.json")
                    metadata_path = self._save_json_to_storage(metadata, "metadata.json")

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
                    manifest_storage_path = (
                        f"{self.config.bucket}/bronze/{self.config.dataset}/"
                        f"{run_timestamp}/manifest.json"
                    )
                    self.storage.upload_json(manifest, manifest_storage_path)

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
                                f"{self.config.bucket}/bronze/"
                                f"{self.config.dataset}/{run_timestamp}/"
                                f"pipeline_metadata.json"
                            )
                            self.storage.upload_json(pipeline_metadata.model_dump(), pm_path)
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
