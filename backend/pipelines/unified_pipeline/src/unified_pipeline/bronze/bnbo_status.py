import os
import ssl
import xml.etree.ElementTree as ET
from asyncio import Semaphore
from typing import Optional

import aiohttp
import pandas as pd
from pydantic import ConfigDict
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from unified_pipeline.common.base import BaseJobConfig, BaseSource
from unified_pipeline.util.gcs_util import GCSUtil


class BNBOStatusBronzeConfig(BaseJobConfig):
    """
    Configuration for BNBO status data source.
    """

    name: str = "Danish BNBO Status"
    dataset: str = "bnbo_status"
    type: str = "wfs"
    description: str = "Municipal status for well-near protection areas (BNBO)"
    url: str = "https://arealeditering-dist-geo.miljoeportal.dk/geoserver/wfs"
    layer: str = "dai:status_bnbo"
    frequency: str = "weekly"
    bucket: str = "landbrugsdata-raw-data"
    create_dissolved: bool = True

    batch_size: int = 100
    max_concurrent: int = 3
    request_timeout: int = 300
    storage_batch_size: int = 5000
    request_timeout_config: aiohttp.ClientTimeout = aiohttp.ClientTimeout(
        total=request_timeout, connect=60, sock_read=300
    )
    headers: dict[str, str] = {"User-Agent": "Mozilla/5.0 QGIS/33603/macOS 15.1"}
    request_semaphore: Semaphore = Semaphore(max_concurrent)

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class BNBOStatusBronze(BaseSource[BNBOStatusBronzeConfig]):
    def __init__(self, config: BNBOStatusBronzeConfig, gcs_util: GCSUtil):
        super().__init__(config, gcs_util)
        self.config = config

    def _get_params(self, start_index: int = 0) -> dict:
        """Get WFS request parameters"""
        return {
            "SERVICE": "WFS",
            "REQUEST": "GetFeature",
            "VERSION": "2.0.0",
            "TYPENAMES": "dai:status_bnbo",
            "STARTINDEX": str(start_index),
            "COUNT": str(self.config.batch_size),
            "SRSNAME": "urn:ogc:def:crs:EPSG::25832",
        }

    @retry(
        retry=retry_if_exception_type(Exception),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(5),
    )
    async def _fetch_chunck(self, session: aiohttp.ClientSession, start_index: int) -> dict:
        """Fetch a chunk of data from the WFS service"""
        async with self.config.request_semaphore:
            self.log.debug(
                f"Trying to fetch data from {start_index} to {start_index + self.config.batch_size}"
            )
            params = self._get_params(start_index)
            try:
                async with session.get(self.config.url, params=params) as response:
                    if response.status != 200:
                        err_msg = f"Failed to fetch data. Status: {response.status}"
                        self.log.error(err_msg)
                        raise Exception(err_msg)

                    text = await response.text()
                    try:
                        root = ET.fromstring(text)
                        return {
                            "text": text,
                            "start_index": start_index,
                            "total_features": int(root.get("numberMatched", "0")),
                            "returned_features": int(root.get("numberReturned", "0")),
                        }
                    except ET.ParseError as e:
                        err_msg = f"Failed to parse XML response: {e}"
                        self.log.error(err_msg)
                        raise Exception(err_msg)
            except Exception as e:
                err_msg = f"Error fetching data: {e}"
                self.log.error(err_msg)
                raise Exception(err_msg)

    async def _fetch_raw_data(self) -> Optional[list[str]]:
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        connector = aiohttp.TCPConnector(ssl=ssl_context)
        raw_features = []
        async with aiohttp.ClientSession(
            headers=self.config.headers, connector=connector
        ) as session:
            try:
                raw_data = await self._fetch_chunck(session, 0)
                if raw_data is None:
                    return None
                total_features = raw_data["total_features"]
                returned_features = raw_data["returned_features"]
                raw_features.append(raw_data["text"])
                fetched_features_count = returned_features
                self.log.info(f"Fetched {fetched_features_count} out of {total_features}")

                for start_index in range(returned_features, total_features, self.config.batch_size):
                    try:
                        raw_data = await self._fetch_chunck(session, start_index)
                        if raw_data is None:
                            return None
                        raw_features.append(raw_data["text"])
                        fetched_features_count += raw_data["returned_features"]
                        self.log.info(f"Fetched {fetched_features_count} out of {total_features}")
                    except Exception as e:
                        self.log.error("Error occured while fetching chunk: {e}")
                        raise e

                return raw_features
            except Exception as e:
                self.log.error("Error occured while fetching chunk: {e}")
                raise e

    async def _save_raw_data(self, raw_data: list[str], dataset: str) -> None:
        """Save raw data to GCS"""
        bucket = self.gcs_util.get_gcs_client().bucket(self.config.bucket)
        df = pd.DataFrame(
            {
                "payload": raw_data,
            }
        )
        df["source"] = self.config.name
        df["created_at"] = pd.Timestamp.now()
        df["updated_at"] = pd.Timestamp.now()

        temp_dir = f"/tmp/bronze/{dataset}"
        os.makedirs(temp_dir, exist_ok=True)
        current_date = pd.Timestamp.now().strftime("%Y-%m-%d")
        temp_file = f"{temp_dir}/{current_date}.parquet"
        working_blob = bucket.blob(f"bronze/{dataset}/{current_date}.parquet")

        df.to_parquet(temp_file)
        working_blob.upload_from_filename(temp_file)
        self.log.info(f"Uploaded working file has {len(df):,} features")

    async def run(self) -> None:
        """Run the job"""
        self.log.info("Running BNBO status data source")
        raw_data = await self._fetch_raw_data()
        if raw_data is None:
            self.log.error("Failed to fetch raw data")
            return
        self.log.info("Fetched raw data successfully")
        await self._save_raw_data(raw_data, self.config.dataset)
        self.log.info("Saved raw data successfully")
