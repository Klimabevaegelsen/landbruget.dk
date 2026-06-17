"""Bronze layer data ingestion for Danish afforestation areas."""

import asyncio
import ssl
from asyncio import Semaphore
from typing import ClassVar

import aiohttp
from pydantic import ConfigDict
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from unified_pipeline.common.base import BaseJobConfig, BaseSource, BronzeJobInterface
from unified_pipeline.util.timing import AsyncTimer


class SkovrejsningBronzeConfig(BaseJobConfig):
    """Configuration for Danish afforestation areas from Plandata WFS."""

    name: str = "Danish Afforestation Areas"
    dataset: str = "skovrejsning"
    type: str = "wfs"
    description: str = "Municipal afforestation area classifications"
    url: str = "https://geoserver.plandata.dk/geoserver/wfs"
    frequency: str = "weekly"
    bucket: str = "landbruget-data"

    batch_size: int = 100
    max_concurrent: int = 3
    request_timeout: int = 300
    storage_batch_size: int = 5000
    request_timeout_config: aiohttp.ClientTimeout = aiohttp.ClientTimeout(
        total=request_timeout, connect=60, sock_read=300
    )
    headers: ClassVar[dict[str, str]] = {"User-Agent": "Mozilla/5.0 QGIS/33603/macOS 15.1"}
    request_semaphore: Semaphore = Semaphore(max_concurrent)

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class SkovrejsningBronze(BaseSource[SkovrejsningBronzeConfig], BronzeJobInterface):
    """Bronze layer processor for Danish afforestation areas."""

    def __init__(self, config: SkovrejsningBronzeConfig):
        """Initialize the skovrejsning bronze processor."""
        super().__init__(config)

    def _get_params(self, start_index: int = 0) -> dict[str, str]:
        """Get WFS request parameters for GeoJSON pagination."""
        return {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeNames": "pdk:theme_pdk_skovrejsningsomraade_vedtaget",
            "outputFormat": "application/json",
            "srsName": "urn:ogc:def:crs:EPSG::25832",
            "startIndex": str(start_index),
            "count": str(self.config.batch_size),
        }

    @retry(
        retry=retry_if_exception_type(Exception),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(5),
    )
    async def _fetch_chunck(self, session: aiohttp.ClientSession, start_index: int) -> dict:
        """Fetch a GeoJSON chunk from the WFS service."""
        async with (
            self.config.request_semaphore,
            AsyncTimer(
                f"Fetching chunk from {start_index} to {start_index + self.config.batch_size}"
            ),
        ):
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
                        parsed = await response.json(content_type=None)
                        return {
                            "text": text,
                            "start_index": start_index,
                            "total_features": int(parsed.get("numberMatched", 0) or 0),
                            "returned_features": int(parsed.get("numberReturned", 0) or 0),
                        }
                    except Exception as e:
                        err_msg = f"Failed to parse GeoJSON response: {e}"
                        self.log.error(err_msg)
                        raise Exception(err_msg) from e
            except Exception as e:
                err_msg = f"Error fetching data: {e}"
                self.log.error(err_msg)
                raise Exception(err_msg) from e

    async def _fetch_raw_data(self) -> list[str] | None:
        """Fetch all available skovrejsning GeoJSON chunks from the WFS service."""
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        connector = aiohttp.TCPConnector(ssl=ssl_context)
        raw_features = []
        async with (
            aiohttp.ClientSession(headers=self.config.headers, connector=connector) as session,
            AsyncTimer("Fetching raw data from WFS service for Skovrejsning"),
        ):
            try:
                raw_data = await self._fetch_chunck(session, 0)
                total_features = raw_data["total_features"]
                returned_features = raw_data["returned_features"]
                raw_features.append(raw_data["text"])
                fetched_features_count = returned_features
                self.log.info(f"Fetched {fetched_features_count} out of {total_features}")

                tasks = [
                    self._fetch_chunck(session, start_index)
                    for start_index in range(
                        returned_features, total_features, self.config.batch_size
                    )
                ]

                results = await asyncio.gather(*tasks, return_exceptions=True)

                for result in results:
                    if isinstance(result, Exception):
                        self.log.error(f"Error occurred while fetching chunk: {result}")
                        raise result

                    if isinstance(result, dict):
                        raw_features.append(result["text"])
                        fetched_features_count += result["returned_features"]
                        self.log.debug(
                            f"Processed chunk with {result['returned_features']} features"
                        )
                    else:
                        self.log.error(f"Unexpected result type: {type(result)}")

                self.log.info(
                    f"Fetched all {fetched_features_count} out of {total_features} features"
                )
                return raw_features
            except Exception as e:
                self.log.error(f"Error occured while fetching chunk: {e}")
                raise e

    def create_dataframe(self, raw_data: list[str]):
        """Create a DuckDB table with raw GeoJSON payloads and metadata."""
        current_timestamp = self.conn.execute("SELECT current_timestamp").fetchone()[0]

        self.conn.execute("CREATE OR REPLACE TABLE temp_skovrejsning_data (payload VARCHAR)")

        for data_str in raw_data:
            self.conn.execute("INSERT INTO temp_skovrejsning_data VALUES (?)", [data_str])

        self.conn.execute(
            """
            CREATE OR REPLACE TABLE final_dataframe AS
            SELECT
                payload,
                ? as source,
                ? as created_at,
                ? as updated_at
            FROM temp_skovrejsning_data
        """,
            [self.config.name, current_timestamp, current_timestamp],
        )

        self.conn.execute("DROP TABLE temp_skovrejsning_data")

        return "final_dataframe"

    async def run(self) -> list[str] | None:
        """Run the complete skovrejsning bronze layer job."""
        async with AsyncTimer("Running Skovrejsning bronze job for"):
            self.log.info("Running Skovrejsning bronze job")
            raw_data = await self._fetch_raw_data()
            if not raw_data:
                self.log.error("No raw data fetched")
                return None
            self.log.info("Fetched raw data successfully")

            table_name = self.create_dataframe(raw_data)

            self._save_data(
                data=table_name,
                dataset=self.config.dataset,
                bucket=self.config.bucket,
                stage="bronze",
                conn=self.conn,
            )
            self.log.info("Saved raw data successfully")
            self.log.info("Skovrejsning bronze job completed successfully")

            return raw_data
