"""
Bronze layer data ingestion for Wetlands data.

This module handles the extraction of wetlands data from a WFS service.
It fetches raw data in chunks, processes it, and saves it to Google Cloud Storage
for further processing in the silver layer.

The module contains:
- WetlandsBronzeConfig: Configuration class for the data source
- WetlandsBronze: Implementation class for fetching and processing data

The data is fetched in parallel batches to optimize performance, with proper
error handling and retry logic for robustness.
"""

import asyncio
import ssl
import xml.etree.ElementTree as ET
from asyncio import Semaphore
from typing import Optional

import aiohttp
from pydantic import ConfigDict
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from unified_pipeline.common.base import BaseJobConfig, BaseSource
from unified_pipeline.util.gcs_util import GCSUtil
from unified_pipeline.util.timing import AsyncTimer


class WetlandsBronzeConfig(BaseJobConfig):
    """
    Configuration for the Wetlands Bronze source.

    This class defines all configuration parameters needed for fetching wetlands
    data from the Danish WFS (Web Feature Service). It includes endpoint URL, dataset name,
    performance tuning parameters, and request configuration.

    Attributes:
        name (str): Human-readable name of the data source
        dataset (str): Name of the dataset in storage
        type (str): Type of the data source (wfs)
        description (str): Brief description of the data
        url (str): URL for fetching wetlands data
        frequency (str): How often the data is updated
        bucket (str): GCS bucket name for raw data storage
        batch_size (int): Number of records to fetch in each request
        max_concurrent (int): Maximum number of concurrent requests
        request_timeout (int): Timeout for requests in seconds
        storage_batch_size (int): Batch size for storage operations
        request_timeout_config (aiohttp.ClientTimeout): Request timeout configuration
        headers (dict[str, str]): HTTP headers for WFS requests
        request_semaphore (Semaphore): Semaphore to limit concurrent requests
    """

    name: str = "Danish Wetlands Map"
    dataset: str = "wetlands"
    type: str = "wfs"
    description: str = "Wetland areas from Danish EPA"
    url: str = "https://wfs2-miljoegis.mim.dk/natur/wfs"
    frequency: str = "static"
    bucket: str = "landbrugsdata-raw-data"

    batch_size: int = 10000
    max_concurrent: int = 3
    request_timeout: int = 300
    storage_batch_size: int = 5000
    request_timeout_config: aiohttp.ClientTimeout = aiohttp.ClientTimeout(
        total=request_timeout, connect=60, sock_read=300
    )
    headers: dict[str, str] = {"User-Agent": "Mozilla/5.0 QGIS/33603/macOS 15.1"}
    request_semaphore: Semaphore = Semaphore(max_concurrent)

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class WetlandsBronze(BaseSource[WetlandsBronzeConfig]):
    """
    Bronze layer processing for wetlands data.

    This class is responsible for fetching raw wetlands data from the WFS service.
    It handles pagination, parallel fetching, and error handling, and stores the
    raw data in Google Cloud Storage for further processing in the silver layer.

    The class implements retry logic for resilience against transient failures and uses
    semaphores to control the number of concurrent requests to avoid overwhelming the API.

    Processing flow:
    1. Fetch initial data chunk to determine total number of features
    2. Fetch remaining data in parallel batches based on configuration
    3. Save raw XML responses to Google Cloud Storage
    """

    def __init__(self, config: WetlandsBronzeConfig, gcs_util: GCSUtil):
        """
        Initialize the WetlandsBronze source.

        Args:
            config (WetlandsBronzeConfig): Configuration for the data source
            gcs_util (GCSUtil): Utility for Google Cloud Storage operations
        """
        super().__init__(config, gcs_util)

    def _get_params(self, start_index: int = 0) -> dict:
        """
        Generate WFS request parameters.

        This method creates a dictionary of parameters needed for a WFS GetFeature request,
        including pagination information for fetching data in chunks.

        Args:
            start_index (int, optional): Starting index for the batch of features to fetch.
                                        Defaults to 0.

        Returns:
            dict: Dictionary of WFS request parameters
        """
        return {
            "SERVICE": "WFS",
            "REQUEST": "GetFeature",
            "VERSION": "2.0.0",
            "TYPENAMES": "natur:kulstof2022",
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
        """
        Fetch a chunk of features with retry logic.

        This method retrieves a batch of features from the WFS service starting at the specified
        index. It implements exponential backoff retry logic using the tenacity library to handle
        transient failures. The method is designed to be used in parallel for efficient data
        retrieval.

        Args:
            session (aiohttp.ClientSession): HTTP session for making requests
            start_index (int): Starting index for the batch of features to fetch

        Returns:
            dict: Dictionary containing the text response, start index, total features count,
                  and number of returned features

        Raises:
            Exception: If the API request fails after all retry attempts or if XML parsing fails

        Note:
            The method uses a semaphore to control the number of concurrent requests
            to avoid overwhelming the service.
        """
        async with (
            self.config.request_semaphore,
            AsyncTimer(
                f"Fetching chunk starting at index {start_index} to {start_index + self.config.batch_size}"
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
        """
        Fetch all raw data from the WFS service.

        This method orchestrates the data retrieval workflow:
        1. Establishes an HTTP session with proper SSL and header configuration
        2. Fetches the first chunk to determine total features available
        3. Fetches remaining data in parallel chunks using _fetch_chunck method
        4. Processes all responses and returns them as a list of XML strings

        Returns:
            Optional[list[str]]: List of XML responses, or None if fetching fails

        Raises:
            Exception: If there are issues with data fetching, parsing, or processing

        Note:
            This method disables SSL certificate verification to avoid issues with
            certain endpoint configurations. Use with trusted endpoints only.
        """

        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        connector = aiohttp.TCPConnector(ssl=ssl_context)
        raw_features = []
        async with (
            aiohttp.ClientSession(headers=self.config.headers, connector=connector) as session,
            AsyncTimer("Fetching raw data for Wetlands bronze job"),
        ):
            try:
                raw_data = await self._fetch_chunck(session, 0)
                total_features = raw_data["total_features"]
                returned_features = raw_data["returned_features"]
                raw_features.append(raw_data["text"])
                fetched_features_count = returned_features
                self.log.info(f"Total features to fetch: {total_features:,}")
                self.log.debug(f"Fetched {fetched_features_count} out of {total_features}")

                # Create a list of tasks for all remaining chunks to fetch
                tasks = []
                for start_index in range(returned_features, total_features, self.config.batch_size):
                    tasks.append(self._fetch_chunck(session, start_index))

                # Fetch all chunks in parallel using asyncio.gather
                results = await asyncio.gather(*tasks, return_exceptions=True)

                # Process the results
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

    async def run(self) -> None:
        """
        Run the data source processing pipeline.

        This method orchestrates the entire data retrieval process:
        1. Fetches all raw wetlands data from the WFS service
        2. Saves the retrieved XML data to Google Cloud Storage
        3. Logs the process stages and completion status

        Returns:
            None

        Note:
            This is the main entry point for the bronze layer processing of wetlands data.
        """
        async with AsyncTimer("Running Wetlands bronze job for"):
            self.log.info("Running Wetlands bronze job")
            raw_data = await self._fetch_raw_data()
            if raw_data is None:
                self.log.error("Failed to fetch raw data")
                return
            self.log.info("Fetched raw data successfully")
            self._save_raw_data(raw_data, self.config.dataset, self.config.name, self.config.bucket)
            self.log.info("Saved raw data successfully")
            self.log.info("Wetlands bronze job completed successfully")
