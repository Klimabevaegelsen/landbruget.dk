"""
Bronze layer data ingestion for Agricultural Fields data.

This module handles the extraction of agricultural fields data from an ArcGIS REST API.
It fetches raw data in chunks, processes it, and saves it to Google Cloud Storage
for further processing in the silver layer.

The module contains:
- AgriculturalFieldsBronzeConfig: Configuration class for the data source
- AgriculturalFieldsBronze: Implementation class for fetching and processing data

The data is fetched in parallel batches to optimize performance, with proper
error handling and retry logic for robustness.
"""

import asyncio
import json
import ssl
from asyncio import Semaphore

import aiohttp
from pydantic import ConfigDict
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from unified_pipeline.common.base import BaseJobConfig, BaseSource
from unified_pipeline.util.gcs_util import GCSUtil
from unified_pipeline.util.timing import AsyncTimer


class AgriculturalFieldsBronzeConfig(BaseJobConfig):
    """
    Configuration for the Agricultural Fields Bronze source.

    This class defines all configuration parameters needed for fetching agricultural fields
    data from the Danish ArcGIS REST API. It includes endpoint URLs, dataset names,
    performance tuning parameters, and request configuration.

    Attributes:
        name (str): Human-readable name of the data source
        type (str): Type of the data source (arcgis)
        description (str): Brief description of the data
        fields_url (str): URL for fetching agricultural fields data
        blocks_url (str): URL for fetching agricultural blocks data
        fields_dataset (str): Name of the fields dataset in storage
        blocks_dataset (str): Name of the blocks dataset in storage
        frequency (str): How often the data is updated
        bucket (str): GCS bucket name for raw data storage
        batch_size (int): Number of records to fetch in each request
        max_concurrent (int): Maximum number of concurrent requests
        storage_batch_size (int): Batch size for storage operations
        timeout_config (aiohttp.ClientTimeout): Request timeout configuration
        request_semaphore (Semaphore): Semaphore to limit concurrent requests
    """

    name: str = "Danish Agricultural Fields"
    type: str = "arcgis"
    description: str = "Weekly updated agricultural field data"
    fields_url: str = (
        "https://kort.vd.dk/server/rest/services/Grunddata/Marker_og_Markblokke/MapServer/13/query"
    )
    blocks_url: str = (
        "https://kort.vd.dk/server/rest/services/Grunddata/Marker_og_Markblokke/MapServer/6/query"
    )
    fields_dataset: str = "agricultural_fields"
    blocks_dataset: str = "agricultural_blocks"
    frequency: str = "weekly"
    bucket: str = "landbrugsdata-raw-data"

    batch_size: int = 20000
    max_concurrent: int = 5
    storage_batch_size: int = 10000

    timeout_config: aiohttp.ClientTimeout = aiohttp.ClientTimeout(
        total=1200, connect=60, sock_read=540
    )
    request_semaphore: Semaphore = Semaphore(max_concurrent)

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class AgriculturalFieldsBronze(BaseSource[AgriculturalFieldsBronzeConfig]):
    """
    Bronze layer processing for agricultural fields data.

    This class is responsible for fetching raw agricultural fields data from the ArcGIS API,
    including both field polygons and block polygons. It handles pagination, parallel fetching,
    and error handling, and stores the raw data in Google Cloud Storage for further processing.

    The class implements retry logic for resilience against transient failures and uses
    semaphores to control the number of concurrent requests to avoid overwhelming the API.

    Processing flow:
    1. Determine total record count from the API
    2. Fetch data in parallel batches based on configuration
    3. Save raw responses to Google Cloud Storage
    """

    def __init__(self, config: AgriculturalFieldsBronzeConfig, gcs_util: GCSUtil):
        """
        Initialize the AgriculturalFieldsBronze source.

        Args:
            config (AgriculturalFieldsBronzeConfig): Configuration for the data source
            gcs_util (GCSUtil): Utility for Google Cloud Storage operations
        """
        super().__init__(config, gcs_util)

    async def _get_total_count(self, session: aiohttp.ClientSession, url: str) -> int:
        """
        Get total number of features available from a specific endpoint.

        This method makes a request to the ArcGIS API to retrieve the total count
        of features (fields or blocks) available for fetching.

        Args:
            session (aiohttp.ClientSession): HTTP session for making requests
            url (str): URL of the ArcGIS endpoint to query

        Returns:
            int: Total number of features available

        Raises:
            Exception: If the API request fails or returns an error status
        """
        params = {"f": "json", "where": "1=1", "returnCountOnly": "true"}

        try:
            self.log.info(f"Fetching total count from {url}")
            async with (
                session.get(url, params=params) as response,
                AsyncTimer(f"Request total count from {url}"),
            ):
                if response.status == 200:
                    data = await response.json()
                    total = data.get("count", 0)
                    return int(total)
                else:
                    response_text = await response.text()
                    raise Exception(
                        f"Error getting count for {url}: {response.status} - {response_text}"
                    )
        except Exception as e:
            raise Exception(f"Error getting total count for {url}: {str(e)}")

    @retry(
        retry=retry_if_exception_type(Exception),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(5),
    )
    async def _fetch_chunk(self, session: aiohttp.ClientSession, url: str, start_index: int) -> str:
        """
        Fetch a chunk of features with retry logic.

        This method retrieves a batch of features from the ArcGIS API starting at the specified
        index. It implements exponential backoff retry logic using the tenacity library to handle
        transient failures. The method is designed to be used in parallel for efficient data
        retrieval.

        Args:
            session (aiohttp.ClientSession): HTTP session for making requests
            url (str): URL of the ArcGIS endpoint to query
            start_index (int): Starting index for the batch of features to fetch

        Returns:
            str: JSON string containing the fetched features

        Raises:
            Exception: If the API request fails after all retry attempts

        Note:
            The method uses a semaphore to control the number of concurrent requests
            and times the execution for performance monitoring.
        """
        params = {
            "f": "json",
            "where": "1=1",
            "returnGeometry": "true",
            "outFields": "*",
            "resultOffset": str(start_index),
            "resultRecordCount": str(self.config.batch_size),
        }

        async with (
            self.config.request_semaphore,
            AsyncTimer(
                f"Request chunk at index {start_index} to {start_index + self.config.batch_size}"
            ),
        ):
            self.log.debug(f"Fetching from URL: {url} with params: {params}")
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    return json.dumps(data)

                response_text = await response.text()
                err_msg = (
                    f"Error response {response.status} at index {start_index}. "
                    f"Response: {response_text[:500]}..."
                )
                self.log.error(err_msg)
                raise Exception(err_msg)

    async def _process_data(self, url: str, dataset: str) -> None:
        """
        Process data from the specified URL and save it to Google Cloud Storage.

        This method orchestrates the data retrieval workflow for a specific dataset:
        1. Establishes an HTTP session with proper SSL and timeout configuration
        2. Gets the total count of available features from the API
        3. Fetches data in parallel chunks using _fetch_chunk method
        4. Combines results and saves them to Google Cloud Storage

        Args:
            url (str): The URL of the ArcGIS endpoint to fetch data from
            dataset (str): The name of the dataset, used for logging and storage path

        Returns:
            None

        Raises:
            Exception: If there are issues with data fetching or processing

        Note:
            This method disables SSL certificate verification to avoid issues with
            certain endpoint configurations. Use with trusted endpoints only.
        """

        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        connector = aiohttp.TCPConnector(ssl=ssl_context)
        async with (
            aiohttp.ClientSession(
                timeout=self.config.timeout_config, connector=connector
            ) as session,
            AsyncTimer(f"Processing data for {dataset}"),
        ):
            total_count = await self._get_total_count(session, url)
            self.log.info(f"Total count: {total_count}")

            if total_count == 0:
                self.log.warning("No data to process.")
                return

            tasks = []
            for start_index in range(0, total_count, self.config.batch_size):
                tasks.append(self._fetch_chunk(session, url, start_index))

            raw_data = await asyncio.gather(*tasks)
            self.log.info(f"Saving data to GCS for {dataset}")
            self._save_raw_data(raw_data, dataset, self.config.name, self.config.bucket)
            self.log.info(f"Data processing completed for {dataset}")

    async def run(self) -> None:
        """
        Run the data source processing pipeline.

        This method orchestrates the entire data retrieval process:
        1. First processes agricultural fields data from the fields endpoint
        2. Then processes agricultural blocks data from the blocks endpoint
        3. Tracks overall execution time for performance monitoring

        The method uses the configured URLs and dataset names from the configuration object,
        and delegates the actual processing to the _process_data method for each dataset.

        Returns:
            None

        Note:
            This is the main entry point for the bronze layer processing of
            agricultural fields data.
        """
        self.log.info("Running Agricultural Fields bronze job")
        async with AsyncTimer("Total run time"):
            await self._process_data(self.config.fields_url, self.config.fields_dataset)
            await self._process_data(self.config.blocks_url, self.config.blocks_dataset)
            self.log.info("Agricultural Fields bronze job completed successfully")
