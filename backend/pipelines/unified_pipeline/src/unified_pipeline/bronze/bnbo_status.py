import asyncio
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
from unified_pipeline.util.timing import AsyncTimer


class BNBOStatusBronzeConfig(BaseJobConfig):
    """
    Configuration for BNBO (Boringsnære Beskyttelsesområder) status data source in the bronze layer.

    This class defines all the configuration parameters needed for fetching BNBO status data
    from the WFS service and storing it in the bronze layer of the data pipeline. It includes
    parameters for the data source, connection settings, and processing options.

    Attributes:
        name (str): Human-readable name for the data source.
        dataset (str): Machine name for the dataset, used in storage paths.
        type (str): Type of the data source, in this case "wfs" for Web Feature Service.
        description (str): A brief description of the data source.
        url (str): The endpoint URL for the WFS service.
        frequency (str): How often the data should be updated.
        bucket (str): Google Cloud Storage bucket name for storing the data.
        batch_size (int): Number of features to fetch in a single request.
        max_concurrent (int): Maximum number of concurrent requests.
        request_timeout (int): Timeout for HTTP requests in seconds.
        storage_batch_size (int): Batch size for storage operations.
        request_timeout_config (aiohttp.ClientTimeout): Detailed timeout configuration for aiohttp.
        headers (dict): HTTP headers to send with requests.
        request_semaphore (Semaphore): Semaphore to limit concurrent requests.
    """

    name: str = "Danish BNBO Status"
    dataset: str = "bnbo_status"
    type: str = "wfs"
    description: str = "Municipal status for well-near protection areas (BNBO)"
    url: str = "https://arealeditering-dist-geo.miljoeportal.dk/geoserver/wfs"
    frequency: str = "weekly"
    bucket: str = "landbrugsdata-raw-data"

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
    """
    Bronze layer processor for BNBO status data.

    This class is responsible for fetching BNBO (Boringsnære Beskyttelsesområder)
    status data from a WFS service and storing it in the bronze layer of the data
    pipeline. It handles HTTP requests, pagination, error handling, and data storage.

    Attributes:
        config (BNBOStatusBronzeConfig): Configuration object containing settings for the processor.
    """

    def __init__(self, config: BNBOStatusBronzeConfig, gcs_util: GCSUtil):
        """
        Initialize the BNBOStatusBronze processor.

        Args:
            config (BNBOStatusBronzeConfig): Configuration object for the processor.
            gcs_util (GCSUtil): Utility for interacting with Google Cloud Storage.
        """
        super().__init__(config, gcs_util)

    def _get_params(self, start_index: int = 0) -> dict:
        """
        Get WFS request parameters for pagination.

        Constructs a dictionary of query parameters to use when requesting data
        from the WFS service. These parameters specify the dataset, pagination,
        and spatial reference system.

        Args:
            start_index (int, optional): The starting index for pagination. Defaults to 0.

        Returns:
            dict: A dictionary of WFS request parameters.

        Example:
            >>> self._get_params(100)
            {
                'SERVICE': 'WFS',
                'REQUEST': 'GetFeature',
                'VERSION': '2.0.0',
                'TYPENAMES': 'dai:status_bnbo',
                'STARTINDEX': '100',
                'COUNT': '100',
                'SRSNAME': 'urn:ogc:def:crs:EPSG::25832'
            }
        """
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
        """
        Fetch a chunk of data from the WFS service.

        Makes an HTTP request to the WFS service to retrieve a subset of features
        starting from the specified index. This method uses retry logic to handle
        transient connection issues.

        Args:
            session (aiohttp.ClientSession): The HTTP session to use for the request.
            start_index (int): The starting index for pagination.

        Returns:
            dict: A dictionary containing the raw XML text, pagination information,
                 and feature counts.

        Raises:
            Exception: If the HTTP request fails or if the response cannot be parsed.

        Note:
            This method is decorated with retry logic to handle transient failures.
            It will retry up to 5 times with exponential backoff between 4 and 10 seconds.
        """
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
        Fetch all available BNBO status data from the WFS service.

        This method sets up an HTTP session with the appropriate SSL context,
        then fetches data in batches (chunks) until all available features have
        been retrieved. It handles pagination and collects all raw XML responses.

        Returns:
            Optional[list[str]]: A list of XML strings, each containing a chunk of data,
                               or None if the fetching process fails.

        Raises:
            Exception: If there are errors during the data fetching process.

        Note:
            This method disables SSL certificate verification because some
            government services might use self-signed certificates or have
            certificate issues.
        """
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        connector = aiohttp.TCPConnector(ssl=ssl_context)
        raw_features = []
        async with (
            aiohttp.ClientSession(headers=self.config.headers, connector=connector) as session,
            AsyncTimer("Fetching raw data from WFS service for BNBO Status"),
        ):
            try:
                raw_data = await self._fetch_chunck(session, 0)
                total_features = raw_data["total_features"]
                returned_features = raw_data["returned_features"]
                raw_features.append(raw_data["text"])
                fetched_features_count = returned_features
                self.log.info(f"Fetched {fetched_features_count} out of {total_features}")

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

    def create_dataframe(self, raw_data: list[str]) -> pd.DataFrame:
        """
        Create a DataFrame from the raw data.
        This method takes a list of strings and converts it into a pandas DataFrame.

        Args:
            raw_data (list[str]): List of strings.

        Returns:
            pd.DataFrame: DataFrame containing the raw data with metadata.
        """
        df = pd.DataFrame(
            {
                "payload": raw_data,
            }
        )
        df["source"] = self.config.name
        df["created_at"] = pd.Timestamp.now()
        df["updated_at"] = pd.Timestamp.now()
        return df

    async def run(self) -> None:
        """
        Run the complete BNBO status bronze layer job.

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
        async with AsyncTimer("Running BNBO Status bronze job for"):
            self.log.info("Running BNBO Status bronze job")
            raw_data = await self._fetch_raw_data()
            if not raw_data:
                self.log.error("No raw data fetched")
                return
            self.log.info("Fetched raw data successfully")
            df = self.create_dataframe(raw_data)
            self._save_raw_data(df, self.config.dataset, self.config.bucket)
            self.log.info("Saved raw data successfully")
            self.log.info("BNBO Status bronze job completed successfully")
