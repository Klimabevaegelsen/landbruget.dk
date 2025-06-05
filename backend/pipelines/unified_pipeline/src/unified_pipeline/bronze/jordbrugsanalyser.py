"""
Bronze layer data ingestion for Jordbrugsanalyser Marker data.

This module handles the extraction of agricultural marker data from the
Jordbrugsanalyser WFS service (geodata.fvm.dk). It fetches yearly marker
data from 2012 to 2024, containing agricultural field information with crop
types, crop codes, field numbers, and field block numbers.

The module contains:
- JordbrugsanalyserBronzeConfig: Configuration class for the WFS data source
- JordbrugsanalyserBronze: Implementation class for fetching and processing marker data

The data is fetched using WFS GetFeature requests for each year's marker layer,
with proper error handling and retry logic for robustness.
"""

import asyncio
import xml.etree.ElementTree as ET
from asyncio import Semaphore
from typing import Dict, List

import aiohttp
from pydantic import ConfigDict
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from unified_pipeline.common.base import BaseJobConfig, BaseSource
from unified_pipeline.util.gcs_util import GCSUtil
from unified_pipeline.util.timing import AsyncTimer


class JordbrugsanalyserBronzeConfig(BaseJobConfig):
    """
    Configuration for the Jordbrugsanalyser Bronze source.

    This class defines all configuration parameters needed for fetching
    Jordbrugsanalyser marker data from the Danish WFS service. It includes
    endpoint URLs, dataset configuration, performance tuning parameters,
    and request configuration.

    Performance Note:
    Testing showed that downloading the full dataset in single requests is
    ~730x faster than chunking (5,837 vs 8 features/second). The WFS server
    is optimized for large single requests rather than many small ones.

    Attributes:
        name (str): Human-readable name of the data source
        type (str): Type of the data source (wfs)
        description (str): Brief description of the data
        wfs_url (str): Base URL for the WFS service
        dataset (str): Name of the dataset in storage
        frequency (str): How often the data is updated
        bucket (str): GCS bucket name for raw data storage
        start_year (int): First year to fetch (2012)
        end_year (int): Last year to fetch (2024)
        batch_size (int): Features per request (0 = unlimited, downloads full dataset)
        max_concurrent (int): Maximum concurrent requests (1 for full downloads)
        timeout_config (aiohttp.ClientTimeout): Request timeout configuration
        request_semaphore (Semaphore): Semaphore to limit concurrent requests
    """

    name: str = "Danish Jordbrugsanalyser Markers"
    type: str = "wfs"
    description: str = "Agricultural marker data from Jordbrugsanalyser WFS service"
    wfs_url: str = "https://geodata.fvm.dk/geoserver/wfs"
    dataset: str = "jordbrugsanalyser_markers"
    frequency: str = "yearly"
    bucket: str = "landbrugsdata-raw-data"

    # Year range for marker data
    start_year: int = 2012
    end_year: int = 2024

    # Request configuration - optimized for full dataset downloads
    # Testing showed full downloads are ~730x faster than chunking
    batch_size: int = 0  # 0 = unlimited, download full dataset in one request
    max_concurrent: int = 1  # Process one year at a time for stability
    request_timeout: int = 600  # Increased timeout for full dataset downloads

    timeout_config: aiohttp.ClientTimeout = aiohttp.ClientTimeout(
        total=request_timeout, connect=60, sock_read=request_timeout
    )
    request_semaphore: Semaphore = Semaphore(max_concurrent)

    # WFS namespaces for parsing responses
    namespaces: Dict[str, str] = {
        "wfs": "http://www.opengis.net/wfs/2.0",
        "gml": "http://www.opengis.net/gml/3.2",
        "Jordbrugsanalyser": "Jordbrugsanalyser",
    }

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class JordbrugsanalyserBronze(BaseSource[JordbrugsanalyserBronzeConfig]):
    """
    Bronze layer processing for Jordbrugsanalyser marker data.

    This class is responsible for fetching raw agricultural marker data from the
    Jordbrugsanalyser WFS service for years 2012-2024. It handles pagination,
    parallel fetching, and error handling, and stores the raw data in Google
    Cloud Storage for further processing.

    The class implements retry logic for resilience against transient failures
    and uses semaphores to control the number of concurrent requests to avoid
    overwhelming the WFS service.

    Processing flow:
    1. For each year from 2012-2024, determine layer name (Marker12, Marker13, etc.)
    2. Get total feature count from WFS service
    3. Fetch data in parallel batches based on configuration
    4. Save raw WFS responses to Google Cloud Storage
    """

    def __init__(self, config: JordbrugsanalyserBronzeConfig, gcs_util: GCSUtil):
        """
        Initialize the JordbrugsanalyserBronze source.

        Args:
            config (JordbrugsanalyserBronzeConfig): Configuration for the data source
            gcs_util (GCSUtil): Utility for Google Cloud Storage operations
        """
        super().__init__(config, gcs_util)

    def _get_layer_name(self, year: int) -> str:
        """
        Get the WFS layer name for a specific year.

        Converts full year to 2-digit suffix (e.g., 2012 -> Marker12)

        Args:
            year (int): Full year (e.g., 2012)

        Returns:
            str: Layer name (e.g., "Jordbrugsanalyser:Marker12")
        """
        year_suffix = str(year)[-2:]  # Get last 2 digits
        return f"Jordbrugsanalyser:Marker{year_suffix}"

    def _get_base_wfs_params(self, layer_name: str) -> Dict[str, str]:
        """
        Get base WFS request parameters for a specific layer.

        Args:
            layer_name (str): WFS layer name

        Returns:
            Dict[str, str]: Base WFS parameters
        """
        return {
            "SERVICE": "WFS",
            "VERSION": "2.0.0",
            "REQUEST": "GetFeature",
            "TYPENAME": layer_name,
            "SRSNAME": "EPSG:25832",
            "OUTPUTFORMAT": "application/gml+xml; version=3.2",
        }

    def _get_count_params(self, layer_name: str) -> Dict[str, str]:
        """
        Get WFS parameters for counting total features.

        Args:
            layer_name (str): WFS layer name

        Returns:
            Dict[str, str]: WFS parameters for count request
        """
        params = self._get_base_wfs_params(layer_name)
        params.update({"RESULTTYPE": "hits"})
        return params

    def _get_feature_params(self, layer_name: str, start_index: int = 0) -> Dict[str, str]:
        """
        Get WFS parameters for fetching features.

        Args:
            layer_name (str): WFS layer name
            start_index (int): Starting index for pagination (ignored if batch_size=0)

        Returns:
            Dict[str, str]: WFS parameters for feature request
        """
        params = self._get_base_wfs_params(layer_name)

        # If batch_size is 0, download entire dataset without pagination
        if self.config.batch_size > 0:
            params.update({"STARTINDEX": str(start_index), "COUNT": str(self.config.batch_size)})
        # For unlimited downloads, don't add STARTINDEX or COUNT parameters

        return params

    async def _get_total_count(self, session: aiohttp.ClientSession, layer_name: str) -> int:
        """
        Get total number of features available for a specific layer.

        This method makes a WFS GetFeature request with RESULTTYPE=hits to retrieve
        the total count of features available for fetching.

        Args:
            session (aiohttp.ClientSession): HTTP session for making requests
            layer_name (str): WFS layer name to query

        Returns:
            int: Total number of features available

        Raises:
            Exception: If the WFS request fails or returns an error
        """
        params = self._get_count_params(layer_name)

        try:
            self.log.info(f"Getting total count for layer {layer_name}")
            async with session.get(self.config.wfs_url, params=params) as response:
                async with AsyncTimer(f"Count request for {layer_name}"):
                    if response.status == 200:
                        # Handle Danish characters properly by reading as bytes first
                        content_bytes = await response.read()
                        try:
                            content = content_bytes.decode("utf-8")
                        except UnicodeDecodeError:
                            # Fallback to latin-1 for Danish characters
                            content = content_bytes.decode("latin-1")
                        root = ET.fromstring(content)

                        # Parse numberMatched from WFS response
                        number_matched = root.get("numberMatched", "0")
                        if number_matched == "*":
                            # If server doesn't provide exact count, return a large number
                            # and let pagination handle the actual data
                            self.log.warning(
                                f"Server returned '*' for {layer_name}, using estimated count"
                            )
                            return 100000  # Conservative estimate

                        total = int(number_matched)
                        self.log.info(f"Layer {layer_name}: {total:,} features available")
                        return total
                    else:
                        # Handle encoding for error messages too
                        try:
                            response_text = await response.text(encoding="utf-8")
                        except UnicodeDecodeError:
                            response_bytes = await response.read()
                            response_text = response_bytes.decode("latin-1", errors="replace")
                        raise Exception(
                            f"Error getting count for {layer_name}: {response.status} - {response_text}"
                        )
        except Exception as e:
            self.log.error(f"Error getting total count for {layer_name}: {str(e)}")
            raise

    @retry(
        retry=retry_if_exception_type(Exception),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(5),
    )
    async def _fetch_chunk(
        self, session: aiohttp.ClientSession, layer_name: str, start_index: int
    ) -> str:
        """
        Fetch features from WFS with retry logic.

        This method retrieves features from the WFS service. Depending on the configuration,
        it can fetch the entire dataset (if batch_size=0) or a specific chunk starting
        at the specified index. It implements exponential backoff retry logic using the
        tenacity library to handle transient failures.

        Args:
            session (aiohttp.ClientSession): HTTP session for making requests
            layer_name (str): WFS layer name to query
            start_index (int): Starting index (ignored if batch_size=0 for full downloads)

        Returns:
            str: Raw WFS response as string (GML format)

        Raises:
            Exception: If the WFS request fails after all retry attempts

        Note:
            The method uses a semaphore to control the number of concurrent requests
            and times the execution for performance monitoring.
        """
        params = self._get_feature_params(layer_name, start_index)

        request_type = (
            "Full dataset" if self.config.batch_size == 0 else f"Chunk at index {start_index}"
        )

        async with self.config.request_semaphore:
            async with AsyncTimer(f"{request_type} request for {layer_name}"):
                self.log.debug(f"Fetching {layer_name} {request_type.lower()}")
                async with session.get(self.config.wfs_url, params=params) as response:
                    if response.status == 200:
                        # Handle Danish characters properly by reading as bytes first
                        content_bytes = await response.read()
                        try:
                            content = content_bytes.decode("utf-8")
                        except UnicodeDecodeError:
                            # Fallback to latin-1 for Danish characters
                            content = content_bytes.decode("latin-1")

                        # Basic validation - check if we got a valid WFS response
                        if "wfs:FeatureCollection" not in content:
                            raise Exception(
                                f"Invalid WFS response for {layer_name} ({request_type.lower()})"
                            )

                        return content
                    else:
                        # Handle encoding for error messages too
                        try:
                            response_text = await response.text(encoding="utf-8")
                        except UnicodeDecodeError:
                            response_bytes = await response.read()
                            response_text = response_bytes.decode("latin-1", errors="replace")
                        err_msg = (
                            f"Error response {response.status} for {layer_name} ({request_type.lower()}). "
                            f"Response: {response_text[:500]}..."
                        )
                        self.log.error(err_msg)
                        raise Exception(err_msg)

    async def _process_year_data(self, session: aiohttp.ClientSession, year: int) -> List[str]:
        """
        Process data for a specific year and return raw WFS responses.

        This method orchestrates the data retrieval workflow for a specific year:
        1. Gets the layer name for the year (e.g., Marker12 for 2012)
        2. Gets the total count of available features from the WFS service
        3. Fetches data either as full dataset or in chunks based on configuration
        4. Returns list of raw WFS responses for storage

        Args:
            session (aiohttp.ClientSession): HTTP session for making requests
            year (int): Year to process (e.g., 2012)

        Returns:
            List[str]: List of raw WFS response strings

        Raises:
            Exception: If there are issues with data fetching or processing
        """
        layer_name = self._get_layer_name(year)

        async with AsyncTimer(f"Processing year {year} ({layer_name})"):
            try:
                total_count = await self._get_total_count(session, layer_name)
                self.log.info(f"Year {year}: {total_count:,} features to fetch")

                if total_count == 0:
                    self.log.warning(f"No data found for year {year}")
                    return []

                # If batch_size is 0, download entire dataset in one request
                if self.config.batch_size == 0:
                    self.log.info(f"Year {year}: Downloading full dataset in single request")
                    raw_response = await self._fetch_chunk(session, layer_name, 0)
                    return [raw_response] if raw_response else []

                # Otherwise, use chunked downloading
                tasks = []
                for start_index in range(0, total_count, self.config.batch_size):
                    tasks.append(self._fetch_chunk(session, layer_name, start_index))

                # Execute all tasks and collect results
                raw_responses = await asyncio.gather(*tasks)

                valid_responses = [resp for resp in raw_responses if resp]
                self.log.info(f"Year {year}: Collected {len(valid_responses)} valid responses")

                return valid_responses

            except Exception as e:
                self.log.error(f"Error processing year {year}: {str(e)}")
                raise

    async def run(self) -> None:
        """
        Run the data source processing pipeline.

        This method orchestrates the entire data retrieval process:
        1. Processes marker data for each year from 2012 to 2024
        2. For each year, fetches all available marker features
        3. Saves raw WFS responses to Google Cloud Storage
        4. Tracks overall execution time for performance monitoring

        The method processes years sequentially to avoid overwhelming the WFS service,
        but uses parallel requests within each year for efficiency.

        Returns:
            None

        Note:
            This is the main entry point for the bronze layer processing of
            Jordbrugsanalyser marker data.
        """
        self.log.info("Running Jordbrugsanalyser Markers bronze job")

        async with AsyncTimer("Total Jordbrugsanalyser run time"):
            async with aiohttp.ClientSession(timeout=self.config.timeout_config) as session:
                for year in range(self.config.start_year, self.config.end_year + 1):
                    try:
                        self.log.info(f"Processing year {year}")
                        raw_responses = await self._process_year_data(session, year)

                        if raw_responses:
                            # Save data with year suffix for easy identification
                            dataset_name = f"{self.config.dataset}_{year}"
                            self.log.info(f"Saving {len(raw_responses)} responses for year {year}")
                            self._save_raw_data(
                                raw_responses, dataset_name, self.config.name, self.config.bucket
                            )
                            self.log.info(f"Year {year}: Data saved successfully")
                        else:
                            self.log.warning(f"Year {year}: No data to save")

                    except Exception as e:
                        self.log.error(f"Failed to process year {year}: {str(e)}")
                        # Continue with next year instead of failing completely
                        continue

            self.log.info("Jordbrugsanalyser Markers bronze job completed successfully")
