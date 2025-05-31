"""
Bronze layer data ingestion for Water Projects data.

This module handles the extraction of water projects data from Danish environmental services.
It supports fetching data from WFS (Web Feature Service) and ArcGIS REST API services.
It fetches raw data in chunks, processes it, and saves it to Google Cloud Storage
for further processing in the silver layer.

The module contains:
- WaterProjectsBronzeConfig: Configuration class for the data source
- WaterProjectsBronze: Implementation class for fetching and processing data

The data is fetched in parallel batches to optimize performance, with proper
error handling and retry logic for robustness.
"""

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


class WaterProjectsBronzeConfig(BaseJobConfig):
    """
    Configuration class for the Water Projects Bronze job.

    This configuration defines parameters for fetching water project data from various
    Danish environmental services including WFS (Web Feature Service) and ArcGIS endpoints.
    The job collects data from multiple layers related to water projects, hydrology,
    wetlands, and climate adaptation projects.

    Attributes:
        name: Human-readable name of the data source
        dataset: Internal dataset identifier
        type: Service type identifier
        description: Brief description of the data source
        url: Default WFS service URL
        frequency: How often the job should run
        bucket: GCS bucket name for storing raw data
        batch_size: Number of features to fetch per request
        max_concurrent: Maximum number of concurrent HTTP requests
        request_timeout: HTTP request timeout in seconds
        storage_batch_size: Number of records to batch for storage operations
        request_timeout_config: aiohttp timeout configuration
        headers: HTTP headers to send with requests
        request_semaphore: Semaphore to limit concurrent requests
        layers: List of layer names to fetch from various services
        url_mapping: Mapping of specific layers to their service URLs
        service_types: Mapping of layers to their service types (wfs/arcgis)
    """

    name: str = "Danish Water Projects Map"
    dataset: str = "water_projects"
    type: str = "wfs"
    description: str = "Water projects from various Danish programs"
    url: str = "https://geodata.fvm.dk/geoserver/wfs"
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
    layers: list[str] = [
        "N2000_projekter:Hydrologi_E",
        "N2000_projekter:Hydrologi_F",
        "Ovrige_projekter:Vandloebsrestaurering_E",
        "Ovrige_projekter:Vandloebsrestaurering_F",
        "Vandprojekter:Fosfor_E_samlet",
        "Vandprojekter:Fosfor_F_samlet",
        "Vandprojekter:Kvaelstof_E_samlet",
        "Vandprojekter:Kvaelstof_F_samlet",
        "Vandprojekter:Lavbund_E_samlet",
        "Vandprojekter:Lavbund_F_samlet",
        "Vandprojekter:Private_vaadomraader",
        "Vandprojekter:Restaurering_af_aadale_2024",
        "vandprojekter:kla_projektforslag",
        "vandprojekter:kla_projektomraader",
        "Klima_lavbund_demarkation___offentlige_projekter:0",
    ]
    url_mapping: dict[str, str] = {
        "vandprojekter:kla_projektforslag": "https://wfs2-miljoegis.mim.dk/vandprojekter/wfs",
        "vandprojekter:kla_projektomraader": "https://wfs2-miljoegis.mim.dk/vandprojekter/wfs",
        "Klima_lavbund_demarkation___offentlige_projekter:0": "https://gis.nst.dk/server/rest/services/autonom/Klima_lavbund_demarkation___offentlige_projekter/FeatureServer",
    }
    service_types: dict[str, str] = {"Klima_lavbund_demarkation___offentlige_projekter:0": "arcgis"}

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class WaterProjectsBronze(BaseSource[WaterProjectsBronzeConfig]):
    """
    Bronze layer data processor for Danish water projects.

    This class handles the extraction of water project data from multiple Danish
    environmental data sources including WFS and ArcGIS services. It fetches raw
    geospatial data about water projects, hydrology, wetlands, and climate adaptation
    initiatives and stores them in a standardized format for further processing.

    The class supports:
    - WFS (Web Feature Service) data fetching with pagination
    - ArcGIS REST API data fetching
    - Concurrent processing with rate limiting
    - Error handling and retry mechanisms
    - Unicode handling for various data encodings

    Args:
        config: Configuration object containing job parameters
        gcs_util: Utility for Google Cloud Storage operations
    """

    def __init__(self, config: WaterProjectsBronzeConfig, gcs_util: GCSUtil):
        super().__init__(config, gcs_util)

    def _get_params(self, layer: str, start_index: int = 0) -> dict[str, str]:
        """
        Generate WFS GetFeature request parameters.

        Creates the parameter dictionary needed for WFS GetFeature requests,
        including pagination support through start index and count parameters.
        Uses EPSG:25832 (UTM Zone 32N) coordinate system which is standard
        for Danish geographic data.

        Args:
            layer: The layer name/type name to fetch features from
            start_index: Starting index for pagination (default: 0)

        Returns:
            Dictionary containing WFS request parameters
        """
        return {
            "SERVICE": "WFS",
            "REQUEST": "GetFeature",
            "VERSION": "2.0.0",
            "TYPENAMES": layer,
            "STARTINDEX": str(start_index),
            "COUNT": str(self.config.batch_size),
            "SRSNAME": "urn:ogc:def:crs:EPSG::25832",
        }

    @retry(
        retry=retry_if_exception_type(Exception),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(5),
    )
    async def _fetch_chunk(
        self, session: aiohttp.ClientSession, layer: str, url: str, start_index: int
    ) -> dict:
        """
        Fetch a single chunk of data from a WFS service.

        This method fetches a batch of features from a WFS service with automatic
        retry logic and error handling. It handles XML parsing, Unicode decoding
        issues, and extracts metadata about the total number of features available.

        The method uses a semaphore to limit concurrent requests and includes
        comprehensive error handling for network issues, HTTP errors, and XML
        parsing problems.

        Args:
            session: aiohttp client session for making HTTP requests
            layer: Layer name to fetch data from
            url: WFS service URL
            start_index: Starting index for this chunk (for pagination)

        Returns:
            Dictionary containing:
                - text: Raw XML response text
                - start_index: The starting index of this chunk
                - total_features: Total number of features available in the layer
                - returned_features: Number of features returned in this chunk

        Raises:
            Exception: If HTTP request fails, XML parsing fails, or other errors occur
        """
        async with (
            self.config.request_semaphore,
            AsyncTimer(
                f"Fetching chunk for layer {layer} starting at index {start_index} "
                f"to {start_index + self.config.batch_size}"
            ),
        ):
            params = self._get_params(layer, start_index)
            self.log.debug(
                f"Trying to fetch data from {url} for layer {layer} "
                f"with index {start_index} to {start_index + self.config.batch_size} "
                f"with params {params}"
            )
            try:
                async with session.get(url, params=params) as response:
                    if response.status != 200:
                        err_msg = f"Failed to fetch data. Status: {response.status}"
                        self.log.error(err_msg)
                        raise Exception(err_msg)

                    try:
                        text = await response.text()
                    except UnicodeDecodeError:
                        # Handle decoding errors by using 'replace' strategy
                        text = await response.text(errors="replace")
                        self.log.warning(
                            f"Handled Unicode decoding error in response for layer "
                            f"{layer} at index {start_index}"
                        )

                    root = ET.fromstring(text)
                    return {
                        "text": text,
                        "start_index": start_index,
                        "total_features": int(root.get("numberMatched", "0")),
                        "returned_features": int(root.get("numberReturned", "0")),
                    }
            except ET.ParseError as e:
                err_msg = f"Failed to parse XML response: {e}"
                self.log.error(err_msg, exc_info=True)
                raise Exception(err_msg)
            except Exception as e:
                err_msg = f"Error fetching chunk: {e}"
                self.log.error(err_msg, exc_info=True)
                raise Exception(err_msg)

    @retry(
        retry=retry_if_exception_type(Exception),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(5),
    )
    async def _fetch_arcgis_data(
        self, session: aiohttp.ClientSession, layer: str, url: str
    ) -> list[str]:
        """
        Fetch data from an ArcGIS REST API service.

        This method handles fetching geospatial data from ArcGIS REST API endpoints.
        It extracts the layer ID from the layer name, constructs the appropriate
        query URL, and fetches all features using the standard ArcGIS query endpoint.

        The method includes error handling for HTTP failures and Unicode decoding
        issues. It fetches all features in a single request since ArcGIS services
        typically handle large datasets efficiently.

        Args:
            session: aiohttp client session for making HTTP requests
            layer: Layer name in format "layername:id" (e.g., "project:0")
            url: Base ArcGIS FeatureServer URL

        Returns:
            List containing the raw JSON response text from the ArcGIS service

        Raises:
            Exception: If HTTP request fails or other errors occur
        """
        async with (
            self.config.request_semaphore,
            AsyncTimer(f"Fetching ArcGIS data for layer {layer}"),
        ):
            self.log.debug(f"Fetching data for layer {layer} from {url}")
            # Extract layer ID from the full layer string
            # This will get "0" from "Klima_lavbund_demarkation___offentlige_projekter:0"
            layer_id = layer.split(":")[1]

            params = {
                "f": "json",
                "where": "1=1",
                "outFields": "*",
                "geometryPrecision": "6",
                "outSR": "25832",
                "returnGeometry": "true",
            }
            try:
                fetch_url = f"{url}/{layer_id}/query"
                self.log.debug(f"Fetching data from {fetch_url} with params {params}")
                async with session.get(fetch_url, params=params) as response:
                    if response.status != 200:
                        response_err = await response.text()
                        err_msg = f"Failed to fetch ArcGIS data. Status: {response.status}, "
                        f"Error: {response_err[:500]}"
                        self.log.error(err_msg)
                        raise Exception(err_msg)

                    json_data = await response.json()  # Just check if this is a valid JSON response
                    total_features = len(json_data.get("features", []))
                    self.log.info(f"Total features fetched  ArcGIS: {total_features}")

                    try:
                        response_text = await response.text()
                    except UnicodeDecodeError:
                        # Handle decoding errors by using 'replace' strategy
                        self.log.warning(
                            f"Unicode decode error when reading response text "
                            f"for ArcGIS layer {layer}. Using 'replace' strategy."
                        )
                        response_text = await response.text(errors="replace")

                    return [response_text]
            except Exception as e:
                err_msg = f"Error fetching data: {e}"
                self.log.error(err_msg)
                raise Exception(err_msg)

    async def _fetch_wfs_data(
        self, session: aiohttp.ClientSession, layer: str, url: str
    ) -> list[str]:
        """
        Fetch all data from a WFS service using pagination.

        This method orchestrates the fetching of all features from a WFS layer
        by first fetching an initial chunk to determine the total number of features,
        then creating concurrent tasks to fetch all remaining chunks in parallel.

        The method implements efficient parallel processing while respecting
        rate limits through semaphores. It handles large datasets by breaking
        them into manageable chunks and processing them concurrently.

        Args:
            session: aiohttp client session for making HTTP requests
            layer: Layer name to fetch data from
            url: WFS service URL

        Returns:
            List of raw XML response texts, one for each fetched chunk

        Raises:
            Exception: If any chunk fetch fails or other errors occur
        """
        raw_features = []
        self.log.info(f"Fetching WFS data for layer: {layer} from {url}")
        async with AsyncTimer(f"Fetching WFS data for layer {layer}"):
            raw_data = await self._fetch_chunk(session, layer, url, 0)
            total_features = raw_data["total_features"]
            returned_features = raw_data["returned_features"]
            raw_features.append(raw_data["text"])
            fetched_features_count = returned_features
            self.log.info(f"Fetched {fetched_features_count} out of {total_features}")

            # Create a list of tasks for all remaining chunks to fetch
            tasks = []
            for start_index in range(returned_features, total_features, self.config.batch_size):
                tasks.append(self._fetch_chunk(session, layer, url, start_index))

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
                    self.log.debug(f"Processed chunk with {result['returned_features']} features")
                else:
                    self.log.error(f"Unexpected result type: {type(result)}")

            self.log.info(f"Fetched all {fetched_features_count} out of {total_features} features")
            return raw_features

    async def _fetch_raw_data(self) -> Optional[list[tuple[str, str]]]:
        """
        Fetch raw data from all configured layers and services.

        This is the main orchestration method that fetches data from all configured
        layers across multiple service types (WFS and ArcGIS). It handles different
        service types appropriately and aggregates all raw data with layer metadata.

        The method:
        - Sets up SSL context for HTTPS requests
        - Iterates through all configured layers
        - Determines the appropriate service type and URL for each layer
        - Calls the appropriate fetch method (WFS or ArcGIS)
        - Aggregates all raw data with layer identification

        Returns:
            List of tuples where each tuple contains:
                - layer name (str): Identifier of the source layer
                - raw data (str): Raw XML/JSON response text
            Returns None if no data was fetched from any layer

        Raises:
            Exception: If any layer fetch fails critically
        """
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        connector = aiohttp.TCPConnector(ssl=ssl_context)
        raw_features = []
        async with (
            aiohttp.ClientSession(headers=self.config.headers, connector=connector) as session,
            AsyncTimer("Fetching raw data for Water Projects bronze job"),
        ):
            for layer in self.config.layers:
                self.log.info(f"Fetching data for layer: {layer}")
                url = self.config.url_mapping.get(layer, self.config.url)
                service_type = self.config.service_types.get(layer, "wfs")
                try:
                    if service_type == "arcgis":
                        raw_data = await self._fetch_arcgis_data(session, layer, url)
                    else:
                        raw_data = await self._fetch_wfs_data(session, layer, url)
                    if not raw_data:
                        self.log.warning(f"No data fetched for layer: {layer}")
                        continue
                    self.log.info(f"Fetched {len(raw_data)} features for layer: {layer}")
                    raw_data_with_metadata = [
                        (layer, data) for data in raw_data
                    ]  # Add layer as metadata
                    raw_features.extend(raw_data_with_metadata)
                except Exception as e:
                    self.log.error(f"Error occured while fetching chunk: {e}")
                    raise e
        if not raw_features:
            self.log.warning("No raw features fetched")
            return None
        self.log.info(f"Total raw features fetched: {len(raw_features)}")
        return raw_features

    def create_dataframe(self, raw_data: list[tuple[str, str]]) -> pd.DataFrame:
        """
        Create a DataFrame from the raw data.
        This method takes a list of tuples and converts it into a pandas DataFrame.

        Args:
            raw_data (list[tuple[str, str]]): List of tuples containing layer and feature data.

        Returns:
            pd.DataFrame: DataFrame containing the raw data with metadata.
        """
        df = pd.DataFrame(
            {
                "payload": [data[1] for data in raw_data],
                "layer": [data[0] for data in raw_data],
            }
        )
        df["source"] = self.config.name
        df["created_at"] = pd.Timestamp.now()
        df["updated_at"] = pd.Timestamp.now()
        return df

    async def run(self) -> None:
        """
        Execute the complete Water Projects bronze job workflow.

        This is the main entry point for the bronze job that orchestrates the
        entire data extraction and storage process. The method:

        1. Fetches raw data from all configured layers and services
        2. Validates that data was successfully retrieved
        3. Creates a standardized DataFrame with metadata
        4. Saves the raw data to Google Cloud Storage
        5. Logs the completion status

        The method includes comprehensive timing and logging to track job
        performance and troubleshoot any issues.

        Raises:
            Exception: If critical errors occur during data fetching or storage
        """
        async with AsyncTimer("Running Water Projects bronze job for"):
            self.log.info("Running Water Projects bronze job")
            raw_data = await self._fetch_raw_data()
            if not raw_data:
                self.log.error("No raw data fetched")
                return
            self.log.info("Fetched raw data successfully")
            df = self.create_dataframe(raw_data)
            self._save_raw_data(df, self.config.dataset, self.config.bucket)
            self.log.info("Saved raw data successfully")
            self.log.info("Water Projects bronze job completed successfully")
