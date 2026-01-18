"""
Bronze layer data ingestion for GEUS Borehole Pesticides data.

This module handles the extraction of borehole locations and pesticide analyses
data from the GEUS Jupiter database WFS service. It fetches raw GML data in chunks,
processes it, and saves it to Google Cloud Storage for further processing in the
silver layer.

The module contains:
- GEUSBoreholePesticidesBronzeConfig: Configuration class for the data source
- GEUSBoreholePesticidesBronze: Implementation class for fetching and processing data

Data sources:
- jupiter_boringer_ws: Danish boreholes (442,000+ records)
- jupiter_anlaegsanalyser: Facility substance analyses (limited to 48 substances)
- mc_analyse: Full groundwater chemical analyses (stofgruppe 50 = pesticides)

The data is fetched in parallel batches to optimize performance, with proper
error handling and retry logic for robustness.
"""

import asyncio
import ssl
import xml.etree.ElementTree as ET
from asyncio import Semaphore
from typing import ClassVar

import aiohttp
import certifi
from pydantic import ConfigDict
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from unified_pipeline.common.base import BaseJobConfig, BaseSource, BronzeJobInterface
from unified_pipeline.util.timing import AsyncTimer


class GEUSBoreholePesticidesBronzeConfig(BaseJobConfig):
    """
    Configuration for the GEUS Borehole Pesticides Bronze source.

    This class defines all configuration parameters needed for fetching borehole
    and pesticide analysis data from the GEUS Jupiter WFS (Web Feature Service).
    It includes endpoint URL, dataset name, performance tuning parameters, and
    request configuration.

    Attributes:
        name (str): Human-readable name of the data source
        dataset (str): Name of the dataset in storage
        type (str): Type of the data source (wfs)
        description (str): Brief description of the data
        url (str): URL for fetching GEUS data
        frequency (str): How often the data is updated
        bucket (str): GCS bucket name for raw data storage
        source_crs (str): Coordinate reference system of source data
        boreholes_typename (str): WFS layer name for boreholes
        analyses_typename (str): WFS layer name for analyses
        batch_size (int): Number of records to fetch in each request
        max_concurrent (int): Maximum number of concurrent requests
        request_timeout (int): Timeout for requests in seconds
        storage_batch_size (int): Batch size for storage operations
        request_timeout_config (aiohttp.ClientTimeout): Request timeout configuration
        headers (dict[str, str]): HTTP headers for WFS requests
        request_semaphore (Semaphore): Semaphore to limit concurrent requests
    """

    name: str = "GEUS Borehole Pesticides"
    dataset: str = "geus_borehole_pesticides"
    type: str = "wfs"
    description: str = "Borehole locations and pesticide analyses from GEUS Jupiter"
    url: str = "https://data.geus.dk/geusmap/ows/25832.jsp"
    frequency: str = "monthly"
    bucket: str = "landbrugsdata-raw-data"
    source_crs: str = "EPSG:25832"

    # WFS layer names
    boreholes_typename: str = "jupiter_boringer_ws"
    analyses_typename: str = "jupiter_anlaegsanalyser"  # Facility analyses (48 substances)
    mc_analyse_typename: str = "mc_analyse"  # Full groundwater analyses (all substances)

    # Stofgruppe codes for filtering mc_analyse
    # 50 = "Pesticider, nedbrydningsprodukter og beslægtede stoffer"
    pesticide_stofgruppe: int = 50

    batch_size: int = 5000
    max_concurrent: int = 2
    request_timeout: int = 600
    storage_batch_size: int = 5000
    request_timeout_config: aiohttp.ClientTimeout = aiohttp.ClientTimeout(
        total=600, connect=60, sock_read=600
    )
    headers: ClassVar[dict[str, str]] = {"User-Agent": "Mozilla/5.0 QGIS/33603/macOS 15.1"}
    request_semaphore: Semaphore = Semaphore(2)

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class GEUSBoreholePesticidesBronze(
    BaseSource[GEUSBoreholePesticidesBronzeConfig], BronzeJobInterface
):
    """
    Bronze layer processing for GEUS borehole pesticides data.

    This class is responsible for fetching raw borehole and pesticide analysis data
    from the GEUS Jupiter WFS service. It handles pagination, parallel fetching, and
    error handling, and stores the raw data in Google Cloud Storage for further
    processing in the silver layer.

    The class implements retry logic for resilience against transient failures and uses
    semaphores to control the number of concurrent requests to avoid overwhelming the API.

    Processing flow:
    1. Fetch boreholes data with pagination
    2. Fetch analyses data with pagination
    3. Save raw GML responses to Google Cloud Storage
    """

    def __init__(self, config: GEUSBoreholePesticidesBronzeConfig):
        """
        Initialize the GEUSBoreholePesticidesBronze source.

        Args:
            config (GEUSBoreholePesticidesBronzeConfig): Configuration for the data source
        """
        super().__init__(config)

    def _get_params(
        self, typename: str, start_index: int = 0, cql_filter: str | None = None
    ) -> dict:
        """
        Generate WFS request parameters.

        This method creates a dictionary of parameters needed for a WFS GetFeature request,
        including pagination information for fetching data in chunks.

        Args:
            typename (str): The WFS layer name to fetch
            start_index (int, optional): Starting index for the batch of features to fetch.
                                        Defaults to 0.
            cql_filter (str, optional): CQL filter to apply to the request.

        Returns:
            dict: Dictionary of WFS request parameters
        """
        params = {
            "SERVICE": "WFS",
            "REQUEST": "GetFeature",
            "VERSION": "2.0.0",
            "TYPENAMES": typename,
            "STARTINDEX": str(start_index),
            "COUNT": str(self.config.batch_size),
            "SRSNAME": "urn:ogc:def:crs:EPSG::25832",
        }
        if cql_filter:
            params["CQL_FILTER"] = cql_filter
        return params

    @retry(
        retry=retry_if_exception_type(Exception),
        wait=wait_exponential(multiplier=1, min=4, max=30),
        stop=stop_after_attempt(5),
    )
    async def _fetch_chunk(
        self,
        session: aiohttp.ClientSession,
        typename: str,
        start_index: int,
        cql_filter: str | None = None,
    ) -> dict:
        """
        Fetch a chunk of features with retry logic.

        This method retrieves a batch of features from the WFS service starting at the
        specified index. It implements exponential backoff retry logic using the tenacity
        library to handle transient failures. The method is designed to be used in parallel
        for efficient data retrieval.

        Args:
            session (aiohttp.ClientSession): HTTP session for making requests
            typename (str): The WFS layer name to fetch
            start_index (int): Starting index for the batch of features to fetch
            cql_filter (str, optional): CQL filter to apply to the request

        Returns:
            dict: Dictionary containing the text response, start index, total features count,
                  and number of returned features

        Raises:
            Exception: If the API request fails after all retry attempts or if XML parsing fails

        Note:
            The method uses a semaphore to control the number of concurrent requests
            to avoid overwhelming the service.
        """
        filter_desc = f" (filter: {cql_filter})" if cql_filter else ""
        async with (
            self.config.request_semaphore,
            AsyncTimer(
                f"Fetching {typename}{filter_desc} chunk starting at index {start_index} "
                f"to {start_index + self.config.batch_size}"
            ),
        ):
            self.log.debug(
                f"Trying to fetch {typename} data from {start_index} to "
                f"{start_index + self.config.batch_size}"
            )
            params = self._get_params(typename, start_index, cql_filter)
            try:
                async with session.get(self.config.url, params=params) as response:
                    if response.status != 200:
                        err_msg = f"Failed to fetch {typename} data. Status: {response.status}"
                        self.log.error(err_msg)
                        raise Exception(err_msg)

                    text = await response.text()
                    try:
                        root = ET.fromstring(text)
                        # Handle numberMatched - GEUS WFS often returns "unknown"
                        number_matched_str = root.get("numberMatched", "0")
                        if number_matched_str.lower() == "unknown":
                            total_features = None  # Unknown total, will paginate until empty
                        else:
                            total_features = int(number_matched_str)

                        returned_features = int(root.get("numberReturned", "0"))

                        return {
                            "text": text,
                            "typename": typename,
                            "start_index": start_index,
                            "total_features": total_features,
                            "returned_features": returned_features,
                        }
                    except ET.ParseError as e:
                        err_msg = f"Failed to parse XML response for {typename}: {e}"
                        self.log.error(err_msg)
                        raise Exception(err_msg) from e
            except Exception as e:
                err_msg = f"Error fetching {typename} data: {e}"
                self.log.error(err_msg)
                raise Exception(err_msg) from e

    async def _fetch_layer_data(
        self, session: aiohttp.ClientSession, typename: str, cql_filter: str | None = None
    ) -> list[str]:
        """
        Fetch all data for a specific WFS layer.

        This method orchestrates the data retrieval for a single layer:
        1. Fetches the first chunk to determine total features available
        2. If total is known, fetches remaining data in parallel chunks
        3. If total is unknown, fetches sequentially until no more data
        4. Returns all responses as a list of GML strings

        Args:
            session (aiohttp.ClientSession): HTTP session for making requests
            typename (str): The WFS layer name to fetch
            cql_filter (str, optional): CQL filter to apply to the request

        Returns:
            list[str]: List of GML responses

        Raises:
            Exception: If there are issues with data fetching or processing
        """
        raw_features = []
        filter_desc = f" (filter: {cql_filter})" if cql_filter else ""

        async with AsyncTimer(f"Fetching all {typename}{filter_desc} data"):
            # Fetch first chunk to get total count
            first_chunk = await self._fetch_chunk(session, typename, 0, cql_filter)
            total_features = first_chunk["total_features"]
            returned_features = first_chunk["returned_features"]
            raw_features.append(first_chunk["text"])
            fetched_count = returned_features

            if total_features is None:
                # Total is unknown - fetch sequentially until we get fewer than batch_size
                self.log.info(
                    f"[{typename}]{filter_desc} Total features unknown, fetching until exhausted..."
                )
                self.log.debug(f"[{typename}] First chunk returned {fetched_count} features")

                current_index = returned_features
                while returned_features >= self.config.batch_size:
                    chunk = await self._fetch_chunk(session, typename, current_index, cql_filter)
                    returned_features = chunk["returned_features"]

                    if returned_features > 0:
                        raw_features.append(chunk["text"])
                        fetched_count += returned_features
                        self.log.debug(
                            f"[{typename}] Fetched chunk at index {current_index} "
                            f"with {returned_features} features (total so far: {fetched_count:,})"
                        )

                    current_index += self.config.batch_size

                self.log.info(
                    f"[{typename}] Fetched all {fetched_count:,} features (total was unknown)"
                )
            else:
                # Total is known - can fetch remaining chunks in parallel
                self.log.info(
                    f"[{typename}]{filter_desc} Total features to fetch: {total_features:,}"
                )
                self.log.debug(f"[{typename}] Fetched {fetched_count} out of {total_features}")

                # Create tasks for remaining chunks
                tasks = [
                    self._fetch_chunk(session, typename, start_index, cql_filter)
                    for start_index in range(
                        returned_features, total_features, self.config.batch_size
                    )
                ]

                if tasks:
                    results = await asyncio.gather(*tasks, return_exceptions=True)

                    for result in results:
                        if isinstance(result, Exception):
                            self.log.error(f"Error fetching {typename} chunk: {result}")
                            raise result

                        if isinstance(result, dict):
                            raw_features.append(result["text"])
                            fetched_count += result["returned_features"]
                            self.log.debug(
                                f"[{typename}] Processed chunk with {result['returned_features']} features"
                            )
                        else:
                            self.log.error(f"Unexpected result type: {type(result)}")

                self.log.info(
                    f"[{typename}] Fetched all {fetched_count:,} out of {total_features:,} features"
                )

        return raw_features

    async def _fetch_raw_data(self) -> dict[str, list[str]] | None:
        """
        Fetch all raw data from the GEUS WFS service.

        This method orchestrates the data retrieval workflow:
        1. Establishes an HTTP session with proper SSL and header configuration
        2. Fetches boreholes data
        3. Fetches analyses data
        4. Returns both datasets as a dictionary

        Returns:
            Optional[dict[str, list[str]]]: Dictionary with 'boreholes' and 'analyses' keys,
                                           each containing a list of GML strings,
                                           or None if fetching fails

        Raises:
            Exception: If there are issues with data fetching, parsing, or processing

        Note:
            Uses certifi package for proper SSL certificate verification to ensure
            secure connections to the GEUS WFS endpoint.
        """
        # Use certifi for proper SSL certificate verification
        # GEUS uses Let's Encrypt certificates which may not be in system trust store
        ssl_context = ssl.create_default_context(cafile=certifi.where())

        connector = aiohttp.TCPConnector(ssl=ssl_context)

        async with (
            aiohttp.ClientSession(
                headers=self.config.headers,
                connector=connector,
                timeout=self.config.request_timeout_config,
            ) as session,
            AsyncTimer("Fetching raw data for GEUS Borehole Pesticides bronze job"),
        ):
            try:
                # Fetch boreholes data
                self.log.info("Fetching boreholes data...")
                boreholes_data = await self._fetch_layer_data(
                    session, self.config.boreholes_typename
                )

                # Fetch facility analyses data (jupiter_anlaegsanalyser - 48 substances)
                self.log.info("Fetching facility analyses data (jupiter_anlaegsanalyser)...")
                facility_analyses_data = await self._fetch_layer_data(
                    session, self.config.analyses_typename
                )

                # Fetch full groundwater analyses filtered by stofgruppe 50 (pesticides)
                # mc_analyse contains ALL groundwater chemical analyses with full substance list
                pesticide_filter = f"stofgruppe={self.config.pesticide_stofgruppe}"
                self.log.info(
                    f"Fetching full pesticide analyses data (mc_analyse, {pesticide_filter})..."
                )
                pesticide_analyses_data = await self._fetch_layer_data(
                    session, self.config.mc_analyse_typename, cql_filter=pesticide_filter
                )

                return {
                    "boreholes": boreholes_data,
                    "analyses": facility_analyses_data,  # Legacy name for compatibility
                    "pesticide_analyses": pesticide_analyses_data,  # New: full pesticide data
                }

            except Exception as e:
                self.log.error(f"Error occurred while fetching data: {e}")
                raise e

    def create_dataframe(self, raw_data: dict[str, list[str]]) -> str:
        """
        Create a DuckDB table from the raw data.

        This method takes a dictionary of layer data and converts it into a DuckDB table
        with separate payload columns for boreholes, facility analyses, and pesticide analyses.

        Args:
            raw_data (dict[str, list[str]]): Dictionary with 'boreholes', 'analyses',
                                             and 'pesticide_analyses' keys

        Returns:
            str: Table name containing the raw data with metadata.
        """
        current_timestamp = self.conn.execute("SELECT current_timestamp").fetchone()[0]

        # Create tables for each layer
        self.conn.execute("CREATE OR REPLACE TABLE temp_boreholes (payload VARCHAR)")
        self.conn.execute("CREATE OR REPLACE TABLE temp_analyses (payload VARCHAR)")
        self.conn.execute("CREATE OR REPLACE TABLE temp_pesticide_analyses (payload VARCHAR)")

        # Insert boreholes data
        for data_str in raw_data["boreholes"]:
            self.conn.execute("INSERT INTO temp_boreholes VALUES (?)", [data_str])

        # Insert facility analyses data (jupiter_anlaegsanalyser)
        for data_str in raw_data["analyses"]:
            self.conn.execute("INSERT INTO temp_analyses VALUES (?)", [data_str])

        # Insert pesticide analyses data (mc_analyse filtered by stofgruppe=50)
        for data_str in raw_data.get("pesticide_analyses", []):
            self.conn.execute("INSERT INTO temp_pesticide_analyses VALUES (?)", [data_str])

        # Create final combined table with metadata
        self.conn.execute(
            """
            CREATE OR REPLACE TABLE final_dataframe AS
            SELECT
                'boreholes' as layer_type,
                payload,
                ? as source,
                ? as source_crs,
                ? as created_at,
                ? as updated_at
            FROM temp_boreholes
            UNION ALL
            SELECT
                'analyses' as layer_type,
                payload,
                ? as source,
                ? as source_crs,
                ? as created_at,
                ? as updated_at
            FROM temp_analyses
            UNION ALL
            SELECT
                'pesticide_analyses' as layer_type,
                payload,
                ? as source,
                ? as source_crs,
                ? as created_at,
                ? as updated_at
            FROM temp_pesticide_analyses
        """,
            [
                self.config.name,
                self.config.source_crs,
                current_timestamp,
                current_timestamp,
                self.config.name,
                self.config.source_crs,
                current_timestamp,
                current_timestamp,
                self.config.name,
                self.config.source_crs,
                current_timestamp,
                current_timestamp,
            ],
        )

        # Clean up temporary tables
        self.conn.execute("DROP TABLE temp_boreholes")
        self.conn.execute("DROP TABLE temp_analyses")
        self.conn.execute("DROP TABLE temp_pesticide_analyses")

        return "final_dataframe"

    async def run(self) -> dict[str, list[str]] | None:
        """
        Run the data source processing pipeline.

        This method orchestrates the entire data retrieval process:
        1. Fetches all raw borehole and analyses data from the WFS service
        2. Saves the retrieved GML data to Google Cloud Storage
        3. Returns the raw data for in-memory passing to silver stage

        Returns:
            Optional[dict[str, list[str]]]: Raw GML data that can be passed to silver stage,
                                           or None if processing fails

        Note:
            This is the main entry point for the bronze layer processing of GEUS data.
        """
        async with AsyncTimer("Running GEUS Borehole Pesticides bronze job"):
            self.log.info("Running GEUS Borehole Pesticides bronze job")

            # Set source CRS for tracking
            self.set_source_crs(self.config.source_crs)

            raw_data = await self._fetch_raw_data()
            if not raw_data:
                self.log.error("No raw data fetched")
                return None

            self.log.info(
                f"Fetched raw data successfully: "
                f"{len(raw_data['boreholes'])} borehole chunks, "
                f"{len(raw_data['analyses'])} facility analyses chunks, "
                f"{len(raw_data.get('pesticide_analyses', []))} pesticide analyses chunks"
            )

            # Create dataframe for storage
            table_name = self.create_dataframe(raw_data)

            # Save using unified method
            self._save_data(table_name, self.config.dataset, self.config.bucket, stage="bronze")
            self.log.info("Saved raw data successfully")
            self.log.info("GEUS Borehole Pesticides bronze job completed successfully")

            # Return raw data for in-memory passing
            return raw_data
