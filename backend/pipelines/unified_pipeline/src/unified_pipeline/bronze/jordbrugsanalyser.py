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
from typing import Any, ClassVar

import aiohttp
from pydantic import ConfigDict
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from unified_pipeline.common.base import BaseJobConfig, BaseSource, BronzeJobInterface
from unified_pipeline.util.jordbrugsanalyser_gml import (
    FIELD_MAPPING,
    NAMESPACES,
    parse_wfs_response,
)
from unified_pipeline.util.timing import AsyncTimer


class JordbrugsanalyserBronzeConfig(BaseJobConfig):
    """
    Configuration for the Jordbrugsanalyser Bronze source.

    This class defines all configuration parameters needed for fetching
    Jordbrugsanalyser marker data from the Danish WFS service. It includes
    endpoint URLs, dataset configuration, performance tuning parameters,
    and request configuration.

    Performance Note:
    Large layers (575k+ features) are fetched in paginated STARTINDEX/COUNT
    chunks (batch_size). Single unlimited requests now time out server-side
    (~300s) and return truncated GML, so pagination is required for reliability.

    Attributes:
        name (str): Human-readable name of the data source
        type (str): Type of the data source (wfs)
        description (str): Brief description of the data
        wfs_url (str): Base URL for the WFS service
        dataset (str): Name of the dataset in storage
        frequency (str): How often the data is updated
        bucket (str): storage bucket name for raw data storage
        start_year (int): First year to fetch (2012)
        end_year (int): Last year to fetch (2024)
        batch_size (int): Features per request (0 = unlimited, downloads full dataset)
        max_concurrent (int): Maximum concurrent requests (1 for full downloads)
        timeout_config (aiohttp.ClientTimeout): Request timeout configuration
        max_concurrent (int): Max concurrent page requests per year
    """

    name: str = "Danish Jordbrugsanalyser Markers"
    type: str = "wfs"
    description: str = "Agricultural marker data from Jordbrugsanalyser WFS service"
    wfs_url: str = "https://geodata.fvm.dk/geoserver/wfs"
    dataset: str = "jordbrugsanalyser_markers"
    frequency: str = "yearly"
    bucket: str = "landbruget-data"

    # Year range for marker data
    start_year: int = 2012
    end_year: int = 2024

    # Request configuration.
    # NOTE: single unlimited requests (batch_size=0) now time out server-side
    # (~300s) for the large layers (575k+ features), returning truncated/invalid
    # GML that fails parsing (ValueError) and leaves corrupt bronze. Paginate
    # instead: chunked STARTINDEX/COUNT requests complete fast and reliably.
    batch_size: int = 50000  # features per WFS page (0 would download full dataset)
    # Pages within a year are fetched concurrently up to this limit (years stay
    # sequential to bound memory). The semaphore is built per-instance in
    # JordbrugsanalyserBronze.__init__ so this value actually takes effect.
    max_concurrent: int = 6
    request_timeout: int = 600  # Increased timeout for full dataset downloads

    timeout_config: aiohttp.ClientTimeout = aiohttp.ClientTimeout(
        total=request_timeout, connect=60, sock_read=request_timeout
    )

    # WFS namespaces for parsing responses
    namespaces: ClassVar[dict[str, str]] = NAMESPACES
    field_mapping: ClassVar[dict[str, tuple[str, Any]]] = FIELD_MAPPING

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class JordbrugsanalyserBronze(BaseSource[JordbrugsanalyserBronzeConfig], BronzeJobInterface):
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
    4. Save raw WFS responses to cloud storage
    """

    def __init__(self, config: JordbrugsanalyserBronzeConfig):
        """
        Initialize the JordbrugsanalyserBronze source.

        Args:
            config (JordbrugsanalyserBronzeConfig): Configuration for the data source"""
        super().__init__(config)
        # Build the request semaphore here (not as a frozen class-level default,
        # which would evaluate to Semaphore(1) at class-definition time and force
        # sequential fetches) so config.max_concurrent actually limits concurrency.
        self._request_semaphore = Semaphore(config.max_concurrent)

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

    def _get_base_wfs_params(self, layer_name: str) -> dict[str, str]:
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

    def _get_count_params(self, layer_name: str) -> dict[str, str]:
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

    def _get_feature_params(self, layer_name: str, start_index: int = 0) -> dict[str, str]:
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

    def _decode_response_content(self, content_bytes: bytes) -> str:
        """Decode WFS response bytes while preserving Danish characters."""
        try:
            return content_bytes.decode("utf-8")
        except UnicodeDecodeError:
            return content_bytes.decode("latin-1")

    def _parse_valid_wfs_response(
        self, content: str, layer_name: str, request_description: str
    ) -> ET.Element:
        """Parse and validate a WFS XML response before storing it."""
        try:
            root = ET.fromstring(content)
        except ET.ParseError as exc:
            snippet = content[:300].replace("\n", " ")
            raise ValueError(
                f"Invalid XML response for {layer_name} ({request_description}): {snippet}"
            ) from exc

        if root.tag.endswith("ExceptionReport"):
            exception_text = " ".join(root.itertext()).strip()
            raise ValueError(
                f"WFS exception for {layer_name} ({request_description}): {exception_text[:500]}"
            )

        if not root.tag.endswith("FeatureCollection"):
            snippet = content[:300].replace("\n", " ")
            raise ValueError(
                f"Unexpected WFS response root {root.tag!r} for {layer_name} "
                f"({request_description}): {snippet}"
            )

        return root

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
            async with (
                session.get(self.config.wfs_url, params=params) as response,
                AsyncTimer(f"Count request for {layer_name}"),
            ):
                if response.status == 200:
                    # Handle Danish characters properly by reading as bytes first
                    content_bytes = await response.read()
                    content = self._decode_response_content(content_bytes)
                    root = self._parse_valid_wfs_response(content, layer_name, "count request")

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
            self.log.error(f"Error getting total count for {layer_name}: {e!s}")
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

        async with (
            self._request_semaphore,
            AsyncTimer(f"{request_type} request for {layer_name}"),
        ):
            self.log.debug(f"Fetching {layer_name} {request_type.lower()}")
            async with session.get(self.config.wfs_url, params=params) as response:
                if response.status == 200:
                    # Handle Danish characters properly by reading as bytes first
                    content_bytes = await response.read()
                    content = self._decode_response_content(content_bytes)
                    self._parse_valid_wfs_response(content, layer_name, request_type.lower())

                    return content
                # Handle encoding for error messages too
                try:
                    response_text = await response.text(encoding="utf-8")
                except UnicodeDecodeError:
                    response_bytes = await response.read()
                    response_text = response_bytes.decode("latin-1", errors="replace")
                err_msg = (
                    f"Error response {response.status} for {layer_name} "
                    f"({request_type.lower()}). "
                    f"Response: {response_text[:500]}..."
                )
                self.log.error(err_msg)
                raise Exception(err_msg)

    def _parse_wfs_response(self, xml_content: str, year: int) -> list[dict[str, Any]]:
        """Parse a WFS FeatureCollection into compact structured feature rows."""
        return parse_wfs_response(xml_content, year, self.log)

    def _create_structured_bronze_table(self, raw_responses: list[str], year: int) -> str | None:
        """Create compact structured bronze table from paginated WFS GML responses."""
        all_features: list[dict[str, Any]] = []
        for response in raw_responses:
            if response:
                all_features.extend(self._parse_wfs_response(response, year))

        if not all_features:
            self.log.warning(f"No features parsed for year {year}")
            return None

        try:
            self.conn.execute("INSTALL spatial")
            self.conn.execute("LOAD spatial")
        except Exception:
            pass

        temp_table = f"temp_jordbrugsanalyser_features_{year}"
        table_name = f"jordbrugsanalyser_bronze_{year}"
        self.conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
        self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        self.conn.execute(f"""
            CREATE TABLE {temp_table} (
                owner_number BIGINT,
                field_block VARCHAR,
                field_number VARCHAR,
                crop_category VARCHAR,
                crop_name VARCHAR,
                crop_code INTEGER,
                area_ha DOUBLE,
                total_area_ha DOUBLE,
                centroid_x DOUBLE,
                centroid_y DOUBLE,
                geometry_wkt VARCHAR,
                year INTEGER
            )
        """)

        insert_sql = f"""
            INSERT INTO {temp_table} (
                owner_number,
                field_block,
                field_number,
                crop_category,
                crop_name,
                crop_code,
                area_ha,
                total_area_ha,
                centroid_x,
                centroid_y,
                geometry_wkt,
                year
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        for feature in all_features:
            self.conn.execute(
                insert_sql,
                [
                    feature.get("owner_number"),
                    feature.get("field_block"),
                    feature.get("field_number"),
                    feature.get("crop_category"),
                    feature.get("crop_name"),
                    feature.get("crop_code"),
                    feature.get("area_ha"),
                    feature.get("total_area_ha"),
                    feature.get("centroid_x"),
                    feature.get("centroid_y"),
                    feature.get("geometry_wkt"),
                    feature.get("year"),
                ],
            )

        self.conn.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT
                owner_number,
                field_block,
                field_number,
                crop_category,
                crop_name,
                crop_code,
                area_ha,
                total_area_ha,
                centroid_x,
                centroid_y,
                geometry,
                year
            FROM (
                SELECT
                    *,
                    TRY(ST_GeomFromText(geometry_wkt)) AS geometry
                FROM {temp_table}
                WHERE geometry_wkt IS NOT NULL
            )
            WHERE geometry IS NOT NULL
              AND TRY(ST_IsValid(geometry)) = true
        """)
        self.conn.execute(f"DROP TABLE IF EXISTS {temp_table}")

        feature_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        self.log.info(f"Year {year}: Created {feature_count:,} compact bronze features")
        if feature_count == 0:
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            return None

        return table_name

    async def _process_year_data(self, session: aiohttp.ClientSession, year: int) -> str | None:
        """
        Process data for a specific year and return a compact structured table.

        This method orchestrates the data retrieval workflow for a specific year:
        1. Gets the layer name for the year (e.g., Marker12 for 2012)
        2. Gets the total count of available features from the WFS service
        3. Fetches data either as full dataset or in chunks based on configuration
        4. Parses the GML responses and returns a structured DuckDB table

        Args:
            session (aiohttp.ClientSession): HTTP session for making requests
            year (int): Year to process (e.g., 2012)

        Returns:
            str | None: DuckDB table name containing structured bronze rows

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
                    return None

                # If batch_size is 0, download entire dataset in one request
                if self.config.batch_size == 0:
                    self.log.info(f"Year {year}: Downloading full dataset in single request")
                    raw_response = await self._fetch_chunk(session, layer_name, 0)
                    if raw_response:
                        return self._create_structured_bronze_table([raw_response], year)
                    return None

                # Otherwise, use chunked downloading
                tasks = [
                    self._fetch_chunk(session, layer_name, start_index)
                    for start_index in range(0, total_count, self.config.batch_size)
                ]

                # Execute all tasks and collect results
                raw_responses = await asyncio.gather(*tasks)

                # 🧹 CLEANUP: Filter and return immediately, don't hold invalid responses
                valid_responses = [resp for resp in raw_responses if resp]
                self.log.info(f"Year {year}: Collected {len(valid_responses)} valid responses")

                # 🧹 CLEANUP: Clear the original raw_responses list to free memory
                raw_responses.clear()
                raw_responses = None

                return self._create_structured_bronze_table(valid_responses, year)

            except Exception as e:
                self.log.error(f"Error processing year {year}: {e!s}")
                raise

    async def run(self) -> dict[str, list[str]] | None:
        """
        Run the data source processing pipeline.

        This method orchestrates the entire data retrieval process:
        1. Processes marker data for each year from 2012 to 2024
        2. For each year, fetches all available marker features
        3. Saves compact structured marker rows to cloud storage
        4. Tracks overall execution time for performance monitoring
        Returns the raw data for in-memory passing to silver stage.

        Returns:
            Optional[Dict[str, List[str]]]: Dictionary mapping year to storage references,
                                           or None if processing fails

        Note:
            This is the main entry point for the bronze layer processing of
            Jordbrugsanalyser marker data.
        """
        self.log.info("Running Jordbrugsanalyser Markers bronze job")

        all_year_data = {}

        async with AsyncTimer("Total Jordbrugsanalyser run time"):
            async with aiohttp.ClientSession(timeout=self.config.timeout_config) as session:
                for year in range(self.config.start_year, self.config.end_year + 1):
                    try:
                        self.log.info(f"Processing year {year}")

                        # 🧹 CLEANUP: Log memory usage before processing each year
                        memory_info = self.get_memory_usage()
                        if "system" in memory_info:
                            self.log.info(
                                f"Year {year}: Memory usage before processing: "
                                f"{memory_info['system']['used_gb']:.1f}GB "
                                f"({memory_info['system']['percent']:.1f}%)"
                            )

                        structured_table = await self._process_year_data(session, year)

                        if structured_table:
                            # Save data with year suffix for easy identification using
                            # new unified method
                            dataset_name = f"{self.config.dataset}_{year}"
                            feature_count = self.conn.execute(
                                f"SELECT COUNT(*) FROM {structured_table}"
                            ).fetchone()[0]
                            self.log.info(
                                f"Saving {feature_count:,} compact features for year {year}"
                            )
                            self._save_data(
                                structured_table,
                                dataset_name,
                                self.config.bucket,
                                stage="bronze",
                                crs="EPSG:25832",
                            )
                            self.log.info(f"Year {year}: Data saved successfully")

                            all_year_data[str(year)] = [f"saved_to_storage_{dataset_name}"]

                            # 🧹 CLEANUP: Clean up any temporary tables and force memory
                            # cleanup after each year
                            self.conn.execute(f"DROP TABLE IF EXISTS {structured_table}")
                            self.cleanup_resources()

                            # 🧹 CLEANUP: Log memory usage after cleanup
                            memory_info = self.get_memory_usage()
                            if "system" in memory_info:
                                self.log.info(
                                    f"Year {year}: Memory usage after cleanup: "
                                    f"{memory_info['system']['used_gb']:.1f}GB "
                                    f"({memory_info['system']['percent']:.1f}%)"
                                )
                        else:
                            self.log.warning(f"Year {year}: No data to save")

                    except Exception as e:
                        self.log.error(f"Failed to process year {year}: {e!s}")
                        # Continue with next year instead of failing completely
                        continue

            self.log.info("Jordbrugsanalyser Markers bronze job completed successfully")

            # Return data for in-memory passing
            return all_year_data if all_year_data else None
