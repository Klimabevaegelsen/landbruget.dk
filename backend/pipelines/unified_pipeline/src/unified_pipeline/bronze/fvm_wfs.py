"""
Bronze layer data ingestion for FVM WFS Agricultural data.

This module handles the extraction of agricultural field data from the Danish
Agricultural Agency (FVM) WFS service at geodata.fvm.dk. It fetches both
Markblokke (field blocks) and Marker (field markers) data across all available
years from 2005-2026.

Based on performance testing, the FVM WFS service can handle full dataset downloads
in single requests efficiently (~1 minute per layer for 400k+ features).

The module contains:
- FVMWFSBronzeConfig: Configuration class for the FVM WFS data source
- FVMWFSBronze: Implementation class for fetching and processing data

The data is fetched using WFS GetFeature requests for each year's layer,
with proper error handling and retry logic for robustness.
"""

import ssl
from asyncio import Semaphore
from typing import Dict, List, Optional

import aiohttp

# ✅ MIGRATION: Removed pandas import - using DuckDB for  operations
from pydantic import ConfigDict
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from unified_pipeline.common.base import BaseJobConfig, BaseSource, BronzeJobInterface
from unified_pipeline.util.timing import AsyncTimer


class FVMWFSBronzeConfig(BaseJobConfig):
    """
    Configuration for the FVM WFS Bronze source.

    This class defines all configuration parameters needed for fetching
    agricultural field data from the Danish FVM WFS service. It includes
    layer definitions, performance tuning parameters, and request configuration.

    Performance Note:
    Testing showed that downloading full datasets in single requests is optimal.
    The FVM WFS server can handle 400k+ features in ~1 minute with excellent
    throughput (6,045 features/sec, 8.67 MB/sec).

    Attributes:
        name (str): Human-readable name of the data source
        type (str): Type of the data source (wfs)
        description (str): Brief description of the data
        wfs_url (str): Base URL for the FVM WFS service
        dataset_markblokke (str): Name of the markblokke dataset in storage
        dataset_marker (str): Name of the marker dataset in storage
        dataset_organic_areas (str): Name of the organic areas dataset in storage
        frequency (str): How often the data is updated
        bucket (str): GCS bucket name for raw data storage
        markblokke_years (List[int]): Years available for Markblokke data (2005-2026)
        marker_years (List[int]): Years available for Marker data (2008-2025)
        smaabiotoper_years (List[int]): Years available for Smaabiotoper data (2023-2025)
        organic_areas_years (List[int]): Years available for Organic Areas data (2012-2024)
        batch_size (int): Features per request (0 = unlimited, downloads full dataset)
        max_concurrent (int): Maximum concurrent requests
        timeout_config (aiohttp.ClientTimeout): Request timeout configuration
        request_semaphore (Semaphore): Semaphore to limit concurrent requests
    """

    name: str = "Danish FVM WFS Agricultural Data"
    type: str = "wfs"
    description: str = "Agricultural field blocks and markers from FVM WFS service"
    wfs_url: str = "https://geodata.fvm.dk/geoserver/wfs"
    dataset_markblokke: str = "fvm_markblokke"
    dataset_marker: str = "fvm_marker"
    dataset_organic_areas: str = "fvm_organic_areas"
    frequency: str = "yearly"
    bucket: str = "landbrugsdata-raw-data"

    # Year ranges based on FVM WFS capabilities analysis
    markblokke_years: List[int] = list(range(2005, 2027))  # 2005-2026 (22 years)
    marker_years: List[int] = list(range(2008, 2026))  # 2008-2025 (18 years)
    smaabiotoper_years: List[int] = [2023, 2024, 2025]  # Special biotope layers
    organic_areas_years: List[int] = list(range(2012, 2025))  # 2012-2024 (13 years of organic data)

    # Request configuration - optimized for full dataset downloads
    # Testing showed full downloads are optimal (no chunking needed)
    batch_size: int = 0  # 0 = unlimited, download full dataset in one request
    max_concurrent: int = 2  # Process layers sequentially for stability
    request_timeout: int = 900  # 15 minutes timeout for large datasets

    timeout_config: aiohttp.ClientTimeout = aiohttp.ClientTimeout(
        total=request_timeout, connect=60, sock_read=request_timeout
    )
    request_semaphore: Semaphore = Semaphore(max_concurrent)

    # WFS request parameters
    output_format: str = "application/json"  # Most efficient format
    srs_name: str = "EPSG:25832"  # Danish UTM Zone 32N (default for FVM)

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    def apply_cli_filters(self, cli_config) -> None:
        """
        Apply CLI filtering for matrix job processing.

        This method modifies the year lists based on CLI parameters to enable
        processing of specific layer types and years for parallel matrix jobs.

        Args:
            cli_config: CLI configuration containing fvm_layer_type and fvm_year filters
        """
        # Import here to avoid circular imports
        from unified_pipeline.model.cli import FVMLayerType

        if cli_config.source.value != "fvm_wfs":
            return  # Only apply filters for FVM WFS source

        if cli_config.fvm_layer_type or cli_config.fvm_year:
            # Clear all years initially for filtered processing
            object.__setattr__(self, "markblokke_years", [])
            object.__setattr__(self, "marker_years", [])
            object.__setattr__(self, "smaabiotoper_years", [])
            object.__setattr__(self, "organic_areas_years", [])

            # Apply layer type filter
            if cli_config.fvm_layer_type:
                layer_type = cli_config.fvm_layer_type

                if layer_type == FVMLayerType.markblokke:
                    years = list(range(2005, 2027))  # 2005-2026
                elif layer_type == FVMLayerType.marker:
                    years = list(range(2008, 2026))  # 2008-2025
                elif layer_type == FVMLayerType.smaabiotoper:
                    years = [2023, 2024, 2025]
                elif layer_type == FVMLayerType.organic_areas:
                    years = list(range(2012, 2025))  # 2012-2024
                else:
                    years = []

                # Apply year filter if specified
                if cli_config.fvm_year:
                    if cli_config.fvm_year in years:
                        years = [cli_config.fvm_year]
                    else:
                        years = []  # Invalid year for this layer type

                # Set the appropriate year list
                if layer_type == FVMLayerType.markblokke:
                    object.__setattr__(self, "markblokke_years", years)
                elif layer_type == FVMLayerType.marker:
                    object.__setattr__(self, "marker_years", years)
                elif layer_type == FVMLayerType.smaabiotoper:
                    object.__setattr__(self, "smaabiotoper_years", years)
                elif layer_type == FVMLayerType.organic_areas:
                    object.__setattr__(self, "organic_areas_years", years)


class FVMWFSBronze(BaseSource[FVMWFSBronzeConfig], BronzeJobInterface):
    """
    Bronze layer processing for FVM WFS agricultural data.

    This class is responsible for fetching raw agricultural field data from the
    FVM WFS service for all available years. It handles both Markblokke (field blocks)
    and Marker (field markers) data, plus special Smaabiotoper layers.

    The class implements retry logic for resilience against transient failures
    and uses semaphores to control the number of concurrent requests.

    Processing flow:
    1. For each layer type (Markblokke, Marker, Smaabiotoper), iterate through years
    2. Get total feature count from WFS service
    3. Fetch complete dataset in single request (optimal based on testing)
    4. Save raw WFS responses to Google Cloud Storage
    """

    def __init__(self, config: FVMWFSBronzeConfig):
        """
        Initialize the FVMWFSBronze source.

        Args:
            config (FVMWFSBronzeConfig): Configuration for the data source"""
        super().__init__(config)

    def _get_layer_name(self, layer_type: str, year: int) -> str:
        """
        Get the WFS layer name for a specific type and year.

        Args:
            layer_type (str): Type of layer ('Markblokke', 'Marker', 'Smaabiotoper', or 'OrganicAreas')
            year (int): Year for the layer

        Returns:
            str: Full layer name (e.g., "Markblokke:Markblokke_2024")
        """
        if layer_type == "Smaabiotoper":
            return f"Marker:Smaabiotoper_{year}"
        elif layer_type == "OrganicAreas":
            return f"Miljoe_og_oekologitilsagn:Oekologiske_arealer_{year}"
        else:
            return f"{layer_type}:{layer_type}_{year}"

    def _get_wfs_params(
        self, layer_name: str, get_count_only: bool = False, start_index: int = 0
    ) -> Dict[str, str]:
        """
        Get WFS GetFeature request parameters.

        Args:
            layer_name (str): Name of the layer to request
            get_count_only (bool): If True, request only feature count
            start_index (int): Starting index for pagination (ignored if batch_size=0)

        Returns:
            Dict[str, str]: WFS request parameters
        """
        params = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeName": layer_name,
            "srsName": self.config.srs_name,
        }

        if get_count_only:
            params["resultType"] = "hits"
        else:
            params["outputFormat"] = self.config.output_format
            # Add pagination parameters when batch_size > 0
            if self.config.batch_size > 0:
                params["startIndex"] = str(start_index)
                params["count"] = str(self.config.batch_size)
            # For unlimited downloads (batch_size=0), don't add pagination parameters
            # but the server may still apply default limits, so we need to handle that

        return params

    async def _get_total_count(self, session: aiohttp.ClientSession, layer_name: str) -> int:
        """
        Get total number of features available from a specific layer.

        Args:
            session (aiohttp.ClientSession): HTTP session for making requests
            layer_name (str): Name of the layer to query

        Returns:
            int: Total number of features available

        Raises:
            Exception: If the WFS request fails or returns an error
        """
        params = self._get_wfs_params(layer_name, get_count_only=True)

        try:
            self.log.info(f"Getting feature count for {layer_name}")
            async with (
                session.get(self.config.wfs_url, params=params) as response,
                AsyncTimer(f"Get count for {layer_name}"),
            ):
                if response.status == 200:
                    response_text = await response.text()

                    # Parse the WFS response to extract feature count
                    # WFS hits response contains numberMatched attribute
                    if 'numberMatched="' in response_text:
                        start = response_text.find('numberMatched="') + 15
                        end = response_text.find('"', start)
                        count_str = response_text[start:end]
                        try:
                            return int(count_str)
                        except ValueError:
                            self.log.warning(f"Could not parse feature count: {count_str}")
                            return 0
                    else:
                        self.log.warning(f"No numberMatched found in response for {layer_name}")
                        return 0
                else:
                    response_text = await response.text()
                    raise Exception(
                        f"Error getting count for {layer_name}: {response.status} - {response_text[:500]}"
                    )
        except Exception as e:
            self.log.error(f"Error getting total count for {layer_name}: {str(e)}")
            raise

    @retry(
        retry=retry_if_exception_type(Exception),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
    )
    async def _fetch_layer_data(
        self, session: aiohttp.ClientSession, layer_name: str
    ) -> Optional[str]:
        """
        Fetch complete layer data with retry logic.

        This method retrieves the complete dataset for a layer from the FVM WFS service.
        Based on testing, single requests for full datasets are optimal.

        Args:
            session (aiohttp.ClientSession): HTTP session for making requests
            layer_name (str): Name of the layer to fetch

        Returns:
            str: Raw WFS response data, or None if fetch fails

        Raises:
            Exception: If the WFS request fails after all retry attempts
        """
        params = self._get_wfs_params(layer_name)

        async with (
            self.config.request_semaphore,
            AsyncTimer(f"Fetch layer {layer_name}"),
        ):
            self.log.info(f"Fetching layer: {layer_name}")
            async with session.get(self.config.wfs_url, params=params) as response:
                if response.status == 200:
                    data = await response.text()
                    self.log.info(f"Successfully fetched {layer_name} ({len(data)} characters)")
                    return data
                else:
                    response_text = await response.text()
                    err_msg = (
                        f"Error response {response.status} for {layer_name}. "
                        f"Response: {response_text[:500]}..."
                    )
                    self.log.error(err_msg)
                    raise Exception(err_msg)

    def create_dataframe(self, raw_data: List[str], layer_type: str, year: int):
        """
        Create a  from the raw WFS data using DuckDB.

        Args:
            raw_data (List[str]): List of raw WFS response strings
            layer_type (str): Type of layer (Markblokke, Marker, Smaabiotoper)
            year (int): Year of the data

        Returns:
            str: Table name containing the raw data with metadata
        """
        # ✅ MIGRATION: Use DuckDB with proper Python list handling
        current_timestamp = self.conn.execute("SELECT current_timestamp").fetchone()[0]

        # Create a table by inserting values one by one to avoid complex escaping
        self.conn.execute("CREATE OR REPLACE TABLE temp_wfs_data (payload VARCHAR)")

        # Use prepared statements to safely insert the WFS response strings
        for wfs_str in raw_data:
            self.conn.execute("INSERT INTO temp_wfs_data VALUES (?)", [wfs_str])

        # ✅ MIGRATION: Create the final table with metadata columns directly
        self.conn.execute(
            """
            CREATE OR REPLACE TABLE final_dataframe AS
            SELECT 
                payload,
                ? as source,
                ? as layer_type,
                ? as year,
                ? as created_at,
                ? as updated_at
            FROM temp_wfs_data
        """,
            [self.config.name, layer_type, year, current_timestamp, current_timestamp],
        )

        # Clean up the temporary table
        self.conn.execute("DROP TABLE temp_wfs_data")

        return "final_dataframe"

    async def _process_layer_type(
        self, session: aiohttp.ClientSession, layer_type: str, years: List[int], dataset_name: str
    ) -> Dict[int, str]:
        """
        Process all years for a specific layer type.

        Args:
            session (aiohttp.ClientSession): HTTP session for making requests
            layer_type (str): Type of layer to process
            years (List[int]): List of years to process
            dataset_name (str): Name of the dataset for storage

        Returns:
            Dict[int, str]: Dictionary mapping years to raw data
        """
        layer_data = {}

        self.log.info(f"Processing {layer_type} data for {len(years)} years")

        for year in years:
            try:
                layer_name = self._get_layer_name(layer_type, year)

                # Get feature count first
                total_count = await self._get_total_count(session, layer_name)
                self.log.info(f"{layer_name}: {total_count:,} features")

                if total_count == 0:
                    self.log.warning(f"No features found for {layer_name}, skipping")
                    continue

                # Fetch the complete layer data
                raw_data = await self._fetch_layer_data(session, layer_name)

                if raw_data:
                    layer_data[year] = raw_data

                    # Save individual year data
                    table_name = self.create_dataframe([raw_data], layer_type, year)
                    dataset_with_year = f"{dataset_name}_{year}"
                    self._save_data(
                        table_name, dataset_with_year, self.config.bucket, stage="bronze"
                    )

                    self.log.info(f"Saved {layer_name} to {dataset_with_year}")
                else:
                    self.log.warning(f"No data received for {layer_name}")

            except Exception as e:
                self.log.error(f"Error processing {layer_type} {year}: {e}")
                continue

        return layer_data

    async def run(self) -> Optional[Dict]:
        """
        Run the FVM WFS data source processing pipeline for all available layers.

        This method orchestrates the entire data retrieval process:
        1. Processes Markblokke data for years 2005-2026
        2. Processes Marker data for years 2008-2025
        3. Processes Smaabiotoper data for years 2023-2025
        4. Tracks overall execution time for performance monitoring
        5. Returns all processed data for potential in-memory passing

        Returns:
            Optional[Dict]: Dictionary containing all processed data organized by layer type and year,
                           or None if processing fails
        """
        self.log.info("Starting FVM WFS bronze job for all available layers")

        # Configure SSL context for the session
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        connector = aiohttp.TCPConnector(ssl=ssl_context)

        async with (
            aiohttp.ClientSession(
                timeout=self.config.timeout_config, connector=connector
            ) as session,
            AsyncTimer("Total FVM WFS run time"),
        ):
            all_data = {"markblokke": {}, "marker": {}, "smaabiotoper": {}, "organic_areas": {}}

            # Process Markblokke data (field blocks) 2005-2026
            self.log.info("Processing Markblokke (field blocks) data")
            markblokke_data = await self._process_layer_type(
                session, "Markblokke", self.config.markblokke_years, self.config.dataset_markblokke
            )
            all_data["markblokke"] = markblokke_data

            # Process Marker data (field markers) 2008-2025
            self.log.info("Processing Marker (field markers) data")
            marker_data = await self._process_layer_type(
                session, "Marker", self.config.marker_years, self.config.dataset_marker
            )
            all_data["marker"] = marker_data

            # Process Smaabiotoper data (small biotopes) 2023-2025
            self.log.info("Processing Smaabiotoper (small biotopes) data")
            smaabiotoper_data = await self._process_layer_type(
                session,
                "Smaabiotoper",
                self.config.smaabiotoper_years,
                f"{self.config.dataset_marker}_smaabiotoper",
            )
            all_data["smaabiotoper"] = smaabiotoper_data

            # Process Organic Areas data (organic areas) 2012-2024
            self.log.info("Processing Organic Areas (organic areas) data")
            organic_areas_data = await self._process_layer_type(
                session,
                "OrganicAreas",
                self.config.organic_areas_years,
                self.config.dataset_organic_areas,
            )
            all_data["organic_areas"] = organic_areas_data

            # Log summary statistics
            total_markblokke = len(markblokke_data)
            total_marker = len(marker_data)
            total_smaabiotoper = len(smaabiotoper_data)
            total_organic_areas = len(organic_areas_data)

            self.log.info("FVM WFS data processing summary:")
            self.log.info(
                f"  Markblokke layers: {total_markblokke}/{len(self.config.markblokke_years)}"
            )
            self.log.info(f"  Marker layers: {total_marker}/{len(self.config.marker_years)}")
            self.log.info(
                f"  Smaabiotoper layers: {total_smaabiotoper}/{len(self.config.smaabiotoper_years)}"
            )
            self.log.info(
                f"  Organic Areas layers: {total_organic_areas}/{len(self.config.organic_areas_years)}"
            )

            self.log.info("FVM WFS bronze job completed successfully")
            return all_data
