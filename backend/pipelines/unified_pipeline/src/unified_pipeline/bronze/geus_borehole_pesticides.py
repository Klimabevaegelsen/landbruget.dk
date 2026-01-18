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

The data is fetched in batches with incremental saving to GCS to avoid memory issues.
Each batch is saved as a separate parquet file, which the silver layer reads in
chunks for processing.
"""

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
    # Save to GCS every N chunks to avoid memory accumulation
    # With batch_size=5000, this saves every 50k records
    save_interval: int = 10
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

    def _save_intermediate_chunk(self, layer_type: str, chunk_num: int, gml_data: list[str]) -> str:
        """
        Save intermediate GML chunks to GCS to avoid memory accumulation.

        Path structure: bronze/{dataset}/{run_timestamp}/chunks/{layer_type}/chunk_{num}.parquet
        This puts the run timestamp at the top level so all chunks from a run are grouped together.

        Args:
            layer_type: Type of layer (boreholes, analyses, pesticide_analyses)
            chunk_num: Chunk number for filename
            gml_data: List of GML strings to save

        Returns:
            GCS path where the chunk was saved (relative to bucket)
        """
        current_timestamp = self.conn.execute("SELECT current_timestamp").fetchone()[0]

        # Create temporary table for this chunk
        temp_table = f"temp_chunk_{layer_type}_{chunk_num}"
        self.conn.execute(f"CREATE OR REPLACE TABLE {temp_table} (payload VARCHAR)")

        for data_str in gml_data:
            self.conn.execute(f"INSERT INTO {temp_table} VALUES (?)", [data_str])

        # Create table with metadata
        chunk_table = f"chunk_{layer_type}_{chunk_num}"
        self.conn.execute(
            f"""
            CREATE OR REPLACE TABLE {chunk_table} AS
            SELECT
                '{layer_type}' as layer_type,
                payload,
                ? as source,
                ? as source_crs,
                ? as created_at,
                ? as updated_at
            FROM {temp_table}
            """,
            [
                self.config.name,
                self.config.source_crs,
                current_timestamp,
                current_timestamp,
            ],
        )

        # Build GCS path with run timestamp at top level:
        # bronze/{dataset}/{run_timestamp}/chunks/{layer_type}/chunk_0000.parquet
        run_timestamp = self.date_pattern  # Consistent across entire run
        relative_path = (
            f"bronze/{self.config.dataset}/{run_timestamp}/chunks/{layer_type}/"
            f"chunk_{chunk_num:04d}.parquet"
        )
        gcs_path = f"gs://{self.config.bucket}/{relative_path}"

        # Save directly to GCS using gcs_access (bypasses _save_data's timestamp logic)
        self.gcs_access.upload_from_duckdb_table(chunk_table, gcs_path)

        # Clean up tables
        self.conn.execute(f"DROP TABLE {temp_table}")
        self.conn.execute(f"DROP TABLE {chunk_table}")

        self.log.info(
            f"Saved intermediate chunk {chunk_num} for {layer_type} "
            f"({len(gml_data)} GML responses) to {relative_path}"
        )

        # Return the relative path (without gs://bucket/) for manifest
        return relative_path

    async def _fetch_layer_data_streaming(
        self, session: aiohttp.ClientSession, typename: str, cql_filter: str | None = None
    ) -> dict:
        """
        Fetch all data for a specific WFS layer with incremental saving to GCS.

        This method orchestrates the data retrieval for a single layer:
        1. Fetches chunks sequentially
        2. Saves to GCS every save_interval chunks to avoid memory accumulation
        3. Returns metadata about saved chunks instead of raw data

        Args:
            session (aiohttp.ClientSession): HTTP session for making requests
            typename (str): The WFS layer name to fetch
            cql_filter (str, optional): CQL filter to apply to the request

        Returns:
            dict: Metadata about the fetched data including chunk paths

        Raises:
            Exception: If there are issues with data fetching or processing
        """
        # Map typename to layer_type for storage
        layer_type_map = {
            self.config.boreholes_typename: "boreholes",
            self.config.analyses_typename: "analyses",
            self.config.mc_analyse_typename: "pesticide_analyses",
        }
        layer_type = layer_type_map.get(typename, typename)

        pending_chunks: list[str] = []  # GML strings pending save
        saved_paths: list[str] = []  # GCS paths of saved chunks
        chunk_save_num = 0
        filter_desc = f" (filter: {cql_filter})" if cql_filter else ""

        async with AsyncTimer(f"Fetching all {typename}{filter_desc} data (streaming)"):
            # Fetch first chunk to get total count
            first_chunk = await self._fetch_chunk(session, typename, 0, cql_filter)
            total_features = first_chunk["total_features"]
            returned_features = first_chunk["returned_features"]
            pending_chunks.append(first_chunk["text"])
            fetched_count = returned_features
            chunk_count = 1

            if total_features is None:
                # Total is unknown - fetch sequentially until we get fewer than batch_size
                self.log.info(
                    f"[{typename}]{filter_desc} Total features unknown, fetching until exhausted..."
                )

                current_index = returned_features
                while returned_features >= self.config.batch_size:
                    chunk = await self._fetch_chunk(session, typename, current_index, cql_filter)
                    returned_features = chunk["returned_features"]

                    if returned_features > 0:
                        pending_chunks.append(chunk["text"])
                        fetched_count += returned_features
                        chunk_count += 1

                        # Save intermediate results every save_interval chunks
                        if len(pending_chunks) >= self.config.save_interval:
                            path = self._save_intermediate_chunk(
                                layer_type, chunk_save_num, pending_chunks
                            )
                            saved_paths.append(path)
                            pending_chunks = []  # Clear memory
                            chunk_save_num += 1
                            self.log.info(
                                f"[{typename}] Progress: {fetched_count:,} features fetched, "
                                f"{len(saved_paths)} chunks saved to GCS"
                            )

                    current_index += self.config.batch_size

                self.log.info(
                    f"[{typename}] Fetched all {fetched_count:,} features (total was unknown)"
                )
            else:
                # Total is known - fetch sequentially with intermediate saves
                self.log.info(
                    f"[{typename}]{filter_desc} Total features to fetch: {total_features:,}"
                )

                for start_index in range(returned_features, total_features, self.config.batch_size):
                    chunk = await self._fetch_chunk(session, typename, start_index, cql_filter)

                    if chunk["returned_features"] > 0:
                        pending_chunks.append(chunk["text"])
                        fetched_count += chunk["returned_features"]
                        chunk_count += 1

                        # Save intermediate results every save_interval chunks
                        if len(pending_chunks) >= self.config.save_interval:
                            path = self._save_intermediate_chunk(
                                layer_type, chunk_save_num, pending_chunks
                            )
                            saved_paths.append(path)
                            pending_chunks = []  # Clear memory
                            chunk_save_num += 1
                            self.log.info(
                                f"[{typename}] Progress: {fetched_count:,}/{total_features:,} "
                                f"features, {len(saved_paths)} chunks saved to GCS"
                            )

                self.log.info(
                    f"[{typename}] Fetched all {fetched_count:,} out of {total_features:,} features"
                )

            # Save any remaining pending chunks
            if pending_chunks:
                path = self._save_intermediate_chunk(layer_type, chunk_save_num, pending_chunks)
                saved_paths.append(path)
                pending_chunks = []

        return {
            "layer_type": layer_type,
            "typename": typename,
            "total_features": fetched_count,
            "chunk_count": chunk_count,
            "saved_paths": saved_paths,
        }

    async def _fetch_raw_data_streaming(self) -> dict | None:
        """
        Fetch all raw data from the GEUS WFS service with streaming saves to GCS.

        This method orchestrates the data retrieval workflow with memory optimization:
        1. Establishes an HTTP session with proper SSL and header configuration
        2. Fetches boreholes data with incremental saves
        3. Fetches analyses data with incremental saves
        4. Returns metadata about saved chunks (not raw data)

        Returns:
            Optional[dict]: Metadata about saved chunks for each layer,
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
            AsyncTimer("Fetching raw data for GEUS Borehole Pesticides bronze job (streaming)"),
        ):
            try:
                # Fetch boreholes data with incremental saves
                self.log.info("Fetching boreholes data (streaming mode)...")
                boreholes_meta = await self._fetch_layer_data_streaming(
                    session, self.config.boreholes_typename
                )

                # Fetch facility analyses data (jupiter_anlaegsanalyser - 48 substances)
                self.log.info(
                    "Fetching facility analyses data (jupiter_anlaegsanalyser, streaming mode)..."
                )
                facility_analyses_meta = await self._fetch_layer_data_streaming(
                    session, self.config.analyses_typename
                )

                # Fetch full groundwater analyses filtered by stofgruppe 50 (pesticides)
                # mc_analyse contains ALL groundwater chemical analyses with full substance list
                pesticide_filter = f"stofgruppe={self.config.pesticide_stofgruppe}"
                self.log.info(
                    f"Fetching full pesticide analyses data (mc_analyse, {pesticide_filter}, streaming mode)..."
                )
                pesticide_analyses_meta = await self._fetch_layer_data_streaming(
                    session, self.config.mc_analyse_typename, cql_filter=pesticide_filter
                )

                return {
                    "boreholes": boreholes_meta,
                    "analyses": facility_analyses_meta,
                    "pesticide_analyses": pesticide_analyses_meta,
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

    async def run(self) -> dict | None:
        """
        Run the data source processing pipeline with streaming saves.

        This method orchestrates the entire data retrieval process:
        1. Fetches all raw borehole and analyses data from the WFS service
        2. Incrementally saves chunks to GCS to avoid memory issues
        3. Returns metadata about saved chunks for silver layer to read

        Returns:
            Optional[dict]: Metadata about saved chunks that silver layer can use
                           to read data from GCS, or None if processing fails

        Note:
            This is the main entry point for the bronze layer processing of GEUS data.
            Uses streaming mode to handle large datasets (400k+ records) without OOM.
        """
        async with AsyncTimer("Running GEUS Borehole Pesticides bronze job"):
            self.log.info("Running GEUS Borehole Pesticides bronze job (streaming mode)")

            # Set source CRS for tracking
            self.set_source_crs(self.config.source_crs)

            # Use streaming fetch which saves incrementally to GCS
            metadata = await self._fetch_raw_data_streaming()
            if not metadata:
                self.log.error("No data fetched")
                return None

            boreholes_meta = metadata["boreholes"]
            analyses_meta = metadata["analyses"]
            pesticide_meta = metadata["pesticide_analyses"]

            self.log.info(
                f"Fetched and saved data successfully: "
                f"{boreholes_meta['total_features']:,} boreholes in {len(boreholes_meta['saved_paths'])} chunks, "
                f"{analyses_meta['total_features']:,} facility analyses in {len(analyses_meta['saved_paths'])} chunks, "
                f"{pesticide_meta['total_features']:,} pesticide analyses in {len(pesticide_meta['saved_paths'])} chunks"
            )

            # Save metadata manifest for silver layer
            # Path structure: bronze/{dataset}/{run_timestamp}/manifest.json
            run_timestamp = self.date_pattern
            manifest = {
                "dataset": self.config.dataset,
                "bucket": self.config.bucket,
                "source_crs": self.config.source_crs,
                "run_timestamp": run_timestamp,
                "layers": {
                    "boreholes": {
                        "total_features": boreholes_meta["total_features"],
                        "chunk_count": boreholes_meta["chunk_count"],
                        "saved_paths": boreholes_meta["saved_paths"],
                    },
                    "analyses": {
                        "total_features": analyses_meta["total_features"],
                        "chunk_count": analyses_meta["chunk_count"],
                        "saved_paths": analyses_meta["saved_paths"],
                    },
                    "pesticide_analyses": {
                        "total_features": pesticide_meta["total_features"],
                        "chunk_count": pesticide_meta["chunk_count"],
                        "saved_paths": pesticide_meta["saved_paths"],
                    },
                },
            }

            # Save manifest directly to GCS with proper path hierarchy
            # bronze/{dataset}/{run_timestamp}/manifest.json
            manifest_path = (
                f"gs://{self.config.bucket}/bronze/{self.config.dataset}/"
                f"{run_timestamp}/manifest.json"
            )
            self.gcs_access.upload_json(manifest, manifest_path)

            # Create and save pipeline metadata for data tracing
            if self.pipeline_metadata_manager and self.processing_start_time:
                try:
                    import time

                    total_records = (
                        boreholes_meta["total_features"]
                        + analyses_meta["total_features"]
                        + pesticide_meta["total_features"]
                    )
                    processing_duration = time.time() - self.processing_start_time

                    pipeline_metadata = self.pipeline_metadata_manager.create_metadata(
                        source_key="geus_borehole_pesticides",
                        record_count=total_records,
                        processing_duration=processing_duration,
                    )

                    # Save pipeline metadata alongside manifest
                    metadata_path = (
                        f"gs://{self.config.bucket}/bronze/{self.config.dataset}/"
                        f"{run_timestamp}/pipeline_metadata.json"
                    )
                    self.gcs_access.upload_json(pipeline_metadata.model_dump(), metadata_path)
                    self.log.info(f"✅ Pipeline metadata saved to {metadata_path}")
                except Exception as e:
                    self.log.warning(f"⚠️ Failed to create pipeline metadata: {e}")

            self.log.info("GEUS Borehole Pesticides bronze job completed successfully")

            # Return metadata for silver layer (or it can read from GCS manifest)
            return manifest
