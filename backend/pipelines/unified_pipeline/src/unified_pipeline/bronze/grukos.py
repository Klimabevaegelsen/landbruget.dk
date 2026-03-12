"""
Grukos Bronze Layer Implementation

This module implements the bronze layer data ingestion for Danish groundwater
mapping data (Grundvandskortlægning). It fetches raw data from the grukos WFS
service for indsatsområder (action areas for pesticide-sensitive and nitrate-sensitive
groundwater intake areas) and stores it in the bronze layer following the medallion
architecture pattern.

The module contains:
- GrukosBronzeConfig: Configuration class for the grukos data source
- GrukosBronze: Implementation class for fetching and processing grukos WFS data

The data is fetched from the grukos WFS service with proper error handling
and async processing for robustness.
"""

from asyncio import Semaphore
from typing import Any, ClassVar

import aiohttp
from pydantic import ConfigDict

from unified_pipeline.common.base import BaseJobConfig, BaseSource, BronzeJobInterface
from unified_pipeline.util.timing import AsyncTimer, timed


class GrukosBronzeConfig(BaseJobConfig):
    """
    Configuration for Grukos bronze layer data processing.

    This class defines all the necessary parameters and settings required
    for fetching grukos (groundwater mapping) data from WFS services and storing it
    in the bronze layer of the data pipeline.

    The configuration includes WFS service endpoints, layer specifications,
    performance settings, and authentication details.

    Attributes:
        dataset (str): Name of the dataset being processed
        bucket (str): GCS bucket name for data storage
        max_concurrent (int): Maximum number of concurrent WFS requests
        request_timeout (int): Timeout for individual requests in seconds
        batch_size (int): Number of features to fetch per request
        request_timeout_config (aiohttp.ClientTimeout): HTTP client timeout configuration
        headers (dict): HTTP headers for WFS requests
        request_semaphore (Semaphore): Semaphore for controlling concurrent requests
        layers (list): List of WFS layer names to fetch
        url_mapping (dict): Mapping of layer names to WFS service URLs
        service_types (dict): Service type overrides for specific layers
    """

    dataset: str = "grukos"
    bucket: str = "landbruget-data"
    max_concurrent: int = 10
    request_timeout: int = 300
    batch_size: int = 10000
    request_timeout_config: aiohttp.ClientTimeout = aiohttp.ClientTimeout(
        total=request_timeout, connect=60, sock_read=300
    )
    headers: ClassVar[dict[str, str]] = {"User-Agent": "Mozilla/5.0 QGIS/33603/macOS 15.1"}
    request_semaphore: Semaphore = Semaphore(max_concurrent)
    layers: ClassVar[list[str]] = [
        "grukos:indsatsomraader",  # Indsatsområder (action areas for groundwater protection)
        "grukos:indvindingsoplande_alle",  # Indvindingsoplande (groundwater abstraction catchments)
    ]
    url_mapping: ClassVar[dict[str, str]] = {
        "grukos:indsatsomraader": "https://wfs2-miljoegis.mim.dk/grukos/wfs",
        "grukos:indvindingsoplande_alle": "https://wfs2-miljoegis.mim.dk/grukos/ows",
    }
    service_types: ClassVar[dict[str, str]] = {}  # All layers use default WFS service type

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class GrukosBronze(BaseSource[GrukosBronzeConfig], BronzeJobInterface):
    """
    Bronze layer processor for Grukos (groundwater mapping) data.

    This class handles the extraction of grukos data from WFS services
    and stores the raw data in the bronze layer of the data pipeline.
    It fetches data for indsatsområder (action areas for pesticide-sensitive
    and nitrate-sensitive groundwater intake areas) from the Danish
    environmental WFS services.

    The processing includes:
    1. Fetching raw data from WFS endpoints
    2. Handling different service types (WFS)
    3. Processing responses and extracting features
    4. Storing raw data in Google Cloud Storage (GCS)
    5. Error handling and retry logic for robustness
    """

    def __init__(self, config: GrukosBronzeConfig):
        """
        Initialize the GrukosBronze processor.

        Args:
            config (GrukosBronzeConfig): Configuration object containing settings
                                         for the processor.
        """
        super().__init__(config)
        self.log.info("✅ GrukosBronze: Using unified GCS access and DuckDB connection")

    @timed(name="Fetching WFS data")  # type: ignore
    async def fetch_wfs_data(self, layer: str, url: str) -> str | None:
        """
        Fetch data from a WFS service for a specific layer.

        Args:
            layer (str): The WFS layer name to fetch
            url (str): The WFS service URL

        Returns:
            Optional[str]: The raw WFS response as XML string, or None if failed
        """
        try:
            # Construct WFS GetFeature request
            params = {
                "service": "WFS",
                "version": "2.0.0",
                "request": "GetFeature",
                "typeName": layer,
                "outputFormat": "application/gml+xml; version=3.2",
                "maxFeatures": self.config.batch_size,
            }

            self.log.info(f"Fetching WFS data for layer {layer} from {url}")

            async with (
                self.config.request_semaphore,
                aiohttp.ClientSession(
                    timeout=self.config.request_timeout_config,
                    headers=self.config.headers,
                ) as session,
                session.get(url, params=params) as response,
            ):
                if response.status == 200:
                    content = await response.text()
                    self.log.info(f"Successfully fetched {len(content)} bytes for layer {layer}")
                    return content
                self.log.error(f"Failed to fetch layer {layer}: HTTP {response.status}")
                return None

        except Exception as e:
            self.log.error(f"Error fetching WFS data for layer {layer}: {e}")
            return None

    async def run(self, bronze_data: Any | None = None) -> Any | None:
        """
        Run the Grukos bronze layer processing.

        This method orchestrates the fetching of all grukos layers
        from their respective WFS services and stores the raw data.

        Args:
            bronze_data: Optional in-memory data (not used in bronze stage)

        Returns:
            Optional[Any]: List of (layer, raw_data) tuples for potential
                          silver stage consumption, or None if processing fails.
        """
        self.log.info("Running Grukos bronze job")
        async with AsyncTimer("Grukos bronze job"):
            try:
                # Fetch all layers concurrently
                raw_data_list = []

                for layer in self.config.layers:
                    # Get the appropriate URL for this layer
                    url = self.config.url_mapping.get(layer)
                    if not url:
                        self.log.warning(f"No URL mapping found for layer {layer}, skipping")
                        continue

                    # Fetch the layer data
                    raw_data = await self.fetch_wfs_data(layer, url)
                    if raw_data:
                        raw_data_list.append((layer, raw_data))
                        self.log.info(f"Successfully fetched data for layer {layer}")
                    else:
                        self.log.warning(f"Failed to fetch data for layer {layer}")

                if not raw_data_list:
                    self.log.error("No data was successfully fetched from any layer")
                    return None

                # Store raw data using prepared statements (safe for XML content)
                current_timestamp = self.conn.execute("SELECT current_timestamp").fetchone()[0]

                for layer, raw_data in raw_data_list:
                    # Create safe table name
                    table_name = f"{layer.replace(':', '_')}_raw"

                    # Create table with proper schema
                    self.conn.execute(f"""
                        CREATE OR REPLACE TABLE {table_name} (
                            payload TEXT,
                            layer VARCHAR,
                            source VARCHAR,
                            created_at TIMESTAMP,
                            updated_at TIMESTAMP
                        )
                    """)

                    # Use prepared statement to safely insert XML data
                    self.conn.execute(
                        f"""
                        INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?)
                    """,
                        [
                            raw_data,
                            layer,
                            self.config.dataset,
                            current_timestamp,
                            current_timestamp,
                        ],
                    )

                    # Save to GCS
                    self._save_data(
                        table_name,
                        f"{self.config.dataset}_{layer.replace(':', '_')}",
                        self.config.bucket,
                        "bronze",
                        conn=self.conn,
                    )

                self.log.info(f"Successfully processed {len(raw_data_list)} grukos layers")

                # Return data that can be passed to silver stage
                return raw_data_list

            except Exception as e:
                self.log.error(f"Error in Grukos bronze processing: {e}")
                return None
