"""
Water Typology Bronze Layer Implementation

This module implements the bronze layer data ingestion for Danish water typology data.
It fetches raw data from WFS services for lakes, coastal waters, and watercourses typology
and stores it in the bronze layer following the medallion architecture pattern.

The module contains:
- WaterTypologyBronzeConfig: Configuration class for the water typology data source
- WaterTypologyBronze: Implementation class for fetching and processing water typology WFS data

The data is fetched from the vp3endelig2022 WFS service with proper error handling
and async processing for robustness.
"""

from typing import Any, Optional
from asyncio import Semaphore

import aiohttp
from pydantic import ConfigDict

from unified_pipeline.common.base import BaseJobConfig, BaseSource, BronzeJobInterface
from unified_pipeline.util.timing import AsyncTimer, timed


class WaterTypologyBronzeConfig(BaseJobConfig):
    """
    Configuration for Water Typology bronze layer data processing.

    This class defines all the necessary parameters and settings required
    for fetching water typology data from WFS services and storing it
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

    dataset: str = "water_typology"
    bucket: str = "landbrugsdata-raw-data"
    max_concurrent: int = 10
    request_timeout: int = 300
    batch_size: int = 10000
    request_timeout_config: aiohttp.ClientTimeout = aiohttp.ClientTimeout(
        total=request_timeout, connect=60, sock_read=300
    )
    headers: dict[str, str] = {"User-Agent": "Mozilla/5.0 QGIS/33603/macOS 15.1"}
    request_semaphore: Semaphore = Semaphore(max_concurrent)
    layers: list[str] = [
        "vp3endelig2022:vp3e2022_soe_samlet",      # Lakes typology (Søer typologi)
        "vp3endelig2022:vp3e2022_marin_samlet",    # Coastal waters typology (Kystvande typologi)
        "vp3endelig2022:vp3e2022_vandloeb_samlet", # Watercourses typology (Åer typologi)
    ]
    url_mapping: dict[str, str] = {
        "vp3endelig2022:vp3e2022_soe_samlet": "https://wfs2-miljoegis.mim.dk/vp3endelig2022/ows",
        "vp3endelig2022:vp3e2022_marin_samlet": "https://wfs2-miljoegis.mim.dk/vp3endelig2022/ows",
        "vp3endelig2022:vp3e2022_vandloeb_samlet": "https://wfs2-miljoegis.mim.dk/vp3endelig2022/ows",
    }
    service_types: dict[str, str] = {}  # All layers use default WFS service type

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class WaterTypologyBronze(BaseSource[WaterTypologyBronzeConfig], BronzeJobInterface):
    """
    Bronze layer processor for Water Typology data.

    This class handles the extraction of water typology data from WFS services
    and stores the raw data in the bronze layer of the data pipeline.
    It fetches data for lakes, coastal waters, and watercourses typology
    from the Danish environmental WFS services.

    The processing includes:
    1. Fetching raw data from WFS endpoints
    2. Handling different service types (WFS)
    3. Processing responses and extracting features
    4. Storing raw data in Google Cloud Storage (GCS)
    5. Error handling and retry logic for robustness
    """

    def __init__(self, config: WaterTypologyBronzeConfig):
        """
        Initialize the WaterTypologyBronze processor.

        Args:
            config (WaterTypologyBronzeConfig): Configuration object containing settings
                                                for the processor.
        """
        super().__init__(config)

        # ✅ MIGRATION: BaseSource already created GCSDataAccess and configured DuckDB
        # No need to create another instance or setup DuckDB again
        self.log.info("✅ WaterTypologyBronze: Using unified GCS access and DuckDB connection")

    @timed(name="Fetching WFS data")  # type: ignore
    async def fetch_wfs_data(self, layer: str, url: str) -> Optional[str]:
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

            async with self.config.request_semaphore:
                async with aiohttp.ClientSession(
                    timeout=self.config.request_timeout_config,
                    headers=self.config.headers,
                ) as session:
                    async with session.get(url, params=params) as response:
                        if response.status == 200:
                            content = await response.text()
                            self.log.info(f"Successfully fetched {len(content)} bytes for layer {layer}")
                            return content
                        else:
                            self.log.error(
                                f"Failed to fetch layer {layer}: HTTP {response.status}"
                            )
                            return None

        except Exception as e:
            self.log.error(f"Error fetching WFS data for layer {layer}: {e}")
            return None

    async def run(self, bronze_data: Optional[Any] = None) -> Optional[Any]:
        """
        Run the Water Typology bronze layer processing.

        This method orchestrates the fetching of all water typology layers
        from their respective WFS services and stores the raw data.

        Args:
            bronze_data: Optional in-memory data (not used in bronze stage)

        Returns:
            Optional[Any]: List of (layer, raw_data) tuples for potential
                          silver stage consumption, or None if processing fails.
        """
        self.log.info("Running Water Typology bronze job")
        async with AsyncTimer("Water Typology bronze job"):
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
                    self.conn.execute(f"""
                        INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?)
                    """, [raw_data, layer, self.config.dataset, current_timestamp, current_timestamp])

                    # Save to GCS
                    self._save_data(
                        table_name,
                        f"{self.config.dataset}_{layer.replace(':', '_')}",
                        self.config.bucket,
                        "bronze",
                        conn=self.conn,
                    )

                self.log.info(f"Successfully processed {len(raw_data_list)} water typology layers")

                # Return data that can be passed to silver stage
                return raw_data_list

            except Exception as e:
                self.log.error(f"Error in Water Typology bronze processing: {e}")
                return None