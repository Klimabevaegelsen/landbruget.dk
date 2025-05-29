"""
Bronze layer data ingestion for DAGI (Danish Administrative Geographic Division) data.

This module handles the extraction of administrative geographic data from the Danish
DAWA API (https://api.dataforsyningen.dk/). It fetches raw GeoJSON data for various
administrative divisions including municipalities, regions, postal codes, and landsdele.

The module contains:
- DAGIBronzeConfig: Configuration class for the DAGI DAWA API data source
- DAGIBronze: Implementation class for fetching and processing GeoJSON data

The data is fetched using simple HTTP requests to the DAWA API endpoints with proper
error handling and retry logic for robustness.
"""

import asyncio
from asyncio import Semaphore
from typing import Dict, Optional

import aiohttp
from pydantic import Field
from tenacity import retry, stop_after_attempt, wait_exponential

from unified_pipeline.common.base import BaseJobConfig, BaseSource
from unified_pipeline.util.gcs_util import GCSUtil
from unified_pipeline.util.timing import AsyncTimer


class DAGIBronzeConfig(BaseJobConfig):
    """
    Configuration for DAGI bronze layer data extraction.

    Attributes:
        name: Human-readable name of the data source
        type: Type of the data source
        description: Brief description of the data
        base_url: Base URL for the DAWA API
        endpoints: Dictionary mapping layer names to API endpoints
        dataset: Name of the dataset in storage
        bucket: GCS bucket name for raw data storage
        timeout: Request timeout in seconds
        max_concurrent_requests: Maximum number of concurrent requests
        retries: Number of retry attempts for failed requests
    """

    name: str = "Danish Administrative Geographic Division"
    type: str = "dawa_api"
    description: str = "Administrative geographic divisions from Danish DAWA API"
    dataset: str = "dagi"
    bucket: str = "landbrugsdata-raw-data"

    base_url: str = Field(
        default="https://api.dataforsyningen.dk", description="Base URL for the DAWA API"
    )

    endpoints: Dict[str, str] = Field(
        default={
            "kommuner": "kommuner",
            "regioner": "regioner",
            "landsdele": "landsdele",
            "postnumre": "postnumre",
        },
        description="Mapping of layer names to API endpoints",
    )

    timeout: int = Field(default=120, description="Request timeout in seconds")

    max_concurrent_requests: int = Field(
        default=5, description="Maximum number of concurrent requests"
    )

    retries: int = Field(default=3, description="Number of retry attempts for failed requests")


class DAGIBronze(BaseSource[DAGIBronzeConfig]):
    """
    Bronze layer implementation for DAGI (Danish Administrative Geographic Division) data.

    Fetches administrative geographic data from the Danish DAWA API and saves it as
    raw data files. Supports multiple administrative division types including:
    - kommuner (municipalities)
    - regioner (regions)
    - landsdele (parts of country)
    - postnumre (postal codes)
    """

    def __init__(self, config: DAGIBronzeConfig, gcs_util: GCSUtil):
        """Initialize the DAGI bronze layer with configuration."""
        super().__init__(config, gcs_util)
        self.semaphore = Semaphore(config.max_concurrent_requests)

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10), reraise=True
    )
    async def _fetch_layer_data(
        self, session: aiohttp.ClientSession, layer_name: str, endpoint: str
    ) -> Optional[str]:
        """
        Fetch data for a specific DAGI layer from the DAWA API.

        Args:
            session: aiohttp client session
            layer_name: Name of the administrative layer
            endpoint: API endpoint for the layer

        Returns:
            JSON string response or None if failed
        """
        async with self.semaphore:
            url = f"{self.config.base_url}/{endpoint}?format=geojson"

            try:
                self.log.info(f"Fetching DAGI layer: {layer_name} from {url}")

                async with session.get(
                    url, timeout=aiohttp.ClientTimeout(total=self.config.timeout)
                ) as response:
                    response.raise_for_status()

                    data = await response.text()
                    self.log.info(f"Successfully fetched data for {layer_name}")

                    return data

            except aiohttp.ClientError as e:
                self.log.error(f"HTTP error fetching {layer_name}: {e}")
                raise
            except asyncio.TimeoutError as e:
                self.log.error(f"Timeout error fetching {layer_name}: {e}")
                raise
            except Exception as e:
                self.log.error(f"Unexpected error fetching {layer_name}: {e}")
                raise

    async def _fetch_all_layers(self) -> Dict[str, str]:
        """
        Fetch data for all configured DAGI layers concurrently.

        Returns:
            Dictionary mapping layer names to their raw JSON data
        """
        timeout = aiohttp.ClientTimeout(total=self.config.timeout)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            tasks = []

            for layer_name, endpoint in self.config.endpoints.items():
                task = asyncio.create_task(self._fetch_layer_data(session, layer_name, endpoint))
                tasks.append((layer_name, task))

            results = {}
            for layer_name, task in tasks:
                try:
                    raw_data = await task
                    if raw_data is not None:
                        results[layer_name] = raw_data
                        self.log.info(f"Successfully processed {layer_name}")
                    else:
                        self.log.warning(f"No data retrieved for {layer_name}")
                except Exception as e:
                    self.log.error(f"Failed to fetch {layer_name}: {e}")
                    # Continue with other layers even if one fails
                    continue

            return results

    async def run(self) -> None:
        """
        Run the DAGI data source processing pipeline.

        This method processes all configured DAGI layers, fetching
        their data from the DAWA API and storing it in Google Cloud Storage.
        """
        try:
            async with AsyncTimer("DAGI bronze layer processing") as timer:
                self.log.info("Starting DAGI bronze layer processing")
                self.log.info(f"Processing {len(self.config.endpoints)} DAGI layers")

                # Fetch all layer data concurrently
                layer_data = await self._fetch_all_layers()

                if not layer_data:
                    raise RuntimeError("No DAGI data could be fetched from any layer")

                # Save each layer as raw data
                for layer_name, raw_data in layer_data.items():
                    try:
                        dataset_name = f"{self.config.dataset}_{layer_name}"
                        self._save_raw_data(
                            [raw_data],
                            dataset_name,
                            f"{self.config.name} - {layer_name}",
                            self.config.bucket,
                        )
                        self.log.info(f"Saved raw data for {layer_name}")
                    except Exception as e:
                        self.log.error(f"Failed to save {layer_name}: {e}")
                        continue

                self.log.info(
                    f"DAGI bronze processing completed in {timer.elapsed():.2f}s. "
                    f"Processed {len(layer_data)} layers: {list(layer_data.keys())}"
                )

        except Exception as e:
            self.log.error(f"Critical error in DAGI bronze processing: {e}")
            raise
