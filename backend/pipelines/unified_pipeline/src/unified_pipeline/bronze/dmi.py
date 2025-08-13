"""
DMI (Danish Meteorological Institute) Bronze Layer Implementation

This module implements the bronze layer data ingestion for Danish climate data.
It fetches raw data from the DMI GovCloud API and stores it in the bronze layer
following the medallion architecture pattern.

The module contains:
- DMIBronzeConfig: Configuration class for the DMI data source
- DMIBronze: Implementation class for fetching and processing DMI climate data

The data is fetched from multiple climate parameters with proper error handling,
async processing, and retry logic for robustness.
"""

import asyncio
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import aiohttp
from pydantic import ConfigDict
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from unified_pipeline.common.base import BaseJobConfig, BaseSource, BronzeJobInterface
from unified_pipeline.util.timing import timed


class DMIBronzeConfig(BaseJobConfig):
    """
    Configuration for the DMI Bronze source.

    This class defines all configuration parameters needed for fetching climate data
    from the Danish Meteorological Institute GovCloud API, including parameter
    configurations, API settings, and performance parameters.

    Attributes:
        name (str): Human-readable name of the data source
        dataset (str): Dataset name for storage
        type (str): Type of the data source (api)
        description (str): Brief description of the data
        frequency (str): How often the data is updated
        bucket (str): GCS bucket name for raw data storage
        parameters (List[str]): List of climate parameters to fetch
        api_base_url (str): Base URL for DMI GovCloud API
        max_concurrent_fetches (int): Maximum number of concurrent API requests
        start_year (int): Starting year for historical data (DMI data available from 2011)
        temporal_resolution (str): Temporal resolution for data fetching (monthly/daily)
    """

    name: str = "Danish Meteorological Institute"
    dataset: str = "dmi"
    type: str = "api"
    description: str = "DMI monthly climate data from GovCloud API (2011-present)"
    frequency: str = "monthly"
    bucket: str = "landbrugsdata-raw-data"

    # DMI-specific configuration
    parameters: List[str] = ["pot_evaporation_makkink", "acc_precip"]
    api_base_url: str = "https://dmigw.govcloud.dk/v2/climateData"
    max_concurrent_fetches: int = 5
    start_year: int = 2011  # DMI grid data available from 2011
    temporal_resolution: str = "monthly"  # Changed from daily to monthly

    model_config = ConfigDict(frozen=True)


class DMIApiClient:
    """Client for interacting with DMI's climate data API"""

    def __init__(self, config: DMIBronzeConfig):
        self.config = config
        self.api_key = os.getenv("DMI_GOV_CLOUD_API_KEY")
        if not self.api_key:
            raise ValueError(
                "DMI_GOV_CLOUD_API_KEY environment variable is required. "
                "Please set this environment variable with your DMI GovCloud API key. "
                "For GitHub Actions, ensure the secret is configured in repository settings. "
                "For local development, add it to your .env file."
            )

        self.headers = {"Accept": "application/geo+json", "X-Gravitee-Api-Key": self.api_key}

    @retry(
        retry=retry_if_exception_type(aiohttp.ClientError),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
    )
    async def _make_request(
        self, session: aiohttp.ClientSession, endpoint: str, params: Dict[str, Any] = None
    ) -> Dict:
        """Make an authenticated request to the DMI API with error handling and retry logic"""
        if params is None:
            params = {}

        url = f"{self.config.api_base_url}/{endpoint}"

        try:
            async with session.get(url, headers=self.headers, params=params) as response:
                if response.status == 429:
                    raise aiohttp.ClientError("Rate limit exceeded")

                response.raise_for_status()
                text = await response.text()
                if not text:
                    raise aiohttp.ClientError("Empty response from DMI API")

                return await response.json()

        except Exception as e:
            raise aiohttp.ClientError(f"Error making request to DMI API: {str(e)}") from e

    async def fetch_grid_data(
        self,
        session: aiohttp.ClientSession,
        parameter_id: str,
        start_time: datetime,
        end_time: datetime,
    ) -> Dict:
        """
        Fetch raw climate grid data and return as JSON.

        For monthly data, the API will return monthly aggregated values when the
        datetime range spans multiple months. The DMI API automatically handles
        temporal aggregation based on the time span requested.
        """
        params = {
            "parameterId": parameter_id,
            "limit": 10000,  # Increased limit for monthly data spanning many years
            "datetime": f"{start_time.strftime('%Y-%m-%dT%H:%M:%SZ')}/{end_time.strftime('%Y-%m-%dT%H:%M:%SZ')}",
        }

        try:
            data = await self._make_request(session, "collections/10kmGridValue/items", params)
            if not data or "features" not in data or not data["features"]:
                return {"features": [], "parameter_id": parameter_id, "message": "No data returned"}

            return {
                "features": data["features"],
                "parameter_id": parameter_id,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "feature_count": len(data["features"]),
                "temporal_resolution": "monthly",
            }

        except Exception as e:
            return {
                "features": [],
                "parameter_id": parameter_id,
                "error": str(e),
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
            }


class DMIBronze(BaseSource[DMIBronzeConfig], BronzeJobInterface):
    """
    Bronze layer processing for DMI climate data.

    This class is responsible for fetching raw climate data from the DMI GovCloud API
    for multiple climate parameters. It handles async API communication, error handling,
    and stores the raw data in Google Cloud Storage for further processing.

    Processing flow:
    1. Initialize API client with configuration
    2. Calculate time range for data fetching (2011 to present)
    3. Fetch monthly aggregated data for each parameter concurrently
    4. Store raw responses in GCS
    5. Return structured data for in-memory passing to silver stage
    """

    def __init__(self, config: DMIBronzeConfig):
        """
        Initialize the DMIBronze source.

        Args:
            config (DMIBronzeConfig): Configuration for the data source"""
        super().__init__(config)
        self.api_client = DMIApiClient(config)

    def _calculate_time_range(self) -> tuple[datetime, datetime]:
        """
        Calculate the time range for data fetching from start_year to present.

        For monthly data, we fetch from January 1st of start_year to the current date.
        This maximizes the historical data coverage available from DMI.

        Returns:
            tuple[datetime, datetime]: Start and end datetime for data fetching
        """
        end_time = datetime.now()
        start_time = datetime(self.config.start_year, 1, 1)  # Start from January 1, 2011
        return start_time, end_time

    @timed(name="Fetching DMI parameter data")
    async def _fetch_parameter_data(
        self,
        session: aiohttp.ClientSession,
        parameter_id: str,
        start_time: datetime,
        end_time: datetime,
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch monthly-aggregated data for one climate parameter by iterating month-by-month.
        This avoids the 10 000-feature cap on the DMI endpoint and guarantees we retrieve
        the full history from start_year (2011) to present.
        """
        try:
            self.log.info(
                f"📡 Fetching DMI monthly grid data for {parameter_id} from {start_time:%Y-%m} to {end_time:%Y-%m}"
            )

            # Helper to advance one month without pandas
            def _add_one_month(dt: datetime) -> datetime:
                year, month = dt.year, dt.month
                if month == 12:
                    return datetime(year + 1, 1, 1)
                return datetime(year, month + 1, 1)

            all_features: list = []
            cur_start = datetime(start_time.year, start_time.month, 1)
            while cur_start < end_time:
                cur_end = _add_one_month(cur_start) - timedelta(seconds=1)
                if cur_end > end_time:
                    cur_end = end_time

                monthly_data = await self.api_client.fetch_grid_data(
                    session, parameter_id, cur_start, cur_end
                )

                if monthly_data.get("features"):
                    all_features.extend(monthly_data["features"])

                cur_start = _add_one_month(cur_start)

            if not all_features:
                self.log.warning(f"No monthly features returned for {parameter_id}")
                return None

            parameter_data = {
                "features": all_features,
                "parameter_id": parameter_id,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "feature_count": len(all_features),
                "temporal_resolution": "monthly",
            }

            # Metadata
            metadata = {
                "parameter_id": parameter_id,
                "fetch_time": datetime.now().isoformat(),
                "pipeline_start_time": self.pipeline_start_time.isoformat(),
                "api_base_url": self.config.api_base_url,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "temporal_resolution": self.config.temporal_resolution,
                "years_span": end_time.year - start_time.year + 1,
                "feature_count": len(all_features),
                "has_error": False,
            }

            # Persist
            self._save_parameter_data(parameter_id, parameter_data, metadata)

            return {
                "parameter_id": parameter_id,
                "data": parameter_data,
                "metadata": metadata,
            }
        except Exception as e:
            self.log.error(f"Error fetching monthly data for parameter {parameter_id}: {e}")
            return None

    def _save_parameter_data(
        self,
        parameter_id: str,
        parameter_data: Dict[str, Any],
        metadata: Dict[str, Any],
    ) -> None:
        """
        Save parameter data and metadata to storage.

        Args:
            parameter_id (str): Climate parameter identifier
            parameter_data (Dict[str, Any]): Raw parameter data from API
            metadata (Dict[str, Any]): Processing metadata
        """
        try:
            # Save parameter data
            data_path = (
                f"gs://{self.config.bucket}/bronze/dmi/{self.date_pattern}/{parameter_id}_data.json"
            )
            self.gcs_access.upload_json(parameter_data, data_path)

            # Save metadata
            metadata_path = f"gs://{self.config.bucket}/bronze/dmi/{self.date_pattern}/{parameter_id}_metadata.json"
            self.gcs_access.upload_json(metadata, metadata_path)

            self.log.info(f"Saved DMI parameter {parameter_id} data to storage")

        except Exception as e:
            self.log.error(f"Error saving data for parameter {parameter_id}: {e}")
            raise

    async def run(self) -> Optional[Dict[str, Any]]:
        """
        Run bronze processing for all configured DMI parameters.

        Returns:
            Optional[Dict[str, Any]]: Dictionary mapping parameter_id to parameter data,
                                    or None if processing fails
        """
        try:
            self.log.info(
                f"Starting DMI bronze processing for monthly data - parameters: {self.config.parameters}"
            )

            # Calculate time range
            start_time, end_time = self._calculate_time_range()
            years_span = end_time.year - start_time.year + 1
            self.log.info(
                f"Fetching DMI monthly data from {start_time.strftime('%Y-%m')} to {end_time.strftime('%Y-%m')} ({years_span} years)"
            )

            # Create semaphore to limit concurrent requests
            semaphore = asyncio.Semaphore(self.config.max_concurrent_fetches)

            async def fetch_with_semaphore(session, parameter_id):
                async with semaphore:
                    return await self._fetch_parameter_data(
                        session, parameter_id, start_time, end_time
                    )

            # Fetch data for all parameters concurrently
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(
                    total=600, connect=60, sock_read=300
                )  # Increased timeout for larger monthly datasets
            ) as session:
                tasks = [
                    fetch_with_semaphore(session, parameter_id)
                    for parameter_id in self.config.parameters
                ]

                results_list = await asyncio.gather(*tasks, return_exceptions=True)

            # Process results
            results = {}
            for i, result in enumerate(results_list):
                parameter_id = self.config.parameters[i]

                if isinstance(result, Exception):
                    self.log.error(
                        f"Exception fetching monthly data for parameter {parameter_id}: {result}"
                    )
                    continue

                if result:
                    results[parameter_id] = result
                else:
                    self.log.warning(f"Failed to fetch monthly data for parameter {parameter_id}")

            if not results:
                self.log.error("No DMI parameters were successfully processed")
                return None

            self.log.info(f"Successfully processed {len(results)} DMI parameters with monthly data")
            return results

        except Exception as e:
            self.log.error(f"Error in DMI bronze processing: {e}")
            return None
