"""
DST (Danmarks Statistik) Bronze Layer Implementation

This module implements the bronze layer data ingestion for Danish Statistics data.
It fetches raw data from the DST API and stores it in the bronze layer following
the medallion architecture pattern.

The module contains:
- DSTBronzeConfig: Configuration class for the DST data source
- DSTBronze: Implementation class for fetching and processing DST API data

The data is fetched from multiple DST tables (HST77, GARTN1, FRO, HALM1) with
proper error handling and retry logic for robustness.
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
from pydantic import ConfigDict
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from unified_pipeline.common.base import BaseJobConfig, BaseSource, BronzeJobInterface
from unified_pipeline.util.timing import timed

class DSTBronzeConfig(BaseJobConfig):
    """
    Configuration for the DST Bronze source.

    This class defines all configuration parameters needed for fetching data
    from Danmarks Statistik API, including table configurations, API settings,
    and performance parameters.

    Attributes:
        name (str): Human-readable name of the data source
        dataset (str): Dataset name for storage
        type (str): Type of the data source (api)
        description (str): Brief description of the data
        frequency (str): How often the data is updated
        bucket (str): GCS bucket name for raw data storage
        table_ids (List[str]): List of DST table IDs to fetch
        lang (str): Language for API responses
        api_base_url (str): Base URL for DST API
    """

    name: str = "Danmarks Statistik API"
    dataset: str = "dst"
    type: str = "api"
    description: str = "Danish Statistics data from DST API"
    frequency: str = "monthly"
    bucket: str = "landbrugsdata-raw-data"

    # DST-specific configuration
    table_ids: List[str] = ["HST77", "GARTN1", "FRO", "HALM1"]
    lang: str = "da"
    api_base_url: str = "https://api.statbank.dk/v1"

    model_config = ConfigDict(frozen=True)

class DSTApiClient:
    """Client for interacting with Danmarks Statistik API"""

    def __init__(self, base_url: str = "https://api.statbank.dk/v1", lang: str = "da"):
        self.base_url = base_url
        self.lang = lang
        self.session = requests.Session()
        # Set proper headers for DST API
        self.session.headers.update(
            {
                "User-Agent": "DanishStatsPipeline/1.0",
                "Accept": "application/json",
            }
        )

    def get_table_info(self, table_id: str) -> Optional[Dict[str, Any]]:
        """Fetch table metadata from the API"""
        try:
            url = f"{self.base_url}/tableinfo"
            params = {"table": table_id, "lang": self.lang, "format": "JSON"}

            logging.info(f"Fetching table info for {table_id}")
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()

            # Return raw JSON response exactly as received
            return response.json()

        except requests.RequestException as e:
            logging.error(f"Failed to fetch table info for {table_id}: {e}")
            return None

    def get_table_data(
        self,
        table_id: str,
        variables: Optional[List[str]] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Fetch table data from the API"""
        try:
            url = f"{self.base_url}/data"

            # Build request payload based on table configuration
            payload = self._build_request_payload(table_id, variables, start_time, end_time)

            logging.info(f"Fetching data for {table_id}")
            logging.debug(f"Request payload: {json.dumps(payload, indent=2)}")

            # Make request with exponential backoff
            response = self._make_request_with_retry(url, payload)
            if response is None:
                return None

            # Return raw JSON response exactly as received
            return response.json()

        except requests.RequestException as e:
            logging.error(f"Failed to fetch data for {table_id}: {e}")
            return None

    def _build_request_payload(
        self,
        table_id: str,
        variables: Optional[List[str]] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Build request payload based on table ID and parameters"""

        # Build payload based on known table configurations
        payload = {"table": table_id, "lang": self.lang, "format": "JSONSTAT"}

        # Add variable selections based on table type
        if table_id == "HST77":
            payload["variables"] = [
                {
                    "code": "OMRÅDE",
                    "values": ["000", "15", "04", "085", "07", "08", "09", "10", "081"],
                },
                {"code": "AFGRØDE", "values": ["*"]},
                {"code": "MÆNGDE4", "values": ["020"]},
                {"code": "Tid", "values": ["*"]},
            ]
        elif table_id == "GARTN1":
            payload["variables"] = [
                {
                    "code": "OMRÅDE",
                    "values": ["000", "15", "04", "085", "07", "08", "09", "10", "081"],
                },
                {"code": "TAL", "values": ["*"]},
                {"code": "AFGRØDE", "values": ["*"]},
                {"code": "Tid", "values": ["*"]},
            ]
        elif table_id == "FRO":
            payload["variables"] = [
                {"code": "AFGRØDE", "values": ["*"]},
                {"code": "MÆNGDE4", "values": ["*"]},
                {"code": "Tid", "values": ["*"]},
            ]
        elif table_id == "HALM1":
            payload["variables"] = [
                {"code": "AFGRØDE", "values": ["*"]},
                {"code": "MÆNGDE4", "values": ["*"]},
                {"code": "Tid", "values": ["*"]},
            ]
        else:
            # Generic payload for unknown tables
            payload["variables"] = [{"code": "*", "values": ["*"]}]

        return payload

    @retry(
        retry=retry_if_exception_type(requests.RequestException),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
    )
    def _make_request_with_retry(
        self, url: str, payload: Dict[str, Any]
    ) -> Optional[requests.Response]:
        """Make API request with exponential backoff retry logic"""
        try:
            response = self.session.post(url, json=payload, timeout=60)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            logging.warning(f"Request failed: {e}, retrying...")
            raise

class DSTBronze(BaseSource[DSTBronzeConfig], BronzeJobInterface):
    """
    Bronze layer processing for DST data.

    This class is responsible for fetching raw data from the Danmarks Statistik API
    for multiple table types. It handles API communication, error handling, and
    stores the raw data in Google Cloud Storage for further processing.

    Processing flow:
    1. Initialize API client with configuration
    2. Fetch data and metadata for each configured table
    3. Store raw responses in GCS
    4. Return structured data for in-memory passing to silver stage
    """

    def __init__(self, config: DSTBronzeConfig):
        """
        Initialize the DSTBronze source.

        Args:
            config (DSTBronzeConfig): Configuration for the data source        """
        super().__init__(config)
        self.api_client = DSTApiClient(base_url=self.config.api_base_url, lang=self.config.lang)

    @timed(name="Fetching DST table data")
    async def _fetch_table_data(self, table_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch data and metadata for a specific DST table.

        Args:
            table_id (str): DST table identifier

        Returns:
            Optional[Dict[str, Any]]: Dictionary containing table data and metadata,
                                    or None if fetching fails
        """
        try:
            self.log.info(f"Fetching data for DST table {table_id}")

            # Fetch table info (metadata)
            table_info = self.api_client.get_table_info(table_id)
            if not table_info:
                self.log.warning(f"Could not fetch table info for {table_id}")

            # Fetch table data
            table_data = self.api_client.get_table_data(table_id)
            if not table_data:
                self.log.error(f"Could not fetch table data for {table_id}")
                return None

            # Create metadata
            metadata = {
                "table_id": table_id,
                "fetch_time": datetime.now().isoformat(),
                "pipeline_start_time": self.pipeline_start_time.isoformat(),
                "api_base_url": self.config.api_base_url,
                "language": self.config.lang,
                "record_count": len(table_data.get("value", [])) if "value" in table_data else 0,
            }

            # Save raw data to storage
            self._save_table_data(table_id, table_data, table_info, metadata)

            return {
                "table_id": table_id,
                "data": table_data,
                "table_info": table_info,
                "metadata": metadata,
            }

        except Exception as e:
            self.log.error(f"Error fetching data for table {table_id}: {e}")
            return None

    def _save_table_data(
        self,
        table_id: str,
        table_data: Dict[str, Any],
        table_info: Optional[Dict[str, Any]],
        metadata: Dict[str, Any],
    ) -> None:
        """
        Save table data, info, and metadata to storage.

        Args:
            table_id (str): DST table identifier
            table_data (Dict[str, Any]): Raw table data from API
            table_info (Optional[Dict[str, Any]]): Table metadata from API
            metadata (Dict[str, Any]): Processing metadata
        """
        try:
            # Save table data
            data_path = (
                f"gs://{self.config.bucket}/bronze/dst/{self.date_pattern}/{table_id}_data.json"
            )
            self.gcs_access.upload_json(table_data, data_path)

            # Save table info if available
            if table_info:
                info_path = f"gs://{self.config.bucket}/bronze/dst/{self.date_pattern}/{table_id}_tableinfo.json"
                self.gcs_access.upload_json(table_info, info_path)

            # Save metadata
            metadata_path = (
                f"gs://{self.config.bucket}/bronze/dst/{self.date_pattern}/{table_id}_metadata.json"
            )
            self.gcs_access.upload_json(metadata, metadata_path)

            self.log.info(f"Saved DST table {table_id} data to storage")

        except Exception as e:
            self.log.error(f"Error saving data for table {table_id}: {e}")
            raise

    async def run(self) -> Optional[Dict[str, Any]]:
        """
        Run bronze processing for all configured DST tables.

        Returns:
            Optional[Dict[str, Any]]: Dictionary mapping table_id to table data,
                                    or None if processing fails
        """
        try:
            self.log.info(f"Starting DST bronze processing for tables: {self.config.table_ids}")

            results = {}
            for table_id in self.config.table_ids:
                table_result = await self._fetch_table_data(table_id)
                if table_result:
                    results[table_id] = table_result
                else:
                    self.log.warning(f"Failed to fetch data for table {table_id}")

            if not results:
                self.log.error("No DST tables were successfully processed")
                return None

            self.log.info(f"Successfully processed {len(results)} DST tables")
            return results

        except Exception as e:
            self.log.error(f"Error in DST bronze processing: {e}")
            return None
