"""
DMI Climate Data Extraction Layer
Handles API communication and data retrieval from the Danish Meteorological Institute.
Saves raw data without any transformations.
"""

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import aiohttp
from fastapi import HTTPException

# Configure logging
logger = logging.getLogger(__name__)


class DMIConfig:
    """Configuration for DMI API access"""

    def __init__(self):
        self.api_key = os.getenv("DMI_GOV_CLOUD_API_KEY")
        if not self.api_key:
            raise ValueError("DMI_GOV_CLOUD_API_KEY environment variable is required")

        self.base_url = "https://dmigw.govcloud.dk/v2/climateData"
        self.max_retries = int(os.getenv("MAX_RETRIES", 3))
        self.retry_delay = int(os.getenv("RETRY_DELAY", 5))


class DMIApiClient:
    """Client for interacting with DMI's climate data API"""

    def __init__(self, config: DMIConfig):
        self.config = config
        self.headers = {"Accept": "application/geo+json", "X-Gravitee-Api-Key": self.config.api_key}

    async def _make_request(self, endpoint: str, params: Dict[str, Any] = None) -> Dict:
        """Make an authenticated request to the DMI API with error handling"""
        if params is None:
            params = {}

        url = f"{self.config.base_url}/{endpoint}"
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, headers=self.headers, params=params) as response:
                    if response.status == 429:
                        logger.warning("Rate limit exceeded")
                        raise HTTPException(status_code=429, detail="Rate limit exceeded")

                    text = await response.text()
                    if not text:
                        raise HTTPException(status_code=500, detail="Empty response from DMI API")

                    return await response.json()
            except Exception as e:
                logger.error(f"Error making request to DMI API: {str(e)}")
                raise

    def save_raw_data(self, data: Dict, output_dir: Path, filename: str) -> bool:
        """Save raw JSON data to the bronze layer"""
        try:
            # Create output directory if it doesn't exist
            output_dir.mkdir(parents=True, exist_ok=True)

            # Save as JSON file
            output_path = output_dir / f"{filename}.json"
            with open(output_path, "w") as f:
                json.dump(data, f, indent=2)

            logger.info(f"Successfully saved raw data to {output_path}")
            return True
        except Exception as e:
            logger.error(f"Error saving raw data to {output_dir}: {str(e)}")
            return False

    async def fetch_grid_data(
        self, parameter_id: str, start_time: datetime, end_time: datetime, output_dir: Path = None
    ) -> Dict:
        """Fetch raw climate grid data and return as JSON"""
        params = {
            "parameterId": parameter_id,
            "limit": 1000,
            "datetime": f"{start_time.strftime('%Y-%m-%dT%H:%M:%SZ')}/{end_time.strftime('%Y-%m-%dT%H:%M:%SZ')}",
        }

        try:
            data = await self._make_request("collections/10kmGridValue/items", params)
            if not data or "features" not in data or not data["features"]:
                logger.warning(f"No data returned for parameter {parameter_id}")
                return {"features": []}

            logger.info(f"Successfully fetched {len(data['features'])} records for {parameter_id}")

            # Save raw data if output directory is provided
            if output_dir:
                self.save_raw_data(data, output_dir, f"{parameter_id}_raw")

            return data

        except Exception as e:
            logger.error(f"Error fetching grid data for {parameter_id}: {str(e)}")
            return {"features": []}
