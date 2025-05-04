import asyncio
import ssl
import pandas as pd
from typing import Optional
import xml.etree.ElementTree as ET


import aiohttp
from unified_pipeline.bronze.base import BaseSource
from unified_pipeline.model.bnbo_status import BNBOStatusConfig
from unified_pipeline.util.gcs_util import GCSUtil


class BNBOStatusBronze(BaseSource[BNBOStatusConfig]):
    def __init__(self, config: BNBOStatusConfig, gcs_util: GCSUtil):
        super().__init__(config, gcs_util)
        self.config = config
        # self.bucket = gcs_util.get_bucket(config.bucket)

    def _get_params(self, start_index: int = 0) -> dict:
        """Get WFS request parameters"""
        return {
            "SERVICE": "WFS",
            "REQUEST": "GetFeature",
            "VERSION": "2.0.0",
            "TYPENAMES": "dai:status_bnbo",
            "STARTINDEX": str(start_index),
            "COUNT": str(self.config.batch_size),
            "SRSNAME": "urn:ogc:def:crs:EPSG::25832",
        }

    async def _fetch_raw_data(self) -> Optional[str]:
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        connector = aiohttp.TCPConnector(ssl=ssl_context)
        async with aiohttp.ClientSession(
            headers=self.config.headers, connector=connector
        ) as session:
            # Get total count
            params = self._get_params(start_index=0)
            async with session.get(self.config.url, params=params) as response:
                if response.status != 200:
                    self.log.error(f"Failed initial request. Status: {response.status}")
                    return None

                text = await response.text()
                root = ET.fromstring(text)
                total_features = int(root.get("numberMatched", "0"))
                self.log.info(f"Found {total_features:,} total features")

                return text

    async def _save_raw_data(self, raw_data: str) -> None:
        """Save raw data to GCS"""
        df = pd.DataFrame(
            {
                "payload": [raw_data],
                "source": [self.config.name],
                "created_at": [pd.Timestamp.now()],
                "updated_at": [pd.Timestamp.now()],
            }
        )
        # Save to Delta Lake in GCS

    async def run(self) -> None:
        """Run the data source"""
        self.log.info("Running BNBO status data source")
        raw_data = await self._fetch_raw_data()
        if raw_data is None:
            self.log.error("Failed to fetch raw data")
            return
        self.log.info("Fetched raw data successfully")
        await self._save_raw_data(raw_data)
        self.log.info("Saved raw data successfully")
