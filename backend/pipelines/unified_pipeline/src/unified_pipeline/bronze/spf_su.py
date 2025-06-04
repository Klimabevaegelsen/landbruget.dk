import asyncio
import xml.etree.ElementTree as ET
from asyncio import Semaphore

import aiohttp
from pydantic import ConfigDict
from dotenv import load_dotenv
from unified_pipeline.common.base import BaseJobConfig, BaseSource
from unified_pipeline.util.gcs_util import GCSUtil
import os
import logging
import time
from datetime import datetime
import duckdb


# async def fetch(session, url):
#     async with session.get(url) as response:
#         response.raise_for_status()
#         return await response.text()

class SpfSuBronzeConfig(BaseJobConfig):
    name: str = "Danish SPF SU"
    dataset: str = "spf_su"
    type: str = "wfs"
    description: str = "SPF SU from WFS"
    load_dotenv()
    frequency: str = "weekly"
    bucket: str = os.getenv("GCS_BUCKET")
    
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)
    max_concurrent: int = os.getenv("MAX_CONCURRENT", 20)
    

class SpfSuBronze(BaseSource[SpfSuBronzeConfig]):
    
    def __init__(self, config: SpfSuBronzeConfig, gcs_util: GCSUtil) -> None:
        super().__init__(config, gcs_util)
    
    def fetch_silver_data_chr(self):
        """
        Fetch all herd_number values from parquet files in the latest silver/chr timestamp folder.
        """
        import pandas as pd
        import tempfile

        client = self.gcs_util.get_gcs_client()
        prefix = "silver/chr/"
        blobs = client.list_blobs(self.config.bucket, prefix=prefix)
        ts_map: dict[str, list[str]] = {}
        for blob in blobs:
            if not blob.name.endswith(".parquet"):
                continue
            parts = blob.name.split("/")
            if len(parts) < 4:
                continue
            ts = parts[2]
            ts_map.setdefault(ts, []).append(blob.name)
        if not ts_map:
            return []
        latest_ts = max(ts_map.keys())
        # Download and read parquet files to extract herd_number
        bucket = client.bucket(self.config.bucket)
        temp_dir = tempfile.mkdtemp(prefix=f"spf_su_{latest_ts}_")
        herd_numbers: list = []
        for path in ts_map[latest_ts]:
            local = os.path.join(temp_dir, os.path.basename(path))
            bucket.blob(path).download_to_filename(local)
            chr_data = self.conn.execute(f"SELECT * FROM read_parquet('{local}')").fetchdf()
            if "herd_number" in chr_data.columns:
                herd_numbers.extend(chr_data["herd_number"].dropna().tolist())
        return herd_numbers
    
    async def get_spf_su(self, session, herd_number: int):
        self.log.info(f"Fetching SPF SU for herd number: {herd_number}")
        url = f"https://spfsus.dk/api/farm/{herd_number}/da/false/0/0/false?format=json"
        response = await session.get(url)
        if response.status != 200:
            self.log.error(f"Herd number does not have data: {herd_number}")
            return None
        return await response.json()
    
    async def _fetch_raw_data(self):
        herd_numbers = list(set(self.fetch_silver_data_chr()))
        self.log.info(f"Fetching SPF SU for {len(herd_numbers)} herd numbers")
        sem = asyncio.Semaphore(self.config.max_concurrent)
        async with aiohttp.ClientSession() as session:
            async def bounded(item):
                async with sem:
                    return await self.get_spf_su(session, item)
            tasks = [bounded(item) for item in herd_numbers] 
            raw_data = await asyncio.gather(*tasks)
        return [item for item in raw_data if item is not None]
    
    async def run(self) -> None:
        self.log.info("Running SPF SU bronze layer job")
        raw_data = await self._fetch_raw_data()
        if raw_data is None:
            self.log.error("Failed to fetch raw data")
            return
        self.log.info("Fetched raw data successfully")
        self._save_raw_json(raw_data, self.config.dataset, self.config.bucket)
        self.log.info("Saved raw data successfully")
