import asyncio
import os
import re
from typing import List, Optional

import aiohttp
from dotenv import load_dotenv
from pydantic import ConfigDict

from unified_pipeline.common.base import BaseJobConfig, BaseSource, BronzeJobInterface

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


class SpfSuBronze(BaseSource[SpfSuBronzeConfig], BronzeJobInterface):
    def __init__(self, config: SpfSuBronzeConfig) -> None:
        super().__init__(config)

    def fetch_silver_data_chr(self) -> List[int]:
        """
        Fetch all herd_number values from parquet files in the latest silver/chr folder.
        """
        self.log.info("Fetching CHR data to get herd numbers for SPF SU")

        # Use modern GCS access to find the latest CHR data
        gcs_pattern = f"gs://{self.config.bucket}/silver/chr/*/*.parquet"
        files = self.gcs_access.list_files(gcs_pattern)

        if not files:
            self.log.warning("No CHR silver data found.")
            return []

        # The latest timestamp will be last when sorted
        latest_file = sorted(files, reverse=True)[0]
        timestamp_match = re.search(r"/(\d{8}_\d{6})/", latest_file)
        if not timestamp_match:
            self.log.warning(f"Could not extract timestamp from {latest_file}")
            return []

        latest_ts = timestamp_match.group(1)
        self.log.info(f"Found latest CHR data from timestamp: {latest_ts}")

        # Get all files from the latest timestamp
        latest_files_pattern = f"gs://{self.config.bucket}/silver/chr/{latest_ts}/*.parquet"
        latest_files = self.gcs_access.list_files(latest_files_pattern)

        if not latest_files:
            self.log.warning(f"No files found for latest timestamp: {latest_ts}")
            return []

        # Create a unified table from all parquet files of the latest run
        table_name = "chr_silver_data"
        file_list_str = ", ".join([f"'{f}'" for f in latest_files])
        self.conn.execute(
            f"CREATE OR REPLACE TABLE {table_name} AS SELECT herd_number FROM read_parquet([{file_list_str}])"
        )

        # Fetch herd numbers and filter out nulls
        herd_numbers_df = self.conn.execute(
            f"SELECT DISTINCT herd_number FROM {table_name} WHERE herd_number IS NOT NULL"
        ).fetchdf()

        return herd_numbers_df["herd_number"].tolist()

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

    async def run(self) -> Optional[List[dict]]:
        """
        Run the SPF SU bronze layer job.

        This method fetches SPF SU data and saves it to storage while also
        returning the data for in-memory passing to the silver stage.

        Returns:
            Optional[List[dict]]: Raw SPF SU data that can be passed to silver stage,
                                 or None if processing fails
        """
        self.log.info("Running SPF SU bronze layer job")
        raw_data = await self._fetch_raw_data()
        if raw_data is None:
            self.log.error("Failed to fetch raw data")
            return None
        self.log.info("Fetched raw data successfully")

        # Save using new unified method
        self._save_data(raw_data, self.config.dataset, self.config.bucket, stage="bronze")
        self.log.info("Saved raw data successfully")

        # Return data for in-memory passing
        return raw_data
