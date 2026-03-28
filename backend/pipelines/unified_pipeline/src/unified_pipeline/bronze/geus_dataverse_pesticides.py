"""
Bronze layer data ingestion for GEUS Dataverse Pesticides and PFAS data.

This module handles the download of pesticide and PFAS groundwater data from the GEUS
Dataverse repository for the River Basin Management Plan (Vandområdeplan 4). It downloads
raw .rds files and stores them in GCS for processing in the silver layer.

Data source:
- Dataset: "Groundwater chemistry data for the fourth River Basin Management Plan"
- DOI: https://doi.org/10.22008/FK2/IHVDXL
- Files:
  - AM_pest.rds (Annual Mean pesticide concentrations) - 633 substances, 4.2M+ analyses
  - AM_pfas.rds (Annual Mean PFAS concentrations) - 26 substances, 397k analyses
- Format: R Data Serialization format (.rds)

Key advantages over WFS:
- 633 pesticide substances + 26 PFAS substances (vs 6-8 via WFS)
- 4.6M+ total analyses (vs ~50k via WFS)
- Full temporal data 1981-2025 with reliable PROEVEAAR column
- Pre-processed annual means ready for analysis
"""

import ssl
from datetime import datetime
from typing import Any

import aiohttp
import certifi
from pydantic import ConfigDict
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from unified_pipeline.common.base import BaseJobConfig, BaseSource, BronzeJobInterface
from unified_pipeline.util.timing import AsyncTimer


class GEUSDataversePesticidesBronzeConfig(BaseJobConfig):
    """
    Configuration for the GEUS Dataverse Pesticides Bronze source.

    This class defines configuration parameters for downloading pesticide and PFAS
    groundwater data from the GEUS Dataverse repository.

    Attributes:
        name: Human-readable name of the data source
        dataset: Name of the dataset in storage
        type: Type of the data source (dataverse)
        description: Brief description of the data
        pest_url: Direct download URL for the AM_pest.rds file
        pfas_url: Direct download URL for the AM_pfas.rds file
        frequency: How often the data is updated (annual for VP4 dataset)
        bucket: GCS bucket name for raw data storage
        source_crs: Coordinate reference system of source data
        request_timeout: Timeout for download requests in seconds
    """

    name: str = "GEUS Dataverse Pesticides and PFAS"
    dataset: str = "geus_dataverse_pesticides"
    type: str = "dataverse"
    description: str = (
        "Groundwater pesticide and PFAS data from GEUS Dataverse for VP4 "
        "(633 pesticides + 26 PFAS substances, 4.6M+ analyses, 1981-2025)"
    )
    # Direct download URLs from https://doi.org/10.22008/FK2/IHVDXL
    pest_url: str = "https://dataverse.geus.dk/api/access/datafile/99149"  # AM_pest.rds
    pfas_url: str = "https://dataverse.geus.dk/api/access/datafile/99150"  # AM_pfas.rds
    frequency: str = "annual"
    bucket: str = "landbruget-data"
    source_crs: str = "EPSG:25832"  # Data uses UTM32 EUREF89 coordinates

    # Download settings
    request_timeout: int = 300  # 5 minutes for larger files

    model_config = ConfigDict(frozen=True)


class GEUSDataversePesticidesBronze(
    BaseSource[GEUSDataversePesticidesBronzeConfig], BronzeJobInterface
):
    """
    Bronze layer processing for GEUS Dataverse pesticides and PFAS data.

    This class downloads the raw AM_pest.rds and AM_pfas.rds files from GEUS
    Dataverse and stores them in GCS. The silver layer will then read and
    convert these to parquet format.

    Processing flow:
    1. Download AM_pest.rds and AM_pfas.rds from GEUS Dataverse API
    2. Store raw .rds files in GCS bronze layer
    3. Save metadata manifest for silver layer
    """

    def __init__(self, config: GEUSDataversePesticidesBronzeConfig):
        """
        Initialize the GEUSDataversePesticidesBronze source.

        Args:
            config: Configuration for the data source
        """
        super().__init__(config)

    @retry(
        retry=retry_if_exception_type(Exception),
        wait=wait_exponential(multiplier=1, min=4, max=30),
        stop=stop_after_attempt(3),
    )
    async def _download_rds_file(
        self, session: aiohttp.ClientSession, url: str, filename: str
    ) -> bytes:
        """
        Download an .rds file from GEUS Dataverse.

        Args:
            session: HTTP session for making requests
            url: Download URL for the file
            filename: Name of the file being downloaded (for logging)

        Returns:
            Raw bytes of the .rds file

        Raises:
            Exception: If download fails after retries
        """
        self.log.info(f"Downloading {filename} from {url}")

        async with session.get(url) as response:
            if response.status != 200:
                err_msg = f"Failed to download {filename}. Status: {response.status}"
                self.log.error(err_msg)
                raise Exception(err_msg)

            # Read the entire file into memory
            content = await response.read()
            content_length = len(content)

            self.log.info(f"Downloaded {filename}: {content_length / (1024 * 1024):.1f} MB")

            return content

    def _save_rds_to_storage(self, rds_content: bytes, filename: str) -> str:
        """
        Save a raw .rds file to storage.

        Path structure: bronze/{dataset}/{run_timestamp}/{filename}

        Args:
            rds_content: Raw bytes of the .rds file
            filename: Name of the file (AM_pest.rds or AM_pfas.rds)

        Returns:
            Storage path where the file was saved (relative to bucket)
        """
        run_timestamp = self.date_pattern
        relative_path = f"bronze/{self.config.dataset}/{run_timestamp}/{filename}"
        storage_path = f"{self.config.bucket}/{relative_path}"

        # Stream binary content directly to storage using s3fs (R2)
        with self.storage.fs.open(storage_path, "wb") as f:
            f.write(rds_content)

        self.log.info(f"Saved {filename} to {storage_path}")
        return relative_path

    async def run(self) -> dict[str, Any] | None:
        """
        Run the bronze layer processing pipeline.

        Downloads the raw AM_pest.rds and AM_pfas.rds files from GEUS Dataverse
        and stores them in GCS for the silver layer to process.

        Returns:
            Manifest dictionary with paths and metadata for silver layer,
            or None if processing fails
        """
        async with AsyncTimer("Running GEUS Dataverse Pesticides bronze job"):
            self.log.info("Running GEUS Dataverse Pesticides and PFAS bronze job")

            # Set source CRS for tracking
            self.set_source_crs(self.config.source_crs)

            # Configure SSL context
            ssl_context = ssl.create_default_context(cafile=certifi.where())
            connector = aiohttp.TCPConnector(ssl=ssl_context)
            timeout = aiohttp.ClientTimeout(total=self.config.request_timeout)

            async with aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
            ) as session:
                try:
                    # Download both .rds files
                    pest_content = await self._download_rds_file(
                        session, self.config.pest_url, "AM_pest.rds"
                    )
                    pfas_content = await self._download_rds_file(
                        session, self.config.pfas_url, "AM_pfas.rds"
                    )

                    # Save to GCS
                    pest_path = self._save_rds_to_storage(pest_content, "AM_pest.rds")
                    pfas_path = self._save_rds_to_storage(pfas_content, "AM_pfas.rds")

                    # Create manifest for silver layer
                    run_timestamp = self.date_pattern
                    total_size = len(pest_content) + len(pfas_content)
                    manifest = {
                        "dataset": self.config.dataset,
                        "bucket": self.config.bucket,
                        "source_crs": self.config.source_crs,
                        "run_timestamp": run_timestamp,
                        "source_doi": "https://doi.org/10.22008/FK2/IHVDXL",
                        "downloaded_at": datetime.now().isoformat(),
                        # Pesticide data
                        "pest_url": self.config.pest_url,
                        "pest_file_size_bytes": len(pest_content),
                        "rds_path": pest_path,  # Backwards compatible
                        "pest_rds_path": pest_path,
                        # PFAS data
                        "pfas_url": self.config.pfas_url,
                        "pfas_file_size_bytes": len(pfas_content),
                        "pfas_rds_path": pfas_path,
                    }

                    # Save manifest to GCS
                    manifest_path = (
                        f"{self.config.bucket}/bronze/{self.config.dataset}/"
                        f"{run_timestamp}/manifest.json"
                    )
                    self.storage.upload_json(manifest, manifest_path)

                    # Create and save pipeline metadata for data tracing
                    if self.pipeline_metadata_manager and self.processing_start_time:
                        try:
                            import time

                            processing_duration = time.time() - self.processing_start_time

                            pipeline_metadata = self.pipeline_metadata_manager.create_metadata(
                                source_key="geus_dataverse_pesticides",
                                record_count=2,  # Two files
                                processing_duration=processing_duration,
                            )

                            metadata_path = (
                                f"{self.config.bucket}/bronze/{self.config.dataset}/"
                                f"{run_timestamp}/pipeline_metadata.json"
                            )
                            self.storage.upload_json(pipeline_metadata.model_dump(), metadata_path)
                            self.log.info(f"✅ Pipeline metadata saved to {metadata_path}")
                        except Exception as e:
                            self.log.warning(f"⚠️ Failed to create pipeline metadata: {e}")

                    self.log.info(
                        f"GEUS Dataverse bronze job completed successfully. "
                        f"Downloaded {total_size / (1024 * 1024):.1f} MB total "
                        f"(pest: {len(pest_content) / (1024 * 1024):.1f} MB, "
                        f"pfas: {len(pfas_content) / (1024 * 1024):.1f} MB)"
                    )

                    return manifest

                except Exception as e:
                    self.log.error(f"Error occurred while downloading data: {e}")
                    raise e
