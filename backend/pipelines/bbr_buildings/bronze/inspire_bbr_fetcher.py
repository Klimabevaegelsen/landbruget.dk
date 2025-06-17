"""
INSPIRE BBR Data Fetcher for the BBR Buildings Pipeline.

This module handles fetching the DK_INSPIRE_BBR.zip file from SDFE's FTP server
by parsing the FTP page to get the actual download link dynamically.
"""

import json
import logging
import re
import shutil
import zipfile
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from config import Settings


class InspireBBRFetcher:
    """Fetches INSPIRE BBR data from SDFE FTP server."""

    def __init__(self, settings: Settings, logger: logging.Logger):
        """
        Initialize the INSPIRE BBR fetcher.

        Args:
            settings: Pipeline settings
            logger: Logger instance
        """
        self.settings = settings
        self.logger = logger
        self.session = requests.Session()
        self.session.headers.update(
            {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        )

    def fetch_data(
        self, output_dir: Path, sample_size: int | None = None, return_data: bool = False
    ):
        """
        Fetch INSPIRE BBR data from SDFE FTP server.

        Args:
            output_dir: Directory to save the data
            sample_size: Optional sample size for testing
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_dir = output_dir / timestamp
        run_dir.mkdir(parents=True, exist_ok=True)

        self.logger.info(f"Starting INSPIRE BBR data fetch to {run_dir}")

        try:
            # Parse the FTP page to get the actual download link
            download_url, file_info = self._parse_ftp_page()

            if not download_url:
                raise ValueError("Could not extract download URL from FTP page")

            self.logger.info(f"Found download URL: {download_url}")
            self.logger.info(f"File info: {file_info}")

            # Download the ZIP file
            zip_path = run_dir / "DK_INSPIRE_BBR.zip"
            self._download_file(download_url, zip_path, file_info.get("size"))

            # Extract the GPKG file
            gpkg_path = self._extract_gpkg(zip_path, run_dir)

            # Save metadata
            self._save_metadata(run_dir, file_info, download_url, sample_size)

            # Clean up ZIP file to save space
            if zip_path.exists():
                zip_path.unlink()
                self.logger.info("Cleaned up ZIP file to save storage space")

            self.logger.info(f"Successfully fetched INSPIRE BBR data to {gpkg_path}")

            # Optionally return data for in-memory processing
            if return_data:
                self.logger.info("Loading data for in-memory processing")
                try:
                    import geopandas as gpd
                    import pandas as pd

                    # Load both building and otherConstruction layers for comprehensive analysis
                    self.logger.info("Loading 'building' layer from GPKG file")
                    buildings_data = gpd.read_file(gpkg_path, layer="building")
                    self.logger.info(f"Loaded {len(buildings_data):,} building records")

                    self.logger.info("Loading 'otherConstruction' layer from GPKG file")
                    constructions_data = gpd.read_file(gpkg_path, layer="otherConstruction")
                    self.logger.info(
                        f"Loaded {len(constructions_data):,} other construction records"
                    )

                    # Add a source column to distinguish between the two layers
                    buildings_data["layer_source"] = "building"
                    constructions_data["layer_source"] = "otherConstruction"

                    # Combine both datasets
                    data = gpd.GeoDataFrame(
                        pd.concat([buildings_data, constructions_data], ignore_index=True)
                    )
                    self.logger.info(
                        f"Combined total: {len(data):,} records for in-memory processing"
                    )
                    return {
                        "data": data,
                        "gpkg_path": gpkg_path,
                        "metadata": {
                            "source": "inspire_bbr",
                            "sample_size": sample_size,
                            "file_info": file_info,
                            "download_url": download_url,
                        },
                    }
                except Exception as e:
                    self.logger.warning(f"Failed to load data for in-memory processing: {e}")
                    # Still return path info for fallback
                    return {
                        "gpkg_path": gpkg_path,
                        "metadata": {
                            "source": "inspire_bbr",
                            "sample_size": sample_size,
                            "file_info": file_info,
                            "download_url": download_url,
                        },
                    }

            return None

        except Exception as e:
            self.logger.error(f"Failed to fetch INSPIRE BBR data: {e}")
            raise

    def _parse_ftp_page(self) -> tuple[str | None, dict]:
        """
        Parse the SDFE FTP page to extract the download URL and file information.

        Returns:
            Tuple of (download_url, file_info)
        """
        self.logger.info(f"Parsing FTP page: {self.settings.sdfe_ftp_base_url}")

        try:
            response = self.session.get(self.settings.sdfe_ftp_base_url, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "html.parser")

            # Look for download link
            download_link = None
            file_info = {}

            # Method 1: Look for direct download link with DK_INSPIRE_BBR
            for link in soup.find_all("a", href=True):
                href = link.get("href")
                if href and "DK_INSPIRE_BBR" in href and "download" in href:
                    # Convert relative URL to absolute
                    if href.startswith("/"):
                        base_url = f"{urlparse(self.settings.sdfe_ftp_base_url).scheme}://{urlparse(self.settings.sdfe_ftp_base_url).netloc}"
                        download_link = urljoin(base_url, href)
                    elif href.startswith("main.html"):
                        base_url = self.settings.sdfe_ftp_base_url.rsplit("/", 1)[0]
                        download_link = f"{base_url}/{href}"
                    else:
                        download_link = href
                    break

            # Method 2: Parse table structure for file information
            for row in soup.find_all("tr"):
                cells = row.find_all(["td", "th"])
                if len(cells) >= 3:
                    # Look for DK_INSPIRE_BBR.zip in the filename column
                    filename_cell = cells[0] if cells else None
                    if filename_cell and "DK_INSPIRE_BBR.zip" in filename_cell.get_text():
                        # Extract file info
                        if len(cells) > 1:
                            size_text = cells[1].get_text().strip()
                            file_info["size_text"] = size_text
                            # Parse size (e.g., "761.9 MB")
                            size_match = re.search(
                                r"([\d.]+)\s*(MB|GB|KB)", size_text, re.IGNORECASE
                            )
                            if size_match:
                                size_value = float(size_match.group(1))
                                size_unit = size_match.group(2).upper()
                                multiplier = {"KB": 1024, "MB": 1024**2, "GB": 1024**3}
                                file_info["size"] = int(size_value * multiplier.get(size_unit, 1))

                        if len(cells) > 2:
                            file_info["modified"] = cells[2].get_text().strip()

                        # Look for download link in this row
                        for link in row.find_all("a", href=True):
                            href = link.get("href")
                            if href and "download" in href:
                                if href.startswith("/"):
                                    base_url = f"{urlparse(self.settings.sdfe_ftp_base_url).scheme}://{urlparse(self.settings.sdfe_ftp_base_url).netloc}"
                                    download_link = urljoin(base_url, href)
                                elif href.startswith("main.html"):
                                    base_url = self.settings.sdfe_ftp_base_url.rsplit("/", 1)[0]
                                    download_link = f"{base_url}/{href}"
                                else:
                                    download_link = href
                                break
                        break

            # If we still don't have a download link, try extracting from JavaScript or other sources
            if not download_link:
                # Look for any links containing the weblink parameter
                script_tags = soup.find_all("script")
                for script in script_tags:
                    if script.string and "weblink" in script.string:
                        # Extract potential weblink from JavaScript
                        weblink_match = re.search(r"weblink=([a-f0-9]+)", script.string)
                        if weblink_match:
                            weblink = weblink_match.group(1)
                            base_url = self.settings.sdfe_ftp_base_url.rsplit("?", 1)[0]
                            download_link = f"{base_url}?download&weblink={weblink}&realfilename=DK_INSPIRE_BBR.zip"
                            break

            # Last resort: use the base URL structure we know works
            if not download_link:
                # Extract weblink from the original URL
                weblink_match = re.search(r"weblink=([a-f0-9]+)", self.settings.sdfe_ftp_base_url)
                if weblink_match:
                    weblink = weblink_match.group(1)
                    base_url = self.settings.sdfe_ftp_base_url.rsplit("?", 1)[0]
                    download_link = (
                        f"{base_url}?download&weblink={weblink}&realfilename=DK_INSPIRE_BBR.zip"
                    )

            self.logger.info(f"Extracted download link: {download_link}")
            self.logger.info(f"File info: {file_info}")

            return download_link, file_info

        except Exception as e:
            self.logger.error(f"Failed to parse FTP page: {e}")
            raise

    def _download_file(self, url: str, output_path: Path, expected_size: int | None = None) -> None:
        """
        Download a file from the given URL.

        Args:
            url: URL to download from
            output_path: Path to save the file
            expected_size: Expected file size in bytes for validation
        """
        self.logger.info(f"Downloading file from {url} to {output_path}")

        try:
            with self.session.get(url, stream=True, timeout=60) as response:
                response.raise_for_status()

                # Get file size from headers if available
                content_length = response.headers.get("content-length")
                if content_length:
                    file_size = int(content_length)
                    self.logger.info(f"File size: {file_size / (1024**2):.2f} MB")

                    # Validate against expected size if provided
                    if expected_size and abs(file_size - expected_size) > (
                        expected_size * 0.05
                    ):  # 5% tolerance
                        self.logger.warning(
                            f"File size mismatch: expected {expected_size}, got {file_size}"
                        )

                # Download with progress tracking
                downloaded = 0
                with open(output_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)

                            # Log progress every 100MB
                            if downloaded % (100 * 1024 * 1024) == 0:
                                self.logger.info(f"Downloaded {downloaded / (1024**2):.2f} MB")

                self.logger.info(f"Download completed: {downloaded / (1024**2):.2f} MB")

        except Exception as e:
            if output_path.exists():
                output_path.unlink()
            self.logger.error(f"Failed to download file: {e}")
            raise

    def _extract_gpkg(self, zip_path: Path, output_dir: Path) -> Path:
        """
        Extract the GPKG file from the downloaded ZIP.

        Args:
            zip_path: Path to the ZIP file
            output_dir: Directory to extract to

        Returns:
            Path to the extracted GPKG file
        """
        self.logger.info(f"Extracting GPKG from {zip_path}")

        try:
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                # List all files in the ZIP
                file_list = zip_ref.namelist()
                self.logger.info(f"Files in ZIP: {file_list}")

                # Find the GPKG file
                gpkg_files = [f for f in file_list if f.endswith(".gpkg")]
                if not gpkg_files:
                    raise ValueError("No GPKG file found in the ZIP archive")

                if len(gpkg_files) > 1:
                    self.logger.warning(
                        f"Multiple GPKG files found: {gpkg_files}, using the first one"
                    )

                gpkg_file = gpkg_files[0]
                self.logger.info(f"Extracting {gpkg_file}")

                # Extract the GPKG file
                zip_ref.extract(gpkg_file, output_dir)

                # Move to a standardized name if needed
                extracted_path = output_dir / gpkg_file
                final_path = output_dir / "DK_INSPIRE_BBR.gpkg"

                if extracted_path != final_path:
                    shutil.move(str(extracted_path), str(final_path))
                    self.logger.info(f"Renamed {extracted_path.name} to {final_path.name}")

                return final_path

        except Exception as e:
            self.logger.error(f"Failed to extract GPKG: {e}")
            raise

    def _save_metadata(
        self, output_dir: Path, file_info: dict, download_url: str, sample_size: int | None
    ) -> None:
        """
        Save metadata about the fetched data.

        Args:
            output_dir: Directory to save metadata
            file_info: File information from FTP page
            download_url: URL used for download
            sample_size: Sample size if used
        """
        metadata = {
            "timestamp": datetime.now().isoformat(),
            "source_url": self.settings.sdfe_ftp_base_url,
            "download_url": download_url,
            "file_info": file_info,
            "sample_size": sample_size,
            "pipeline_version": "1.0.0",
        }

        metadata_path = output_dir / "metadata.json"
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)

        self.logger.info(f"Saved metadata to {metadata_path}")
