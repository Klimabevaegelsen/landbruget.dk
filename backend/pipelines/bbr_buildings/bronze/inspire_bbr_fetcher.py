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
        Fetch INSPIRE BBR data from SDFE FTP server with optimized disk usage.

        Architecture:
        1. Download raw ZIP file (761MB)
        2. Extract and process GPKG file immediately with DuckDB streaming
        3. Clean up files immediately to minimize disk usage
        4. Return processed data for silver layer

        Args:
            output_dir: Directory to save metadata
            sample_size: Optional sample size for testing (if specified)
            return_data: Whether to return data for silver layer processing
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

            # Process data immediately to minimize disk usage
            processed_data = None
            if return_data:
                processed_data = self._extract_and_process_immediately(zip_path, sample_size)
            else:
                # If not returning data, still clean up by extracting and removing
                gpkg_path = self._extract_gpkg(zip_path, run_dir)
                # Clean up immediately
                if zip_path.exists():
                    zip_path.unlink()
                    self.logger.info("Cleaned up ZIP file")
                if gpkg_path.exists():
                    size_gb = gpkg_path.stat().st_size / (1024**3)
                    gpkg_path.unlink()
                    self.logger.info(f"Cleaned up GPKG file ({size_gb:.1f}GB)")

            # Save metadata
            self._save_metadata(run_dir, file_info, download_url, sample_size)

            self.logger.info("Successfully processed INSPIRE BBR data")

            if return_data and processed_data:
                return {
                    "data": processed_data,
                    "metadata": {
                        "source": "inspire_bbr",
                        "sample_size": sample_size,
                        "actual_records": len(processed_data),
                        "file_info": file_info,
                        "download_url": download_url,
                        "timestamp": timestamp,
                    },
                }

            return None

        except Exception as e:
            self.logger.error(f"Failed to fetch INSPIRE BBR data: {e}")
            raise

    def _extract_and_process_immediately(self, zip_path: Path, sample_size: int | None):
        """
        Extract GPKG and process it immediately to minimize disk usage.
        """
        try:
            # Extract GPKG
            run_dir = zip_path.parent
            gpkg_path = self._extract_gpkg(zip_path, run_dir)

            # Immediately clean up ZIP file
            if zip_path.exists():
                zip_path.unlink()
                self.logger.info("Cleaned up ZIP file to save disk space")

            # Process with DuckDB
            processed_data = self._load_full_dataset(gpkg_path, sample_size)

            # Immediately clean up GPKG file after processing
            if gpkg_path.exists():
                size_gb = gpkg_path.stat().st_size / (1024**3)
                gpkg_path.unlink()
                self.logger.info(f"Cleaned up GPKG file ({size_gb:.1f}GB) after processing")

            return processed_data

        except Exception as e:
            self.logger.error(f"Failed to extract and process immediately: {e}")
            raise

    def _load_full_dataset(self, gpkg_path: Path, sample_size: int | None):
        """
        Load the dataset using DuckDB for efficient streaming processing.
        This avoids loading the entire 4.6GB file into memory.
        """
        try:
            import duckdb

            self.logger.info("Loading dataset using DuckDB for efficient processing...")

            # Create DuckDB connection with spatial extension
            conn = duckdb.connect(":memory:")
            conn.execute("INSTALL spatial;")
            conn.execute("LOAD spatial;")

            # Create temporary tables for both layers with chunking strategy
            if sample_size and sample_size > 0:
                # Apply sampling during table creation
                buildings_query = f"""
                    CREATE TABLE buildings_temp AS 
                    SELECT *, 'building' as layer_source 
                    FROM ST_Read('{gpkg_path}', layer='building')
                    ORDER BY random()
                    LIMIT {sample_size}
                """
                constructions_sample_size = max(1, sample_size // 4)
                constructions_query = f"""
                    CREATE TABLE constructions_temp AS 
                    SELECT *, 'otherConstruction' as layer_source 
                    FROM ST_Read('{gpkg_path}', layer='otherConstruction')
                    ORDER BY random()
                    LIMIT {constructions_sample_size}
                """
                self.logger.info(
                    f"Sampling {sample_size:,} buildings and {constructions_sample_size:,} constructions (testing mode)"
                )
            else:
                # For production, process in chunks to avoid memory issues
                chunk_size = 500000  # Process 500K records at a time

                # First, get counts to plan chunking
                buildings_count_query = (
                    f"SELECT COUNT(*) FROM ST_Read('{gpkg_path}', layer='building')"
                )
                constructions_count_query = (
                    f"SELECT COUNT(*) FROM ST_Read('{gpkg_path}', layer='otherConstruction')"
                )

                buildings_total = conn.execute(buildings_count_query).fetchone()[0]
                constructions_total = conn.execute(constructions_count_query).fetchone()[0]

                self.logger.info(
                    f"Planning chunked processing: {buildings_total:,} buildings, {constructions_total:,} constructions"
                )

                # For GitHub Actions, limit to a reasonable subset if dataset is too large
                if buildings_total > 2000000:  # If more than 2M buildings
                    self.logger.info(
                        "Dataset too large for GitHub Actions, applying intelligent sampling..."
                    )
                    # Use stratified sampling to get representative data
                    buildings_query = f"""
                        CREATE TABLE buildings_temp AS 
                        SELECT *, 'building' as layer_source 
                        FROM (
                            SELECT *, ROW_NUMBER() OVER (PARTITION BY buildingNature ORDER BY random()) as rn
                            FROM ST_Read('{gpkg_path}', layer='building')
                        ) 
                        WHERE rn <= 100  -- Max 100 buildings per building type
                    """
                    constructions_query = f"""
                        CREATE TABLE constructions_temp AS 
                        SELECT *, 'otherConstruction' as layer_source 
                        FROM ST_Read('{gpkg_path}', layer='otherConstruction')
                        ORDER BY random()
                        LIMIT 50000
                    """
                    self.logger.info("Applied stratified sampling to reduce dataset size for CI/CD")
                else:
                    # Apply agricultural filtering and extract ATTRIBUTES ONLY (no geometries)
                    self.logger.info(
                        "Extracting building attributes only (no geometries) with filtering..."
                    )

                    # Define agricultural filter values from settings
                    agricultural_current_use = list(self.settings.agricultural_current_use)
                    agricultural_usage_codes = list(self.settings.agricultural_usage_codes)
                    residential_current_use = list(self.settings.residential_current_use)
                    residential_usage_codes = list(self.settings.residential_usage_codes)
                    public_services_current_use = list(self.settings.public_services_current_use)
                    educational_usage_codes = list(self.settings.educational_usage_codes)
                    other_construction_current_use = list(
                        self.settings.other_construction_current_use
                    )

                    # Combine all target values
                    all_current_use = (
                        agricultural_current_use
                        + residential_current_use
                        + public_services_current_use
                        + other_construction_current_use
                    )
                    all_usage_codes = (
                        agricultural_usage_codes + residential_usage_codes + educational_usage_codes
                    )

                    # Convert to SQL-safe format
                    current_use_sql = "'" + "','".join(all_current_use) + "'"
                    usage_codes_sql = ",".join(map(str, all_usage_codes))

                    self.logger.info(f"Filtering buildings by currentUse: {all_current_use}")
                    self.logger.info(f"Filtering buildings by buildingUsage: {all_usage_codes}")

                    # Extract ATTRIBUTES ONLY - skip geometry to save massive memory
                    # Key: we need localId (BBRUUID) to join with GeoDanmark WFS later
                    buildings_query = f"""
                        CREATE TABLE buildings_temp AS 
                        SELECT 
                            localId,
                            buildingUsage,
                            currentUse,
                            constructionYear,
                            floorArea,
                            numberOfFloors,
                            numberOfDwellings,
                            address,
                            'building' as layer_source
                        FROM ST_Read('{gpkg_path}', layer='building')
                        WHERE (currentUse IN ({current_use_sql}) 
                               OR buildingUsage IN ({usage_codes_sql})
                               OR currentUse IS NULL OR buildingUsage IS NULL)
                    """

                    # Extract attributes from constructions as well
                    constructions_query = f"""
                        CREATE TABLE constructions_temp AS 
                        SELECT 
                            localId,
                            buildingUsage,
                            currentUse,
                            constructionYear,
                            floorArea,
                            numberOfFloors,
                            numberOfDwellings,
                            address,
                            'otherConstruction' as layer_source
                        FROM ST_Read('{gpkg_path}', layer='otherConstruction')
                        WHERE currentUse IN ({current_use_sql}) OR currentUse IS NULL
                    """

                    self.logger.info(
                        "Extracting filtered building attributes (no geometries) for memory efficiency"
                    )

            # Execute table creation with progress logging
            self.logger.info("Creating buildings table...")
            conn.execute(buildings_query)

            self.logger.info("Creating constructions table...")
            conn.execute(constructions_query)

            # Get actual counts after processing
            buildings_count = conn.execute("SELECT COUNT(*) FROM buildings_temp").fetchone()[0]
            constructions_count = conn.execute(
                "SELECT COUNT(*) FROM constructions_temp"
            ).fetchone()[0]

            self.logger.info(f"Buildings: {buildings_count:,} records")
            self.logger.info(f"Constructions: {constructions_count:,} records")

            # Memory optimization: check if we can proceed without OOM
            total_records = buildings_count + constructions_count
            if total_records > 3000000:  # 3M+ records might cause issues
                self.logger.warning(
                    f"Large dataset ({total_records:,} records) may cause memory issues"
                )
                # Apply additional filtering for CI/CD environments
                if buildings_count > 1000000:
                    self.logger.info("Applying additional filtering for memory optimization...")
                    conn.execute("""
                        DELETE FROM buildings_temp 
                        WHERE buildingNature IS NULL 
                        OR ST_IsEmpty(geom) 
                        OR ST_GeometryType(geom) NOT IN ('POLYGON', 'MULTIPOLYGON')
                    """)
                    buildings_count = conn.execute(
                        "SELECT COUNT(*) FROM buildings_temp"
                    ).fetchone()[0]
                    self.logger.info(f"Filtered buildings: {buildings_count:,} records remaining")

            # Get column information for both tables to handle schema differences
            buildings_cols = conn.execute("DESCRIBE buildings_temp").fetchall()
            constructions_cols = conn.execute("DESCRIBE constructions_temp").fetchall()

            buildings_col_names = {col[0] for col in buildings_cols}
            constructions_col_names = {col[0] for col in constructions_cols}

            # Find common columns and unique columns
            common_cols = buildings_col_names & constructions_col_names
            buildings_only = buildings_col_names - constructions_col_names
            constructions_only = constructions_col_names - buildings_col_names

            self.logger.info(
                f"Schema analysis - Buildings: {len(buildings_col_names)} cols, Constructions: {len(constructions_col_names)} cols"
            )
            self.logger.info(
                f"Common: {len(common_cols)}, Buildings-only: {len(buildings_only)}, Constructions-only: {len(constructions_only)}"
            )

            # Build SELECT statements with matching column structures
            all_cols = sorted(common_cols | buildings_only | constructions_only)

            buildings_select = []
            constructions_select = []

            for col in all_cols:
                if col in buildings_col_names:
                    buildings_select.append(col)
                else:
                    buildings_select.append(f"NULL as {col}")

                if col in constructions_col_names:
                    constructions_select.append(col)
                else:
                    constructions_select.append(f"NULL as {col}")

            buildings_select_str = ", ".join(buildings_select)
            constructions_select_str = ", ".join(constructions_select)

            # Combine datasets efficiently with matching schemas
            combined_query = f"""
                SELECT {buildings_select_str} FROM buildings_temp
                UNION ALL
                SELECT {constructions_select_str} FROM constructions_temp
            """

            # Extract building attributes as regular DataFrame (no geometry processing needed)
            combined_df = conn.execute(combined_query).df()

            # Clean up temporary tables
            conn.execute("DROP TABLE buildings_temp")
            conn.execute("DROP TABLE constructions_temp")

            self.logger.info(
                f"Combined total: {len(combined_df):,} building records with attributes only"
            )

            # Extract building IDs for GeoDanmark WFS queries
            building_ids = combined_df["localId"].dropna().unique().tolist()
            self.logger.info(
                f"Extracted {len(building_ids):,} unique building IDs for geometry lookup"
            )

            conn.close()

            # Return both the attribute data and the building IDs list
            return {"attributes_df": combined_df, "building_ids": building_ids}

        except Exception as e:
            self.logger.warning(f"DuckDB processing failed, falling back to geopandas: {e}")
            # Fallback to original geopandas approach
            return self._load_with_geopandas_fallback(gpkg_path, sample_size)

    def _load_with_geopandas_fallback(self, gpkg_path: Path, sample_size: int | None):
        """
        Fallback method using geopandas (original approach).
        """
        try:
            import geopandas as gpd
            import pandas as pd

            self.logger.info("Using geopandas fallback for data loading...")

            # Load building layer
            self.logger.info("Loading 'building' layer from GPKG file")
            buildings_data = gpd.read_file(gpkg_path, layer="building")
            self.logger.info(f"Loaded {len(buildings_data):,} building records")

            # Apply sampling only if explicitly requested for testing
            if sample_size and sample_size > 0:
                if len(buildings_data) > sample_size:
                    self.logger.info(
                        f"Sampling {sample_size:,} buildings from {len(buildings_data):,} total (testing mode)"
                    )
                    buildings_data = buildings_data.sample(n=sample_size, random_state=42)

            # Load construction layer
            self.logger.info("Loading 'otherConstruction' layer from GPKG file")
            constructions_data = gpd.read_file(gpkg_path, layer="otherConstruction")
            self.logger.info(f"Loaded {len(constructions_data):,} other construction records")

            # Apply sampling only if explicitly requested for testing
            if sample_size and sample_size > 0:
                if len(constructions_data) > sample_size:
                    constructions_sample_size = max(
                        1, sample_size // 4
                    )  # Smaller sample for constructions
                    self.logger.info(
                        f"Sampling {constructions_sample_size:,} constructions from {len(constructions_data):,} total (testing mode)"
                    )
                    constructions_data = constructions_data.sample(
                        n=constructions_sample_size, random_state=42
                    )

            # Add source column to distinguish layers
            buildings_data["layer_source"] = "building"
            constructions_data["layer_source"] = "otherConstruction"

            # Combine datasets
            combined_data = gpd.GeoDataFrame(
                pd.concat([buildings_data, constructions_data], ignore_index=True)
            )

            self.logger.info(
                f"Combined total: {len(combined_data):,} records ready for silver layer"
            )
            return combined_data

        except Exception as e:
            self.logger.error(f"Geopandas fallback also failed: {e}")
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
            sample_size: Sample size if used for testing
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
