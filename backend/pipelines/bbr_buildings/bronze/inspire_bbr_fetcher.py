"""
INSPIRE BBR Data Fetcher for the BBR Buildings Pipeline.

This module handles fetching the DK_INSPIRE_BBR.zip file from SDFE's FTP server
and enriching building data with detailed BBR codes via GraphQL API.

Strategy:
1. Extract building UUIDs and attributes from INSPIRE BBR GPKG
2. Use GraphQL API to get detailed BBR usage codes for agriculture and publicServices
3. Filter publicServices for education buildings only (420-441 codes)
4. Keep all agriculture buildings but attach detailed BBR codes (210-219 series)
5. Keep all residential buildings as-is
"""

import json
import logging
import re
import shutil
import time
import zipfile
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

from config import Settings


class InspireBBRFetcher:
    """Fetches INSPIRE BBR data from SDFE FTP server and enriches with GraphQL API."""

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
        self,
        output_dir: Path,
        sample_size: int | None = None,
        return_data: bool = False,
        pipeline_start_time: datetime = None,
    ):
        """
        Fetch INSPIRE BBR data and enrich with GraphQL API.

        Strategy:
        1. Download and process INSPIRE BBR GPKG
        2. Extract building UUIDs for residential, agriculture, and publicServices
        3. Use GraphQL API to get detailed BBR codes for agriculture and publicServices
        4. Filter publicServices for education buildings only
        5. Return enriched building data

        Args:
            output_dir: Directory to save metadata
            sample_size: Optional sample size for testing
            return_data: Whether to return data for silver layer processing
        """
        if pipeline_start_time is None:
            pipeline_start_time = datetime.now()
        timestamp = pipeline_start_time.strftime("%Y%m%d_%H%M%S")
        run_dir = output_dir / timestamp
        run_dir.mkdir(parents=True, exist_ok=True)

        self.logger.info(f"Starting INSPIRE BBR data fetch with GraphQL enrichment to {run_dir}")

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
                processed_data = self._extract_and_process_with_graphql(zip_path, sample_size)
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
            self._save_metadata(run_dir, file_info, download_url, sample_size, pipeline_start_time)

            self.logger.info("Successfully processed INSPIRE BBR data with GraphQL enrichment")

            if return_data and processed_data is not None:
                # Handle the new data structure
                if isinstance(processed_data, dict) and "attributes_df" in processed_data:
                    actual_records = len(processed_data["attributes_df"])
                else:
                    actual_records = len(processed_data) if processed_data is not None else 0

                return {
                    "data": processed_data,
                    "metadata": {
                        "source": "inspire_bbr_with_graphql",
                        "sample_size": sample_size,
                        "actual_records": actual_records,
                        "file_info": file_info,
                        "download_url": download_url,
                        "timestamp": timestamp,
                    },
                }

            return None

        except Exception as e:
            self.logger.error(f"Failed to fetch INSPIRE BBR data: {e}")
            raise

    def _extract_and_process_with_graphql(self, zip_path: Path, sample_size: int | None):
        """
        Extract GPKG and process it with GraphQL API enrichment.
        """
        try:
            # Extract GPKG
            run_dir = zip_path.parent
            gpkg_path = self._extract_gpkg(zip_path, run_dir)

            # Immediately clean up ZIP file
            if zip_path.exists():
                zip_path.unlink()
                self.logger.info("Cleaned up ZIP file to save disk space")

            # Process with DuckDB and GraphQL enrichment
            processed_data = self._load_and_enrich_with_graphql(gpkg_path, sample_size)

            # Immediately clean up GPKG file after processing
            if gpkg_path.exists():
                size_gb = gpkg_path.stat().st_size / (1024**3)
                gpkg_path.unlink()
                self.logger.info(f"Cleaned up GPKG file ({size_gb:.1f}GB) after processing")

            return processed_data

        except Exception as e:
            self.logger.error(f"Failed to extract and process with GraphQL: {e}")
            raise

    def _load_and_enrich_with_graphql(self, gpkg_path: Path, sample_size: int | None):
        """
        Load INSPIRE BBR data and enrich with GraphQL API for detailed BBR codes.

        Strategy:
        1. Extract all residential, agriculture, and publicServices buildings from INSPIRE BBR
        2. For agriculture and publicServices: query GraphQL API for detailed BBR codes
        3. Filter publicServices to keep only education buildings (420-441)
        4. Keep all agriculture buildings but attach BBR codes
        5. Keep all residential buildings as-is
        """
        try:
            import duckdb

            self.logger.info("Loading INSPIRE BBR data and enriching with GraphQL API...")

            # Create DuckDB connection with spatial extension
            conn = duckdb.connect(":memory:")
            conn.execute("INSTALL spatial;")
            conn.execute("LOAD spatial;")

            # Step 1: Extract building data from INSPIRE BBR GPKG
            if sample_size and sample_size > 0:
                self.logger.info(
                    f"Testing mode: sampling {sample_size} buildings from each category"
                )
                buildings_data = self._extract_sample_buildings(conn, gpkg_path, sample_size)
            else:
                self.logger.info("Production mode: processing all relevant buildings")
                buildings_data = self._extract_all_buildings(conn, gpkg_path)

            # Step 2: Enrich with GraphQL API
            enriched_data = self._enrich_with_graphql_api(buildings_data)

            conn.close()
            return enriched_data

        except Exception as e:
            self.logger.error(f"Failed to load and enrich with GraphQL: {e}")
            raise

    def _extract_sample_buildings(self, conn, gpkg_path: Path, sample_size: int):
        """Extract sample buildings for testing"""

        # Define target categories
        residential_categories = ["individualResidence", "collectiveResidence", "twoDwellings"]
        agriculture_categories = ["agriculture"]
        publicservices_categories = ["publicServices"]

        all_buildings = []

        for category_group, categories in [
            ("residential", residential_categories),
            ("agriculture", agriculture_categories),
            ("publicServices", publicservices_categories),
        ]:
            category_sql = "'" + "','".join(categories) + "'"

            query = f"""
            SELECT 
                inspireId_localId as building_uuid,
                externalReference_reference1 as external_ref,
                currentUse,
                buildingNature,
                dateOfConstruction_dateOfEvent_anyPoint as construction_year,
                officialArea as floor_area,
                numberOfFloorsAboveGround as floors,
                numberOfDwellings as dwellings,
                addressRepresentation as address,
                '{category_group}' as category_group
            FROM ST_Read('{gpkg_path}', layer='building')
            WHERE currentUse IN ({category_sql})
            ORDER BY random()
            LIMIT {min(sample_size, 500)}
            """

            result = conn.execute(query).fetchall()

            for row in result:
                all_buildings.append(
                    {
                        "building_uuid": row[0],
                        "external_ref": row[1],
                        "current_use": row[2],
                        "building_nature": row[3],
                        "construction_year": row[4],
                        "floor_area": row[5],
                        "floors": row[6],
                        "dwellings": row[7],
                        "address": row[8],
                        "category_group": row[9],
                    }
                )

            self.logger.info(f"Extracted {len(result)} {category_group} buildings")

        return all_buildings

    def _extract_all_buildings(self, conn, gpkg_path: Path):
        """Extract all relevant buildings for production"""

        # Extract all residential, agriculture, and publicServices buildings
        query = f"""
        SELECT 
            inspireId_localId as building_uuid,
            externalReference_reference1 as external_ref,
            currentUse,
            buildingNature,
            dateOfConstruction_dateOfEvent_anyPoint as construction_year,
            officialArea as floor_area,
            numberOfFloorsAboveGround as floors,
            numberOfDwellings as dwellings,
            addressRepresentation as address,
            CASE 
                WHEN currentUse IN ('individualResidence', 'collectiveResidence', 'twoDwellings') THEN 'residential'
                WHEN currentUse = 'agriculture' THEN 'agriculture'
                WHEN currentUse = 'publicServices' THEN 'publicServices'
                ELSE 'other'
            END as category_group
        FROM ST_Read('{gpkg_path}', layer='building')
        WHERE currentUse IN ('individualResidence', 'collectiveResidence', 'twoDwellings', 'agriculture', 'publicServices')
        """

        result = conn.execute(query).fetchall()

        buildings = []
        for row in result:
            buildings.append(
                {
                    "building_uuid": row[0],
                    "external_ref": row[1],
                    "current_use": row[2],
                    "building_nature": row[3],
                    "construction_year": row[4],
                    "floor_area": row[5],
                    "floors": row[6],
                    "dwellings": row[7],
                    "address": row[8],
                    "category_group": row[9],
                }
            )

        self.logger.info(f"Extracted {len(buildings)} total buildings")

        # Count by category
        category_counts = {}
        for building in buildings:
            cat = building["category_group"]
            category_counts[cat] = category_counts.get(cat, 0) + 1

        for category, count in category_counts.items():
            self.logger.info(f"  {category}: {count:,} buildings")

        return buildings

    def _enrich_with_graphql_api(self, buildings_data):
        """
        Enrich building data with detailed BBR codes from GraphQL API.

        Strategy:
        - Residential: Keep as-is (no GraphQL needed)
        - Agriculture: Get BBR codes, keep all buildings
        - PublicServices: Get BBR codes, filter for education only (420-441)
        """

        # Separate buildings by category
        residential_buildings = [b for b in buildings_data if b["category_group"] == "residential"]
        agriculture_buildings = [b for b in buildings_data if b["category_group"] == "agriculture"]
        publicservices_buildings = [
            b for b in buildings_data if b["category_group"] == "publicServices"
        ]

        self.logger.info(f"Processing {len(residential_buildings)} residential (no GraphQL needed)")
        self.logger.info(
            f"Processing {len(agriculture_buildings)} agriculture (GraphQL for BBR codes)"
        )
        self.logger.info(
            f"Processing {len(publicservices_buildings)} publicServices (GraphQL + filtering)"
        )

        # Process agriculture buildings with GraphQL
        enriched_agriculture = self._enrich_buildings_with_graphql(
            agriculture_buildings,
            "agriculture",
            filter_codes=None,  # Keep all agriculture buildings
        )

        # Process publicServices buildings with GraphQL and filtering
        enriched_education = self._enrich_buildings_with_graphql(
            publicservices_buildings,
            "education",
            filter_codes=[420, 421, 422, 429, 440, 441],  # Filter for education only
        )

        # Combine all results
        final_buildings = []
        final_buildings.extend(residential_buildings)  # Keep as-is
        final_buildings.extend(enriched_agriculture)  # All agriculture with BBR codes
        final_buildings.extend(enriched_education)  # Education buildings only

        # Convert to DataFrame
        df = pd.DataFrame(final_buildings)

        # Extract building IDs for geometry lookup
        building_ids = df["building_uuid"].dropna().unique().tolist()

        self.logger.info(f"Final dataset: {len(final_buildings)} buildings")
        self.logger.info(f"  Residential: {len(residential_buildings)}")
        self.logger.info(f"  Agriculture: {len(enriched_agriculture)}")
        self.logger.info(f"  Education: {len(enriched_education)}")
        self.logger.info(f"Building IDs for geometry lookup: {len(building_ids)}")

        return {"attributes_df": df, "building_ids": building_ids}

    def _enrich_buildings_with_graphql(self, buildings, category_name, filter_codes=None):
        """
        Enrich buildings with GraphQL API data and optionally filter by BBR codes.
        """
        if not buildings:
            return []

        if not self.settings.datafordeler_graphql_api_key:
            self.logger.warning(
                f"No GraphQL API key configured, skipping {category_name} enrichment"
            )
            return buildings

        self.logger.info(f"Enriching {len(buildings)} {category_name} buildings with GraphQL API")

        # Extract UUIDs for GraphQL queries (now using actual BBR UUIDs from inspireId_localId)
        valid_uuids = [
            b["building_uuid"]
            for b in buildings
            if b["building_uuid"] and b["building_uuid"] != "other:unpopulated"
        ]

        if not valid_uuids:
            self.logger.warning(f"No valid UUIDs found for {category_name} buildings")
            return buildings

        self.logger.info(f"Found {len(valid_uuids)} valid BBR UUIDs for GraphQL queries")

        # Query GraphQL API in batches
        bbr_codes_data = self._query_graphql_for_bbr_codes(valid_uuids, category_name)

        # Create lookup dictionary mapping UUIDs to BBR codes
        bbr_lookup = {}
        for item in bbr_codes_data:
            uuid = item["building_uuid"]
            bbr_lookup[uuid] = item["bbr_code"]

        # Enrich buildings with BBR codes
        enriched_buildings = []
        filtered_count = 0

        for building in buildings:
            uuid = building["building_uuid"]
            bbr_code = bbr_lookup.get(uuid)

            # Add BBR code to building
            building_copy = building.copy()
            building_copy["bbr_usage_code"] = bbr_code

            # Apply filtering if specified
            if filter_codes is not None:
                if bbr_code and int(bbr_code) in filter_codes:
                    enriched_buildings.append(building_copy)
                else:
                    filtered_count += 1
            else:
                # No filtering, keep all buildings
                enriched_buildings.append(building_copy)

        if filter_codes:
            self.logger.info(
                f"Filtered {category_name}: kept {len(enriched_buildings)}, filtered out {filtered_count}"
            )
        else:
            self.logger.info(
                f"Enriched {category_name}: {len(enriched_buildings)} buildings with BBR codes"
            )

        return enriched_buildings

    def _query_graphql_for_bbr_codes(self, uuids, category_name, batch_size=50):
        """
        Query GraphQL API to get BBR usage codes for building UUIDs using proper batch queries.
        """
        graphql_url = f"https://graphql.datafordeler.dk/BBR/v1?apikey={self.settings.datafordeler_graphql_api_key}"

        all_results = []
        total_batches = (len(uuids) + batch_size - 1) // batch_size

        self.logger.info(
            f"Querying GraphQL API for {category_name} BBR codes ({total_batches} batches)"
        )

        # Use current timestamp for temporal filtering (required by API)
        current_timestamp = datetime.now().isoformat() + "Z"

        # Debug: Log API key status and timestamp
        api_key_status = "SET" if self.settings.datafordeler_graphql_api_key else "MISSING"
        self.logger.info(f"API Key status: {api_key_status}, Using timestamp: {current_timestamp}")

        for batch_idx in range(0, len(uuids), batch_size):
            batch_uuids = uuids[batch_idx : batch_idx + batch_size]
            batch_num = (batch_idx // batch_size) + 1

            self.logger.info(f"  Batch {batch_num}/{total_batches} ({len(batch_uuids)} UUIDs)")

            # Create GraphQL query with IN operator for efficient batching
            uuids_list = '["' + '","'.join(batch_uuids) + '"]'
            batch_query = f"""
            {{
              BBR_Bygning(
                registreringstid: "{current_timestamp}",
                where: {{
                  id_lokalId: {{ in: {uuids_list} }}
                }}
              ) {{
                nodes {{ 
                  id_lokalId 
                  byg021BygningensAnvendelse 
                }}
              }}
            }}
            """

            # Debug: Log query for first few batches
            if batch_num <= 2:
                self.logger.info(f"    GraphQL batch query: {batch_query}")

            try:
                response = requests.post(
                    graphql_url,
                    headers={"Content-Type": "application/json"},
                    json={"query": batch_query},
                    timeout=60,  # Longer timeout for batch queries
                )

                # Debug: Log response details for first few batches
                if batch_num <= 2:
                    self.logger.info(f"    HTTP Status: {response.status_code}")
                    self.logger.info(f"    Response: {response.text[:500]}")

                if response.status_code != 200:
                    self.logger.warning(
                        f"    HTTP Error for batch {batch_num}: {response.status_code}"
                    )
                    continue

                data = response.json()

                if "errors" in data:
                    self.logger.warning(
                        f"    GraphQL Errors for batch {batch_num}: {data['errors']}"
                    )
                    continue

                if "data" not in data or not data["data"] or not data["data"].get("BBR_Bygning"):
                    self.logger.info(f"    No buildings found for batch {batch_num}")
                    continue

                batch_buildings = data["data"]["BBR_Bygning"]["nodes"]
                batch_results = [
                    {
                        "building_uuid": b["id_lokalId"],
                        "bbr_code": b.get("byg021BygningensAnvendelse"),
                    }
                    for b in batch_buildings
                    if b.get("byg021BygningensAnvendelse")
                ]

                all_results.extend(batch_results)

                self.logger.info(
                    f"    ✅ Batch {batch_num}: {len(batch_results)} buildings with BBR codes"
                )

                # Rate limiting between batch requests (much less frequent now)
                time.sleep(0.5)

            except Exception as e:
                self.logger.warning(f"    Error for batch {batch_num}: {e}")
                continue

        self.logger.info(
            f"GraphQL API results for {category_name}: {len(all_results)} buildings with BBR codes"
        )
        return all_results

    def _save_metadata(
        self,
        output_dir: Path,
        file_info: dict,
        download_url: str,
        sample_size: int | None,
        pipeline_start_time: datetime,
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
            "timestamp": pipeline_start_time.isoformat(),
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
