"""
Bulk GeoDanmark Buildings Fetcher

Downloads all buildings from GeoDanmark WFS service using pagination
and saves to GeoParquet format for efficient local processing.
"""

import logging
import os
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import duckdb
import geopandas as gpd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class BulkGeoDanmarkFetcher:
    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password
        self.base_url = (
            "https://wfs.datafordeler.dk/GeoDanmarkVektor/GeoDanmark60_NOHIST_GML3/1.0.0/WFS"
        )
        self.session = self._create_session()
        self.output_dir = Path("data")
        self.output_dir.mkdir(exist_ok=True)

    def _create_session(self) -> requests.Session:
        """Create a session with retry strategy and connection pooling."""
        session = requests.Session()

        # Retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )

        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=20)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Set timeout
        session.timeout = 300  # 5 minutes

        return session

    def get_total_building_count(self) -> int:
        """Get total number of buildings available."""
        params = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeName": "gdk60:Bygning",
            "resultType": "hits",
            "srsName": "EPSG:4326",  # Request data in WGS84 for consistency
            "username": self.username,
            "password": self.password,
        }

        try:
            response = self.session.get(self.base_url, params=params)
            response.raise_for_status()

            # Parse XML to get numberMatched
            root = ET.fromstring(response.content)

            # Find the FeatureCollection element with numberMatched attribute
            for elem in root.iter():
                if "numberMatched" in elem.attrib:
                    count = int(elem.attrib["numberMatched"])
                    logger.info(f"Total buildings available: {count:,}")
                    return count

            logger.warning("Could not find numberMatched in response")
            return 0

        except Exception as e:
            logger.error(f"Error getting building count: {e}")
            return 0

    def fetch_buildings_batch(self, start_index: int = 0, count: int = 30000) -> str | None:
        """Fetch a batch of buildings as GML."""
        params = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeName": "gdk60:Bygning",
            "outputFormat": "application/gml+xml; version=3.2",
            "srsName": "EPSG:4326",  # Request data in WGS84 for consistency
            "startIndex": start_index,
            "count": count,
            "username": self.username,
            "password": self.password,
        }

        try:
            logger.info(f"Fetching buildings {start_index:,} to {start_index + count:,}")
            response = self.session.get(self.base_url, params=params, timeout=300)
            response.raise_for_status()

            if response.status_code == 200:
                logger.info(f"Successfully fetched batch starting at {start_index:,}")
                return response.text
            else:
                logger.error(f"HTTP {response.status_code}: {response.text}")
                return None

        except requests.exceptions.Timeout:
            logger.error(f"Timeout fetching batch starting at {start_index:,}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error fetching batch starting at {start_index:,}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching batch starting at {start_index:,}: {e}")
            return None

    def parse_gml_to_geodataframe(self, gml_content: str) -> gpd.GeoDataFrame | None:
        """Parse GML content to GeoDataFrame."""
        try:
            # Save GML to temporary file for GeoPandas to read
            temp_file = self.output_dir / "temp_batch.gml"
            with open(temp_file, "w", encoding="utf-8") as f:
                f.write(gml_content)

            # Read with GeoPandas
            gdf = gpd.read_file(temp_file)

            # Clean up temp file
            temp_file.unlink()

            logger.info(f"Parsed {len(gdf)} buildings from GML")
            return gdf

        except Exception as e:
            logger.error(f"Error parsing GML: {e}")
            # Clean up temp file if it exists
            temp_file = self.output_dir / "temp_batch.gml"
            if temp_file.exists():
                temp_file.unlink()
            return None

    def bulk_download_buildings(self, batch_size: int = 30000):
        """Download all buildings in batches and save to GeoParquet."""

        # Don't rely on get_total_building_count() as WFS service limits it to 30,000
        # Instead, keep fetching until we get fewer records than batch_size
        logger.info("🚀 Starting bulk download - will fetch until no more data available")
        logger.info(f"Using batch size: {batch_size:,}")

        all_gdfs = []
        successful_batches = 0
        batch_num = 0
        total_buildings_downloaded = 0

        while True:
            start_index = batch_num * batch_size

            logger.info(f"Processing batch {batch_num + 1} (starting at index {start_index:,})")

            # Fetch batch
            gml_content = self.fetch_buildings_batch(start_index, batch_size)
            if not gml_content:
                logger.error(f"Failed to fetch batch {batch_num + 1}, stopping download")
                break

            # Parse to GeoDataFrame
            gdf = self.parse_gml_to_geodataframe(gml_content)
            if gdf is None:
                logger.error(f"Failed to parse batch {batch_num + 1}, stopping download")
                break

            current_batch_size = len(gdf)
            total_buildings_downloaded += current_batch_size
            logger.info(f"Downloaded {current_batch_size:,} buildings in batch {batch_num + 1}")
            logger.info(f"Total buildings downloaded so far: {total_buildings_downloaded:,}")

            if current_batch_size == 0:
                logger.info("No more buildings to download - reached end of dataset")
                break

            all_gdfs.append(gdf)
            successful_batches += 1

            # Save intermediate results every 10 batches
            if len(all_gdfs) >= 10:
                self._save_intermediate_results(all_gdfs, batch_num)
                all_gdfs = []

            # Check if we got fewer records than requested - indicates end of dataset
            if current_batch_size < batch_size:
                logger.info(
                    f"Got {current_batch_size:,} buildings (less than batch size {batch_size:,}) - reached end of dataset"
                )
                break

            batch_num += 1

            # Rate limiting
            time.sleep(0.5)

        # Save any remaining results
        if all_gdfs:
            self._save_intermediate_results(all_gdfs, batch_num)

        logger.info(f"✅ Completed bulk download: {successful_batches} batches successful")
        logger.info(f"🏢 Total buildings downloaded: {total_buildings_downloaded:,}")

        # Combine all intermediate files into final result
        self._combine_intermediate_files()

    def _save_intermediate_results(self, gdfs: list, batch_num: int):
        """Save intermediate results to avoid memory issues."""
        if not gdfs:
            return

        try:
            # Combine GeoDataFrames
            combined_gdf = gpd.pd.concat(gdfs, ignore_index=True)

            # Save to intermediate file
            output_file = self.output_dir / f"geodanmark_buildings_batch_{batch_num:04d}.geoparquet"
            combined_gdf.to_parquet(output_file)

            logger.info(f"Saved {len(combined_gdf)} buildings to {output_file}")

        except Exception as e:
            logger.error(f"Error saving intermediate results: {e}")

    def _combine_intermediate_files(self):
        """Combine all intermediate files into final GeoParquet."""
        try:
            # Find all intermediate files
            intermediate_files = list(
                self.output_dir.glob("geodanmark_buildings_batch_*.geoparquet")
            )

            if not intermediate_files:
                logger.warning("No intermediate files found to combine")
                return

            logger.info(f"Combining {len(intermediate_files)} intermediate files")

            # Use DuckDB for efficient combining
            conn = duckdb.connect()

            # Install and load spatial extension
            conn.execute("INSTALL spatial")
            conn.execute("LOAD spatial")

            # Create a single table from all files
            file_paths = [str(f) for f in intermediate_files]
            file_list = "', '".join(file_paths)

            query = f"""
            COPY (
                SELECT * FROM read_parquet(['{file_list}'])
            ) TO '{self.output_dir}/geodanmark_buildings_complete.geoparquet' 
            (FORMAT PARQUET)
            """

            conn.execute(query)

            # Get final count
            count_query = f"SELECT COUNT(*) as total FROM read_parquet(['{file_list}'])"
            result = conn.execute(count_query).fetchone()
            total_buildings = result[0] if result else 0

            conn.close()

            logger.info(
                f"🏢 Combined {total_buildings:,} buildings into final file: geodanmark_buildings_complete.geoparquet"
            )

            # Clean up intermediate files
            for file in intermediate_files:
                file.unlink()

            logger.info("🧹 Cleaned up intermediate files")

        except Exception as e:
            logger.error(f"Error combining intermediate files: {e}")


def main():
    """Main function to run bulk download."""

    # Get credentials from environment
    username = os.getenv("DATAFORDELER_USERNAME")
    password = os.getenv("DATAFORDELER_PASSWORD")

    if not username or not password:
        logger.error(
            "DATAFORDELER_USERNAME and DATAFORDELER_PASSWORD environment variables required"
        )
        return

    # Create fetcher
    fetcher = BulkGeoDanmarkFetcher(username, password)

    # Download all buildings
    logger.info("Starting full bulk download")
    fetcher.bulk_download_buildings(batch_size=30000)


if __name__ == "__main__":
    main()
