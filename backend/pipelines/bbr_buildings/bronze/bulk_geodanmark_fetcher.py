"""
Bulk GeoDanmark Buildings Fetcher

Downloads all buildings from GeoDanmark WFS service using pagination
and saves to GeoParquet format for efficient local processing.

Uses DuckDB with spatial extension instead of GeoPandas for optimal performance.
"""

import logging
import os
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import duckdb
import requests
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class BulkGeoDanmarkFetcher:
    def __init__(self, username: str, password: str) -> None:
        self.username = username
        self.password = password
        self.base_url = (
            "https://wfs.datafordeler.dk/GeoDanmarkVektor/GeoDanmark60_NOHIST_GML3/1.0.0/WFS"
        )
        self.session = self._create_session()
        self.output_dir = Path("data")
        self.output_dir.mkdir(exist_ok=True)

        # Initialize DuckDB connection with spatial extension
        self.conn = duckdb.connect()
        self.conn.execute("INSTALL spatial")
        self.conn.execute("LOAD spatial")

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

    def parse_gml_to_duckdb_table(self, gml_content: str, table_name: str) -> bool:
        """Parse GML content to DuckDB table using spatial extension."""
        try:
            # Save GML to temporary file for DuckDB spatial to read
            temp_file = self.output_dir / "temp_batch.gml"
            with open(temp_file, "w", encoding="utf-8") as f:
                f.write(gml_content)

            # Use DuckDB spatial to read GML file
            try:
                # Read GML file into DuckDB table
                self.conn.execute(f"""
                    CREATE OR REPLACE TABLE {table_name} AS
                    SELECT * FROM ST_Read('{temp_file}')
                """)

                # Get record count
                result = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                record_count = result[0] if result else 0

                logger.info(f"Parsed {record_count} buildings from GML into table {table_name}")

                # Clean up temp file
                temp_file.unlink()

                return record_count > 0

            except Exception as e:
                logger.error(f"DuckDB spatial read failed: {e}")
                # Fallback: Try to parse GML manually
                return self._parse_gml_manually(gml_content, table_name)

        except Exception as e:
            logger.error(f"Error parsing GML: {e}")
            # Clean up temp file if it exists
            temp_file = self.output_dir / "temp_batch.gml"
            if temp_file.exists():
                temp_file.unlink()
            return False

    def _parse_gml_manually(self, gml_content: str, table_name: str) -> bool:
        """Fallback: Parse GML manually using XML parsing."""
        try:
            logger.info("Attempting manual GML parsing as fallback")

            # Parse XML
            root = ET.fromstring(gml_content)

            # Extract building features
            buildings = []

            # Define namespaces
            namespaces = {
                "gml": "http://www.opengis.net/gml/3.2",
                "gdk60": "http://www.geodanmark.dk/gdk60",
            }

            # Find all building features
            for feature in root.findall(".//gdk60:Bygning", namespaces):
                building = {}

                # Extract attributes
                for child in feature:
                    if child.tag.endswith("}geometry"):
                        # Handle geometry - extract coordinates
                        coords_elem = child.find(".//gml:coordinates", namespaces)
                        if coords_elem is not None:
                            coords_text = coords_elem.text
                            if coords_text:
                                # Parse coordinates and create WKT
                                coords = coords_text.strip().split()
                                if len(coords) >= 2:
                                    # Simple point geometry for now
                                    building["geometry"] = f"POINT({coords[0]} {coords[1]})"
                    else:
                        # Extract other attributes
                        tag_name = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                        building[tag_name] = child.text

                if building:
                    buildings.append(building)

            if not buildings:
                logger.warning("No buildings extracted from GML")
                return False

            # Create table with extracted data
            if buildings:
                # Get all unique keys
                all_keys = set()
                for building in buildings:
                    all_keys.update(building.keys())

                # Create table schema
                columns = []
                for key in sorted(all_keys):
                    if key == "geometry":
                        columns.append(f"{key} GEOMETRY")
                    else:
                        columns.append(f"{key} VARCHAR")

                schema = ", ".join(columns)
                self.conn.execute(f"CREATE OR REPLACE TABLE {table_name} ({schema})")

                # Insert data using parameterized queries
                for building in buildings:
                    cols = list(building.keys())
                    values = []
                    for col in cols:
                        value = building[col]
                        if col == "geometry" and value:
                            # Handle geometry specially - still need ST_GeomFromText
                            values.append(
                                value
                            )  # Store WKT string, will use ST_GeomFromText in query
                        else:
                            values.append(value)

                    # Create placeholders, handling geometry column specially
                    placeholders = []
                    for col in cols:
                        if col == "geometry":
                            placeholders.append("ST_GeomFromText(?)")
                        else:
                            placeholders.append("?")

                    cols_str = ", ".join(cols)
                    placeholders_str = ", ".join(placeholders)
                    self.conn.execute(
                        f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders_str})", values
                    )

                logger.info(f"Manually parsed {len(buildings)} buildings into table {table_name}")
                return True

            return False

        except Exception as e:
            logger.error(f"Manual GML parsing failed: {e}")
            return False

    def bulk_download_buildings(self, batch_size: int = 30000) -> None:
        """
        Download all GeoDanmark buildings in batches with progress tracking.

        This method continuously fetches batches until no more data is available.
        """
        logger.info("🚀 Starting bulk download - will fetch until no more data available")
        logger.info(f"Using batch size: {batch_size:,}")

        successful_batches = 0
        batch_num = 0
        total_buildings_downloaded = 0
        batch_tables = []

        # Initialize progress bar (we don't know total batches, so use unknown total)
        progress_bar = tqdm(desc="Downloading batches", unit="batch", ncols=80, disable=False)

        try:
            while True:
                start_index = batch_num * batch_size

                progress_bar.set_description(f"Batch {batch_num + 1} (index {start_index:,})")

                # Fetch batch
                gml_content = self.fetch_buildings_batch(start_index, batch_size)
                if not gml_content:
                    logger.error(f"Failed to fetch batch {batch_num + 1}, stopping download")
                    break

                # Parse to DuckDB table
                table_name = f"buildings_batch_{batch_num}"
                success = self.parse_gml_to_duckdb_table(gml_content, table_name)
                if not success:
                    logger.error(f"Failed to parse batch {batch_num + 1}, stopping download")
                    break

                # Get batch size
                result = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                current_batch_size = result[0] if result else 0

                total_buildings_downloaded += current_batch_size

                # Update progress bar with current stats
                progress_bar.set_postfix(
                    {
                        "buildings": f"{total_buildings_downloaded:,}",
                        "batch_size": f"{current_batch_size:,}",
                    }
                )
                progress_bar.update(1)

                if current_batch_size == 0:
                    logger.info("No more buildings to download - reached end of dataset")
                    break

                batch_tables.append(table_name)
                successful_batches += 1

                # Save intermediate results every 10 batches
                if len(batch_tables) >= 10:
                    self._save_intermediate_results(batch_tables, batch_num)
                    batch_tables = []

                # Check if we got fewer records than requested - indicates end of dataset
                if current_batch_size < batch_size:
                    logger.info(
                        f"Got {current_batch_size:,} buildings "
                        f"(less than batch size {batch_size:,}) - reached end of dataset"
                    )
                    break

                batch_num += 1

                # Rate limiting
                time.sleep(0.5)

        finally:
            progress_bar.close()

        # Save any remaining results
        if batch_tables:
            self._save_intermediate_results(batch_tables, batch_num)

        logger.info(f"✅ Completed bulk download: {successful_batches} batches successful")
        logger.info(f"🏢 Total buildings downloaded: {total_buildings_downloaded:,}")

        # Combine all intermediate files into final result
        self._combine_intermediate_files()

    def _save_intermediate_results(self, table_names: list, batch_num: int) -> None:
        """Save intermediate results to avoid memory issues."""
        if not table_names:
            return

        try:
            # First, get all unique columns across all tables to normalize schemas
            all_columns = set()
            table_schemas = {}

            for table_name in table_names:
                columns_info = self.conn.execute(f"DESCRIBE {table_name}").fetchall()
                table_columns = [col[0] for col in columns_info]
                table_schemas[table_name] = table_columns
                all_columns.update(table_columns)

            # Sort columns for consistent ordering
            all_columns = sorted(all_columns)

            logger.info(
                f"Normalizing schemas across {len(table_names)} tables "
                f"with {len(all_columns)} total columns"
            )

            # Build normalized SELECT statements for each table
            normalized_selects = []
            for table_name in table_names:
                table_columns = table_schemas[table_name]

                # Build SELECT with all columns, using NULL for missing ones
                select_parts = []
                for col in all_columns:
                    if col in table_columns:
                        select_parts.append(f'"{col}"')
                    else:
                        # Use appropriate NULL type based on common column patterns
                        if col in ["geometry", "geom"]:
                            select_parts.append('CAST(NULL AS GEOMETRY) as "' + col + '"')
                        elif any(keyword in col.lower() for keyword in ["id", "uuid", "nummer"]):
                            select_parts.append('CAST(NULL AS VARCHAR) as "' + col + '"')
                        elif any(
                            keyword in col.lower()
                            for keyword in ["area", "length", "width", "height"]
                        ):
                            select_parts.append('CAST(NULL AS DOUBLE) as "' + col + '"')
                        else:
                            select_parts.append('CAST(NULL AS VARCHAR) as "' + col + '"')

                normalized_select = f"SELECT {', '.join(select_parts)} FROM {table_name}"
                normalized_selects.append(normalized_select)

            # Combine all normalized tables using UNION ALL
            union_query = " UNION ALL ".join(normalized_selects)

            # Save to intermediate GeoParquet file
            output_file = self.output_dir / f"geodanmark_buildings_batch_{batch_num:04d}.geoparquet"

            self.conn.execute(f"""
                COPY (
                    {union_query}
                ) TO '{output_file}' (FORMAT PARQUET)
            """)

            # Get total count
            result = self.conn.execute(f"SELECT COUNT(*) FROM ({union_query})").fetchone()
            total_count = result[0] if result else 0

            logger.info(f"Saved {total_count} buildings to {output_file}")

            # Drop the batch tables to free memory
            for table_name in table_names:
                self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")

        except Exception as e:
            logger.error(f"Error saving intermediate results: {e}")
            # Log table schemas for debugging
            try:
                logger.info("Table schemas for debugging:")
                for table_name in table_names:
                    columns_info = self.conn.execute(f"DESCRIBE {table_name}").fetchall()
                    logger.info(f"  {table_name}: {[col[0] for col in columns_info]}")
            except Exception:
                pass

    def _combine_intermediate_files(self) -> None:
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

            # Create a single table from all files with schema normalization
            file_paths = [str(f) for f in intermediate_files]
            file_list = "', '".join(file_paths)

            # First, normalize the schema by casting problematic columns to consistent types
            # The synligBygning column appears to have mixed VARCHAR/BOOLEAN values
            query = f"""
            COPY (
                SELECT
                    * EXCLUDE (synligBygning),
                    CASE
                        WHEN synligBygning IS NULL THEN NULL
                        WHEN synligBygning = 'true' OR synligBygning = '1' THEN true
                        WHEN synligBygning = 'false' OR synligBygning = '0' THEN false
                        ELSE NULL  -- Handle 'Mangler afklaring' and other non-boolean values
                    END as synligBygning
                FROM read_parquet(['{file_list}'], union_by_name=true)
            ) TO '{self.output_dir}/geodanmark_buildings_complete.geoparquet'
            (FORMAT PARQUET)
            """

            self.conn.execute(query)

            # Get final count using the union_by_name option
            count_query = (
                f"SELECT COUNT(*) as total FROM read_parquet(['{file_list}'], union_by_name=true)"
            )
            result = self.conn.execute(count_query).fetchone()
            total_buildings = result[0] if result else 0

            logger.info(
                f"🏢 Combined {total_buildings:,} buildings into final file: "
                f"geodanmark_buildings_complete.geoparquet"
            )

            # Clean up intermediate files
            for file in intermediate_files:
                file.unlink()

            logger.info("🧹 Cleaned up intermediate files")

        except Exception as e:
            logger.error(f"Error combining intermediate files: {e}")

            # Add debugging information for schema issues
            if "failed to cast column" in str(e) or "Conversion Error" in str(e):
                logger.info("Schema mismatch detected. Attempting to diagnose...")
                try:
                    # Check schemas of each file
                    for i, file_path in enumerate(file_paths[:3]):  # Check first 3 files
                        schema_query = f"DESCRIBE SELECT * FROM read_parquet('{file_path}') LIMIT 1"
                        schema_result = self.conn.execute(schema_query).fetchall()
                        logger.info(f"Schema for file {i + 1} ({Path(file_path).name}):")
                        for col_name, col_type, _null, _key, _default, _extra in schema_result:
                            logger.info(f"  {col_name}: {col_type}")
                except Exception as debug_e:
                    logger.error(f"Failed to debug schema: {debug_e}")

                # Try a more aggressive fallback: read each file individually and
                # cast all columns to string
                logger.info("Attempting fallback combination with string casting...")
                try:
                    # Create a temporary table to collect all data
                    self.conn.execute("DROP TABLE IF EXISTS combined_buildings")

                    for i, file_path in enumerate(file_paths):
                        logger.info(
                            f"Processing file {i + 1}/{len(file_paths)}: {Path(file_path).name}"
                        )

                        if i == 0:
                            # First file: create the table structure
                            self.conn.execute(f"""
                                CREATE TABLE combined_buildings AS
                                SELECT * FROM read_parquet('{file_path}')
                            """)
                        else:
                            # Subsequent files: insert with union_by_name
                            self.conn.execute(f"""
                                INSERT INTO combined_buildings
                                SELECT * FROM read_parquet('{file_path}')
                            """)

                    # Now save the combined table
                    self.conn.execute(f"""
                        COPY combined_buildings
                        TO '{self.output_dir}/geodanmark_buildings_complete.geoparquet'
                        (FORMAT PARQUET)
                    """)

                    # Get final count
                    result = self.conn.execute("SELECT COUNT(*) FROM combined_buildings").fetchone()
                    total_buildings = result[0] if result else 0

                    logger.info(
                        f"🏢 Fallback combination successful: {total_buildings:,} buildings"
                    )

                    # Clean up
                    self.conn.execute("DROP TABLE combined_buildings")

                    # Clean up intermediate files
                    for file in intermediate_files:
                        file.unlink()
                    logger.info("🧹 Cleaned up intermediate files")

                    return  # Success with fallback

                except Exception as fallback_e:
                    logger.error(f"Fallback combination also failed: {fallback_e}")

            raise

    def __del__(self) -> None:
        """Clean up DuckDB connection."""
        if hasattr(self, "conn"):
            self.conn.close()


def main() -> None:
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
