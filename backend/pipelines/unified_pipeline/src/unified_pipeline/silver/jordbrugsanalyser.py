"""
Silver layer data processing for Jordbrugsanalyser Marker data.

This module handles the processing of raw WFS responses from the bronze layer and
convert them into structured tables with proper field mappings and data types.

The module contains:
- JordbrugsanalyserSilverConfig: Configuration class for the silver processing
- JordbrugsanalyserSilver: Implementation class for parsing and cleaning marker data

The data is processed from raw WFS XML responses into standardized tables
with Danish field names mapped to English equivalents and proper geometry handling.
"""

import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any, Dict, List, Optional

import duckdb

# ✅ MIGRATION: Removed pandas import - using DuckDB for data operations
from pydantic import ConfigDict
from shapely import wkt
from shapely.geometry import MultiPolygon, Polygon

from unified_pipeline.common.base import BaseJobConfig, BaseSource, SilverJobInterface
from unified_pipeline.util.timing import AsyncTimer


class JordbrugsanalyserSilverConfig(BaseJobConfig):
    """
    Configuration for the Jordbrugsanalyser Silver processing.

    This class defines all configuration parameters needed for processing
    raw Jordbrugsanalyser marker data from the bronze layer into clean,
    structured data for the silver layer.

    Attributes:
        name (str): Human-readable name of the data processing
        type (str): Type of the data processing (wfs)
        description (str): Brief description of the processing
        dataset (str): Name of the dataset in storage
        bronze_dataset (str): Name of the bronze dataset to read from
        bucket (str): GCS bucket name for data storage
        start_year (int): First year to process (2012)
        end_year (int): Last year to process (2024)
    """

    name: str = "Danish Jordbrugsanalyser Markers Silver"
    type: str = "wfs"
    description: str = "Processed agricultural marker data from Jordbrugsanalyser"
    dataset: str = "jordbrugsanalyser_markers"
    bronze_dataset: str = "jordbrugsanalyser_markers"
    bucket: str = "landbrugsdata-raw-data"

    # Year range for marker data processing
    start_year: int = 2012
    end_year: int = 2024

    # WFS namespaces for parsing XML responses
    namespaces: Dict[str, str] = {
        "wfs": "http://www.opengis.net/wfs/2.0",
        "gml": "http://www.opengis.net/gml/3.2",
        "Jordbrugsanalyser": "Jordbrugsanalyser",
    }

    # Field mapping from Danish WFS fields to standardized English field names
    field_mapping: Dict[str, tuple] = {
        "AfgKat": ("crop_category", str),
        "AfgNavn": ("crop_name", str),
        "AfgNr": ("crop_code", lambda x: int(x) if x and x.isdigit() else None),
        "EjerNr": ("owner_number", lambda x: int(x) if x and x.isdigit() else None),
        "Ha": ("area_ha", lambda x: float(x) if x else None),
        "HaIalt": ("total_area_ha", lambda x: float(x) if x else None),
        "MarkBlok": ("field_block", str),
        "MarkNr": ("field_number", str),
        "X": ("centroid_x", lambda x: float(x) if x else None),
        "Y": ("centroid_y", lambda x: float(x) if x else None),
    }

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class JordbrugsanalyserSilver(BaseSource[JordbrugsanalyserSilverConfig], SilverJobInterface):
    """
    Silver layer processing for Jordbrugsanalyser marker data.

    This class is responsible for processing raw agricultural marker data from the
    bronze layer. It parses WFS XML responses, extracts feature attributes and
    geometries, applies data cleaning and validation, and stores the processed
    data in Google Cloud Storage.

    The class handles:
    - XML parsing of WFS FeatureCollection responses
    - Geometry extraction and validation from GML
    - Field mapping and data type conversion
    - Data quality validation and cleaning
    - Table creation with proper CRS

    Processing flow:
    1. Read raw WFS responses from bronze layer for each year
    2. Parse XML and extract features with attributes and geometries
    3. Apply field mapping and data type conversions
    4. Validate and clean the data
    5. Save processed tables to Google Cloud Storage
    """

    def __init__(self, config: JordbrugsanalyserSilverConfig):
        """
        Initialize the JordbrugsanalyserSilver processor.

        Args:
            config (JordbrugsanalyserSilverConfig): Configuration for the processor"""
        super().__init__(config)

    def _clean_text_value(self, value: Optional[str]) -> Optional[str]:
        """
        Clean and normalize text values.

        Args:
            value: Raw text value from XML

        Returns:
            Cleaned text value or None if empty/invalid
        """
        if not value or not isinstance(value, str):
            return None

        # Strip whitespace and normalize
        cleaned = value.strip()
        if not cleaned:
            return None

        # Handle encoding issues (XML contains ISO-8859-1 encoded data)
        # Common Danish characters that might need fixing
        replacements = {
            "gr�s": "græs",
            "\ufffd": "ø",  # replacement character to ø
            # Note: Some Unicode characters may appear the same but have different encodings
            # We handle the most common ones here
        }

        for old, new in replacements.items():
            cleaned = cleaned.replace(old, new)

        return cleaned

    def _parse_geometry(self, geom_elem: ET.Element) -> Optional[str]:
        """
        Parse GML geometry element to WKT string.

        Handles MultiSurface geometries with Polygon components, including
        exterior and interior rings (holes).

        Args:
            geom_elem: XML element containing GML geometry

        Returns:
            WKT string representation of geometry or None if parsing fails
        """
        try:
            # Find MultiSurface or Polygon elements
            multi_surface = geom_elem.find(".//gml:MultiSurface", self.config.namespaces)
            if multi_surface is None:
                # Try direct Polygon
                polygon_elem = geom_elem.find(".//gml:Polygon", self.config.namespaces)
                if polygon_elem is None:
                    self.log.warning("No MultiSurface or Polygon found in geometry")
                    return None
                polygons = [polygon_elem]
            else:
                # Get all polygon surface members
                polygons = multi_surface.findall(".//gml:Polygon", self.config.namespaces)

            if not polygons:
                self.log.warning("No Polygon elements found in geometry")
                return None

            parsed_polygons = []

            for polygon in polygons:
                # Parse exterior ring
                exterior_elem = polygon.find(
                    ".//gml:exterior/gml:LinearRing/gml:posList", self.config.namespaces
                )
                if exterior_elem is None or not exterior_elem.text:
                    self.log.warning("No exterior ring found in polygon")
                    continue

                # Parse coordinates from posList
                coords_text = exterior_elem.text.strip()
                coords = [float(x) for x in coords_text.split()]

                # Convert to coordinate pairs (X, Y)
                exterior_coords = [(coords[i], coords[i + 1]) for i in range(0, len(coords), 2)]

                # Ensure the ring is closed
                if exterior_coords[0] != exterior_coords[-1]:
                    exterior_coords.append(exterior_coords[0])

                # Parse interior rings (holes)
                interior_rings = []
                interior_elems = polygon.findall(
                    ".//gml:interior/gml:LinearRing/gml:posList", self.config.namespaces
                )

                for interior_elem in interior_elems:
                    if interior_elem.text:
                        interior_coords_text = interior_elem.text.strip()
                        interior_coords_raw = [float(x) for x in interior_coords_text.split()]
                        interior_coords = [
                            (interior_coords_raw[i], interior_coords_raw[i + 1])
                            for i in range(0, len(interior_coords_raw), 2)
                        ]

                        # Ensure the ring is closed
                        if interior_coords[0] != interior_coords[-1]:
                            interior_coords.append(interior_coords[0])

                        interior_rings.append(interior_coords)

                # Create Shapely Polygon
                try:
                    if interior_rings:
                        polygon_geom = Polygon(exterior_coords, interior_rings)
                    else:
                        polygon_geom = Polygon(exterior_coords)

                    if polygon_geom.is_valid:
                        parsed_polygons.append(polygon_geom)
                    else:
                        self.log.warning("Invalid polygon geometry, attempting to fix")
                        from shapely.ops import make_valid

                        fixed_geom = make_valid(polygon_geom)
                        if hasattr(fixed_geom, "geom_type") and fixed_geom.geom_type in [
                            "Polygon",
                            "MultiPolygon",
                        ]:
                            parsed_polygons.append(fixed_geom)

                except Exception as e:
                    self.log.warning(f"Error creating polygon: {e}")
                    continue

            if not parsed_polygons:
                return None

            # Create final geometry
            if len(parsed_polygons) == 1:
                final_geom = parsed_polygons[0]
            else:
                final_geom = MultiPolygon(parsed_polygons)

            return wkt.dumps(final_geom)

        except Exception as e:
            self.log.error(f"Error parsing geometry: {e}")
            return None

    def _parse_feature(self, feature_elem: ET.Element, year: int) -> Optional[Dict[str, Any]]:
        """
        Parse a single Marker feature from XML.

        Args:
            feature_elem: XML element containing the Marker feature
            year: Year this feature belongs to

        Returns:
            Dictionary with parsed feature data or None if parsing fails
        """
        try:
            feature_data = {"year": year}

            # Parse geometry
            geom_elem = feature_elem.find(".//Jordbrugsanalyser:the_geom", self.config.namespaces)
            if geom_elem is not None:
                geometry_wkt = self._parse_geometry(geom_elem)
                if geometry_wkt:
                    feature_data["geometry"] = geometry_wkt
                else:
                    self.log.warning("Failed to parse geometry for feature")
                    return None
            else:
                self.log.warning("No geometry element found for feature")
                return None

            # Parse attribute fields using field mapping
            for xml_field, (target_field, converter) in self.config.field_mapping.items():
                elem = feature_elem.find(
                    f".//Jordbrugsanalyser:{xml_field}", self.config.namespaces
                )
                if elem is not None and elem.text:
                    try:
                        raw_value = elem.text.strip()
                        if raw_value:
                            if converter is str:
                                feature_data[target_field] = self._clean_text_value(raw_value)
                            else:
                                feature_data[target_field] = converter(raw_value)
                    except (ValueError, TypeError) as e:
                        self.log.warning(f"Error converting field {xml_field}: {e}")
                        feature_data[target_field] = None
                else:
                    feature_data[target_field] = None

            # Add metadata
            feature_data["processed_at"] = datetime.now()

            return feature_data

        except Exception as e:
            self.log.error(f"Error parsing feature: {e}")
            return None

    def _parse_wfs_response(self, xml_content: str, year: int) -> List[Dict[str, Any]]:
        """
        Parse a WFS FeatureCollection XML response.

        Args:
            xml_content: Raw XML content from WFS response
            year: Year this data belongs to

        Returns:
            List of parsed feature dictionaries
        """
        try:
            root = ET.fromstring(xml_content)

            # Find all Marker features (the element name includes the year)
            layer_name = f"Marker{str(year)[-2:]}"  # e.g., Marker24
            features = root.findall(f".//Jordbrugsanalyser:{layer_name}", self.config.namespaces)

            parsed_features = []

            for feature_elem in features:
                feature_data = self._parse_feature(feature_elem, year)
                if feature_data:
                    parsed_features.append(feature_data)

            self.log.info(
                f"Parsed {len(parsed_features)} features from {len(features)} "
                f"XML elements for year {year}"
            )
            return parsed_features

        except ET.ParseError as e:
            self.log.error(f"XML parsing error for year {year}: {e}")
            return []
        except Exception as e:
            self.log.error(f"Error parsing WFS response for year {year}: {e}")
            return []

    def _process_year_data(self, year: int, bronze_data: Optional[Any] = None) -> Optional[Any]:
        """
        Process all data for a specific year from bronze layer.

        Args:
            year: Year to process (e.g., 2012)
            bronze_data: Optional in-memory data from bronze stage

        Returns:
            Table name with processed data or None if no data/errors
        """
        try:
            # Read data with support for in-memory passing
            if bronze_data is not None:
                self.log.info(
                    f"Using bronze data from memory for year {year} (in-memory data passing)"
                )
                # Bronze data is a dict mapping year to list of raw WFS responses
                if isinstance(bronze_data, dict) and str(year) in bronze_data:
                    raw_responses = bronze_data[str(year)]
                    # ✅ MIGRATION: Create table with DuckDB instead of
                    temp_conn = duckdb.connect()
                    temp_conn.execute("CREATE OR REPLACE TABLE temp_responses (payload VARCHAR)")

                    # Insert responses directly into table
                    for resp in raw_responses:
                        temp_conn.execute("INSERT INTO temp_responses VALUES (?)", [resp])

                    # Keep as table name for processing
                    bronze_table = "temp_responses"
                    bronze_conn = temp_conn
                else:
                    self.log.warning(f"No in-memory data found for year {year}")
                    return None
            else:
                # Fallback to reading from storage
                self.log.info(f"Reading bronze data from storage for year {year} (fallback)")
                bronze_dataset_name = f"{self.config.bronze_dataset}_{year}"
                bronze_df = self._read_bronze_data(bronze_dataset_name, self.config.bucket)

                if bronze_df is None or len(bronze_df) == 0:
                    self.log.warning(f"No bronze data found for year {year}")
                    return None

                # Convert to table for consistent processing
                bronze_conn = duckdb.connect()
                bronze_conn.register("temp_responses", bronze_df)
                bronze_table = "temp_responses"

            # Get record count for logging
            record_count = bronze_conn.execute(f"SELECT COUNT(*) FROM {bronze_table}").fetchone()[0]
            self.log.info(f"Processing {record_count} bronze records for year {year}")

            all_features = []

            # Process each bronze record (raw WFS response)
            payload_rows = bronze_conn.execute(f"SELECT payload FROM {bronze_table}").fetchall()
            for row in payload_rows:
                xml_content = row[0]
                if xml_content:
                    features = self._parse_wfs_response(xml_content, year)
                    all_features.extend(features)

            if not all_features:
                self.log.warning(f"No features parsed for year {year}")
                return None

            self.log.info(f"Total features parsed for year {year}: {len(all_features)}")

            # ✅ MIGRATION: Process features using DuckDB directly
            # Create table directly from the list of dictionaries
            if not all_features:
                self.log.warning(f"No features to process for year {year}")
                bronze_conn.close()
                return None

            # Get column names from the first feature
            columns = list(all_features[0].keys())

            # Create the table schema
            bronze_conn.execute(f"""
                CREATE OR REPLACE TABLE temp_features (
                    {", ".join([f"{col} VARCHAR" for col in columns])}
                )
            """)

            # Insert data in batches
            batch_size = 1000
            for i in range(0, len(all_features), batch_size):
                batch = all_features[i : i + batch_size]

                # Use parameterized queries instead of string concatenation
                for feature in batch:
                    values = [feature.get(col) for col in columns]
                    placeholders = ", ".join(["?" for _ in columns])

                    bronze_conn.execute(
                        f"""
                        INSERT INTO temp_features ({", ".join(columns)})
                        VALUES ({placeholders})
                    """,
                        values,
                    )

            # Install spatial extension if not already installed
            try:
                bronze_conn.execute("INSTALL spatial")
                bronze_conn.execute("LOAD spatial")
            except Exception:
                pass  # May already be installed

            # Create processed table with spatial geometries
            bronze_conn.execute("""
                CREATE OR REPLACE TABLE processed_features AS
                SELECT
                    *,
                    ST_GeomFromText(geometry) as geom,
                    ST_IsValid(ST_GeomFromText(geometry)) as is_valid_geometry
                FROM temp_features
                WHERE geometry IS NOT NULL
            """)

            # Get count of valid features
            valid_count = bronze_conn.execute(
                "SELECT COUNT(*) FROM processed_features WHERE is_valid_geometry = true"
            ).fetchone()[0]

            if valid_count == 0:
                self.log.error(f"No valid geometries found for year {year}")
                bronze_conn.close()
                return None

            self.log.info(f"Created {valid_count} valid features for year {year}")

            # Create final table in main connection
            table_name = f"jordbrugsanalyser_{year}"
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")

            # Transfer data to main connection
            result_data = bronze_conn.execute(
                "SELECT * FROM processed_features WHERE is_valid_geometry = true"
            ).fetchall()
            columns = [desc[0] for desc in bronze_conn.description]

            # Create table in main connection
            self.conn.register(f"temp_{table_name}", result_data)
            self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM temp_{table_name}")
            self.conn.execute(f"DROP TABLE temp_{table_name}")

            # Close the bronze connection
            bronze_conn.close()

            return table_name

        except Exception as e:
            self.log.error(f"Error processing year {year}: {e}")
            return None

    async def run(self, bronze_data: Optional[Any] = None) -> None:
        """
        Run the silver layer processing pipeline.

        This method orchestrates the entire silver processing workflow:
        1. For each year from 2012 to 2024, reads bronze layer data
        2. Parses WFS XML responses and extracts features
        3. Applies data cleaning and validation
        4. Creates tables with proper geometries and field mapping
        5. Saves processed data to Google Cloud Storage

        Args:
            bronze_data: Optional in-memory data from bronze stage. If provided,
                        this data will be used instead of reading from storage.

        The method processes years sequentially to manage memory usage
        and provides detailed logging for monitoring progress.

        Returns:
            None

        Note:
            This is the main entry point for the silver layer processing of
            Jordbrugsanalyser marker data.
        """
        self.log.info("Running Jordbrugsanalyser Markers silver job")

        async with AsyncTimer("Total Jordbrugsanalyser silver processing time"):
            for year in range(self.config.start_year, self.config.end_year + 1):
                try:
                    self.log.info(f"Processing silver layer for year {year}")

                    processed_table = self._process_year_data(year, bronze_data)

                    if processed_table is not None:
                        # Get table statistics
                        total_features = self.conn.execute(
                            f"SELECT COUNT(*) FROM {processed_table}"
                        ).fetchone()[0]

                        # Save data with year suffix for easy identification
                        dataset_name = f"{self.config.dataset}_{year}"
                        self.log.info(f"Saving {total_features:,} features for year {year}")

                        # ✅ OPTIMIZED: Save directly without DataFrame conversion
                        gcs_path = self.save_data_direct(
                            processed_table, dataset_name, self.config.bucket, "silver"
                        )
                        self.log.info(f"Year {year}: Silver data saved successfully to {gcs_path}")

                        # Log some statistics using DuckDB
                        total_area = (
                            self.conn.execute(
                                f"SELECT SUM(area_ha) FROM {processed_table} "
                                f"WHERE area_ha IS NOT NULL"
                            ).fetchone()[0]
                            or 0
                        )
                        unique_crops = self.conn.execute(
                            f"SELECT COUNT(DISTINCT crop_code) FROM {processed_table} "
                            f"WHERE crop_code IS NOT NULL"
                        ).fetchone()[0]
                        unique_blocks = self.conn.execute(
                            f"SELECT COUNT(DISTINCT field_block) FROM {processed_table} "
                            f"WHERE field_block IS NOT NULL"
                        ).fetchone()[0]

                        self.log.info(f"Year {year} statistics:")
                        self.log.info(f"  - Total features: {total_features:,}")
                        self.log.info(f"  - Total area (ha): {total_area:.2f}")
                        self.log.info(f"  - Unique crop codes: {unique_crops}")
                        self.log.info(f"  - Unique field blocks: {unique_blocks}")

                        # Clean up table
                        self.conn.execute(f"DROP TABLE IF EXISTS {processed_table}")

                    else:
                        self.log.warning(f"Year {year}: No data to save")

                except Exception as e:
                    self.log.error(f"Failed to process year {year}: {e}")
                    # Continue with next year instead of failing completely
                    continue

            self.log.info("Jordbrugsanalyser Markers silver job completed successfully")
