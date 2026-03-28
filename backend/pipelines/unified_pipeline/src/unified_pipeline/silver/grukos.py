"""
Grukos Silver Layer Implementation

This module implements the silver layer data processing for Danish groundwater
mapping data (Grundvandskortlægning). It transforms raw grukos data from the
bronze layer into structured geometries, validates geometries, and saves the
processed data to Google Cloud Storage (GCS).

This pipeline outputs BOTH:
1. Individual features (grukos) - preserves all individual indsatsområder
2. Dissolved geometry (grukos_dissolved) - single geometry combining all areas

The module contains:
- GrukosSilverConfig: Configuration class for the silver layer processing
- GrukosSilver: Implementation class for processing and transforming data

The processing includes:
1. Reading raw data from GCS
2. Extracting features from XML payloads
3. Parsing geometries and calculating areas
4. Standardizing attribute names and types
5. Saving BOTH individual features and dissolved geometries to GCS
"""

import xml.etree.ElementTree as ET
from typing import Any, ClassVar

from common.geometry_validator import (
    validate_and_normalize_to_utm,
    validate_and_transform_geometries_duckdb,
)

from unified_pipeline.common.base import BaseJobConfig, BaseSource, SilverJobInterface
from unified_pipeline.util.timing import AsyncTimer, timed

# CRS Strategy: Use EPSG:25832 for processing, transform to EPSG:4326 only at Supabase upload
USE_UTM_PROCESSING = True


class GrukosSilverConfig(BaseJobConfig):
    """
    Configuration for Grukos silver layer data processing.

    This class defines all the necessary parameters and settings required
    for processing grukos data in the silver layer of the data pipeline.

    The configuration includes layer specifications, data processing settings,
    and namespace definitions for XML parsing.

    Attributes:
        dataset (str): Name of the dataset being processed
        bucket (str): GCS bucket name for data storage
        storage_batch_size (int): Batch size for data storage operations
        namespaces (dict): XML namespace mappings for parsing WFS responses
        gml_ns (str): GML namespace string for geometry parsing
        layers (list): List of layer names to process
        service_types (dict): Service type overrides for specific layers
    """

    dataset: str = "grukos"
    bucket: str = "landbruget-data"
    storage_batch_size: int = 8000
    namespaces: ClassVar[dict[str, str]] = {
        "wfs": "http://www.opengis.net/wfs/2.0",
        "gml": "http://www.opengis.net/gml/3.2",
        "grukos": "https://wfs2-miljoegis.mim.dk/grukos",
    }
    gml_ns: str = "{http://www.opengis.net/gml/3.2}"  # This is not a f-string.
    layers: ClassVar[list[str]] = [
        "grukos:indsatsomraader",  # Indsatsområder (action areas for groundwater protection)
        "grukos:indvindingsoplande_alle",  # Indvindingsoplande (groundwater abstraction catchments)
    ]
    service_types: ClassVar[dict[str, str]] = {}  # All layers use default WFS service type


class GrukosSilver(BaseSource[GrukosSilverConfig], SilverJobInterface):
    """
    Silver layer processing for Grukos (groundwater mapping) data.

    This class transforms raw grukos data from the bronze layer into
    structured geometries, validates geometries, and saves the processed data
    to Google Cloud Storage (GCS).
    It handles XML data formats, extracting features and converting
    them into geometries with appropriate attributes.

    This pipeline outputs BOTH:
    1. Individual features (grukos) - preserves all individual indsatsområder
       with their kategorization (NFI = nitrate-sensitive, SFI = pesticide-sensitive, etc.)
    2. Dissolved geometry (grukos_dissolved) - single geometry combining all areas

    The processing includes:
    1. Reading raw data from GCS
    2. Extracting features from XML payloads
    3. Parsing geometries and calculating areas
    4. Standardizing attribute names and types
    5. Saving BOTH individual features and dissolved geometries to GCS
    """

    def __init__(self, config: GrukosSilverConfig):
        """
        Initialize the GrukosSilver processor.

        Args:
            config (GrukosSilverConfig): Configuration object containing settings
                                         for the processor.
        """
        super().__init__(config)
        self.log.info("✅ GrukosSilver: Using unified GCS access and DuckDB connection")

    def get_first_namespace(self, root: ET.Element) -> str | None:
        """
        Extract the namespace from an XML root element.

        This method iterates through the XML elements to find and extract
        the first namespace used in the document.

        Args:
            root (ET.Element): The root element of an XML document.

        Returns:
            Optional[str]: The namespace string if found, None otherwise.
        """
        for elem in root.iter():
            if "}" in elem.tag:
                return elem.tag.split("}")[0].strip("{")
        return None

    def clean_value(self, value: Any) -> str | None:
        """
        Clean and standardize string values from XML.

        This method converts values to strings and removes leading/trailing whitespace.
        Empty strings are converted to None.

        Args:
            value (Any): The value to clean, can be any type.

        Returns:
            Optional[str]: The cleaned string value, or None if the value is empty.
        """
        if value is None:
            return None
        cleaned = str(value).strip()
        return cleaned if cleaned else None

    def _parse_gml_to_wkt(self, gml_xml: str) -> str | None:
        """
        Parse GML geometry XML to WKT format.

        This method handles the common GML geometry types found in grukos data:
        - MultiSurface -> MULTIPOLYGON
        - Simple polygons

        Args:
            gml_xml (str): GML geometry XML string

        Returns:
            Optional[str]: WKT geometry string, or None if parsing fails
        """
        try:
            # Parse the GML XML
            root = ET.fromstring(gml_xml)

            # Find posList elements which contain the coordinates
            pos_lists = []
            for elem in root.iter():
                if elem.tag.endswith("posList") and elem.text:
                    coordinates = elem.text.strip()
                    if coordinates:
                        pos_lists.append(coordinates)

            if not pos_lists:
                return None

            # Determine geometry type based on GML structure
            gml_text = gml_xml.lower()

            if "multisurface" in gml_text or "multipolygon" in gml_text:
                # Handle MultiSurface
                return self._create_multipolygon_wkt(pos_lists)
            if "polygon" in gml_text:
                # Handle simple Polygon
                return self._create_polygon_wkt(pos_lists[0]) if pos_lists else None
            # Default to polygon for unknown types
            return self._create_polygon_wkt(pos_lists[0]) if pos_lists else None

        except Exception as e:
            self.log.debug(f"Error parsing GML geometry: {e}")
            return None

    def _create_multipolygon_wkt(self, pos_lists: list[str]) -> str | None:
        """Create MULTIPOLYGON WKT from coordinate lists."""
        try:
            polygons = []
            for coords in pos_lists:
                polygon_coords = self._parse_coordinates(coords)
                if polygon_coords and len(polygon_coords) >= 3:
                    # Ensure polygon is closed
                    if polygon_coords[0] != polygon_coords[-1]:
                        polygon_coords.append(polygon_coords[0])

                    coord_pairs = [f"{x} {y}" for x, y in polygon_coords]
                    polygons.append(f"(({', '.join(coord_pairs)}))")

            if polygons:
                return f"MULTIPOLYGON({', '.join(polygons)})"
            return None

        except Exception:
            return None

    def _create_polygon_wkt(self, coords: str) -> str | None:
        """Create POLYGON WKT from coordinate string."""
        try:
            polygon_coords = self._parse_coordinates(coords)
            if polygon_coords and len(polygon_coords) >= 3:
                # Ensure polygon is closed
                if polygon_coords[0] != polygon_coords[-1]:
                    polygon_coords.append(polygon_coords[0])

                coord_pairs = [f"{x} {y}" for x, y in polygon_coords]
                return f"POLYGON(({', '.join(coord_pairs)}))"
            return None

        except Exception:
            return None

    def _parse_coordinates(self, coord_string: str) -> list[tuple[float, float]]:
        """
        Parse coordinate string into list of (x, y) tuples.

        Args:
            coord_string (str): Space-separated coordinate values "x1 y1 x2 y2 ..."

        Returns:
            list[tuple[float, float]]: List of coordinate pairs
        """
        try:
            # Clean up the coordinate string
            coords = coord_string.strip().replace("\n", " ").replace("\t", " ")
            # Split by whitespace and filter out empty strings
            values = [v for v in coords.split() if v]

            # Group into coordinate pairs
            coord_pairs = []
            for i in range(0, len(values) - 1, 2):
                try:
                    x = float(values[i])
                    y = float(values[i + 1])
                    coord_pairs.append((x, y))
                except (ValueError, IndexError):
                    continue

            return coord_pairs

        except Exception:
            return []

    @timed(name="Processing XML data")  # type: ignore
    def process_xml_payload(self, payload: str, layer: str) -> str | None:
        """
        Process XML payload from WFS response and extract features.

        This method parses the XML response, extracts features with their geometries
        and attributes, and stores them in a DuckDB table.

        Args:
            payload (str): Raw XML payload from WFS service
            layer (str): Layer name for logging and identification

        Returns:
            Optional[str]: Name of the DuckDB table containing processed features,
                          or None if processing fails
        """
        try:
            # Parse XML
            root = ET.fromstring(payload)

            # Register namespaces for XPath queries
            for prefix, uri in self.config.namespaces.items():
                ET.register_namespace(prefix, uri)

            # Find all feature members
            features = []

            # Look for features in the XML structure
            # WFS responses typically have features under wfs:FeatureCollection/wfs:member
            for member in root.findall(".//wfs:member", self.config.namespaces):
                for feature in member:
                    feature_data = {}

                    # Extract geometry
                    geometry_elem = feature.find(".//gml:*", self.config.namespaces)
                    if geometry_elem is not None:
                        # Store the raw geometry XML for DuckDB processing
                        feature_data["geometry_xml"] = ET.tostring(
                            geometry_elem, encoding="unicode"
                        )

                    # Extract attributes (all child elements that are not geometries)
                    for child in feature:
                        if not any(
                            child.tag.startswith(f"{{{ns}}}")
                            for ns in ["http://www.opengis.net/gml/3.2"]
                        ):
                            # This is an attribute, not a geometry
                            tag_name = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                            feature_data[tag_name] = self.clean_value(child.text)

                    # Add metadata
                    feature_data["layer"] = layer
                    feature_data["source"] = self.config.dataset

                    features.append(feature_data)

            if not features:
                self.log.warning(f"No features found in XML payload for layer {layer}")
                return None

            self.log.info(f"Extracted {len(features)} features from layer {layer}")

            # Create DuckDB table from features
            table_name = f"{layer.replace(':', '_')}_processed"

            # Get all unique column names from all features
            all_columns = set()
            for feature in features:
                all_columns.update(feature.keys())

            # Create table schema
            columns_sql = []
            for col in sorted(all_columns):
                if col == "geometry_xml":
                    columns_sql.append(f"{col} TEXT")
                else:
                    columns_sql.append(f"{col} VARCHAR")

            # Create table
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE {table_name} (
                    {", ".join(columns_sql)}
                )
            """)

            # Insert features
            for feature in features:
                # Prepare values for all columns
                values = []
                for col in sorted(all_columns):
                    value = feature.get(col)
                    if value is None:
                        values.append("NULL")
                    else:
                        # Escape single quotes
                        escaped_value = str(value).replace("'", "''")
                        values.append(f"'{escaped_value}'")

                self.conn.execute(f"""
                    INSERT INTO {table_name} VALUES ({", ".join(values)})
                """)

            self.log.info(
                f"Successfully processed {len(features)} features into table {table_name}"
            )
            return table_name

        except Exception as e:
            self.log.error(f"Error processing XML payload for layer {layer}: {e}")
            return None

    @timed(name="Processing raw data")  # type: ignore
    def _process_data(self, raw_data: list[dict[str, Any]]) -> str | None:
        """
        Process raw data from multiple layers and combine into a single table.

        Args:
            raw_data (list): List of raw data dictionaries from bronze layer

        Returns:
            Optional[str]: Name of combined DuckDB table, or None if processing fails
        """
        try:
            all_tables = []

            for entry in raw_data:
                payload = entry.get("payload", "")
                layer = entry.get("layer", "unknown")

                if not payload:
                    self.log.warning(f"Empty payload for layer {layer}")
                    continue

                # Process the XML payload
                table_name = self.process_xml_payload(payload, layer)
                if table_name:
                    all_tables.append(table_name)

            if not all_tables:
                self.log.error("No tables were successfully processed")
                return None

            # Combine all tables into one, handling different schemas
            combined_table = f"{self.config.dataset}_combined"

            if len(all_tables) == 1:
                # If only one table, just rename it
                self.conn.execute(f"""
                    CREATE OR REPLACE TABLE {combined_table} AS
                    SELECT * FROM {all_tables[0]}
                """)
            else:
                # Get all unique columns from all tables
                all_columns = set()
                for table in all_tables:
                    columns = self.conn.execute(f"DESCRIBE {table}").fetchall()
                    for col in columns:
                        all_columns.add(col[0])  # column name is first element

                all_columns = sorted(all_columns)
                self.log.info(f"Combining tables with unified schema: {len(all_columns)} columns")

                # Create union with NULL for missing columns
                union_parts = []
                for table in all_tables:
                    # Get existing columns for this table
                    existing_columns = set()
                    table_columns = self.conn.execute(f"DESCRIBE {table}").fetchall()
                    for col in table_columns:
                        existing_columns.add(col[0])

                    # Build SELECT with NULL for missing columns
                    select_parts = []
                    for col in all_columns:
                        if col in existing_columns:
                            select_parts.append(f"{col}")
                        else:
                            select_parts.append(f"NULL as {col}")

                    union_parts.append(f"SELECT {', '.join(select_parts)} FROM {table}")

                self.conn.execute(f"""
                    CREATE OR REPLACE TABLE {combined_table} AS
                    {" UNION ALL ".join(union_parts)}
                """)

            # Clean up individual tables
            for table in all_tables:
                self.conn.execute(f"DROP TABLE IF EXISTS {table}")

            # Convert geometry XML to actual geometries using DuckDB-spatial
            self.conn.execute(f"""
                ALTER TABLE {combined_table}
                ADD COLUMN geometry_spatial GEOMETRY
            """)

            # Also add the geometry column for WKT text (needed by downstream consumers)
            self.conn.execute(f"""
                ALTER TABLE {combined_table}
                ADD COLUMN geometry TEXT
            """)

            # Convert GML geometries to WKT using Python-based parser
            total_feature_count = self.conn.execute(
                f"SELECT COUNT(*) FROM {combined_table}"
            ).fetchone()[0]
            self.log.info(f"Converting {total_feature_count} GML geometries to spatial objects...")

            # Process geometries row by row using Python for reliable parsing
            converted_count = 0
            failed_count = 0

            # Get all records with geometry_xml
            rows = self.conn.execute(f"""
                SELECT
                    ROWID,
                    geometry_xml,
                    layer
                FROM {combined_table}
                WHERE geometry_xml IS NOT NULL
            """).fetchall()

            for rowid, geometry_xml, _layer in rows:
                try:
                    # Parse the GML XML to extract coordinates
                    wkt_geom = self._parse_gml_to_wkt(geometry_xml)

                    if wkt_geom:
                        # Convert to spatial geometry using DuckDB with validation & repair
                        try:
                            # First try direct conversion
                            self.conn.execute(
                                f"""
                                UPDATE {combined_table}
                                SET geometry_spatial = ST_GeomFromText(?)
                                WHERE ROWID = ?
                            """,
                                [wkt_geom, rowid],
                            )

                            # Check if geometry is valid using proper type casting
                            is_valid = self.conn.execute(
                                "SELECT ST_IsValid(ST_GeomFromText(?))", [wkt_geom]
                            ).fetchone()

                            if is_valid and is_valid[0]:
                                # Geometry is valid - success!
                                converted_count += 1
                            else:
                                # Geometry is invalid - fix with ST_MakeValid
                                try:
                                    repaired_wkt = self.conn.execute(
                                        "SELECT ST_AsText(ST_MakeValid(ST_GeomFromText(?)))",
                                        [wkt_geom],
                                    ).fetchone()

                                    if repaired_wkt and repaired_wkt[0]:
                                        # Update with repaired geometry
                                        self.conn.execute(
                                            f"""
                                            UPDATE {combined_table}
                                            SET geometry_spatial = ST_GeomFromText(?)
                                            WHERE ROWID = ?
                                        """,
                                            [repaired_wkt[0], rowid],
                                        )

                                        # Verify the repaired geometry is valid
                                        repair_valid = self.conn.execute(
                                            "SELECT ST_IsValid(ST_GeomFromText(?))",
                                            [repaired_wkt[0]],
                                        ).fetchone()

                                        if repair_valid and repair_valid[0]:
                                            converted_count += 1
                                            self.log.debug(
                                                f"Fixed invalid geometry for row {rowid} "
                                                f"with ST_MakeValid"
                                            )
                                        else:
                                            failed_count += 1
                                            self.log.debug(
                                                f"ST_MakeValid repair failed validation "
                                                f"for row {rowid}"
                                            )
                                    else:
                                        failed_count += 1
                                        self.log.debug(
                                            f"ST_MakeValid returned null for row {rowid}"
                                        )

                                except Exception as e:
                                    failed_count += 1
                                    self.log.debug(f"ST_MakeValid error for row {rowid}: {e}")

                        except Exception as e:
                            # Even ST_GeomFromText failed - this is a parsing error
                            self.log.debug(f"Failed to parse WKT for row {rowid}: {e}")
                            failed_count += 1
                    else:
                        failed_count += 1

                except Exception as e:
                    self.log.debug(f"Failed to convert geometry for row {rowid}: {e}")
                    failed_count += 1

            total_features = converted_count + failed_count
            success_rate = (converted_count / total_features * 100) if total_features > 0 else 0
            self.log.info(
                f"GML→WKT conversion: {converted_count:,}/{total_features:,} "
                f"({success_rate:.1f}% success)"
            )

            if success_rate >= 95:
                self.log.info("✅ Excellent geometry conversion rate!")
            elif success_rate >= 80:
                self.log.info("🟡 Good geometry conversion rate")
            elif success_rate >= 50:
                self.log.warning("⚠️ Moderate geometry conversion rate")
            else:
                self.log.error("❌ Poor geometry conversion rate")

            if failed_count > 0:
                self.log.warning(
                    f"⚠️ {failed_count:,} geometries failed to convert even with ST_MakeValid repair"
                )

            final_feature_count = self.conn.execute(
                f"SELECT COUNT(*) FROM {combined_table}"
            ).fetchone()[0]
            self.log.info(
                f"Combined {len(all_tables)} tables into {combined_table} "
                f"with {final_feature_count} features"
            )

            return combined_table

        except Exception as e:
            self.log.error(f"Error processing raw data: {e}")
            return None

    @timed(name="Creating dissolved tables using DuckDB-spatial")  # type: ignore
    def _create_dissolved_df(self, input_table_name: str) -> dict[str, str]:
        """
        Create separate dissolved tables for each layer type using DuckDB-spatial.

        Creates separate dissolved geometries for:
        - indsatsomraader (action areas for groundwater protection)
        - indvindingsoplande (groundwater abstraction catchments)

        Args:
            input_table_name: Name of the DuckDB table containing processed features

        Returns:
            dict[str, str]: Mapping of layer type to dissolved table name
        """
        dissolved_tables = {}

        # Define layer configurations for dissolving
        layer_configs = [
            {
                "layer_filter": "grukos:indsatsomraader",
                "table_name": "grukos_indsatsomraader_dissolved",
                "description": "indsatsområder (action areas)",
            },
            {
                "layer_filter": "grukos:indvindingsoplande_alle",
                "table_name": "grukos_indvindingsoplande_dissolved",
                "description": "indvindingsoplande (abstraction catchments)",
            },
        ]

        for config in layer_configs:
            layer_filter = config["layer_filter"]
            table_name = config["table_name"]
            description = config["description"]

            try:
                self.log.info(f"Creating dissolved geometry for {description}...")

                # Get count of valid geometries for this layer
                valid_count = self.conn.execute(f"""
                    SELECT COUNT(*)
                    FROM {input_table_name}
                    WHERE geometry_spatial IS NOT NULL
                        AND ST_IsValid(geometry_spatial)
                        AND layer = '{layer_filter}'
                """).fetchone()[0]

                if valid_count == 0:
                    self.log.warning(f"No valid geometries found for {description}")
                    # Create empty table with proper schema
                    self.conn.execute(f"""
                        CREATE OR REPLACE TABLE {table_name} AS
                        SELECT
                            CAST(NULL AS GEOMETRY) as geometry,
                            CAST(0 AS DOUBLE) as area_ha,
                            CAST(0 AS INTEGER) as feature_count,
                            CAST(NULL AS TIMESTAMP) as dissolved_at
                        WHERE FALSE
                    """)
                    dissolved_tables[layer_filter] = table_name
                    continue

                # Create dissolved geometry for this layer
                self.conn.execute(f"""
                    CREATE OR REPLACE TABLE {table_name} AS
                    SELECT
                        ST_Union_Agg(geometry_spatial) as geometry,
                        COUNT(*) as feature_count,
                        CURRENT_TIMESTAMP as dissolved_at
                    FROM {input_table_name}
                    WHERE geometry_spatial IS NOT NULL
                        AND ST_IsValid(geometry_spatial)
                        AND layer = '{layer_filter}'
                """)

                # Calculate area in hectares for dissolved geometry
                self.conn.execute(f"""
                    ALTER TABLE {table_name}
                    ADD COLUMN area_ha DOUBLE
                """)

                self.conn.execute(f"""
                    UPDATE {table_name}
                    SET area_ha = ST_Area(ST_Transform(geometry, 'EPSG:4326', 'EPSG:25832')) / 10000
                    WHERE geometry IS NOT NULL
                """)

                # Get statistics
                stats = self.conn.execute(f"""
                    SELECT
                        feature_count,
                        ROUND(area_ha, 2) as area_ha
                    FROM {table_name}
                    WHERE geometry IS NOT NULL
                """).fetchone()

                if stats:
                    feat_count, area = stats
                    self.log.info(
                        f"  {description}: {feat_count:,} features dissolved, "
                        f"{area:,.2f} ha total area"
                    )

                dissolved_tables[layer_filter] = table_name
                self.log.info(f"Created dissolved table '{table_name}'")

            except Exception as e:
                self.log.error(f"Error creating dissolved geometry for {description}: {e!s}")
                # Create empty table on error
                self.conn.execute(f"""
                    CREATE OR REPLACE TABLE {table_name} AS
                    SELECT
                        CAST(NULL AS GEOMETRY) as geometry,
                        CAST(0 AS DOUBLE) as area_ha,
                        CAST(0 AS INTEGER) as feature_count,
                        CAST(NULL AS TIMESTAMP) as dissolved_at
                    WHERE FALSE
                """)
                dissolved_tables[layer_filter] = table_name

        return dissolved_tables

    async def run(self, bronze_data: Any | None = None) -> Any | None:
        """
        Run the Grukos silver layer processing.

        Args:
            bronze_data: Optional in-memory data from bronze stage. If provided,
                        this data will be used instead of reading from storage.

        Returns:
            Optional[Any]: Processed data that can be passed to gold stage,
                          or None if processing fails.
        """
        self.log.info("Running Grukos silver job")
        async with AsyncTimer("Grukos silver job"):
            try:
                # Read data with support for in-memory passing
                if bronze_data is not None:
                    self.log.info("Using bronze data from memory (in-memory data passing)")
                    # Bronze data is expected to be a list of tuples (layer, raw_data)
                    if isinstance(bronze_data, list):
                        # Create data structure compatible with _process_data
                        current_timestamp = self.conn.execute(
                            "SELECT current_timestamp"
                        ).fetchone()[0]

                        raw_data_list = [
                            {
                                "payload": data,
                                "layer": layer,
                                "source": self.config.dataset,
                                "created_at": current_timestamp,
                                "updated_at": current_timestamp,
                            }
                            for layer, data in bronze_data
                        ]
                    else:
                        self.log.error("Bronze data format not supported")
                        return None
                else:
                    # Read from GCS storage
                    raw_data_list = []
                    for layer in self.config.layers:
                        f"{layer.replace(':', '_')}_raw"
                        dataset_name = f"{self.config.dataset}_{layer.replace(':', '_')}"

                        try:
                            # Load data from GCS using the correct method
                            table_name = self._read_bronze_data(dataset_name, self.config.bucket)

                            if table_name:
                                # Extract the payload from the loaded data
                                result = self.conn.execute(f"""
                                    SELECT payload, layer, source, created_at, updated_at
                                    FROM {table_name}
                                    LIMIT 1
                                """).fetchone()

                                if result:
                                    raw_data_list.append(
                                        {
                                            "payload": result[0],
                                            "layer": result[1],
                                            "source": result[2],
                                            "created_at": result[3],
                                            "updated_at": result[4],
                                        }
                                    )
                        except Exception as e:
                            self.log.warning(f"Could not load data for layer {layer}: {e}")

                if not raw_data_list:
                    self.log.error("No raw data available to process")
                    return None

                self.log.info("Read raw data successfully")
                table_name = self._process_data(raw_data_list)
                if table_name is None:
                    self.log.error("Failed to process raw data")
                    return None

                self.log.info("Processed raw data successfully")

                # Apply geometry validation and transformation
                # Check if we have any spatial geometries to validate
                spatial_geom_count = self.conn.execute(
                    f"SELECT COUNT(*) FROM {table_name} WHERE geometry_spatial IS NOT NULL"
                ).fetchone()[0]

                if spatial_geom_count > 0:
                    self.log.info(
                        f"Validating and transforming {spatial_geom_count:,} spatial geometries..."
                    )
                    # CRS Strategy: Keep in EPSG:25832 for processing
                    if USE_UTM_PROCESSING:
                        validate_and_normalize_to_utm(
                            self.conn,
                            table_name,
                            self.config.dataset,
                            geometry_column="geometry_spatial",
                        )
                    else:
                        # Legacy: Transform to WGS84 in silver layer
                        validate_and_transform_geometries_duckdb(
                            self.conn,
                            table_name,
                            self.config.dataset,
                            geometry_column="geometry_spatial",
                        )

                    # ✅ UPDATE: Replace original geometry column with transformed WKT
                    self.conn.execute(f"""
                        UPDATE {table_name} SET
                            geometry = ST_AsText(geometry_spatial)
                        WHERE geometry_spatial IS NOT NULL
                    """)
                else:
                    self.log.warning(
                        "⚠️ No spatial geometries found after XML conversion - skipping validation"
                    )
                    self.log.warning("This may indicate issues with geometry parsing from XML")

                # Create separate dissolved geometries for each layer type
                dissolved_tables = self._create_dissolved_df(table_name)

                # Save individual features table (contains all layers combined)
                self._save_data(
                    table_name, self.config.dataset, self.config.bucket, "silver", conn=self.conn
                )
                self.log.info("Saved individual features successfully")

                # Save each dissolved table separately
                for layer_name, dissolved_table in dissolved_tables.items():
                    # Create dataset name from layer (e.g., "grukos_indsatsomraader_dissolved")
                    layer_short = layer_name.split(":")[
                        -1
                    ]  # "indsatsomraader" or "indvindingsoplande_alle"
                    dataset_name = f"{self.config.dataset}_{layer_short}_dissolved"
                    self._save_data(
                        dissolved_table,
                        dataset_name,
                        self.config.bucket,
                        "silver",
                        conn=self.conn,
                    )
                    self.log.info(f"Saved dissolved geometries for {layer_short}")

                self.log.info("Saved all dissolved geometries successfully")

                # Return processed data for potential gold layer consumption
                return {
                    "processed_data": table_name,
                    "dissolved_tables": dissolved_tables,
                    "dataset": self.config.dataset,
                }

            except Exception as e:
                self.log.error(f"Error in Grukos silver processing: {e}")
                raise  # Re-raise the exception to ensure pipeline fails
