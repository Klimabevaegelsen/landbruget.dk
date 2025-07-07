"""
Silver layer processing for Water Projects data.

This module transforms raw data (from the bronze layer) into cleaner,
more structured data for analytical purposes. It handles the extraction
of GeoJSON features from API responses, converts them to Geos,
and applies transformations such as column renaming and geometry validation.

The module consists of two main components:
- WaterProjectsSilverConfig: Configuration for Silver processing
- WaterProjectsSilver: Implementation of Silver processing logic

The process reads in bronze layer data, transforms it into Geos,
validates geometries, and stores the processed data in GCS.
"""

import json
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any, Optional

#   # MIGRATED: Replaced with DuckDB-spatial operations
#   # MIGRATED: Replaced with DuckDB operations
# from shapely import MultiPolygon, Polygon, unary_union, wkt  # MIGRATED: Replaced with DuckDB ST_* functions
# from shapely.validation import explain_validity  # MIGRATED: Using DuckDB ST_IsValid instead
from unified_pipeline.common.base import BaseJobConfig, BaseSource, SilverJobInterface
from unified_pipeline.util.timing import AsyncTimer, timed


class WaterProjectsSilverConfig(BaseJobConfig):
    """
    Configuration for Water Projects silver layer processing.

    This configuration defines parameters for transforming water projects data
    from raw (bronze) to structured (silver) format, including dataset names,
    storage parameters, and layer definitions.

    Attributes:
        dataset (str): Name of the water projects dataset
        bucket (str): GCS bucket name for storing processed data
        storage_batch_size (int): Batch size for storage operations
        namespaces (dict[str, str]): XML namespaces used in the data
        gml_ns (str): GML namespace string
        layers (list[str]): List of layer names to process
        service_types (dict[str, str]): Mapping of layer names to service types
    """

    dataset: str = "water_projects"
    bucket: str = "landbrugsdata-raw-data"
    storage_batch_size: int = 8000  # Increased for better performance with 16GB RAM
    namespaces: dict[str, str] = {
        "wfs": "http://www.opengis.net/wfs/2.0",
        "natur": "http://wfs2-miljoegis.mim.dk/natur",
        "gml": "http://www.opengis.net/gml/3.2",
    }
    gml_ns: str = "{http://www.opengis.net/gml/3.2}"  # This is not a f-string.
    layers: list[str] = [
        "N2000_projekter:Hydrologi_E",
        "N2000_projekter:Hydrologi_F",
        "Ovrige_projekter:Vandloebsrestaurering_E",
        "Ovrige_projekter:Vandloebsrestaurering_F",
        "Vandprojekter:Fosfor_E_samlet",
        "Vandprojekter:Fosfor_F_samlet",
        "Vandprojekter:Kvaelstof_E_samlet",
        "Vandprojekter:Kvaelstof_F_samlet",
        "Vandprojekter:Lavbund_E_samlet",
        "Vandprojekter:Lavbund_F_samlet",
        "Vandprojekter:Private_vaadomraader",
        "Vandprojekter:Restaurering_af_aadale_2024",
        "vandprojekter:kla_projektforslag",
        "vandprojekter:kla_projektomraader",
        "Klima_lavbund_demarkation___offentlige_projekter:0",
    ]
    service_types: dict[str, str] = {"Klima_lavbund_demarkation___offentlige_projekter:0": "arcgis"}


class WaterProjectsSilver(BaseSource[WaterProjectsSilverConfig], SilverJobInterface):
    """
    Silver layer processing for Water Projects data.
    This class transforms raw water projects data from the bronze layer into
    structured Geos, validates geometries, and saves the processed data
    to Google Cloud Storage (GCS).
    It handles both XML and JSON data formats, extracting features and converting
    them into Geos with appropriate geometries and attributes.

    The processing includes:
    1. Reading raw data from GCS
    2. Extracting features from XML or JSON payloads
    3. Parsing geometries and calculating areas
    4. Standardizing attribute names and types
    5. Dissolving geometries based on status categories
    6. Saving processed data back to GCS
    """

    def __init__(self, config: WaterProjectsSilverConfig):
        """
        Initialize the WaterProjectsSilver processor.

        Args:
            config (WaterProjectsSilverConfig): Configuration object containing settings
                                                for the processor."""
        super().__init__(config)

        # ✅ MIGRATION: BaseSource already created GCSDataAccess and configured DuckDB
        # No need to create another instance or setup DuckDB again
        self.log.info("✅ WaterProjectsSilver: Using unified GCS access and DuckDB connection")

    def get_first_namespace(self, root: ET.Element) -> Optional[str]:
        """
        Extract the namespace from an XML root element.

        This method iterates through the XML elements to find and extract
        the first namespace used in the document.

        Args:
            root (ET.Element): The root element of an XML document.

        Returns:
            Optional[str]: The namespace string if found, None otherwise.

        Example:
            >>> namespace = get_first_namespace(root)
            >>> print(namespace)
            'http://www.opengis.net/gml/3.2'
        """
        for elem in root.iter():
            if "}" in elem.tag:
                return elem.tag.split("}")[0].strip("{")
        return None

    def clean_value(self, value: Any) -> Optional[str]:
        """
        Clean and standardize string values from XML.

        This method converts values to strings and removes leading/trailing whitespace.
        Empty strings are converted to None.

        Args:
            value (Any): The value to clean, can be any type.

        Returns:
            Optional[str]: The cleaned string value, or None if the value is empty.

        Example:
            >>> clean_value("  Example  ")
            'Example'
            >>> clean_value("")
            None
        """
        if not isinstance(value, str):
            return str(value)
        value = value.strip()
        return value if value else None

    def _parse_geometry(self, geom_elem: ET.Element) -> Optional[dict[str, Any]]:
        """
        Parse GML geometry into WKT format and calculate area.

        This method extracts polygon coordinates from GML elements and constructs
        Shapely geometry objects. It also calculates the area in hectares.

        Args:
            geom_elem (ET.Element): The XML element containing GML geometry data.

        Returns:
            Optional[dict[str, Any]]: A dictionary containing the WKT representation
                                     and area (in hectares) of the geometry, or None
                                     if parsing fails.

        Raises:
            Exception: If there are issues parsing the geometry.
        """
        try:
            multi_surface = geom_elem.find(f".//{self.config.gml_ns}MultiSurface")
            if multi_surface is None:
                self.log.error("No MultiSurface element found")
                return None

            polygons = []
            for surface_member in multi_surface.findall(f".//{self.config.gml_ns}surfaceMember"):
                polygon = surface_member.find(f".//{self.config.gml_ns}Polygon")
                if polygon is None:
                    continue

                pos_list = polygon.find(f".//{self.config.gml_ns}posList")
                if pos_list is None or not pos_list.text:
                    continue

                try:
                    pos = [float(x) for x in pos_list.text.strip().split()]

                    # Detect if coordinates are 2D or 3D and parse accordingly
                    if len(pos) < 4:  # Need at least 4 values for a polygon (2 coordinate pairs)
                        self.log.warning(f"Insufficient coordinate data: {len(pos)} values")
                        continue

                    # Parse as 2D coordinates (x, y pairs) - Danish UTM coordinates
                    if len(pos) % 2 != 0:
                        self.log.warning(
                            f"Odd number of coordinates: {len(pos)} values, cannot parse as coordinate pairs"
                        )
                        continue

                    coords = [(pos[i], pos[i + 1]) for i in range(0, len(pos), 2)]

                    if len(coords) >= 4:
                        # Ensure polygon is closed (first and last coordinate should be the same)
                        if coords[0] != coords[-1]:
                            coords.append(coords[0])

                        # Store coordinate lists instead of Shapely objects
                        polygons.append(coords)
                    else:
                        self.log.warning(
                            f"Insufficient coordinates for polygon: {len(coords)} pairs (need at least 4)"
                        )
                except Exception as e:
                    self.log.error(f"Failed to parse coordinates: {str(e)}")
                    continue

            if not polygons:
                return None

            # Create WKT geometry directly from coordinates using DuckDB-spatial format
            # Use the same approach as BNBO status pipeline for consistency
            polygon_wkts = []
            for i, coords in enumerate(polygons):
                # Create coordinate pairs with proper WKT format (space between x y, comma between pairs)
                coord_pairs = [f"{x} {y}" for x, y in coords]
                polygon_wkt = f"POLYGON(({', '.join(coord_pairs)}))"

                # Validate WKT completeness - check for proper closing
                if not polygon_wkt.endswith("))"):
                    self.log.error(
                        f"Invalid WKT detected - missing closing parentheses: {polygon_wkt[:100]}..."
                    )
                    continue

                # Check for proper opening
                if not polygon_wkt.startswith("POLYGON(("):
                    self.log.error(
                        f"Invalid WKT detected - malformed opening: {polygon_wkt[:100]}..."
                    )
                    continue

                # Count parentheses to ensure they're balanced
                open_count = polygon_wkt.count("(")
                close_count = polygon_wkt.count(")")
                if open_count != close_count:
                    self.log.error(
                        f"Invalid WKT detected - unbalanced parentheses ({open_count} open, {close_count} close): {polygon_wkt[:100]}..."
                    )
                    continue

                polygon_wkts.append(polygon_wkt)

            if not polygon_wkts:
                self.log.warning("No valid polygons found after WKT validation")
                return None

            # Create final WKT (MultiPolygon if multiple, single Polygon otherwise)
            if len(polygon_wkts) == 1:
                geometry_wkt = polygon_wkts[0]
            else:
                # Create MultiPolygon WKT - properly extract coordinate parts
                # Each polygon_wkt is like "POLYGON((x1 y1, x2 y2, ...))"
                # We need to extract just the "((x1 y1, x2 y2, ...))" part
                polygon_parts = []
                for wkt in polygon_wkts:
                    # Extract everything after "POLYGON" - this gives us "((x1 y1, x2 y2, ...))"
                    if wkt.startswith("POLYGON"):
                        coord_part = wkt[7:]  # Remove "POLYGON" prefix
                        polygon_parts.append(coord_part)
                    else:
                        self.log.warning(f"Unexpected WKT format: {wkt}")
                        continue

                if polygon_parts:
                    geometry_wkt = f"MULTIPOLYGON({', '.join(polygon_parts)})"
                else:
                    self.log.error("No valid polygon parts found for MultiPolygon")
                    return None

            # Calculate area using DuckDB-spatial
            try:
                self.conn.execute("CREATE OR REPLACE TABLE temp_geom (geometry_wkt TEXT)")
                self.conn.execute("INSERT INTO temp_geom VALUES (?)", [geometry_wkt])
                area_result = self.conn.execute("""
                    SELECT ST_Area(ST_GeomFromText(geometry_wkt)) / 10000 as area_ha
                    FROM temp_geom
                """).fetchone()
                area_ha = area_result[0] if area_result else 0
            except Exception as area_error:
                self.log.error(f"Error calculating area for feature: {str(area_error)}")
                self.log.error(f"Geometry WKT length: {len(geometry_wkt)}")
                self.log.error(f"Geometry WKT starts with: {geometry_wkt[:100]}")
                self.log.error(f"Geometry WKT ends with: {geometry_wkt[-100:]}")
                # Set area to 0 but continue processing the feature
                area_ha = 0

            return {"wkt": geometry_wkt, "area_ha": area_ha}

        except Exception as e:
            self.log.error(f"Error parsing geometry: {str(e)}")
            return None

    def _parse_feature(self, feature: ET.Element) -> Optional[dict[str, Any]]:
        """
        Parse a single XML feature into a dictionary of attributes.

        This method extracts geometry and attribute data from an XML feature element.
        It processes the geometry using _parse_geometry and extracts all other attributes
        as key-value pairs.

        Args:
            feature (ET.Element): The XML element containing feature data.

        Returns:
            Optional[dict[str, Any]]: A dictionary containing feature attributes including
                                     geometry and area, or None if parsing fails.

        Raises:
            Exception: If there are issues parsing the feature.
        """
        try:
            namespace = feature.tag.split("}")[0].strip("{")

            geom_elem = feature.find(f"{{{namespace}}}the_geom") or feature.find(
                f"{{{namespace}}}wkb_geometry"
            )
            if geom_elem is None:
                self.log.warning("No geometry found in feature")
                return None

            geometry_data = self._parse_geometry(geom_elem)
            if geometry_data is None:
                self.log.warning("Failed to parse geometry")
                return None

            data = {"geometry": geometry_data["wkt"], "area_ha": geometry_data["area_ha"]}

            for elem in feature:
                if not elem.tag.endswith(("the_geom", "wkb_geometry")):
                    key = elem.tag.split("}")[-1].lower()
                    if elem.text:
                        value: Any = self.clean_value(elem.text)
                        if value is not None:
                            # Convert specific fields
                            try:
                                if key in ["area", "budget"]:
                                    value = float(
                                        "".join(c for c in value if c.isdigit() or c == ".")
                                    )
                                elif key in ["startaar", "tilsagnsaa", "slutaar"]:
                                    value = int(value)
                                elif key in ["startdato", "slutdato"]:
                                    # ✅ MIGRATION: Use DuckDB for date parsing instead of pandas
                                    try:
                                        self.conn.execute(
                                            "CREATE OR REPLACE TABLE temp_date (date_str VARCHAR)"
                                        )
                                        self.conn.execute(
                                            "INSERT INTO temp_date VALUES (?)", [value]
                                        )
                                        result = self.conn.execute(
                                            "SELECT CAST(date_str AS DATE) as parsed_date FROM temp_date"
                                        ).fetchone()
                                        value = result[0] if result else None
                                    except:
                                        value = None
                            except (ValueError, TypeError):
                                self.log.warning(f"Failed to convert {key} value: {value}")
                                value = None
                            data[key] = value
            return data
        except Exception as e:
            self.log.error(f"Error parsing feature: {str(e)}", exc_info=True)
            return None

    @timed(name="Processing XML data")  # type: ignore
    def _process_xml_data(self, xml_data: str, layer: str) -> list[dict]:
        features = []
        # Parse the XML data
        root = ET.fromstring(xml_data)

        # Get the namespace
        namespace = self.get_first_namespace(root)
        if namespace is None:
            err_msg = "Error processing XML data: No namespace found in XML"
            self.log.error(err_msg)
            raise Exception(err_msg)
        for member in root.findall(".//ns:member", namespaces={"ns": namespace}):
            for feature in member:
                parsed = self._parse_feature(feature)
                if parsed and parsed.get("geometry"):
                    parsed["layer"] = layer
                    features.append(parsed)
        return features

    @timed(name="Processing JSON data")  # type: ignore
    def _process_json_data(self, json_data: str, layer: str) -> list[dict]:
        features = []
        data = json.loads(json_data)
        for feature in data.get("features", []):
            attrs = feature.get("attributes", {})
            geom = feature.get("geometry", {})

            if "rings" not in geom:
                continue

            # Convert geometry using DuckDB-spatial
            # In ArcGIS/GeoJSON format, rings represent exterior and interior rings of a polygon
            # First ring is exterior, subsequent rings are holes (interior rings)
            if not geom["rings"]:
                continue

            rings_wkt = []
            for ring in geom["rings"]:
                coords = [(x, y) for x, y in ring]
                if len(coords) >= 4:
                    points = ", ".join([f"{x} {y}" for x, y in coords])
                    rings_wkt.append(f"({points})")

            if not rings_wkt:
                continue

            # Create single polygon with exterior ring and interior rings (holes)
            # Format: POLYGON((exterior_ring), (interior_ring1), (interior_ring2), ...)
            geometry_wkt = f"POLYGON({', '.join(rings_wkt)})"

            # Calculate area using DuckDB-spatial
            try:
                self.conn.execute("CREATE OR REPLACE TABLE temp_geom (geometry_wkt TEXT)")
                self.conn.execute("INSERT INTO temp_geom VALUES (?)", [geometry_wkt])
                area_result = self.conn.execute("""
                    SELECT ST_Area(ST_GeomFromText(geometry_wkt)) / 10000 as area_ha
                    FROM temp_geom
                """).fetchone()
                area_ha = area_result[0] if area_result else 0
            except Exception as area_error:
                self.log.error(f"Error calculating area for feature: {str(area_error)}")
                self.log.error(f"Geometry WKT length: {len(geometry_wkt)}")
                self.log.error(f"Geometry WKT starts with: {geometry_wkt[:100]}")
                self.log.error(f"Geometry WKT ends with: {geometry_wkt[-100:]}")
                # Set area to 0 but continue processing the feature
                area_ha = 0

            # Convert timestamps
            start_date = (
                datetime.fromtimestamp(attrs.get("projektstart") / 1000)
                if attrs.get("projektstart")
                else None
            )
            end_date = (
                datetime.fromtimestamp(attrs.get("projektslut") / 1000)
                if attrs.get("projektslut")
                else None
            )

            processed_feature = {
                "layer_name": layer,
                "geometry": geometry_wkt,
                "area_ha": area_ha,
                "projektnavn": attrs.get("projektnavn"),
                "enhedskontakt": attrs.get("enhedskontakt"),
                "startdato": start_date,
                "slutdato": end_date,
                "status": attrs.get("status"),
                "object_id": attrs.get("OBJECTID"),
                "global_id": attrs.get("GlobalID"),
            }

            features.append(processed_feature)

        return features

    @timed(name="Processing bronze data")  # type: ignore
    def _process_data(self, raw_data) -> Optional[str]:
        """
        Process raw data from the bronze layer into a DuckDB table for the silver layer.
        This method extracts features from the raw data, processes them into a list of dictionaries,
        and creates a DuckDB table with geometries.

        Args:
            raw_data:  containing raw data from the bronze layer.

        Returns:
            Optional[str]: Name of DuckDB table containing processed features with
                          geometries or None if processing fails or no features
                          are extracted.
        """
        # ✅ MIGRATION: Handle only table names now
        if isinstance(raw_data, str):
            # Keep as table name and process directly
            table_name = raw_data
            raw_data_rows = self.conn.execute(f"SELECT payload, layer FROM {table_name}").fetchall()
        else:
            self.log.error(f"Expected table name (string), got {type(raw_data)}")
            return None

        if not raw_data_rows:
            self.log.warning("No raw data to process")
            return None

        self.log.info("Processing raw data from bronze")
        features = []
        for index, (data, layer) in enumerate(raw_data_rows):
            try:
                service_type = self.config.service_types.get(layer, "wfs")
                self.log.debug(
                    f"Processing row {index + 1}: layer={layer}, service_type={service_type}"
                )

                if service_type == "arcgis":
                    processed_features = self._process_json_data(data, layer)
                    self.log.debug(f"JSON processing returned {len(processed_features)} features")
                    features.extend(processed_features)
                else:
                    processed_features = self._process_xml_data(data, layer)
                    self.log.debug(f"XML processing returned {len(processed_features)} features")
                    features.extend(processed_features)
            except Exception as e:
                self.log.error(f"Error processing row {index + 1} (layer: {layer}): {str(e)}")
                self.log.error(
                    f"Data type: {type(data)}, Data length: {len(data) if data else 'None'}"
                )
                if data:
                    self.log.error(f"Data starts with: {data[:100]}")
                    self.log.error(f"Data ends with: {data[-100:]}")
                continue
        if not features:
            self.log.warning("No features extracted from raw data")
            return None
        self.log.info(f"Extracted {len(features):,} features from raw data")

        # ✅ MIGRATION: Create DuckDB table instead of pandas DataFrame
        # Create table directly from the list of dictionaries using DuckDB's native capabilities
        # Get column names from the first feature
        columns = list(features[0].keys())

        # Create the table schema
        # Use appropriate column types - geometry needs to be TEXT to store long WKT strings
        column_definitions = []
        for col in columns:
            if col == "geometry":
                column_definitions.append(f"{col} TEXT")  # Use TEXT for long WKT strings
            elif col in ["area_ha", "budget"]:
                column_definitions.append(f"{col} DOUBLE")  # Use DOUBLE for numeric values
            elif col in ["startaar", "tilsagnsaa", "slutaar", "object_id"]:
                column_definitions.append(f"{col} INTEGER")  # Use INTEGER for numeric IDs
            elif col in ["startdato", "slutdato"]:
                column_definitions.append(f"{col} DATE")  # Use DATE for date values
            else:
                column_definitions.append(f"{col} VARCHAR")  # Default to VARCHAR for other fields

        self.conn.execute(f"""
            CREATE OR REPLACE TABLE temp_features (
                {", ".join(column_definitions)}
            )
        """)

        # Insert data in batches using parameterized queries to avoid SQL injection and truncation
        batch_size = 1000
        for i in range(0, len(features), batch_size):
            batch = features[i : i + batch_size]

            # Use parameterized queries instead of string concatenation
            for feature in batch:
                values = [feature.get(col) for col in columns]
                placeholders = ", ".join(["?" for _ in columns])

                self.conn.execute(
                    f"""
                    INSERT INTO temp_features ({", ".join(columns)})
                    VALUES ({placeholders})
                """,
                    values,
                )
        table_name = "water_projects_processed"

        # Create the final table, handling geometry conversion failures gracefully
        # First, create a table with valid geometries only
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE {table_name}_temp AS
            SELECT *
            FROM temp_features
            WHERE geometry IS NOT NULL
            AND geometry LIKE '%))' -- Only include properly closed geometries
        """)

        # Now add geometry_spatial column by testing each geometry individually
        self.conn.execute(f"""
            ALTER TABLE {table_name}_temp ADD COLUMN geometry_spatial GEOMETRY
        """)

        # Process geometries in batches to handle failures gracefully
        total_rows = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}_temp").fetchone()[0]
        batch_size = 100
        successful_conversions = 0
        failed_conversions = 0

        for offset in range(0, total_rows, batch_size):
            batch_geometries = self.conn.execute(f"""
                SELECT geometry FROM {table_name}_temp 
                LIMIT {batch_size} OFFSET {offset}
            """).fetchall()

            for i, (geom_wkt,) in enumerate(batch_geometries):
                try:
                    # Test the geometry conversion first
                    self.conn.execute("CREATE OR REPLACE TABLE temp_geom_test (wkt TEXT)")
                    self.conn.execute("INSERT INTO temp_geom_test VALUES (?)", [geom_wkt])
                    result = self.conn.execute(
                        "SELECT ST_GeomFromText(wkt) FROM temp_geom_test"
                    ).fetchone()

                    # If successful, update the main table
                    self.conn.execute(
                        f"""
                        UPDATE {table_name}_temp 
                        SET geometry_spatial = ST_GeomFromText(?)
                        WHERE geometry = ?
                    """,
                        [geom_wkt, geom_wkt],
                    )
                    successful_conversions += 1

                except Exception as e:
                    failed_conversions += 1
                    self.log.warning(f"Failed to convert geometry {offset + i + 1}: {str(e)}")
                    self.log.warning(
                        f"Geometry length: {len(geom_wkt)}, starts with: {geom_wkt[:100]}"
                    )
                    # Leave geometry_spatial as NULL for this row

        # Create the final table with only successfully converted geometries
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM {table_name}_temp
            WHERE geometry_spatial IS NOT NULL
        """)

        # Clean up temporary table
        self.conn.execute(f"DROP TABLE IF EXISTS {table_name}_temp")
        self.conn.execute("DROP TABLE IF EXISTS temp_geom_test")

        feature_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

        total_processed = successful_conversions + failed_conversions
        success_rate = (
            (successful_conversions / total_processed * 100) if total_processed > 0 else 0
        )

        self.log.info("Geometry conversion results:")
        self.log.info(f"  - Successfully converted: {successful_conversions:,} geometries")
        self.log.info(f"  - Failed conversions: {failed_conversions:,} geometries")
        self.log.info(
            f"  - Total success rate: {success_rate:.1f}% ({successful_conversions:,}/{total_processed:,})"
        )
        self.log.info(f"  - Final feature count: {feature_count:,} features")

        if failed_conversions > 0:
            self.log.warning(
                f"⚠️  {failed_conversions} features excluded due to geometry conversion failures"
            )
        else:
            self.log.info("✅ All geometries converted successfully!")

        self.log.info(f"Created DuckDB table '{table_name}' with {feature_count:,} features")

        return table_name

    @timed(name="Creating dissolved table using DuckDB-spatial")  # type: ignore
    def _create_dissolved_df(self, input_table_name: str, dataset: str) -> str:
        """
        Create dissolved water project features using DuckDB-spatial ST_Union_Agg.

        This method takes a DuckDB table containing water project features with geometries
        and dissolves overlapping features using DuckDB-spatial operations for optimal
        performance.

        Args:
            input_table_name (str): Name of the DuckDB table containing features with geometries
            dataset (str): Name of the dataset being processed (for logging)

        Returns:
            str: Name of DuckDB table containing the dissolved geometries using
                 DuckDB-spatial ST_Union_Agg operations.
        """
        try:
            dissolved_table_name = f"{dataset}_dissolved"

            # Check if input table has data
            feature_count = self.conn.execute(
                f"SELECT COUNT(*) FROM {input_table_name}"
            ).fetchone()[0]
            if feature_count == 0:
                self.log.warning(f"No data to dissolve for {dataset}")
                # Create empty table with proper schema
                self.conn.execute(f"""
                    CREATE OR REPLACE TABLE {dissolved_table_name} AS
                    SELECT 
                        CAST(NULL AS VARCHAR) as project_id,
                        CAST(NULL AS GEOMETRY) as geometry,
                        CAST(NULL AS INTEGER) as feature_count,
                        CAST(NULL AS TIMESTAMP) as dissolved_at
                    WHERE FALSE
                """)
                return dissolved_table_name

            self.log.info(f"Dissolving {feature_count} features for {dataset} using DuckDB-spatial")

            # First, let's check how many valid geometries we have
            valid_geom_count = self.conn.execute(f"""
                SELECT COUNT(*) FROM {input_table_name}
                WHERE geometry_spatial IS NOT NULL
                AND ST_IsValid(geometry_spatial)
            """).fetchone()[0]

            self.log.info(
                f"Found {valid_geom_count} valid geometries out of {feature_count} total features"
            )

            if valid_geom_count == 0:
                self.log.warning(f"No valid geometries found for dissolving {dataset}")
                # Create empty table
                self.conn.execute(f"""
                    CREATE OR REPLACE TABLE {dissolved_table_name} AS
                    SELECT 
                        CAST(NULL AS VARCHAR) as project_id,
                        CAST(NULL AS GEOMETRY) as geometry,
                        CAST(0 AS INTEGER) as feature_count,
                        current_timestamp as dissolved_at
                    WHERE FALSE
                """)
                return dissolved_table_name

            # Use DuckDB-spatial ST_Union_Agg to dissolve overlapping geometries
            # Transform geometries to EPSG:4326 first, then dissolve
            # ✅ COORDINATE FIX: Apply ST_FlipCoordinates to fix swapped lat/lon coordinates
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE {dissolved_table_name}_temp AS
                SELECT 
                    ST_FlipCoordinates(ST_Transform(geometry_spatial, 'EPSG:25832', 'EPSG:4326')) as geometry_4326
                FROM {input_table_name}
                WHERE geometry_spatial IS NOT NULL
                AND ST_IsValid(geometry_spatial)
            """)

            # Now dissolve the transformed geometries
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE {dissolved_table_name} AS
                SELECT 
                    'water_project_dissolved' as project_id,
                    ST_Union_Agg(geometry_4326) as geometry,
                    COUNT(*) as feature_count,
                    current_timestamp as dissolved_at
                FROM {dissolved_table_name}_temp
                WHERE geometry_4326 IS NOT NULL
            """)

            # Clean up temp table
            self.conn.execute(f"DROP TABLE IF EXISTS {dissolved_table_name}_temp")

            final_count = self.conn.execute(
                f"SELECT COUNT(*) FROM {dissolved_table_name}"
            ).fetchone()[0]

            self.log.info(
                f"Successfully dissolved {dataset} features using DuckDB-spatial ST_Union_Agg"
            )
            self.log.info(f"Dissolved {feature_count} features into {final_count} geometries")
            return dissolved_table_name

        except Exception as e:
            self.log.error(f"Error dissolving features for {dataset}: {e}")
            # Create empty table on error
            empty_table_name = f"{dataset}_dissolved_empty"
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE {empty_table_name} AS
                SELECT 
                    CAST(NULL AS VARCHAR) as project_id,
                    CAST(NULL AS GEOMETRY) as geometry,
                    CAST(0 AS INTEGER) as feature_count,
                    current_timestamp as dissolved_at
                WHERE FALSE
            """)
            return empty_table_name

    async def run(self, bronze_data: Optional[Any] = None) -> Optional[Any]:
        """
        Run the Water Projects silver layer processing.

        Args:
            bronze_data: Optional in-memory data from bronze stage. If provided,
                        this data will be used instead of reading from storage.

        Returns:
            Optional[Any]: Processed data that can be passed to gold stage,
                          or None if processing fails.
        """
        self.log.info("Running Water Projects silver job for")
        async with AsyncTimer("Water Projects silver job"):
            # Read data with support for in-memory passing
            if bronze_data is not None:
                self.log.info("Using bronze data from memory (in-memory data passing)")
                # Bronze data is expected to be a list of tuples (layer, raw_data)
                if isinstance(bronze_data, list):
                    # ✅ MIGRATION: Create  using DuckDB instead of pandas
                    current_timestamp = self.conn.execute("SELECT current_timestamp").fetchone()[0]

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

                    # Create table directly from the list of dictionaries
                    if not raw_data_list:
                        self.log.warning("No raw data to process")
                        return None

                    # Get column names from the first item
                    columns = list(raw_data_list[0].keys())

                    # Create the table schema with proper column types
                    column_definitions = []
                    for col in columns:
                        if col == "payload":
                            column_definitions.append(f"{col} TEXT")  # Use TEXT for payload data
                        else:
                            column_definitions.append(
                                f"{col} VARCHAR"
                            )  # Default to VARCHAR for other fields

                    self.conn.execute(f"""
                        CREATE OR REPLACE TABLE temp_raw_data (
                            {", ".join(column_definitions)}
                        )
                    """)

                    # Insert data in batches
                    batch_size = 1000
                    for i in range(0, len(raw_data_list), batch_size):
                        batch = raw_data_list[i : i + batch_size]

                        # Use parameterized queries instead of string concatenation
                        for item in batch:
                            values = [item.get(col) for col in columns]
                            placeholders = ", ".join(["?" for _ in columns])

                            self.conn.execute(
                                f"""
                                INSERT INTO temp_raw_data ({", ".join(columns)})
                                VALUES ({placeholders})
                            """,
                                values,
                            )
                    # ✅ MIGRATION: Keep as table instead of converting to
                    raw_data = "temp_raw_data"
                else:
                    self.log.error(
                        f"Expected list of tuples from bronze stage, got {type(bronze_data)}"
                    )
                    return None
            else:
                # Fallback to reading from storage
                self.log.info("Reading bronze data from storage (fallback)")
                raw_data = self._read_bronze_data(self.config.dataset, self.config.bucket)
                if raw_data is None:
                    self.log.error("Failed to read raw data")
                    return None

            self.log.info("Read raw data successfully")
            table_name = self._process_data(raw_data)
            if table_name is None:
                self.log.error("Failed to process raw data")
                return None
            self.log.info("Processed raw data successfully")
            dissolved_table_name = self._create_dissolved_df(table_name, self.config.dataset)

            # ✅ MIGRATION: Save DuckDB tables using standard _save_data method like other pipelines
            self._save_data(
                table_name, self.config.dataset, self.config.bucket, "silver", conn=self.conn
            )
            self._save_data(
                dissolved_table_name,
                f"{self.config.dataset}_dissolved",
                self.config.bucket,
                "silver",
                conn=self.conn,
            )

            self.log.info("Saved processed data successfully")

            # Return processed data for potential gold layer consumption
            return {
                "processed_data": table_name,
                "dissolved_data": dissolved_table_name,
                "dataset": self.config.dataset,
            }
