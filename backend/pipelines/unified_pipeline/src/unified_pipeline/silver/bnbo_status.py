"""
Silver layer processing for BNBO Status data.

This module transforms raw data (from the bronze layer) into cleaner,
more structured data for analytical purposes. It handles the extraction
of GeoJSON features from API responses, converts them using DuckDB-spatial,
and applies transformations such as column renaming and geometry validation.

The module consists of two main components:
- BNBOStatusSilverConfig: Configuration for Silver processing
- BNBOStatusSilver: Implementation of Silver processing logic using DuckDB-spatial

The process reads in bronze layer data, transforms it using DuckDB-spatial,
validates geometries, and stores the processed data in GCS.
"""

import xml.etree.ElementTree as ET
from typing import Any, Optional

# ✅ MIGRATION: Removed pandas/geopandas imports - using DuckDB-spatial for all operations
# ✅ MIGRATION: Removed shapely imports - using pure coordinate-based WKT generation
from unified_pipeline.common.base import BaseJobConfig, BaseSource, SilverJobInterface
from unified_pipeline.util.timing import AsyncTimer, timed


class BNBOStatusSilverConfig(BaseJobConfig):
    """
    Configuration for BNBO (Boringsnære Beskyttelsesområder) status data source.

    This class defines the configuration parameters for processing BNBO status data
    in the silver layer of the data pipeline. It specifies dataset names, storage
    settings, and mappings for transforming status values.

    Attributes:
        dataset (str): The name of the dataset, defaults to "bnbo_status".
        bucket (str): The GCS bucket name where data is stored,
                        defaults to "landbrugsdata-raw-data".
        storage_batch_size (int): The batch size for storage operations, defaults to 5000.
        status_mapping (dict): A mapping from detailed status descriptions to simplified categories.
        gml_ns (str): The GML namespace used in the XML data.
    """

    dataset: str = "bnbo_status"
    bucket: str = "landbrugsdata-raw-data"
    storage_batch_size: int = 5000
    status_mapping: dict[str, str] = {
        "Frivillig aftale tilbudt (UDGÅET)": "Action Required",
        "Gennemgået, indsats nødvendig": "Action Required",
        "Ikke gennemgået (default værdi)": "Action Required",
        "Gennemgået, indsats ikke nødvendig": "Completed",
        "Indsats gennemført": "Completed",
        "Ingen erhvervsmæssig anvendelse af pesticider": "Completed",
    }
    gml_ns: str = "{http://www.opengis.net/gml/3.2}"  # This is not a f-string.


class BNBOStatusSilver(BaseSource[BNBOStatusSilverConfig], SilverJobInterface):
    """
    Silver layer processor for BNBO status data.

    This class handles the processing of BNBO status data from the bronze layer
    to the silver layer. It reads, transforms, and saves the data according to
    the data pipeline architecture. The class handles XML processing, geometry
    operations, and data storage in GCS.

    The processing includes:
    1. Reading raw XML data from the bronze layer.
    2. Parsing XML features and extracting geometries.
    3. Converting geometries to WKT format and calculating areas.
    4. Creating a Geo with the processed features.
    5. Dissolving geometries based on status categories.
    6. Saving the processed data back to GCS.
    """

    def __init__(self, config: BNBOStatusSilverConfig):
        """
        Initialize the BNBOStatusSilver processor.

        Args:
            config (BNBOStatusSilverConfig): Configuration object for the processor."""
        super().__init__(config)

        # ✅ MIGRATION: BaseSource already created GCSDataAccess and configured DuckDB
        # No need to create another instance or setup DuckDB again
        self.log.info("✅ BNBOStatusSilver: Using unified GCS access and DuckDB connection")

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
        Parse GML geometry into WKT format using pure coordinate-based approach.

        ✅ OPTIMIZED: This method now creates WKT directly from coordinates without
        using shapely, and uses DuckDB ST_Area for area calculation.

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

            polygon_wkts = []
            for surface_member in multi_surface.findall(f".//{self.config.gml_ns}surfaceMember"):
                polygon = surface_member.find(f".//{self.config.gml_ns}Polygon")
                if polygon is None:
                    continue

                pos_list = polygon.find(f".//{self.config.gml_ns}posList")
                if pos_list is None or not pos_list.text:
                    continue

                try:
                    pos = [float(x) for x in pos_list.text.strip().split()]
                    coords = [(pos[i], pos[i + 1]) for i in range(0, len(pos), 2)]
                    if len(coords) >= 4:
                        # Create WKT polygon directly from coordinates
                        coord_pairs = [f"{x} {y}" for x, y in coords]
                        polygon_wkt = f"POLYGON(({', '.join(coord_pairs)}))"
                        polygon_wkts.append(polygon_wkt)
                except Exception as e:
                    self.log.error(f"Failed to parse coordinates: {str(e)}")
                    continue

            if not polygon_wkts:
                return None

            # Create final WKT (MultiPolygon if multiple, single Polygon otherwise)
            if len(polygon_wkts) == 1:
                final_wkt = polygon_wkts[0]
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
                    final_wkt = f"MULTIPOLYGON({', '.join(polygon_parts)})"
                else:
                    self.log.error("No valid polygon parts found for MultiPolygon")
                    return None

            # ✅ OPTIMIZED: Use DuckDB ST_Area for area calculation instead of shapely
            try:
                area_result = self.conn.execute(
                    "SELECT ST_Area(ST_GeomFromText(?)) as area", [final_wkt]
                ).fetchone()
                area_ha = area_result[0] / 10000 if area_result and area_result[0] else 0.0
            except Exception as e:
                self.log.warning(f"Could not calculate area with DuckDB: {e}, defaulting to 0")
                area_ha = 0.0

            return {"wkt": final_wkt, "area_ha": area_ha}

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

            geom_elem = feature.find("{%s}Shape" % namespace)
            if geom_elem is None:
                self.log.warning("No geometry found in feature")
                return None

            geometry_data = self._parse_geometry(geom_elem)
            if geometry_data is None:
                self.log.warning("Failed to parse geometry")
                return None

            data = {"geometry": geometry_data["wkt"], "area_ha": geometry_data["area_ha"]}

            for elem in feature:
                if not elem.tag.endswith("Shape"):
                    key = elem.tag.split("}")[-1].lower()
                    if elem.text:
                        value = self.clean_value(elem.text)
                        if value is not None:
                            data[key] = value

            # Map the status to simplified categories
            if "status_bnbo" in data:
                data["status_category"] = self.config.status_mapping.get(
                    data["status_bnbo"], "Unknown"
                )

            return data

        except Exception as e:
            self.log.error(f"Error parsing feature: {str(e)}", exc_info=True)
            return None

    @timed(name="Processing bronze data")  # type: ignore
    def _process_xml_data(self, raw_data) -> Optional[str]:
        """
        Process XML data from the bronze layer using DuckDB-spatial.

        This method iterates through all XML data, parses features using _parse_feature,
        and constructs a DuckDB table with the extracted geometries and attributes.

        Args:
            raw_data: Raw data containing XML data in a 'payload' column.

        Returns:
            Optional[str]: Table name in DuckDB containing the processed features,
                          or None if processing fails.

        Raises:
            Exception: If there are issues processing the XML data.
        """
        if raw_data is None:
            self.log.warning("No raw data to process")
            return None

        # ✅ MIGRATION: Handle DuckDB table name (string) from bronze layer
        if isinstance(raw_data, str):
            # raw_data is a DuckDB table name from bronze layer
            raw_df_table = raw_data
            # Get row count to check if empty
            row_count = self.conn.execute(f"SELECT COUNT(*) FROM {raw_data}").fetchone()[0]
            if row_count == 0:
                self.log.warning("No raw data to process")
                return None
            # Get data for iteration - only fetch when needed
            raw_df = self.conn.execute(f"SELECT * FROM {raw_data}").fetchall()
            columns = [desc[0] for desc in self.conn.description]
        elif isinstance(raw_data, dict) and "payload" in raw_data:
            # Handle dictionary format for backward compatibility (mainly for tests)
            self.log.info("Processing dictionary format data (test compatibility)")
            raw_df = [(payload,) for payload in raw_data["payload"]]
            columns = ["payload"]
        else:
            # Handle other data types (fallback)
            self.log.warning(f"Unexpected raw_data type: {type(raw_data)}")
            return None

        self.log.info("Processing XML data from bronze layer using DuckDB-spatial")

        features = []
        # ✅ MIGRATION: Process fetchall() results from DuckDB table
        for index, row_tuple in enumerate(raw_df):
            try:
                # Convert tuple to dict using column names
                row = dict(zip(columns, row_tuple))
                # Parse the XML data
                xml_data = row["payload"]
                root = ET.fromstring(xml_data)

                # Get the namespace
                namespace = self.get_first_namespace(root)
                if namespace is None:
                    err_msg = f"Error processing row {index}: No namespace found in XML"
                    self.log.error(err_msg)
                    raise Exception(err_msg)
                for member in root.findall(".//ns:member", namespaces={"ns": namespace}):
                    for feature in member:
                        parsed = self._parse_feature(feature)
                        if parsed and parsed.get("geometry"):
                            features.append(parsed)

            except Exception as e:
                self.log.error(f"Error processing row {index}: {str(e)}", exc_info=True)
                raise e

        self.log.info(f"Parsed {len(features):,} features from XML data")

        # ✅ MIGRATION: Create DuckDB table instead of Geo
        if not features:
            self.log.warning("No features extracted from XML data")
            return None

        # Create table directly from the list of dictionaries using DuckDB's native capabilities
        # Get column names from the first feature
        columns = list(features[0].keys())

        # Create the table schema
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE bnbo_features_raw (
                {", ".join([f"{col} VARCHAR" for col in columns])}
            )
        """)

        # Insert data in batches
        batch_size = 1000
        for i in range(0, len(features), batch_size):
            batch = features[i : i + batch_size]

            # Use parameterized queries instead of string concatenation
            for feature in batch:
                values = [feature.get(col) for col in columns]
                placeholders = ", ".join(["?" for _ in columns])

                self.conn.execute(
                    f"""
                    INSERT INTO bnbo_features_raw ({", ".join(columns)})
                    VALUES ({placeholders})
                """,
                    values,
                )

                table_name = "bnbo_processed_features"

        self.conn.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT 
                *,
                ST_GeomFromText(geometry) as geometry_spatial
            FROM bnbo_features_raw
            WHERE geometry IS NOT NULL
        """)

        feature_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        self.log.info(f"Created DuckDB table '{table_name}' with {feature_count:,} features")

        # Clean up temporary table
        self.conn.execute("DROP TABLE IF EXISTS bnbo_features_raw")

        return table_name

    @timed(name="Creating dissolved table using DuckDB-spatial")  # type: ignore
    def _create_dissolved_df(self, input_table_name: str, dataset: str) -> str:
        """
        Create dissolved table for BNBO status data using DuckDB-spatial.

        Args:
            input_table_name: Name of the DuckDB table containing processed features
            dataset: Dataset name for logging

        Returns:
            str: Name of the DuckDB table containing dissolved geometries
        """
        try:
            dissolved_table_name = "bnbo_dissolved_features"

            self.log.info(
                "Creating dissolved geometries by status category using DuckDB-spatial..."
            )

            # Create dissolved geometries for each category
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE {dissolved_table_name} AS
                SELECT 
                    status_category,
                    ST_Union_Agg(geometry_spatial) as dissolved_geometry
                FROM {input_table_name}
                WHERE ST_IsValid(geometry_spatial)
                    AND status_category IN ('Action Required', 'Completed')
                GROUP BY status_category
            """)

            # Check what categories we have
            categories_result = self.conn.execute(f"""
                SELECT status_category, dissolved_geometry
                FROM {dissolved_table_name}
                ORDER BY status_category
            """).fetchall()

            if not categories_result:
                self.log.warning("No valid geometries to dissolve")
                # Create empty table with proper schema
                self.conn.execute(f"""
                    CREATE OR REPLACE TABLE {dissolved_table_name} AS
                    SELECT 
                        CAST(NULL AS VARCHAR) as status_category,
                        CAST(NULL AS GEOMETRY) as geometry,
                        CAST(NULL AS TIMESTAMP) as dissolved_at
                    WHERE FALSE
                """)
                return dissolved_table_name

            # Handle overlaps - remove completed areas that overlap with action required
            action_required_geom = None
            completed_geom = None

            for category, dissolved_geom in categories_result:
                if category == "Action Required":
                    action_required_geom = dissolved_geom
                elif category == "Completed":
                    completed_geom = dissolved_geom

            # Process overlaps using DuckDB-spatial ST_Difference
            if action_required_geom and completed_geom:
                self.log.info("Handling overlaps between Action Required and Completed areas...")

                # Create final table with overlap handling
                self.conn.execute(f"""
                    CREATE OR REPLACE TABLE {dissolved_table_name}_final AS
                    SELECT 
                        'Action Required' as status_category,
                        dissolved_geometry as geometry,
                        CURRENT_TIMESTAMP as dissolved_at
                    FROM {dissolved_table_name}
                    WHERE status_category = 'Action Required'
                    
                    UNION ALL
                    
                    SELECT 
                        'Completed' as status_category,
                        ST_Difference(
                            (SELECT dissolved_geometry FROM {dissolved_table_name} WHERE status_category = 'Completed'),
                            (SELECT dissolved_geometry FROM {dissolved_table_name} WHERE status_category = 'Action Required')
                        ) as geometry,
                        CURRENT_TIMESTAMP as dissolved_at
                """)
            else:
                # No overlaps to handle, use original dissolved geometries
                self.conn.execute(f"""
                    CREATE OR REPLACE TABLE {dissolved_table_name}_final AS
                    SELECT 
                        status_category,
                        dissolved_geometry as geometry,
                        CURRENT_TIMESTAMP as dissolved_at
                    FROM {dissolved_table_name}
                """)

            # Clean up intermediate table and rename final
            self.conn.execute(f"DROP TABLE IF EXISTS {dissolved_table_name}")
            self.conn.execute(
                f"ALTER TABLE {dissolved_table_name}_final RENAME TO {dissolved_table_name}"
            )

            # Get final count
            final_count = self.conn.execute(f"""
                SELECT COUNT(*) FROM {dissolved_table_name}
                WHERE geometry IS NOT NULL
                    AND NOT ST_IsEmpty(geometry)
            """).fetchone()[0]

            self.log.info(
                f"Created dissolved table '{dissolved_table_name}' with {final_count} dissolved geometries"
            )
            return dissolved_table_name

        except Exception as e:
            self.log.error(f"Error creating dissolved geometries: {str(e)}", exc_info=True)
            # Create empty table on error
            empty_table_name = "bnbo_dissolved_empty"
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE {empty_table_name} AS
                SELECT 
                    CAST(NULL AS VARCHAR) as status_category,
                    CAST(NULL AS GEOMETRY) as geometry,
                    CAST(NULL AS TIMESTAMP) as dissolved_at
                WHERE FALSE
            """)
            return empty_table_name

    async def run(self, bronze_data: Optional[Any] = None) -> None:
        """
        Run the complete BNBO status silver layer processing job.

        This is the main entry point that orchestrates the entire process:
        1. Reads data from the bronze layer (either in-memory or from storage)
        2. Processes XML data into a Geo
        3. Creates a dissolved version of the Geo
        4. Saves both the original and dissolved data to GCS

        Args:
            bronze_data: Optional in-memory data from bronze stage. If provided,
                        this data will be used instead of reading from storage.

        Returns:
            None

        Raises:
            Exception: If there are issues at any step in the process.
        """
        self.log.info("Running BNBO status silver job")
        async with AsyncTimer("Running BNBO status silver job for"):
            # Read data with support for in-memory passing
            raw_data = self._read_bronze_data(
                self.config.dataset, self.config.bucket, bronze_data=bronze_data
            )
            if raw_data is None:
                self.log.error("Failed to read raw data")
                return
            self.log.info("Read raw data successfully")

            table_name = self._process_xml_data(raw_data)
            if table_name is None:
                self.log.error("Failed to process raw data")
                return
            self.log.info("Processed raw data successfully")

            dissolved_table_name = self._create_dissolved_df(table_name, self.config.dataset)

            # ✅ MIGRATION: Save DuckDB tables using optimized methods
            # Save the main features table directly from main connection
            self.save_data_direct(table_name, self.config.dataset, self.config.bucket, "silver")

            # Save the dissolved features table directly from main connection
            self.save_data_direct(
                dissolved_table_name,
                f"{self.config.dataset}_dissolved",
                self.config.bucket,
                "silver",
            )

            self.log.info("Saved processed data successfully")
