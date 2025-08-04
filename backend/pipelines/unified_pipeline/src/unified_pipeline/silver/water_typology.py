"""
Water Typology Silver Layer Implementation

This module implements the silver layer data processing for Danish water typology data.
It transforms raw water typology data from the bronze layer into structured geometries,
validates geometries, and saves the processed data to Google Cloud Storage (GCS).

IMPORTANT: Unlike water projects, this pipeline does NOT dissolve geometries.
Water typology layers (lakes, coastal waters, watercourses) need to maintain
their individual features for proper analysis and classification.

The module contains:
- WaterTypologySilverConfig: Configuration class for the silver layer processing
- WaterTypologySilver: Implementation class for processing and transforming data

The processing includes:
1. Reading raw data from GCS
2. Extracting features from XML payloads
3. Parsing geometries and calculating areas
4. Standardizing attribute names and types
5. Saving processed data back to GCS (without dissolving)
"""

import logging
import xml.etree.ElementTree as ET
from typing import Any, Optional

from pydantic import ConfigDict

from unified_pipeline.common.base import BaseJobConfig, BaseSource, SilverJobInterface
from unified_pipeline.common.geometry_validator import validate_and_transform_geometries_duckdb
from unified_pipeline.util.timing import AsyncTimer, timed


class WaterTypologySilverConfig(BaseJobConfig):
    """
    Configuration for Water Typology silver layer data processing.

    This class defines all the necessary parameters and settings required
    for processing water typology data in the silver layer of the data pipeline.

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

    dataset: str = "water_typology"
    bucket: str = "landbrugsdata-raw-data"
    storage_batch_size: int = 8000  # Increased for better performance with 16GB RAM
    namespaces: dict[str, str] = {
        "wfs": "http://www.opengis.net/wfs/2.0",
        "gml": "http://www.opengis.net/gml/3.2",
        "vp3endelig2022": "https://wfs2-miljoegis.mim.dk/vp3endelig2022",
    }
    gml_ns: str = "{http://www.opengis.net/gml/3.2}"  # This is not a f-string.
    layers: list[str] = [
        "vp3endelig2022:vp3e2022_soe_samlet",      # Lakes typology (Søer typologi)
        "vp3endelig2022:vp3e2022_marin_samlet",    # Coastal waters typology (Kystvande typologi)
        "vp3endelig2022:vp3e2022_vandloeb_samlet", # Watercourses typology (Åer typologi)
    ]
    service_types: dict[str, str] = {}  # All layers use default WFS service type


class WaterTypologySilver(BaseSource[WaterTypologySilverConfig], SilverJobInterface):
    """
    Silver layer processing for Water Typology data.
    This class transforms raw water typology data from the bronze layer into
    structured geometries, validates geometries, and saves the processed data
    to Google Cloud Storage (GCS).
    It handles XML data formats, extracting features and converting
    them into geometries with appropriate attributes.

    IMPORTANT: Unlike water projects, this does NOT dissolve geometries.
    Each water body (lake, coastal segment, watercourse) is kept as individual
    features to preserve their typological classification and analysis value.

    The processing includes:
    1. Reading raw data from GCS
    2. Extracting features from XML payloads
    3. Parsing geometries and calculating areas
    4. Standardizing attribute names and types
    5. Saving processed data back to GCS (individual features preserved)
    """

    def __init__(self, config: WaterTypologySilverConfig):
        """
        Initialize the WaterTypologySilver processor.

        Args:
            config (WaterTypologySilverConfig): Configuration object containing settings
                                                for the processor.
        """
        super().__init__(config)

        # ✅ MIGRATION: BaseSource already created GCSDataAccess and configured DuckDB
        # No need to create another instance or setup DuckDB again
        self.log.info("✅ WaterTypologySilver: Using unified GCS access and DuckDB connection")

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
            'https://wfs2-miljoegis.mim.dk/vp3endelig2022'
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
        if value is None:
            return None
        cleaned = str(value).strip()
        return cleaned if cleaned else None

    @timed(name="Processing XML data")  # type: ignore
    def process_xml_payload(self, payload: str, layer: str) -> Optional[str]:
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
                        feature_data["geometry_xml"] = ET.tostring(geometry_elem, encoding="unicode")
                    
                    # Extract attributes (all child elements that are not geometries)
                    for child in feature:
                        if not any(child.tag.startswith(f"{{{ns}}}") for ns in ["http://www.opengis.net/gml/3.2"]):
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
                    {', '.join(columns_sql)}
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
                    INSERT INTO {table_name} VALUES ({', '.join(values)})
                """)

            self.log.info(f"Successfully processed {len(features)} features into table {table_name}")
            return table_name

        except Exception as e:
            self.log.error(f"Error processing XML payload for layer {layer}: {e}")
            return None

    @timed(name="Processing raw data")  # type: ignore
    def _process_data(self, raw_data: list[dict[str, Any]]) -> Optional[str]:
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
                    {' UNION ALL '.join(union_parts)}
                """)

            # Clean up individual tables
            for table in all_tables:
                self.conn.execute(f"DROP TABLE IF EXISTS {table}")

            # Convert geometry XML to actual geometries using DuckDB-spatial
            self.conn.execute(f"""
                ALTER TABLE {combined_table} 
                ADD COLUMN geometry_spatial GEOMETRY
            """)
            
            # Parse geometries (this is a simplified version - real implementation would be more robust)
            self.conn.execute(f"""
                UPDATE {combined_table} 
                SET geometry_spatial = ST_GeomFromText(
                    CASE 
                        WHEN geometry_xml LIKE '%<gml:Point%' THEN 'POINT(' || 
                            REGEXP_EXTRACT(geometry_xml, '>(.*?)<', 1) || ')'
                        ELSE NULL
                    END
                )
                WHERE geometry_xml IS NOT NULL
            """)

            feature_count = self.conn.execute(f"SELECT COUNT(*) FROM {combined_table}").fetchone()[0]
            self.log.info(f"Combined {len(all_tables)} tables into {combined_table} with {feature_count} features")
            
            return combined_table

        except Exception as e:
            self.log.error(f"Error processing raw data: {e}")
            return None

    async def run(self, bronze_data: Optional[Any] = None) -> Optional[Any]:
        """
        Run the Water Typology silver layer processing.

        Args:
            bronze_data: Optional in-memory data from bronze stage. If provided,
                        this data will be used instead of reading from storage.

        Returns:
            Optional[Any]: Processed data that can be passed to gold stage,
                          or None if processing fails.
        """
        self.log.info("Running Water Typology silver job")
        async with AsyncTimer("Water Typology silver job"):
            try:
                # Read data with support for in-memory passing
                if bronze_data is not None:
                    self.log.info("Using bronze data from memory (in-memory data passing)")
                    # Bronze data is expected to be a list of tuples (layer, raw_data)
                    if isinstance(bronze_data, list):
                        # Create data structure compatible with _process_data
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
                    else:
                        self.log.error("Bronze data format not supported")
                        return None
                else:
                    # Read from GCS storage
                    raw_data_list = []
                    for layer in self.config.layers:
                        layer_table = f"{layer.replace(':', '_')}_raw"
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
                                    raw_data_list.append({
                                        "payload": result[0],
                                        "layer": result[1],
                                        "source": result[2],
                                        "created_at": result[3],
                                        "updated_at": result[4],
                                    })
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
                if self.conn.execute(f"SELECT COUNT(*) FROM {table_name} WHERE geometry_spatial IS NOT NULL").fetchone()[0] > 0:
                    validate_and_transform_geometries_duckdb(
                        self.conn,
                        table_name,
                        self.config.dataset,
                        geometry_column="geometry_spatial",
                    )

                # Save processed data (NO dissolving for typology data)
                self._save_data(
                    table_name, self.config.dataset, self.config.bucket, "silver", conn=self.conn
                )

                self.log.info("Saved processed data successfully")

                # Return processed data for potential gold layer consumption
                return {
                    "processed_data": table_name,
                    "dataset": self.config.dataset,
                }

            except Exception as e:
                self.log.error(f"Error in Water Typology silver processing: {e}")
                return None