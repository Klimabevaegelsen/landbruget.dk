import xml.etree.ElementTree as ET
from typing import Any, Optional

import geopandas as gpd
import pandas as pd
from shapely import MultiPolygon, Polygon, difference, unary_union, wkt

from unified_pipeline.common.base import BaseJobConfig, BaseSource
from unified_pipeline.util.gcs_util import GCSUtil
from unified_pipeline.util.geometry_validator import validate_and_transform_geometries
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


class BNBOStatusSilver(BaseSource[BNBOStatusSilverConfig]):
    """
    Silver layer processor for BNBO status data.

    This class handles the processing of BNBO status data from the bronze layer
    to the silver layer. It reads, transforms, and saves the data according to
    the data pipeline architecture. The class handles XML processing, geometry
    operations, and data storage in GCS.

    Attributes:
        config (BNBOStatusSilverConfig): Configuration object containing settings for the processor.
    """

    def __init__(self, config: BNBOStatusSilverConfig, gcs_util: GCSUtil):
        """
        Initialize the BNBOStatusSilver processor.

        Args:
            config (BNBOStatusSilverConfig): Configuration object for the processor.
            gcs_util (GCSUtil): Utility for interacting with Google Cloud Storage.
        """
        super().__init__(config, gcs_util)

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
                    coords = [(pos[i], pos[i + 1]) for i in range(0, len(pos), 2)]
                    if len(coords) >= 4:
                        polygons.append(Polygon(coords))
                except Exception as e:
                    self.log.error(f"Failed to parse coordinates: {str(e)}")
                    continue

            if not polygons:
                return None

            geom = MultiPolygon(polygons) if len(polygons) > 1 else polygons[0]
            area_ha = geom.area / 10000  # Convert square meters to hectares

            return {"wkt": geom.wkt, "area_ha": area_ha}

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
    def _process_xml_data(self, raw_data: pd.DataFrame) -> Optional[gpd.GeoDataFrame]:
        """
        Process XML data from the bronze layer into a GeoDataFrame.

        This method iterates through all XML data in the input DataFrame, parses
        features using _parse_feature, and constructs a GeoDataFrame with the
        extracted geometries and attributes.

        Args:
            raw_data (pd.DataFrame): DataFrame containing XML data in a 'payload' column.

        Returns:
            Optional[gpd.GeoDataFrame]: A GeoDataFrame containing the processed features,
                                       or None if processing fails.

        Raises:
            Exception: If there are issues processing the XML data.
        """
        if raw_data is None or raw_data.empty:
            self.log.warning("No raw data to process")
            return None

        self.log.info("Processing XML data from bronze layer")

        features = []
        for index, row in raw_data.iterrows():
            try:
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
        df = pd.DataFrame(features)
        geometries = [wkt.loads(f["geometry"]) for f in features]
        return gpd.GeoDataFrame(df, geometry=geometries, crs="EPSG:25832")

    @timed(name="Creating dissolved GeoDataFrame")  # type: ignore
    def _create_dissolved_df(self, df: gpd.GeoDataFrame, dataset: str) -> gpd.GeoDataFrame:
        """
        Create a dissolved GeoDataFrame by merging geometries by status category.

        This method groups geometries by their status category ("Action Required" or
        "Completed"), dissolves them into unified geometries, and handles overlapping
        areas by giving priority to "Action Required" areas.

        Args:
            df (gpd.GeoDataFrame): The input GeoDataFrame containing features with geometries
                                  and status_category attributes.
            dataset (str): The name of the dataset, used for logging and validation.

        Returns:
            gpd.GeoDataFrame: A new GeoDataFrame containing the dissolved geometries.

        Raises:
            Exception: If there are issues during the dissolve operation.
        """
        try:
            # Convert to WGS84 before processing
            if df.crs and df.crs.to_epsg() != 4326:
                df = df.to_crs("EPSG:4326")

            # Split into two categories
            action_required = df[df["status_category"] == "Action Required"]
            completed = df[df["status_category"] == "Completed"]

            # Dissolve each category
            action_required_dissolved = None
            if not action_required.empty:
                action_required_dissolved = unary_union(action_required.geometry.values).buffer(0)

            completed_dissolved = None
            if not completed.empty:
                completed_dissolved = unary_union(completed.geometry.values).buffer(0)

            # Handle overlaps - remove completed areas that overlap with action required
            if action_required_dissolved is not None and completed_dissolved is not None:
                completed_dissolved = difference(completed_dissolved, action_required_dissolved)

            # Create final dissolved GeoDataFrame
            dissolved_geometries = []
            categories = []

            if action_required_dissolved is not None:
                if action_required_dissolved.geom_type == "MultiPolygon":
                    dissolved_geometries.extend(list(action_required_dissolved.geoms))  # type: ignore
                    categories.extend(["Action Required"] * len(action_required_dissolved.geoms))  # type: ignore
                else:
                    dissolved_geometries.append(action_required_dissolved)
                    categories.append("Action Required")

            if completed_dissolved is not None:
                if completed_dissolved.geom_type == "MultiPolygon":
                    dissolved_geometries.extend(list(completed_dissolved.geoms))  # type: ignore
                    categories.extend(["Completed"] * len(completed_dissolved.geoms))  # type: ignore
                else:
                    dissolved_geometries.append(completed_dissolved)
                    categories.append("Completed")

            dissolved_gdf = gpd.GeoDataFrame(
                {"status_category": categories, "geometry": dissolved_geometries}, crs="EPSG:4326"
            )

            # Final validation
            dissolved_gdf = validate_and_transform_geometries(
                dissolved_gdf, f"silver.{dataset}_dissolved"
            )
            self.log.info(
                f"Dissolved {len(dissolved_gdf):,} features into "
                f"{len(dissolved_gdf.geometry):,} geometries"
            )
            return dissolved_gdf
        except Exception as e:
            self.log.error(f"Error during dissolve operation: {str(e)}")
            raise e

    async def run(self) -> None:
        """
        Run the complete BNBO status silver layer processing job.

        This is the main entry point that orchestrates the entire process:
        1. Reads data from the bronze layer
        2. Processes XML data into a GeoDataFrame
        3. Creates a dissolved version of the GeoDataFrame
        4. Saves both the original and dissolved data to GCS

        Returns:
            None

        Raises:
            Exception: If there are issues at any step in the process.
        """
        self.log.info("Running BNBO status silver job")
        async with AsyncTimer("Running Water Projects silver job for"):
            raw_data = self._read_bronze_data(self.config.dataset, self.config.bucket)
            if raw_data is None:
                self.log.error("Failed to read raw data")
                return
            self.log.info("Read raw data successfully")
            geo_df = self._process_xml_data(raw_data)
            if geo_df is None:
                self.log.error("Failed to process raw data")
                return
            self.log.info("Processed raw data successfully")
            dissolved_df = self._create_dissolved_df(geo_df, self.config.dataset)
            self._save_data(geo_df, self.config.dataset, self.config.bucket)
            self._save_data(dissolved_df, f"{self.config.dataset}_dissolved", self.config.bucket)
            self.log.info("Saved processed data successfully")
