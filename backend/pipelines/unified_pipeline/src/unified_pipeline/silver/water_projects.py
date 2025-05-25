import json
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any, Optional

import geopandas as gpd
import pandas as pd
from shapely import MultiPolygon, Polygon, unary_union, wkt
from shapely.validation import explain_validity

from unified_pipeline.common.base import BaseJobConfig, BaseSource
from unified_pipeline.util.gcs_util import GCSUtil
from unified_pipeline.util.geometry_validator import validate_and_transform_geometries
from unified_pipeline.util.timing import AsyncTimer, timed


class WaterProjectsSilverConfig(BaseJobConfig):
    dataset: str = "water_projects"
    bucket: str = "landbrugsdata-raw-data"
    storage_batch_size: int = 5000
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


class WaterProjectsSilver(BaseSource[WaterProjectsSilverConfig]):
    def __init__(self, config: WaterProjectsSilverConfig, gcs_util: GCSUtil):
        """
        Initialize the WaterProjectsSilver processor.

        Args:
            config (WaterProjectsSilverConfig): Configuration object containing settings 
                                                for the processor.
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

            geom_elem = feature.find(f"{namespace}the_geom") or feature.find(
                f"{namespace}wkb_geometry"
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
                        value = self.clean_value(elem.text)
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
                                    value = pd.to_datetime(value, dayfirst=True)
                            except (ValueError, TypeError):
                                self.log.warning(f"Failed to convert {key} value: {value}")
                                value = None
                            data[key] = value
            return data
        except Exception as e:
            self.log.error(f"Error parsing feature: {str(e)}", exc_info=True)
            return None

    @timed(name="Processing XML data")
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

    @timed(name="Processing JSON data")
    def _process_json_data(self, json_data: str, layer: str) -> list[dict]:
        features = []
        data = json.loads(json_data)
        for feature in data.get("features", []):
            try:
                attrs = feature.get("attributes", {})
                geom = feature.get("geometry", {})

                if "rings" not in geom:
                    continue

                # Convert geometry
                polygons = []
                for ring in geom["rings"]:
                    coords = [(x, y) for x, y in ring]
                    polygons.append(Polygon(coords))

                multi_poly = MultiPolygon(polygons) if len(polygons) > 1 else polygons[0]
                area_ha = multi_poly.area / 10000

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
                    "geometry": multi_poly.wkt,
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

            except Exception as e:
                self.log.error(f"Error processing feature: {str(e)}")
                continue

        return features

    @timed(name="Processing bronze data")
    def _process_data(self, raw_data: pd.DataFrame) -> Optional[gpd.GeoDataFrame]:
        if raw_data is None or raw_data.empty:
            self.log.warning("No raw data to process")
            return None

        self.log.info("Processing raw data into from bronze")
        features = []
        for index, row in raw_data.iterrows():
            try:
                data = row["payload"]
                layer = row["layer"]
                service_type = self.config.service_types.get(layer, "wfs")
                if service_type == "arcgis":
                    features.extend(self._process_json_data(data, layer))
                else:
                    features.extend(self._process_xml_data(data, layer))
            except Exception as e:
                self.log.error(f"Error processing row {index}: {e}")
                continue
        if not features:
            self.log.warning("No features extracted from raw data")
            return None
        self.log.info(f"Extracted {len(features):,} features from raw data")
        df = pd.DataFrame(features)
        geometries = [wkt.loads(f["geometry"]) for f in features]
        return gpd.GeoDataFrame(df, geometry=geometries, crs="EPSG:25832")

    @timed(name="Creating dissolved GeoDataFrame")
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

            dissolved = unary_union(df.geometry.values)
            self.log.info(f"Dissolved geometry type: {dissolved.geom_type}")

            if dissolved.geom_type == "MultiPolygon":
                self.log.info(f"Got MultiPolygon with {len(dissolved.geoms)} parts")  # type: ignore
                # Clean each geometry with buffer(0)
                cleaned_geoms = [geom.buffer(0) for geom in dissolved.geoms]  # type: ignore
                dissolved_gdf = gpd.GeoDataFrame(geometry=cleaned_geoms, crs="EPSG:4326")
            else:
                # Clean single geometry with buffer(0)
                cleaned = dissolved.buffer(0)
                dissolved_gdf = gpd.GeoDataFrame(geometry=[cleaned], crs="EPSG:4326")

            # Detailed geometry inspection after dissolve
            if dissolved.geom_type == "MultiPolygon":
                self.log.info(f"Post-dissolve parts: {len(dissolved.geoms)}")  # type: ignore
                self.log.info(f"Post-dissolve validity: {dissolved.is_valid}")
                self.log.info(f"Post-dissolve simplicity: {dissolved.is_simple}")

                # Inspect each part in detail
                for i, part in enumerate(dissolved.geoms):  # type: ignore
                    if not part.is_valid or not part.is_simple:
                        self.log.error(f"Invalid part {i}:")
                        self.log.error(f"Validity explanation: {explain_validity(part)}")
                        self.log.error(
                            f"Number of exterior points: {len(list(part.exterior.coords))}"
                        )
                        self.log.error(f"Number of interior rings: {len(part.interiors)}")
                        # Log coordinates of problematic part
                        self.log.error(f"Exterior coordinates: {list(part.exterior.coords)}")
                        for j, interior in enumerate(part.interiors):
                            self.log.error(
                                f"Interior ring {j} coordinates: {list(interior.coords)}"
                            )
            else:
                if not dissolved.is_valid or not dissolved.is_simple:
                    self.log.error("Invalid single polygon:")
                    self.log.error(f"Validity explanation: {explain_validity(dissolved)}")
                    self.log.error(
                        f"Number of exterior points: {len(list(dissolved.exterior.coords))}"  # type: ignore
                    )  # type: ignore
                    self.log.error(f"Number of interior rings: {len(dissolved.interiors)}")  # type: ignore
                    self.log.error(f"Exterior coordinates: {list(dissolved.exterior.coords)}")  # type: ignore
                    for j, interior in enumerate(dissolved.interiors):  # type: ignore
                        self.log.error(f"Interior ring {j} coordinates: {list(interior.coords)}")

            # Final validation will handle BigQuery compatibility
            dissolved_gdf = validate_and_transform_geometries(dissolved_gdf, f"{dataset}_dissolved")

            self.log.info(
                f"Dissolved {len(dissolved_gdf):,} features into "
                f"{len(dissolved_gdf.geometry):,} geometries"
            )
            return dissolved_gdf
        except Exception as e:
            self.log.error(f"Error during dissolve operation: {str(e)}")
            raise e

    async def run(self) -> None:
        self.log.info("Running Water Projects silver job for")
        async with AsyncTimer("Water Projects silver job"):
            raw_data = self._read_bronze_data(self.config.dataset, self.config.bucket)
            if raw_data is None:
                self.log.error("Failed to read raw data")
                return
            self.log.info("Read raw data successfully")
            geo_df = self._process_data(raw_data)
            if geo_df is None:
                self.log.error("Failed to process raw data")
                return
            self.log.info("Processed raw data successfully")
            dissolved_df = self._create_dissolved_df(geo_df, self.config.dataset)
            self._save_data(geo_df, self.config.dataset, self.config.bucket)
            self._save_data(dissolved_df, f"{self.config.dataset}_dissolved", self.config.bucket)
            self.log.info("Saved processed data successfully")
