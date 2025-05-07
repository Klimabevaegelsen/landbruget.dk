import os
import xml.etree.ElementTree as ET
from typing import Any, Dict, Optional

import geopandas as gpd
import pandas as pd
from shapely import MultiPolygon, Polygon, difference, unary_union, wkt

from unified_pipeline.common.base import BaseJobConfig, BaseSource
from unified_pipeline.util.gcs_util import GCSUtil
from unified_pipeline.util.geometry_validator import validate_and_transform_geometries


class BNBOStatusSilverConfig(BaseJobConfig):
    """
    Configuration for BNBO status data source.
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
    def __init__(self, config: BNBOStatusSilverConfig, gcs_util: GCSUtil):
        super().__init__(config, gcs_util)
        self.config = config

    def read_data(self, dataset: str) -> Optional[pd.DataFrame]:
        """Read data from the source"""
        self.log.info("Reading BNBO status data from bronze layer")

        # Get the GCS bucket
        bucket = self.gcs_util.get_gcs_client().bucket(self.config.bucket)

        # Define the path to the bronze data
        current_date = pd.Timestamp.now().strftime("%Y-%m-%d")
        bronze_path = f"bronze/{dataset}/{current_date}.parquet"
        blob = bucket.blob(bronze_path)

        if not blob.exists():
            self.log.error(f"Bronze data not found at {bronze_path}")
            return None

        # Download to temporary file
        temp_dir = f"/tmp/bronze/{dataset}"
        os.makedirs(temp_dir, exist_ok=True)
        temp_file = f"{temp_dir}/{current_date}.parquet"
        blob.download_to_filename(temp_file)

        # Load the parquet file
        raw_data = pd.read_parquet(temp_file)
        self.log.info(f"Loaded {len(raw_data):,} records from bronze layer")

        return raw_data

    def get_first_namespace(self, root: ET.Element) -> Optional[str]:
        """Get the namespace from the XML root element"""
        for elem in root.iter():
            if "}" in elem.tag:
                return elem.tag.split("}")[0].strip("{")
        return None

    def clean_value(self, value: Any) -> Optional[str]:
        """Clean string values"""
        if not isinstance(value, str):
            return str(value)
        value = value.strip()
        return value if value else None

    def _parse_geometry(self, geom_elem: ET.Element) -> Optional[Dict[str, Any]]:
        """Parse GML geometry into WKT and calculate area"""
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

    def _parse_feature(self, feature: ET.Element) -> Optional[Dict[str, Any]]:
        """Parse a single feature into a dictionary"""
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

    def _process_xml_data(self, raw_data: pd.DataFrame) -> Optional[gpd.GeoDataFrame]:
        """Process XML data from bronze layer"""
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

    def _create_dissolved_df(self, df: gpd.GeoDataFrame, dataset: str) -> gpd.GeoDataFrame:
        """Create a dissolved DataFrame"""
        try:
            # Convert to WGS84 before processing
            if df.crs.to_epsg() != 4326:
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
                    dissolved_geometries.extend(list(action_required_dissolved.geoms))
                    categories.extend(["Action Required"] * len(action_required_dissolved.geoms))
                else:
                    dissolved_geometries.append(action_required_dissolved)
                    categories.append("Action Required")

            if completed_dissolved is not None:
                if completed_dissolved.geom_type == "MultiPolygon":
                    dissolved_geometries.extend(list(completed_dissolved.geoms))
                    categories.extend(["Completed"] * len(completed_dissolved.geoms))
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

    def _save_data(self, df: gpd.GeoDataFrame, dataset: str) -> None:
        """Save processed data to GCS"""
        if df is None or df.empty:
            self.log.warning("No processed data to save")
            return

        self.log.info("Saving processed BNBO status data to GCS")
        bucket = self.gcs_util.get_gcs_client().bucket(self.config.bucket)

        temp_dir = f"/tmp/silver/{dataset}"
        os.makedirs(temp_dir, exist_ok=True)
        current_date = pd.Timestamp.now().strftime("%Y-%m-%d")
        temp_file = f"{temp_dir}/{current_date}.parquet"
        working_blob = bucket.blob(f"silver/{dataset}/{current_date}.parquet")

        df.to_parquet(temp_file)
        working_blob.upload_from_filename(temp_file)
        self.log.info(f"Uploaded working file has {len(df):,} features")

    async def run(self) -> None:
        """Run the job"""
        self.log.info("Running BNBO status silver job")
        raw_data = self.read_data(self.config.dataset)
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
        self._save_data(geo_df, self.config.dataset)
        self._save_data(dissolved_df, f"{self.config.dataset}_dissolved")
        self.log.info("Saved processed data successfully")
