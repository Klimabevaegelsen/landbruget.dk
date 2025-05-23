"""
Silver layer processing for Wetlands data.

This module handles the transformation of raw wetlands data from the bronze layer.
It parses XML data, processes geometries, and creates both original and dissolved
versions of the wetlands dataset for analysis and visualization.

The module contains:
- WetlandsSilverConfig: Configuration class for the processing
- WetlandsSilver: Implementation class for transforming and analyzing data

The processing includes XML parsing, geometry validation, spatial operations for
merging adjacent polygons, and comprehensive logging of geometry statistics.
"""

import xml.etree.ElementTree as ET
from collections import Counter
from typing import Any, Optional

import geopandas as gpd
import pandas as pd
from shapely import Polygon, unary_union

from unified_pipeline.common.base import BaseJobConfig, BaseSource
from unified_pipeline.util.gcs_util import GCSUtil
from unified_pipeline.util.geometry_validator import validate_and_transform_geometries
from unified_pipeline.util.timing import AsyncTimer, timed


class WetlandsSilverConfig(BaseJobConfig):
    """
    Configuration for the Wetlands Silver processing.

    This class defines all configuration parameters needed for transforming wetlands
    data from the bronze layer to the silver layer, including dataset names,
    storage settings, and XML namespaces.

    Attributes:
        dataset (str): Name of the dataset in storage
        bucket (str): GCS bucket name for data storage
        storage_batch_size (int): Batch size for storage operations
        namespaces (dict[str, str]): XML namespaces used in the wetlands data
        gml_ns (str): GML namespace prefix for XML parsing
    """

    dataset: str = "wetlands"
    bucket: str = "landbrugsdata-raw-data"
    storage_batch_size: int = 5000
    namespaces: dict[str, str] = {
        "wfs": "http://www.opengis.net/wfs/2.0",
        "natur": "http://wfs2-miljoegis.mim.dk/natur",
        "gml": "http://www.opengis.net/gml/3.2",
    }
    gml_ns: str = "{http://www.opengis.net/gml/3.2}"  # This is not a f-string.


class WetlandsSilver(BaseSource[WetlandsSilverConfig]):
    """
    Silver layer processor for wetlands data.

    This class handles the processing of wetlands data from the bronze layer
    to the silver layer. It reads, transforms, and saves the data according to
    the data pipeline architecture. The class handles XML processing, geometry
    operations, and data storage in GCS.

    Key functionalities include:
    1. Parsing XML data to extract features and geometries
    2. Creating standard and dissolved (merged) versions of the dataset
    3. Analyzing and logging geometry statistics for data quality assessment
    """

    def __init__(self, config: WetlandsSilverConfig, gcs_util: GCSUtil):
        """
        Initialize the WetlandsSilver processor.

        Args:
            config (WetlandsSilverConfig): Configuration object containing settings for the processor.
            gcs_util (GCSUtil): Utility for interacting with Google Cloud Storage.
        """
        super().__init__(config, gcs_util)

    def analyze_geometry(self, geom):
        """
        Analyze a geometry and extract key metrics.

        This method calculates width, height, area, grid alignment, and vertex count
        for a geometry. It is used to generate statistics about the wetlands dataset.

        Args:
            geom: A shapely geometry object

        Returns:
            dict: Dictionary containing geometry metrics:
                - width: Width of the geometry's bounding box
                - height: Height of the geometry's bounding box
                - area: Area of the bounding box
                - grid_aligned: Whether the geometry is aligned to a 10-unit grid
                - vertices: Number of vertices in the geometry
        """
        bounds = geom.bounds
        width = bounds[2] - bounds[0]
        height = bounds[3] - bounds[1]
        area = width * height

        # Check grid alignment
        vertices = list(geom.exterior.coords)
        is_grid_aligned = all(
            abs(round(coord / 10) * 10 - coord) < 0.01 for vertex in vertices for coord in vertex
        )

        return {
            "width": width,
            "height": height,
            "area": area,
            "grid_aligned": is_grid_aligned,
            "vertices": len(vertices),
        }

    def log_geometry_statistics(self, gdf):
        """
        Analyze and log statistics about the geometries in a GeoDataFrame.

        This method calculates and logs various statistics about the geometries,
        including total features, dimensions, grid alignment, and area coverage.
        The statistics help understand the nature and quality of the dataset.

        Args:
            gdf (gpd.GeoDataFrame): The GeoDataFrame containing geometries to analyze

        Returns:
            None: Results are logged directly
        """
        stats = []
        for geom in gdf.geometry:
            stats.append(self.analyze_geometry(geom))

        # Convert to DataFrame for easy analysis
        stats_df = pd.DataFrame(stats)

        # Unique dimensions
        dimensions = Counter(zip(stats_df["width"], stats_df["height"]))

        self.log.info("Geometry Statistics:")
        self.log.info(f"Total features: {len(stats_df)}")
        self.log.info("\nUnique dimensions (width x height, count):")
        for (width, height), count in dimensions.most_common():
            self.log.info(f"{width:.1f}m x {height:.1f}m: {count} features")

        self.log.info(f"\nNon-grid-aligned features: {sum(~stats_df['grid_aligned'])}")
        self.log.info(f"Average vertices per feature: {stats_df['vertices'].mean():.1f}")
        self.log.info(f"Total area covered: {stats_df['area'].sum() / 1_000_000:.2f} km²")

    def _parse_geometry(self, geom_elem):
        """
        Parse a GML geometry element into a Shapely polygon.

        This method extracts coordinates from a GML posList element and
        creates a Shapely polygon. It also ensures the polygon is valid.

        Args:
            geom_elem (ET.Element): The XML element containing the geometry

        Returns:
            Optional[Polygon]: A Shapely polygon if successful, None otherwise

        Raises:
            None: Exceptions are caught and logged
        """
        try:
            coords = geom_elem.find(".//gml:posList", self.config.namespaces).text.split()
            coords = [(float(coords[i]), float(coords[i + 1])) for i in range(0, len(coords), 2)]
            poly = Polygon(coords)

            # Ensure the polygon is valid
            if not poly.is_valid:
                poly = poly.buffer(0)
            return poly
        except Exception as e:
            self.log.error(f"Error parsing geometry: {str(e)}")
            return None

    def _get_attribute(self, element: ET.Element, tag: str) -> Optional[str]:
        """
        Get an attribute value from an XML element.

        This method finds a child element with the specified tag and
        returns its text value if found.

        Args:
            element (ET.Element): The parent XML element
            tag (str): The tag name of the child element to find

        Returns:
            Optional[str]: The text value of the child element if found, None otherwise
        """
        attr = element.find(tag, self.config.namespaces)
        return attr.text if attr is not None else None

    def _parse_feature(self, feature: ET.Element) -> Optional[dict[str, Any]]:
        """
        Parse an XML feature element into a GeoJSON-like feature dictionary.

        This method extracts geometry and attributes from an XML feature element,
        and constructs a GeoJSON-like dictionary with properties.

        Args:
            feature (ET.Element): The XML element containing the feature

        Returns:
            Optional[dict[str, Any]]: A GeoJSON-like feature dictionary if successful,
                                     None if required attributes are missing

        Raises:
            None: Exceptions are caught and logged
        """
        try:
            geom = self._parse_geometry(feature.find(".//gml:Polygon", self.config.namespaces))
            if not geom:
                return None

            gridcode = self._get_attribute(feature, "natur:gridcode")
            if gridcode is None:
                self.log.error("Missing gridcode in feature")
                return None
            gridcode = int(gridcode)

            toerv_pct = self._get_attribute(feature, "natur:toerv_pct")
            if toerv_pct is None:
                self.log.error("Missing toerv_pct in feature")
                return None

            return {
                "type": "Feature",
                "geometry": geom.__geo_interface__,
                "properties": {
                    "id": feature.get(f"{self.config.gml_ns}id"),
                    "gridcode": gridcode,
                    "toerv_pct": toerv_pct,
                },
            }
        except Exception as e:
            self.log.error(f"Error parsing feature: {str(e)}")
            return None

    @timed(name="Processing XML data")
    def _process_xml_data(self, raw_data: pd.DataFrame) -> Optional[gpd.GeoDataFrame]:
        """
        Process raw XML data into a GeoDataFrame.

        This method parses XML data containing wetland features and converts it to
        a GeoDataFrame with attributes and geometries. It processes each row in the
        input DataFrame independently and combines the results.

        Args:
            raw_data (pd.DataFrame): DataFrame containing raw XML data in the 'payload' column

        Returns:
            Optional[gpd.GeoDataFrame]: A GeoDataFrame containing wetland features,
                                        or None if no data is available

        Raises:
            Exception: If there are errors during processing that cannot be handled
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

                for member in root.findall(
                    ".//natur:kulstof2022", namespaces=self.config.namespaces
                ):
                    parsed = self._parse_feature(member)
                    if parsed and parsed.get("geometry"):
                        features.append(parsed)

                    if len(features) % 100000 == 0:
                        self.log.info(f"Processed {len(features):,} features")

            except Exception as e:
                self.log.error(f"Error processing row {index}: {str(e)}", exc_info=True)
                raise e

        self.log.info(f"Parsed {len(features):,} features from XML data")

        df = pd.DataFrame([f["properties"] for f in features])
        geometries = [Polygon(f["geometry"]["coordinates"][0]) for f in features]

        return gpd.GeoDataFrame(df, geometry=geometries, crs="EPSG:25832")

    @timed(name="Dissolving geometries")
    def _create_dissolved_df(self, df: gpd.GeoDataFrame, dataset: str) -> gpd.GeoDataFrame:
        """
        Create a dissolved (merged) version of the wetlands dataset.

        This method merges adjacent wetland polygons to create a more aggregated
        dataset for analysis. It uses spatial indexing to efficiently find adjacent
        polygons, and then merges them using unary_union.

        Args:
            df (gpd.GeoDataFrame): The original GeoDataFrame with wetland features
            dataset (str): Name of the dataset, used for logging and transformation

        Returns:
            gpd.GeoDataFrame: A new GeoDataFrame with merged wetland polygons and wetland IDs

        Raises:
            Exception: If there are errors during the dissolve operation
        """
        try:
            self.log_geometry_statistics(df)
            self.log.info(f"Starting merge of {len(df):,} features...")

            # Create spatial index for efficient neighbor finding
            self.log.info("Creating spatial index...")
            df["idx"] = range(len(df))
            spatial_index = df.sindex

            # Function to check if two polygons share an edge
            def shares_edge(geom1, geom2):
                intersection = geom1.intersection(geom2)
                return (
                    intersection.geom_type == "LineString" and intersection.length >= 10
                )  # At least one grid cell length

            # Find and merge adjacent polygons
            self.log.info("Finding and merging adjacent polygons...")
            merged = set()  # Keep track of merged polygons
            merged_polygons = []

            for idx, row in df.iterrows():
                if idx in merged:
                    continue

                # Find potential neighbors using spatial index
                bounds = row["geometry"].bounds
                possible_matches_idx = list(spatial_index.intersection(bounds))
                possible_matches = df.iloc[possible_matches_idx]  # type: ignore

                # Start with current polygon
                current_group = [row["geometry"]]
                merged.add(idx)

                # Check each potential neighbor
                for match_idx, match_row in possible_matches.iterrows():
                    if match_idx != idx and match_idx not in merged:
                        if shares_edge(row["geometry"], match_row["geometry"]):
                            current_group.append(match_row["geometry"])
                            merged.add(match_idx)

                # Merge the group if we found any adjacent polygons
                if len(current_group) > 1:
                    merged_poly = unary_union(current_group)
                else:
                    merged_poly = current_group[0]

                merged_polygons.append(merged_poly)

                if len(merged_polygons) % 10000 == 0:
                    self.log.info(f"Processed {len(merged_polygons):,} groups")

            # Create new GeoDataFrame with merged polygons
            dissolved_gdf = gpd.GeoDataFrame(geometry=merged_polygons, crs=df.crs)

            dissolved_gdf["wetland_id"] = range(1, len(dissolved_gdf) + 1)

            self.log.info(f"Created {len(dissolved_gdf):,} merged polygons")
            self.log.info(f"Reduced from {len(df):,} grid cells")

            self.log.info("Analyzing dissolved geometries:")
            self.log_geometry_statistics(dissolved_gdf)

            # Transform and validate final geometries
            self.log.info("Transforming geometries to BigQuery-compatible CRS...")
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
        Run the wetlands silver layer processing pipeline.

        This method orchestrates the entire data processing workflow:
        1. Reads raw data from the bronze layer
        2. Processes XML data into a GeoDataFrame
        3. Creates a dissolved version with merged adjacent polygons
        4. Saves both the original and dissolved datasets to GCS

        The method handles error conditions at each step and logs progress.

        Returns:
            None

        Note:
            This is the main entry point for the silver layer processing of wetlands data.
        """
        self.log.info("Running Wetlands silver job")
        async with AsyncTimer("Wetlands silver job"):
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
