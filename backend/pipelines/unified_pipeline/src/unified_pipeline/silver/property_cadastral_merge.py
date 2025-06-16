import logging
import os
from datetime import datetime
from typing import Optional

import geopandas as gpd
import pandas as pd
from dotenv import load_dotenv
from pydantic import ConfigDict

from unified_pipeline.common.base import BaseJobConfig, BaseSource
from unified_pipeline.common.geometry_validator import validate_and_transform_geometries
from unified_pipeline.util.gcs_util import GCSUtil

logger = logging.getLogger(__name__)


class PropertyCadastralMergeConfig(BaseJobConfig):
    """Configuration for the Property-Cadastral merge pipeline."""

    name: str = "Property Owners Cadastral Merge"
    dataset: str = "property_cadastral_merged"
    type: str = "merge"
    description: str = "Merge property owners data with cadastral parcels"
    frequency: str = "weekly"
    bucket: str = os.getenv("GCS_BUCKET")

    # Data source paths
    property_owners_silver_path: str = "silver/property_owners/"
    cadastral_silver_path: str = "silver/cadastral/"

    # Merge configuration
    spatial_join_method: str = "intersects"  # Options: intersects, within, contains
    buffer_distance_meters: float = 0.0  # Buffer for spatial join tolerance
    min_overlap_threshold: float = 0.1  # Minimum overlap ratio for valid matches

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)
    load_dotenv()
    save_local: bool = os.getenv("SAVE_LOCAL", False)


class PropertyCadastralMerge(BaseSource[PropertyCadastralMergeConfig]):
    """Merge property owners data with cadastral parcels."""

    def __init__(self, config: PropertyCadastralMergeConfig, gcs_util: GCSUtil) -> None:
        super().__init__(config, gcs_util)

    def _load_property_owners_data(self) -> Optional[gpd.GeoDataFrame]:
        """Load the latest property owners data from silver layer."""
        try:
            self.log.info("Loading property owners data from silver layer...")

            # Get the latest property owners file
            property_files = self.gcs_util.list_files(
                bucket_name=self.config.bucket, prefix=self.config.property_owners_silver_path
            )

            if not property_files:
                self.log.error("No property owners files found in silver layer")
                return None

            # Find the most recent file
            latest_file = max(property_files, key=lambda x: x.time_created)
            self.log.info(f"Loading latest property owners file: {latest_file.name}")

            # Download and load the parquet file
            temp_path = f"/tmp/property_owners_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            self.gcs_util.download_file(
                bucket_name=self.config.bucket,
                source_blob_name=latest_file.name,
                destination_file_name=temp_path,
            )

            # Load as DataFrame first
            df = pd.read_parquet(temp_path)
            self.log.info(f"Loaded property owners data: {len(df)} records")

            # Convert geometry from JSON to shapely geometry
            if "geometry_json" in df.columns:
                import json

                def parse_geometry(geom_json):
                    if pd.isna(geom_json) or not geom_json:
                        return None
                    try:
                        geom_dict = (
                            json.loads(geom_json) if isinstance(geom_json, str) else geom_json
                        )
                        from shapely.geometry import shape

                        return shape(geom_dict)
                    except Exception as e:
                        logger.warning(f"Failed to parse geometry: {e}")
                        return None

                df["geometry"] = df["geometry_json"].apply(parse_geometry)
                df = df.drop("geometry_json", axis=1)

                # Filter out records without valid geometry
                df = df[df["geometry"].notna()]
                self.log.info(f"Property owners with valid geometry: {len(df)} records")

                # Create GeoDataFrame
                gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
            else:
                self.log.error("No geometry information found in property owners data")
                return None

            # Clean up temp file
            os.unlink(temp_path)

            return gdf

        except Exception as e:
            self.log.error(f"Failed to load property owners data: {e}")
            return None

    def _load_cadastral_data(self) -> Optional[gpd.GeoDataFrame]:
        """Load the latest cadastral data from silver layer."""
        try:
            self.log.info("Loading cadastral data from silver layer...")

            # Get the latest cadastral file
            cadastral_files = self.gcs_util.list_files(
                bucket_name=self.config.bucket, prefix=self.config.cadastral_silver_path
            )

            if not cadastral_files:
                self.log.error("No cadastral files found in silver layer")
                return None

            # Find the most recent file
            latest_file = max(cadastral_files, key=lambda x: x.time_created)
            self.log.info(f"Loading latest cadastral file: {latest_file.name}")

            # Download and load the parquet file
            temp_path = f"/tmp/cadastral_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            self.gcs_util.download_file(
                bucket_name=self.config.bucket,
                source_blob_name=latest_file.name,
                destination_file_name=temp_path,
            )

            # Load as GeoDataFrame
            gdf = gpd.read_parquet(temp_path)
            self.log.info(f"Loaded cadastral data: {len(gdf)} records")

            # Ensure consistent CRS (EPSG:4326)
            if gdf.crs != "EPSG:4326":
                self.log.info(f"Converting cadastral CRS from {gdf.crs} to EPSG:4326")
                gdf = gdf.to_crs("EPSG:4326")

            # Clean up temp file
            os.unlink(temp_path)

            return gdf

        except Exception as e:
            self.log.error(f"Failed to load cadastral data: {e}")
            return None

    def _perform_spatial_merge(
        self, property_gdf: gpd.GeoDataFrame, cadastral_gdf: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
        """Perform spatial join between property owners and cadastral data."""
        self.log.info("Performing spatial merge...")

        # Ensure both datasets have the same CRS
        if property_gdf.crs != cadastral_gdf.crs:
            self.log.info(
                f"Converting property owners CRS from {property_gdf.crs} to {cadastral_gdf.crs}"
            )
            property_gdf = property_gdf.to_crs(cadastral_gdf.crs)

        # Apply buffer if specified (for tolerance in spatial matching)
        if self.config.buffer_distance_meters > 0:
            self.log.info(
                f"Applying {self.config.buffer_distance_meters}m buffer for spatial matching"
            )
            # Convert to a projected CRS for accurate buffering (using UTM Zone 32N for Denmark)
            property_buffered = property_gdf.to_crs("EPSG:25832")
            property_buffered["geometry"] = property_buffered.geometry.buffer(
                self.config.buffer_distance_meters
            )
            property_buffered = property_buffered.to_crs(cadastral_gdf.crs)
        else:
            property_buffered = property_gdf.copy()

        # Perform spatial join
        self.log.info(f"Performing spatial join using method: {self.config.spatial_join_method}")

        # Add unique identifiers to avoid column conflicts
        property_buffered = property_buffered.copy()
        property_buffered["property_id"] = range(len(property_buffered))

        cadastral_gdf = cadastral_gdf.copy()
        cadastral_gdf["cadastral_id"] = range(len(cadastral_gdf))

        # Perform the spatial join
        merged_gdf = gpd.sjoin(
            property_buffered,
            cadastral_gdf,
            how="left",
            predicate=self.config.spatial_join_method,
        )

        self.log.info(f"Spatial join completed. Result: {len(merged_gdf)} records")

        # Filter out matches with insufficient overlap if threshold is set
        if self.config.min_overlap_threshold > 0:
            self.log.info(
                f"Applying minimum overlap threshold: {self.config.min_overlap_threshold}"
            )
            merged_gdf = self._filter_by_overlap(merged_gdf, property_gdf, cadastral_gdf)

        return merged_gdf

    def _filter_by_overlap(
        self,
        merged_gdf: gpd.GeoDataFrame,
        property_gdf: gpd.GeoDataFrame,
        cadastral_gdf: gpd.GeoDataFrame,
    ) -> gpd.GeoDataFrame:
        """Filter merged results by minimum overlap threshold."""

        # This is computationally expensive, so we'll implement a simplified version
        # In a production environment, you might want to optimize this further

        valid_matches = []

        for idx, row in merged_gdf.iterrows():
            if pd.isna(row["cadastral_id"]):
                # No cadastral match found
                valid_matches.append(True)
                continue

            try:
                property_geom = property_gdf.loc[
                    property_gdf["property_id"] == row["property_id"], "geometry"
                ].iloc[0]
                cadastral_geom = cadastral_gdf.loc[
                    cadastral_gdf["cadastral_id"] == row["cadastral_id"], "geometry"
                ].iloc[0]

                # Calculate overlap ratio
                intersection = property_geom.intersection(cadastral_geom)
                overlap_ratio = (
                    intersection.area / property_geom.area if property_geom.area > 0 else 0
                )

                valid_matches.append(overlap_ratio >= self.config.min_overlap_threshold)

            except Exception as e:
                self.log.warning(f"Error calculating overlap for record {idx}: {e}")
                valid_matches.append(False)

        filtered_gdf = merged_gdf[valid_matches].copy()
        self.log.info(f"Filtered by overlap threshold: {len(filtered_gdf)} records remain")

        return filtered_gdf

    def _clean_and_standardize(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Clean and standardize the merged dataset."""
        self.log.info("Cleaning and standardizing merged dataset...")

        # Handle duplicate columns from spatial join
        # When sjoin finds duplicates, it adds _left and _right suffixes
        rename_mapping = {}
        columns_to_drop = []

        for col in gdf.columns:
            if col.endswith("_left"):
                # Keep the _left version (from property data) with original name
                original_name = col.replace("_left", "")
                if f"{original_name}_right" in gdf.columns:
                    # Drop the _right version and rename _left to original
                    columns_to_drop.append(f"{original_name}_right")
                    rename_mapping[col] = original_name
            elif col.endswith("_right"):
                # This is cadastral data - prefix with cadastral_
                if col.replace("_right", "_left") not in gdf.columns:
                    # No corresponding _left, so this is unique to cadastral
                    original_name = col.replace("_right", "")
                    rename_mapping[col] = f"cadastral_{original_name}"

        if columns_to_drop:
            gdf = gdf.drop(columns=columns_to_drop)
        if rename_mapping:
            gdf = gdf.rename(columns=rename_mapping)

        # Add merge metadata
        gdf["merge_timestamp"] = datetime.utcnow()
        gdf["merge_method"] = self.config.spatial_join_method
        gdf["has_cadastral_match"] = ~gdf["cadastral_id"].isna()

        # Calculate match quality metrics
        total_properties = len(gdf)
        matched_properties = len(gdf[gdf["has_cadastral_match"]])
        match_rate = (matched_properties / total_properties) * 100 if total_properties > 0 else 0

        self.log.info("Merge statistics:")
        self.log.info(f"  Total properties: {total_properties}")
        self.log.info(f"  Matched with cadastral: {matched_properties}")
        self.log.info(f"  Match rate: {match_rate:.1f}%")

        return gdf

    def _validate_and_transform(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Validate and transform the merged GeoDataFrame."""
        return validate_and_transform_geometries(gdf, self.config.dataset)

    async def run(self):
        """
        Run the complete property-cadastral merge job.

        This orchestrates the entire process:
        1. Load property owners data from silver layer
        2. Load cadastral data from silver layer
        3. Perform spatial merge
        4. Clean and standardize the result
        5. Save merged data to GCS
        """
        self.log.info("Running Property-Cadastral merge job")

        try:
            # Load input datasets
            property_gdf = self._load_property_owners_data()
            if property_gdf is None:
                self.log.error("Failed to load property owners data")
                return

            cadastral_gdf = self._load_cadastral_data()
            if cadastral_gdf is None:
                self.log.error("Failed to load cadastral data")
                return

            # Perform spatial merge
            merged_gdf = self._perform_spatial_merge(property_gdf, cadastral_gdf)

            # Clean and standardize
            cleaned_gdf = self._clean_and_standardize(merged_gdf)

            # Validate and transform
            final_gdf = self._validate_and_transform(cleaned_gdf)

            # Save the result
            self._save_data(final_gdf, self.config.dataset, self.config.bucket)

            self.log.info("Property-Cadastral merge job completed successfully")

        except Exception as e:
            self.log.error(f"Property-Cadastral merge job failed: {e}")
            raise
