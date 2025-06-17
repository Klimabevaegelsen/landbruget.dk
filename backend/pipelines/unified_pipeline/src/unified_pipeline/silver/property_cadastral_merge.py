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
    description: str = "Merge property owners data with cadastral parcels using BFE numbers"
    frequency: str = "weekly"
    bucket: str = os.getenv("GCS_BUCKET")

    # Data source paths
    property_owners_silver_path: str = "silver/property_owners/"
    cadastral_silver_path: str = "silver/cadastral/"

    # Merge configuration
    join_method: str = "inner"  # Use inner join to ensure complete records only
    validate_bfe_numbers: bool = True  # Validate BFE number format and consistency
    include_merge_metadata: bool = True  # Add metadata about the merge process

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)
    load_dotenv()
    save_local: bool = os.getenv("SAVE_LOCAL", False)


class PropertyCadastralMerge(BaseSource[PropertyCadastralMergeConfig]):
    """Merge property owners data with cadastral parcels."""

    def __init__(self, config: PropertyCadastralMergeConfig, gcs_util: GCSUtil) -> None:
        super().__init__(config, gcs_util)

    def _load_property_owners_data(self) -> Optional[pd.DataFrame]:
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

            # Load as DataFrame
            df = pd.read_parquet(temp_path)
            self.log.info(f"Loaded property owners data: {len(df)} records")

            # Check for BFE number field
            if "bestemtFastEjendomBFENr" not in df.columns:
                self.log.error(
                    "BFE number field 'bestemtFastEjendomBFENr' not found in property owners data"
                )
                return None

            # Validate BFE numbers if requested
            if self.config.validate_bfe_numbers:
                original_count = len(df)
                df = df[df["bestemtFastEjendomBFENr"].notna()]
                df = df[df["bestemtFastEjendomBFENr"] > 0]  # BFE numbers should be positive
                self.log.info(f"Valid BFE numbers: {len(df)} of {original_count} records")

            # Clean up temp file
            os.unlink(temp_path)

            return df

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

    def _perform_bfe_merge(
        self, property_df: pd.DataFrame, cadastral_gdf: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
        """Perform BFE-based join between property owners and cadastral data."""
        self.log.info("Performing BFE-based merge...")

        # Check that cadastral data has bfe_number field
        if "bfe_number" not in cadastral_gdf.columns:
            self.log.error("BFE number field 'bfe_number' not found in cadastral data")
            raise ValueError("Missing bfe_number field in cadastral data")

        # Log initial statistics
        self.log.info(f"Property owners records: {len(property_df)}")
        self.log.info(f"Cadastral parcels: {len(cadastral_gdf)}")

        # Get unique BFE numbers for analysis
        property_bfe_count = property_df["bestemtFastEjendomBFENr"].nunique()
        cadastral_bfe_count = cadastral_gdf["bfe_number"].nunique()
        self.log.info(
            f"Unique BFE numbers - Properties: {property_bfe_count}, Cadastral: {cadastral_bfe_count}"
        )

        # Perform the merge based on BFE numbers
        self.log.info(f"Performing {self.config.join_method} join on BFE numbers")

        merged_df = pd.merge(
            property_df,
            cadastral_gdf,
            left_on="bestemtFastEjendomBFENr",
            right_on="bfe_number",
            how=self.config.join_method,
            suffixes=("_property", "_cadastral"),
        )

        self.log.info(f"BFE join completed. Result: {len(merged_df)} records")

        # Convert back to GeoDataFrame if we have geometry from cadastral data
        if "geometry" in merged_df.columns:
            merged_gdf = gpd.GeoDataFrame(merged_df, geometry="geometry", crs=cadastral_gdf.crs)
        else:
            self.log.warning("No geometry column found in merged result")
            # Create a basic GeoDataFrame with empty geometry
            merged_gdf = gpd.GeoDataFrame(merged_df, geometry=None)

        return merged_gdf

    def _validate_bfe_merge_quality(
        self,
        merged_gdf: gpd.GeoDataFrame,
        property_df: pd.DataFrame,
        cadastral_gdf: gpd.GeoDataFrame,
    ) -> dict:
        """Validate the quality of the BFE-based merge."""

        # Calculate merge statistics
        total_properties = len(property_df)
        total_cadastral = len(cadastral_gdf)
        merged_records = len(merged_gdf)

        # Count unique BFE matches
        if "bfe_number" in merged_gdf.columns:
            unique_bfe_matches = merged_gdf["bfe_number"].nunique()
        else:
            unique_bfe_matches = 0

        # Calculate match rates
        if self.config.join_method == "inner":
            match_rate = (merged_records / total_properties) * 100 if total_properties > 0 else 0
        elif self.config.join_method == "left":
            matched_properties = len(merged_gdf[merged_gdf["bfe_number"].notna()])
            match_rate = (
                (matched_properties / total_properties) * 100 if total_properties > 0 else 0
            )
        else:
            match_rate = 0  # For other join types, calculation would be different

        quality_stats = {
            "total_properties": total_properties,
            "total_cadastral_parcels": total_cadastral,
            "merged_records": merged_records,
            "unique_bfe_matches": unique_bfe_matches,
            "match_rate_percent": match_rate,
            "join_method": self.config.join_method,
        }

        # Log quality statistics
        self.log.info("BFE Merge Quality Statistics:")
        self.log.info(f"  Total property records: {total_properties}")
        self.log.info(f"  Total cadastral parcels: {total_cadastral}")
        self.log.info(f"  Merged records: {merged_records}")
        self.log.info(f"  Unique BFE matches: {unique_bfe_matches}")
        self.log.info(f"  Match rate: {match_rate:.1f}%")

        return quality_stats

    def _clean_and_standardize(
        self, gdf: gpd.GeoDataFrame, quality_stats: dict
    ) -> gpd.GeoDataFrame:
        """Clean and standardize the merged dataset."""
        self.log.info("Cleaning and standardizing merged dataset...")

        # Handle duplicate columns from merge
        # When pandas merge finds duplicates, it adds _property and _cadastral suffixes
        rename_mapping = {}
        columns_to_drop = []

        for col in gdf.columns:
            if col.endswith("_property"):
                # Keep the _property version (from property data) with original name
                original_name = col.replace("_property", "")
                if f"{original_name}_cadastral" in gdf.columns:
                    # Drop the _cadastral version and rename _property to original
                    columns_to_drop.append(f"{original_name}_cadastral")
                    rename_mapping[col] = original_name
            elif col.endswith("_cadastral"):
                # This is cadastral data - prefix with cadastral_
                if col.replace("_cadastral", "_property") not in gdf.columns:
                    # No corresponding _property, so this is unique to cadastral
                    original_name = col.replace("_cadastral", "")
                    rename_mapping[col] = f"cadastral_{original_name}"

        if columns_to_drop:
            gdf = gdf.drop(columns=columns_to_drop)
        if rename_mapping:
            gdf = gdf.rename(columns=rename_mapping)

        # Add merge metadata if requested
        if self.config.include_merge_metadata:
            gdf["merge_timestamp"] = datetime.utcnow()
            gdf["merge_method"] = "bfe_join"
            gdf["join_type"] = self.config.join_method
            gdf["has_cadastral_match"] = gdf["bfe_number"].notna()

            # Add quality statistics as metadata
            for key, value in quality_stats.items():
                gdf[f"merge_stats_{key}"] = value

        # Ensure BFE numbers are properly typed
        if "bestemtFastEjendomBFENr" in gdf.columns:
            gdf["bestemtFastEjendomBFENr"] = pd.to_numeric(
                gdf["bestemtFastEjendomBFENr"], errors="coerce"
            )
        if "bfe_number" in gdf.columns:
            gdf["bfe_number"] = pd.to_numeric(gdf["bfe_number"], errors="coerce")

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
        3. Perform BFE-based merge
        4. Validate merge quality
        5. Clean and standardize the result
        6. Save merged data to GCS
        """
        self.log.info("Running Property-Cadastral BFE merge job")

        try:
            # Load input datasets
            property_df = self._load_property_owners_data()
            if property_df is None:
                self.log.error("Failed to load property owners data")
                return

            cadastral_gdf = self._load_cadastral_data()
            if cadastral_gdf is None:
                self.log.error("Failed to load cadastral data")
                return

            # Perform BFE-based merge
            merged_gdf = self._perform_bfe_merge(property_df, cadastral_gdf)

            # Validate merge quality
            quality_stats = self._validate_bfe_merge_quality(merged_gdf, property_df, cadastral_gdf)

            # Clean and standardize
            cleaned_gdf = self._clean_and_standardize(merged_gdf, quality_stats)

            # Validate and transform geometries
            final_gdf = self._validate_and_transform(cleaned_gdf)

            # Save the result
            self._save_data(final_gdf, self.config.dataset, self.config.bucket)

            self.log.info("Property-Cadastral BFE merge job completed successfully")
            self.log.info(
                f"Final dataset: {len(final_gdf)} records with {quality_stats['match_rate_percent']:.1f}% match rate"
            )

        except Exception as e:
            self.log.error(f"Property-Cadastral BFE merge job failed: {e}")
            raise
