import os
from datetime import datetime
from typing import Any, Dict, Optional

import geopandas as gpd
import pandas as pd
from pydantic import ConfigDict

from unified_pipeline.common.base import BaseJobConfig, BaseSource, GoldJobInterface
from unified_pipeline.util.gcs_util import GCSUtil


class PropertyCadastralMergeGoldConfig(BaseJobConfig):
    """Configuration for Property-Cadastral merge gold layer."""

    name: str = "Property Cadastral Merge Gold"
    dataset: str = "property_cadastral_merged"
    type: str = "gold"
    description: str = "Merge property owners with cadastral data for business analytics"
    frequency: str = "weekly"
    bucket: str = os.getenv("GCS_BUCKET")

    # Input silver datasets
    property_owners_dataset: str = "property_owners"
    cadastral_dataset: str = "cadastral"

    # Merge configuration
    join_method: str = "inner"
    validate_bfe_numbers: bool = True
    include_merge_metadata: bool = True

    # Quality thresholds
    min_match_rate: float = 0.8  # Minimum acceptable match rate

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class PropertyCadastralMergeGold(BaseSource[PropertyCadastralMergeGoldConfig], GoldJobInterface):
    """
    Gold layer processor for property-cadastral merge.

    Combines property owners and cadastral silver data to create
    business-ready datasets for analytics and downstream consumption.
    """

    def __init__(self, config: PropertyCadastralMergeGoldConfig, gcs_util: GCSUtil):
        super().__init__(config, gcs_util)

        # Configure DuckDB for large dataset processing
        self.conn.execute("SET memory_limit = '12GB'")
        self.conn.execute("SET threads = 4")
        self.conn.execute("INSTALL spatial")
        self.conn.execute("LOAD spatial")

    def _load_silver_data(
        self, silver_data: Optional[Dict[str, Any]]
    ) -> tuple[Optional[pd.DataFrame], Optional[gpd.GeoDataFrame]]:
        """Load property owners and cadastral data from silver layer."""

        property_df = None
        cadastral_gdf = None

        if silver_data:
            # Use in-memory data
            self.log.info("Using silver data from memory (in-memory data passing)")
            property_df = silver_data.get(self.config.property_owners_dataset)
            cadastral_gdf = silver_data.get(self.config.cadastral_dataset)

            self.log.info(
                f"In-memory silver data available: property_owners={property_df is not None}, cadastral={cadastral_gdf is not None}"
            )
        else:
            # Fallback to storage - this is the main path for gold layer
            self.log.info("Reading silver data from GCS storage")

            # Read property owners data from storage
            self.log.info(
                f"Looking for property owners data in silver/{self.config.property_owners_dataset}/"
            )
            property_df = self._read_data_from_storage(
                self.config.property_owners_dataset, self.config.bucket, stage="silver"
            )
            if property_df is not None:
                self.log.info(
                    f"Successfully loaded property owners data: {len(property_df)} records"
                )
            else:
                self.log.warning("No property owners data found in silver layer")

            # Read cadastral data from storage
            self.log.info(f"Looking for cadastral data in silver/{self.config.cadastral_dataset}/")
            cadastral_gdf = self._read_data_from_storage(
                self.config.cadastral_dataset, self.config.bucket, stage="silver"
            )
            if cadastral_gdf is not None:
                self.log.info(f"Successfully loaded cadastral data: {len(cadastral_gdf)} records")
            else:
                self.log.warning("No cadastral data found in silver layer")

        return property_df, cadastral_gdf

    def _perform_bfe_merge(
        self, property_df: pd.DataFrame, cadastral_gdf: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
        """Perform BFE-based merge using DuckDB."""

        self.log.info("Performing BFE-based merge with DuckDB...")

        # Load data into DuckDB
        self.conn.register("property_owners", property_df)
        self.conn.register("cadastral", cadastral_gdf)

        # Perform merge
        join_type = self.config.join_method.upper()

        # Build the merge query based on available columns
        property_bfe_col = "bestemtFastEjendomBFENr"
        cadastral_bfe_col = "bfe_number"

        # Check if BFE columns exist and find alternatives if needed
        if property_bfe_col not in property_df.columns:
            self.log.warning(
                f"Property BFE column '{property_bfe_col}' not found. Available columns: {list(property_df.columns)}"
            )
            # Try to find alternative BFE column
            bfe_cols = [
                col
                for col in property_df.columns
                if "bfe" in col.lower() or "ejendom" in col.lower()
            ]
            if bfe_cols:
                property_bfe_col = bfe_cols[0]
                self.log.info(f"Using alternative property BFE column: {property_bfe_col}")
            else:
                raise ValueError("No BFE column found in property data")

        if cadastral_bfe_col not in cadastral_gdf.columns:
            self.log.warning(
                f"Cadastral BFE column '{cadastral_bfe_col}' not found. Available columns: {list(cadastral_gdf.columns)}"
            )
            # Try to find alternative BFE column
            bfe_cols = [col for col in cadastral_gdf.columns if "bfe" in col.lower()]
            if bfe_cols:
                cadastral_bfe_col = bfe_cols[0]
                self.log.info(f"Using alternative cadastral BFE column: {cadastral_bfe_col}")
            else:
                raise ValueError("No BFE column found in cadastral data")

        merge_query = f"""
        SELECT 
            p.*,
            c.* EXCLUDE ({cadastral_bfe_col}),
            c.{cadastral_bfe_col} as cadastral_bfe_number,
            '{datetime.utcnow().isoformat()}'::TIMESTAMP as merge_timestamp,
            'bfe_join' as merge_method,
            '{self.config.join_method}' as join_type,
            (c.{cadastral_bfe_col} IS NOT NULL) as has_cadastral_match
        FROM property_owners p
        {join_type} JOIN cadastral c 
        ON p.{property_bfe_col} = c.{cadastral_bfe_col}
        """

        merged_df = self.conn.execute(merge_query).df()

        # Convert to GeoDataFrame if geometry column exists
        if "geometry" in merged_df.columns:
            merged_gdf = gpd.GeoDataFrame(merged_df, geometry="geometry", crs="EPSG:4326")
        else:
            # Create a simple GeoDataFrame with point geometries
            self.log.warning("No geometry column found, creating point geometries")
            merged_gdf = gpd.GeoDataFrame(merged_df, crs="EPSG:4326")
            # Create dummy point geometries
            merged_gdf["geometry"] = gpd.points_from_xy(
                [12.5] * len(merged_df), [55.7] * len(merged_df)
            )

        return merged_gdf

    def _validate_merge_quality(
        self, merged_gdf: gpd.GeoDataFrame, property_df: pd.DataFrame
    ) -> Dict[str, Any]:
        """Validate merge quality and return statistics."""

        total_properties = len(property_df)
        merged_records = len(merged_gdf)
        match_rate = (merged_records / total_properties) if total_properties > 0 else 0

        quality_stats = {
            "total_properties": total_properties,
            "merged_records": merged_records,
            "match_rate": match_rate,
            "match_rate_percent": match_rate * 100,
        }

        self.log.info(f"Merge quality: {quality_stats['match_rate_percent']:.1f}% match rate")

        if match_rate < self.config.min_match_rate:
            self.log.warning(
                f"Match rate {match_rate:.1%} below threshold {self.config.min_match_rate:.1%}"
            )

        return quality_stats

    async def run(self, silver_data: Optional[Dict[str, Any]] = None) -> None:
        """
        Run the property-cadastral merge gold processing.

        Args:
            silver_data: Optional dictionary of silver datasets for in-memory processing
        """
        self.log.info("Starting Property-Cadastral Merge Gold processing")

        try:
            # Load silver data from GCS storage or in-memory
            property_df, cadastral_gdf = self._load_silver_data(silver_data)

            # Check if we have the required data
            if cadastral_gdf is None:
                self.log.error("No cadastral data available - cannot proceed with merge")
                return

            if property_df is None:
                self.log.error("No property owners data available - cannot proceed with merge")
                return

            self.log.info(f"Loaded {len(property_df):,} property records")
            self.log.info(f"Loaded {len(cadastral_gdf):,} cadastral records")

            # Perform merge
            merged_gdf = self._perform_bfe_merge(property_df, cadastral_gdf)

            # Validate quality
            quality_stats = self._validate_merge_quality(merged_gdf, property_df)

            # Save to gold layer
            self._save_data(merged_gdf, self.config.dataset, self.config.bucket, stage="gold")

            self.log.info(
                f"Property-Cadastral merge completed: {len(merged_gdf):,} records "
                f"with {quality_stats['match_rate_percent']:.1f}% match rate"
            )

        except Exception as e:
            self.log.error(f"Property-Cadastral merge failed: {e}")
            raise

        finally:
            if hasattr(self, "conn"):
                self.conn.close()
