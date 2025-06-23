"""
DST Zone Mapping silver layer component for DAGI pipeline.

This module creates a spatial lookup table that maps field geometries to DST (Danmarks Statistik) zones
by combining DAGI administrative data with DST regional classifications.

The module contains:
- DSTZoneMappingConfig: Configuration for DST zone mapping processing
- DSTZoneMapping: Implementation class for creating the spatial lookup table

The processing creates a comprehensive mapping between:
- DST regions (from Danmarks Statistik)
- DAGI landsdele (administrative geographic divisions)
- DAGI regions (current administrative regions)
- DAGI municipalities (local administrative units)
"""

import json
from typing import Any, Dict, Optional

import geopandas as gpd
import pandas as pd
from pydantic import Field

from unified_pipeline.common.base import BaseJobConfig, BaseSource, SilverJobInterface
from unified_pipeline.util.gcs_util import GCSUtil
from unified_pipeline.util.timing import AsyncTimer


class DSTZoneMappingConfig(BaseJobConfig):
    """
    Configuration for DST zone mapping processing.

    Attributes:
        name: Human-readable name of the component
        type: Type of the component
        description: Brief description of the functionality
        dataset: Name of the output dataset
        bucket: GCS bucket name for data storage
        target_crs: Target coordinate reference system
        dst_mappings: Dictionary defining DST region to DAGI landsdele mappings
    """

    name: str = "DST Zone Spatial Mapping"
    type: str = "dst_zone_mapping"
    description: str = "Spatial lookup table for mapping field geometries to DST statistical zones"
    dataset: str = "dst_zone_mapping"
    bucket: str = "landbrugsdata-raw-data"

    target_crs: str = Field(
        default="EPSG:4326",
        description="Target coordinate reference system - WGS84 for consistency",
    )

    dst_mappings: Dict[str, Dict[str, Any]] = Field(
        default={
            "Hele landet": {
                "landsdele_codes": [
                    "DK011",
                    "DK012",
                    "DK013",
                    "DK014",
                    "DK021",
                    "DK022",
                    "DK031",
                    "DK032",
                    "DK041",
                    "DK042",
                    "DK050",
                ],
                "description": "Entire country - all landsdele",
            },
            "Landsdel Bornholm": {"landsdele_codes": ["DK014"], "description": "Bornholm island"},
            "Landsdel Fyn": {"landsdele_codes": ["DK031"], "description": "Fyn island"},
            "Landsdel Sydjylland": {
                "landsdele_codes": ["DK032"],
                "description": "Southern Jutland",
            },
            "Landsdel Vestjylland": {
                "landsdele_codes": ["DK041"],
                "description": "Western Jutland",
            },
            "Landsdel Østjylland": {"landsdele_codes": ["DK042"], "description": "Eastern Jutland"},
            "Landsdelene Byen København, Københavns omegn og Nordsjælland": {
                "landsdele_codes": ["DK011", "DK012", "DK013"],
                "description": "Copenhagen city, suburbs and North Zealand",
            },
            "Region Nordjylland": {
                "landsdele_codes": ["DK050"],
                "description": "North Jutland region",
            },
            "Region Sjælland": {
                "landsdele_codes": ["DK021", "DK022"],
                "description": "Zealand region (East and West/South Zealand)",
            },
        },
        description="Mapping of DST regions to DAGI landsdele codes",
    )


class DSTZoneMapping(BaseSource[DSTZoneMappingConfig], SilverJobInterface):
    """
    DST Zone Mapping implementation for creating spatial lookup tables.

    This component processes DAGI administrative data and creates a comprehensive
    spatial lookup table that can be used to map any field geometry to its
    corresponding DST statistical zones.

    The output includes:
    - Spatial geometries for each landsdel
    - Mapping to DST regions
    - DAGI region and municipality information
    - Metadata for analysis and validation
    """

    def __init__(self, config: DSTZoneMappingConfig, gcs_util: GCSUtil):
        """Initialize the DST zone mapping component."""
        super().__init__(config, gcs_util)

    def _load_dagi_data(
        self, bronze_data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, gpd.GeoDataFrame]:
        """
        Load DAGI data from silver layer or bronze data.

        Args:
            bronze_data: Optional in-memory data from bronze stage

        Returns:
            Dictionary containing landsdele, regioner, and kommuner GeoDataFrames
        """
        try:
            dagi_data = {}

            # Required DAGI layers for DST mapping
            required_layers = ["landsdele", "regioner", "kommuner"]

            # Column mapping for standardization (same as DAGI silver layer)
            column_mapping = {
                "kode": "code",
                "navn": "name",
                "nr": "code",
                "nuts3": "code",
                "regionskode": "region_code",
            }

            for layer in required_layers:
                try:
                    if bronze_data and layer in bronze_data:
                        # Use in-memory data if available
                        self.log.info(f"Using in-memory data for DAGI {layer}")
                        raw_json = bronze_data[layer]
                        data = json.loads(raw_json)
                        if "features" in data and data["features"]:
                            gdf = gpd.GeoDataFrame.from_features(data["features"], crs="EPSG:4326")

                            # Apply column standardization (similar to DAGI silver layer)
                            for old_name, new_name in column_mapping.items():
                                if old_name in gdf.columns:
                                    gdf = gdf.rename(columns={old_name: new_name})

                            # Ensure standard data types
                            if "code" in gdf.columns:
                                gdf["code"] = gdf["code"].astype(str)
                            if "name" in gdf.columns:
                                gdf["name"] = gdf["name"].astype(str).str.strip()

                            dagi_data[layer] = gdf
                            self.log.info(f"Loaded {len(gdf)} features for {layer} from memory")
                    else:
                        # Fallback to reading from silver layer
                        dataset_name = f"dagi_{layer}"
                        self.log.info(f"Reading DAGI {layer} from silver layer")
                        df = self._read_silver_data(dataset_name, self.config.bucket)
                        if df is not None and not df.empty:
                            # Convert to GeoDataFrame if it's not already
                            if not isinstance(df, gpd.GeoDataFrame):
                                df = gpd.GeoDataFrame(df, geometry="geometry")
                            dagi_data[layer] = df
                            self.log.info(f"Loaded {len(df)} features for {layer} from silver")
                        else:
                            self.log.warning(f"No data found for DAGI {layer}")

                except Exception as e:
                    self.log.error(f"Error loading DAGI {layer}: {e}")
                    continue

            # Validate that we have all required data
            missing_layers = [layer for layer in required_layers if layer not in dagi_data]
            if missing_layers:
                raise ValueError(f"Missing required DAGI layers: {missing_layers}")

            return dagi_data

        except Exception as e:
            self.log.error(f"Error loading DAGI data: {e}")
            raise

    def _create_dst_zone_lookup(self, dagi_data: Dict[str, gpd.GeoDataFrame]) -> gpd.GeoDataFrame:
        """
        Create the DST zone spatial lookup table.

        Args:
            dagi_data: Dictionary containing DAGI GeoDataFrames

        Returns:
            GeoDataFrame with DST zone mappings
        """
        try:
            landsdele = dagi_data["landsdele"]
            regioner = dagi_data["regioner"]

            self.log.info(f"Creating DST zone lookup from {len(landsdele)} landsdele")

            # Debug: Log available columns
            self.log.info(f"Landsdele columns: {list(landsdele.columns)}")
            self.log.info(f"Regioner columns: {list(regioner.columns)}")

            # Create lookup records for each landsdel
            lookup_records = []

            for _, landsdel in landsdele.iterrows():
                # Handle both raw and standardized column names for landsdel
                landsdel_code = landsdel.get("code") or landsdel.get("nuts3") or ""
                landsdel_name = landsdel.get("name") or landsdel.get("navn") or ""

                if not landsdel_code:
                    self.log.warning(f"No code found for landsdel: {landsdel_name}")
                    continue

                # Find which DST regions this landsdel belongs to
                dst_regions = []
                for dst_region, mapping in self.config.dst_mappings.items():
                    if landsdel_code in mapping["landsdele_codes"]:
                        dst_regions.append(dst_region)

                if not dst_regions:
                    self.log.warning(f"No DST mapping found for landsdel {landsdel_code}")
                    continue

                # Get the DAGI region info - handle both raw and standardized column names
                region_code = landsdel.get("region_code") or landsdel.get("regionskode") or ""
                region_name = landsdel.get("regionsnavn", "")

                # Find the corresponding DAGI region details
                region_info = None
                if region_code:
                    # Handle both raw and standardized column names for regioner
                    if "code" in regioner.columns:
                        region_matches = regioner[regioner["code"] == region_code]
                    elif "kode" in regioner.columns:
                        region_matches = regioner[regioner["kode"] == region_code]
                    else:
                        self.log.warning(
                            f"No code column found in regioner. Available columns: {list(regioner.columns)}"
                        )
                        region_matches = gpd.GeoDataFrame()

                    if not region_matches.empty:
                        region_info = region_matches.iloc[0]

                # Create record
                record = {
                    "landsdel_code": landsdel_code,
                    "landsdel_name": landsdel_name,
                    "landsdel_dagi_id": landsdel.get("dagi_id", ""),
                    "dagi_region_code": region_code,
                    "dagi_region_name": region_name
                    or (
                        region_info.get("name") or region_info.get("navn", "")
                        if region_info is not None
                        else ""
                    ),
                    "dagi_region_nuts2": (
                        region_info.get("nuts2", "") if region_info is not None else ""
                    ),
                    "dst_regions": "|".join(dst_regions),  # Multiple DST regions separated by |
                    "geometry": landsdel["geometry"],
                    "area_m2": landsdel.get("area_m2", 0),
                    "centroid_x": landsdel.get("centroid_x", 0),
                    "centroid_y": landsdel.get("centroid_y", 0),
                }

                lookup_records.append(record)

            # Create GeoDataFrame
            if not lookup_records:
                raise ValueError("No lookup records could be created")

            gdf_lookup = gpd.GeoDataFrame(lookup_records, crs=self.config.target_crs)

            # Add metadata
            gdf_lookup["created_at"] = pd.Timestamp.now(tz="UTC")
            gdf_lookup["data_source"] = "dst_zone_mapping"
            gdf_lookup["mapping_version"] = "1.0"

            self.log.info(f"Created DST zone lookup table with {len(gdf_lookup)} records")

            # Log mapping summary
            dst_zone_counts = {}
            for _, row in gdf_lookup.iterrows():
                dst_zones = row["dst_regions"].split("|")
                for dst_zone in dst_zones:
                    dst_zone_counts[dst_zone] = dst_zone_counts.get(dst_zone, 0) + 1

            self.log.info("DST zone coverage:")
            for dst_zone, count in dst_zone_counts.items():
                self.log.info(f"  {dst_zone}: {count} landsdele")

            return gdf_lookup

        except Exception as e:
            self.log.error(f"Error creating DST zone lookup: {e}")
            raise

    def _create_reference_table(self, lookup_gdf: gpd.GeoDataFrame) -> pd.DataFrame:
        """
        Create a reference table without geometry for easy viewing.

        Args:
            lookup_gdf: GeoDataFrame with full lookup data

        Returns:
            DataFrame with reference information
        """
        try:
            # Drop geometry column and create reference table
            reference_df = lookup_gdf.drop(columns=["geometry"]).copy()

            self.log.info(f"Created reference table with {len(reference_df)} records")

            return reference_df

        except Exception as e:
            self.log.error(f"Error creating reference table: {e}")
            raise

    async def run(self, bronze_data: Optional[Any] = None) -> None:
        """
        Run the DST zone mapping processing.

        This method creates a comprehensive spatial lookup table that maps
        DAGI administrative divisions to DST statistical zones.

        Args:
            bronze_data: Optional in-memory data from bronze stage
        """
        try:
            async with AsyncTimer("DST zone mapping processing") as timer:
                self.log.info("Starting DST zone mapping processing")

                # Load DAGI data
                dagi_data = self._load_dagi_data(bronze_data)

                # Create the DST zone lookup table
                lookup_gdf = self._create_dst_zone_lookup(dagi_data)

                # Save the spatial lookup table
                self._save_data(lookup_gdf, self.config.dataset, self.config.bucket, stage="silver")
                self.log.info("Saved DST zone spatial lookup table")

                # Create and save reference table (without geometry)
                reference_df = self._create_reference_table(lookup_gdf)
                reference_dataset = f"{self.config.dataset}_reference"
                self._save_data(reference_df, reference_dataset, self.config.bucket, stage="silver")
                self.log.info("Saved DST zone reference table")

                self.log.info(
                    f"DST zone mapping processing completed in {timer.elapsed():.2f}s. "
                    f"Created lookup table with {len(lookup_gdf)} records covering "
                    f"{len(self.config.dst_mappings)} DST regions"
                )

        except Exception as e:
            self.log.error(f"Critical error in DST zone mapping processing: {e}")
            raise
