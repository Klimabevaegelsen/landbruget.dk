"""
Silver layer data processing for DAGI (Danish Administrative Geographic Division) data.

This module handles the transformation of raw DAGI data from the bronze layer
into clean, structured geographic data in the silver layer. It processes raw JSON
from the DAWA API and standardizes them into consistent GeoDataFrame formats.

The module contains:
- DAGISilverConfig: Configuration class for the DAGI silver processing
- DAGISilver: Implementation class for transforming and processing DAGI data

The data processing includes:
- Parsing raw JSON into GeoDataFrames
- Standardizing column names and data types
- Validating geometries and coordinate systems
- Adding consistent metadata fields
- Creating unified datasets for each administrative division type
"""

import json
from typing import Dict, Optional

import geopandas as gpd
import pandas as pd
from pydantic import Field

from unified_pipeline.common.base import BaseJobConfig, BaseSource
from unified_pipeline.util.gcs_util import GCSUtil
from unified_pipeline.util.geometry_validator import validate_and_transform_geometries
from unified_pipeline.util.timing import AsyncTimer


class DAGISilverConfig(BaseJobConfig):
    """
    Configuration for DAGI silver layer data processing.

    Attributes:
        name: Human-readable name of the data source
        type: Type of the data source
        description: Brief description of the data
        dataset: Name of the dataset in storage
        bucket: GCS bucket name for data storage
        target_crs: Target coordinate reference system for geometries
        endpoints: Dictionary mapping layer names to API endpoints (should match bronze)
        required_columns: Mapping of layer types to their required columns
        column_mapping: Mapping of original column names to standardized names
    """

    name: str = "Danish Administrative Geographic Division - Silver"
    type: str = "dawa_api_silver"
    description: str = "Processed administrative geographic divisions from Danish DAWA API"
    dataset: str = "dagi"
    bucket: str = "landbrugsdata-raw-data"

    target_crs: str = Field(
        default="EPSG:25832",
        description="Target coordinate reference system for Danish data (UTM Zone 32N)",
    )

    endpoints: Dict[str, str] = Field(
        default={
            "kommuner": "kommuner",
            "regioner": "regioner",
            "landsdele": "landsdele",
            "postnumre": "postnumre",
        },
        description="Mapping of layer names to API endpoints (should match bronze config)",
    )

    required_columns: Dict[str, list] = Field(
        default={
            "kommuner": ["kode", "navn", "regionskode"],
            "regioner": ["kode", "navn"],
            "landsdele": ["nuts3", "navn"],
            "postnumre": ["nr", "navn"],
        },
        description="Required columns for each layer type",
    )

    column_mapping: Dict[str, str] = Field(
        default={
            "kode": "code",
            "navn": "name",
            "nr": "code",
            "nuts3": "code",
            "regionskode": "region_code",
        },
        description="Mapping of Danish column names to English standardized names",
    )


class DAGISilver(BaseSource[DAGISilverConfig]):
    """
    Silver layer implementation for DAGI (Danish Administrative Geographic Division) data.

    Processes raw JSON data from the bronze layer and transforms them into clean,
    standardized GeoDataFrames suitable for analysis and downstream processing.

    Processing includes:
    - Parsing raw JSON into GeoDataFrames
    - Geometry validation and coordinate system transformation
    - Column standardization and type conversion
    - Data quality validation and cleaning
    - Metadata enrichment
    """

    def __init__(self, config: DAGISilverConfig, gcs_util: GCSUtil):
        """Initialize the DAGI silver layer with configuration."""
        super().__init__(config, gcs_util)

    def _parse_json_to_geodataframe(
        self, raw_json: str, layer_type: str
    ) -> Optional[gpd.GeoDataFrame]:
        """
        Parse raw JSON data into a GeoDataFrame.

        Args:
            raw_json: Raw JSON string from bronze layer
            layer_type: Type of administrative layer

        Returns:
            GeoDataFrame or None if parsing fails
        """
        try:
            data = json.loads(raw_json)

            if "features" not in data:
                self.log.error(f"No features found in JSON for {layer_type}")
                return None

            features = data["features"]
            if not features:
                self.log.warning(f"Empty features list for {layer_type}")
                return None

            # Create GeoDataFrame from GeoJSON features
            gdf = gpd.GeoDataFrame.from_features(features, crs="EPSG:4326")

            self.log.info(
                f"Parsed {len(gdf)} features for {layer_type} with columns: {list(gdf.columns)}"
            )

            return gdf

        except json.JSONDecodeError as e:
            self.log.error(f"JSON decode error for {layer_type}: {e}")
            return None
        except Exception as e:
            self.log.error(f"Error parsing JSON for {layer_type}: {e}")
            return None

    def _standardize_columns(self, gdf: gpd.GeoDataFrame, layer_type: str) -> gpd.GeoDataFrame:
        """
        Standardize column names and ensure required columns exist.

        Args:
            gdf: Input GeoDataFrame
            layer_type: Type of administrative layer

        Returns:
            GeoDataFrame with standardized columns
        """
        try:
            # Make a copy to avoid modifying the original
            result = gdf.copy()

            # Apply column mapping for standard names
            for old_name, new_name in self.config.column_mapping.items():
                if old_name in result.columns:
                    result = result.rename(columns={old_name: new_name})

            # Check for required columns
            required_cols = self.config.required_columns.get(layer_type, [])
            mapped_required = [self.config.column_mapping.get(col, col) for col in required_cols]

            missing_cols = [col for col in mapped_required if col not in result.columns]
            if missing_cols:
                self.log.warning(f"Missing required columns for {layer_type}: {missing_cols}")

            # Ensure standard data types
            if "code" in result.columns:
                # Convert codes to string to handle leading zeros
                result["code"] = result["code"].astype(str)

            if "name" in result.columns:
                # Clean up name strings
                result["name"] = result["name"].astype(str).str.strip()

            self.log.info(f"Standardized columns for {layer_type}: {list(result.columns)}")

            return result

        except Exception as e:
            self.log.error(f"Error standardizing columns for {layer_type}: {e}")
            raise

    def _validate_data_quality(self, gdf: gpd.GeoDataFrame, layer_type: str) -> gpd.GeoDataFrame:
        """
        Validate and clean data quality issues.

        Args:
            gdf: Input GeoDataFrame
            layer_type: Type of administrative layer

        Returns:
            Cleaned GeoDataFrame
        """
        try:
            original_count = len(gdf)
            result = gdf.copy()

            # Remove rows with null geometries
            null_geom_mask = result.geometry.isnull()
            if null_geom_mask.any():
                null_count = null_geom_mask.sum()
                self.log.warning(
                    f"Removing {null_count} rows with null geometries from {layer_type}"
                )
                result = result[~null_geom_mask]

            # Remove rows with invalid geometries
            invalid_geom_mask = ~result.geometry.is_valid
            if invalid_geom_mask.any():
                invalid_count = invalid_geom_mask.sum()
                self.log.warning(f"Found {invalid_count} invalid geometries in {layer_type}")
                # Try to fix invalid geometries
                result.loc[invalid_geom_mask, "geometry"] = result.loc[
                    invalid_geom_mask, "geometry"
                ].buffer(0)
                # Check if fix worked
                still_invalid = ~result.geometry.is_valid
                if still_invalid.any():
                    self.log.warning(
                        f"Removing {still_invalid.sum()} unfixable invalid geometries from {layer_type}"
                    )
                    result = result[~still_invalid]

            # Remove duplicate codes if code column exists
            if "code" in result.columns:
                duplicates_mask = result["code"].duplicated()
                if duplicates_mask.any():
                    dup_count = duplicates_mask.sum()
                    self.log.warning(f"Removing {dup_count} duplicate codes from {layer_type}")
                    result = result[~duplicates_mask]

            final_count = len(result)
            if final_count != original_count:
                self.log.info(
                    f"Data quality validation for {layer_type}: {original_count} -> {final_count} rows"
                )

            return result

        except Exception as e:
            self.log.error(f"Error validating data quality for {layer_type}: {e}")
            raise

    def _add_metadata(self, gdf: gpd.GeoDataFrame, layer_type: str) -> gpd.GeoDataFrame:
        """
        Add metadata fields to the GeoDataFrame.

        Args:
            gdf: Input GeoDataFrame
            layer_type: Type of administrative layer

        Returns:
            GeoDataFrame with additional metadata
        """
        try:
            result = gdf.copy()

            # Add processing metadata
            result["layer_type"] = layer_type
            result["processing_timestamp"] = pd.Timestamp.now(tz="UTC")
            result["data_source"] = "dagi_dawa_api"
            result["crs_epsg"] = self.config.target_crs

            # Add geometry-based metadata
            result["area_m2"] = result.geometry.area
            result["centroid_x"] = result.geometry.centroid.x
            result["centroid_y"] = result.geometry.centroid.y

            # Add record count for the layer
            result["total_records_in_layer"] = len(result)

            self.log.info(f"Added metadata to {layer_type} with {len(result)} features")

            return result

        except Exception as e:
            self.log.error(f"Error adding metadata to {layer_type}: {e}")
            raise

    def _process_layer(self, raw_data: pd.DataFrame, layer_type: str) -> Optional[gpd.GeoDataFrame]:
        """
        Process a single DAGI layer through the silver transformation pipeline.

        Args:
            raw_data: Raw data DataFrame from bronze layer
            layer_type: Type of administrative layer

        Returns:
            Processed and standardized GeoDataFrame or None if processing fails
        """
        try:
            self.log.info(f"Processing DAGI layer: {layer_type} with {len(raw_data)} raw records")

            # Parse JSON from the first payload (should only be one for DAGI)
            if raw_data.empty:
                self.log.warning(f"No raw data found for {layer_type}")
                return None

            raw_json = raw_data.iloc[0]["payload"]
            gdf = self._parse_json_to_geodataframe(raw_json, layer_type)

            if gdf is None or len(gdf) == 0:
                self.log.warning(f"No data could be parsed for {layer_type}")
                return None

            # Validate and transform geometries
            processed_gdf = validate_and_transform_geometries(gdf, f"dagi_{layer_type}")

            # Transform to target CRS if different
            if processed_gdf.crs != self.config.target_crs:
                self.log.info(
                    f"Transforming {layer_type} from {processed_gdf.crs} to {self.config.target_crs}"
                )
                processed_gdf = processed_gdf.to_crs(self.config.target_crs)

            # Standardize columns
            processed_gdf = self._standardize_columns(processed_gdf, layer_type)

            # Validate data quality
            processed_gdf = self._validate_data_quality(processed_gdf, layer_type)

            # Add metadata
            processed_gdf = self._add_metadata(processed_gdf, layer_type)

            self.log.info(f"Successfully processed {layer_type}: {len(processed_gdf)} features")

            return processed_gdf

        except Exception as e:
            self.log.error(f"Error processing DAGI layer {layer_type}: {e}")
            return None

    async def run(self) -> None:
        """
        Run the DAGI silver layer processing for all configured layers.

        This method processes all DAGI layers from the bronze layer,
        transforming raw JSON data into structured GeoDataFrames
        and saving them in the silver layer.
        """
        try:
            async with AsyncTimer("DAGI silver layer processing") as timer:
                self.log.info("Starting DAGI silver layer processing")

                processed_count = 0

                # Process each layer
                for layer_name in self.config.endpoints.keys():
                    try:
                        dataset_name = f"{self.config.dataset}_{layer_name}"
                        self.log.info(f"Processing silver layer for DAGI {layer_name}")

                        # Read bronze data
                        bronze_df = self._read_bronze_data(dataset_name, self.config.bucket)
                        if bronze_df is None or bronze_df.empty:
                            self.log.warning(f"No bronze data found for DAGI {layer_name}")
                            continue

                        # Process the data
                        processed_df = self._process_layer(bronze_df, layer_name)
                        if processed_df is None:
                            self.log.warning(f"No processed data for DAGI {layer_name}")
                            continue

                        # Save to silver layer
                        self._save_data(
                            processed_df, dataset_name, self.config.bucket, stage="silver"
                        )
                        self.log.info(
                            f"Successfully processed and saved DAGI {layer_name} silver data"
                        )
                        processed_count += 1

                    except Exception as e:
                        self.log.error(f"Error processing DAGI {layer_name} silver layer: {e}")
                        continue

                self.log.info(
                    f"DAGI silver processing completed in {timer.elapsed():.2f}s. "
                    f"Successfully processed {processed_count}/{len(self.config.endpoints)} layers"
                )

        except Exception as e:
            self.log.error(f"Critical error in DAGI silver processing: {e}")
            raise
