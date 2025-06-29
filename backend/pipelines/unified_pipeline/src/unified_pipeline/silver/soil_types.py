"""
Silver layer data processing for Soil Types data.

This module handles the transformation and cleaning of soil types data from the bronze layer.
It processes the raw WFS data into a standardized format suitable for analysis and
further downstream processing.

The module contains:
- SoilTypesSilverConfig: Configuration class for the silver layer processing
- SoilTypesSilver: Implementation class for processing and transforming data

The processing includes data validation, geometry cleaning, attribute standardization,
and quality assurance checks.
"""

import os
from typing import Any, Optional


# ✅ MIGRATION: Removed pandas import - using DuckDB for data operations
from dotenv import load_dotenv
from pydantic import ConfigDict

from unified_pipeline.common.base import BaseJobConfig, BaseSource, SilverJobInterface
from unified_pipeline.common.geometry_validator import validate_and_transform_geometries
from unified_pipeline.util.gcs_util import GCSUtil

load_dotenv()


class SoilTypesSilverConfig(BaseJobConfig):
    """
    Configuration for the Soil Types Silver source.

    This class defines all configuration parameters needed for processing soil types
    data in the silver layer. It includes dataset configuration, storage settings,
    and processing parameters.

    Attributes:
        name (str): Human-readable name of the data source
        dataset (str): Name of the dataset
        type (str): Type of the data source (wfs)
        description (str): Brief description of the data
        frequency (str): How often the data is updated
        bucket (str): GCS bucket name for processed data storage
        save_local (bool): Whether to save data locally instead of uploading to GCS
    """

    name: str = "Danish Soil Types"
    dataset: str = "soil_types"
    type: str = "wfs"
    description: str = "Processed soil types data from Danish Environmental Portal"
    frequency: str = "monthly"
    bucket: str = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")
    save_local: bool = os.getenv("SAVE_LOCAL", "False").lower() == "true"

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class SoilTypesSilver(BaseSource[SoilTypesSilverConfig], SilverJobInterface):
    """
    Silver layer processing for soil types data.

    This class is responsible for processing and transforming soil types data from
    the bronze layer into a clean, standardized format. It handles data validation,
    geometry cleaning, attribute standardization, and quality assurance.

    Processing flow:
    1. Read data from the bronze layer
    2. Validate and clean geometries
    3. Standardize attribute names and values
    4. Perform quality checks
    5. Save processed data to the silver layer
    """

    def __init__(self, config: SoilTypesSilverConfig, gcs_util: GCSUtil) -> None:
        """
        Initialize the SoilTypesSilver source.

        Args:
            config (SoilTypesSilverConfig): Configuration for the silver layer processing
            gcs_util (GCSUtil): Utility for Google Cloud Storage operations
        """
        super().__init__(config, gcs_util)

    def _validate_and_transform(self, gdf: gGeo) -> gGeo:
        """
        Validate and transform the soil types Geo.

        This method validates geometries and transforms the data into a standardized
        format suitable for analysis. It includes geometry validation, attribute
        cleaning, and data type standardization.

        Args:
            gdf (gGeo): The raw Geo from the bronze layer

        Returns:
            gGeo: The validated and transformed Geo

        Raises:
            Exception: If validation or transformation fails
        """
        try:
            self.log.info("Starting soil types data validation and transformation")

            # Validate and fix geometries using the common validator
            validated_gdf = validate_and_transform_geometries(gdf, self.config.dataset)

            # Standardize column names to lowercase with underscores
            column_mapping = {
                "JORDHT": "soil_height",
                "JORD_TEKST": "soil_description",
                "TEMANAVN": "theme_name",
                "KODE": "soil_code",
                "geom": "geometry",
            }

            # Rename columns if they exist
            for old_name, new_name in column_mapping.items():
                if old_name in validated_gdf.columns:
                    validated_gdf = validated_gdf.rename(columns={old_name: new_name})

            # Ensure geometry column is named 'geometry'
            if "geom" in validated_gdf.columns and "geometry" not in validated_gdf.columns:
                validated_gdf = validated_gdf.rename(columns={"geom": "geometry"})

            # Set the geometry column
            if "geometry" in validated_gdf.columns:
                validated_gdf = validated_gdf.set_geometry("geometry")

            # ✅ MIGRATION: Clean and standardize data types using DuckDB
            # Convert Geo to regular  for DuckDB processing
            df_for_processing = validated_gdf.copy()
            if hasattr(df_for_processing, "geometry"):
                df_for_processing["geometry_wkt"] = validated_gdf.geometry.to_wkt()
                df_for_processing = df_for_processing.drop("geometry", axis=1)

            # Use DuckDB for numeric conversions
            if (
                "soil_height" in df_for_processing.columns
                or "soil_code" in df_for_processing.columns
            ):
                import duckdb

                temp_conn = duckdb.connect()
                temp_conn.register("temp_soil_data", df_for_processing)

                # Build conversion query
                select_parts = []
                for col in df_for_processing.columns:
                    if col == "soil_height":
                        select_parts.append("TRY_CAST(soil_height AS DOUBLE) as soil_height")
                    elif col == "soil_code":
                        select_parts.append("TRY_CAST(soil_code AS DOUBLE) as soil_code")
                    else:
                        select_parts.append(f'"{col}"')

                conversion_query = f"SELECT {', '.join(select_parts)} FROM temp_soil_data"
                # ✅ MIGRATION: Update validated_gdf columns directly from DuckDB query results
                result = temp_conn.execute(conversion_query).fetchall()
                columns = [desc[0] for desc in temp_conn.description]
                temp_conn.close()

                # Update specific columns in validated_gdf with converted values
                for col in ["soil_height", "soil_code"]:
                    if col in columns:
                        col_idx = columns.index(col)
                        validated_gdf[col] = [row[col_idx] for row in result]

            # Clean text fields
            text_columns = ["soil_description", "theme_name"]
            for col in text_columns:
                if col in validated_gdf.columns:
                    validated_gdf[col] = validated_gdf[col].astype(str).str.strip()
                    # ✅ MIGRATION: Replace 'nan' strings with actual NaN using native pandas
                    validated_gdf[col] = validated_gdf[col].replace("nan", None)

            # ✅ MIGRATION: Add processing metadata using DuckDB timestamp
            import duckdb

            temp_conn = duckdb.connect()
            current_timestamp = temp_conn.execute("SELECT current_timestamp").fetchone()[0]
            temp_conn.close()
            validated_gdf["processed_at"] = current_timestamp
            validated_gdf["data_quality"] = "validated"

            # Remove any completely empty rows
            validated_gdf = validated_gdf.dropna(subset=["geometry"])

            self.log.info(f"Validation completed. Processed {len(validated_gdf):,} features")
            self.log.info(f"Final columns: {list(validated_gdf.columns)}")

            return validated_gdf

        except Exception as e:
            self.log.error(f"Error in validation and transformation: {str(e)}")
            raise

    def _perform_quality_checks(self, gdf: gGeo) -> None:
        """
        Perform quality checks on the processed data.

        This method runs various quality assurance checks on the processed data
        to ensure data integrity and completeness.

        Args:
            gdf (gGeo): The processed Geo to check

        Returns:
            None
        """
        try:
            self.log.info("Performing quality checks on soil types data")

            # Check for empty geometries
            empty_geoms = gdf.geometry.is_empty.sum()
            if empty_geoms > 0:
                self.log.warning(f"Found {empty_geoms} empty geometries")

            # Check for invalid geometries
            invalid_geoms = (~gdf.geometry.is_valid).sum()
            if invalid_geoms > 0:
                self.log.warning(f"Found {invalid_geoms} invalid geometries")

            # Check data completeness
            total_features = len(gdf)

            if "soil_description" in gdf.columns:
                missing_descriptions = gdf["soil_description"].isna().sum()
                self.log.info(
                    f"Missing soil descriptions: {missing_descriptions}/{total_features} "
                    f"({missing_descriptions / total_features * 100:.1f}%)"
                )

            if "soil_code" in gdf.columns:
                missing_codes = gdf["soil_code"].isna().sum()
                self.log.info(
                    f"Missing soil codes: {missing_codes}/{total_features} "
                    f"({missing_codes / total_features * 100:.1f}%)"
                )

            # Check for duplicate soil codes
            if "soil_code" in gdf.columns:
                unique_codes = gdf["soil_code"].nunique()
                total_codes = gdf["soil_code"].notna().sum()
                self.log.info(
                    f"Unique soil codes: {unique_codes} out of {total_codes} non-null values"
                )

            self.log.info("Quality checks completed")

        except Exception as e:
            self.log.error(f"Error in quality checks: {str(e)}")
            # Don't raise here as quality checks are informational

    async def run(self, bronze_data: Optional[Any] = None) -> None:
        """
        Run the complete soil types silver layer processing job.

        This is the main entry point that orchestrates the entire silver layer process:
        1. Reads data from the bronze layer (either in-memory or from storage)
        2. Validates and transforms the data
        3. Performs quality checks
        4. Saves the processed data to the silver layer

        Args:
            bronze_data: Optional in-memory data from bronze stage. If provided,
                        this data will be used instead of reading from storage.

        Returns:
            None

        Raises:
            Exception: If there are issues at any step in the process
        """
        try:
            self.log.info("Starting soil types silver layer processing")

            # Read data with support for in-memory passing
            if bronze_data is not None:
                self.log.info("Using bronze data from memory (in-memory data passing)")
                if isinstance(bronze_data, gGeo):
                    raw_data = bronze_data
                else:
                    self.log.error(
                        f"Expected Geo from bronze stage, got {type(bronze_data)}"
                    )
                    return
            else:
                # Fallback to reading from storage
                self.log.info("Reading bronze data from storage (fallback)")
                raw_data = self._read_bronze_data_from_storage(
                    self.config.dataset, self.config.bucket
                )
                if raw_data is None:
                    self.log.error("Failed to read raw data from storage")
                    return

            self.log.info(f"Loaded {len(raw_data):,} records from bronze layer")

            # Validate and transform the data
            processed_data = self._validate_and_transform(raw_data)

            # Perform quality checks
            self._perform_quality_checks(processed_data)

            # Save the processed data using new unified method
            self._save_data(
                data=processed_data,
                dataset=self.config.dataset,
                bucket=self.config.bucket,
                stage="silver",
            )

            self.log.info("Soil types silver layer processing completed successfully")

        except Exception as e:
            self.log.error(f"Failed to process soil types silver layer: {e}")
            raise
