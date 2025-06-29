"""Geospatial validator for Silver layer."""

from typing import Any

import geopandas as gpd
import pandas as pd
from shapely.validation import explain_validity

from ...utils.logging import get_logger
from .base import BaseValidator, ValidationResult

# Get logger
logger = get_logger()


class GeospatialValidator(BaseValidator):
    """Validator for geospatial data with both GeoPandas and DuckDB-spatial support."""

    def __init__(
        self,
        geometry_column: str = "geometry",
        target_crs: str = "EPSG:4326",
        validate_geometry: bool = True,
        auto_fix_geometry: bool = True,
        use_duckdb_spatial: bool = True,
    ):
        """Initialize the geospatial validator.

        Args:
            geometry_column: Name of the geometry column
            target_crs: Target coordinate reference system (CRS)
            validate_geometry: Whether to validate geometries
            auto_fix_geometry: Whether to automatically fix invalid geometries
            use_duckdb_spatial: Whether to use DuckDB-spatial (recommended) or GeoPandas
        """
        super().__init__()
        self.geometry_column = geometry_column
        self.target_crs = target_crs
        self.validate_geometry = validate_geometry
        self.auto_fix_geometry = auto_fix_geometry
        self.use_duckdb_spatial = use_duckdb_spatial

        # ✅ MIGRATION: Create long-lived DuckDB connection for spatial operations
        self.conn = None
        if self.use_duckdb_spatial:
            try:
                import duckdb

                self.conn = duckdb.connect()
                self.conn.execute("INSTALL spatial")
                self.conn.execute("LOAD spatial")
                logger.info("✅ DuckDB-spatial connection established for geo validation")
            except Exception as e:
                logger.warning(f"Failed to initialize DuckDB-spatial: {e}")
                self.use_duckdb_spatial = False

    def __del__(self):
        """Clean up DuckDB connection."""
        if hasattr(self, "conn") and self.conn:
            try:
                self.conn.close()
            except:
                pass

    def validate(self, data: Any) -> ValidationResult:
        """Validate the geospatial data.

        Args:
            data: Data to validate (e.g., DataFrame, GeoDataFrame)

        Returns:
            ValidationResult with the result of the validation
        """
        result = ValidationResult(is_valid=True)

        if self.use_duckdb_spatial:
            return self._validate_with_duckdb_spatial(data, result)
        else:
            return self._validate_with_geopandas(data, result)

    def _validate_with_duckdb_spatial(
        self, data: Any, result: ValidationResult
    ) -> ValidationResult:
        """Validate geospatial data using DuckDB-spatial."""
        try:
            # ✅ MIGRATION: Use persistent connection instead of creating new one
            if not self.conn:
                self.add_error(result, "DuckDB-spatial connection not available")
                return result

            conn = self.conn

            # Convert data to format suitable for DuckDB
            if isinstance(data, gpd.GeoDataFrame):
                # Convert GeoDataFrame to DataFrame with WKT geometry
                df_wkt = data.copy()
                df_wkt[f"{self.geometry_column}_wkt"] = data[self.geometry_column].to_wkt()
                df_wkt = df_wkt.drop(self.geometry_column, axis=1)
                conn.register("geo_data", df_wkt)

                # Create spatial table
                conn.execute(f"""
                    CREATE TABLE spatial_data AS
                    SELECT 
                        *,
                        ST_GeomFromText({self.geometry_column}_wkt) as {self.geometry_column}
                    FROM geo_data
                """)
            elif isinstance(data, pd.DataFrame) and self.geometry_column in data.columns:
                # Assume geometry is already in WKT format
                conn.register("geo_data", data)
                conn.execute(f"""
                    CREATE TABLE spatial_data AS
                    SELECT 
                        *,
                        ST_GeomFromText({self.geometry_column}) as geom
                    FROM geo_data
                    WHERE {self.geometry_column} IS NOT NULL
                """)
                self.geometry_column = "geom"
            else:
                self.add_error(
                    result,
                    f"Geometry column '{self.geometry_column}' not found or unsupported data type",
                )
                return result

            # Check for null geometries
            null_count = conn.execute(f"""
                SELECT COUNT(*) FROM spatial_data 
                WHERE {self.geometry_column} IS NULL
            """).fetchone()[0]

            if null_count > 0:
                self.add_warning(result, f"Found {null_count} null geometries")

            # Check for valid geometries
            invalid_count = conn.execute(f"""
                SELECT COUNT(*) FROM spatial_data 
                WHERE {self.geometry_column} IS NOT NULL 
                    AND NOT ST_IsValid({self.geometry_column})
            """).fetchone()[0]

            if invalid_count > 0:
                self.add_error(result, f"Found {invalid_count} invalid geometries")

                # Get details of first few invalid geometries
                if invalid_count <= 5:
                    invalid_details = conn.execute(f"""
                        SELECT ROW_NUMBER() OVER () as row_num,
                               ST_AsText({self.geometry_column}) as geom_wkt
                        FROM spatial_data 
                        WHERE {self.geometry_column} IS NOT NULL 
                            AND NOT ST_IsValid({self.geometry_column})
                        LIMIT 5
                    """).fetchall()

                    for row_num, geom_wkt in invalid_details:
                        try:
                            from shapely import wkt

                            geom = wkt.loads(geom_wkt)
                            reason = explain_validity(geom)
                            self.add_error(result, f"Invalid geometry at row {row_num}: {reason}")
                        except Exception:
                            self.add_error(
                                result,
                                f"Invalid geometry at row {row_num}: Could not determine reason",
                            )

            # Check coordinate system (assume EPSG:25832 for Danish data if not specified)
            total_count = conn.execute("SELECT COUNT(*) FROM spatial_data").fetchone()[0]
            if total_count > 0:
                self.add_warning(
                    result,
                    f"CRS validation: Assuming geometries are in EPSG:25832, will transform to {self.target_crs}",
                )

            # ✅ MIGRATION: Don't close persistent connection
            logger.info("DuckDB-spatial validation completed")

        except Exception as e:
            self.add_error(result, f"DuckDB-spatial validation failed: {str(e)}")

        return result

    def _validate_with_geopandas(self, data: Any, result: ValidationResult) -> ValidationResult:
        """Validate geospatial data using GeoPandas (legacy method)."""
        logger.warning("Using legacy GeoPandas validation. Consider switching to DuckDB-spatial.")

        # Check if the data has a geometry column
        if not self._has_geometry_column(data):
            self.add_error(result, f"Geometry column '{self.geometry_column}' not found")
            return result

        # Convert to GeoDataFrame if needed
        if not isinstance(data, gpd.GeoDataFrame):
            try:
                data = self._convert_to_geodataframe(data)
            except Exception as e:
                self.add_error(result, f"Failed to convert to GeoDataFrame: {str(e)}")
                return result

        # Check coordinate reference system
        if data.crs is None:
            self.add_warning(result, "CRS is not defined, assuming target CRS")
        elif data.crs != self.target_crs:
            self.add_warning(result, f"CRS is {data.crs}, will be reprojected to {self.target_crs}")

        # Validate geometries
        if self.validate_geometry:
            self._validate_geometries(data, result)

        return result

    def standardize(self, data: Any) -> gpd.GeoDataFrame:
        """Standardize the geospatial data.

        Args:
            data: Data to standardize (e.g., DataFrame, GeoDataFrame)

        Returns:
            Standardized GeoDataFrame
        """
        if self.use_duckdb_spatial:
            return self._standardize_with_duckdb_spatial(data)
        else:
            return self._standardize_with_geopandas(data)

    def _standardize_with_duckdb_spatial(self, data: Any) -> gpd.GeoDataFrame:
        """Standardize geospatial data using DuckDB-spatial."""
        try:
            # ✅ MIGRATION: Use persistent connection instead of creating new one
            if not self.conn:
                raise ValueError("DuckDB-spatial connection not available")

            conn = self.conn

            # Convert data to format suitable for DuckDB
            if isinstance(data, gpd.GeoDataFrame):
                df_wkt = data.copy()
                df_wkt[f"{self.geometry_column}_wkt"] = data[self.geometry_column].to_wkt()
                df_wkt = df_wkt.drop(self.geometry_column, axis=1)
                conn.register("geo_data", df_wkt)

                # Create spatial table
                conn.execute(f"""
                    CREATE TABLE spatial_data AS
                    SELECT 
                        *,
                        ST_GeomFromText({self.geometry_column}_wkt) as {self.geometry_column}
                    FROM geo_data
                """)
            else:
                # Assume DataFrame with WKT geometry column
                conn.register("geo_data", data)
                conn.execute(f"""
                    CREATE TABLE spatial_data AS
                    SELECT 
                        *,
                        ST_GeomFromText({self.geometry_column}) as geom
                    FROM geo_data
                    WHERE {self.geometry_column} IS NOT NULL
                """)
                self.geometry_column = "geom"

            # Use the DuckDB-spatial geometry validator
            from unified_pipeline.common.geometry_validator import (
                validate_and_transform_geometries_duckdb,
            )

            validate_and_transform_geometries_duckdb(conn, "spatial_data", "drive_data_validation")

            # Get the result back as DataFrame with WKT geometry
            result_df = conn.execute(f"""
                SELECT 
                    * EXCLUDE {self.geometry_column},
                    ST_AsText({self.geometry_column}) as {self.geometry_column}
                FROM spatial_data
            """).df()

            # ✅ MIGRATION: Don't close persistent connection

            # Convert back to GeoDataFrame
            from shapely import wkt

            result_df[self.geometry_column] = result_df[self.geometry_column].apply(wkt.loads)
            standardized_gdf = gpd.GeoDataFrame(result_df, crs=self.target_crs)

            logger.info("DuckDB-spatial standardization completed")
            return standardized_gdf

        except Exception as e:
            logger.error(f"DuckDB-spatial standardization failed: {e}")
            # Fallback to GeoPandas
            logger.info("Falling back to GeoPandas standardization")
            return self._standardize_with_geopandas(data)

    def _standardize_with_geopandas(self, data: Any) -> gpd.GeoDataFrame:
        """Standardize geospatial data using GeoPandas (legacy method)."""
        logger.warning(
            "Using legacy GeoPandas standardization. Consider switching to DuckDB-spatial."
        )

        # Convert to GeoDataFrame if needed
        if not isinstance(data, gpd.GeoDataFrame):
            data = self._convert_to_geodataframe(data)

        # Reproject to target CRS if needed
        if data.crs is None:
            data.crs = self.target_crs
            logger.info(f"Set CRS to {self.target_crs}")
        elif data.crs != self.target_crs:
            data = data.to_crs(self.target_crs)
            logger.info(f"Reprojected data to {self.target_crs}")

        # Fix invalid geometries if needed
        if self.validate_geometry and self.auto_fix_geometry:
            data = self._fix_geometries(data)

        return data

    def _has_geometry_column(self, data: Any) -> bool:
        """Check if the data has a geometry column.

        Args:
            data: Data to check

        Returns:
            True if the data has a geometry column, False otherwise
        """
        if isinstance(data, gpd.GeoDataFrame):
            return True

        if hasattr(data, "columns"):
            return self.geometry_column in data.columns

        return False

    def _convert_to_geodataframe(self, data: Any) -> gpd.GeoDataFrame:
        """Convert data to a GeoDataFrame.

        Args:
            data: Data to convert

        Returns:
            GeoDataFrame
        """
        if isinstance(data, pd.DataFrame):
            # Convert a pandas DataFrame to a GeoDataFrame
            return gpd.GeoDataFrame(data, geometry=self.geometry_column, crs=self.target_crs)

        raise ValueError(f"Cannot convert {type(data)} to GeoDataFrame")

    def _validate_geometries(self, data: gpd.GeoDataFrame, result: ValidationResult):
        """Validate geometries in a GeoDataFrame.

        Args:
            data: GeoDataFrame to validate
            result: ValidationResult to update
        """
        # Check for null geometries
        null_geoms = data.geometry.isna().sum()
        if null_geoms > 0:
            self.add_warning(result, f"Found {null_geoms} null geometries")

        # Check for valid geometries
        invalid_geoms = 0
        for i, geom in enumerate(data.geometry):
            if geom is not None and not geom.is_valid:
                invalid_geoms += 1
                if invalid_geoms <= 5:  # Limit detailed reporting
                    reason = explain_validity(geom)
                    self.add_error(result, f"Invalid geometry at index {i}: {reason}")

        if invalid_geoms > 5:
            self.add_error(result, f"Found {invalid_geoms} invalid geometries in total")

    def _fix_geometries(self, data: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Fix invalid geometries in a GeoDataFrame.

        Args:
            data: GeoDataFrame to fix

        Returns:
            GeoDataFrame with fixed geometries
        """
        # Make a copy to avoid modifying the original
        fixed_data = data.copy()

        # Use buffer(0) to fix common issues like self-intersections
        invalid_mask = ~fixed_data.geometry.is_valid
        if invalid_mask.any():
            invalid_count = invalid_mask.sum()
            logger.info(f"Fixing {invalid_count} invalid geometries")

            # Apply buffer(0) to fix geometries
            fixed_data.loc[invalid_mask, "geometry"] = fixed_data.loc[
                invalid_mask, "geometry"
            ].buffer(0)

            # Check if any geometries are still invalid
            still_invalid = ~fixed_data.geometry.is_valid
            if still_invalid.any():
                logger.warning(
                    f"{still_invalid.sum()} geometries remain invalid after attempting to fix"
                )

        return fixed_data
