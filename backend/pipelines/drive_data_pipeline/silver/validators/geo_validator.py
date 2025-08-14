"""Geospatial validator for Silver layer using DuckDB-spatial."""

from typing import Any

import geopandas as gpd
from shapely.validation import explain_validity

# Handle imports for both standalone and package usage
try:
    from ...utils.logging import get_logger
    from ..duckdb_base import DuckDBProcessor
except ImportError:
    # Fallback for standalone usage
    import logging

    def get_logger() -> logging.Logger:
        return logging.getLogger(__name__)

    from silver.duckdb_base import DuckDBProcessor
from .base import BaseValidator, ValidationResult

# Get logger
logger = get_logger()


class GeospatialValidator(BaseValidator, DuckDBProcessor):
    """Validator for geospatial data using DuckDB-spatial."""

    def __init__(
        self,
        geometry_column: str = "geometry",
        target_crs: str = "EPSG:4326",
        validate_geometry: bool = True,
        auto_fix_geometry: bool = True,
        use_duckdb_spatial: bool = True,
    ) -> None:
        """Initialize the geospatial validator.

        Args:
            geometry_column: Name of the geometry column
            target_crs: Target coordinate reference system
            validate_geometry: Whether to validate geometries
            auto_fix_geometry: Whether to attempt to fix invalid geometries
            use_duckdb_spatial: Whether to use DuckDB-spatial (recommended)
        """
        BaseValidator.__init__(self)
        DuckDBProcessor.__init__(self)

        self.geometry_column = geometry_column
        self.target_crs = target_crs
        self.validate_geometry = validate_geometry
        self.auto_fix_geometry = auto_fix_geometry
        self.use_duckdb_spatial = use_duckdb_spatial

        logger.info(f"Initialized GeospatialValidator with DuckDB-spatial: {use_duckdb_spatial}")

    def validate(self, table_name_or_data: Any) -> ValidationResult:
        """Validate geospatial data.

        Args:
            table_name_or_data: DuckDB table name (str) or data to validate

        Returns:
            ValidationResult with validation results
        """
        result = ValidationResult(is_valid=True)

        if self.use_duckdb_spatial:
            return self._validate_with_duckdb_spatial(table_name_or_data, result)
        else:
            return self._validate_with_geopandas(table_name_or_data, result)

    def _validate_with_duckdb_spatial(
        self, table_name_or_data: Any, result: ValidationResult
    ) -> ValidationResult:
        """Validate geospatial data using DuckDB-spatial."""
        try:
            # Handle different input types
            if isinstance(table_name_or_data, str):
                table_name = table_name_or_data
            else:
                # Register data as a table
                table_name = "geo_validation_data"
                self.register_table(table_name_or_data, table_name)

            # Check if geometry column exists
            columns_info = self.conn.execute(f"DESCRIBE {table_name}").fetchall()
            column_names = [col[0] for col in columns_info]

            if self.geometry_column not in column_names:
                self.add_error(result, f"Geometry column '{self.geometry_column}' not found")
                return result

            # Create spatial table if geometry is in WKT format
            spatial_table = f"{table_name}_spatial"
            try:
                # Try to interpret geometry as WKT
                self.conn.execute(f"""
                    CREATE TABLE {spatial_table} AS
                    SELECT
                        * EXCLUDE {self.geometry_column},
                        ST_GeomFromText({self.geometry_column}) as {self.geometry_column}
                    FROM {table_name}
                    WHERE {self.geometry_column} IS NOT NULL AND {self.geometry_column} != ''
                """)
            except Exception:
                # If that fails, assume it's already spatial geometry
                try:
                    self.conn.execute(f"""
                        CREATE TABLE {spatial_table} AS
                        SELECT * FROM {table_name}
                        WHERE {self.geometry_column} IS NOT NULL
                    """)
                except Exception as e:
                    self.add_error(result, f"Failed to create spatial table: {str(e)}")
                    return result

            # Check for null geometries
            null_count = self.conn.execute(f"""
                SELECT COUNT(*) FROM {spatial_table}
                WHERE {self.geometry_column} IS NULL
            """).fetchone()[0]

            total_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            if null_count > 0:
                self.add_warning(result, f"Found {null_count}/{total_count} null geometries")

            # Check for valid geometries
            if self.validate_geometry:
                try:
                    invalid_count = self.conn.execute(f"""
                        SELECT COUNT(*) FROM {spatial_table}
                        WHERE {self.geometry_column} IS NOT NULL
                            AND NOT ST_IsValid({self.geometry_column})
                    """).fetchone()[0]

                    if invalid_count > 0:
                        self.add_error(result, f"Found {invalid_count} invalid geometries")

                        # Get details of first few invalid geometries
                        if invalid_count <= 5:
                            invalid_details = self.conn.execute(f"""
                                SELECT ROW_NUMBER() OVER () as row_num,
                                       ST_AsText({self.geometry_column}) as geom_wkt
                                FROM {spatial_table}
                                WHERE {self.geometry_column} IS NOT NULL
                                    AND NOT ST_IsValid({self.geometry_column})
                                LIMIT 5
                            """).fetchall()

                            for row_num, geom_wkt in invalid_details:
                                try:
                                    from shapely import wkt

                                    geom = wkt.loads(geom_wkt)
                                    reason = explain_validity(geom)
                                    self.add_error(
                                        result, f"Invalid geometry at row {row_num}: {reason}"
                                    )
                                except Exception:
                                    self.add_error(
                                        result,
                                        f"Invalid geometry at row {row_num}: "
                                        f"Could not determine reason",
                                    )
                except Exception as e:
                    self.add_warning(result, f"Could not validate geometries: {str(e)}")

            # Check coordinate system (assume EPSG:25832 for Danish data if not specified)
            valid_geom_count = self.conn.execute(f"""
                SELECT COUNT(*) FROM {spatial_table}
                WHERE {self.geometry_column} IS NOT NULL
            """).fetchone()[0]

            if valid_geom_count > 0:
                self.add_warning(
                    result,
                    f"CRS validation: Assuming geometries are in EPSG:25832, "
                    f"will transform to {self.target_crs}",
                )

            # Clean up temporary table
            self.conn.execute(f"DROP TABLE IF EXISTS {spatial_table}")

            logger.info("DuckDB-spatial validation completed")

        except Exception as e:
            self.add_error(result, f"DuckDB-spatial validation failed: {str(e)}")

        return result

    def _validate_with_geopandas(
        self, table_name_or_data: Any, result: ValidationResult
    ) -> ValidationResult:
        """Validate geospatial data using GeoPandas (legacy method)."""
        logger.warning("Using legacy GeoPandas validation. Consider switching to DuckDB-spatial.")

        try:
            # Convert to GeoDataFrame if needed
            if isinstance(table_name_or_data, str):
                # Export from DuckDB to GeoDataFrame
                df = self.conn.execute(f"SELECT * FROM {table_name_or_data}").df()
                if not self._has_geometry_column(df):
                    self.add_error(result, f"Geometry column '{self.geometry_column}' not found")
                    return result
                data = self._convert_to_geodataframe(df)
            else:
                data = table_name_or_data

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
                self.add_warning(
                    result, f"CRS is {data.crs}, will be reprojected to {self.target_crs}"
                )

            # Validate geometries
            if self.validate_geometry:
                self._validate_geometries(data, result)

        except Exception as e:
            self.add_error(result, f"GeoPandas validation failed: {str(e)}")

        return result

    def standardize(self, table_name_or_data: Any) -> str:
        """Standardize the geospatial data using DuckDB-spatial.

        Args:
            table_name_or_data: DuckDB table name (str) or data to standardize

        Returns:
            DuckDB table name with standardized data
        """
        if self.use_duckdb_spatial:
            return self._standardize_with_duckdb_spatial(table_name_or_data)
        else:
            # Legacy mode - convert to GeoDataFrame, standardize, then back to DuckDB
            gdf = self._standardize_with_geopandas(table_name_or_data)
            result_table = "standardized_geo_data"
            self.register_table(gdf, result_table)
            return result_table

    def _standardize_with_duckdb_spatial(self, table_name_or_data: Any) -> str:
        """Standardize geospatial data using DuckDB-spatial."""
        try:
            # Handle different input types
            if isinstance(table_name_or_data, str):
                source_table = table_name_or_data
            else:
                # Register data as a table
                source_table = "geo_standardization_source"
                self.register_table(table_name_or_data, source_table)

            result_table = f"{source_table}_standardized"

            # Check if geometry column exists
            columns_info = self.conn.execute(f"DESCRIBE {source_table}").fetchall()
            column_names = [col[0] for col in columns_info]

            if self.geometry_column not in column_names:
                raise ValueError(f"Geometry column '{self.geometry_column}' not found")

            # Create spatial table with standardized geometries
            self.conn.execute(f"""
                CREATE TABLE {result_table} AS
                SELECT
                    * EXCLUDE {self.geometry_column},
                    CASE
                        WHEN {self.geometry_column} IS NULL THEN NULL
                        WHEN ST_IsValid(ST_GeomFromText({self.geometry_column})) THEN
                            ST_Transform(
                                ST_GeomFromText({self.geometry_column}), 
                                'EPSG:25832', 
                                '{self.target_crs}'
                            )
                        ELSE
                            -- Try to fix invalid geometries
                            ST_Transform(
                                ST_MakeValid(ST_GeomFromText({self.geometry_column})), 
                                'EPSG:25832', 
                                '{self.target_crs}'
                            )
                    END as {self.geometry_column}
                FROM {source_table}
            """)

            # Validate the result
            valid_count = self.conn.execute(f"""
                SELECT COUNT(*) FROM {result_table}
                WHERE {self.geometry_column} IS NOT NULL
                    AND ST_IsValid({self.geometry_column})
            """).fetchone()[0]

            total_count = self.conn.execute(f"""
                SELECT COUNT(*) FROM {result_table}
                WHERE {self.geometry_column} IS NOT NULL
            """).fetchone()[0]

            logger.info(f"Standardized {valid_count}/{total_count} geometries using DuckDB-spatial")
            return result_table

        except Exception as e:
            logger.error(f"DuckDB-spatial standardization failed: {str(e)}")
            raise

    def _standardize_with_geopandas(self, table_name_or_data: Any) -> gpd.GeoDataFrame:
        """Standardize geospatial data using GeoPandas (legacy method)."""
        logger.warning("Using legacy GeoPandas standardization.")

        try:
            # Convert to GeoDataFrame if needed
            if isinstance(table_name_or_data, str):
                # Export from DuckDB to DataFrame, then convert to GeoDataFrame
                df = self.conn.execute(f"SELECT * FROM {table_name_or_data}").df()
                data = self._convert_to_geodataframe(df)
            else:
                data = table_name_or_data

            if not isinstance(data, gpd.GeoDataFrame):
                data = self._convert_to_geodataframe(data)

            # Fix geometries if requested
            if self.auto_fix_geometry:
                data = self._fix_geometries(data)

            # Reproject to target CRS
            if data.crs is None:
                # Assume EPSG:25832 for Danish data
                data = data.set_crs("EPSG:25832", allow_override=True)
                logger.warning("No CRS defined, assuming EPSG:25832")

            if data.crs != self.target_crs:
                data = data.to_crs(self.target_crs)
                logger.info(f"Reprojected from {data.crs} to {self.target_crs}")

            return data

        except Exception as e:
            logger.error(f"GeoPandas standardization failed: {str(e)}")
            raise

    def _has_geometry_column(self, data: Any) -> bool:
        """Check if data has the specified geometry column.

        Args:
            data: Data to check

        Returns:
            True if geometry column exists, False otherwise
        """
        if hasattr(data, "columns"):
            return self.geometry_column in data.columns
        elif isinstance(data, str):
            # It's a table name - check via DuckDB
            try:
                columns_info = self.conn.execute(f"DESCRIBE {data}").fetchall()
                column_names = [col[0] for col in columns_info]
                return self.geometry_column in column_names
            except Exception:
                return False
        return False

    def _convert_to_geodataframe(self, data: Any) -> gpd.GeoDataFrame:
        """Convert data to GeoDataFrame.

        Args:
            data: Data to convert (DataFrame or similar)

        Returns:
            GeoDataFrame
        """
        import pandas as pd

        if isinstance(data, gpd.GeoDataFrame):
            return data

        if not isinstance(data, pd.DataFrame):
            raise ValueError("Data must be a pandas DataFrame or GeoDataFrame")

        if self.geometry_column not in data.columns:
            raise ValueError(f"Geometry column '{self.geometry_column}' not found")

        # Convert geometry column from WKT to geometry objects
        try:
            from shapely import wkt

            data[self.geometry_column] = data[self.geometry_column].apply(
                lambda x: wkt.loads(x) if x and isinstance(x, str) else x
            )
        except Exception as e:
            logger.warning(f"Failed to convert WKT to geometry: {str(e)}")

        # Create GeoDataFrame
        gdf = gpd.GeoDataFrame(data, geometry=self.geometry_column)
        return gdf

    def _validate_geometries(self, data: gpd.GeoDataFrame, result: ValidationResult) -> None:
        """Validate geometries in a GeoDataFrame.

        Args:
            data: GeoDataFrame to validate
            result: ValidationResult to update
        """
        # Check for invalid geometries
        invalid_mask = ~data.geometry.is_valid
        invalid_count = invalid_mask.sum()

        if invalid_count > 0:
            self.add_error(result, f"Found {invalid_count} invalid geometries")

            # Get details of first few invalid geometries
            invalid_indices = data[invalid_mask].index[:5]
            for idx in invalid_indices:
                try:
                    geom = data.loc[idx, self.geometry_column]
                    reason = explain_validity(geom)
                    self.add_error(result, f"Invalid geometry at row {idx}: {reason}")
                except Exception:
                    self.add_error(
                        result, f"Invalid geometry at row {idx}: Could not determine reason"
                    )

    def _fix_geometries(self, data: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Fix invalid geometries in a GeoDataFrame.

        Args:
            data: GeoDataFrame with potentially invalid geometries

        Returns:
            GeoDataFrame with fixed geometries
        """
        # Make a copy to avoid modifying original
        fixed_data = data.copy()

        # Find invalid geometries
        invalid_mask = ~fixed_data.geometry.is_valid

        if invalid_mask.any():
            logger.info(f"Fixing {invalid_mask.sum()} invalid geometries")

            # Try to fix invalid geometries using buffer(0)
            try:
                fixed_data.loc[invalid_mask, self.geometry_column] = fixed_data.loc[
                    invalid_mask, self.geometry_column
                ].buffer(0)

                # Check if fixing worked
                still_invalid = ~fixed_data.geometry.is_valid
                if still_invalid.any():
                    logger.warning(f"Could not fix {still_invalid.sum()} geometries")
                else:
                    logger.info("Successfully fixed all invalid geometries")

            except Exception as e:
                logger.error(f"Failed to fix geometries: {str(e)}")

        return fixed_data
