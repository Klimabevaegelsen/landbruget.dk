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
    """Validator for geospatial data."""

    def __init__(
        self,
        geometry_column: str = "geometry",
        target_crs: str = "EPSG:4326",
        validate_geometry: bool = True,
        auto_fix_geometry: bool = True,
    ):
        """Initialize the geospatial validator.

        Args:
            geometry_column: Name of the geometry column
            target_crs: Target coordinate reference system (CRS)
            validate_geometry: Whether to validate geometries
            auto_fix_geometry: Whether to automatically fix invalid geometries
        """
        super().__init__()
        self.geometry_column = geometry_column
        self.target_crs = target_crs
        self.validate_geometry = validate_geometry
        self.auto_fix_geometry = auto_fix_geometry
    
    def validate(self, data: Any) -> ValidationResult:
        """Validate the geospatial data.

        Args:
            data: Data to validate (e.g., DataFrame, GeoDataFrame)

        Returns:
            ValidationResult with the result of the validation
        """
        result = ValidationResult(is_valid=True)
        
        # Check if the data has a geometry column
        if not self._has_geometry_column(data):
            self.add_error(
                result, 
                f"Geometry column '{self.geometry_column}' not found"
            )
            return result
        
        # Convert to GeoDataFrame if needed
        if not isinstance(data, gpd.GeoDataFrame):
            try:
                data = self._convert_to_geodataframe(data)
            except Exception as e:
                self.add_error(
                    result,
                    f"Failed to convert to GeoDataFrame: {str(e)}"
                )
                return result
        
        # Check coordinate reference system
        if data.crs is None:
            self.add_warning(
                result, "CRS is not defined, assuming target CRS"
            )
        elif data.crs != self.target_crs:
            self.add_warning(
                result, 
                f"CRS is {data.crs}, will be reprojected to {self.target_crs}"
            )
        
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
        
        if hasattr(data, 'columns'):
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
            return gpd.GeoDataFrame(
                data, geometry=self.geometry_column, crs=self.target_crs
            )
        
        raise ValueError(f"Cannot convert {type(data)} to GeoDataFrame")
    
    def _validate_geometries(
        self, data: gpd.GeoDataFrame, result: ValidationResult
    ):
        """Validate geometries in a GeoDataFrame.

        Args:
            data: GeoDataFrame to validate
            result: ValidationResult to update
        """
        # Check for null geometries
        null_geoms = data.geometry.isna().sum()
        if null_geoms > 0:
            self.add_warning(
                result, f"Found {null_geoms} null geometries"
            )
        
        # Check for valid geometries
        invalid_geoms = 0
        for i, geom in enumerate(data.geometry):
            if geom is not None and not geom.is_valid:
                invalid_geoms += 1
                if invalid_geoms <= 5:  # Limit detailed reporting
                    reason = explain_validity(geom)
                    self.add_error(
                        result,
                        f"Invalid geometry at index {i}: {reason}"
                    )
        
        if invalid_geoms > 5:
            self.add_error(
                result, 
                f"Found {invalid_geoms} invalid geometries in total"
            )
    
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
            fixed_data.loc[invalid_mask, 'geometry'] = fixed_data.loc[
                invalid_mask, 'geometry'
            ].buffer(0)
            
            # Check if any geometries are still invalid
            still_invalid = ~fixed_data.geometry.is_valid
            if still_invalid.any():
                logger.warning(
                    f"{still_invalid.sum()} geometries remain invalid "
                    "after attempting to fix"
                )
        
        return fixed_data 