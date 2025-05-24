import geopandas as gpd
import logging

logger = logging.getLogger(__name__)

def validate_and_transform_geometries(gdf: gpd.GeoDataFrame, dataset_name: str) -> gpd.GeoDataFrame:
    """
    Validates and transforms geometries for BigQuery compatibility.
    
    This function performs cleanup operations to ensure geometries are valid
    and meet BigQuery's requirements. All operations are performed in UTM zone 32N (EPSG:25832)
    where possible to maintain geometric precision for Danish data.
    
    The process:
    1. Converts to UTM (EPSG:25832)
    2. Cleans geometries with buffer(0) in UTM
    3. Converts to WGS84 (EPSG:4326) for BigQuery
    4. Final cleanup and validation in WGS84
    
    Args:
        gdf: GeoDataFrame with geometries in any CRS
        dataset_name: Name of dataset for logging
    
    Returns:
        GeoDataFrame with valid geometries in EPSG:4326
        
    Raises:
        ValueError: If geometries cannot be made valid
    """
    try:
        initial_count = len(gdf)
        logger.info(f"{dataset_name}: Starting validation with {initial_count} features")
        logger.info(f"{dataset_name}: Input CRS: {gdf.crs}")
        
        # Convert to UTM
        if gdf.crs != "EPSG:25832":
            logger.info(f"{dataset_name}: Converting to UTM (EPSG:25832) for better precision")
            gdf = gdf.to_crs("EPSG:25832")
        
        # Initial cleanup in UTM
        logger.info(f"{dataset_name}: Performing initial cleanup")
        gdf.geometry = gdf.geometry.apply(lambda g: g.buffer(0))
        
        # Validate in UTM
        invalid_mask = ~gdf.geometry.is_valid
        if invalid_mask.any():
            logger.warning(f"{dataset_name}: Found {invalid_mask.sum()} invalid geometries after cleanup")
            raise ValueError(f"Found {invalid_mask.sum()} invalid geometries after cleanup")
        
        # Convert to WGS84
        logger.info(f"{dataset_name}: Converting to WGS84 (EPSG:4326)")
        gdf = gdf.to_crs("EPSG:4326")
        
        # Final cleanup in WGS84
        gdf.geometry = gdf.geometry.apply(lambda g: g.buffer(0))
        
        # Final validation
        invalid_wgs84 = ~gdf.geometry.is_valid
        if invalid_wgs84.any():
            raise ValueError(f"Found {invalid_wgs84.sum()} invalid geometries after WGS84 conversion")
        
        # Check for self-intersections
        self_intersecting = ~gdf.geometry.is_simple
        if self_intersecting.any():
            logger.warning(f"{dataset_name}: Found {self_intersecting.sum()} self-intersecting geometries in WGS84")
            raise ValueError(f"Found {self_intersecting.sum()} self-intersecting geometries")
        
        # Remove nulls and empty geometries
        gdf = gdf.dropna(subset=['geometry'])
        gdf = gdf[~gdf.geometry.is_empty]
        
        final_count = len(gdf)
        removed_count = initial_count - final_count
        
        logger.info(f"{dataset_name}: Validation complete")
        logger.info(f"{dataset_name}: Initial features: {initial_count}")
        logger.info(f"{dataset_name}: Valid features: {final_count}")
        logger.info(f"{dataset_name}: Removed features: {removed_count}")
        logger.info(f"{dataset_name}: Output CRS: {gdf.crs}")
        
        return gdf
        
    except Exception as e:
        logger.error(f"{dataset_name}: Error in geometry validation: {str(e)}")
        raise