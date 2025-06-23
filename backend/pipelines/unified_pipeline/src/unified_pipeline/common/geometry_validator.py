import logging

import geopandas as gpd

logger = logging.getLogger(__name__)


def validate_and_transform_geometries(gdf: gpd.GeoDataFrame, dataset_name: str) -> gpd.GeoDataFrame:
    """
    Validates and transforms geometries to EPSG:4326 for BigQuery compatibility.

    This function performs cleanup operations to ensure geometries are valid
    and standardizes all data to EPSG:4326 (WGS84) as required by the silver layer.

    The process:
    1. Clean geometries with buffer(0) in original CRS
    2. Convert to WGS84 (EPSG:4326) if not already
    3. Final validation in WGS84

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

        # Clean geometries in original CRS first
        logger.info(f"{dataset_name}: Cleaning geometries in original CRS")
        gdf.geometry = gdf.geometry.apply(lambda g: g.buffer(0) if g is not None else g)

        # Validate in original CRS
        invalid_mask = ~gdf.geometry.is_valid
        if invalid_mask.any():
            logger.warning(
                f"{dataset_name}: Found {invalid_mask.sum()} invalid geometries after cleanup"
            )
            # Try to fix invalid geometries
            from shapely.ops import make_valid

            gdf.loc[invalid_mask, "geometry"] = gdf.loc[invalid_mask, "geometry"].apply(make_valid)

            # Check again
            still_invalid = ~gdf.geometry.is_valid
            if still_invalid.any():
                logger.error(
                    f"{dataset_name}: {still_invalid.sum()} geometries remain invalid after make_valid"
                )
                # Remove invalid geometries rather than failing
                gdf = gdf[gdf.geometry.is_valid]
                logger.info(f"{dataset_name}: Removed {still_invalid.sum()} invalid geometries")

        # Convert to WGS84 if not already
        if gdf.crs != "EPSG:4326":
            logger.info(f"{dataset_name}: Converting to WGS84 (EPSG:4326)")
            gdf = gdf.to_crs("EPSG:4326")
        else:
            logger.info(f"{dataset_name}: Already in EPSG:4326, no conversion needed")

        # Final validation in WGS84
        invalid_wgs84 = ~gdf.geometry.is_valid
        if invalid_wgs84.any():
            logger.warning(
                f"{dataset_name}: Found {invalid_wgs84.sum()} invalid geometries after WGS84 conversion"
            )
            # Try to fix again
            from shapely.ops import make_valid

            gdf.loc[invalid_wgs84, "geometry"] = gdf.loc[invalid_wgs84, "geometry"].apply(
                make_valid
            )

            # Final check
            final_invalid = ~gdf.geometry.is_valid
            if final_invalid.any():
                logger.error(
                    f"{dataset_name}: {final_invalid.sum()} geometries remain invalid, removing them"
                )
                gdf = gdf[gdf.geometry.is_valid]

        # Remove nulls and empty geometries
        gdf = gdf.dropna(subset=["geometry"])
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
