import logging

import duckdb

logger = logging.getLogger(__name__)


def validate_and_transform_geometries_duckdb(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    dataset_name: str,
    geometry_column: str = "geometry",
) -> None:
    """
    Validates and transforms geometries to EPSG:4326 using DuckDB-spatial.

    This function performs cleanup operations to ensure geometries are valid
    and standardizes all data to EPSG:4326 (WGS84) as required by the silver layer.

    The process:
    1. Clean geometries with ST_Buffer(0) in original CRS
    2. Convert to WGS84 (EPSG:4326) if not already
    3. Final validation in WGS84

    Args:
        conn: DuckDB connection with spatial extension loaded
        table_name: Name of the table containing geometries
        dataset_name: Name of dataset for logging
        geometry_column: Name of the geometry column (default: "geometry")

    Raises:
        ValueError: If geometries cannot be made valid
    """
    try:
        # Get initial count and CRS info
        initial_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        logger.info(f"{dataset_name}: Starting validation with {initial_count} features")

        # Check current CRS - assume EPSG:25832 if not specified
        # Note: DuckDB-spatial doesn't store CRS metadata, so we assume Danish data is in EPSG:25832
        logger.info(f"{dataset_name}: Assuming input CRS: EPSG:25832 (Danish UTM)")

        # Clean geometries in original CRS first using ST_Buffer(0)
        logger.info(f"{dataset_name}: Cleaning geometries in original CRS")
        conn.execute(f"""
            UPDATE {table_name} SET 
                {geometry_column} = ST_Buffer({geometry_column}, 0)
            WHERE {geometry_column} IS NOT NULL
        """)

        # Validate in original CRS
        invalid_count = conn.execute(f"""
            SELECT COUNT(*) FROM {table_name} 
            WHERE {geometry_column} IS NOT NULL AND NOT ST_IsValid({geometry_column})
        """).fetchone()[0]

        if invalid_count > 0:
            logger.warning(
                f"{dataset_name}: Found {invalid_count} invalid geometries after cleanup"
            )

            # Try to fix invalid geometries using ST_MakeValid (if available) or remove them
            try:
                conn.execute(f"""
                    UPDATE {table_name} SET 
                        {geometry_column} = ST_MakeValid({geometry_column})
                    WHERE {geometry_column} IS NOT NULL AND NOT ST_IsValid({geometry_column})
                """)

                # Check if any are still invalid
                still_invalid = conn.execute(f"""
                    SELECT COUNT(*) FROM {table_name} 
                    WHERE {geometry_column} IS NOT NULL AND NOT ST_IsValid({geometry_column})
                """).fetchone()[0]

                if still_invalid > 0:
                    logger.error(
                        f"{dataset_name}: {still_invalid} geometries remain invalid after ST_MakeValid"
                    )
                    # Remove invalid geometries rather than failing
                    conn.execute(f"""
                        DELETE FROM {table_name} 
                        WHERE {geometry_column} IS NOT NULL AND NOT ST_IsValid({geometry_column})
                    """)
                    logger.info(f"{dataset_name}: Removed {still_invalid} invalid geometries")

            except Exception as make_valid_error:
                logger.warning(
                    f"{dataset_name}: ST_MakeValid not available, removing invalid geometries: {make_valid_error}"
                )
                # Remove invalid geometries if ST_MakeValid is not available
                conn.execute(f"""
                    DELETE FROM {table_name} 
                    WHERE {geometry_column} IS NOT NULL AND NOT ST_IsValid({geometry_column})
                """)
                logger.info(f"{dataset_name}: Removed {invalid_count} invalid geometries")

        # Convert to WGS84 (EPSG:4326) - assume source is EPSG:25832
        logger.info(f"{dataset_name}: Converting to WGS84 (EPSG:4326)")
        conn.execute(f"""
            UPDATE {table_name} SET 
                {geometry_column} = ST_Transform({geometry_column}, 'EPSG:25832', 'EPSG:4326')
            WHERE {geometry_column} IS NOT NULL
        """)

        # Final validation in WGS84
        invalid_wgs84 = conn.execute(f"""
            SELECT COUNT(*) FROM {table_name} 
            WHERE {geometry_column} IS NOT NULL AND NOT ST_IsValid({geometry_column})
        """).fetchone()[0]

        if invalid_wgs84 > 0:
            logger.warning(
                f"{dataset_name}: Found {invalid_wgs84} invalid geometries after WGS84 conversion"
            )

            # Try to fix again
            try:
                conn.execute(f"""
                    UPDATE {table_name} SET 
                        {geometry_column} = ST_MakeValid({geometry_column})
                    WHERE {geometry_column} IS NOT NULL AND NOT ST_IsValid({geometry_column})
                """)

                final_invalid = conn.execute(f"""
                    SELECT COUNT(*) FROM {table_name} 
                    WHERE {geometry_column} IS NOT NULL AND NOT ST_IsValid({geometry_column})
                """).fetchone()[0]

                if final_invalid > 0:
                    logger.error(
                        f"{dataset_name}: {final_invalid} geometries remain invalid, removing them"
                    )
                    conn.execute(f"""
                        DELETE FROM {table_name} 
                        WHERE {geometry_column} IS NOT NULL AND NOT ST_IsValid({geometry_column})
                    """)

            except Exception:
                # Remove invalid geometries if ST_MakeValid fails
                conn.execute(f"""
                    DELETE FROM {table_name} 
                    WHERE {geometry_column} IS NOT NULL AND NOT ST_IsValid({geometry_column})
                """)
                logger.info(f"{dataset_name}: Removed {invalid_wgs84} invalid geometries")

        # Remove null and empty geometries
        conn.execute(f"""
            DELETE FROM {table_name} 
            WHERE {geometry_column} IS NULL OR ST_IsEmpty({geometry_column})
        """)

        final_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        removed_count = initial_count - final_count

        logger.info(f"{dataset_name}: Validation complete")
        logger.info(f"{dataset_name}: Initial features: {initial_count}")
        logger.info(f"{dataset_name}: Valid features: {final_count}")
        logger.info(f"{dataset_name}: Removed features: {removed_count}")
        logger.info(f"{dataset_name}: Output CRS: EPSG:4326")

    except Exception as e:
        logger.error(f"{dataset_name}: Error in geometry validation: {str(e)}")
        raise


def verify_spatial_join_usage(conn: duckdb.DuckDBPyConnection, query: str) -> bool:
    """
    Verify that SPATIAL_JOIN operator is being used in a query.

    Args:
        conn: DuckDB connection
        query: SQL query to analyze

    Returns:
        True if SPATIAL_JOIN operator is detected, False otherwise
    """
    try:
        explain_result = conn.execute(f"EXPLAIN {query}").fetchdf()
        spatial_join_detected = any(
            "SPATIAL_JOIN" in str(row) for row in explain_result.values.flatten()
        )

        if spatial_join_detected:
            logger.info("✅ SPATIAL_JOIN operator detected in query plan!")
        else:
            logger.warning("⚠️ SPATIAL_JOIN operator not used - check query structure")

        return spatial_join_detected
    except Exception as e:
        logger.warning(f"Could not verify SPATIAL_JOIN usage: {e}")
        return False


# Keep the original function for backward compatibility during migration


def validate_and_transform_geometries(gdf: gGeo, dataset_name: str) -> gGeo:
    """
    DEPRECATED: GeoPandas-based geometry validation.

    This function is kept for backward compatibility during the migration to DuckDB-spatial.
    New code should use validate_and_transform_geometries_duckdb() instead.

    Args:
        gdf: Geo with geometries in any CRS
        dataset_name: Name of dataset for logging

    Returns:
        Geo with valid geometries in EPSG:4326

    Raises:
        ValueError: If geometries cannot be made valid
    """
    logger.warning(
        f"{dataset_name}: Using deprecated GeoPandas geometry validation. Consider migrating to DuckDB-spatial."
    )

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
