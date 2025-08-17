"""
Geometry validation utilities for the unified pipeline.

This module provides functions for validating and transforming geometries using DuckDB-spatial,
which offers significant performance improvements over GeoPandas for large datasets.
"""

import duckdb

from unified_pipeline.util.log_util import Logger

logger = Logger.get_logger()

# DuckDB-spatial geometry validation utilities


def validate_and_transform_geometries_duckdb(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    dataset_name: str,
    geometry_column: str = "geometry",
) -> None:
    """
    Validate and transform geometries using DuckDB-spatial for optimal performance.

    This function validates geometries in a DuckDB table, fixes invalid ones,
    and transforms them to EPSG:4326. It's designed for high-performance processing
    of large spatial datasets.

    Args:
        conn: DuckDB connection with spatial extension loaded
        table_name: Name of the table containing geometries
        dataset_name: Name of dataset for logging
        geometry_column: Name of the geometry column (default: "geometry")

    Raises:
        Exception: If spatial extension is not available or validation fails
    """
    try:
        logger.info(f"{dataset_name}: Starting DuckDB-spatial geometry validation")

        # Verify spatial extension is loaded
        try:
            conn.execute("SELECT ST_Point(0, 0)")
        except Exception:
            logger.info(f"{dataset_name}: Loading spatial extension")
            conn.execute("INSTALL spatial")
            conn.execute("LOAD spatial")

        initial_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        logger.info(f"{dataset_name}: Initial features: {initial_count}")

        # Check if geometry column exists
        columns = [row[0] for row in conn.execute(f"DESCRIBE {table_name}").fetchall()]
        if geometry_column not in columns:
            logger.error(f"{dataset_name}: Geometry column '{geometry_column}' not found")
            raise ValueError(f"Geometry column '{geometry_column}' not found in table")

        # Convert string geometries to spatial objects if needed
        logger.info(f"{dataset_name}: Converting geometries to spatial objects")

        # Check the actual type of the geometry column
        geom_type_result = conn.execute(f"""
            SELECT DISTINCT typeof({geometry_column}) as geom_type
            FROM {table_name}
            WHERE {geometry_column} IS NOT NULL
            LIMIT 5
        """).fetchall()

        geom_types = [row[0] for row in geom_type_result]
        logger.info(f"{dataset_name}: Geometry column types found: {geom_types}")

        # Only convert if we have VARCHAR geometries
        if "VARCHAR" in geom_types:
            logger.info(f"{dataset_name}: Converting VARCHAR geometries to spatial objects")
            conn.execute(f"""
                UPDATE {table_name} SET
                    {geometry_column} = ST_GeomFromText({geometry_column})
                WHERE {geometry_column} IS NOT NULL
                    AND typeof({geometry_column}) = 'VARCHAR'
            """)
        else:
            logger.info(
                f"{dataset_name}: Geometries are already spatial objects, skipping conversion"
            )

        # Validate geometries and fix invalid ones
        invalid_count = conn.execute(f"""
            SELECT COUNT(*) FROM {table_name}
            WHERE {geometry_column} IS NOT NULL AND NOT ST_IsValid({geometry_column})
        """).fetchone()[0]

        if invalid_count > 0:
            logger.warning(f"{dataset_name}: Found {invalid_count} invalid geometries, fixing...")

            # Try to fix invalid geometries
            try:
                conn.execute(f"""
                    UPDATE {table_name} SET
                        {geometry_column} = ST_MakeValid({geometry_column})
                    WHERE {geometry_column} IS NOT NULL AND NOT ST_IsValid({geometry_column})
                """)

                # Check how many are still invalid
                still_invalid = conn.execute(f"""
                    SELECT COUNT(*) FROM {table_name}
                    WHERE {geometry_column} IS NOT NULL AND NOT ST_IsValid({geometry_column})
                """).fetchone()[0]

                if still_invalid > 0:
                    logger.error(
                        f"{dataset_name}: {still_invalid} geometries remain invalid, removing them"
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
                logger.info(f"{dataset_name}: Removed {invalid_count} invalid geometries")

        # Detect CRS before transformation by checking coordinate bounds
        logger.info(f"{dataset_name}: Detecting coordinate reference system")
        initial_bounds = conn.execute(f"""
            SELECT
                MIN(ST_XMin({geometry_column})) as min_x,
                MAX(ST_XMax({geometry_column})) as max_x,
                MIN(ST_YMin({geometry_column})) as min_y,
                MAX(ST_YMax({geometry_column})) as max_y
            FROM {table_name}
            WHERE {geometry_column} IS NOT NULL
            LIMIT 1000
        """).fetchone()

        if initial_bounds:
            min_x, max_x, min_y, max_y = initial_bounds

            # Check if coordinates are already in WGS84 (longitude/latitude ranges)
            is_wgs84_lon_lat = (
                7 <= min_x <= 16 and 7 <= max_x <= 16 and 54 <= min_y <= 58 and 54 <= max_y <= 58
            )
            is_wgs84_lat_lon = (
                54 <= min_x <= 58 and 54 <= max_x <= 58 and 7 <= min_y <= 16 and 7 <= max_y <= 16
            )
            is_utm = 400000 <= min_x <= 900000 and 6000000 <= min_y <= 7000000

            if is_wgs84_lon_lat:
                logger.info(
                    f"{dataset_name}: Data in WGS84 (LAT, LON) order - CORRECT for "
                    "ST_Area_Spheroid direct usage"
                )
                # COORDINATE APPROACH: Data is correctly in (LAT, LON) EPSG:4326 standard
                # ST_Area_Spheroid expects (LAT, LON) input - no transformation needed
            elif is_wgs84_lat_lon:
                logger.info(
                    f"{dataset_name}: Data in WGS84 but lat/lon order - will flip after processing"
                )
            elif is_utm:
                logger.info(
                    f"{dataset_name}: Data in Danish UTM (EPSG:25832) - transforming to WGS84"
                )
                conn.execute(f"""
                    UPDATE {table_name} SET
                        {geometry_column} = ST_Transform(
                            {geometry_column}, 'EPSG:25832', 'EPSG:4326'
                        )
                    WHERE {geometry_column} IS NOT NULL
                """)
            else:
                logger.warning(
                    f"{dataset_name}: Unknown CRS (X: {min_x:.1f}-{max_x:.1f}, "
                    f"Y: {min_y:.1f}-{max_y:.1f}) - assuming UTM"
                )
                conn.execute(f"""
                    UPDATE {table_name} SET
                        {geometry_column} = ST_Transform(
                            {geometry_column}, 'EPSG:25832', 'EPSG:4326'
                        )
                    WHERE {geometry_column} IS NOT NULL
                """)
        else:
            logger.warning(f"{dataset_name}: Could not detect CRS - assuming UTM and transforming")
            conn.execute(f"""
                UPDATE {table_name} SET
                    {geometry_column} = ST_Transform({geometry_column}, 'EPSG:25832', 'EPSG:4326')
                WHERE {geometry_column} IS NOT NULL
            """)

        # Apply coordinate flipping if needed - check bounds AFTER any transformation
        # Skip if we already flipped coordinates for WGS84 lon/lat data
        already_flipped_for_wgs84 = (
            initial_bounds
            and 7 <= initial_bounds[0] <= 16
            and 7 <= initial_bounds[1] <= 16
            and 54 <= initial_bounds[2] <= 58
            and 54 <= initial_bounds[3] <= 58
        )

        if not already_flipped_for_wgs84:
            post_transform_bounds = conn.execute(f"""
                SELECT
                    MIN(ST_XMin({geometry_column})) as min_x,
                    MAX(ST_XMax({geometry_column})) as max_x,
                    MIN(ST_YMin({geometry_column})) as min_y,
                    MAX(ST_YMax({geometry_column})) as max_y
                FROM {table_name}
                WHERE {geometry_column} IS NOT NULL
                LIMIT 100
            """).fetchone()

            if post_transform_bounds:
                min_x, max_x, min_y, max_y = post_transform_bounds
                is_wgs84_lat_lon_after = (
                    54 <= min_x <= 58
                    and 54 <= max_x <= 58
                    and 7 <= min_y <= 16
                    and 7 <= max_y <= 16
                )

                if is_wgs84_lat_lon_after:
                    logger.info(
                        f"{dataset_name}: Detected (LAT, LON) order - CORRECT EPSG:4326 "
                        "format for direct ST_Area_Spheroid usage"
                    )
                    # COORDINATE APPROACH: Data is correctly in (LAT, LON) EPSG:4326 standard
                    # ST_Area_Spheroid expects (LAT, LON) input - no transformation needed

                # Verify the flip worked
                final_bounds = conn.execute(f"""
                    SELECT
                        MIN(ST_XMin({geometry_column})) as min_x,
                        MAX(ST_XMax({geometry_column})) as max_x,
                        MIN(ST_YMin({geometry_column})) as min_y,
                        MAX(ST_YMax({geometry_column})) as max_y
                    FROM {table_name}
                    WHERE {geometry_column} IS NOT NULL
                    LIMIT 100
                """).fetchone()

                if final_bounds:
                    final_min_x, final_max_x, final_min_y, final_max_y = final_bounds
                    logger.info(
                        f"{dataset_name}: Final coordinates "
                        f"(X: {final_min_x:.3f}-{final_max_x:.3f}, "
                        f"Y: {final_min_y:.3f}-{final_max_y:.3f})"
                    )
            else:
                logger.info(f"{dataset_name}: Coordinates are in correct order")

            # 🔍 COORDINATE ORDER VERIFICATION: Extract raw coordinate pairs
            # to verify actual storage order
            try:
                sample_wkt = conn.execute(f"""
                    SELECT
                        ST_AsText(ST_Centroid({geometry_column})) as wkt_centroid
                    FROM {table_name}
                    WHERE {geometry_column} IS NOT NULL
                    LIMIT 5
                """).fetchall()

                if sample_wkt:
                    logger.info(
                        f"🧭 {dataset_name}: COORDINATE ORDER VERIFICATION - "
                        f"Found {len(sample_wkt)} sample centroids:"
                    )
                    coord_pairs = []
                    for i, (wkt,) in enumerate(sample_wkt[:3]):
                        logger.info(f"   Raw WKT {i+1}: {wkt}")
                        # Extract coordinates from "POINT(x y)" format
                        if wkt and "POINT(" in wkt:
                            coords_str = wkt.replace("POINT(", "").replace(")", "")
                            try:
                                coord_parts = coords_str.split()
                                if len(coord_parts) >= 2:
                                    first_val, second_val = (
                                        float(coord_parts[0]),
                                        float(coord_parts[1]),
                                    )
                                    coord_pairs.append((first_val, second_val))
                                    logger.info(
                                        f"   Sample {i+1}: POINT({first_val:.6f} {second_val:.6f})"
                                    )
                                else:
                                    logger.warning(
                                        f"   Sample {i+1}: Invalid coordinate format: {coords_str}"
                                    )
                            except Exception as parse_e:
                                logger.warning(f"   Sample {i+1}: Parse error: {parse_e}")
                                continue
                        else:
                            logger.warning(f"   Sample {i+1}: Not a POINT geometry: {wkt}")

                    if coord_pairs:
                        logger.info(
                            f"🧭 {dataset_name}: Successfully parsed "
                            f"{len(coord_pairs)} coordinate pairs"
                        )
                        # Analyze the pattern - first value in coordinate pair
                        first_vals = [pair[0] for pair in coord_pairs]
                        second_vals = [pair[1] for pair in coord_pairs]
                        first_range = (min(first_vals), max(first_vals))
                        second_range = (min(second_vals), max(second_vals))

                        logger.info(
                            f"🧭 {dataset_name}: First values range: "
                            f"{first_range[0]:.2f} to {first_range[1]:.2f}"
                        )
                        logger.info(
                            f"🧭 {dataset_name}: Second values range: "
                            f"{second_range[0]:.2f} to {second_range[1]:.2f}"
                        )

                        # Check coordinate order - EPSG:4326 standard is LAT/LON
                        if (
                            54 <= first_range[0] <= 58
                            and 54 <= first_range[1] <= 58
                            and 8 <= second_range[0] <= 15
                            and 8 <= second_range[1] <= 15
                        ):
                            logger.info(
                                f"✅ {dataset_name}: CONFIRMED - Data stored as "
                                f"(LAT, LON) EPSG:4326 standard - "
                                f"({first_range[0]:.2f}-{first_range[1]:.2f}, "
                                f"{second_range[0]:.2f}-{second_range[1]:.2f})"
                            )
                            logger.info(
                                "   → ST_Area_Spheroid can be used directly "
                                "(expects LAT/LON input)"
                            )
                        elif (
                            8 <= first_range[0] <= 15
                            and 8 <= first_range[1] <= 15
                            and 54 <= second_range[0] <= 58
                            and 54 <= second_range[1] <= 58
                        ):
                            logger.warning(
                                f"⚠️ {dataset_name}: ALERT - Data stored as "
                                f"(LON, LAT) GIS convention - "
                                f"({first_range[0]:.2f}-{first_range[1]:.2f}, "
                                f"{second_range[0]:.2f}-{second_range[1]:.2f})"
                            )
                            logger.warning(
                                "   → ST_Area_Spheroid would need ST_FlipCoordinates "
                                "wrapper if data is (LON, LAT)!"
                            )
                        else:
                            logger.warning(
                                f"❓ {dataset_name}: UNCLEAR - Coordinate order unclear - "
                                f"({first_range[0]:.2f}-{first_range[1]:.2f}, "
                                f"{second_range[0]:.2f}-{second_range[1]:.2f})"
                            )
                            logger.warning(
                                "   → Manual verification needed for ST_Area_Spheroid usage"
                            )
                    else:
                        logger.warning(
                            f"🧭 {dataset_name}: No valid coordinate pairs extracted from centroids"
                        )
                else:
                    logger.warning(
                        f"🧭 {dataset_name}: No sample WKT centroids returned from query"
                    )
            except Exception as e:
                logger.warning(f"⚠️ {dataset_name}: Could not verify coordinate order: {e}")
                logger.warning(f"   Exception type: {type(e).__name__}")
                import traceback

                logger.warning(f"   Traceback: {traceback.format_exc()}")

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
        explain_result = conn.execute(f"EXPLAIN {query}").fetchall()
        explain_text = "\n".join([str(row[0]) for row in explain_result])

        spatial_join_detected = "SPATIAL_JOIN" in explain_text

        if spatial_join_detected:
            logger.info("✅ SPATIAL_JOIN operator detected in query plan!")
            # Log a snippet of the plan showing the SPATIAL_JOIN
            for line in explain_text.split("\n"):
                if "SPATIAL_JOIN" in line:
                    logger.info(f"   📍 {line.strip()}")
        else:
            logger.warning("⚠️ SPATIAL_JOIN operator not used - query may use standard join")
            logger.warning(
                "💡 Tip: SPATIAL_JOIN requires simple spatial predicates without "
                "complex calculations in SELECT"
            )
            logger.debug(f"Query plan:\n{explain_text}")

        return spatial_join_detected
    except Exception as e:
        logger.warning(f"Could not verify SPATIAL_JOIN usage: {e}")
        return False
