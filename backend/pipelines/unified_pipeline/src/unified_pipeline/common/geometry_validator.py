"""
Geometry validation utilities for the unified pipeline.

This module provides functions for validating and transforming geometries using DuckDB-spatial,
which offers significant performance improvements over GeoPandas for large datasets.

CRS Strategy (Optimal):
- Process in EPSG:25832 throughout Bronze/Silver/Gold layers
- Transform to EPSG:4326 only at final Supabase upload
- This eliminates unnecessary back-and-forth transforms and allows buffer/distance
  operations to work directly in meters without transformation
"""

import duckdb

from unified_pipeline.util.log_util import Logger

logger = Logger.get_logger()

# Import CRS constants from centralized module
try:
    from common.crs_utils import (
        DANISH_UTM,
        WGS84,
        TARGET_CRS_PROCESSING,
        sql_transform_to_processing_crs,
    )
except ImportError:
    # Fallback if common module not available
    DANISH_UTM = "EPSG:25832"
    WGS84 = "EPSG:4326"
    TARGET_CRS_PROCESSING = DANISH_UTM

    def sql_transform_to_processing_crs(geometry_expr: str, source_crs: str) -> str:
        if source_crs == DANISH_UTM:
            return geometry_expr
        return f"ST_Transform({geometry_expr}, '{source_crs}', '{DANISH_UTM}')"


# DuckDB-spatial geometry validation utilities


def validate_and_transform_geometries_duckdb(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    dataset_name: str,
    geometry_column: str = "geometry",
    target_crs: str = "EPSG:4326",
) -> None:
    """
    Validate and transform geometries using DuckDB-spatial for optimal performance.

    This function validates geometries in a DuckDB table, fixes invalid ones,
    and optionally transforms them to the target CRS.

    CRS Strategy:
    - target_crs="EPSG:25832" (recommended): Keep in Danish UTM for processing.
      Buffer/distance operations work directly in meters.
    - target_crs="EPSG:4326" (default for backward compatibility): Transform to WGS84.
      Use this only for final Supabase upload or legacy compatibility.

    Args:
        conn: DuckDB connection with spatial extension loaded
        table_name: Name of the table containing geometries
        dataset_name: Name of dataset for logging
        geometry_column: Name of the geometry column (default: "geometry")
        target_crs: Target CRS - "EPSG:25832" for processing, "EPSG:4326" for storage
                   (default: "EPSG:4326" for backward compatibility)

    Raises:
        Exception: If spatial extension is not available or validation fails
    """
    # If target is EPSG:25832, delegate to the UTM-specific function
    if target_crs == "EPSG:25832":
        return validate_and_normalize_to_utm(conn, table_name, dataset_name, geometry_column)
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

        # Handle empty tables gracefully
        if initial_count == 0:
            logger.info(f"{dataset_name}: Table is empty, nothing to validate")
            return

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
            # Get all column names for rebuilding the table
            all_columns = [row[0] for row in conn.execute(f"DESCRIBE {table_name}").fetchall()]
            other_columns = [c for c in all_columns if c != geometry_column]

            # Build column selection for non-geometry columns
            other_cols_str = ", ".join(other_columns) if other_columns else ""

            # Recreate table with proper geometry type by using CTAS
            # This is necessary because UPDATE doesn't change the column type
            conn.execute(f"""
                CREATE OR REPLACE TABLE {table_name}_converted AS
                SELECT
                    {other_cols_str + ", " if other_cols_str else ""}
                    ST_GeomFromText({geometry_column}) as {geometry_column}
                FROM {table_name}
                WHERE {geometry_column} IS NOT NULL
            """)

            # Drop original and rename
            conn.execute(f"DROP TABLE {table_name}")
            conn.execute(f"ALTER TABLE {table_name}_converted RENAME TO {table_name}")
            logger.info(f"{dataset_name}: Converted VARCHAR to spatial geometry type")
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
            # Expanded ranges to handle edge cases and neighboring countries
            is_wgs84_lon_lat = (
                3 <= min_x <= 17 and 3 <= max_x <= 17 and 53 <= min_y <= 59 and 53 <= max_y <= 59
            )
            is_wgs84_lat_lon = (
                53 <= min_x <= 59 and 53 <= max_x <= 59 and 3 <= min_y <= 17 and 3 <= max_y <= 17
            )
            is_utm = 400000 <= min_x <= 900000 and 6000000 <= min_y <= 7000000

            if is_wgs84_lon_lat:
                logger.info(
                    f"{dataset_name}: Data in WGS84 (LON, LAT) order - CORRECT for "
                    "PostGIS EPSG:4326 standard"
                )
                # No transformation needed - coordinates are already in PostGIS standard order
            elif is_wgs84_lat_lon:
                logger.info(
                    f"{dataset_name}: Data in WGS84 (LAT, LON) order - standard WGS84 format, "
                    "will need coordinate flip for PostGIS during migration"
                )
                # Keep in WGS84 standard (LAT, LON) format
                # Migration system will handle PostGIS conversion
            elif is_utm:
                logger.info(
                    f"{dataset_name}: Data in Danish UTM (EPSG:25832) - transforming to WGS84"
                )
                conn.execute(f"""
                    UPDATE {table_name} SET
                        {geometry_column} = ST_Transform(
                            {geometry_column}, 'EPSG:25832', 'EPSG:4326', always_xy := true
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
                            {geometry_column}, 'EPSG:25832', 'EPSG:4326', always_xy := true
                        )
                    WHERE {geometry_column} IS NOT NULL
                """)
        else:
            logger.warning(f"{dataset_name}: Could not detect CRS - assuming UTM and transforming")
            conn.execute(f"""
                UPDATE {table_name} SET
                    {geometry_column} = ST_Transform(
                        {geometry_column}, 'EPSG:25832', 'EPSG:4326', always_xy := true
                    )
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
                        logger.info(f"   Raw WKT {i + 1}: {wkt}")
                        # Extract coordinates from "POINT(x y)" or "POINT (x y)" format
                        if wkt and ("POINT(" in wkt or "POINT (" in wkt):
                            # Handle both "POINT(x y)" and "POINT (x y)" formats
                            coords_str = (
                                wkt.replace("POINT(", "")
                                .replace("POINT (", "")
                                .replace(")", "")
                                .strip()
                            )
                            try:
                                coord_parts = coords_str.split()
                                if len(coord_parts) >= 2:
                                    first_val, second_val = (
                                        float(coord_parts[0]),
                                        float(coord_parts[1]),
                                    )
                                    coord_pairs.append((first_val, second_val))
                                    logger.info(
                                        f"   Sample {i + 1}: POINT({first_val:.6f} {second_val:.6f})"
                                    )
                                else:
                                    logger.warning(
                                        f"   Sample {i + 1}: Invalid coordinate format: {coords_str}"
                                    )
                            except Exception as parse_e:
                                logger.warning(f"   Sample {i + 1}: Parse error: {parse_e}")
                                continue
                        else:
                            logger.warning(f"   Sample {i + 1}: Not a POINT geometry: {wkt}")

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
                        # Expanded ranges to match the updated CRS detection ranges
                        if (
                            53 <= first_range[0] <= 59
                            and 53 <= first_range[1] <= 59
                            and 3 <= second_range[0] <= 17
                            and 3 <= second_range[1] <= 17
                        ):
                            logger.info(
                                f"✅ {dataset_name}: CONFIRMED - Data stored as "
                                f"(LAT, LON) EPSG:4326 standard - "
                                f"({first_range[0]:.2f}-{first_range[1]:.2f}, "
                                f"{second_range[0]:.2f}-{second_range[1]:.2f})"
                            )
                            logger.info(
                                "   → ST_Area_Spheroid can be used directly (expects LAT/LON input)"
                            )
                        elif (
                            3 <= first_range[0] <= 17
                            and 3 <= first_range[1] <= 17
                            and 53 <= second_range[0] <= 59
                            and 53 <= second_range[1] <= 59
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
        logger.error(f"{dataset_name}: Error in geometry validation: {e!s}")
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


def validate_and_normalize_to_utm(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    dataset_name: str,
    geometry_column: str = "geometry",
) -> None:
    """
    Validate geometries and normalize to EPSG:25832 (Danish UTM) for processing.

    This function follows the OPTIMAL CRS strategy:
    - Keep data in EPSG:25832 throughout Bronze/Silver/Gold layers
    - Only transform to EPSG:4326 at final Supabase upload (handled separately)

    Benefits of this approach:
    - Buffer/distance operations work directly in meters (no transform needed)
    - Area calculations work directly in square meters
    - Fewer CRS transformations = better performance and fewer bugs

    Args:
        conn: DuckDB connection with spatial extension loaded
        table_name: Name of the table containing geometries
        dataset_name: Name of dataset for logging
        geometry_column: Name of the geometry column (default: "geometry")

    Raises:
        Exception: If spatial extension is not available or validation fails
    """
    try:
        logger.info(f"{dataset_name}: Starting geometry validation (target CRS: EPSG:25832)")

        # Verify spatial extension is loaded
        try:
            conn.execute("SELECT ST_Point(0, 0)")
        except Exception:
            logger.info(f"{dataset_name}: Loading spatial extension")
            conn.execute("INSTALL spatial")
            conn.execute("LOAD spatial")

        initial_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        logger.info(f"{dataset_name}: Initial features: {initial_count}")

        # Handle empty tables gracefully
        if initial_count == 0:
            logger.info(f"{dataset_name}: Table is empty, nothing to validate")
            return

        # Check if geometry column exists
        columns = [row[0] for row in conn.execute(f"DESCRIBE {table_name}").fetchall()]
        if geometry_column not in columns:
            logger.error(f"{dataset_name}: Geometry column '{geometry_column}' not found")
            raise ValueError(f"Geometry column '{geometry_column}' not found in table")

        # Convert string geometries to spatial objects if needed
        geom_type_result = conn.execute(f"""
            SELECT DISTINCT typeof({geometry_column}) as geom_type
            FROM {table_name}
            WHERE {geometry_column} IS NOT NULL
            LIMIT 5
        """).fetchall()

        geom_types = [row[0] for row in geom_type_result]
        logger.info(f"{dataset_name}: Geometry column types found: {geom_types}")

        if "VARCHAR" in geom_types:
            logger.info(f"{dataset_name}: Converting VARCHAR geometries to spatial objects")
            # Get all column names for rebuilding the table
            all_columns = [row[0] for row in conn.execute(f"DESCRIBE {table_name}").fetchall()]
            other_columns = [c for c in all_columns if c != geometry_column]

            # Build column selection for non-geometry columns
            other_cols_str = ", ".join(other_columns) if other_columns else ""

            # Recreate table with proper geometry type by using CTAS
            # This is necessary because UPDATE doesn't change the column type
            conn.execute(f"""
                CREATE OR REPLACE TABLE {table_name}_converted AS
                SELECT
                    {other_cols_str + ", " if other_cols_str else ""}
                    ST_GeomFromText({geometry_column}) as {geometry_column}
                FROM {table_name}
                WHERE {geometry_column} IS NOT NULL
            """)

            # Drop original and rename
            conn.execute(f"DROP TABLE {table_name}")
            conn.execute(f"ALTER TABLE {table_name}_converted RENAME TO {table_name}")
            logger.info(f"{dataset_name}: Converted VARCHAR to spatial geometry type")

        # Validate and fix invalid geometries
        invalid_count = conn.execute(f"""
            SELECT COUNT(*) FROM {table_name}
            WHERE {geometry_column} IS NOT NULL AND NOT ST_IsValid({geometry_column})
        """).fetchone()[0]

        if invalid_count > 0:
            logger.warning(f"{dataset_name}: Found {invalid_count} invalid geometries, fixing...")
            try:
                conn.execute(f"""
                    UPDATE {table_name} SET
                        {geometry_column} = ST_MakeValid({geometry_column})
                    WHERE {geometry_column} IS NOT NULL AND NOT ST_IsValid({geometry_column})
                """)

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
                conn.execute(f"""
                    DELETE FROM {table_name}
                    WHERE {geometry_column} IS NOT NULL AND NOT ST_IsValid({geometry_column})
                """)
                logger.info(f"{dataset_name}: Removed {invalid_count} invalid geometries")

        # Detect CRS from coordinate bounds
        logger.info(f"{dataset_name}: Detecting coordinate reference system")
        bounds = conn.execute(f"""
            SELECT
                MIN(ST_XMin({geometry_column})) as min_x,
                MAX(ST_XMax({geometry_column})) as max_x,
                MIN(ST_YMin({geometry_column})) as min_y,
                MAX(ST_YMax({geometry_column})) as max_y
            FROM {table_name}
            WHERE {geometry_column} IS NOT NULL
            LIMIT 1000
        """).fetchone()

        if bounds:
            min_x, max_x, min_y, max_y = bounds
            logger.info(
                f"{dataset_name}: Coordinate bounds: X=[{min_x:.2f}, {max_x:.2f}], "
                f"Y=[{min_y:.2f}, {max_y:.2f}]"
            )

            # Detect CRS based on coordinate ranges
            is_utm = 400000 <= min_x <= 900000 and 6000000 <= min_y <= 6500000
            is_wgs84_lon_lat = 7.5 <= min_x <= 15.5 and 54.5 <= min_y <= 58
            is_wgs84_lat_lon = 54.5 <= min_x <= 58 and 7.5 <= min_y <= 15.5

            if is_utm:
                logger.info(
                    f"{dataset_name}: Data already in EPSG:25832 (Danish UTM) - no transform needed"
                )
                # Already in target CRS, nothing to do
            elif is_wgs84_lon_lat:
                # Data is in GeoJSON standard lon/lat order (X=lon, Y=lat)
                # Use always_xy=true to tell PROJ to expect X=lon, Y=lat order
                # See: https://github.com/duckdb/duckdb-spatial/issues/295
                logger.info(
                    f"{dataset_name}: Data in EPSG:4326 (WGS84, lon/lat order) - transforming TO EPSG:25832 with always_xy"
                )
                conn.execute(f"""
                    UPDATE {table_name} SET
                        {geometry_column} = ST_Transform(
                            {geometry_column}, 'EPSG:4326', 'EPSG:25832', always_xy := true
                        )
                    WHERE {geometry_column} IS NOT NULL
                """)
            elif is_wgs84_lat_lon:
                # Data is in lat/lon order (X=lat, Y=lon) - flip to lon/lat first
                # This is less common but can happen with some data sources
                logger.info(
                    f"{dataset_name}: Data in EPSG:4326 (WGS84, lat/lon swapped) - flipping and transforming TO EPSG:25832"
                )
                conn.execute(f"""
                    UPDATE {table_name} SET
                        {geometry_column} = ST_Transform(
                            ST_FlipCoordinates({geometry_column}), 'EPSG:4326', 'EPSG:25832', always_xy := true
                        )
                    WHERE {geometry_column} IS NOT NULL
                """)
            else:
                logger.warning(
                    f"{dataset_name}: Unknown CRS (X: {min_x:.1f}-{max_x:.1f}, Y: {min_y:.1f}-{max_y:.1f}) "
                    f"- assuming EPSG:25832 (no transform)"
                )
        else:
            logger.warning(f"{dataset_name}: No valid geometries found for CRS detection")

        # Remove null and empty geometries
        conn.execute(f"""
            DELETE FROM {table_name}
            WHERE {geometry_column} IS NULL OR ST_IsEmpty({geometry_column})
        """)

        # Final validation
        final_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        removed_count = initial_count - final_count

        # Verify final CRS
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
            min_x, max_x, min_y, max_y = final_bounds
            is_utm_final = 400000 <= min_x <= 900000 and 6000000 <= min_y <= 6500000
            if is_utm_final:
                logger.info(f"{dataset_name}: ✅ Output CRS confirmed: EPSG:25832 (Danish UTM)")
            else:
                logger.warning(
                    f"{dataset_name}: ⚠️ Output coordinates may not be EPSG:25832: "
                    f"X=[{min_x:.2f}, {max_x:.2f}], Y=[{min_y:.2f}, {max_y:.2f}]"
                )

        logger.info(f"{dataset_name}: Validation complete")
        logger.info(f"{dataset_name}: Initial features: {initial_count}")
        logger.info(f"{dataset_name}: Valid features: {final_count}")
        logger.info(f"{dataset_name}: Removed features: {removed_count}")
        logger.info(f"{dataset_name}: Output CRS: EPSG:25832 (buffer/distance work in meters)")

    except Exception as e:
        logger.error(f"{dataset_name}: Error in geometry validation: {e!s}")
        raise
