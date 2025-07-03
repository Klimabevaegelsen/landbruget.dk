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
