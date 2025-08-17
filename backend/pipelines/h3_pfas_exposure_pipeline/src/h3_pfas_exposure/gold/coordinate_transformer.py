"""
Coordinate transformation utilities for H3 PFAS exposure analysis.

Note: As of 2025-01-07, coordinate system issues have been fixed at the source
in the silver layer pipelines. This module now provides backward compatibility
while using the corrected coordinates from the silver layer.
"""

import duckdb
from loguru import logger

from ..config import H3SpatialConfig


class CoordinateTransformer:
    """Handles coordinate system transformations and area calculations."""

    def __init__(self, conn: duckdb.DuckDBPyConnection, config: H3SpatialConfig):
        self.conn = conn
        self.config = config
        self.log = logger.bind(component="CoordinateTransformer")

    def prepare_geometries(self, table_name: str, geometry_column: str = "geometry") -> str:
        """Prepare geometries for spatial operations with coordinate system alignment."""

        prepared_table = f"{table_name}_prepared"

        # Field geometries are now correctly in lat/lon order for spherical calculations
        # No transformation needed - just validate
        query = f"""
        CREATE OR REPLACE TABLE {prepared_table} AS
        SELECT *
        FROM {table_name}
        WHERE {geometry_column} IS NOT NULL
        AND ST_IsValid({geometry_column})
        """

        try:
            self.conn.execute(query)
            self.log.info(
                f"✅ Prepared geometries for {table_name} (field geometries in correct lat/lon order)"
            )
        except Exception as e:
            # Fallback: just use the original geometries if coordinate swap fails
            self.log.warning(f"⚠️ Coordinate swap failed, using original geometries: {e}")
            fallback_query = f"""
            CREATE OR REPLACE TABLE {prepared_table} AS
            SELECT *
            FROM {table_name}
            WHERE {geometry_column} IS NOT NULL
            AND ST_IsValid({geometry_column})
            """
            self.conn.execute(fallback_query)

        return prepared_table
