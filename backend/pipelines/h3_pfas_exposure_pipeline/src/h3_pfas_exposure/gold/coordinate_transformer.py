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

    def prepare_geometries(self, table_name: str, geometry_column: str = "geometry_wkt") -> str:
        """Prepare geometries for spatial operations (coordinates now fixed in silver layer)."""

        prepared_table = f"{table_name}_prepared"

        query = f"""
        CREATE OR REPLACE TABLE {prepared_table} AS
        SELECT *,
            -- Geometry for both area calculations and spatial operations (coordinates fixed in silver layer)
            ST_GeomFromText({geometry_column}) as geometry
        FROM {table_name}
        WHERE {geometry_column} IS NOT NULL
        AND ST_IsValid(ST_GeomFromText({geometry_column}))
        """

        self.conn.execute(query)
        self.log.debug(
            f"✅ Prepared geometries for {table_name} (coordinates fixed in silver layer)"
        )
        return prepared_table
