"""
Coordinate transformation utilities for H3 PFAS exposure analysis.
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
        """Prepare geometries with coordinate flipping for spatial operations."""

        prepared_table = f"{table_name}_prepared"

        query = f"""
        CREATE OR REPLACE TABLE {prepared_table} AS
        SELECT *,
            -- Original geometry for area calculations (LAT/LON)
            ST_GeomFromText({geometry_column}) as original_geometry,
            -- Flipped geometry for spatial operations (LON/LAT)
            ST_FlipCoordinates(ST_GeomFromText({geometry_column})) as flipped_geometry
        FROM {table_name}
        WHERE {geometry_column} IS NOT NULL
        AND ST_IsValid(ST_GeomFromText({geometry_column}))
        """

        self.conn.execute(query)
        self.log.debug(f"✅ Prepared geometries for {table_name}")
        return prepared_table
