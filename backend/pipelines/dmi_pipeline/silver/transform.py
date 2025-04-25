"""
DMI Climate Data Transformation Layer
Processes raw geospatial climate data into aggregated statistics using DuckDB.
Handles CRS transformation and calculates key metrics like averages, min/max values, and counts per time period.
"""

import logging
import duckdb
import geopandas as gpd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataTransformer:
    """Transforms raw climate data into processed statistics"""
    def __init__(self):
        self.TARGET_CRS = "EPSG:4326"  # Required target CRS

    def transform_data(self, gdf: gpd.GeoDataFrame) -> duckdb.DuckDBPyRelation:
        """Transforms raw climate data into processed statistics using DuckDB"""
        if gdf.empty:
            return None

        # Convert CRS to target CRS
        gdf = gdf.to_crs(self.TARGET_CRS)
        logger.info(f"Converted GeoDataFrame to target CRS: {self.TARGET_CRS}")

        # Create DuckDB connection
        con = duckdb.connect(':memory:')

        # Register GeoDataFrame directly with DuckDB
        con.register('climate_data', gdf)

        # Process data using DuckDB
        result = con.execute("""
            SELECT
                parameter_id,
                valid_time,
                created,
                AVG(value) as avg_value,
                MIN(value) as min_value,
                MAX(value) as max_value,
                COUNT(*) as count,
                geo_crs_source
            FROM climate_data
            GROUP BY parameter_id, valid_time, created, geo_crs_source
            ORDER BY valid_time DESC
        """)

        return result