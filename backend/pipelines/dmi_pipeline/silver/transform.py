"""
DMI Climate Data Transformation Layer
Processes raw geospatial climate data into aggregated statistics using DuckDB.
Handles CRS transformation and calculates key metrics like averages, min/max values, and counts per time period.
"""

import json
import logging
from pathlib import Path
from typing import Dict, Optional

import duckdb
import pandas as pd

# Configure logging
logger = logging.getLogger(__name__)


class DataTransformer:
    """Transforms raw climate data into processed statistics"""

    def __init__(self):
        self.TARGET_CRS = "EPSG:4326"  # Required target CRS
        self.SOURCE_CRS = "EPSG:25832"  # DMI's native CRS

    def load_raw_data(self, input_path: Path) -> Optional[Dict]:
        """Load raw JSON data from the bronze layer"""
        try:
            with open(input_path, "r") as f:
                data = json.load(f)
            logger.info(f"Successfully loaded raw data from {input_path}")
            return data
        except Exception as e:
            logger.error(f"Error loading raw data from {input_path}: {str(e)}")
            return None

    def transform_data(self, raw_data: Dict) -> Optional[duckdb.DuckDBPyRelation]:
        """Transforms raw climate data into processed statistics using DuckDB"""
        if not raw_data or "features" not in raw_data or not raw_data["features"]:
            logger.warning("No features found in raw data")
            return None

        try:
            # Create DuckDB connection and enable spatial extension
            con = duckdb.connect(":memory:")
            con.execute("INSTALL spatial;")
            con.execute("LOAD spatial;")

            # Create a list of dictionaries with extracted properties
            features = []
            for feature in raw_data["features"]:
                properties = feature.get("properties", {})
                geometry = feature.get("geometry", {})
                features.append(
                    {
                        "value": properties.get("value"),
                        "parameter_id": properties.get("parameterId"),
                        "valid_time": properties.get("from"),
                        "created": properties.get("created"),
                        "geometry": json.dumps(geometry) if geometry else None,
                    }
                )

            # Convert features to a format DuckDB can understand
            df = pd.DataFrame(features)

            # Register the DataFrame as a table
            con.register("features", df)

            # Create a table from the extracted features
            con.execute("""
                CREATE TABLE extracted_data AS
                SELECT
                    CAST(value AS DOUBLE) as value,
                    parameter_id,
                    valid_time,
                    created,
                    ST_GeomFromGeoJSON(geometry) as geometry
                FROM features
                WHERE value IS NOT NULL
            """)

            # Transform CRS using DuckDB's spatial functions
            con.execute(f"""
                CREATE TABLE transformed_data AS
                SELECT
                    value,
                    parameter_id,
                    valid_time,
                    created,
                    ST_Transform(geometry, '{self.SOURCE_CRS}', '{self.TARGET_CRS}') as geometry
                FROM extracted_data
            """)

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
                    ST_AsGeoJSON(geometry) as geometry
                FROM transformed_data
                GROUP BY parameter_id, valid_time, created, geometry
                ORDER BY valid_time DESC
            """)

            logger.info(f"Successfully transformed {len(raw_data['features'])} records")
            return result

        except Exception as e:
            logger.error(f"Error transforming data: {str(e)}")
            return None

    def transform_from_file(self, input_path: Path) -> Optional[duckdb.DuckDBPyRelation]:
        """Transform data from a JSON file in the bronze layer"""
        raw_data = self.load_raw_data(input_path)
        if raw_data:
            return self.transform_data(raw_data)
        return None
