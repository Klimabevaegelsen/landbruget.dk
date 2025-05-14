#!/usr/bin/env python3
"""
Test script for geopandas integration.
This script verifies that geopandas is installed and works with the 
ParquetManager.
"""

import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd

# Add the parent directory to sys.path
sys.path.append(str(Path(__file__).parent))

from drive_data_pipeline.silver.parquet_manager import ParquetManager
from drive_data_pipeline.utils.logging import get_logger

# Get logger
logger = get_logger()


def test_geopandas_integration():
    """Test geopandas integration with ParquetManager."""
    print("Testing geopandas integration...")
    
    # Create a test DataFrame with coordinates
    df = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'latitude': [55.676098, 55.673858, 55.676973, 55.682062, 55.678052],
        'longitude': [12.568337, 12.564678, 12.568746, 12.571121, 12.573855]
    })
    
    print("Created test DataFrame with coordinates:")
    print(df.head())
    
    # Create a ParquetManager instance
    parquet_manager = ParquetManager()
    
    # Convert DataFrame to GeoDataFrame
    gdf = parquet_manager.dataframe_to_geodataframe(
        df, 'latitude', 'longitude', 'EPSG:4326'
    )
    
    print("Converted to GeoDataFrame:")
    print(f"Type: {type(gdf)}")
    print(f"CRS: {gdf.crs}")
    print(gdf.head())
    
    # Create a directory for test output
    output_dir = Path("./test_output")
    output_dir.mkdir(exist_ok=True)
    
    # Save to GeoParquet
    output_path = output_dir / "test_geo.geoparquet"
    parquet_manager.save_geodataframe_to_geoparquet(
        gdf, output_path, schema_metadata={'datasource': 'test'}
    )
    
    print(f"Saved GeoDataFrame to GeoParquet: {output_path}")
    
    # Try to read back
    try:
        read_gdf = gpd.read_parquet(output_path)
        print("Successfully read back GeoParquet file:")
        print(f"CRS: {read_gdf.crs}")
        print(f"Number of records: {len(read_gdf)}")
        print("GeoParquet test successful!")
    except Exception as e:
        print(f"Error reading GeoParquet file: {e}")
        return False
    
    return True


if __name__ == "__main__":
    if test_geopandas_integration():
        print("\nAll tests completed successfully!")
    else:
        print("\nTests failed!") 