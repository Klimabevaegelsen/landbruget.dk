#!/usr/bin/env python3
"""
Simple test for geopandas functionality.
"""

from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point


def test_geopandas():
    """Test basic geopandas functionality."""
    print("Testing basic geopandas functionality...")
    
    # Create a test DataFrame with coordinates
    df = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'latitude': [55.676098, 55.673858, 55.676973, 55.682062, 55.678052],
        'longitude': [12.568337, 12.564678, 12.568746, 12.571121, 12.573855]
    })
    
    print("Created test DataFrame with coordinates:")
    print(df.head())
    
    # Create geometry column
    geometry = [
        Point(lon, lat) 
        for lon, lat in zip(df['longitude'], df['latitude'], strict=False)
    ]
    
    # Create GeoDataFrame
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
    
    print("Converted to GeoDataFrame:")
    print(f"Type: {type(gdf)}")
    print(f"CRS: {gdf.crs}")
    print(gdf.head())
    
    # Create a directory for test output
    output_dir = Path("./test_output")
    output_dir.mkdir(exist_ok=True)
    
    # Save to GeoParquet
    output_path = output_dir / "test_geo_simple.geoparquet"
    gdf.to_parquet(output_path)
    
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
    
    print("Geopandas test completed successfully!")
    return True


if __name__ == "__main__":
    test_geopandas() 