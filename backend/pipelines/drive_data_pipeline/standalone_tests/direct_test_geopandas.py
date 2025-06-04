#!/usr/bin/env python3
"""
Direct test for geopandas integration without going through __init__.py.
"""

# Standard library imports
import sys
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Third-party imports
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

# Import the logger directly
from utils.logging import get_logger

logger = get_logger()


class ParquetManager:
    """Simplified ParquetManager for testing geopandas integration."""
    
    def __init__(self, compression="snappy"):
        """Initialize the Parquet manager."""
        self.compression = compression
        logger.info(f"Initialized ParquetManager with compression={compression}")

    def dataframe_to_geodataframe(
        self, df, latitude_col, longitude_col, target_crs="EPSG:4326"
    ):
        """Convert a DataFrame with lat/lon columns to a GeoDataFrame."""
        try:
            logger.info(
                f"Converting DataFrame with {latitude_col}/{longitude_col} to GeoDataFrame"
            )
            
            # Create copy to avoid modifying original
            df_copy = df.copy()
            
            # Create geometry column
            geometry = [
                Point(lon, lat) if pd.notna(lon) and pd.notna(lat) else None
                for lon, lat in zip(df_copy[longitude_col], df_copy[latitude_col], strict=False)
            ]
            
            # Create GeoDataFrame
            gdf = gpd.GeoDataFrame(
                df_copy, geometry=geometry, crs=target_crs
            )
            
            logger.info(f"Converted DataFrame to GeoDataFrame with CRS {target_crs}")
            return gdf
        
        except Exception as e:
            error_msg = f"Failed to convert DataFrame to GeoDataFrame: {str(e)}"
            logger.error(error_msg)
            raise RuntimeError(error_msg) from e
    
    def save_geodataframe_to_geoparquet(
        self, gdf, output_path, schema_metadata=None
    ):
        """Save a GeoDataFrame to GeoParquet format."""
        try:
            logger.info(f"Saving GeoDataFrame to GeoParquet: {output_path}")
            
            # Create parent directory if it doesn't exist
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Add schema metadata if provided
            if schema_metadata:
                # Convert metadata to dictionary format
                metadata = {"schema": schema_metadata}
                
                # Save with metadata
                gdf.to_parquet(
                    output_path,
                    compression=self.compression,
                    metadata=metadata,
                )
            else:
                # Standard save
                gdf.to_parquet(
                    output_path,
                    compression=self.compression,
                )
            
            logger.info(f"Saved GeoParquet file to {output_path}")
            return output_path
        
        except Exception as e:
            error_msg = f"Failed to save GeoDataFrame to GeoParquet: {str(e)}"
            logger.error(error_msg)
            raise RuntimeError(error_msg) from e


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
    output_dir = project_root / "test_output"
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