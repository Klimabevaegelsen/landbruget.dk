import logging
import duckdb
import tempfile
import os
import json
from typing import Dict, Any
import geopandas as gpd
import pandas as pd
import shapely

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def wetlands_format_geojson(data, *args, **kwargs):
    """
    Pipeline-specific transformer for Danish wetlands data.
    Converts GeoJSON features to GeoParquet format.
    The GeoJSON data already contains CRS information (EPSG:4326).
    
    The WFS service returns features with the following structure:
    - gridcode: Integer field representing peat percentage ranges:
        - 12: represents peat percentage 6-12%
        - 60: represents peat percentage >12%
    - geometry: Polygon geometry in EPSG:4326
    
    Note: The original toerv_pct field is dropped as it contains the same information
    as gridcode in a less efficient string format.
    
    Args:
        data: Dictionary containing:
            - geojson_data: GeoJSON FeatureCollection from WFS
            - metadata: Optional metadata including:
                - source_crs: Source CRS (EPSG code)
                - total_features: Total number of features
                - num_batches: Number of batches combined
                    
    Returns:
        Dictionary containing:
            - input_path: Path to GeoParquet file with formatted features
            - metadata: Processing metadata including:
                - output_format: Always 'geoparquet'
                - feature_count: Number of features read from GeoJSON
                - field_types: Dictionary of field types
    
    Raises:
        ValueError: If GeoJSON data is missing or invalid
        Exception: If any error occurs during processing
    """
    logger = logging.getLogger(__name__)
    
    # Get GeoJSON data
    geojson_data = data.get('geojson_data')
    if not geojson_data:
        print("❌ Missing GeoJSON data in input")
        raise ValueError("Missing GeoJSON data in input")
    
    metadata = data.get('metadata', {})
    
    print("\n🔄 Starting GeoJSON to GeoParquet conversion")
    
    # Initialize variables
    con = None
    geojson_path = None
    output_path = None
    
    try:
        # Create temporary files
        fd, geojson_path = tempfile.mkstemp(suffix='.geojson')
        os.close(fd)
        
        fd, output_path = tempfile.mkstemp(suffix='.parquet')
        os.close(fd)
        
        # Write GeoJSON to file
        print("\n🔍 GeoJSON CRS Information:")
        if 'crs' in geojson_data:
            print(f"  • CRS in GeoJSON: {geojson_data['crs']}")
        else:
            print("  • No CRS information in GeoJSON")
            
        with open(geojson_path, 'w') as f:
            json.dump(geojson_data, f)
        
        print("📥 Reading GeoJSON with GeoPandas")
        # Read the file - coordinates are already in EPSG:4326
        gdf = gpd.read_file(geojson_path)
        
        # Just tell GeoPandas these coordinates are in EPSG:4326
        # DO NOT transform the coordinates - they are already in the right CRS
        gdf.set_crs(epsg=4326, inplace=True, allow_override=True)
        
        # Debug CRS information
        print("\n🔍 Current CRS Information:")
        print(f"  • Raw CRS object: {gdf.crs}")
        if gdf.crs is not None:
            print(f"  • CRS type: {type(gdf.crs)}")
            if hasattr(gdf.crs, 'to_epsg'):
                print(f"  • EPSG code: {gdf.crs.to_epsg()}")
        
        print("\n🔧 Processing and filtering data")
        # Keep only the required columns and convert gridcode to integer
        if 'gridcode' in gdf.columns:
            gdf['gridcode'] = gdf['gridcode'].astype(int)
            print("  • Converted gridcode to integer")
        else:
            # If gridcode is in properties, extract it
            if 'properties' in gdf.columns and gdf['properties'].iloc[0] and 'gridcode' in gdf['properties'].iloc[0]:
                gdf['gridcode'] = gdf['properties'].apply(lambda x: int(x.get('gridcode', 0)))
                print("  • Extracted gridcode from properties")
            else:
                print("⚠️ No gridcode column found, setting default value of 0")
                gdf['gridcode'] = 0
        
        # Keep only necessary columns
        gdf = gdf[['gridcode', 'geometry']]
        print("  • Filtered to required columns: gridcode, geometry")
        
        print("\n🔍 Validating and fixing geometries")
        initial_count = len(gdf)
        
        # Check for null geometries
        null_count = gdf['geometry'].isna().sum()
        if null_count > 0:
            print(f"⚠️ Found {null_count:,} null geometries, dropping them")
            gdf = gdf.dropna(subset=['geometry'])
        
        # Check for empty geometries
        empty_count = gdf['geometry'].apply(lambda g: g.is_empty if g is not None else True).sum()
        if empty_count > 0:
            print(f"⚠️ Found {empty_count:,} empty geometries, dropping them")
            gdf = gdf[~gdf['geometry'].apply(lambda g: g.is_empty if g is not None else True)]
        
        # Fix invalid geometries
        invalid_count = gdf['geometry'].apply(lambda g: not shapely.is_valid(g) if g is not None else True).sum()
        if invalid_count > 0:
            print(f"🔧 Found {invalid_count:,} invalid geometries, fixing them")
            gdf['geometry'] = gdf['geometry'].apply(lambda g: shapely.make_valid(g) if g is not None and not shapely.is_valid(g) else g)
        
        # Final check for validity
        final_invalid = gdf['geometry'].apply(lambda g: not shapely.is_valid(g) if g is not None else True).sum()
        if final_invalid > 0:
            print(f"⚠️ Still have {final_invalid:,} invalid geometries after fixing, dropping them")
            gdf = gdf[gdf['geometry'].apply(lambda g: shapely.is_valid(g) if g is not None else False)]
        
        # Log final counts
        print(f"\n📊 Final dataset statistics:")
        print(f"  • Total features: {len(gdf):,}")
        print(f"  • Features removed: {initial_count - len(gdf):,}")
        
        print("\n🔢 Features by gridcode:")
        gridcode_counts = gdf['gridcode'].value_counts().to_dict()
        for code, count in sorted(gridcode_counts.items()):
            print(f"  • Code {code}: {count:,} features")
        
        # Write to GeoParquet using GeoPandas
        print(f"\n💾 Writing {len(gdf):,} features to GeoParquet")
        
        # Write to GeoParquet with explicit CRS and schema version
        gdf.to_parquet(
            output_path,
            schema_version="1.0.0",  # Use latest GeoParquet schema version
            compression="snappy"  # Use snappy compression for good balance of speed/size
        )
        
        # Verify CRS was written correctly
        check_gdf = gpd.read_parquet(output_path)
        if check_gdf.crs is None or check_gdf.crs.to_epsg() != 4326:
            raise ValueError("Failed to preserve EPSG:4326 CRS in GeoParquet output")
        print("✅ Verified CRS is EPSG:4326 in output file")
        
        # Update metadata
        metadata.update({
            'output_format': 'geoparquet',
            'feature_count': len(gdf),
            'field_types': {
                'gridcode': 'INTEGER',  # 12: 6-12%, 60: >12%
                'geometry': 'GEOMETRY'
            }
        })
        
        print("\n✅ Successfully converted to GeoParquet format")
        print(f"  • Output path: {output_path}")
        print(f"  • File size: {os.path.getsize(output_path)/1024/1024:.2f} MB")
        
        return {
            'input_path': output_path,
            'metadata': metadata
        }
        
    except Exception as e:
        print(f"❌ Failed to process wetlands data: {str(e)}")
        # Clean up temporary files before re-raising
        if output_path and os.path.exists(output_path):
            os.remove(output_path)
        if geojson_path and os.path.exists(geojson_path):
            os.remove(geojson_path)
        # Re-raise the exception to trigger pipeline failure
        raise
    finally:
        if geojson_path and os.path.exists(geojson_path):
            os.remove(geojson_path)


@test
def test_output(data):
    """Test the output of the transformer"""
    assert data is not None, 'Output should not be None'
    assert isinstance(data, dict), 'Output should be a dictionary'
    assert 'input_path' in data, 'Output should contain input_path'
    assert 'metadata' in data, 'Output should contain metadata'
    
    metadata = data['metadata']
    assert metadata.get('output_format') == 'geoparquet', 'Output format should be geoparquet'
    assert 'feature_count' in metadata, 'Metadata should contain feature_count'
    assert 'field_types' in metadata, 'Metadata should contain field_types'
    
    field_types = metadata['field_types']
    assert 'gridcode' in field_types, 'Field types should include gridcode'
    assert 'geometry' in field_types, 'Field types should include geometry'


if __name__ == "__main__":
    # For local testing - using real data from WFS service in EPSG:4326
    test_case = {
        'geojson_data': {
            'type': 'FeatureCollection',
            'crs': {
                'type': 'name',
                'properties': {
                    'name': 'EPSG:4326'
                }
            },
            'features': [
                {
                    'type': 'Feature',
                    'geometry': {
                        'type': 'Polygon',
                        'coordinates': [[[10.6121587, 57.74290978], [10.61199079, 57.74291192], 
                                      [10.61199479, 57.74300171], [10.6121627, 57.74299957], 
                                      [10.6121587, 57.74290978]]]
                    },
                    'properties': {
                        'gridcode': 12,
                        'toerv_pct': '6-12'
                    }
                },
                {
                    'type': 'Feature',
                    'geometry': {
                        'type': 'Polygon',
                        'coordinates': [[[10.61929851, 57.74102192], [10.61913061, 57.74102406],
                                      [10.61913463, 57.74111385], [10.61879883, 57.74111814],
                                      [10.61880284, 57.74120793], [10.61930654, 57.74120149],
                                      [10.61929851, 57.74102192]]]
                    },
                    'properties': {
                        'gridcode': 60,
                        'toerv_pct': '>12'
                    }
                }
            ]
        },
        'metadata': {
            'source_crs': 4326,
            'total_features': 2,
            'num_batches': 1
        }
    }
    
    try:
        result = wetlands_format_geojson(test_case)
        print(f"Success - Converted {result['metadata'].get('feature_count', 'unknown')} features")
        print(f"Output path: {result['input_path']}")
        
        # Additional test: verify CRS and data in output
        import geopandas as gpd
        gdf = gpd.read_parquet(result['input_path'])
        assert gdf.crs.to_epsg() == 4326, "Output CRS is not EPSG:4326"
        assert len(gdf) == 2, "Expected 2 features in output"
        assert set(gdf['gridcode'].unique()) == {12, 60}, "Expected gridcodes 12 and 60"
        print("✅ Verified output CRS is EPSG:4326 and data is correct")
    except Exception as e:
        print(f"Error processing wetlands: {str(e)}")
        raise 