import logging
import os
import json
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader

# Only import Google Cloud libraries when needed
# This avoids any dependency on credentials in development mode

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_exporter
def export_data(data, *args, **kwargs):
    """
    Store raw wetlands data in GeoJSON format.
    
    Args:
        data: Dictionary containing:
            geojson_data: GeoJSON FeatureCollection
            metadata: Processing metadata
    """
    # Get configuration
    config = kwargs.get('config', {})
    storage_path = config.get('storage_path', 'wetlands/raw_data.jsonl')
    
    print("\n📁 Storage Configuration:")
    print(f"  • Target path: {storage_path}")
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(storage_path), exist_ok=True)
    
    # Get GeoJSON data
    if not data or not isinstance(data, dict):
        error_msg = "❌ Invalid input data format"
        print(error_msg)
        raise ValueError(error_msg)
        
    geojson_data = data.get('geojson_data')
    if not geojson_data or not isinstance(geojson_data, dict):
        error_msg = "❌ Missing or invalid GeoJSON data"
        print(error_msg)
        raise ValueError(error_msg)
    
    try:
        print("\n🔄 Starting raw data storage process")
        
        # Write GeoJSON to file
        with open(storage_path, 'w') as f:
            json.dump(geojson_data, f)
            
        feature_count = len(geojson_data.get('features', []))
        file_size = os.path.getsize(storage_path)
        
        print("\n📊 Storage Statistics:")
        print(f"  • Features stored: {feature_count:,}")
        print(f"  • File size: {file_size/1024/1024:.2f} MB")
        print("  • Storage mode: local")
        
        print("\n✅ Successfully stored raw wetlands data")
        
        # Return required output format
        return {
            'raw_data_path': storage_path,
            'feature_count': feature_count,
            'storage_mode': 'local'
        }
        
    except Exception as e:
        error_msg = f"❌ Failed to store raw data: {str(e)}"
        print(error_msg)
        raise ValueError(error_msg)


@test
def test_output(data):
    """
    Test the output of the data exporter.
    """
    assert data is not None, 'The output is undefined'
    assert 'raw_data_path' in data, 'Output should contain raw_data_path'
    assert 'feature_count' in data, 'Output should contain feature_count'
    assert 'storage_mode' in data, 'Output should contain storage_mode'


if __name__ == "__main__":
    # For local testing - would only run if directly executed
    test_data = {
        'geojson_data': {
            'type': 'FeatureCollection',
            'features': [{
                'type': 'Feature',
                'geometry': {
                    'type': 'Polygon',
                    'coordinates': [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]
                },
                'properties': {
                    'gridcode': 12,
                    'toerv_pct': '6-12'
                }
            }]
        },
        'metadata': {
            'total_features': 1,
            'num_batches': 1
        }
    }
    # Test with mock config
    test_config = {
        'storage_path': 'wetlands/test_raw.jsonl'
    }
    result = export_data(test_data, config=test_config)
    print(f"Saved {result['feature_count']} features to {result['raw_data_path']} in {result['storage_mode']} mode") 