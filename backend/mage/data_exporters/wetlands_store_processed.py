import os
import shutil
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader

# Only import Google Cloud libraries when needed
# This avoids any dependency on credentials in development mode

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_exporter
def export_processed_wetlands(data, *args, **kwargs):
    """
    Export processed wetlands data using configured storage backend.
    Takes GeoParquet data from validation step and stores it in the configured location.
    
    Args:
        data: Dictionary containing:
            - validated_data_path: Path to validated GeoParquet file
            - validated_count: Number of valid features
            - metadata: Processing metadata including:
                - source_crs: Source CRS (EPSG code)
                - target_crs: Target CRS (EPSG code)
                - validation_rules: Applied validation rules
                - stats: Validation statistics
    """
    # Remove unused logger
    
    # Get data from previous step
    validated_data_path = data.get('validated_data_path')
    if not validated_data_path or not os.path.exists(validated_data_path):
        error_msg = "❌ Missing or invalid validated_data_path in input"
        print(error_msg)
        raise ValueError(error_msg)
    
    validated_count = data.get('validated_count', 0)
    metadata = data.get('metadata', {})
    
    # Get path from block configuration
    block_config = kwargs.get('config', {})
    storage_path = block_config.get('storage_path', 'wetlands/processed_current.parquet')
    
    print("\n📁 Storage Configuration:")
    print(f"  • Source path: {validated_data_path}")
    print(f"  • Target path: {storage_path}")
    
    # Load IO configuration
    config_path = os.path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'production' if os.getenv('MAGE_ENVIRONMENT') == 'production' else 'default'
    config = ConfigFileLoader(config_path, config_profile)
    
    print(f"\n🔄 Starting processed data storage in {config_profile} mode")
    
    try:
        # Get storage configuration based on environment
        if config_profile == 'production':
            # Production mode: use Google Cloud Storage
            from google.cloud import storage
            
            # Get GCS configuration
            storage_config = config.get('GCS_STORAGE') or {}
            bucket_name = storage_config.get('bucket')
            
            if not bucket_name:
                bucket_name = os.getenv('GCS_BUCKET_NAME')
                if not bucket_name:
                    error_msg = "❌ GCS bucket name not found in configuration or environment variables"
                    print(error_msg)
                    raise ValueError(error_msg)
            
            print("\n☁️ Using Google Cloud Storage:")
            print(f"  • Bucket: {bucket_name}")
            print(f"  • Path: {storage_path}")
            
            # Initialize GCS client
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(storage_path)
            
            # Upload the data
            print("\n📤 Uploading to GCS...")
            with open(validated_data_path, 'rb') as f:
                blob.upload_from_file(f)
            
            print(f"✅ Stored {validated_count:,} processed features to gs://{bucket_name}/{storage_path}")
            storage_mode = 'gcs'
        else:
            # Development mode: use local file system
            storage_config = config.get('LOCAL_STORAGE') or {}
            base_path = storage_config.get('base_path') or os.getenv('MAGEAI_DATA_DIR') or '/home/src/mage_data'
            
            print("\n💾 Using local storage:")
            print(f"  • Base path: {base_path}")
            
            # Ensure the directory exists
            if not os.path.exists(os.path.dirname(storage_path)):
                os.makedirs(os.path.dirname(storage_path))
            
            # Copy the validated file to the destination
            print("\n📋 Copying data to local storage...")
            shutil.copy2(validated_data_path, storage_path)
            
            print(f"✅ Stored {validated_count:,} processed features to {storage_path}")
            storage_mode = 'local'
        
        # Log CRS information
        source_crs = metadata.get('source_crs')
        target_crs = metadata.get('target_crs')
        if source_crs and target_crs:
            print("\n�� CRS Information:")
            print(f"  • Source: EPSG:{source_crs}")
            print(f"  • Target: EPSG:{target_crs}")
        
        # Log validation statistics
        stats = metadata.get('stats', {})
        if stats:
            print("\n📊 Validation Statistics:")
            for key, value in stats.items():
                print(f"  • {key}: {value}")
        
        # Log file size
        if storage_mode == 'local':
            file_size = os.path.getsize(storage_path)
            print(f"\n📦 File size: {file_size/1024/1024:.2f} MB")
        
        return {
            'processed_data_path': storage_path,
            'feature_count': validated_count,
            'storage_mode': storage_mode,
            'metadata': metadata
        }
            
    except Exception as e:
        error_msg = f"❌ Failed to store processed data: {str(e)}"
        print(error_msg)
        raise


@test
def test_output(data):
    """
    Test the output of the data exporter.
    """
    assert data is not None, 'The output is undefined'
    assert 'processed_data_path' in data, 'Output should contain processed_data_path'
    assert 'feature_count' in data, 'Output should contain feature_count'
    assert 'storage_mode' in data, 'Output should contain storage_mode'
    assert 'metadata' in data, 'Output should contain metadata'


if __name__ == "__main__":
    # For local testing - would only run if directly executed
    import tempfile
    
    # Create a test GeoParquet file
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
        temp_path = temp_file.name
        
        # Write some test data to the file
        with open(temp_path, 'wb') as f:
            f.write(b'Test GeoParquet data')
    
    try:
        # Test with mock data
        test_data = {
            'validated_data_path': temp_path,
            'validated_count': 2,
            'metadata': {
                'source_crs': 25832,
                'target_crs': 25832,
                'validation_rules': {
                    'min_points': 4,
                    'min_area': 1.0,
                    'max_area': 10000000.0
                },
                'stats': {
                    'total_valid': 2,
                    'avg_points': 5,
                    'avg_area_m2': 1000,
                    'min_area_m2': 500,
                    'max_area_m2': 1500
                }
            }
        }
        
        # Test with mock config
        test_config = {
            'storage_path': 'wetlands/test_processed.parquet'
        }
        
        result = export_processed_wetlands(test_data, config=test_config)
        print(f"Saved {result['feature_count']} features to {result['processed_data_path']} in {result['storage_mode']} mode")
        print("Metadata:")
        for key, value in result['metadata'].items():
            print(f"  {key}: {value}")
            
    finally:
        # Clean up test file
        if os.path.exists(temp_path):
            os.remove(temp_path) 