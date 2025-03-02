import logging
import time

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
    from mage_ai.data_preparation.shared.secrets import get_secret_value
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def combine_batches(data, *args, **kwargs):
    """
    Combine multiple batches of wetlands GeoJSON data into a single FeatureCollection.
    
    Args:
        data: Single batch or list of batches, each containing:
            - geojson_data: GeoJSON FeatureCollection
            - metadata: Optional processing metadata
            
    Returns:
        Dictionary containing:
            - geojson_data: GeoJSON FeatureCollection
            - metadata: Combined metadata including:
                - total_features: Total number of features
                - num_batches: Number of batches combined
            
    Raises:
        ValueError: If input data is invalid
        TypeError: If batch data is not in correct format
    """
    print("\n🔍 Debug: Received data type:", type(data))
    print("🔍 Debug: Data content:", data)
    print("🔍 Debug: Args:", args)
    print("🔍 Debug: Kwargs:", kwargs)
    
    if not data:
        print("❌ No data provided to combine_batches")
        raise ValueError("No data provided to combine_batches")
    
    # Ensure data is a list
    batches = [data] if not isinstance(data, list) else data
    
    if not batches:
        print("❌ No batches to combine")
        raise ValueError("No batches to combine")
    
    print("\n🔄 Starting batch combination process")
    print(f"📦 Number of batches to combine: {len(batches):,}")
    
    # Initialize variables
    metadata = None
    combined_features = []
    batch_stats = []
    start_time = time.time()
    
    try:
        # Loop through each batch and combine features
        for i, batch in enumerate(batches):
            if not isinstance(batch, dict):
                print(f"❌ Batch {i} is not a dictionary")
                raise TypeError(f"Batch {i} is not a dictionary")
                
            if 'geojson_data' not in batch:
                print(f"❌ Batch {i} missing geojson_data")
                raise ValueError(f"Batch {i} missing geojson_data")
                
            geojson_data = batch['geojson_data']
            if not isinstance(geojson_data, dict) or 'features' not in geojson_data:
                print(f"❌ Batch {i} has invalid GeoJSON structure")
                raise ValueError(f"Batch {i} has invalid GeoJSON structure")
            
            # Get metadata from first batch if not already set
            if metadata is None and 'metadata' in batch:
                metadata = batch['metadata']
            
            # Add features from this batch
            batch_features = geojson_data['features']
            combined_features.extend(batch_features)
            
            # Collect batch statistics
            batch_stats.append({
                'batch_index': i + 1,
                'feature_count': len(batch_features)
            })
            
            # Calculate progress percentage
            progress = ((i + 1) / len(batches)) * 100
            print(f"\n📊 Processing batch {i+1}/{len(batches)} ({progress:.1f}%)")
            print(f"  • Features in batch: {len(batch_features):,}")
            print(f"  • Total features so far: {len(combined_features):,}")
        
        # Create combined GeoJSON FeatureCollection
        combined_geojson = {
            'type': 'FeatureCollection',
            'features': combined_features
        }
            
        # Calculate final statistics
        feature_count = len(combined_features)
        processing_time = time.time() - start_time
        features_per_second = feature_count / processing_time if processing_time > 0 else 0
        
        print("\n📈 Final Statistics:")
        print(f"  • Total features: {feature_count:,}")
        print(f"  • Number of batches: {len(batches):,}")
        print(f"  • Average features/batch: {feature_count/len(batches):,.1f}")
        print(f"  • Processing time: {processing_time:.1f} seconds")
        print(f"  • Processing speed: {features_per_second:,.1f} features/second")
        
        # Print batch-specific statistics
        print("\n📊 Batch Statistics:")
        for stat in batch_stats:
            print(f"  • Batch {stat['batch_index']}: {stat['feature_count']:,} features")
        
        # Update metadata if it exists
        if metadata:
            metadata.update({
                'total_features': feature_count,
                'num_batches': len(batches),
                'batch_stats': batch_stats,
                'processing_time_seconds': processing_time,
                'features_per_second': features_per_second
            })
        
        print("\n✅ Successfully combined all batches")
        return {
            'geojson_data': combined_geojson,
            'metadata': metadata
        }
        
    except Exception as e:
        print(f"❌ Failed to combine batches: {str(e)}")
        raise


@test
def test_output(*args) -> None:
    """
    Test the output of the transformer.
    When dynamic mapping is enabled with reduce_output=true, this function will
    receive either:
    1. Multiple batch results as separate arguments (old behavior)
    2. A single argument containing a list of batch results (new behavior)
    
    Args:
        *args: One or more batch results
        
    Raises:
        AssertionError: If any test fails
    """
    # Check that we received at least one argument
    assert len(args) > 0, 'No output was produced'
    
    # Get the first argument
    data = args[0]
    
    logger = logging.getLogger(__name__)
    logger.info(f"Test received {len(args)} arguments")
    logger.info(f"First argument type: {type(data)}")
    
    if len(args) > 1:
        # Case 1: Multiple arguments (old behavior)
        logger.info("Testing with multiple arguments (from reduce_output=true)")
        
        # Test each batch result
        for i, batch_result in enumerate(args):
            assert isinstance(batch_result, dict), f'Batch result {i} should be a dictionary'
            assert 'geojson_data' in batch_result, f'Batch result {i} should contain geojson_data'
            assert isinstance(batch_result['geojson_data'], dict), f'Batch result {i} geojson_data should be a dictionary'
            assert 'features' in batch_result['geojson_data'], f'Batch result {i} should contain features'
    elif isinstance(data, list):
        # Case 2: Single argument containing list of batches (new behavior)
        logger.info("Testing with single list argument (from reduce_output=true)")
        
        # Test each batch in the list
        for i, batch_result in enumerate(data):
            assert isinstance(batch_result, dict), f'Batch {i} should be a dictionary'
            assert 'geojson_data' in batch_result, f'Batch {i} should contain geojson_data'
            assert isinstance(batch_result['geojson_data'], dict), f'Batch {i} geojson_data should be a dictionary'
            assert 'features' in batch_result['geojson_data'], f'Batch {i} should contain features'
    else:
        # Case 3: Single combined result
        logger.info("Testing with single combined result")
        assert isinstance(data, dict), 'Output should be a dictionary'
        
        # Check geojson_data
        assert 'geojson_data' in data, 'Output should contain geojson_data'
        assert isinstance(data['geojson_data'], dict), 'geojson_data should be a dictionary'
        assert 'type' in data['geojson_data'], 'geojson_data should contain type'
        assert 'features' in data['geojson_data'], 'geojson_data should contain features'
        assert data['geojson_data']['type'] == 'FeatureCollection', 'geojson_data type should be FeatureCollection'
        assert isinstance(data['geojson_data']['features'], list), 'features should be a list'


if __name__ == "__main__":
    # For local testing
    test_batches = [
        {
            'batch_index': 0,
            'geojson_data': {
                'type': 'FeatureCollection',
                'features': [{
                    'type': 'Feature',
                    'geometry': {
                        'type': 'Polygon',
                        'coordinates': [[[595970, 6401230], [595960, 6401230], [595960, 6401240], [595970, 6401240], [595970, 6401230]]]
                    },
                    'properties': {
                        'gridcode': 12,
                        'toerv_pct': '6-12'
                    }
                }]
            }
        },
        {
            'batch_index': 1,
            'geojson_data': {
                'type': 'FeatureCollection',
                'features': [{
                    'type': 'Feature',
                    'geometry': {
                        'type': 'Polygon',
                        'coordinates': [[[595980, 6401240], [595970, 6401240], [595970, 6401250], [595980, 6401250], [595980, 6401240]]]
                    },
                    'properties': {
                        'gridcode': 12,
                        'toerv_pct': '6-12'
                    }
                }]
            }
        }
    ]
    
    try:
        result = combine_batches(test_batches)
        print(f"Combined GeoJSON data length: {len(result['geojson_data']['features'])}")
        print(f"Total features: {result['metadata'].get('total_features', 'unknown')}")
    except Exception as e:
        print(f"Error combining batches: {str(e)}")
        raise 