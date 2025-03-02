import requests
import xml.etree.ElementTree as ET
import logging
import math

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


def get_wfs_capabilities(wfs_url: str) -> dict:
    """Get WFS capabilities including feature type details."""
    
    params = {
        'SERVICE': 'WFS',
        'REQUEST': 'GetCapabilities',
        'VERSION': '2.0.0'
    }
    
    try:
        print(f"🔍 Fetching WFS capabilities from {wfs_url}")
        response = requests.get(wfs_url, params=params)
        response.raise_for_status()
        
        root = ET.fromstring(response.content)
        
        # Get feature type details - simpler XPath
        feature_types = {}
        for feature_type in root.findall('.//{*}FeatureType'):
            name_elem = feature_type.find('.//{*}Name')
            if name_elem is not None:
                name = name_elem.text
                feature_types[name] = {
                    'title': feature_type.findtext('.//{*}Title'),
                    'default_crs': feature_type.findtext('.//{*}DefaultCRS'),
                }
        
        if not feature_types:
            raise RuntimeError("No feature types found in WFS capabilities response")
            
        print(f"✨ Found feature types: {list(feature_types.keys())}")
        return feature_types
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to connect to WFS service: {str(e)}")
        raise  # Re-raise the original exception
    except ET.ParseError as e:
        print(f"❌ Failed to parse WFS capabilities XML: {str(e)}")
        raise  # Re-raise the original exception
    except Exception as e:
        print(f"❌ Unexpected error in get_wfs_capabilities: {str(e)}")
        raise RuntimeError(f"Failed to get WFS capabilities: {str(e)}") from e


@data_loader
def load_wetlands_data(**kwargs):
    """
    Load wetlands data from Danish WFS service.
    Designed for fetching large datasets in batches.
    
    Returns data in the format expected by dynamic blocks:
    [
        [batch_item1, batch_item2, ...],  # Data for each batch
        [metadata1, metadata2, ...]        # Metadata for each batch
    ]
    """
    
    # Parameters with sensible defaults
    config = kwargs.get('config', {})
    wfs_url = config.get('wetlands_url', 'https://wfs2-miljoegis.mim.dk/natur/ows')
    layer_name = config.get('layer_name', 'natur:kulstof2022')
    batch_size = int(config.get('batch_size', 50000))
    
    print("\n🚀 Starting wetlands data load")
    print(f"📡 WFS URL: {wfs_url}")
    print(f"📂 Layer: {layer_name}")
    print(f"📦 Batch size: {batch_size:,}")
    
    try:
        # Get WFS capabilities first
        feature_types = get_wfs_capabilities(wfs_url)
        
        if layer_name not in feature_types:
            available_layers = list(feature_types.keys())
            raise RuntimeError(f"Layer {layer_name} not found. Available layers: {available_layers}")
            
        layer_info = feature_types[layer_name]
        print(f"📋 Layer info: {layer_info}")
        
        # Get feature count using a simple GetFeature request with resultType=hits
        count_params = {
            'SERVICE': 'WFS',
            'VERSION': '2.0.0',
            'REQUEST': 'GetFeature',
            'TYPENAMES': layer_name,
            'resultType': 'hits'
        }
        
        response = requests.get(wfs_url, params=count_params)
        response.raise_for_status()
        root = ET.fromstring(response.content)
        total_features = int(root.get('numberMatched', 0))
        
        if total_features == 0:
            raise RuntimeError(f"No features found for layer {layer_name}")
        
        print(f"📊 Total features available: {total_features:,}")
        
        # Calculate number of batches needed
        num_batches = (total_features + batch_size - 1) // batch_size
        print(f"🔄 Will process {total_features:,} features in {num_batches:,} batches of {batch_size:,}")
        
        # Create batch items and metadata
        data_items = []
        metadata_items = []
        
        for batch_index in range(num_batches):
            batch_item = {
                'batch_index': batch_index,
                'metadata': {
                    'wfs_url': wfs_url,
                    'layer_name': layer_name,
                    'batch_size': batch_size,
                    'num_batches': num_batches,
                    'total_features': total_features
                }
            }
            data_items.append(batch_item)
            
            metadata_items.append({
                'block_uuid': f'batch_{batch_index}'
            })
        
        print(f"✅ Successfully created {num_batches:,} batch configurations")
        return [data_items, metadata_items]
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to connect to WFS service: {str(e)}")
        raise  # Re-raise the original exception
    except ET.ParseError as e:
        print(f"❌ Failed to parse WFS response: {str(e)}")
        raise  # Re-raise the original exception
    except Exception as e:
        print(f"❌ Unexpected error in load_wetlands_data: {str(e)}")
        raise RuntimeError(f"Failed to load wetlands data: {str(e)}") from e


@test
def test_output(*args):
    """
    Test the output of the data loader.
    Validates that the output matches the expected format for dynamic mapping
    and contains all required fields for wetlands_process_batch.py.
    """
    # Check that we received at least one argument
    assert len(args) > 0, 'No output was produced'
    
    # Get the first argument
    data = args[0]
    
    if isinstance(data, list) and len(data) == 2:
        # Standard case - we received [data_items, metadata_items]
        assert len(data_items) > 0, 'Data items list should not be empty'
        assert len(metadata_items) > 0, 'Metadata items list should not be empty'
        assert len(data_items) == len(metadata_items), 'Data items and metadata items should have the same length'
        
        # Check first data item has all required fields for wetlands_process_batch.py
        first_data_item = data_items[0]
        assert 'batch_index' in first_data_item, 'Each data item should contain batch_index'
        assert 'metadata' in first_data_item, 'Each data item should contain metadata'
        
        # Validate metadata contains all required fields
        required_metadata_fields = {
            'wfs_url': str,
            'layer_name': str,
            'batch_size': int,
            'num_batches': int
        }
        
        for field, expected_type in required_metadata_fields.items():
            assert field in first_data_item['metadata'], f'Metadata missing required field: {field}'
            assert isinstance(first_data_item['metadata'][field], expected_type), \
                f'Metadata field {field} should be of type {expected_type}'
        
        # Check first metadata item
        first_metadata_item = metadata_items[0]
        assert 'block_uuid' in first_metadata_item, 'Each metadata item should contain block_uuid'
    else:
        # If we're getting a single item (e.g., in reduce_output mode)
        if isinstance(data, dict):
            assert 'batch_index' in data, 'Output should contain batch_index'
            assert 'metadata' in data, 'Output should contain metadata'
            
            # Validate metadata contains all required fields
            required_metadata_fields = {
                'wfs_url': str,
                'layer_name': str,
                'batch_size': int,
                'num_batches': int
            }
            
            for field, expected_type in required_metadata_fields.items():
                assert field in data['metadata'], f'Metadata missing required field: {field}'
                assert isinstance(data['metadata'][field], expected_type), \
                    f'Metadata field {field} should be of type {expected_type}'


if __name__ == "__main__":
    # For local testing
    result = load_wetlands_data()
    print(f"Created {len(result[0])} batch items for dynamic mapping")
    if len(result[0]) > 0:
        first_batch = result[0][0]
        print(f"Sample batch item: Batch index {first_batch['batch_index']}")
        print(f"Total features: {first_batch['metadata']['total_features']}")
        print(f"Total batches: {first_batch['metadata']['num_batches']}") 