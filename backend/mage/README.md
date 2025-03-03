# Mage.ai Pipeline Technical Documentation

This document outlines the architecture, best practices, and technical implementation details of our large-scale geospatial data pipelines with Mage.ai, derived from our implementation of the wetlands data processing pipeline.

> **Looking to contribute?** Please see the [CONTRIBUTING.md](./CONTRIBUTING.md) file for detailed setup instructions and workflow guidelines.

## Table of Contents

1. [Core Principles](#core-principles)
2. [Pipeline Architecture](#pipeline-architecture)
3. [Memory Management](#memory-management)
4. [Error Handling](#error-handling)
5. [Deployment](#deployment)
6. [Dependencies](#dependencies)
7. [Testing](#testing)
8. [Known Limitations](#known-limitations)

## Core Principles

1. **Early Format Standardization**: Convert to standard formats (e.g., EPSG:4326) at data loading
2. **Memory-Efficient Processing**: Implement chunked processing and garbage collection
3. **Environment-Aware Storage**: Seamless switching between local and cloud storage
4. **Comprehensive Logging**: Visual feedback with emojis in Mage UI
5. **Robust Error Handling**: Graceful degradation with context

## Pipeline Architecture

### Project Structure
```
backend/mage/
├── data_loaders/      # Data loader blocks
├── transformers/      # Transformer blocks
├── data_exporters/    # Data exporter blocks
├── pipelines/         # Pipeline definitions
├── custom/           # Custom code and functions
├── utils/            # Utility functions
├── io_config.yaml    # Data source configuration
└── metadata.yaml     # Project metadata
```

### 1. Data Loading
From `wetlands_load_wfs.py`:
```python
@data_loader
def load_data(**kwargs):
    config = kwargs.get('config', {})
    batch_size = int(config.get('batch_size', 50000))
    
    # Early format standardization
    params = {
        'SRSNAME': 'EPSG:4326',
        'outputFormat': 'application/json'
    }
```

Key learnings:
- Use dynamic mapping for parallel processing
- Convert to standard formats early
- Implement robust error handling
- Log progress clearly

### 2. Batch Processing
From `wetlands_process_batch.py`:
```python
def process_batch(data, *args, **kwargs):
    # Memory-efficient processing
    batch_index = data.get('batch_index', 0)
    start_index = batch_index * batch_size
    
    print(f"\n🔄 Processing batch {batch_index+1} of {metadata['num_batches']}")
```

Key learnings:
- Process in manageable chunks
- Track progress with clear logging
- Maintain data lineage
- Handle errors gracefully

### 3. Data Storage
From `wetlands_store_processed.py`:
```python
# Environment-aware storage
config_profile = 'production' if os.getenv('MAGE_ENVIRONMENT') == 'production' else 'default'
if config_profile == 'production':
    # Use Google Cloud Storage
    from google.cloud import storage
    client = storage.Client()
else:
    # Use local storage
    storage_path = os.path.join(base_path, file_name)
```

Key learnings:
- Implement environment-aware storage
- Lazy import of cloud dependencies
- Validate data before storage
- Maintain clear logging

## Memory Management

### 1. Chunked Processing
From `wetlands_merge_grid.py`:
```python
def process_chunk(chunk_gdf: gpd.GeoDataFrame, gridcode: int):
    # Process in memory-efficient chunks
    filtered_df = chunk_gdf[chunk_gdf['gridcode'] == gridcode].copy()
    gc.collect()  # Force garbage collection
```

### 2. Resource Monitoring
```python
import psutil

def log_memory_usage():
    memory_used = psutil.virtual_memory().used / (1024**3)
    print(f"📊 Memory usage: {memory_used:.2f} GB")
```

## Error Handling

```python
if not validate_input(data):
    error_msg = "❌ Invalid input format"
    print(error_msg)
    raise ValueError(f"{error_msg}: {data_format}")
```

Key patterns:
- Clear error messages with emojis
- Context in exceptions
- Graceful degradation
- Clean up resources in finally blocks

## Deployment

Our Mage.ai pipeline is deployed on Google Cloud Run. The deployment is managed automatically by GitHub Actions workflows.

### Local Development
For local development instructions, refer to the [CONTRIBUTING.md](./CONTRIBUTING.md) file.

### Production Architecture
In production:
- The pipeline runs on Google Cloud Run with increased resources
- Data is stored in Google Cloud Storage
- Authentication uses Cloud Run's service identity
- Pipeline metadata contains environment-specific settings

From `metadata.yaml`:
```yaml
production:
  executor_type: gcp_cloud_run
  remote_variables_dir: gs://${GCS_BUCKET_NAME}/mage_data
  storage:
    type: gcs
    bucket: ${GCS_BUCKET_NAME}
```

Critical environment variables:
```bash
MAGE_ENVIRONMENT=production
GCS_BUCKET_NAME=europe-west1-landbrug-bc7a96db-bucket
```

## Dependencies

Critical geospatial dependencies:
```
shapely>=2.0.7
geopandas>=1.0.1
dask[dataframe]>=2024.3.0
dask-geopandas>=0.4.2
pyarrow>=15.0.0
distributed>=2024.6.0
psutil>=5.9.0
```

For the complete list, see the requirements.txt file.

## Testing

Our pipelines include built-in test functions for validation:

```python
@test
def test_output(*args):
    """Validate component output"""
    assert len(args) > 0, 'No output produced'
    validate_format(args[0])
    check_performance_metrics(args[0])
```

Key testing patterns:
- Validate input/output formats
- Check resource usage
- Test error handling
- Verify data quality

## Known Limitations

1. **Memory Usage**:
   - Grid merging operations are memory-intensive
   - Use chunked processing for large datasets
   - Monitor memory usage with psutil

2. **WFS Service**:
   - Rate limiting may apply
   - Batch size of 50,000 is optimal
   - Early format standardization is crucial

3. **Storage**:
   - Local vs. Cloud storage requires different handling
   - Lazy import of cloud dependencies
   - Environment-aware configuration 