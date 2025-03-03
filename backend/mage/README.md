# Mage.ai Pipeline Development Guide

This guide outlines best practices, learnings, and contribution guidelines for developing large-scale geospatial data pipelines with Mage.ai, derived from our implementation of the wetlands data processing pipeline.

## Table of Contents

1. [Core Principles](#core-principles)
2. [Pipeline Architecture](#pipeline-architecture)
3. [Memory Management](#memory-management)
4. [Error Handling](#error-handling)
5. [Deployment](#deployment)
6. [Contributing](#contributing)
7. [Dependencies](#dependencies)
8. [Testing](#testing)
9. [Known Limitations](#known-limitations)

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

## Contributing

### Local Development Setup

1. **Prerequisites**:
   - Docker installed
   - Git installed
   - Repository access

2. **Setup Steps**:
   ```bash
   # Clone repository
   git clone https://github.com/Klimabevaegelsen/landbruget.dk.git
   cd landbruget.dk
   
   # Create feature branch
   git checkout -b feature/your-feature-name
   
   # Build and run Mage.ai locally
   docker build -t mage-local -f backend/mage.Dockerfile .
   docker run -it -p 6789:6789 mage-local
   ```

   > **Note**: The Dockerfile is configured to automatically run in development mode when built locally.
   > Build arguments `MAGE_ENVIRONMENT` and `MAGE_DEV_MODE` default to `dev` and `true` respectively.

3. **Access UI**: Open http://localhost:6789

### Making Changes

1. **Creating/Modifying Pipelines**:
   - Navigate to "Pipelines" in UI
   - Click "New pipeline" or select existing
   - Add/modify blocks as needed

2. **Adding Custom Code**:
   ```python
   from mage_ai.data_preparation.repo_manager import get_repo_path
   from os import path
   import sys

   repo_path = get_repo_path()
   sys.path.append(repo_path)

   from custom.your_module import your_function
   ```

### Best Practices

1. Keep changes focused on single concerns
2. Document complex logic
3. Use descriptive names
4. Create reusable utility functions
5. Test thoroughly
6. Update documentation

### Submitting Changes

1. **Commit**:
   ```bash
   git add backend/mage/
   git commit -m "Description of changes"
   ```

2. **Push and Create PR**:
   ```bash
   git push origin feature/your-feature-name
   ```
   Then create PR on GitHub with detailed description

## Deployment

### Local Development with Docker
```bash
# Build the local development image
docker build -t mage-local -f backend/mage.Dockerfile .

# Run the container with port forwarding
docker run -it -p 6789:6789 mage-local
```

The Dockerfile is configured to:
- Use development mode by default for local builds
- Set appropriate environment variables automatically
- Copy all necessary files from backend/mage to the container

### Production Deployment (via GitHub Actions)
Production deployment is handled automatically by the GitHub Actions workflow when changes are pushed to the main branch:

1. The workflow builds the Docker image with production settings:
   ```
   docker build --build-arg MAGE_ENVIRONMENT=production --build-arg MAGE_DEV_MODE=false -f backend/mage.Dockerfile .
   ```

2. The built image is pushed to Google Container Registry

3. Cloud Run service is updated with the new image and environment variables:
   ```
   MAGE_ENVIRONMENT=production
   GCS_BUCKET_NAME=europe-west1-landbrug-bc7a96db-bucket
   ```

From `metadata.yaml`:
```yaml
production:
  executor_type: gcp_cloud_run
  remote_variables_dir: gs://${GCS_BUCKET_NAME}/mage_data
  storage:
    type: gcs
    bucket: ${GCS_BUCKET_NAME}
```

Required environment variables:
```bash
MAGE_ENVIRONMENT=production
GCS_BUCKET_NAME=your-bucket
GOOGLE_APPLICATION_CREDENTIALS=path/to/credentials.json
```

## Dependencies

From our actual `requirements.txt` and `Dockerfile`:
```dockerfile
# Core dependencies
shapely>=2.0.7
geopandas>=1.0.1
dask[dataframe]>=2024.3.0
dask-geopandas>=0.4.2
pyarrow>=15.0.0
distributed>=2024.6.0
psutil>=5.9.0
```

## Testing

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