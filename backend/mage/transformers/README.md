# Wetlands Polygon Merge

This module provides memory-efficient solutions for merging adjacent polygons in large geospatial datasets using Dask and GeoPandas.

## Overview

The `wetlands_merge_grid.py` module implements a parallel processing solution for merging adjacent polygons that share the same grid code. This is particularly useful for simplifying complex wetland datasets while preserving important spatial relationships.

## Key Features

- **Memory Efficiency**: Processes large GeoParquet files in chunks to avoid out-of-memory errors
- **Parallel Processing**: Uses Dask for distributed computing across multiple CPU cores
- **Auto-Scaling**: Automatically switches between direct GeoPandas (for smaller files) and Dask (for larger files)
- **Robust Polygon Merging**: Identifies and merges adjacent polygons based on shared boundaries
- **Detailed Statistics**: Tracks reduction percentages and performance metrics

## Implementation Details

### Memory Management

The implementation includes several strategies to minimize memory usage:

1. **Chunked Reading**: Data is read in manageable chunks using PyArrow
2. **Distributed Processing**: Workload is distributed across worker processes with memory limits
3. **Lazy Evaluation**: Dask provides deferred execution until results are needed
4. **Optimized Geometries**: Uses libpysal for efficient spatial weights and component analysis

### Algorithm

The polygon merging algorithm works as follows:

1. Read and filter data by grid code (only 12 and 60)
2. For each grid code:
   - Create a spatial weights matrix using Queen contiguity
   - Identify connected components (adjacent polygons)
   - Dissolve polygons within each component
   - Track statistics about the merging process
3. Return merged polygons with metadata

## Performance Considerations

- For files under 1GB, uses direct GeoPandas for simplicity
- For larger files, switches to Dask for parallel processing
- Each worker has a configurable memory limit (default 2GB per worker)
- Falls back to a simpler merging method if libpysal is not available

## Requirements

The implementation requires the following dependencies:
- geopandas >= 1.0.0
- dask >= 2024.3.0
- dask-geopandas >= 0.4.2
- shapely >= 2.0.0
- libpysal >= 4.12.0
- pyarrow >= 15.0.0
- distributed >= 2024.6.0

## Usage

```python
# Example usage
from backend.mage.transformers.wetlands_merge_grid import merge_grid

result = merge_grid({
    'input_path': 'path/to/wetlands.parquet',
    'metadata': {'source_crs': 25832},
    'max_workers': 4,       
    'memory_limit': 2       # 2GB memory limit per worker
})

# Access results
print(f"Merged {result['metadata']['merged_features_count']} features")
print(f"Processing time: {result['metadata']['processing_time_seconds']} seconds")
``` 