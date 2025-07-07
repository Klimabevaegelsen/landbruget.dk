# Field Area Analysis Memory Optimization

## Overview

This document explains the memory optimizations implemented for the field area analysis gold layer to work within the 14GB memory limit and leverage the new DuckDB Spatial v1.2.2 spatial join operator.

## Problem

The original field area analysis was running out of memory due to:
1. Complex multi-table spatial joins that exceeded DuckDB's temp directory size limit (9.3 GiB)
2. Not leveraging the new DuckDB Spatial v1.2.2 spatial join operator properly
3. Insufficient memory management for large spatial datasets

## Solution

### 1. DuckDB Spatial v1.2.2 Optimization

Based on [DuckDB Spatial PR #545](https://github.com/duckdb/duckdb-spatial/pull/545), the new spatial join operator has limitations:
- **Single join condition only**: Multiple spatial joins in one query aren't optimized
- **INNER/LEFT/RIGHT/OUTER joins supported**: For predicates like `ST_Intersects`, `ST_Contains`, etc.
- **Temporary spatial index**: Creates on-the-fly spatial index on build side

#### Before (Memory Intensive)
```sql
-- Complex 3-table join causing memory overflow
CREATE TABLE field_wetland_water_overlap AS
SELECT 
    f.field_id,
    f.block_id,
    ST_Area(ST_Intersection(ST_Intersection(f.geom, w.geom), wp.geom)) / ST_Area(f.geom) * 100 as share
FROM current_fields f
JOIN wetlands w ON ST_Intersects(f.geom, w.geom)
JOIN water_projects wp ON ST_Intersects(f.geom, wp.geom)
```

#### After (Memory Optimized)
```sql
-- Step 1: Single spatial join (uses SPATIAL_JOIN operator)
CREATE TABLE fields_with_wetlands AS
SELECT 
    f.field_id,
    f.block_id,
    f.geom as field_geom,
    ST_Intersection(f.geom, w.geom) as wetland_intersection_geom
FROM current_fields f
JOIN wetlands w ON ST_Intersects(f.geom, w.geom)

-- Step 2: Another single spatial join (uses SPATIAL_JOIN operator)
CREATE TABLE field_wetland_water_overlap AS
SELECT 
    fw.field_id,
    fw.block_id,
    ST_Area(ST_Intersection(fw.wetland_intersection_geom, wp.geom)) / ST_Area(fw.field_geom) * 100 as share
FROM fields_with_wetlands fw
JOIN water_projects wp ON ST_Intersects(fw.wetland_intersection_geom, wp.geom)

-- Cleanup intermediate table immediately
DROP TABLE fields_with_wetlands
```

### 2. Memory Configuration

#### Conservative Memory Limits
```python
# Very conservative configuration for 14GB system
memory_limit = "6GB"        # 43% of available memory
max_temp_directory_size = "8GB"  # 57% of available memory
threads = 1                 # Single thread for memory-intensive operations
```

#### Memory Monitoring
```python
def _check_memory_usage(self):
    """Monitor memory usage throughout processing."""
    process = psutil.Process(os.getpid())
    memory_gb = process.memory_info().rss / (1024**3)
    
    if memory_gb > 10:  # Warning threshold
        self.log.warning(f"⚠️ High memory usage: {memory_gb:.1f}GB")
```

### 3. Sequential Processing

#### Year-by-Year Processing
- Process each year separately to avoid memory accumulation
- Immediate cleanup after each year
- Save results per year to avoid large result sets

#### Aggressive Cleanup
```python
def _cleanup_year_processing(self, fields_table_name: str, year: int):
    """Clean up all intermediate tables after processing a year."""
    intermediate_tables = [
        "field_property_intersections",
        "field_soil_intersections", 
        "field_bnbo_intersections",
        # ... all intermediate tables
    ]
    
    for table in intermediate_tables:
        self.conn.execute(f"DROP TABLE IF EXISTS {table}")
    
    self._force_duckdb_checkpoint()
    self._cleanup_temp_files()
```

### 4. Spatial Join Optimization

#### Single Join Conditions
All spatial joins now use single conditions to leverage the SPATIAL_JOIN operator:

```sql
-- ✅ Optimized: Single spatial join condition
FROM current_fields f
JOIN properties p ON ST_Intersects(f.geom, p.geom)

-- ❌ Avoided: Multiple conditions in one join
FROM current_fields f
JOIN properties p ON ST_Intersects(f.geom, p.geom)
JOIN soil_types s ON ST_Intersects(f.geom, s.geom)
```

#### Immediate Cleanup
```sql
-- Process → Cleanup → Checkpoint cycle
CREATE TABLE field_property_intersections AS ...
-- Process the data
CREATE TABLE field_property_shares AS ...
-- Cleanup immediately
DROP TABLE field_property_intersections;
-- Force checkpoint
CHECKPOINT;
```

## Performance Improvements

### Memory Usage
- **Before**: 9.3 GiB temp directory usage → Out of Memory
- **After**: <8 GiB temp directory limit with monitoring

### Spatial Join Performance
- **Before**: Blockwise nested-loop join (O(n×m) comparisons)
- **After**: Spatial join operator with temporary spatial index (much faster)

### Processing Strategy
- **Before**: All years at once → Memory overflow
- **After**: Year-by-year processing → Memory controlled

## Configuration

### Recommended Settings
```python
config = FieldAreaAnalysisGoldConfig(
    memory_limit="6GB",         # Conservative for 14GB system
    thread_count=1,             # Single thread for stability
    batch_size=2500,            # Reasonable batch size
    min_area_threshold=0.01     # 1% minimum area threshold
)
```

### Environment Variables
```bash
export GCS_BUCKET="landbrugsdata-raw-data"
export DUCKDB_TEMP_DIRECTORY="/tmp/duckdb_field_analysis"
```

## Testing

### Memory Test Script
```bash
python scripts/testing/field_area_analysis_memory_test.py
```

### Production Run
```bash
python scripts/analysis/run_field_area_analysis_optimized.py
```

## Monitoring

### Memory Monitoring
- Process memory usage tracking
- System memory availability checks
- Warning thresholds at 10GB process usage

### Disk Monitoring
- Temp directory size tracking
- Main disk space monitoring
- Cleanup verification

## Results

The optimized field area analysis:
1. ✅ Stays within 14GB memory limit
2. ✅ Leverages DuckDB Spatial v1.2.2 spatial join operator
3. ✅ Processes large spatial datasets efficiently
4. ✅ Provides comprehensive memory and disk monitoring
5. ✅ Maintains data quality and completeness

## Future Improvements

1. **Batch Size Optimization**: Tune batch size based on available memory
2. **Parallel Processing**: Re-enable parallel processing when memory allows
3. **Spatial Index Persistence**: Cache spatial indexes between runs
4. **Streaming Results**: Stream results directly to GCS without intermediate storage 