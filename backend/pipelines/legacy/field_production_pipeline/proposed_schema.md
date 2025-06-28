# Proposed Normalized Field Production Schema

## Current Problem
The current `field_production_pipeline` creates a single denormalized table that duplicates all agricultural fields data (~600k rows × 20+ columns) just to add yield estimates. This creates massive data duplication and storage inefficiency.

## Proposed Solution: Normalized Schema

### 1. Keep Existing Agricultural Fields Table
```sql
-- agricultural_fields_{year}_data.parquet (unchanged)
CREATE TABLE agricultural_fields (
    field_id VARCHAR,
    block_id VARCHAR,
    cvr_number VARCHAR,
    area_ha DOUBLE,
    crop_type VARCHAR,
    organic_farming DOUBLE,
    geometry_wkt VARCHAR,
    year BIGINT,
    created_at TIMESTAMP,
    data_source VARCHAR
);
```

### 2. Create New Field Yields Table (Only Yield Data)
```sql
-- field_yields_{year}_data.parquet (new, normalized)
CREATE TABLE field_yields (
    -- JOIN KEYS
    field_id VARCHAR NOT NULL,
    block_id VARCHAR NOT NULL,
    year BIGINT NOT NULL,
    
    -- YIELD DATA (only new information)
    yield_estimate_hkg_ha INTEGER,
    yield_source_table VARCHAR,
    yield_source_unit VARCHAR,
    yield_conversion_applied VARCHAR,
    production_estimate_hkg INTEGER,
    production_unit VARCHAR,
    
    -- DST MAPPING INFO
    has_dst_mapping BOOLEAN,
    dst_table VARCHAR,
    dst_category VARCHAR,
    dst_zone VARCHAR,
    
    -- METADATA
    estimation_method VARCHAR,
    created_at TIMESTAMP,
    
    -- PRIMARY KEY
    PRIMARY KEY (field_id, block_id, year)
);
```

## Benefits of Normalized Approach

### 1. **Massive Storage Savings**
- **Current**: 611,182 rows × 22 columns = ~13.4M data points
- **Proposed**: 
  - Agricultural fields: 611,182 rows × 9 columns = ~5.5M data points
  - Field yields: 227,381 rows × 12 columns = ~2.7M data points (only fields with yields)
  - **Total**: ~8.2M data points (**39% reduction**)

### 2. **Better Data Integrity**
- Agricultural fields data is single source of truth
- Yield estimates are separate concern
- No risk of field data inconsistency

### 3. **Flexible Querying**
```sql
-- Get fields with yields
SELECT f.*, y.yield_estimate_hkg_ha, y.production_estimate_hkg
FROM agricultural_fields f
JOIN field_yields y ON f.field_id = y.field_id 
    AND f.block_id = y.block_id 
    AND f.year = y.year
WHERE f.year = 2024;

-- Get all fields (with optional yields)
SELECT f.*, y.yield_estimate_hkg_ha, y.production_estimate_hkg
FROM agricultural_fields f
LEFT JOIN field_yields y ON f.field_id = y.field_id 
    AND f.block_id = y.block_id 
    AND f.year = y.year
WHERE f.year = 2024;

-- Get only fields without yields (for debugging)
SELECT f.*
FROM agricultural_fields f
LEFT JOIN field_yields y ON f.field_id = y.field_id 
    AND f.block_id = y.block_id 
    AND f.year = y.year
WHERE f.year = 2024 AND y.field_id IS NULL;
```

### 4. **Easier Maintenance**
- Update yield estimates without touching field geometry
- Recalculate yields for specific crops/regions
- Add new yield sources without schema changes

## Implementation Plan

### Phase 1: Update Field Production Pipeline
1. Modify `create_field_production_optimized()` to output only yield data
2. Change output schema to `field_yields` table
3. Update file naming: `field_yields_{year}.parquet`

### Phase 2: Update Consumers
1. Update any queries/analysis that use the current denormalized table
2. Create helper functions for common JOIN patterns
3. Update documentation and examples

### Phase 3: Migration
1. Convert existing `field_production_*.parquet` files to normalized format
2. Clean up old denormalized files
3. Update GCS storage paths

## Example Migration Code

```python
def migrate_to_normalized_schema(year: int):
    """Migrate existing denormalized field production data to normalized schema."""
    
    # Load existing denormalized data
    old_file = f"silver_field_production_{timestamp}_field_production_{year}.parquet"
    df = pd.read_parquet(old_file)
    
    # Extract only yield-specific columns
    yield_columns = [
        'field_id', 'block_id', 'year',
        'yield_estimate_hkg_ha', 'yield_source_table', 'yield_source_unit',
        'yield_conversion_applied', 'production_estimate_hkg', 'production_unit',
        'has_dst_mapping', 'dst_table', 'dst_category', 'dst_zone',
        'estimation_method', 'created_at'
    ]
    
    # Create normalized yield table (only rows with actual yield data)
    yield_df = df[yield_columns].dropna(subset=['yield_estimate_hkg_ha'])
    
    # Save normalized yield data
    yield_df.to_parquet(f"field_yields_{year}.parquet", index=False)
    
    print(f"Migrated {len(yield_df)} yield records for {year}")
    print(f"Reduced from {len(df)} total records ({len(yield_df)/len(df)*100:.1f}% have yields)")
```

## File Structure After Migration

```
silver/
├── agricultural_fields/
│   ├── 20241201_123456/
│   │   └── agricultural_fields_2024.parquet  # Geometry + basic field info
│   └── 20241201_123456/
│       └── agricultural_fields_2023.parquet
├── field_yields/
│   ├── 20241201_143022/
│   │   └── field_yields_2024.parquet         # Only yield estimates
│   └── 20241201_143022/
│       └── field_yields_2023.parquet
└── [other datasets...]
```

This approach follows database normalization principles and eliminates the massive data duplication while maintaining all functionality. 