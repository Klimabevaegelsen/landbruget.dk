# Field Overview Pipeline

This pipeline generates comprehensive agricultural field overviews with production estimates for each year. It combines agricultural field data with DST (Danish Statistics) yield data to provide production estimates for individual fields.

## Features

- **Optimized for DuckDB Spatial v1.2.2**: Leverages the new SPATIAL_JOIN operator (single spatial predicate only, per PR #545)
- **Production Estimates**: Uses DST data to estimate production based on crop type and regional yields
- **Spatial Optimization**: Uses native DuckDB geometry columns and spatial indexing
- **Hybrid Processing**: Smart processing that uses fast single-zone processing for most fields (~99.8%) and area-weighted calculations only for multi-zone fields (~0.2%)
- **Batch Processing**: Processes fields in batches of 5000 for optimal SPATIAL_JOIN efficiency

## Data Sources

### Input Data
- **Agricultural Fields**: Loads from silver layer (`silver/agricultural_fields/agricultural_fields_{year}_data.parquet`)
- **DST Data**: Regional yield data from multiple DST tables (HST77, GARTN1, FRO, HALM1)
- **DST Zone Mapping**: Spatial zones for regional yield calculations

### Output Data
- **Field Overview**: Comprehensive field data with production estimates saved to silver layer (`silver/field_overview/{timestamp}/field_overview_{year}.parquet`)

## Output Schema

The pipeline produces a comprehensive field overview with the following columns:

### Basic Field Information
- `field_id`: Field identifier
- `block_id`: Block identifier  
- `cvr_number`: CVR number (company identifier)
- `area_ha`: Field area in hectares
- `crop_type`: Crop type name
- `organic_farming`: Whether the field uses organic farming

### Spatial Information
- `dst_zone`: DST zone for regional yield data
- `geometry_wkt`: Field geometry in WKT format

### Production Estimates
- `yield_estimate_hkg_ha`: Estimated yield in hectograms per hectare
- `yield_source_table`: DST table used for yield estimate
- `yield_source_unit`: Description of yield calculation method
- `yield_conversion_applied`: Any unit conversions applied
- `production_estimate_hkg`: Total estimated production in hectograms
- `production_unit`: Unit of production estimate (hkg)

### DST Mapping Information
- `has_dst_mapping`: Whether the crop has DST mapping
- `dst_table`: DST table used for this crop
- `dst_category`: DST category for this crop

### Metadata
- `year`: Processing year
- `created_at`: Processing timestamp
- `data_source`: Source data file
- `estimation_method`: Method used for yield estimation

## Usage

### Local Development
```bash
# Process a specific year
python main.py --year 2024

# Process all available years
python main.py --all-years

# Specify output directory
python main.py --year 2024 --output-dir /path/to/output
```

### Production (with GCS)
```bash
# Process with GCS storage
python main.py --year 2024 --bucket your-gcs-bucket

# Process all years with GCS
python main.py --all-years --bucket your-gcs-bucket
```

### Command Line Arguments

- `--year`: Specific year to process (2020-2025)
- `--all-years`: Process all available years
- `--output-dir`: Output directory for results (default: data/silver)
- `--bucket`: GCS bucket name for production use
- `--log-level`: Logging level (DEBUG, INFO, WARNING, ERROR, default: INFO)

## Technical Details

### DuckDB Spatial Optimization

The pipeline is specifically optimized for DuckDB Spatial v1.2.2's SPATIAL_JOIN operator:

1. **Native Geometry Columns**: Uses DuckDB GEOMETRY columns instead of WKT strings
2. **Spatial Indexing**: Pre-builds spatial index on DST zones (build side)
3. **Single Spatial Predicate**: Uses only `ST_Intersects()` to trigger SPATIAL_JOIN (per PR #545 requirement)
4. **Batch Processing**: Large batches (5000 fields) to amortize index creation costs
5. **Memory Management**: Efficient WKB storage and cleanup of temporary tables

### Performance Characteristics

- **Processing Speed**: ~600 fields/second with SPATIAL_JOIN optimization
- **Memory Usage**: Configured for 8GB memory limit with 4 threads
- **Scalability**: Handles 600k+ fields efficiently with batch processing

### Yield Estimation Methods

1. **Single Zone Fields** (~99.8%): Fast lookup using zone-specific yields
2. **Multi Zone Fields** (~0.2%): Area-weighted calculation across intersecting zones
3. **Fallback**: National averages when regional data unavailable

## Dependencies

- `duckdb` with spatial extension
- `geopandas` for geospatial data handling
- `pandas` for data manipulation
- `google-cloud-storage` for GCS operations (production)

## Data Requirements

### Local Development
- Agricultural fields data in `data_cache/agricultural_fields/`
- DST data in `data_cache/dst_pipeline/`
- DST zone mapping in `data_cache/dst_zone_mapping/`

### Production
- Agricultural fields data in GCS silver layer
- DST data available locally (cached)
- DST zone mapping available locally (cached)

## Output Examples

### Summary Statistics
```
Optimized Overview Summary for 2024:
  Total fields: 616,919
  Fields with production estimates: 227,381 (36.9%)
  Total area: 2,547,123.4 ha
  Total estimated production: 94,123,456 hkg
```

### Performance Metrics
```
Processing 616,919 fields in 124 batches of 5000
✅ SPATIAL_JOIN operator detected in query plan!
  Single zone fields: 4,987 (fast processing)
  Multi zone fields: 13 (area-weighted processing)
```

## Error Handling

- **Missing Data**: Gracefully handles missing agricultural fields or DST data
- **Spatial Errors**: Falls back to simpler processing if spatial operations fail
- **Memory Management**: Automatic cleanup of temporary DuckDB tables
- **File Operations**: Robust error handling for GCS operations with cleanup

## Monitoring

The pipeline provides detailed logging including:
- SPATIAL_JOIN operator detection
- Processing speed metrics
- Single vs multi-zone field breakdown
- Summary statistics per year
- Error details with stack traces

## Future Enhancements

- Support for additional DST tables
- Integration with real-time weather data
- Field-level soil quality adjustments
- Crop rotation impact modeling 