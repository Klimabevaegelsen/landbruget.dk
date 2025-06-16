# Property Owners - Cadastral Data Merge Pipeline

This pipeline merges property ownership data from the Danish SFTP system with cadastral parcel data from the WFS system to create a unified dataset linking property owners to their cadastral parcels.

## Overview

**Purpose**: Link property ownership information with cadastral parcel geometries through spatial relationships.

**Input Sources**:
- Property owners data (from SFTP pipeline) - contains privacy-transformed ownership information
- Cadastral data (from WFS pipeline) - contains parcel geometries and administrative metadata

**Output**: Unified parquet files with property ownership linked to cadastral parcels

## Data Flow

```
Property Owners (Silver) + Cadastral (Silver) → Spatial Join → Merged Dataset (Silver)
```

## Key Features

### 1. Spatial Joining
- **Methods**: intersects, within, contains
- **Buffer tolerance**: Configurable buffer distance for imprecise spatial matches
- **Overlap threshold**: Minimum overlap ratio for valid matches

### 2. Privacy Preservation
- Maintains privacy transformations from property owners pipeline
- CPR numbers remain as UUIDs
- Personal address information stays removed

### 3. Data Quality
- Geometry validation and transformation
- Match rate reporting
- Duplicate handling
- Quality thresholds for validation

### 4. Flexible Configuration
- Configurable spatial join methods
- Adjustable quality thresholds
- Optional local/GCS output

## Configuration

### Basic Configuration

```json
{
  "spatial_join_method": "intersects",
  "buffer_distance_meters": 10.0,
  "min_overlap_threshold": 0.1
}
```

### Spatial Join Methods

- **`intersects`** (default): Property geometry intersects with cadastral parcel
- **`within`**: Property geometry is completely within cadastral parcel  
- **`contains`**: Property geometry completely contains cadastral parcel

### Buffer Distance
- Applied to property geometries before spatial join
- Useful for accounting for GPS accuracy or surveying differences
- Specified in meters (converted using Danish UTM Zone 32N)

### Overlap Threshold
- Minimum ratio of intersection area to property area
- Filters out spurious matches with minimal overlap
- Range: 0.0 (no filtering) to 1.0 (complete overlap required)

## Usage

### Basic Usage

```bash
python run_property_cadastral_merge.py
```

### Advanced Usage

```bash
python run_property_cadastral_merge.py \
  --spatial-method intersects \
  --buffer-distance 10.0 \
  --min-overlap 0.1 \
  --config-file config/property_cadastral_merge_config.json
```

### Dry Run

```bash
python run_property_cadastral_merge.py --dry-run
```

## Output Schema

The merged dataset includes:

### Property Owner Fields
- `ejendePerson` - Privacy-transformed person data
- `geometry` - Property geometry (EPSG:4326)
- `property_id` - Unique property identifier

### Cadastral Fields
- `cadastral_bfe_number` - BFE number
- `cadastral_registration_from` - Registration date
- `cadastral_effect_from` - Effective date
- `cadastral_authority` - Authority information
- `cadastral_agricultural_notation` - Agricultural use notes
- `cadastral_is_worker_housing` - Worker housing flag
- `cadastral_is_common_lot` - Common lot flag
- `cadastral_has_owner_apartments` - Owner apartments flag

### Merge Metadata
- `merge_timestamp` - When the merge was performed
- `merge_method` - Spatial join method used
- `has_cadastral_match` - Boolean indicating successful match
- `match_quality_score` - Overlap ratio (if applicable)

## Data Quality Checks

### Match Rate Monitoring
- Reports percentage of properties successfully matched to cadastral parcels
- Configurable thresholds for quality validation
- Alerts if match rate falls below expected levels

### Geometry Validation
- Validates all geometries before and after merge
- Repairs invalid geometries where possible
- Reports geometry issues

### Overlap Analysis
- Calculates intersection areas for matched properties
- Filters matches below minimum overlap threshold
- Provides statistics on match quality

## Performance Considerations

### Memory Management
- Processes data in chunks to handle large datasets
- Temporary file cleanup
- Efficient spatial indexing

### Processing Time
- Spatial joins can be computationally expensive
- Buffer operations add processing overhead
- Consider reducing overlap threshold for faster processing

### Storage Efficiency
- Output in compressed Parquet format
- Column-based storage for analytics
- Efficient spatial data encoding

## Error Handling

### Missing Input Data
- Graceful handling when property owners or cadastral data is unavailable
- Clear error messages with debugging information
- Ability to continue with partial data

### Geometry Issues
- Invalid geometries are logged and either repaired or excluded
- Coordinate system mismatches are automatically resolved
- Empty or null geometries are handled appropriately

### Quality Thresholds
- Configurable quality gates that can fail the pipeline
- Detailed reporting of quality metrics
- Option to proceed with warnings vs. hard failures

## Monitoring and Logging

### Log Levels
- INFO: Progress updates and statistics
- WARNING: Data quality issues and recoverable errors
- ERROR: Critical failures that stop processing

### Key Metrics
- Total properties processed
- Successful matches count and percentage
- Processing time and performance statistics
- Data quality scores

### Output Files
- Main merged dataset: `property_cadastral_merged_YYYYMMDD_HHMMSS.parquet`
- Log file: `property_cadastral_merge.log`
- Quality report: Embedded metadata in output

## Integration

### Upstream Dependencies
- Property owners SFTP pipeline must complete successfully
- Cadastral WFS pipeline must complete successfully
- Both datasets must be available in silver layer

### Downstream Usage
- Analytics and reporting systems
- Frontend applications requiring property-cadastral linkage
- Further data enrichment pipelines

### Scheduling
- Recommended frequency: Weekly (after both input pipelines complete)
- Can be triggered manually or via scheduling system
- Idempotent operation safe for reruns

## Troubleshooting

### Common Issues

1. **Low match rate**
   - Check geometry quality in input datasets
   - Consider increasing buffer distance
   - Verify coordinate systems are consistent

2. **Memory errors**
   - Reduce chunk size in processing
   - Ensure sufficient disk space for temporary files
   - Monitor system resources during execution

3. **Geometry validation failures**
   - Review source data quality
   - Check for coordinate system issues
   - Validate input geometries manually

### Debug Mode
- Use `--dry-run` to validate inputs without processing
- Check log files for detailed error information
- Examine intermediate outputs if save_local is enabled

## Development

### Testing
- Unit tests for spatial join logic
- Integration tests with sample datasets
- Performance benchmarks

### Configuration Updates
- Modify `config/property_cadastral_merge_config.json`
- Update schema mappings as source data evolves
- Add new quality checks as needed

### Performance Optimization
- Spatial indexing improvements
- Parallel processing for large datasets
- Memory usage optimization 