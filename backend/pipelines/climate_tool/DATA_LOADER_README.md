# Climate Tool Data Loader

## Overview

The `data_loader.py` module provides a convenient interface for loading agricultural data from GCS silver/gold layers for climate calculations. It uses the optimized `GCSDataAccess` pattern from the unified pipeline for maximum performance.

## Data Sources

The loader accesses the following data sources:

| Data Type | GCS Path | Source Pipeline | Description |
|-----------|----------|-----------------|-------------|
| **Livestock** | `silver/chr/*/herds*.parquet` | CHR Pipeline | Herd data, animal counts, CHR numbers |
| **Fields** | `silver/fvm_marker_YYYY/*/data.parquet` | FVM WFS | Agricultural fields, crop types, areas |
| **Fertilizer** | `silver/fertiliser/*/data.parquet` | Drive Data | Fertilizer applications (gødningsregnskab) |
| **Climate** | `silver/dmi/*/data.parquet` | DMI (optional) | Weather and climate data |

## Setup

### Dependencies

The data loader requires the unified_pipeline dependencies. Install them:

```bash
cd backend/pipelines/unified_pipeline
pip install -e .
```

Or if using the unified_pipeline virtual environment:

```bash
cd backend/pipelines/unified_pipeline
source venv/bin/activate  # If venv exists
pip install -e .
```

### Environment Variables

Set up your GCS credentials:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"
export GCS_BUCKET="landbrugsdata-raw-data"  # Optional, defaults to this
```

## Usage

### Basic Example

```python
from data_loader import ClimateDataLoader

# Initialize loader
loader = ClimateDataLoader()

# Load livestock data for a CVR
livestock_df = loader.load_livestock(cvr="31373077", year=2024)
print(f"Found {len(livestock_df)} livestock records")

# Load field data
fields_df = loader.load_fields(cvr="31373077", year=2024)
total_area = fields_df['areal_ha'].sum()
print(f"Total field area: {total_area:.2f} ha")

# Load fertilizer data
fertilizer_df = loader.load_fertilizer(cvr="31373077", year=2024)
```

### Advanced Usage

```python
# List available years for FVM marker data
available_years = loader.list_available_years("fvm_marker")
print(f"Available years: {available_years}")

# Get latest data timestamp
chr_timestamp = loader.get_latest_data_timestamp("chr")
print(f"Latest CHR data: {chr_timestamp}")

# Use custom GCS bucket
loader = ClimateDataLoader(bucket="my-custom-bucket")
```

## Methods

### `load_livestock(cvr: str, year: Optional[int] = None) -> pd.DataFrame`

Load CHR livestock data for a specific CVR number.

**Parameters:**
- `cvr`: Company CVR number (8 digits, will be zero-padded)
- `year`: Optional year filter (loads latest if None)

**Returns:** DataFrame with livestock records or empty DataFrame if no data found

**Columns (typical):**
- `cvr`: Company number
- `chr_nummer`: Herd CHR number
- `species_code`: Animal species code
- `animal_count`: Number of animals
- `dato`: Date

### `load_fields(cvr: str, year: int) -> pd.DataFrame`

Load FVM field data for a specific CVR and year.

**Parameters:**
- `cvr`: Company CVR number (8 digits)
- `year`: Agricultural year (YYYY)

**Returns:** DataFrame with field records or empty DataFrame if no data found

**Columns (typical):**
- `cvr`: Company number
- `bfe_nummer`: Field BFE number
- `afgroede`: Crop type
- `areal_ha`: Area in hectares
- `geometry`: Field boundary (PostGIS geometry)

### `load_fertilizer(cvr: str, year: int) -> pd.DataFrame`

Load fertilizer application data for a specific CVR and year.

**Parameters:**
- `cvr`: Company CVR number (8 digits)
- `year`: Agricultural year (YYYY)

**Returns:** DataFrame with fertilizer records or empty DataFrame if no data found

**Columns (typical):**
- `cvr`: Company number
- `goedningstype`: Fertilizer type
- `n_kg_ha`: Nitrogen kg/ha
- `p_kg_ha`: Phosphorus kg/ha
- `k_kg_ha`: Potassium kg/ha
- `areal_ha`: Area in hectares

### `load_climate_data(cvr: str, year: int) -> pd.DataFrame`

Load DMI climate/weather data (optional, may not be available).

**Parameters:**
- `cvr`: Company CVR number
- `year`: Year (YYYY)

**Returns:** DataFrame with climate records or empty DataFrame if not available

### `list_available_years(dataset: str) -> List[int]`

List available years for a specific dataset.

**Parameters:**
- `dataset`: Dataset name (`'fvm_marker'`, `'chr'`, etc.)

**Returns:** Sorted list of available years

### `get_latest_data_timestamp(dataset: str) -> Optional[str]`

Get the timestamp of the latest data file for a dataset.

**Parameters:**
- `dataset`: Dataset name (`'chr'`, `'fvm_marker_2024'`, etc.)

**Returns:** Timestamp string (YYYYMMDD_HHMMSS) or None if not found

## Testing

Run the test script to verify the data loader works:

```bash
cd backend/pipelines/climate_tool

# Test with default CVR and year
python test_data_loader.py

# Test with specific CVR and year
python test_data_loader.py --cvr 12345678 --year 2023
```

## Data Validation

The loader automatically validates:

1. **CVR Format**: Must be 8 digits (zero-padded if needed)
2. **Missing Data**: Returns empty DataFrames gracefully if no data found
3. **Data Quality**: Logs warnings for missing or invalid data

## Error Handling

The loader handles errors gracefully:

- Invalid CVR format: Logs error and returns empty DataFrame
- Missing data files: Logs warning and returns empty DataFrame
- GCS connection issues: Logs error with details
- Query failures: Logs error and returns empty DataFrame

All errors are logged using the unified pipeline logger.

## Performance

The loader uses the optimized `GCSDataAccess` pattern:

- **18x faster** than previous approaches
- **Direct DuckDB queries** without DataFrame conversion bottleneck
- **Streaming downloads** with automatic cleanup
- **Shared connection reuse** for multiple queries

## Integration with Climate Tool

The climate tool can use this loader to fetch farm-specific data:

```python
from data_loader import ClimateDataLoader
from farm_data import FarmData

# Load data for a specific farm
loader = ClimateDataLoader()
livestock = loader.load_livestock(cvr="31373077", year=2024)
fields = loader.load_fields(cvr="31373077", year=2024)
fertilizer = loader.load_fertilizer(cvr="31373077", year=2024)

# Populate FarmData structure
farm = FarmData()

# Convert livestock data to animal counts
for _, row in livestock.iterrows():
    animal_type = f"{row['species_code']}_{row['animal_type']}"
    farm.animal_counts[animal_type] = row['animal_count']

# Convert field data to areas
for _, row in fields.iterrows():
    crop_type = row['afgroede']
    farm.field_areas[crop_type] = row['areal_ha']

# Convert fertilizer data to applications
for _, row in fertilizer.iterrows():
    field_id = row['bfe_nummer']
    farm.fertilizer_applications[field_id] = row['n_kg_ha']
```

## Environment Variables

The loader respects the following environment variables:

- `GCS_BUCKET`: GCS bucket name (default: `landbrugsdata-raw-data`)
- `GOOGLE_APPLICATION_CREDENTIALS`: Path to GCS service account credentials

## Troubleshooting

### No data found

**Problem:** Loader returns empty DataFrames

**Solutions:**
1. Check if data exists in GCS for the specified year/CVR
2. Verify CVR format (must be 8 digits)
3. Check GCS bucket permissions
4. Review logs for specific error messages

### CVR format errors

**Problem:** "Invalid CVR format" error

**Solutions:**
1. Ensure CVR is 8 digits
2. Remove any spaces or special characters
3. Leading zeros are automatically added

### Performance issues

**Problem:** Slow data loading

**Solutions:**
1. Ensure GCS credentials are properly configured
2. Check network connection to GCS
3. Review DuckDB memory settings (default: 12GB)
4. Consider filtering by year to reduce data volume

## Known Limitations

1. **Fertilizer data availability**: May not be available for all years/CVRs (depends on drive_data_pipeline)
2. **Climate data**: Optional and may not be available for all locations
3. **Year filtering**: CHR data is timestamped, not year-specific in path structure
4. **CVR-level data**: Assumes data is stored with CVR as a column (not all datasets may have this)

## Future Enhancements

Potential improvements:

- [ ] Add caching layer for frequently accessed data
- [ ] Support batch loading for multiple CVRs
- [ ] Add data quality checks and validation
- [ ] Integrate with Supabase for gold layer access
- [ ] Add support for spatial queries (e.g., fields near a location)
- [ ] Create typed data models for returned DataFrames
