# Climate Output Writer

## Overview

The `output_writer.py` module handles writing EmissionReport results to:
1. **GCS Gold Layer** (primary storage)
2. **Supabase Database** (optional sync for web access)

This writer follows the medallion architecture pattern and uses the GCSDataAccess class for optimal performance.

## Output Structure

### GCS Gold Layer

```
gs://landbrugsdata-raw-data/gold/climate_emissions/<YYYYMMDD_HHMMSS>/
├── emissions.parquet       # Main emission report data
├── categories.parquet      # Emission categories breakdown
└── metadata.json           # Run metadata
```

#### emissions.parquet Schema

| Column | Type | Description |
|--------|------|-------------|
| `cvr` | string | Company registration number (8 digits) |
| `year` | int | Calculation year |
| `total_co2e_kg` | float | Total emissions in kg CO2e |
| `data_completeness` | float | Data completeness score (0-1) |
| `intensity_co2e_per_kg_milk` | float (nullable) | Emissions per kg milk |
| `intensity_co2e_per_ha` | float (nullable) | Emissions per hectare |
| `intensity_co2e_per_animal_unit` | float (nullable) | Emissions per animal unit |
| `created_at` | timestamp | Record creation timestamp |

#### categories.parquet Schema

| Column | Type | Description |
|--------|------|-------------|
| `cvr` | string | Company registration number (8 digits) |
| `year` | int | Calculation year |
| `category_name` | string | Category name (cattle, fields, energy) |
| `co2e_kg` | float | Emissions for this category in kg CO2e |
| `data_quality` | string | Quality indicator (complete, estimated, unavailable) |
| `sub_sources` | string | JSON string of sub-source breakdown |
| `created_at` | timestamp | Record creation timestamp |

#### metadata.json Structure

```json
{
  "timestamp": "20240101_120000",
  "report_count": 10,
  "cvr_list": ["12345678", "87654321"],
  "year_range": {
    "min": 2024,
    "max": 2024
  },
  "statistics": {
    "total_emissions_kg_co2e": 1500000.0,
    "avg_emissions_kg_co2e": 150000.0,
    "avg_data_completeness": 0.85
  },
  "data_sources": {
    "chr": "silver/chr",
    "fvm_marker": "silver/fvm_marker_YYYY",
    "fertiliser": "silver/fertiliser"
  },
  "pipeline_version": "1.0.0",
  "created_at": "2024-01-01T12:00:00"
}
```

## Usage

### Basic Usage

```python
from climate_calculator import FarmClimateCalculator
from output_writer import ClimateOutputWriter
from data_loader import ClimateDataLoader

# Initialize components
loader = ClimateDataLoader()
calculator = FarmClimateCalculator(loader)
writer = ClimateOutputWriter()

# Calculate emissions for multiple farms
reports = []
for cvr in ["12345678", "87654321"]:
    report = calculator.calculate_emissions(cvr=cvr, year=2024)
    reports.append(report)

# Write to GCS gold layer
output_path = writer.write_to_gold_layer(reports)
print(f"✅ Wrote {len(reports)} reports to {output_path}")
```

### With Custom Timestamp and Metadata

```python
from datetime import datetime

# Custom timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Custom run metadata
run_metadata = {
    "pipeline_version": "1.0.0",
    "run_by": "climate_pipeline",
    "data_sources_version": {
        "chr": "20240101_000000",
        "fvm_marker": "20240101_000000",
    },
    "notes": "Full production run for 2024 data",
}

# Write with custom metadata
output_path = writer.write_to_gold_layer(
    reports,
    timestamp=timestamp,
    run_metadata=run_metadata,
)
```

### Syncing to Supabase

```python
# Optional: Sync to Supabase for web access
success = writer.sync_to_supabase(reports, upsert=True)
if success:
    print("✅ Data synced to Supabase")
else:
    print("⚠️  Supabase sync skipped (not configured)")
```

### Reading Previous Reports

```python
# List available reports
available_reports = writer.list_available_reports()
print(f"Found {len(available_reports)} emission reports")

# Read specific report metadata
latest_report = available_reports[0]
metadata = writer.read_report_metadata(latest_report)
print(f"Report contains {metadata['report_count']} farms")
print(f"Total emissions: {metadata['statistics']['total_emissions_kg_co2e']:,.0f} kg CO2e")
```

### Pattern-Based Report Listing

```python
# List reports for specific date
reports_2024_01 = writer.list_available_reports(pattern="202401*")

# List reports for specific day
reports_today = writer.list_available_reports(pattern="20240101_*")
```

## Supabase Integration (Optional)

### Environment Setup

To enable Supabase sync, set these environment variables:

```bash
export SUPABASE_URL="https://your-project.supabase.co"
export SUPABASE_KEY="your-service-role-key"
```

### Installation

Install the optional Supabase dependency:

```bash
pip install supabase
```

### Supabase Schema

The writer expects these tables to exist in Supabase:

#### climate_emissions

```sql
CREATE TABLE climate_emissions (
  id BIGSERIAL PRIMARY KEY,
  cvr VARCHAR(8) NOT NULL,
  year INTEGER NOT NULL,
  total_co2e_kg DOUBLE PRECISION NOT NULL,
  data_completeness DOUBLE PRECISION,
  intensity_co2e_per_kg_milk DOUBLE PRECISION,
  intensity_co2e_per_ha DOUBLE PRECISION,
  intensity_co2e_per_animal_unit DOUBLE PRECISION,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(cvr, year)
);
```

#### climate_emission_categories

```sql
CREATE TABLE climate_emission_categories (
  id BIGSERIAL PRIMARY KEY,
  cvr VARCHAR(8) NOT NULL,
  year INTEGER NOT NULL,
  category_name VARCHAR(50) NOT NULL,
  co2e_kg DOUBLE PRECISION NOT NULL,
  data_quality VARCHAR(20),
  sub_sources TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(cvr, year, category_name)
);
```

### Custom Table Prefix

You can use custom table names with the `table_prefix` parameter:

```python
# Use custom prefix (e.g., "dev_climate_")
success = writer.sync_to_supabase(
    reports,
    table_prefix="dev_climate_",
    upsert=True
)
# This will sync to: dev_climate_emissions and dev_climate_emission_categories
```

## Integration with Climate Pipeline

### Pipeline Flow

```
1. Data Loading (data_loader.py)
   ↓
2. Emission Calculation (climate_calculator.py)
   ↓
3. Output Writing (output_writer.py)
   ├── GCS Gold Layer (primary)
   └── Supabase (optional)
```

### Complete Example

```python
from data_loader import ClimateDataLoader
from climate_calculator import FarmClimateCalculator
from output_writer import ClimateOutputWriter

def process_farm_emissions(cvr_list: list[str], year: int):
    """Process emissions for a list of farms."""
    # Initialize components
    loader = ClimateDataLoader()
    calculator = FarmClimateCalculator(loader)
    writer = ClimateOutputWriter()

    # Calculate emissions
    reports = []
    for cvr in cvr_list:
        try:
            report = calculator.calculate_emissions(cvr=cvr, year=year)
            reports.append(report)
            print(f"✅ Processed CVR {cvr}: {report.total_co2e_kg:,.0f} kg CO2e")
        except Exception as e:
            print(f"❌ Failed to process CVR {cvr}: {e}")

    # Write results
    if reports:
        output_path = writer.write_to_gold_layer(reports)
        print(f"\n✅ Wrote {len(reports)} reports to {output_path}")

        # Optional: Sync to Supabase
        if writer.sync_to_supabase(reports):
            print("✅ Synced to Supabase")

    return reports

# Example usage
if __name__ == "__main__":
    farms = ["12345678", "87654321", "11111111"]
    reports = process_farm_emissions(farms, year=2024)
```

## Performance Considerations

### GCS Optimization

The writer uses `GCSDataAccess` which provides:
- **Direct DuckDB integration** - No DataFrame conversion overhead
- **Streaming uploads** - Memory-efficient for large datasets
- **Optimized Parquet settings** - zstd compression with configurable row groups
- **Resource monitoring** - Tracks memory and disk usage

### Batch Processing

For processing many farms, use batching:

```python
from itertools import islice

def batch_iterator(iterable, batch_size):
    """Yield batches from an iterable."""
    iterator = iter(iterable)
    while batch := list(islice(iterator, batch_size)):
        yield batch

# Process 1000 farms in batches of 100
all_cvrs = ["12345678", ...]  # 1000 CVRs
writer = ClimateOutputWriter()

for batch_num, cvr_batch in enumerate(batch_iterator(all_cvrs, 100), 1):
    reports = [calculator.calculate_emissions(cvr=cvr, year=2024) for cvr in cvr_batch]
    output_path = writer.write_to_gold_layer(
        reports,
        timestamp=f"20240101_120000_batch{batch_num:03d}"
    )
    print(f"✅ Batch {batch_num}: {len(reports)} reports → {output_path}")
```

## Error Handling

The writer includes comprehensive error handling:

```python
try:
    output_path = writer.write_to_gold_layer(reports)
except Exception as e:
    logger.error(f"Failed to write reports: {e}")
    # Handle error (e.g., retry, alert, etc.)
```

All methods log errors and warnings using the unified Logger from `unified_pipeline.util.log_util`.

## Data Quality Validation

The writer validates CVR format before writing:

```python
# CVR numbers are automatically padded to 8 digits
report = EmissionReport(cvr="1234567", ...)  # Will be stored as "01234567"
```

## Testing

### Unit Tests

```python
# Test basic writing
def test_write_reports():
    writer = ClimateOutputWriter()
    mock_report = EmissionReport(
        cvr="12345678",
        year=2024,
        total_co2e_kg=150000.0,
        categories=[],
        intensity_metrics={},
        data_completeness=0.85,
    )
    output_path = writer.write_to_gold_layer([mock_report])
    assert output_path.startswith("gs://")
```

### Integration Tests

```python
# Test with real data
def test_full_pipeline():
    loader = ClimateDataLoader()
    calculator = FarmClimateCalculator(loader)
    writer = ClimateOutputWriter()

    # Use known test CVR
    report = calculator.calculate_emissions(cvr="31373077", year=2024)
    output_path = writer.write_to_gold_layer([report])

    # Verify output exists
    metadata = writer.read_report_metadata(output_path)
    assert metadata is not None
    assert metadata["report_count"] == 1
```

## Monitoring

The writer logs key metrics:

```
✅ ClimateOutputWriter initialized with bucket: landbrugsdata-raw-data
Writing 10 emission reports to gs://landbrugsdata-raw-data/gold/climate_emissions/20240101_120000
✅ Wrote 10 emission records to .../emissions.parquet
✅ Wrote 30 category records to .../categories.parquet
✅ Wrote metadata to .../metadata.json
✅ Successfully wrote 10 reports to gs://...
```

## Troubleshooting

### Issue: "No module named 'supabase'"

**Solution**: Supabase sync is optional. Either:
1. Skip Supabase: Remove `sync_to_supabase()` call
2. Install dependency: `pip install supabase`

### Issue: "SUPABASE_URL and SUPABASE_KEY environment variables not set"

**Solution**: Set environment variables or skip Supabase sync.

### Issue: "Failed to write reports to gold layer"

**Possible causes**:
1. Missing GCS credentials
2. Insufficient GCS permissions
3. Invalid bucket name

**Solution**: Check `GCS_BUCKET` environment variable and GCS authentication.

## Future Enhancements

Potential improvements for future versions:

1. **Incremental Updates** - Only write changed records
2. **Versioning** - Track changes to emission calculations
3. **Compression Options** - Support multiple compression formats
4. **Partitioning** - Partition by year for faster queries
5. **Data Validation** - Pre-write validation checks
6. **Notifications** - Alert on write completion/failure

## Related Documentation

- [Climate Calculator README](CLIMATE_CALCULATOR_README.md) - Emission calculation logic
- [Data Loader README](DATA_LOADER_README.md) - Data loading from GCS
- [GCS Access Documentation](../unified_pipeline/src/unified_pipeline/util/gcs_access.py) - GCS optimization details
