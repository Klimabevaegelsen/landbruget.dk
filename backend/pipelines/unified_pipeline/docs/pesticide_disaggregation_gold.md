# Pesticide Disaggregation Gold Layer

## Overview

The Pesticide Disaggregation Gold Layer processor disaggregates pesticide applications from company level to individual field level using the proven strategy that achieved 92% coverage in the original pipeline.

**NEW**: Now includes integrated proximity analysis that creates comprehensive environmental and health risk data for pesticide-treated fields, including proximity to residential and educational buildings, and distances to water features.

**CRITICAL**: This implementation preserves the EXACT original logic without any "enhancements" that could break the proven 92% coverage approach.

## Usage

### Command Line

```bash
# Run pesticide disaggregation gold layer
python -m unified_pipeline --source pesticide_disaggregation --stage gold

# Run with custom bucket
GCS_BUCKET=your-bucket python -m unified_pipeline --source pesticide_disaggregation --stage gold
```

### Programmatic Usage

```python
from unified_pipeline.gold.pesticide_disaggregation import (
    PesticideDisaggregationGold,
    PesticideDisaggregationGoldConfig
)

# Create configuration
config = PesticideDisaggregationGoldConfig(
    bucket="your-bucket",
    pesticide_year=2021,
    area_tolerance_pct=2.0  # DO NOT CHANGE - preserves original 92% coverage
)

# Initialize processor
processor = PesticideDisaggregationGold(config)

# Run processing
import asyncio
asyncio.run(processor.run())
```

## Configuration

### Required Parameters

- `bucket`: GCS bucket containing silver data
- `pesticide_year`: Year of pesticide data to process (default: 2021)
- `area_tolerance_pct`: Area tolerance percentage (default: 2.0) **DO NOT CHANGE**

### Proximity Analysis Parameters (NEW)

- `enable_proximity_analysis`: Whether to run proximity analysis after disaggregation (default: True)
- `building_proximity_distance_m`: Distance threshold for building proximity analysis (default: 100m)
- `water_proximity_distance_m`: Maximum distance for water proximity analysis (default: 100m)
- `buildings_dataset`: Dataset name for buildings data (default: "bbr_buildings")
- `water_typology_dataset`: Dataset name for water typology data (default: "water_typology")

### Input Datasets

The processor requires these silver datasets to be available:

- `agricultural_fields`: Agricultural field boundaries with CVR and crop data
- `pesticides`: Pesticide application records

### Additional Datasets for Proximity Analysis

When proximity analysis is enabled (default), these additional datasets are required:

- `bbr_buildings`: Building data with addresses and usage categories (residential, educational)
- `water_typology`: Water feature data with geometries (lakes, coastal waters, watercourses)

### Temporal Pattern

The processor follows the Y+1 temporal pattern discovered in the original analysis:
- Pesticide data for year X uses field boundaries from year X+1
- Example: 2021 pesticide data uses 2022 field boundaries

## Processing Strategies

The processor implements 4 strategies in exact order from the original pipeline:

### 1. Marker CVR-Area Match (Main Strategy - 92% Coverage)

- Matches pesticide application area to total field area by CVR+crop
- Uses 2% area tolerance for matching
- Allocates proportionally to all fields in CVR+crop combination
- **This is the strategy that achieved 92% coverage**

### 2. Marker Non-Organic CVR-Area Match

- Same as main strategy but excludes organic fields
- Uses organic_farming column to identify organic fields
- Fallback for cases where organic fields affect area calculations

### 3. Partial Field Coverage

- Handles single-field CVR+crop combinations
- For cases where pesticide area < field area
- Flags results as partial coverage with spatial uncertainty

### 4. Adjacent Fields Single Cluster (Corrected Implementation)

- Finds spatial clusters of adjacent fields (within 10m buffer)
- **Matches cluster area against pesticide area** (within 2% tolerance)
- Only allocates when both spatial coherence AND area match exist
- Prevents spurious correlations from random field combinations
- Uses connected components algorithm for proper clustering

## Output Schema

The processor outputs disaggregated pesticide applications with this schema:

| Column | Type | Description |
|--------|------|-------------|
| DisaggregatedID | VARCHAR | Unique identifier for disaggregated record |
| OriginalPesticideRowID | VARCHAR | Reference to original pesticide record |
| CompanyRegistrationNumber | VARCHAR | CVR number |
| PesticideName | VARCHAR | Name of pesticide |
| PesticideRegistrationNumber | VARCHAR | Pesticide registration number |
| DosageQuantity | DOUBLE | Dosage amount |
| DosageUnit | VARCHAR | Dosage unit |
| MatchedFieldID | VARCHAR | Field identifier (marker_XXXXX) |
| MatchedBlockID | VARCHAR | Block identifier (block_XXXXX) |
| AllocatedArea | DOUBLE | Area allocated to this field |
| AllocationMethod | VARCHAR | Strategy used for allocation |
| MatchConfidence | DOUBLE | Confidence score (0.0-1.0) |
| IsPartialFieldCoverage | BOOLEAN | Whether this is partial field coverage |
| DisaggregationDate | TIMESTAMP | Processing timestamp |

## Quality Validation

### Coverage Requirement

The processor **MUST** achieve ≥92% coverage or it will fail with an error:

```
ValueError: Coverage X.X% below required 92% - migration failed
```

This ensures the migration preserves the original pipeline's performance.

### Allocation Methods

Results are tagged with allocation methods to track strategy effectiveness:

- `Marker_ApplicationAreaToTotalFieldArea_FieldProportional` - Main strategy
- `Marker_NonOrganic_ApplicationAreaToTotalFieldArea_FieldProportional` - Non-organic strategy  
- `Partial_Field_Coverage_SingleField` - Partial coverage strategy
- `Adjacent_Fields_Spatial_Cluster_AreaMatched` - Spatial clustering with area matching

## Performance

- **Memory**: Uses DuckDB in-memory processing with spatial extensions
- **Processing time**: Should process ≥1000 records per second
- **Coverage**: Must maintain ≥92% coverage from original pipeline

## Dependencies

- DuckDB with spatial extensions
- Agricultural fields silver data (with CVR and crop information)
- Pesticides silver data
- GCS access for input/output

## Troubleshooting

### Low Coverage Error

If coverage drops below 92%:

1. Check input data quality (CVR numbers, crop codes, area values)
2. Verify temporal alignment (Y+1 pattern)
3. Check for schema mismatches between agricultural_fields and expected format
4. Validate area tolerance setting (must be 2.0%)

### No Data Found

If no silver data is found:

1. Verify GCS bucket and paths
2. Check that agricultural_fields and pesticides silver data exist
3. Ensure proper authentication for GCS access

### Memory Issues

For large datasets:

1. DuckDB uses streaming processing to handle large datasets
2. Spatial extensions are optimized for performance
3. Processing is done in batches to manage memory usage

## Migration Notes

This implementation is a **direct migration** of the original standalone pipeline to the unified pipeline architecture. Key preservation points:

- **Exact SQL queries** from original disaggregation.py
- **2% area tolerance** unchanged
- **4 strategies in original order** 
- **Original confidence scoring formula**
- **92% coverage requirement** enforced

No "enhancements" or "optimizations" have been made that could affect the proven 92% coverage. 