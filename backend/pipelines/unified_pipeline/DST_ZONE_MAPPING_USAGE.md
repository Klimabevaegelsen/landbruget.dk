# DST Zone Mapping in Unified Pipeline

The DST Zone Mapping component has been integrated into the DAGI pipeline to automatically create spatial lookup tables for mapping field geometries to DST (Danmarks Statistik) statistical zones.

## Overview

The DST Zone Mapping component:
- Extends the DAGI pipeline with DST zone functionality
- Creates spatial lookup tables mapping DAGI landsdele to DST regions
- Provides both spatial (with geometry) and reference (without geometry) outputs
- Supports in-memory data passing for efficient processing

## Usage

### Running the Pipeline

The DST zone mapping is automatically included when running the DAGI pipeline:

```bash
# Run both DAGI processing and DST zone mapping
python -m unified_pipeline.app --source dagi --stage all

# Run only the silver stage (including DST mapping)
python -m unified_pipeline.app --source dagi --stage silver
```

### Output Files

The pipeline creates two output files in the GCS silver bucket:

1. **`dst_zone_mapping`** - Spatial lookup table with geometries
2. **`dst_zone_mapping_reference`** - Reference table without geometries

### Data Structure

The DST zone mapping output includes:

| Field | Type | Description |
|-------|------|-------------|
| `landsdel_code` | string | DAGI landsdel code (e.g., "DK011") |
| `landsdel_name` | string | Landsdel name (e.g., "Byen København") |
| `landsdel_dagi_id` | string | DAGI unique identifier |
| `dagi_region_code` | string | DAGI region code (e.g., "1084") |
| `dagi_region_name` | string | DAGI region name (e.g., "Region Hovedstaden") |
| `dagi_region_nuts2` | string | NUTS2 European code (e.g., "DK01") |
| `dst_regions` | string | Pipe-separated DST regions (e.g., "Hele landet\|Landsdel Bornholm") |
| `geometry` | geometry | Spatial geometry of the landsdel |
| `area_m2` | float | Area in square meters |
| `centroid_x` | float | Centroid X coordinate |
| `centroid_y` | float | Centroid Y coordinate |
| `created_at` | datetime | Processing timestamp |
| `data_source` | string | Source identifier |
| `mapping_version` | string | Version of the mapping |

## DST Region Mappings

The component maps 9 DST regions to 11 DAGI landsdele:

### Perfect Matches (5)
- **Landsdel Bornholm** → DK014 (Bornholm)
- **Landsdel Fyn** → DK031 (Fyn)
- **Landsdel Sydjylland** → DK032 (Sydjylland)
- **Landsdel Vestjylland** → DK041 (Vestjylland)
- **Landsdel Østjylland** → DK042 (Østjylland)

### Exact Region Matches (2)
- **Region Nordjylland** → DK050 (Nordjylland)
- **Region Sjælland** → DK021 (Østsjælland) + DK022 (Vest- og Sydsjælland)

### Composite Mappings (2)
- **Hele landet** → All 11 landsdele (entire country)
- **Landsdelene Byen København, Københavns omegn og Nordsjælland** → DK011 + DK012 + DK013

## Using the Output

### Loading Data

```python
import geopandas as gpd
import pandas as pd

# Load spatial lookup table
gdf_lookup = gpd.read_parquet('gs://landbrugsdata-raw-data/silver/dst_zone_mapping/latest/data.parquet')

# Load reference table
df_reference = pd.read_parquet('gs://landbrugsdata-raw-data/silver/dst_zone_mapping_reference/latest/data.parquet')
```

### Spatial Joins with Agricultural Fields

```python
# Load agricultural fields
fields = gpd.read_parquet('gs://landbrugsdata-raw-data/silver/fvm_marker_2024/latest/data.parquet')

# Perform spatial join to add DST zones
fields_with_dst = gpd.sjoin(fields, gdf_lookup, how='left', predicate='within')

# Now each field has DST zone information
print(fields_with_dst[['cvr_number', 'crop_type', 'landsdel_name', 'dst_regions']].head())
```

### Filtering by DST Region

```python
# Get all landsdele for a specific DST region
fyn_landsdele = gdf_lookup[gdf_lookup['dst_regions'].str.contains('Landsdel Fyn', na=False)]

# Get fields in Fyn region
fyn_fields = gpd.sjoin(fields, fyn_landsdele, how='inner', predicate='within')
```

## Configuration

The DST mappings are configured in the `DSTZoneMappingConfig` class and can be customized if needed:

```python
# Custom configuration example
config = DSTZoneMappingConfig(
    dataset="custom_dst_mapping",
    dst_mappings={
        # Custom DST region mappings
        "Custom Region": {
            "landsdele_codes": ["DK011", "DK012"],
            "description": "Custom region description"
        }
    }
)
```

## Integration Benefits

1. **Automatic Processing**: DST mapping is created automatically when running DAGI pipeline
2. **In-Memory Efficiency**: Uses in-memory data passing for better performance
3. **Consistent Output**: Follows unified pipeline standards for data structure and storage
4. **Version Control**: Includes versioning and metadata for tracking changes
5. **GCS Integration**: Automatically saves to GCS with proper timestamps and paths

## Troubleshooting

### Common Issues

1. **Missing DAGI Data**: Ensure DAGI bronze stage runs successfully first
2. **Geometry Issues**: Check that landsdele geometries are valid
3. **Mapping Errors**: Verify that all landsdele codes exist in the DST mappings

### Logs

The component provides detailed logging:
- Data loading progress
- Mapping statistics
- Processing times
- Error details

Check the pipeline logs for detailed information about the processing. 