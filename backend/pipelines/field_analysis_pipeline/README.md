# Field Analysis Pipeline

This pipeline performs comprehensive spatial analysis of agricultural fields against multiple datasets including properties, soil types, BNBO status, wetlands, and water projects.

## Overview

The Field Analysis Pipeline analyzes 600K+ agricultural fields against:
- **6.5M+ properties** (ownership analysis)
- **13K+ soil type areas** (soil composition)
- **3.8K+ BNBO areas** (biodiversity protection status)
- **1.6M+ wetlands** (environmental constraints)
- **2.5K+ water projects** (conservation project overlap)

> **📖 Dataset Details**: See [DATA_CACHE_CATALOG.md](../../../DATA_CACHE_CATALOG.md) for comprehensive information about all datasets, including schemas, coverage, and usage patterns.

## Features

- **Dynamic Data Discovery**: Automatically finds the most recent data files from GCS
- **Optimized for GitHub Runners**: 4 vCPUs, 16GB RAM
- **Spatial JOIN Optimization**: Leverages DuckDB Spatial v1.2.2 SPATIAL_JOIN operator
- **Batch Processing**: Configurable batch sizes for memory management
- **Proper Area Calculations**: Uses ST_Union_Agg to avoid double-counting overlapping geometries
- **Water Projects Analysis**: Analyzes overlap between water projects and conservation areas
- **Comprehensive Output**: Field-level analysis with area shares and intersection counts

## Outputs

### Silver Layer
- `field_analysis_{year}_{timestamp}.parquet`: Complete field analysis results
- `water_projects_analysis_{timestamp}.json`: Water projects overlap analysis

### Analysis Includes
For each field:
- **Field metadata**: ID, block ID, CVR, area, crop type, organic status
- **Property ownership**: Count and coverage percentage
- **Soil composition**: Types and coverage percentages
- **BNBO status**: Categories and coverage percentages  
- **Wetland presence**: Intersection count and coverage percentage
- **Geometry**: Field boundary for further analysis

## Usage

### Local Development
```bash
cd backend/pipelines/field_analysis_pipeline
docker-compose up --build
```

### Production (GitHub Actions)
The pipeline runs via GitHub Actions workflow with configurable parameters:
- `year`: Analysis year (default: 2025)
- `batch_size`: Fields per batch (default: 1000)
- `environment`: Deployment environment (dev/prod)

## Configuration

### Environment Variables
```env
# Storage Configuration
OUTPUT_BUCKET=your-gcs-bucket
ENVIRONMENT=dev

# Analysis Configuration
ANALYSIS_YEAR=2025
BATCH_SIZE=1000
MEMORY_LIMIT_GB=14
THREAD_COUNT=4

# Data Sources (Optional - pipeline auto-discovers latest if not set)
FIELDS_DATA_PATH=gs://landbrugsdata-raw-data/silver/agricultural_fields_{year}/[latest]/data.parquet
PROPERTIES_DATA_PATH=gs://landbrugsdata-raw-data/silver/property_cadastral_merged/[latest].parquet
SOIL_DATA_PATH=gs://landbrugsdata-raw-data/silver/soil_types/[latest]/data.parquet
BNBO_DATA_PATH=gs://landbrugsdata-raw-data/silver/bnbo_status_dissolved/[latest]/data.parquet
WETLANDS_DATA_PATH=gs://landbrugsdata-raw-data/silver/wetlands_dissolved/[latest]/data.parquet
WATER_PROJECTS_DATA_PATH=gs://landbrugsdata-raw-data/silver/water_projects_dissolved/[latest]/data.parquet
```

## Data Discovery

The pipeline automatically discovers the most recent data files from GCS:

1. **Agricultural Fields**: Searches `silver/agricultural_fields_{year}/*/data.parquet`
2. **Properties**: Searches `silver/property_cadastral_merged/*.parquet` (date-named files)
3. **Other Datasets**: Searches `silver/{dataset_name}/*/data.parquet`

This ensures the pipeline always uses the latest available data without manual updates.

## Performance

- **Test run (10K fields)**: ~2 minutes
- **Full analysis (611K fields)**: ~120 minutes estimated
- **Memory usage**: Stable with batch processing
- **Output size**: ~50MB for 10K fields (estimated 3GB for full dataset)

## Technical Details

### Data Sources Validation
The pipeline validates all input data paths against the latest available files:
- **Agricultural Fields**: `silver/agricultural_fields_{year}/[timestamp]/data.parquet` (~600K records/year)
- **Properties**: `silver/property_cadastral_merged/[date].parquet` (6.5M records, 1.7GB)
- **Soil Types**: `silver/soil_types/[timestamp]/data.parquet` (13.5K records, 44MB)
- **BNBO Status**: `silver/bnbo_status_dissolved/[timestamp]/data.parquet` (3.8K records, 21MB)
- **Wetlands**: `silver/wetlands_dissolved/[timestamp]/data.parquet` (1.6M records, 215MB)
- **Water Projects**: `silver/water_projects_dissolved/[timestamp]/data.parquet` (2.5K records, 6MB)

### Spatial Analysis
- Uses DuckDB Spatial extension with SPATIAL_JOIN operator
- Proper area calculations with ST_Union_Agg to handle overlapping geometries
- Field uniqueness based on `field_id + block_id` combination

### Data Processing
- Batch processing to manage memory usage
- Efficient spatial joins with indexed geometries
- Progress tracking with ETA estimates

### Output Format
Results saved as Parquet with schema:
- `field_id`, `block_id`: Unique field identifier
- `cvr_number`, `area_ha`, `crop_type`, `organic_farming`: Field metadata
- `property_count`, `soil_type_count`, `bnbo_category_count`, `wetland_intersections`: Intersection counts
- `total_*_coverage`: Actual coverage percentages (0-100%)
- `max_*_share`: Largest single intersection percentage
- `geometry`: Field boundary as WKB 