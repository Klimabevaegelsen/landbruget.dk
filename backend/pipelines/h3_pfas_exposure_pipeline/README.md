# H3 PFAS Exposure Pipeline

A production-ready pipeline for analyzing PFAS exposure using H3 hexagonal spatial indexing. This pipeline creates comprehensive PFAS exposure maps by joining pesticide application data with agricultural field geometries and PFAS-containing pesticide information.

## Overview

This pipeline processes agricultural data to create H3-based PFAS exposure analysis at multiple resolutions:
- **H3 Hexagon Level**: Multi-resolution exposure analysis using H3 resolutions 7-10
  - Resolution 7: ~516 hectares per hexagon (regional level)
  - Resolution 8: ~74 hectares per hexagon (county level) 
  - Resolution 9: ~11 hectares per hexagon (municipal level)
  - Resolution 10: ~1.5 hectares per hexagon (field level)
- **Municipality Level**: Aggregated exposure analysis by Danish municipalities (kommuner)

## Features

### 🚀 Optimized Spatial Processing
- **5-stage spatial join pipeline** for maximum performance
- **Chunked processing** to handle large datasets efficiently
- **Geometric union operations** for accurate area calculations
- **Coordinate system optimization** with ST_FlipCoordinates

### 📊 Comprehensive Analysis
- **PFAS-containing active ingredient tracking** in grams per hectare
- **Pesticide load calculations** with environmental impact metrics
- **Agricultural coverage ratios** and field diversity metrics
- **Multi-year temporal analysis** (2015-2023)

### ☁️ Cloud-Native Architecture
- **GCS integration** for data storage and retrieval
- **Optimized data loading** with fsspec and gcsfs
- **Kepler.gl compatible outputs** for visualization
- **Containerized deployment** with Docker

## Data Sources

The pipeline integrates multiple data sources:

1. **Pesticide Disaggregation Data** (Gold layer)
   - Field-level pesticide application records
   - Dosage quantities and application methods
   - Company registration numbers (CVR)

2. **FVM Agricultural Field Data** (Silver layer)
   - Field geometries and boundaries
   - Crop types and agricultural area
   - Field identification (CVR + block_id + field_id)

3. **BMD Pesticide Products** (Silver layer)
   - Pesticide registration information
   - PFAS-containing active ingredient indicators
   - Environmental load calculations

4. **DAGI Municipality Data** (Silver layer)
   - Danish municipality boundaries
   - Administrative region information

## Installation

### Prerequisites
- Python 3.11+
- Docker and Docker Compose
- GCS credentials (for cloud data access)

### Local Development Setup

```bash
# Clone the repository
cd backend/pipelines/h3_pfas_exposure_pipeline

# Install dependencies
uv pip install -e .

# Set up environment variables
cp .env.example .env
# Edit .env with your configuration
```

### Docker Setup

```bash
# Build and run with Docker Compose
docker-compose up --build

# Or run specific analysis mode
docker-compose run h3_pfas_exposure python main.py --mode h3 --years 2022 2023
```

### GitHub Actions (Recommended for Production)

The pipeline includes a GitHub Actions workflow for automated, parallel execution:

```yaml
# Manually trigger via GitHub UI or API
gh workflow run h3-pfas-analysis.yml \
  -f analysis_modes="h3,kommune" \
  -f years="2022,2023" \
  -f memory_limit="16GB"

# Or use the GitHub UI:
# Actions → H3 PFAS Exposure Analysis → Run workflow
```

**Matrix Job Benefits:**
- ✅ **True parallelism**: H3 and kommune analyses run simultaneously 
- ✅ **Scalable**: Each analysis gets its own runner with dedicated resources
- ✅ **Fault tolerance**: One analysis can fail without affecting the other
- ✅ **Resource isolation**: No memory/CPU contention between analyses
- ✅ **Automated scheduling**: Weekly runs via cron schedule

## Usage

### Command Line Interface

```bash
# Run H3 hexagon analysis for all available years
python main.py --mode h3

# Run municipality analysis for specific years
python main.py --mode kommune --years 2020 2021 2022

# Run both analyses sequentially
python main.py --mode all --years 2023

# Run both analyses in parallel (faster)
python main.py --mode all --parallel --years 2023

# Adjust processing parameters and resolution
python main.py --mode h3 --h3-resolution 8 --memory-limit 16GB --thread-count 8 --chunk-size 50000

# Dry run to validate configuration
python main.py --dry-run --verbose
```

### Analysis Modes

| Mode | Description | Output | Parallel Support |
|------|-------------|--------|------------------|
| `h3` | H3 hexagon-level analysis | H3 cells with PFAS exposure metrics | N/A |
| `kommune` | Municipality-level analysis | Danish municipalities with aggregated exposure | N/A |
| `all` | Both H3 and municipality analysis | Complete exposure analysis | ✅ Use `--parallel` |

### Configuration Options

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--h3-resolution` | 10 | H3 resolution level (7=~516ha, 8=~74ha, 9=~11ha, 10=~1.5ha) |
| `--memory-limit` | 12GB | DuckDB memory limit |
| `--thread-count` | 4 | Number of processing threads |
| `--chunk-size` | 25000 | H3 cells per processing chunk |
| `--bucket` | landbrugsdata-raw-data | GCS bucket name |
| `--parallel` | false | Run H3 and kommune analyses in parallel (mode 'all' only) |

## Architecture

### Pipeline Structure

```
src/h3_pfas_exposure/
├── config/
│   ├── __init__.py
│   └── settings.py          # Configuration management
├── gold/
│   ├── __init__.py
│   ├── h3_processor.py      # Refactored H3 spatial processor
│   └── pipeline.py          # Main pipeline orchestration
├── bronze/                  # (Future: Raw data ingestion)
├── silver/                  # (Future: Data transformation)
└── common/                  # (Future: Shared utilities)
```

### Processing Pipeline

1. **Data Loading**
   - Load BMD pesticide data with PFAS indicators
   - Load FVM agricultural field geometries
   - Load pesticide disaggregation records

2. **Spatial Processing**
   - Generate H3 hexagon grid over Denmark
   - Perform chunked spatial joins
   - Calculate intersection areas and coverage ratios

3. **PFAS Analysis**
   - Join pesticide data with BMD PFAS indicators
   - Calculate PFAS-containing active ingredient amounts
   - Aggregate by H3 cells or municipalities

4. **Output Generation**
   - Create Kepler.gl compatible visualizations
   - Generate CSV and Parquet outputs
   - Upload results to GCS

## Output Data

### H3 Hexagon Analysis

Each H3 cell contains:
- **Spatial Information**: H3 cell ID, center coordinates, area
- **Agricultural Metrics**: Field count, crop diversity, coverage ratio
- **PFAS Exposure**: Total PFAS-containing active ingredients (grams)
- **Pesticide Load**: Environmental impact metrics
- **Intensity Metrics**: Exposure per hectare

### Municipality Analysis

Each municipality contains:
- **Administrative Info**: Municipality code, name, region
- **Agricultural Summary**: Total agricultural area, field count
- **PFAS Exposure**: Area-weighted PFAS-containing active ingredient totals
- **Coverage Statistics**: Agricultural land percentage

## Performance

### Benchmarks
- **13.5 million H3 cells** processed in ~4 minutes
- **1.8 million agricultural H3 cells** with PFAS data
- **Memory usage**: 8-12GB peak
- **Processing speed**: ~3,000 H3 cells/second

### Optimization Features
- Chunked processing for memory efficiency
- Geometric union operations for accuracy
- Coordinate system optimization
- Parallel spatial operations

## Monitoring and Validation

### Built-in Validation
- H3 cell area validation (0.91-1.82 hectares)
- Intersection area consistency checks
- Coverage ratio validation (0-1 range)
- Data completeness verification

### Logging and Progress
- Real-time progress tracking
- Chunk-level performance metrics
- Stage timing information
- Error handling and recovery

## Environment Variables

```bash
# GCS Configuration
GCS_BUCKET=landbrugsdata-raw-data

# H3 Configuration
H3_RESOLUTION=10
CHUNK_SIZE=25000

# Processing Configuration
MEMORY_LIMIT=12GB
THREAD_COUNT=4

# Logging Configuration
LOG_LEVEL=INFO
ENABLE_PROGRESS_TRACKING=true
LOG_CHUNK_DETAILS=true
LOG_STAGE_TIMINGS=true
```

## Output Locations

Results are stored in GCS with timestamped directories and resolution indicators:

```
gs://landbrugsdata-raw-data/gold/
├── h3_pesticide_2023_res10/
│   └── 20250703_214459/
│       ├── h3_pesticide_2023_res10.parquet
│       └── h3_pesticide_2023_res10_kepler.parquet
├── h3_pesticide_2023_res8/
│   └── 20250703_214459/
│       ├── h3_pesticide_2023_res8.parquet
│       └── h3_pesticide_2023_res8_kepler.parquet
└── kommune_pesticide_2023/
    └── 20250703_214459/
        ├── kommune_pesticide_2023.parquet
        └── kommune_pesticide_2023.csv
```

## Development

### Running Tests
```bash
# Install development dependencies
uv pip install -e ".[dev]"

# Run linting
ruff check src/
black --check src/

# Run type checking
mypy src/
```

### Code Quality
- **Black** for code formatting
- **Ruff** for linting and import sorting
- **MyPy** for type checking
- **Structured logging** with Loguru

## Troubleshooting

### Common Issues

1. **Memory Issues**
   - Reduce `--chunk-size` parameter
   - Increase `--memory-limit`
   - Process fewer years at once

2. **GCS Access Issues**
   - Verify GCS credentials
   - Check bucket permissions
   - Ensure network connectivity

3. **Performance Issues**
   - Adjust `--thread-count` based on available CPU
   - Monitor memory usage
   - Check disk space for temporary files

### Support

For issues and questions:
1. Check the logs for detailed error messages
2. Verify configuration parameters
3. Test with `--dry-run` first
4. Review the validation results

## License
