# Pesticide Disaggregation Pipeline - GCS Silver Data Integration

## Overview

The pesticide disaggregation pipeline uses GCS silver data sources with:
- **Dynamic year processing** - Process any pesticide year with appropriate field boundaries
- **Automatic GCS integration** - Download latest silver data from Google Cloud Storage
- **Y+1 temporal pattern** - Implements the optimal temporal alignment discovered through analysis

## Key Features

### Temporal Pattern Implementation
The pipeline implements the **Y+1 temporal pattern** discovered through comprehensive analysis:
- **2021 pesticides** → **2022 field boundaries** (91.9% accuracy)
- **2022 pesticides** → **2023 field boundaries** (89.0% accuracy) 
- **2023 pesticides** → **2024 field boundaries** (~87% accuracy)

This provides **8-15x error reduction** compared to same-year matching.

### GCS Silver Data Sources
The pipeline automatically discovers and downloads the latest silver data:
- `silver/agricultural_fields_{year+1}/` - Field boundaries (Y+1 pattern)
- `silver/pesticide_data_{year}_{year+1}/` - Pesticide applications
- `silver/gkea_{year+1}/` - GKEA subsidy data

## Usage

### Command Line Interface

```bash
# Process 2021 pesticides with 2022 fields (default)
python main.py

# Process 2022 pesticides with 2023 fields
python main.py --year 2022

# Process 2023 pesticides with 2024 fields  
python main.py --year 2023
```

### Environment Configuration

Set environment variables for GCS access:
```bash
export GCS_BUCKET=landbrugsdata-raw-data
export GOOGLE_CLOUD_PROJECT=your-project-id
export PESTICIDE_YEAR=2022  # Optional: set default year
```

### Programmatic Usage

```python
from main import main_orchestrator

# Process specific year
main_orchestrator(pesticide_year=2022)

# Use default year from config
main_orchestrator()
```

## GCS Silver Data Structure

### Automatically discovered paths:
```
gs://landbrugsdata-raw-data/
├── silver/agricultural_fields_2022/
│   └── 20241215_143022/data.parquet
├── silver/agricultural_fields_2023/
│   └── 20241216_091544/data.parquet
├── silver/pesticide_data_2021_2022/
│   └── 20241214_165432/data.parquet
└── silver/gkea_2022/
    └── 20241215_120033/data.parquet
```

## Configuration

### Dynamic Year Configuration
The `config.py` generates appropriate GCS dataset paths:

```python
# Get GCS paths for pesticide year 2022
config.get_gcs_silver_sources(2022)
# Returns:
# {
#   "marker": "silver/agricultural_fields_2023",
#   "pesticide": "silver/pesticide_data_2022_2023", 
#   "gkea": "silver/gkea_2023"
# }
```

## Dependencies

Required dependencies for GCS integration:
```bash
pip install gcsfs google-cloud-storage
```

## Examples

See `run_pipeline_example.py` for complete examples of processing different years.

## Optimal Dataset Selection

Based on the analysis documented in `OPTIMAL_DATASET_SELECTION_GUIDE.md`:

| Pesticide Year | Optimal Field Dataset | Expected Performance |
|----------------|----------------------|---------------------|
| **2021** | agricultural_fields_2022 | 91.9% at ≤1% error |
| **2022** | agricultural_fields_2023 | 89.0% at ≤1% error |
| **2023** | agricultural_fields_2024 | ~87% at ≤1% error |

The pipeline automatically implements this optimal temporal pattern for any year. 