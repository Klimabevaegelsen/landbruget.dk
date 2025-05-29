# Unified Pipeline

A unified data pipeline for fetching and processing various Danish agricultural and environmental datasets.

## Data Sources

The pipeline currently supports the following data sources:

### 1. BNBO Status (`bnbo`)
- **Description**: Boringsnære beskyttelsesområder (Well Protection Areas) data
- **Type**: XML/SOAP API
- **Frequency**: As needed
- **Stages**: Bronze, Silver

### 2. Agricultural Fields (`agricultural_fields`)
- **Description**: Danish agricultural field and block data
- **Type**: ArcGIS REST API
- **Frequency**: Weekly
- **Stages**: Bronze, Silver

### 3. Cadastral (`cadastral`)
- **Description**: Danish cadastral parcels data
- **Type**: WFS (Web Feature Service)
- **Frequency**: Weekly
- **Stages**: Bronze, Silver

### 4. Soil Types (`soil_types`)
- **Description**: Danish soil types data from Environmental Portal
- **Type**: WFS (Web Feature Service)
- **Source**: Danish Environmental Portal (Miljøportalen)
- **Endpoint**: https://arld-extgeo.miljoeportal.dk/geoserver/wfs
- **Layer**: `landbrugsdrift:DJF_FGJOR` (Jordbundstyper)
- **Frequency**: Monthly
- **Stages**: Bronze, Silver
- **Features**: ~13,520 soil type polygons covering Denmark
- **Attributes**:
  - `soil_height`: Soil height measurement
  - `soil_description`: Textual description of soil type
  - `theme_name`: Theme classification
  - `soil_code`: Unique soil type code
  - `geometry`: Polygon geometry

## Usage

Run the pipeline using the CLI:

```bash
# Run bronze stage only
python -m unified_pipeline -s soil_types -j bronze

# Run silver stage only
python -m unified_pipeline -s soil_types -j silver

# Run both bronze and silver stages
python -m unified_pipeline -s soil_types -j all
```

### Available Sources
- `bnbo`
- `agricultural_fields`
- `cadastral`
- `soil_types`

### Available Stages
- `bronze`: Raw data ingestion
- `silver`: Cleaned and processed data
- `all`: Both bronze and silver stages

## Architecture

The pipeline follows a medallion architecture:

- **Bronze Layer**: Raw data ingestion with minimal processing
- **Silver Layer**: Cleaned, validated, and standardized data

## Configuration

The pipeline uses environment variables for configuration:

- `GCS_BUCKET`: Google Cloud Storage bucket for data storage
- `SAVE_LOCAL`: Set to "true" to save data locally instead of uploading to GCS

## Data Processing

### Soil Types Processing

**Bronze Layer:**
- Fetches data from WFS endpoint using geopandas
- Validates WFS response and geometry data
- Adds metadata columns (source, timestamps)
- Saves raw data as GeoParquet format

**Silver Layer:**
- Validates and fixes geometries using common validator
- Standardizes column names to snake_case
- Cleans and validates data types
- Validates and standardizes geometries and attributes
- Performs quality checks and data completeness analysis
- Saves processed data as GeoParquet format

## Requirements

- Python 3.12+
- geopandas
- pandas
- google-cloud-storage
- pydantic
- click
- Other dependencies listed in pyproject.toml
