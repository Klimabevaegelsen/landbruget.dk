# Unified Pipeline

This repository contains the Unified Pipeline for various Danish data sources within the Landbruget.dk project. It orchestrates bronze and silver ETL layers and stores outputs in Google Cloud Storage.

## Supported Sources

- **cadastral**: Danish cadastral parcels via WFS
- **agricultural_fields**: Agricultural field boundaries
- **bnbo**: BNBO status data
- **spf_su**: SPF SU herd health and salmonella data via WFS

## Prerequisites

1. Python 3.9+
2. Google Cloud service account key JSON
3. Create a `.env` file based on `.env.example`:
   ```
   GCS_BUCKET=<your-gcs-bucket>
   MAX_CONCURRENT=20            # for bronze HTTP requests
   SAVE_LOCAL=False            # save locally under /tmp
   ```
4. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

Run a specific source and job stage:

```bash
python -m unified_pipeline -s <source> -j <job>
```

- `<source>`: one of `cadastral`, `agricultural_fields`, `bnbo`, `spf_su`
- `<job>`: `bronze`, `silver`, or `all`

### Example: SPF SU Pipeline

Fetch raw JSON (bronze) and process to Parquet (silver) for SPF SU:

```bash
python -m unified_pipeline -s spf_su -j bronze
python -m unified_pipeline -s spf_su -j silver
# or run both stages:
python -m unified_pipeline -s spf_su -j all
```

## SPF SU Pipeline Details

1. **Bronze**:
   - Fetches herd numbers from the `chr` parquet in silver layer
   - Retrieves raw JSON per herd via SPF SU WFS
   - Saves to `gs://<GCS_BUCKET>/bronze/spf_su/<timestamp>/data.json`

2. **Silver**:
   - Reads bronze JSON
   - Validates & parses via `schema/spf_su.py` Pydantic models
   - Flattens into tables and writes Parquet to `gs://<GCS_BUCKET>/silver/spf_su/<timestamp>/<table>.parquet`

### Tables Produced of SPF_SU
RAW data schema
```
{
  "ownerDetailInfo": {
    "chrNumber": "131264",
    "ownerNumber": 0,
    "herdNumber": "131264",
    "name": "Noam Valentin Winston Goltermann",
    "address": {
      "farmName": "131264",
      "line1": "Brandelev Stationsvj 15",
      "postalCode": "4700",
      "city": "Næstved",
      "name": "Noam Valentin Winston Goltermann"
    },
    "danishCertificate": {
      "approved": false,
      "pdfFileName": "Ingen data fundet",
      "isExpired": false
    },
    "healthData": {
      "conditionalStatus": "Nej",
      "healthStatus": "Ukendt",
      "healthStatusColor": "unknown",
      "supplementaryStatus": ""
    },
    "salmonellaData": {
      "salmonellaLevel": [],
      "salmonellaIndexes": [],
      "hasIndexDetails": true,
      "salmonellaDate": "0001-01-01T00:00:00.0000000",
      "salmonellaStatus": "",
      "salmonellaTestResults": [],
      "showData": false
    }
  },
  "healthStatus": {
    "healthControlInfo": [
      {
        "disease": "Ap2"
      },
      {
        "disease": "Ap6"
      },
      {
        "disease": "Ap12"
      },
      {
        "disease": "Myc"
      },
      {
        "disease": "PRRS"
      }
    ],
    "activeConditionalStatus": [],
    "deliveryOptions": [
      "Direkte"
    ],
    "receptionOptions": [
      "Direkte"
    ],
    "susCoRunningFarms": []
  },
  "hasAccessToDetails": false,
  "googleAnalytics": "var ga = window.ga || {};\r\nga('send', 'event', 'Healthcare Status', 'Search', '131264');\r\n"
}
```
- `farm_owner_details`
- `farm_certificate`
- `farm_general_health_summary`
- `farm_salmonella_data`
- `farm_disease_control_status`
- `farm_veterinarians`
- `deliveryOptions`
- `receptionOptions`

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
