# BMD Scraper Pipeline

A data pipeline for extracting, transforming, and loading data from the Danish Bekæmpelsesmiddel Database (BMD).

## Overview

This pipeline is divided into two main stages:

1. **Bronze Stage**: Extracts raw data from the BMD portal by automating Excel file downloads
2. **Silver Stage**: Processes and transforms the raw data into a more usable format

### Bronze Stage
The Bronze stage downloads raw Excel data from the BMD portal and saves it with the following structure:
```
bronze/bmd/<timestamp>/
  ├── bmd_raw.xlsx        # Raw Excel file from BMD portal
  └── metadata.json       # Metadata about the download (timestamp, source URL, etc.)
```

For production environments, files are also uploaded to Google Cloud Storage with the same structure:
```
gs://<bucket-name>/bronze/bmd/<timestamp>/bmd_raw.xlsx
gs://<bucket-name>/bronze/bmd/<timestamp>/metadata.json
```

### Silver Stage
The Silver stage takes the raw Excel data from the Bronze stage, processes and transforms it into a structured Parquet file:
```
data/silver/bmd/<timestamp>/
  ├── bmd_data_<timestamp>.parquet  # Cleaned and structured data
  └── metadata.json                 # Transformation metadata and validation info
```

The transformation process includes:
- Cleaning column names (lowercase, underscores)
- Type casting (proper INT, FLOAT, TEXT, DATE types)
- Date parsing for date columns
- Standardizing status fields
- Validating data and reporting issues

For production environments, files are also uploaded to Google Cloud Storage with the same structure:
```
gs://<bucket-name>/silver/bmd/<timestamp>/bmd_data_<timestamp>.parquet
gs://<bucket-name>/silver/bmd/<timestamp>/metadata.json
```

## Setup

### Local Development

1. Clone the repository
2. Create a `.env` file from `.env.example`:
   ```bash
   cp .env.example .env
   ```
3. Install dependencies:
   ```bash
   cd backend/pipelines/bmd_scraper
   pip install -e .
   ```

### Production Setup

For production environments with Google Cloud Storage:

1. Install with production dependencies:
   ```bash
   cd backend/pipelines/bmd_scraper
   pip install -e ".[production]"
   ```

2. Update the .env file with your GCS configuration:
   ```
   ENVIRONMENT=production
   GCS_BUCKET_NAME=your-gcs-bucket-name
   GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
   ```

### Docker Deployment

Run using Docker Compose:
```bash
docker-compose up --build
```

This will mount the appropriate data directories:
- `bronze/bmd/` -> `bronze/bmd/` in the container
- `silver/bmd/` -> `silver/bmd/` in the container

## Automated Pipeline Runs

The BMD scraper pipeline is configured to run automatically on the first day of each month at 2:00 AM UTC using GitHub Actions.

### GitHub Actions Configuration

The automation is configured in `.github/workflows/bmd_monthly.yml` with the following features:

- **Schedule**: Monthly runs on the 1st at 2 AM UTC (`cron: '0 2 1 * *'`)
- **Manual Triggering**: Can be triggered manually with environment selection
- **Environment Support**:
  - In production mode, uploads data to GCS and installs production dependencies
  - In development mode, saves artifacts in GitHub Actions
- **GCP Authentication**: Automatically handles authentication for GCS in production mode
- **Notifications**: Can be configured to notify via Slack, email, etc. on success/failure

### Required Secrets for Production Runs

For production runs with GCS integration, the following GitHub secrets must be configured:

- `GCP_SA_KEY`: Google Cloud service account key (JSON) with GCS write permissions
- `GCS_BUCKET_NAME`: Name of the GCS bucket for storing data

## Directory Structure

```
├── bronze/             # Bronze stage processing code
│   ├── __init__.py
│   └── export.py       # BMD portal data extraction
├── silver/             # Silver stage processing code
│   ├── __init__.py
│   └── transform.py    # BMD data transformation
├── .env.example        # Environment variable template
├── Dockerfile          # Container definition
├── docker-compose.yml  # Local development setup
├── main.py             # Main entry point
├── pyproject.toml      # Python project configuration and dependencies
```

## Usage

Run the entire pipeline:
```bash
python main.py
```

Run only the bronze stage:
```bash
python main.py --stage bronze
```

Run only the silver stage:
```bash
python main.py --stage silver
```
