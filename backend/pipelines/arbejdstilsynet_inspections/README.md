# Arbejdstilsynet Inspections Pipeline

This document describes the `arbejdstilsynet_inspections` data pipeline.

## Overview

This pipeline is responsible for fetching, storing, and processing inspection data from Arbejdstilsynet (the Danish Working Environment Authority).

It follows the Medallion architecture (Bronze -> Silver -> Gold layers).

## Command-line Arguments

The pipeline supports the following command-line arguments:

* `--start-date`: Start date in YYYY-MM-DD format (default: 6 months ago)
* `--end-date`: End date in YYYY-MM-DD format (default: today)
* `--log-level`: Logging level (DEBUG, INFO, WARNING, ERROR) (default: INFO)
* `--gcs-bucket`: Google Cloud Storage bucket for export (optional)
* `--stage`: Pipeline stage to run ('all', 'bronze', 'silver') (default: 'all')

### Usage Example

To specify custom parameters, edit the `run.sh` file:

```bash
# Original command
python main.py

# Example with custom parameters
python main.py --start-date 2025-01-01 --end-date 2025-05-01 --log-level DEBUG --stage silver
```

## Bronze Layer

### Purpose

The Bronze layer fetches the raw inspection data and stores it without any modifications. This ensures an exact replica of the source data is preserved.

### Data Source

*   **URL**: The data is fetched from a CSV file. The specific URL is configured via the `SOURCE_CSV_URL` environment variable.
    *   Refer to issue #265 for details on the data origin and how to obtain an up-to-date URL.

### How to Run (Local Development)

1.  **Prerequisites**:
    *   Docker and Docker Compose installed.
    *   Create a `.env` file in the `backend/pipelines/arbejdstilsynet_inspections/` directory by copying `.env.example` and filling in the required environment variables:
        ```bash
        cp .env.example .env
        # Then edit .env with the correct values:
        # - SOURCE_CSV_URL (required)
        # - GOOGLE_APPLICATION_CREDENTIALS (required for GCS export)
        # - GCS_BUCKET (optional)
        ```

2.  **Navigate to the pipeline directory**:
    ```bash
    cd backend/pipelines/arbejdstilsynet_inspections
    ```

3.  **Run Docker Compose**:
    ```bash
    docker-compose up --build
    ```
    This will build the Docker image (if not already built or if changes were made) and run the `main.py` script inside the container.

4.  **Run with Custom Parameters**:
    
    Edit the `run.sh` script to modify the command-line parameters before building:
    ```bash
    # Inside run.sh:
    python main.py --start-date 2025-01-01 --end-date 2025-05-01 --log-level DEBUG --stage silver
    ```
    
    Or pass them directly using environment variables:
    ```bash
    PIPELINE_ARGS="--start-date 2025-01-01 --end-date 2025-05-01 --stage silver" docker-compose up --build
    ```
    This requires adding the following line to the `docker-compose.yml` file's `command`:
    ```yaml
    command: bash -c "PIPELINE_ARGS=${PIPELINE_ARGS:-""} /app/run.sh"
    ```

### Environment Variables

*   `SOURCE_CSV_URL`: **Required**. The URL to the source CSV data file.
*   `GOOGLE_APPLICATION_CREDENTIALS`: Path to your Google Cloud service account key JSON file. Required only if using Google Cloud Storage export.
*   `GCS_BUCKET`: Optional default Google Cloud Storage bucket name. Can be overridden with the `--gcs-bucket` command line argument.

### Output Structure (Bronze Layer)

Upon successful execution, the Bronze layer will produce the following in the `backend/pipelines/arbejdstilsynet_inspections/data/bronze/` directory:

*   A timestamped subdirectory named `YYYYMMDD_HHMMSS` (e.g., `20250509_143000`).
*   Inside this subdirectory:
    *   `data.csv`: The raw, unaltered CSV data fetched from the source.
    *   `metadata.json`: A JSON file containing metadata about the fetch, including:
        *   `source_url`: The URL from which the data was fetched.
        *   `fetch_timestamp_utc_iso`: ISO 8601 timestamp (UTC) of when the data processing (fetch and save) was initiated by the script.
        *   `fetch_timestamp_dirname`: The name of the parent directory (e.g., `20250509_143000`).
        *   `description`: A brief description of the data.
        *   `pipeline_name`: `arbejdstilsynet_inspections`
        *   `layer`: `bronze`
        *   `file_format`: `csv`
        *   `data_filename`: `data.csv`
        *   `relative_data_file_path`: Path to the data file relative to the pipeline's `data` directory (e.g., `bronze/20250509_143000/data.csv`).
        *   `record_count`: (Placeholder, currently -1). Intended to store the number of records in the CSV.

## Silver Layer

The Silver layer processes and transforms the raw data from the Bronze layer, making it more suitable for analysis.

### Purpose

The Silver layer takes the raw CSV data, cleans it, normalizes values, and converts it to a more efficient Parquet format.

### Process

1. **Data Loading**: Reads the latest Bronze layer CSV data.
2. **Date Filtering**: 
   - Filters data based on the specified date range (`--start-date` and `--end-date` command-line arguments)
   - Default date range is from 6 months ago to the current date if not specified
3. **Cleaning**: 
   - Renames columns to follow consistent naming conventions
   - Deduplicates rows
   - Normalizes enum values and special characters (æ, ø, å)
   - Converts empty strings to null values
   - Applies appropriate type casting

4. **Privacy Protection**:
   - Checks for and anonymizes potential PII data

4. **Export**: Saves the processed data as a Parquet file for efficient querying.

### Output Structure (Silver Layer)

Upon successful execution, the Silver layer will produce the following in the `backend/pipelines/arbejdstilsynet_inspections/data/silver/` directory:

* A timestamped subdirectory named `YYYYMMDD_HHMMSS` (e.g., `20250509_143000`).
* Inside this subdirectory:
  * `processed_data.parquet`: The clean, transformed data in Parquet format.

## Gold Layer

*(To be implemented. This layer will provide aggregated data ready for consumption, e.g., for APIs or visualizations.)*

## Google Cloud Storage Export

When the `--gcs-bucket` parameter is provided, the pipeline will export data to the specified Google Cloud Storage bucket in addition to local storage. This enables:

1. **Data Integration**: Seamlessly integrate with other GCP services like BigQuery
2. **Data Sharing**: Make data accessible to other applications and teams
3. **Backup**: Maintain a cloud backup of all pipeline outputs

### Setup for GCS Export

1. **Authentication**: Configure Google Cloud authentication by either:
   * Setting the `GOOGLE_APPLICATION_CREDENTIALS` environment variable in your `.env` file to point to a service account key JSON file
   * Using the Google Cloud SDK's application default credentials: `gcloud auth application-default login`
   * When running in Google Cloud (e.g., Cloud Run, GKE), using the attached service account

2. **Required Permissions**: The service account needs the following permissions:
   * `storage.objects.create`
   * `storage.objects.get` 
   * `storage.objects.list`

3. **Data Storage Path**: The pipeline will export data to: `gs://<bucket-name>/arbejdstilsynet_inspections/<layer>/<timestamp>/`

### Example Usage

```bash
# Run the pipeline with GCS export
python main.py --gcs-bucket your-landbruget-data-bucket --stage all
```

> **Note:** When running in a Docker container, make sure to mount your service account key file into the container and set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to point to the mounted location. For example:
>
> ```yaml
> # In docker-compose.yml
> volumes:
>   - /path/to/your/credentials.json:/app/credentials.json
> environment:
>   - GOOGLE_APPLICATION_CREDENTIALS=/app/credentials.json
> ```

## Docker Environment Configuration

The pipeline is designed to run in a Docker container with the following key configurations:

1. **Virtual Display**: Uses `xvfb-run` to provide a virtual display for browser automation
2. **Browser Configuration**: Special flags are set for running Chrome in a containerized environment
3. **Volume Mounting**: The local directory is mounted to `/app` in the container for data persistence
4. **Environment Variables**: Passed from host to container via docker-compose.yml

If you encounter issues with the browser automation, make sure the `DISPLAY` environment variable is properly set (`DISPLAY=:99`) and xvfb is running correctly.
