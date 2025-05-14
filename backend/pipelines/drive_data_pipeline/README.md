# Google Drive Data Pipeline

A data pipeline that fetches files from Google Drive, processes them according to the medallion architecture (Bronze and Silver layers), and prepares them for analysis.

## Overview

This pipeline accesses a Google Drive folder containing multiple subfolders with PDF and Excel (xlsx/xls) files, downloads these files to a Bronze layer while preserving metadata, and processes them into a Silver layer with standardized formats and schemas.

### Features

- Google Drive API integration with authentication and retries
- Bronze layer for raw data storage with metadata
- Silver layer for cleaned and transformed data
- Configurable via environment variables and command-line arguments
- Containerized for consistent execution

## Setup

### Prerequisites

- Python 3.11+
- Docker and Docker Compose (for containerized execution)
- Google Cloud service account with access to the target Google Drive folder

### Local Development Setup

1. Clone the repository
2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. Install dependencies:
   ```bash
   pip install -e .
   ```
4. Copy `.env.example` to `.env` and fill in the required values:
   ```bash
   cp .env.example .env
   ```
5. Set up Google Drive API credentials:
   - Create a service account in the Google Cloud Console
   - Download the JSON key file
   - Share the target Google Drive folder with the service account email
   - Set `GOOGLE_APPLICATION_CREDENTIALS` in `.env` to point to the JSON key file

### Docker Setup

1. Make sure Docker and Docker Compose are installed
2. Build the Docker image:
   ```bash
   docker-compose build
   ```
3. Run the pipeline using Docker:
   ```bash
   docker-compose up
   ```

## Usage

### Command-Line Arguments

The pipeline can be run with various command-line arguments:

```bash
python main.py --subfolders "folder1,folder2" --file-types "pdf,xlsx" --bronze-only
```

Available arguments:
- `--subfolders`: Specific subfolders to process (comma-separated)
- `--file-types`: Specific file types to process (comma-separated)
- `--start-date`: Process only files modified after this date (YYYY-MM-DD)
- `--end-date`: Process only files modified before this date (YYYY-MM-DD)
- `--bronze-only`: Run only the Bronze layer processing
- `--silver-only`: Run only the Silver layer processing (requires existing Bronze data)
- `--log-level`: Set logging level (DEBUG, INFO, WARNING, ERROR)

### Environment Variables

The pipeline can be configured using the following environment variables:

- `GOOGLE_DRIVE_FOLDER_ID`: ID of the Google Drive folder to process
- `GOOGLE_APPLICATION_CREDENTIALS`: Path to service account credentials file
- `STORAGE_TYPE`: Storage type ("local" or "gcs")
- `GCS_BUCKET`: GCS bucket name (if applicable)
- `LOG_LEVEL`: Logging level (DEBUG, INFO, WARNING, ERROR)
- `MAX_WORKERS`: Number of workers for parallel processing

## Architecture

The pipeline follows a medallion architecture with the following components:

1. **Fetcher**: Retrieves files from Google Drive using the Google Drive API
2. **Bronze Layer Processor**: Stores raw files with metadata
3. **Silver Layer Processor**: Transforms and cleans data
4. **Storage Manager**: Handles file storage and organization

## Project Structure

```
backend/pipelines/drive_data_pipeline/
├── .env.example                # Example environment variables
├── README.md                   # This file
├── main.py                     # Main entry point
├── Dockerfile                  # Docker configuration
├── docker-compose.yml          # Docker Compose configuration
├── pyproject.toml              # Project dependencies
├── config/                     # Configuration management
├── utils/                      # Utility functions
├── bronze/                     # Bronze layer implementation
├── silver/                     # Silver layer implementation
├── tests/                      # Test suite
├── docs/                       # Documentation
└── data/                       # Local data directory (gitignored)
```

## Development

### Testing

Run the tests with pytest:

```bash
pytest
```

### Linting and Type Checking

The codebase uses Ruff for linting and mypy for type checking:

```bash
ruff check .
mypy .
```

## License

[Your license information] 