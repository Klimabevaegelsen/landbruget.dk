# Google Drive Data Pipeline

A data pipeline that fetches files from Google Drive, processes them according to the medallion architecture (Bronze and Silver layers), and prepares them for analysis.

## Overview

This pipeline accesses a Google Drive folder containing multiple subfolders with PDF and Excel (xlsx/xls) files, downloads these files to a Bronze layer while preserving metadata, and processes them into a Silver layer with standardized formats and schemas.

### Features

- Google Drive API integration with authentication and retries
- Bronze layer for raw data storage with metadata
- Silver layer for cleaned and transformed data
- Robust Excel processing with automatic data type standardization
- Parquet file output with CSV fallback for challenging data formats
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
   - **Excel Transformer**: Converts Excel files to Parquet/CSV with standardized schemas
   - **PDF Transformer**: Extracts data from PDFs into structured formats
4. **Storage Manager**: Handles file storage and organization
   - **LocalStorageManager**: For local file system operations
   - **GCSStorageManager**: Placeholder for Google Cloud Storage

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
│   ├── storage.py              # Storage managers (Local, GCS)
│   └── logging.py              # Logging configuration
├── utils/                      # Utility functions
├── bronze/                     # Bronze layer implementation
│   ├── fetcher.py              # Google Drive fetcher
│   ├── processor.py            # Bronze data processor
│   ├── metadata.py             # Metadata management
│   └── storage.py              # Bronze storage management
├── silver/                     # Silver layer implementation
│   ├── processor.py            # Silver data processor
│   ├── storage.py              # Silver storage management
│   ├── parquet_manager.py      # Parquet output management
│   ├── transformers/           # Data transformers
│   │   ├── excel_transformer.py # Excel file transformer
│   │   └── pdf_transformer.py  # PDF file transformer
│   └── models/                 # Data models and schemas
├── tests/                      # Test suite
├── docs/                       # Documentation
└── data/                       # Local data directory (gitignored)
```

## Data Processing

### Excel Transformer

The Excel transformer reads Excel files (.xlsx, .xls) and converts them to Parquet format:

- Reads all sheets from the Excel file
- Standardizes column names (snake_case, special character handling)
- Performs data type detection and standardization
- Converts to Parquet format with appropriate schema
- Falls back to CSV if Parquet conversion fails due to complex data types

### Storage Management

The storage system provides abstraction over different storage backends:

- **StorageManager**: Abstract base class defining the interface
- **LocalStorageManager**: Concrete implementation for local file system
- **GCSStorageManager**: Placeholder for Google Cloud Storage

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