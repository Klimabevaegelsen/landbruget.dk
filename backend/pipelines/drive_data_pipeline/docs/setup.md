# Google Drive Data Pipeline Setup Guide

This document provides detailed instructions for setting up and configuring the Google Drive Data Pipeline.

## Prerequisites

Before setting up the pipeline, ensure you have the following prerequisites installed:

- Python 3.11 or higher
- Docker and Docker Compose (for containerized execution)
- Git (for source code management)

## Installation

### Local Development Setup

1. Clone the repository from version control:

   ```bash
   git clone [repository-url]
   cd [repository-directory]
   ```

2. Create and activate a virtual environment:

   ```bash
   # On Windows
   python -m venv venv
   venv\Scripts\activate

   # On macOS/Linux
   python -m venv venv
   source venv/bin/activate
   ```

3. Install dependencies:

   ```bash
   pip install -e .
   ```

   Alternatively, you can use the requirements.txt file:

   ```bash
   pip install -r requirements.txt
   ```

### Docker Setup

1. Build the Docker image:

   ```bash
   docker-compose build
   ```

2. This creates a containerized environment with all required dependencies.

## Configuration

### Environment Variables

Create a `.env` file based on the provided `.env.example`:

```bash
cp .env.example .env
```

Edit the `.env` file to include the following required variables:

- `GOOGLE_DRIVE_FOLDER_ID`: The ID of the Google Drive folder to process
- `GOOGLE_APPLICATION_CREDENTIALS`: Path to Google service account credentials
- `STORAGE_TYPE`: Storage type (local or gcs)
- `GCS_BUCKET`: GCS bucket name (if applicable)
- `LOG_LEVEL`: Logging level (DEBUG, INFO, WARNING, ERROR)
- `MAX_WORKERS`: Number of workers for parallel processing

### Google Drive API Setup

To access the Google Drive API, you need to:

1. Create a Google Cloud project:
   - Go to [Google Cloud Console](https://console.cloud.google.com/)
   - Create a new project or select an existing one
   - Enable the Google Drive API for your project

2. Create a service account:
   - In the Google Cloud Console, go to "IAM & Admin" > "Service Accounts"
   - Create a new service account
   - Grant the required permissions for Drive API access

3. Generate and download the service account key:
   - In the service account details, go to the "Keys" tab
   - Create a new key (JSON format)
   - Download the key file and store it securely

4. Share the target Google Drive folder with the service account email address

5. Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to the path of the downloaded key file

## Directory Structure

The pipeline follows this directory structure:

```
backend/pipelines/drive_data_pipeline/
├── .env.example              # Example environment variables
├── README.md                 # Project overview
├── main.py                   # Main entry point
├── Dockerfile                # Docker configuration
├── docker-compose.yml        # Docker Compose configuration
├── pyproject.toml            # Project dependencies
├── config/                   # Configuration management
├── utils/                    # Utility functions
├── bronze/                   # Bronze layer implementation
├── silver/                   # Silver layer implementation
├── tests/                    # Test suite
├── docs/                     # Documentation
└── data/                     # Local data directory (gitignored)
```

## Data Storage

The pipeline organizes data in the following structure:

### Bronze Layer

```
data/bronze/YYYYMMDD_HHMMSS/
├── [subfolder1]/
│   ├── file1.xlsx
│   ├── file1.xlsx.metadata.json
│   └── ...
├── [subfolder2]/
│   ├── file2.pdf
│   ├── file2.pdf.metadata.json
│   └── ...
└── metadata.json            # Run metadata
```

### Silver Layer

```
data/silver/YYYYMMDD_HHMMSS/
├── [subfolder1]/
│   ├── file1.parquet
│   └── ...
├── [subfolder2]/
│   ├── file2.parquet
│   └── ...
└── metadata.json            # Run metadata
```

## Verification

After setup, you can verify your installation with:

```bash
python -m drive_data_pipeline.main --verbose
```

This will run the pipeline with verbose logging and verify that all components are functioning correctly.

## Troubleshooting

### Common Issues

1. **Authentication Errors**:
   - Ensure the service account key file is correctly specified
   - Verify that the target Google Drive folder is shared with the service account

2. **Missing Dependencies**:
   - Run `pip install -r requirements.txt` to ensure all dependencies are installed
   - Check for error messages related to missing packages

3. **Folder Access Issues**:
   - Verify the `GOOGLE_DRIVE_FOLDER_ID` is correct
   - Ensure the service account has access to the folder

4. **Storage Errors**:
   - For GCS storage, verify that the credentials have the necessary permissions
   - For local storage, ensure the application has write access to the data directory

### Getting Help

If you encounter issues not covered here, please:

1. Check the logs for detailed error messages
2. Refer to the troubleshooting guide in `docs/troubleshooting.md`
3. Contact the development team for further assistance 