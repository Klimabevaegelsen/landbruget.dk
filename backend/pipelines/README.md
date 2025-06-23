# Creating Bronze Pipelines

This guide provides detailed instructions on how to create a new bronze pipeline in our data processing infrastructure. Bronze pipelines can handle various data source types including:
- REST/SOAP API endpoints
- Static files (CSV, Excel, GeoJSON, etc.) from public Google Drive folders
- FTP/SFTP servers
- Database exports
- Web scraping sources

We'll use our existing pipelines (CHR and Svineflytning) as examples while keeping the guidance applicable to all source types.

We recommend using `uv` for faster Python package management and virtual environments. You can install it following the official `uv` documentation and use it for dependency installation and environment setup.

## Pipeline Structure

```
backend/pipelines
├── pipeline_name_1/
├── pipeline_name_2/
├── pipeline_name_3/
....
├── common/
    ├── logger_utils.py
    ├── argparser_utils.py
    ├── storage.py      # Google Cloud Storage utils
    ├── config.py
├── pyproject.toml
├── README.md
```

Each pipeline should follow this standard directory structure:

```
pipeline_name/
├── .env.example          # Example environment variables
├── .env                  # Local environment variables (gitignored)
├── README.md            # Pipeline documentation
├── main.py              # Main pipeline code
├── Dockerfile           # Container definition
├── docker-compose.yml   # Container orchestration
├── bronze/             # Bronze layer output directory
│   └── ...             # Raw data code and files
└── silver/             # Silver layer output directory
    └── ...             # Processed data code and files
└── handler/             # Google cloud function handler for the pipeline (optional)
    └── ...
```

## Getting Started

1. Create a new directory for your pipeline:
   ```bash
   mkdir -p backend/pipelines/your_pipeline_name
   cd backend/pipelines/your_pipeline_name
   ```

2. Copy the base pipeline structure:
   ```bash
   cp -r ../base/* .
   ```

## Key Components

### 1. Environment Variables (.env.example)

- Always include an `.env.example` file with dummy values
- Document all required credentials and configuration
- Use descriptive variable names
- Example:
  ```env
  # API/Service Credentials
  API_USERNAME=your_username
  API_PASSWORD=your_password

  # Storage Configuration
  OUTPUT_BUCKET=your-gcs-bucket
  ENVIRONMENT=dev

  # Source-specific Configuration
  SOURCE_URL=https://api.example.com
  FTP_HOST=ftp.example.com
  DATABASE_URL=postgresql://user:pass@host/db
  ```

### 2. Dockerfile

- Use the official Python base image (e.g., `python:3.11-slim`).
- Install `uv` for faster dependency management within the container.
- Install only necessary dependencies using `uv pip install --system .`.
- Set appropriate environment variables.
- Example:
  ```dockerfile
  # Use an official Python runtime as a parent image
  FROM python:3.11-slim

  # Install uv for faster package installation
  RUN pip install uv

  # Set the working directory in the container
  WORKDIR /app

  # Copy only necessary files for dependency installation first to leverage Docker cache
  COPY pyproject.toml ./

  # Install dependencies using uv
  # Using --system to install in the global Python environment within the container
  # --no-cache can be useful to reduce image size, but remove it if caching speeds up builds significantly
  RUN uv pip install --system . --no-cache

  # Copy the rest of the application code
  COPY . .

  # Command to run the application
  CMD ["python", "main.py"]
  ```

### 3. docker-compose.yml

- Define service configuration
- Mount necessary volumes
- Set environment variables
- Example:
  ```yaml
  version: '3.8'
  services:
    pipeline:
      build: .
      volumes:
        - ./data:/app/data
      env_file:
        - .env
  ```

### 4. main.py Structure

Your `main.py` should follow this general structure, adapted for your data source type. Synchronous code is generally preferred for simplicity unless asynchronous I/O provides significant performance benefits for your specific source type.

```python
import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import Union, Dict, Any

def setup_logging(level: str) -> None:
    # Configure logging with appropriate format and level
    pass

def parse_args() -> argparse.Namespace:
    # Define source-specific command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-type", choices=["api", "file", "database", "ftp"], required=True)
    # Add source-specific arguments
    return parser.parse_args()

class DataSourceConfig:
    """Configuration for different data source types"""
    def __init__(self, args: argparse.Namespace):
        self.source_type = args.source_type
        # Initialize based on source type
        pass

def fetch_data(config: DataSourceConfig) -> Union[Dict, Any]:
    """Fetch data based on source type"""
    if config.source_type == "api":
        return fetch_from_api(config)
    elif config.source_type == "file":
        return fetch_from_drive(config)
    # ... handle other source types

def save_raw_data(data: Any, output_dir: Path) -> None:
    """Save raw data with metadata, preserving original format"""
    # Save data in its original format (JSON, CSV, XML, etc.)
    # Add metadata including source information
    # **IMPORTANT**: Absolutely NO transformations should happen here.
    # The data saved must be identical to what was fetched.
    pass

def main() -> None:
    args = parse_args()
    setup_logging(args.log_level)

    # Create timestamped output directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path(f"data/bronze/{timestamp}")
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        config = DataSourceConfig(args)
        data = fetch_data(config)
        save_raw_data(data, output_dir)
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()
```

## Best Practices

### 1. Data Source Handling

#### API Sources
- Implement retry logic with exponential backoff
- Handle pagination and rate limits
- Save responses immediately
- Example:
  ```python
  def fetch_from_api(config: DataSourceConfig) -> Dict:
      # [Previous API example remains the same]
  ```

#### Static Files (from public Google Drive)
- Use direct download links from public Google Drive folders
- Handle large file downloads in chunks
- Preserve original file format
- Example:
  ```python
  def fetch_from_drive(config: DataSourceConfig) -> Path:
      """Fetch file from public Google Drive"""
      file_id = config.file_id
      download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
      output_path = Path(f"temp_{datetime.now().timestamp()}")

      try:
          # Download file in chunks
          with requests.get(download_url, stream=True) as r:
              r.raise_for_status()
              with open(output_path, 'wb') as f:
                  for chunk in r.iter_content(chunk_size=8192):
                      f.write(chunk)
          return output_path
      finally:
          # Cleanup temp files if necessary
          pass
  ```

#### Database Sources
- Use connection pooling
- Stream large result sets using methods appropriate for your database connector or libraries like `pandas` or `ibis`.
- Handle connection timeouts
- Example (using pandas):
  ```python
  def fetch_from_database(config: DataSourceConfig) -> Iterator:
      """Stream data from database"""
      with get_db_connection() as conn:
          for chunk in pd.read_sql_query(
              config.query, conn, chunksize=10000
          ):
              yield chunk
  ```

#### FTP/SFTP Sources
- Handle connection drops
- Verify file listings before download
- Support resume for large files

### 2. Error Handling

- Log all errors with appropriate context
- Continue processing on non-critical failures
- Save partial results when possible
- Example:
  ```python
  try:
      process_chunk(data)
  except Exception as e:
      logging.error(f"Failed to process chunk: {e}")
      failed_chunks.append(data)
  ```

### 3. Data Storage

- Use timestamped directories for each run
- Save metadata with each data file (e.g., source, timestamp, record count)
- Use appropriate file formats. Parquet is strongly preferred for structured data due to its efficiency and excellent compatibility with `ibis` and `duckdb`. JSON/XML might be necessary only to preserve the exact raw source format if required.
- **In production, both Bronze and Silver layer data are stored in Google Cloud Storage (GCS).** Silver data should typically be stored as Parquet/GeoParquet files.
- Example:
  ```python
  def save_with_metadata(data: dict, path: Path) -> None:
      # Save data
      data_path = path / "data.json"
      with data_path.open("w") as f:
          json.dump(data, f)

      # Save metadata
      metadata = {
          "timestamp": datetime.now().isoformat(),
          "source": "API_NAME",
          "version": "1.0",
          "record_count": len(data)
      }
      metadata_path = path / "metadata.json"
      with metadata_path.open("w") as f:
          json.dump(metadata, f)
  ```

### 4. Configuration

- Use command-line arguments for runtime configuration (e.g., dates, output paths).
- Use environment variables for secrets and environment-specific settings (credentials, bucket names).
- Provide sensible defaults.
- Consider using a configuration library if complexity grows.
- Example (`argparse`):
  ```python
  parser.add_argument("--workers", type=int, default=10)
  parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING"], default="INFO")
  ```

## Examples

### CHR Pipeline (API Example)
The CHR pipeline demonstrates:
- Handling multiple related data types from a SOAP API (species, herds, movements).
- Complex authentication using certificates (`xmlsec`, `cryptography`).
- Using `ibis-framework[duckdb]` for efficient data transformation and processing (preferred approach).
- Using `pandas` and `geopandas` for initial data loading or specific geo-processing tasks where necessary.
- Structured logging and error handling.

### Svineflytning Pipeline (API Example)
The Svineflytning pipeline shows:
- A simpler SOAP API integration (`zeep`, `lxml`).
- Date-based data fetching logic.
- Saving raw data to GCS using `pandas`.
- Clear separation of concerns (fetching, saving).

### Static File Pipeline Example
A pipeline fetching data from public Google Drive should:
- Use direct download links
- Support different file formats
- Include file metadata in the output
- Handle large files efficiently

## Testing

### Unit and Integration Tests
1. Create test fixtures or use sample data.
2. Test data fetching logic for different scenarios (e.g., date ranges, connection issues).
3. Test data processing/transformation logic, **preferably using `ibis` expressions and `duckdb`** for validation against expected outcomes.
4. Verify output format (ideally Parquet) and metadata correctness.
5. Check error handling mechanisms.

Example:
```python
def test_fetch_data():
    result = fetch_data(start_date="2024-01-01", end_date="2024-01-02")
    assert result is not None
    assert "records" in result
```

### Local Pipeline Testing
Test your pipeline locally before deployment:

```bash
# Test with Docker Compose
docker-compose up --build

# Test the main script directly
python main.py --help
python main.py --source-type api --dry-run
```

### GitHub Actions Workflow Testing with `act`

Before pushing workflow changes, test them locally using [`act`](https://github.com/nektos/act):

```bash
# Install act (if not already installed)
brew install act  # macOS
# or curl https://raw.githubusercontent.com/nektos/act/master/install.sh | sudo bash

# List available workflows
act -l

# Test your pipeline workflow (dry run)
act workflow_dispatch -W .github/workflows/your_pipeline.yml -n

# Test with test secrets
echo "API_KEY=test_key" > .secrets.test
act workflow_dispatch -W .github/workflows/your_pipeline.yml --secret-file .secrets.test

# Test specific workflow inputs
act workflow_dispatch -W .github/workflows/drive_pipeline.yml \
  --input drive_folder_id="test-folder-id" \
  --input environment="test" \
  -n
```

**Benefits of local testing with `act`:**
- Catch workflow syntax errors before pushing
- Validate job dependencies and conditionals
- Test environment variable handling
- Verify workflow logic without using GitHub Actions minutes

**Limitations:**
- Some GitHub-specific features may not work exactly the same
- Certain actions may behave differently in containers
- Always do a final test in the actual GitHub Actions environment

## Deployment

**The target runtime environment for these pipelines is GitHub Actions.** Workflows defined in `.github/workflows/` will build the Docker image and execute the pipeline script (`main.py`) directly within a runner environment.

The typical development and deployment flow is:

1. Develop and test the pipeline locally using `docker-compose up --build`.
2. Push code changes to the repository.
3. Configure the relevant GitHub Actions workflow (typically found in `.github/workflows/`) to trigger on pushes/merges/schedules as needed. This workflow handles building the Docker image and running the containerized pipeline.
4. Monitor pipeline execution and logs directly within the GitHub Actions run interface.

## Common Issues and Solutions

1. API Rate Limiting
   - Implement exponential backoff
   - Use appropriate delays between requests
   - Monitor API quotas

2. Memory Management
   - Process data in chunks
   - Use generators for large datasets
   - Clean up temporary files

3. Error Recovery
   - Save progress frequently
   - Implement resume functionality
   - Log detailed error information

4. File Source Issues
   - Handle corrupt downloads
   - Verify file format matches expectations
   - Track file version/modification dates
   - Handle Google Drive download quotas for large files
   - Use appropriate file download method based on size

## Tooling Recommendations

- **Dependency Management**: `uv` for faster installation and environment management.
- **Data Manipulation**: **`ibis-framework` with the `duckdb` backend is the preferred choice** for performant, scalable, and database-agnostic transformations. Use `pandas` or `geopandas` primarily for initial data loading, simple manipulations, or when specific features (like advanced geospatial operations in `geopandas`) are required that `ibis` doesn't cover directly.
- **API Interaction**: `requests` for REST, `zeep` for SOAP.
- **Cloud Storage**: `google-cloud-storage` and `gcsfs` for seamless GCS integration, especially useful with `ibis` and `duckdb` reading/writing Parquet files directly from/to GCS.

## Contributing

1. Follow the structure of existing pipelines
2. Document all configuration options
3. Include comprehensive error handling
4. Add appropriate logging
5. Test thoroughly before submitting

Remember to follow the general guidelines in the main backend README.md regarding:
- Data quality and formats
- Security considerations
- PII handling
- Performance optimization
- Source-appropriate error handling
