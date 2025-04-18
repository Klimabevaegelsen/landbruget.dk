# Creating Bronze Pipelines

This guide provides detailed instructions on how to create a new bronze pipeline in our data processing infrastructure. Bronze pipelines can handle various data source types including:
- REST/SOAP API endpoints
- Static files (CSV, Excel, GeoJSON, etc.) from public Google Drive folders
- FTP/SFTP servers
- Database exports
- Web scraping sources

We'll use our existing pipelines (CHR and Svineflytning) as examples while keeping the guidance applicable to all source types.

## Pipeline Structure

Each pipeline should follow this standard directory structure:

```
pipeline_name/
├── .env.example          # Example environment variables
├── .env                  # Local environment variables (gitignored)
├── README.md            # Pipeline documentation
├── main.py              # Main pipeline code
├── Dockerfile           # Container definition
├── docker-compose.yml   # Container orchestration
├── pyproject.toml       # Python dependencies
├── bronze/             # Bronze layer output directory
│   └── ...             # Raw data code and files
└── silver/             # Silver layer output directory
    └── ...             # Processed data code and files
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

- Use the official Python base image
- Install only necessary dependencies
- Set appropriate environment variables
- Example:
  ```dockerfile
  FROM python/3.11-slim
  
  WORKDIR /app
  
  COPY pyproject.toml .
  RUN pip install .
  
  COPY . .
  
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

Your `main.py` should follow this general structure, adapted for your data source type:

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
- Stream large result sets
- Handle connection timeouts
- Example:
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
- Save metadata with each data file
- Use appropriate file formats (Parquet/JSON/XML)
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

- Use command-line arguments for runtime configuration
- Use environment variables for secrets
- Provide sensible defaults
- Example:
  ```python
  parser.add_argument("--workers", type=int, default=10)
  parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING"], default="INFO")
  ```

## Examples

### CHR Pipeline (API Example)
The CHR pipeline demonstrates:
- Handling multiple data types (species, herds, movements)
- Complex authentication (certificates)
- Parallel processing
- Comprehensive error handling

### Svineflytning Pipeline (API Example)
The Svineflytning pipeline shows:
- Date-based data fetching
- SOAP API integration
- Chunked processing
- Progress tracking

### Static File Pipeline Example
A pipeline fetching data from public Google Drive should:
- Use direct download links
- Support different file formats
- Include file metadata in the output
- Handle large files efficiently

## Testing

1. Create test fixtures
2. Test with small data samples
3. Verify output format
4. Check error handling
Example:
```python
def test_fetch_data():
    result = fetch_data(start_date="2024-01-01", end_date="2024-01-02")
    assert result is not None
    assert "records" in result
```

## Deployment

1. Test locally with Docker
2. Push to repository
3. Configure GitHub Actions
4. Monitor initial runs

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