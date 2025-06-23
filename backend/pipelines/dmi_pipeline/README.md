# DMI Climate Data Pipeline

This pipeline fetches and processes climate data from the Danish Meteorological Institute (DMI) API, transforming it into a standardized format for analysis.

## Features

- Fetches climate grid data for specified parameters and time ranges
- Processes data in parallel using multiple workers
- Handles geospatial data with proper coordinate system transformations
- Runs daily via GitHub Actions
- Exports data in both raw and processed formats

## Prerequisites

- Docker and Docker Compose
- DMI GovCloud API key

## Setup

1. Copy the example environment file and fill in your credentials:
   ```bash
   cp .env.example .env
   ```

2. Edit `.env` with your actual credentials:
   ```env
   DMI_GOV_CLOUD_API_KEY=your_api_key
   MAX_RETRIES=3
   RETRY_DELAY=5
   ```

3. Create data directories for the raw files:
   ```bash
   mkdir -p data/bronze/dmi
   mkdir -p data/silver/dmi
   ```

## Running the Pipeline

### Using Docker Compose (recommended)

1. Build and run the pipeline:
   ```bash
   docker-compose up --build
   ```

2. To specify a custom parameter and date range:
   ```bash
   docker-compose run --rm dmi-pipeline --parameter temperature --days 30
   ```

### Available Options

- `--parameter`: Climate parameter ID to fetch (required)
- `--days`: Number of days of data to fetch (default: 30)
- `--workers`: Number of parallel workers (default: 10)
- `--log-level`: Logging level (DEBUG, INFO, WARNING, ERROR)
- `--progress`: Show progress information

### Example Commands

1. Run for a specific parameter with debug logging:
   ```bash
   docker-compose run --rm dmi-pipeline \
     --parameter precipitation \
     --days 60 \
     --log-level DEBUG
   ```

2. Run with progress information:
   ```bash
   docker-compose run --rm dmi-pipeline \
     --parameter temperature \
     --progress
   ```

## Data Output

The pipeline outputs data to timestamped directories under:
- Raw data: `data/bronze/dmi/<timestamp>/`
- Processed data: `data/silver/dmi/<timestamp>/`

Each run creates a new directory with format `YYYYMMDD_HHMMSS` containing:
- Raw data in GeoJSON format
- Processed data in Parquet format
- Metadata files with transformation details

## Data Processing

### Bronze Stage
The Bronze stage downloads raw climate data from the DMI API and saves it with the following structure:
```
bronze/dmi/<timestamp>/
  ├── <parameter>_raw.geojson    # Raw data from DMI API
  └── metadata.json              # Metadata about the download
```

### Silver Stage
The Silver stage processes the raw data into a structured format:
```
silver/dmi/<timestamp>/
  ├── <parameter>_processed.parquet  # Cleaned and structured data
  └── metadata.json                  # Transformation metadata
```

The transformation process includes:
- Coordinate system conversion (EPSG:25832 to EPSG:4326)
- Data aggregation by time period
- Calculation of statistics (avg, min, max, count)
- Quality validation

## GitHub Actions

The pipeline runs automatically via GitHub Actions:
- Scheduled to run daily at 2 AM UTC
- Can be triggered manually via workflow_dispatch
- Data is stored in Google Cloud Storage in production

## Error Handling

The pipeline includes comprehensive error handling:
- Retries for failed API requests
- Rate limit handling
- Coordinate system validation
- Data quality checks
- Detailed logging of all operations

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## Troubleshooting

1. If you see API key errors:
   - Check that your .env file exists and contains the correct DMI_GOV_CLOUD_API_KEY
   - Verify that the API key is valid and has the necessary permissions

2. If you see coordinate system errors:
   - Ensure the container has enough memory
   - Check the logs for specific error messages
   - Verify the CRS constants in DMIConfig

3. For connection issues:
   - Verify your network connection
   - Check if the DMI API endpoints are accessible
   - The pipeline includes retry logic for transient failures
