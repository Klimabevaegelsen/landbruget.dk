# Svineflytning Pipeline

This pipeline fetches pig movement data from the SvineflytningWS SOAP service and processes it into a standardized format.

## Features

- Fetches pig movement data for the last 5 years by default
- Processes data in parallel using multiple workers
- Handles pagination and chunking of requests (max 3 days per request as per API requirements)
- Runs daily via GitHub Actions
- Exports data in a structured format

## Prerequisites

- Docker and Docker Compose
- Access credentials for FVM services

## Setup

1. Copy the example environment file and fill in your credentials:
   ```bash
   cp .env.example .env
   ```

2. Edit `.env` with your actual credentials:
   FVM_USERNAME and FVM_PASSWORD

3. Create a data directory for the raw files:
   ```bash
   mkdir -p data/raw/svineflytning
   ```

## Running the Pipeline

### Using Docker Compose (recommended)

1. Build and run the pipeline:
   ```bash
   docker-compose up --build
   ```

2. To specify a custom date range:
   ```bash
   docker-compose run --rm svineflytning-pipeline --start-date 2024-01-01 --end-date 2024-03-31
   ```

### Available Options

- `--start-date`: Start date in YYYY-MM-DD format (default: 5 years ago)
- `--end-date`: End date in YYYY-MM-DD format (default: today)
- `--workers`: Number of parallel workers (default: 10)
- `--log-level`: Logging level (DEBUG, INFO, WARNING, ERROR)
- `--progress`: Show progress information
- `--environment`: Environment to use (prod, test)
- `--test`: Run in test mode with limited data
- `--gcs-bucket`: Google Cloud Storage bucket for export

### Example Commands

1. Run for a specific month with debug logging:
   ```bash
   docker-compose run --rm svineflytning-pipeline \
     --start-date 2024-02-01 \
     --end-date 2024-02-29 \
     --log-level DEBUG
   ```

2. Run with progress information in test mode:
   ```bash
   docker-compose run --rm svineflytning-pipeline \
     --progress \
     --test
   ```

## Data Output

The pipeline outputs data to the following locations:
- Raw data: `/data/raw/svineflytning/`
- Bronze layer transformations: `bronze/`
- Silver layer transformations: `silver/`

## Troubleshooting

1. If you see credential errors:
   - Check that your .env file exists and contains the correct FVM credentials
   - Verify that FVM_USERNAME and FVM_PASSWORD are properly set

2. If you see XML processing errors:
   - Ensure the container has enough memory
   - Check the logs for specific error messages

3. For connection issues:
   - Verify your network connection
   - Check if the service endpoints are accessible
   - The pipeline includes retry logic for transient failures

## GitHub Actions

The pipeline runs automatically via GitHub Actions:
- Scheduled to run daily at 2 AM UTC
- Can be triggered manually via workflow_dispatch
- Artifacts are stored for 7 days

## Error Handling

The pipeline includes comprehensive error handling:
- Configurable logging levels (DEBUG, INFO, WARNING, ERROR)
- Separate logging configuration for pipeline and third-party modules
- Progress tracking with tqdm integration
- Graceful error handling with detailed error messages
- Continues processing on chunk failures
- Maintains progress even if some requests fail

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request
