# Svineflytning Pipeline

This pipeline fetches pig movement data from the SvineflytningWS SOAP service and processes it into a standardized format following the Medallion architecture (Bronze → Silver).

## Features

### Bronze Layer
- Fetches pig movement data for the last 5 years by default
- Processes data in parallel using multiple workers
- Handles pagination and chunking of requests (max 3 days per request as per API requirements)
- Exports raw data in JSON format

### Silver Layer
- Transforms raw pig movement data into clean, structured tables
- Creates three main tables: movements, properties, and vehicles
- Performs data quality checks and validation
- Standardizes column names and data types
- Exports processed data in Parquet format

### Automation
- Runs via GitHub Actions with configurable stages
- Supports both bronze and silver processing
- Can process specific bronze timestamps for silver layer

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

1. Build and run the complete pipeline (bronze + silver):
   ```bash
   docker-compose up --build
   ```

2. Run only bronze stage:
   ```bash
   docker-compose run --rm svineflytning-pipeline --stage bronze --start-date 2024-01-01 --end-date 2024-03-31
   ```

3. Run only silver stage (processes latest bronze data):
   ```bash
   docker-compose run --rm svineflytning-pipeline --stage silver
   ```

4. Run silver stage for specific bronze timestamp:
   ```bash
   docker-compose run --rm svineflytning-pipeline --stage silver --bronze-timestamp 20240707_065654
   ```

### Available Options

- `--stage`: Pipeline stage to run (bronze, silver, all) - default: all
- `--start-date`: Start date in YYYY-MM-DD format (default: 5 years ago) - bronze stage only
- `--end-date`: End date in YYYY-MM-DD format (default: today) - bronze stage only
- `--bronze-timestamp`: Specific bronze timestamp to process (YYYYMMDD_HHMMSS) - silver stage only
- `--log-level`: Logging level (DEBUG, INFO, WARNING, ERROR)
- `--progress`: Show progress information
- `--environment`: Environment to use (prod, test) - bronze stage only
- `--test`: Run in test mode with limited data - bronze stage only
- `--max-concurrent-fetches`: Number of parallel API calls (default: 5) - bronze stage only
- `--buffer-size`: Number of responses to buffer (default: 50) - bronze stage only

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

### Bronze Layer
- **Local**: `/data/raw/svineflytning/{timestamp}/svineflytning.json`
- **GCS**: `gs://landbrugsdata-raw-data/bronze/svineflytning/{timestamp}/svineflytning.json`

### Silver Layer
- **Local**: `/data/silver/svineflytning/{timestamp}/`
  - `movements.parquet` - Main pig movement records
  - `properties.parquet` - Property/farm information
  - `vehicles.parquet` - Transport vehicle data
- **GCS**: `gs://landbrugsdata-raw-data/silver/svineflytning/{timestamp}/`

### Data Schema

#### Movements Table
- `movement_id` - Unique movement identifier
- `movement_date` - Date of pig movement
- `sender_chr_number` - Sender farm CHR number
- `receiver_chr_number` - Receiver farm CHR number
- `total_animals` - Number of animals moved
- `vehicle_registration` - Transport vehicle registration
- Data quality flags and processing metadata

#### Properties Table
- `chr_number` - CHR number (farm identifier)
- `address` - Farm address
- `municipality_code` - Municipality code
- `postal_code` - Postal code

#### Vehicles Table
- `vehicle_registration` - Vehicle registration number
- `usage_count` - Number of times used
- `first_movement_date` - First recorded movement
- `last_movement_date` - Last recorded movement

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
