# CHR Pipeline

This directory contains the bronze, silver, and gold layer processing scripts for the CHR data pipeline.

## Features

- **Bronze Layer**: Fetches comprehensive CHR data including:
  - Species and usage combinations (Stamdata)
  - Herd information and details
  - Animal movements (DIKO)
  - Property details (Ejendom)
  - Veterinary events
  - VetStat antibiotic usage data
- **Silver Layer**: Processes and standardizes raw bronze data
- **Gold Layer**: Creates high-value analytical products:
  - **Veterinary Timeline**: Comprehensive timeline of all veterinary events per CHR
  - Combines animal welfare, SPF-SU data, veterinary status changes, and stable fires
  - Spatially matched stable fire events to CHR properties
- Processes data in parallel using multiple workers
- Handles pagination for large data sets
- Runs daily via GitHub Actions
- Exports data in a structured format

## Prerequisites

- Docker and Docker Compose
- FVM service credentials
- VetStat certificate (`.p12` file)

## Setup

1. Copy the example environment file and fill in your credentials:
   ```bash
   cp .env.example .env
   ```

2. Edit `.env` with your actual credentials:
   ```env
   FVM_USERNAME=your_username
   FVM_PASSWORD=your_password
   VETSTAT_CERTIFICATE_PASSWORD=your_certificate_password
   ```

3. Place your `vetstat.p12` certificate in the `chr_pipeline` directory.

4. Create data directories for the raw files:
   ```bash
   mkdir -p ../../data/bronze/chr
   ```

## Running the Pipeline

### Using Docker Compose (recommended)

1. Build and run the pipeline with test settings:
   ```bash
   docker-compose up --build
   ```

2. To run with custom settings, modify the command in `docker-compose.yml`:
   ```yaml
   command: >
     python main.py
     --test-species-codes "12"
     --limit-total-herds 5
     --progress
     --log-level INFO
   ```

### Available Options

- `--test-species-codes`: Comma-separated list of species codes to process (e.g., "12,13,14")
- `--limit-total-herds`: Maximum number of herds to process
- `--log-level`: Logging level (DEBUG, INFO, WARNING, ERROR)
- `--progress`: Show progress information
- `--steps`: Pipeline steps to run (all, stamdata, herds, herd_details, diko, ejendom, vetstat)
- `--workers`: Number of parallel workers (default: 10)

### Example Commands

1. Run for specific species with debug logging:
   ```bash
   docker-compose run --rm chr_pipeline \
     --test-species-codes "12,13" \
     --log-level DEBUG \
     --progress
   ```

2. Run only certain pipeline steps:
   ```bash
   docker-compose run --rm chr_pipeline \
     --steps "stamdata,herds,herd_details" \
     --progress
   ```

## Data Output

The pipeline outputs data to timestamped directories under `data/bronze/chr/` to avoid overwriting previous runs:
- Each run creates a new directory with format `YYYYMMDD_HHMMSS`
- Data is saved in both JSON and XML formats where applicable
- Metadata is included with each data file

## Troubleshooting

1. If you see credential errors:
   - Check that your .env file exists and contains the correct credentials
   - Verify that the VetStat certificate is properly placed and the password is correct
   - Ensure FVM credentials are valid

2. If you see XML processing errors:
   - Ensure the container has enough memory
   - Check the logs for specific error messages
   - Verify the VetStat certificate is valid

3. For connection issues:
   - Verify your network connection
   - Check if the service endpoints are accessible
   - The pipeline includes retry logic for transient failures

## Gold Layer Processing

The gold layer creates high-value analytical products from processed silver data.

### Veterinary Timeline

The main gold product is a comprehensive veterinary timeline that combines:
- Animal welfare interventions from the drive pipeline
- SPF-SU health certificates and disease statuses
- CHR veterinary status changes
- Spatially matched stable fire events
- Pig tail cutting control inspections

### Running Gold Processing Independently

You can run gold processing separately from the main pipeline:

```bash
# Process latest silver data
python run_gold_processing.py

# Process specific silver timestamp
python run_gold_processing.py --silver-timestamp 20240101_120000

# Debug mode
python run_gold_processing.py --log-level DEBUG --dry-run
```

### Gold Products

The gold layer produces:
- `veterinary_timeline.parquet` - Complete timeline of veterinary events per CHR
- `timeline_summary.parquet` - Summary statistics by data source

Data is exported to both local storage and GCS at `gs://landbrugsdata-raw-data/gold/chr/{timestamp}/`

## GitHub Actions

The pipeline runs automatically via GitHub Actions:
- Scheduled to run daily
- Can be triggered manually via workflow_dispatch
- Now includes gold layer processing after silver completion
- Individual steps can be run: `all`, `bronze_foundation`, `silver_processing`, `gold_processing`
- Data is stored in Google Cloud Storage in production

## Error Handling

The pipeline includes comprehensive error handling:
- Logs errors for failed API requests
- Continues processing on individual failures
- Maintains progress even if some requests fail
- Saves successfully fetched data even if some steps fail

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request
