# Google Drive Data Pipeline User Guide

This guide provides instructions for using the Google Drive Data Pipeline, including command-line options, execution patterns, and common workflows.

## Basic Usage

The pipeline can be run with default settings using:

```bash
python -m drive_data_pipeline.main
```

This will process all files from the configured Google Drive folder through both Bronze and Silver layers.

## Command-Line Options

The pipeline supports several command-line options to customize its behavior:

### Key Options

```bash
# Process specific subfolders only
python -m drive_data_pipeline.main --subfolders "subfolder1,subfolder2"

# Process specific file types only
python -m drive_data_pipeline.main --file-types "pdf,xlsx"

# Filter by modification date
python -m drive_data_pipeline.main --start-date 2023-01-01 --end-date 2023-12-31

# Process only Bronze or Silver layer
python -m drive_data_pipeline.main --bronze-only
python -m drive_data_pipeline.main --silver-only

# Control output verbosity
python -m drive_data_pipeline.main --verbose  # Detailed progress information
python -m drive_data_pipeline.main --quiet    # Suppress all non-essential output

# Use configuration file
python -m drive_data_pipeline.main --config-file "config.json"

# Set logging level
python -m drive_data_pipeline.main --log-level DEBUG
```

### Example Configuration File

You can create a JSON configuration file with desired options:

```json
{
  "subfolders": "animal_welfare,pesticides",
  "file_types": "pdf,xlsx",
  "start_date": "2023-01-01",
  "bronze_only": false,
  "verbose": true,
  "log_level": "INFO"
}
```

Then run the pipeline with:

```bash
python -m drive_data_pipeline.main --config-file config.json
```

## Common Workflows

### Initial Data Load

For the first run to download and process all data:

```bash
python -m drive_data_pipeline.main --verbose
```

### Incremental Updates

To process only files modified after a specific date:

```bash
python -m drive_data_pipeline.main --start-date 2023-06-01
```

### Reprocessing Silver Layer

To reprocess the Bronze data through the Silver layer:

```bash
python -m drive_data_pipeline.main --silver-only
```

### Processing Specific Data Categories

To process only certain subfolders:

```bash
python -m drive_data_pipeline.main --subfolders "animal_welfare,pig_movements"
```

### Focused Processing by File Type

To process only specific file types:

```bash
python -m drive_data_pipeline.main --file-types "pdf"
```

## Docker Execution

To run the pipeline in a Docker container:

```bash
# Build the container (if not already built)
docker-compose build

# Run the pipeline with default settings
docker-compose run --rm pipeline

# Run with specific options
docker-compose run --rm pipeline python -m drive_data_pipeline.main --verbose --subfolders "animal_welfare"
```

## Output and Results

### Data Locations

The pipeline stores data in the following locations:

- **Bronze data**: `data/bronze/YYYYMMDD_HHMMSS/`
- **Silver data**: `data/silver/YYYYMMDD_HHMMSS/`

Each run creates a timestamped directory to ensure data is preserved across runs.

### Logs

The pipeline generates two types of logs:

1. **Console output**: Direct feedback during execution
2. **Log files**: Detailed logs stored in the `logs/` directory

When using the `--verbose` option, additional progress information is displayed in the console.

### Run Summary

At the end of each run, the pipeline displays a summary of processed files, including:

- Number of files processed at each layer
- Error counts
- Total execution time
- Data volume statistics

Example summary:

```
============================================================
PIPELINE EXECUTION SUMMARY
============================================================

Bronze Layer:
  Files identified: 125
  Files downloaded: 122
  Download errors: 3
  Total data size: 45.72 MB

Silver Layer:
  Files processed: 122/122
  Processing errors: 0

Total execution time: 78.53 seconds
```

## Best Practices

1. **Start with verbose mode**: Use `--verbose` to see detailed progress information for initial runs
2. **Use configuration files**: For complex or recurring runs, save your settings in a configuration file
3. **Check logs**: Review logs for warnings and errors after each run
4. **Incremental processing**: Use date filters for regular updates rather than reprocessing all data
5. **Testing subsets**: Test processing on specific subfolders before running on the entire dataset

## Troubleshooting

If you encounter issues while running the pipeline:

1. Check the error messages in the console output
2. Review the detailed logs in the `logs/` directory
3. Try running with `--log-level DEBUG` for more detailed logging
4. Consult the troubleshooting guide in `docs/troubleshooting.md`

## Advanced Usage

### Creating Custom Configuration Files

You can save your current command-line options to a configuration file for future use:

```bash
# Export current settings to a config file
python -m drive_data_pipeline.config_tools.export_config --output config.json

# Then use that config file in future runs
python -m drive_data_pipeline.main --config-file config.json
```

### Parallel Processing

The pipeline uses parallel processing based on the `MAX_WORKERS` environment variable. Adjust this value to optimize for your hardware:

```bash
# Set in .env file
MAX_WORKERS=4

# Or override at runtime
MAX_WORKERS=4 python -m drive_data_pipeline.main
``` 