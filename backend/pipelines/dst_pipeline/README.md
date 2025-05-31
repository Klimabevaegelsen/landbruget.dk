# Danmarks Statistik API Pipeline

This pipeline fetches data from Danmarks Statistik's (Danish Statistics) open API and stores it in the bronze layer following the medallion architecture pattern.

## Overview

The pipeline provides access to all publicly available statistical data from Danmarks Statistik through their REST API. It's designed to fetch raw data and preserve it exactly as received from the source, with no transformations applied (bronze layer principle).

### Key Features

- Fetches data from any table in Danmarks Statistik's API
- Supports HST77 (harvest data) and GARTN1 (fruit/vegetable production) tables
- Handles table metadata and data separately
- Implements proper rate limiting and retry logic
- Preserves raw data format with comprehensive metadata
- Supports filtering by variables and time periods
- Stores data in CSV format as received from the API

### Implemented Tables

#### HST77 - Høstresultat (Harvest Results)
- **Description**: Agricultural harvest data by region, crop, unit, and year
- **Data Coverage**: 
  - **Regions**: 9 areas (whole country + selected regions)
  - **Crops**: 30 different crops (grains, rapeseed, legumes, root vegetables, etc.)
  - **Unit**: Average yield (hkg per hectare)
  - **Years**: 2006-2024
  - **Records**: 5,131 data points
- **Variables**:
  - `OMRÅDE`: Regional areas (000, 15, 04, 085, 07, 08, 09, 10, 081)
  - `AFGRØDE`: Crop types (H100-H685)
  - `MÆNGDE4`: Unit type (020 = Average yield)
  - `Tid`: Years (2006-2024)

#### GARTN1 - Produktion af frugt og grønt (Fruit and Vegetable Production)
- **Description**: Production of fruits and vegetables by region, unit, crop, and year
- **Data Coverage**:
  - **Regions**: 9 areas (same as HST77)
  - **Crops**: 53 different fruits and vegetables
  - **Units**: 3 measurement types (cultivated area, harvested area, production)
  - **Years**: 2003-2023
  - **Records**: 30,052 data points
- **Variables**:
  - `OMRÅDE`: Regional areas (000, 15, 04, 085, 07, 08, 09, 10, 081)
  - `TAL`: Measurement units (500=cultivated area, 600=harvested area, 700=production)
  - `AFGRØDE`: Crop types (0010-0265) - vegetables, fruits, berries
  - `Tid`: Years (2003-2023)

#### FRO - Frøproduktion (Seed Production)
- **Description**: Seed production data by crop, unit, and year (national level only)
- **Data Coverage**:
  - **Regions**: National data only (no regional breakdown)
  - **Crops**: 22 different seed types (grass seeds, legumes, etc.)
  - **Units**: 3 measurement types (area, average yield, production)
  - **Years**: 1989-2023 (35 years of historical data)
  - **Records**: 2,311 data points
- **Variables**:
  - `AFGRØDE`: Seed crop types (1100-1200) - grass seeds, grass legumes, etc.
  - `MÆNGDE4`: Measurement units (010=area, 020=yield, 700=production)
  - `Tid`: Years (1989-2023)

#### HALM1 - Halmudbytte og halmanvendelse (Straw Yield and Usage)
- **Description**: Straw production and usage by region, crop, unit, usage type, and year
- **Data Coverage**:
  - **Regions**: 9 areas (same as HST77)
  - **Crops**: 16 straw-producing crops (grains, rapeseed, legumes)
  - **Units**: 2 measurement types (area, amount in million kg)
  - **Usage**: 5 usage types (total, heating, feed, bedding, not harvested)
  - **Years**: 2006-2024
  - **Records**: 27,361 data points
- **Variables**:
  - `OMRÅDE`: Regional areas (000, 15, 04, 085, 07, 08, 09, 10, 081)
  - `AFGRØDE`: Straw crop types (H90-H312) - all crops, grains, rapeseed, legumes
  - `ENHED`: Measurement units (010=area, 050=amount in million kg)
  - `ANVENDELSE`: Usage types (1010=total, 1020=heating, 1030=feed, 1040=bedding, 1050=not harvested)
  - `Tid`: Years (2006-2024)

## Usage

### Basic Usage

```bash
# Fetch HST77 harvest data
python main.py --table-id HST77 --log-level INFO

# Fetch GARTN1 fruit/vegetable data  
python main.py --table-id GARTN1 --log-level INFO

# Fetch FRO seed production data
python main.py --table-id FRO --log-level INFO

# Fetch HALM1 straw yield and usage data
python main.py --table-id HALM1 --log-level INFO

# Fetch any other table with minimal data
python main.py --table-id TABLE_ID --log-level INFO
```

### Docker Usage

```bash
# Build and run with docker-compose
docker-compose up --build

# Or build manually
docker build -t dst-pipeline .
docker run -v $(pwd)/bronze:/app/bronze dst-pipeline python main.py --table-id HST77
```

### Command Line Options

- `--table-id`: The table ID to fetch (required)
- `--output-dir`: Output directory (default: ./bronze)
- `--lang`: Language for API responses (default: da) 
- `--log-level`: Logging level (default: INFO)
- `--start-time`: Start time filter (optional)
- `--end-time`: End time filter (optional)

## Data Output

The pipeline creates timestamped directories under `bronze/` with the following files:

- `{TABLE_ID}_tableinfo.json`: Complete table metadata from the API
- `{TABLE_ID}_tableinfo_metadata.json`: Fetch metadata for table info
- `{TABLE_ID}_data.csv`: Raw data in CSV format as received from API
- `{TABLE_ID}_data_metadata.json`: Fetch metadata for data

### Data Quality Notes

- Missing data is represented as ".." in the CSV files (as per DST conventions)
- Data is stored exactly as received from the API with no cleaning or transformation
- Some years may have limited data availability (particularly earlier years)
- Regional data may be suppressed for confidentiality reasons in some cases

## API Information

- **Base URL**: https://api.statbank.dk/v1
- **Documentation**: https://www.dst.dk/da/Statistik/hjaelp-til-statistikbanken/api
- **Format**: CSV (semicolon-separated)
- **Rate Limiting**: Built-in with exponential backoff retry logic
- **Authentication**: None required (public API)

## Contributing

To add support for new tables:

1. Run the pipeline with the new table ID to fetch metadata
2. Examine the table structure in the generated `tableinfo.json`
3. Add a new configuration section in `main.py` similar to HST77/GARTN1
4. Define appropriate variable selections based on the table's variables
5. Test with a small subset first, then scale up

## Requirements

See `pyproject.toml` for complete dependency list. Key dependencies:

- `requests`: HTTP client for API calls
- `pandas`: Data manipulation
- `pyarrow`: Parquet support
- `ibis-framework[duckdb]`: Data processing
- `google-cloud-storage`: Cloud storage integration

## Environment

The pipeline supports both local development and production deployment:

- **Development**: Data stored locally in `bronze/` directory
- **Production**: Can be configured to store data in Google Cloud Storage

## Recent Updates

- ✅ Successfully implemented HST77 harvest data pipeline (5,131 records)
- ✅ Successfully implemented GARTN1 fruit/vegetable production pipeline (30,052 records)
- ✅ Successfully implemented FRO seed production pipeline (2,311 records)
- ✅ Successfully implemented HALM1 straw yield and usage pipeline (27,361 records)
- ✅ **Successfully implemented Silver Layer processing for all four tables**
- ✅ Added comprehensive error handling and debug logging
- ✅ Implemented proper CSV format handling for DST API responses
- ✅ Added timestamped output directories for data versioning
- ✅ **Clean, harmonized Parquet files with standardized column names**
- ✅ **Added crop categorization and data type standardization**

## API Documentation

- **Base URL**: https://api.statbank.dk/v1
- **Official Documentation**: https://www.dst.dk/da/Statistik/hjaelp-til-statistikbanken/api
- **API Console**: Available on the official documentation page

## Installation and Setup

### Prerequisites

- Python 3.11+
- Docker and Docker Compose (for containerized execution)
- `uv` package manager (recommended for faster dependency management)

### Local Development Setup

1. Navigate to the pipeline directory:
   ```bash
   cd backend/pipelines/dst_api_pipeline
   ```

2. Install dependencies using uv:
   ```bash
   uv pip install .
   ```

3. Create a `.env` file based on the example (see Configuration section below)

### Docker Setup

Build and run using Docker Compose:
```bash
docker-compose up --build
```

## Configuration

### Environment Variables

Create a `.env` file with the following variables:

```env
# Environment Configuration
ENVIRONMENT=dev

# Storage Configuration (for production)
OUTPUT_BUCKET=your-gcs-bucket

# Danmarks Statistik API Configuration
DST_API_BASE_URL=https://api.statbank.dk/v1
DST_API_LANG=da

# Logging Configuration
LOG_LEVEL=INFO

# Optional: Rate limiting configuration
REQUEST_DELAY_SECONDS=0.5
MAX_RETRIES=3
```

### Command Line Arguments

The pipeline supports the following command line arguments:

- `--table-id` (required): The table ID to fetch (e.g., HST77)
- `--output-dir`: Output directory for bronze data (default: ./bronze)
- `--log-level`: Logging level (DEBUG, INFO, WARNING, ERROR)
- `--lang`: Language for API responses (da, en)
- `--variables`: Specific variables to fetch (optional)
- `--start-time`: Start time filter (optional)
- `--end-time`: End time filter (optional)

## Usage Examples

### Basic Usage - Fetch HST77 Table

```bash
python main.py --table-id HST77
```

### Fetch with Specific Language

```bash
python main.py --table-id HST77 --lang en
```

### Fetch with Variable Filters

```bash
python main.py --table-id HST77 --variables OMRÅDE TID
```

### Fetch with Time Range

```bash
python main.py --table-id HST77 --start-time 2020 --end-time 2023
```

### Debug Mode

```bash
python main.py --table-id HST77 --log-level DEBUG
```

### Using Docker Compose

```bash
# Default HST77 fetch
docker-compose up

# Custom table
docker-compose run dst-pipeline python main.py --table-id YOUR_TABLE_ID
```

## Output Structure

The pipeline creates timestamped directories in the bronze layer:

```
bronze/
└── YYYYMMDD_HHMMSS/
    ├── HST77_tableinfo.json          # Table metadata
    ├── HST77_tableinfo_metadata.json # Metadata about the metadata
    ├── HST77_data.json               # Raw table data
    └── HST77_data_metadata.json      # Metadata about the data
```

### Data Files

- **`{table_id}_tableinfo.json`**: Contains table metadata including variable definitions, descriptions, and structure
- **`{table_id}_data.json`**: Contains the actual statistical data in JSON format
- **`{table_id}_*_metadata.json`**: Contains processing metadata including timestamps, source information, and file statistics

## API Rate Limiting

The pipeline implements responsible API usage:

- Default 0.5 second delay between requests
- Exponential backoff retry logic (up to 3 attempts)
- Proper TLS 1.2 configuration as required by DST API
- Request timeout of 30 seconds

## Error Handling

The pipeline includes comprehensive error handling:

- Network timeouts and connection errors
- API rate limiting responses
- Invalid table IDs or parameters
- Malformed API responses
- File system errors

All errors are logged with appropriate detail levels and the pipeline will exit with non-zero status codes on failure.

## Data Size Considerations

Danmarks Statistik API has limitations:
- Maximum 1,000,000 cells for regular formats
- Large datasets may require streaming formats (not implemented in this version)
- Consider using variable and time filters for large tables

## Next Steps (Silver Layer)

After data is collected in the bronze layer, the next steps would be:

1. **Silver Layer Processing**: Clean and harmonize the raw JSON data
2. **Data Transformation**: Convert to Parquet format using ibis/duckdb
3. **Data Validation**: Ensure data quality and consistency
4. **Schema Standardization**: Apply consistent naming conventions

## Troubleshooting

### Common Issues

1. **TLS/SSL Errors**: Ensure your Python environment supports TLS 1.2
2. **Rate Limiting**: Increase `REQUEST_DELAY_SECONDS` if getting rate limited
3. **Memory Issues**: Use variable filters for large tables
4. **Invalid Table ID**: Check the table exists using the API console

### Getting Help

- Check Danmarks Statistik's API documentation
- Use the interactive API console for testing
- Review pipeline logs for detailed error information

## References

- [Danmarks Statistik API Documentation](https://www.dst.dk/da/Statistik/hjaelp-til-statistikbanken/api)
- [API Console](https://www.dst.dk/da/Statistik/hjaelp-til-statistikbanken/api#konsol)
- [Pipeline Architecture Documentation](../README.md)

## Silver Layer Processing

The silver layer transforms the raw bronze data into clean, harmonized datasets following the medallion architecture:

### Key Features
- **Standardized column names**: Danish column names converted to English lowercase_underscore format
- **Data type standardization**: Proper handling of numeric values, dates, and null values
- **Crop categorization**: Automatic grouping of crops into logical categories
- **Missing data handling**: DST's ".." missing value indicators converted to proper nulls
- **Metadata preservation**: Source system, processing timestamps, and table identifiers added

### Column Standardization
- `OMRÅDE` → `region`
- `AFGRØDE` → `crop_type` 
- `MÆNGDE4`/`TAL`/`ENHED` → `measurement_unit`
- `ANVENDELSE` → `usage_type` (HALM1 only)
- `TID` → `year`
- `INDHOLD` → `value`

### Output Format
- **Format**: Parquet files for efficient storage and processing
- **Location**: `silver/{timestamp}/` directories
- **Naming**: `{table_id}_processed.parquet`
- **Metadata**: JSON files with schema and processing information

### Usage

```bash
# Process all tables from bronze to silver layer
python silver_main.py --log-level INFO

# Process specific tables only
python silver_main.py --tables HST77 GARTN1 --log-level INFO

# Force reprocessing
python silver_main.py --force --log-level INFO
```

### Data Quality
- **Record counts preserved**: All bronze records successfully transformed
- **Data types**: Proper numeric, integer, and string types
- **Null handling**: Missing values properly represented
- **Encoding**: UTF-8 with proper Danish character support 