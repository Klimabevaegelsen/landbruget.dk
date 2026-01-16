# Backend Development Guide - Landbruget.dk

## Technology Stack

- **Language**: Python 3.11+
- **Data Processing**: DuckDB (local analytics), Pandas, GeoPandas
- **Database**: Supabase (PostgreSQL + PostGIS)
- **Storage**: Google Cloud Storage (GCS)
- **API Framework**: FastAPI (when needed)
- **Testing**: Pytest
- **Linting/Formatting**: Ruff
- **Type Checking**: MyPy
- **Task Orchestration**: GitHub Actions

## Architecture Overview

The backend is organized around **data pipelines** that follow the **medallion architecture**:

```
Bronze (Raw) → Silver (Cleaned) → Gold (Analysis-Ready)
```

### Pipeline Structure

```
backend/
├── api/                 # FastAPI endpoints (if needed)
├── common/             # Shared utilities across pipelines
│   ├── gcs_utils.py    # Google Cloud Storage helpers
│   ├── supabase_utils.py # Database utilities
│   └── logging.py      # Logging configuration
│
├── pipelines/          # Data ingestion and transformation
│   ├── unified_pipeline/     # 18+ Danish govt sources (PRIMARY)
│   ├── chr_pipeline/         # Livestock tracking (CHR registry)
│   ├── drive_data_pipeline/  # Regulatory compliance data
│   ├── svineflytning_pipeline/ # Pig movement tracking
│   ├── bmd_scraper/          # Pesticide database
│   ├── dma_scraper/          # Environmental oversight
│   ├── arbejdstilsynet_inspections/ # Workplace safety
│   └── [others]/             # See docs/PIPELINE_INDEX.md
│
└── migrations/         # Legacy database migrations (use Supabase now)
```

## Pipeline Development

### Standard Pipeline Structure

Every pipeline should follow this structure:

```
pipeline_name/
├── README.md           # Comprehensive documentation
├── .env.example        # Environment variable template
├── requirements.txt    # Python dependencies
├── main.py            # Entry point
├── bronze/            # Raw data ingestion
│   └── fetcher.py
├── silver/            # Data cleaning and validation
│   └── transformer.py
├── gold/              # Analysis-ready datasets
│   └── aggregator.py
└── tests/             # Unit and integration tests
    ├── test_bronze.py
    ├── test_silver.py
    └── test_gold.py
```

### Pipeline Template

```python
# main.py
import logging
from pathlib import Path
from bronze.fetcher import fetch_raw_data
from silver.transformer import transform_data
from gold.aggregator import aggregate_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_pipeline():
    """
    Main pipeline execution flow.
    """
    try:
        # Bronze: Fetch raw data
        logger.info("Starting Bronze layer: Fetching raw data...")
        raw_data = fetch_raw_data()

        # Silver: Clean and transform
        logger.info("Starting Silver layer: Cleaning data...")
        clean_data = transform_data(raw_data)

        # Gold: Aggregate and analyze
        logger.info("Starting Gold layer: Creating analysis datasets...")
        final_data = aggregate_data(clean_data)

        logger.info("Pipeline completed successfully")
        return final_data

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    run_pipeline()
```

## Data Quality Standards

### Bronze Layer (Raw Data)

**Purpose**: Preserve data exactly as received from sources

**Rules**:
1. **Never modify** raw data - store as-is
2. **Preserve metadata**: source, fetch timestamp, version
3. **Use GCS** for storage with proper naming: `bronze/<source>/<date>/data.parquet`
4. **Immutable**: Never overwrite existing bronze files

```python
# Example: Bronze layer fetcher
import pandas as pd
from datetime import datetime
from common.gcs_utils import upload_to_gcs

def fetch_raw_data(source_url: str) -> pd.DataFrame:
    """
    Fetch raw data from source and save to bronze layer.

    Args:
        source_url: URL or path to data source

    Returns:
        DataFrame with raw data
    """
    # Fetch data
    df = pd.read_csv(source_url)

    # Add metadata
    df['_fetch_timestamp'] = datetime.utcnow()
    df['_source'] = source_url

    # Save to GCS bronze layer
    date_str = datetime.utcnow().strftime('%Y%m%d')
    gcs_path = f'bronze/my_source/{date_str}/raw_data.parquet'
    upload_to_gcs(df, gcs_path)

    return df
```

### Silver Layer (Cleaned Data)

**Purpose**: Clean, validate, and standardize data

**Rules**:
1. **Type coercion**: Ensure correct data types (dates, numbers, geometries)
2. **Validation**: Check for required fields, valid ranges, data quality
3. **Standardization**: Consistent naming, formats, coordinate systems
4. **Deduplication**: Remove duplicates based on business keys
5. **Documentation**: Log all transformations applied

```python
# Example: Silver layer transformer
import pandas as pd
import geopandas as gpd
from datetime import datetime

def transform_data(raw_df: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    Clean and standardize raw data.

    Args:
        raw_df: Raw data from bronze layer

    Returns:
        Cleaned GeoDataFrame
    """
    df = raw_df.copy()

    # 1. Type coercion
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['cvr_number'] = df['cvr_number'].astype(str).str.zfill(8)
    df['area_ha'] = pd.to_numeric(df['area_ha'], errors='coerce')

    # 2. Validation
    df = df[df['area_ha'] > 0]  # Remove invalid areas
    df = df[df['date'].notna()]  # Remove missing dates
    df = df[df['cvr_number'].str.match(r'^\d{8}$')]  # Valid CVR format

    # 3. Standardization
    df.columns = df.columns.str.lower().str.replace(' ', '_')

    # 4. Deduplication
    df = df.drop_duplicates(subset=['cvr_number', 'date'])

    # 5. Convert to GeoDataFrame if spatial
    if 'longitude' in df.columns and 'latitude' in df.columns:
        gdf = gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(df.longitude, df.latitude),
            crs='EPSG:4326'  # WGS84
        )
    else:
        gdf = df

    return gdf
```

### Gold Layer (Analysis-Ready)

**Purpose**: Create enriched, analysis-ready datasets

**Rules**:
1. **Join data sources**: Combine multiple sources on common keys
2. **Calculate metrics**: Derived fields and aggregations
3. **Optimize for queries**: Create appropriate indexes
4. **Document lineage**: Track which bronze/silver data was used
5. **Update Supabase**: Load final data to database

```python
# Example: Gold layer aggregator
import pandas as pd
from common.supabase_utils import upsert_to_supabase

def aggregate_data(clean_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create analysis-ready datasets.

    Args:
        clean_df: Cleaned data from silver layer

    Returns:
        Aggregated data ready for analysis
    """
    # Join with reference data
    reference_df = load_reference_data()
    enriched_df = clean_df.merge(reference_df, on='cvr_number', how='left')

    # Calculate derived metrics
    enriched_df['yield_per_ha'] = (
        enriched_df['total_yield'] / enriched_df['area_ha']
    )

    # Aggregate by company
    company_summary = enriched_df.groupby('cvr_number').agg({
        'area_ha': 'sum',
        'total_yield': 'sum',
        'yield_per_ha': 'mean',
    }).reset_index()

    # Upload to Supabase
    upsert_to_supabase(
        table='company_agricultural_summary',
        data=company_summary,
        conflict_columns=['cvr_number']
    )

    return company_summary
```

## Common Utilities

### GCS Utilities

```python
# common/gcs_utils.py
from google.cloud import storage
import pandas as pd
import io

def upload_to_gcs(df: pd.DataFrame, gcs_path: str, bucket_name: str = 'landbruget-data'):
    """Upload DataFrame to GCS as Parquet."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)

    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)

    blob.upload_from_file(parquet_buffer, content_type='application/octet-stream')
    print(f"Uploaded to gs://{bucket_name}/{gcs_path}")

def download_from_gcs(gcs_path: str, bucket_name: str = 'landbruget-data') -> pd.DataFrame:
    """Download Parquet from GCS as DataFrame."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)

    parquet_buffer = io.BytesIO()
    blob.download_to_file(parquet_buffer)
    parquet_buffer.seek(0)

    return pd.read_parquet(parquet_buffer)
```

### Supabase Utilities

```python
# common/supabase_utils.py
from supabase import create_client, Client
import pandas as pd
import os

def get_supabase_client() -> Client:
    """Get authenticated Supabase client."""
    url = os.getenv('SUPABASE_URL')
    key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
    return create_client(url, key)

def upsert_to_supabase(table: str, data: pd.DataFrame, conflict_columns: list[str]):
    """
    Upsert DataFrame to Supabase table.

    Args:
        table: Table name
        data: DataFrame to upload
        conflict_columns: Columns to use for conflict resolution
    """
    client = get_supabase_client()
    records = data.to_dict('records')

    # Batch upsert (1000 records at a time)
    batch_size = 1000
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        client.table(table).upsert(
            batch,
            on_conflict=','.join(conflict_columns)
        ).execute()

    print(f"Upserted {len(records)} records to {table}")

def query_supabase(table: str, filters: dict = None) -> pd.DataFrame:
    """Query Supabase table and return as DataFrame."""
    client = get_supabase_client()
    query = client.table(table).select('*')

    if filters:
        for key, value in filters.items():
            query = query.eq(key, value)

    result = query.execute()
    return pd.DataFrame(result.data)
```

## Testing Guidelines

### Test Structure

```python
# tests/test_silver.py
import pytest
import pandas as pd
from silver.transformer import transform_data

def test_cvr_number_formatting():
    """Test that CVR numbers are properly formatted."""
    raw_df = pd.DataFrame({
        'cvr_number': ['123', '12345678'],
        'date': ['2024-01-01', '2024-01-02'],
        'area_ha': [10.5, 20.3]
    })

    result = transform_data(raw_df)

    assert all(result['cvr_number'].str.match(r'^\d{8}$'))
    assert result['cvr_number'].iloc[0] == '00000123'

def test_invalid_areas_removed():
    """Test that records with invalid areas are filtered out."""
    raw_df = pd.DataFrame({
        'cvr_number': ['12345678', '87654321'],
        'date': ['2024-01-01', '2024-01-02'],
        'area_ha': [-5, 20.3]  # Negative area should be removed
    })

    result = transform_data(raw_df)

    assert len(result) == 1
    assert result['area_ha'].iloc[0] == 20.3
```

### Running Tests

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_silver.py

# Run with coverage
pytest --cov=. --cov-report=html

# Run with verbose output
pytest -v
```

## Key Data Identifiers

### CVR Number (Company ID)
- **Format**: 8 digits (e.g., `31373077`)
- **Validation**: `^\d{8}$`
- **Storage**: String (to preserve leading zeros)

```python
def validate_cvr(cvr: str) -> bool:
    """Validate CVR number format."""
    return bool(re.match(r'^\d{8}$', str(cvr)))
```

### CHR Number (Herd ID)
- **Format**: 6 digits (e.g., `123456`)
- **Validation**: `^\d{6}$`
- **Storage**: String

### BFE Number (Cadastral ID)
- **Format**: `kommune-ejerlav-matr` (e.g., `0101-123456-12a`)
- **Storage**: String

### Geospatial Data

**Coordinate Systems**:
- **Input**: EPSG:25832 (ETRS89 / UTM zone 32N) - Danish standard
- **Storage**: EPSG:4326 (WGS84) - Standard for Supabase/PostGIS
- **Display**: EPSG:3857 (Web Mercator) - Standard for web maps

```python
import geopandas as gpd

# Convert from Danish coordinate system to WGS84
gdf = gpd.read_file('danish_data.geojson')
gdf = gdf.to_crs('EPSG:4326')  # Convert to WGS84
```

## Performance Optimization

### Use DuckDB for Large Files

```python
import duckdb

# Query large CSV without loading into memory
result = duckdb.query("""
    SELECT cvr_number, SUM(area_ha) as total_area
    FROM 'large_file.csv'
    WHERE date >= '2024-01-01'
    GROUP BY cvr_number
""").df()
```

### Batch Processing

```python
def process_in_batches(df: pd.DataFrame, batch_size: int = 10000):
    """Process large DataFrame in batches."""
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i + batch_size]
        process_batch(batch)
```

### Lazy Loading

```python
# Use chunked reading for large CSVs
for chunk in pd.read_csv('huge_file.csv', chunksize=10000):
    process_chunk(chunk)
```

## Environment Variables

```bash
# Supabase
SUPABASE_URL=https://<project>.supabase.co
SUPABASE_SERVICE_ROLE_KEY=<service_role_key>

# Google Cloud Storage
GCS_BUCKET=landbruget-data
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

# Pipeline-specific
DATA_SOURCE_URL=https://api.example.com/data
API_KEY=<api_key_if_needed>
```

## Common Issues & Solutions

### Issue: "Module not found" errors
**Solution**: Ensure you're in the virtual environment
```bash
cd backend
source venv/bin/activate
pip install -r requirements.txt
```

### Issue: GCS authentication errors
**Solution**: Set up service account credentials
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
```

### Issue: PostGIS geometry errors
**Solution**: Ensure proper CRS transformation
```python
gdf = gdf.to_crs('EPSG:4326')  # Convert to WGS84 before inserting
```

### Issue: Memory errors with large files
**Solution**: Use chunked processing or DuckDB
```python
# Instead of pd.read_csv()
for chunk in pd.read_csv('large.csv', chunksize=10000):
    process(chunk)
```

## Quick Commands

```bash
# Setup
cd backend
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Run pipeline
python pipelines/<pipeline_name>/main.py

# Testing
pytest
pytest --cov=.

# Linting
ruff check .
ruff format .

# Type checking
mypy .
```

## Documentation Requirements

Every pipeline **must** have a comprehensive README.md covering:

1. **Purpose**: What the pipeline does and why
2. **Data Sources**: Where data comes from (with URLs and access info)
3. **Processing Steps**: Bronze → Silver → Gold transformations
4. **Output**: What datasets are created and where they're stored
5. **Quality Checks**: How data quality is validated
6. **Usage**: How to run the pipeline and use the data
7. **Limitations**: Known issues and appropriate use cases

See `docs/PIPELINE_INDEX.md` for examples and templates.

---

*For frontend integration, see `../frontend/CLAUDE.md`*
*For general project guidelines, see `../CLAUDE.md`*
