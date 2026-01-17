---
name: data-pipeline-operations
description: |
  Activates when working with Python data pipelines, GCS operations, or medallion architecture (Bronze/Silver/Gold).
  Use this skill for: running pipelines, debugging data transformations, GCS uploads/downloads,
  data quality validation, CVR/CHR/BFE identifier handling, GeoPandas/PostGIS operations, and DuckDB queries for large files.
  Keywords: pipeline, bronze, silver, gold, GCS, parquet, CVR, CHR, BFE, transform, ingest, ETL, DuckDB, large files
---

# Data Pipeline Operations Skill

This skill provides guidance for working with Landbruget.dk's data pipelines following the medallion architecture.

## Activation Context

This skill activates when:
- Running or debugging data pipelines
- Working with GCS (Google Cloud Storage)
- Handling data transformations (Bronze → Silver → Gold)
- Validating Danish identifiers (CVR, CHR, BFE)
- Working with geospatial data (GeoPandas, PostGIS)

## Environment Setup

**ALWAYS start with:**
```bash
cd backend
source venv/bin/activate
```

**Verify environment:**
```bash
python -c "import geopandas, supabase; print('Environment OK')"
```

## Data Processing Philosophy

**PREFER DuckDB over Pandas:**
- DuckDB queries files directly without memory limits
- Much faster for large datasets
- Use SQL instead of DataFrame operations
- Only use Pandas for final small result sets or when GeoPandas is required

## CRS Strategy

**Process in EPSG:25832, transform to EPSG:4326 only at final Supabase upload.**

| EPSG | Name | Use |
|------|------|-----|
| 25832 | UTM 32N | **Processing** (Bronze/Silver/Gold) |
| 4326 | WGS84 | **Final storage** (Supabase only) |

This eliminates unnecessary transforms:
- ❌ Old: Source(25832) → Silver(4326) → Gold(25832 for calc) → Supabase(4326) = 2-3 transforms
- ✅ New: Source(25832) → Process(25832) → Supabase(4326) = 1 transform

## Medallion Architecture

### Bronze Layer (Raw Data)
- **Purpose**: Preserve data exactly as received
- **Location**: `gs://landbruget-data/bronze/<source>/<date>/`
- **CRS**: Keep native (usually EPSG:25832 from Danish WFS sources)
- **Rules**:
  - Never modify raw data or geometry
  - Add metadata: `_fetch_timestamp`, `_source`, `_source_crs`
  - Use Parquet format
  - Immutable - never overwrite

```python
# Track source CRS in metadata
_source_crs = detect_crs_from_response(wfs_capabilities)  # e.g., "EPSG:25832"
```

### Silver Layer (Cleaned Data)
- **Purpose**: Clean, validate, standardize
- **CRS**: Keep EPSG:25832 (no transformation yet!)
- **Transformations**:
  - Type coercion (dates, numbers)
  - CVR formatting: 8 digits, zero-padded
  - CHR formatting: 6 digits
  - Transform non-25832 sources (DAGI, H3) to EPSG:25832 here
  - Deduplication
  - Null handling

```python
# Only transform sources that aren't already EPSG:25832
if source_crs != "EPSG:25832":
    ST_Transform(geometry, source_crs, 'EPSG:25832')
```

### Gold Layer (Analysis-Ready)
- **Purpose**: Enriched, joined datasets
- **CRS**: Keep EPSG:25832 for processing (area/buffer/distance work natively!)
- **Operations**:
  - Join multiple sources on CVR/CHR/BFE
  - Calculate derived metrics (meters work directly!)
  - Aggregate by company/farm
  - Transform to EPSG:4326 **only** at final Supabase upload

```python
# Area/buffer/distance work directly in EPSG:25832 - no transforms needed!
ST_Area(geometry) / 10000  # hectares (geometry already in meters)
ST_Buffer(geometry, 1000)  # 1km buffer (meters work directly)

# Transform ONCE at final Supabase upload
ST_Transform(geometry, 'EPSG:25832', 'EPSG:4326')
```

## Data Quality Validation

### CVR Number (Company ID)
```python
import re

def validate_cvr(cvr: str) -> bool:
    """CVR must be 8 digits."""
    return bool(re.match(r'^\d{8}$', str(cvr).zfill(8)))

# Format CVR
df['cvr'] = df['cvr'].astype(str).str.zfill(8)
```

### CHR Number (Herd ID)
```python
def validate_chr(chr_num: str) -> bool:
    """CHR must be 6 digits."""
    return bool(re.match(r'^\d{6}$', str(chr_num)))
```

### Geospatial CRS
```python
import geopandas as gpd

# Danish data comes in EPSG:25832 (UTM zone 32N) - keep it there!
# Only convert to EPSG:4326 at final Supabase upload
gdf_for_supabase = gdf.to_crs('EPSG:4326')
```

### Buffer/Distance in DuckDB

**With EPSG:25832, buffer/distance work natively in meters!**

```sql
-- EPSG:25832 data - buffer works directly in meters ✓
ST_Buffer(geometry, 1000)  -- 1km buffer

-- Area calculation works directly in square meters
ST_Area(geometry) / 10000  -- hectares
```

**If working with EPSG:4326 data (avoid when possible):**
```python
from common.crs_utils import sql_buffer_meters, sql_intersects_with_buffer_meters

# These helpers transform to UTM internally
buffer_sql = sql_buffer_meters("geometry", 100)  # 100 meters
intersect_sql = sql_intersects_with_buffer_meters("a.geom", "b.geom", 1000)  # 1km
```

## GCS Operations

**Bucket**: Set via `GCS_BUCKET` environment variable (see `.env`)

### Upload to GCS with DuckDB
```python
import os
from google.cloud import storage
import duckdb
import io

def upload_to_gcs_duckdb(query: str, gcs_path: str, bucket_name: str = None):
    """Query with DuckDB and upload directly to GCS."""
    bucket_name = bucket_name or os.environ.get('GCS_BUCKET')

    # Execute query and get result
    result = duckdb.query(query)

    # Export to parquet buffer
    buffer = io.BytesIO()
    result.write_parquet(buffer)
    buffer.seek(0)

    # Upload to GCS
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    blob.upload_from_file(buffer, content_type='application/octet-stream')

# Example usage
upload_to_gcs_duckdb(
    "SELECT * FROM 'input.csv' WHERE cvr_number ~ '^\\d{8}$'",
    "silver/cleaned_data.parquet"
)
```

### Query Files Directly from GCS with DuckDB
```python
import os
import duckdb

# Install and load httpfs extension
duckdb.execute("INSTALL httpfs")
duckdb.execute("LOAD httpfs")

bucket = os.environ.get('GCS_BUCKET')

# Query parquet directly from GCS
result = duckdb.query(f"""
    SELECT cvr_number, SUM(area_ha) as total_area
    FROM 'gs://{bucket}/silver/fields.parquet'
    GROUP BY cvr_number
""").df()

# For authenticated access, set credentials first
duckdb.execute(f"SET gcs_access_key_id='{key_id}'")
duckdb.execute(f"SET gcs_secret_access_key='{secret}'")
```

## Running Pipelines

### Standard Pipeline Execution
```bash
cd backend
source venv/bin/activate
cd pipelines/<pipeline_name>
python main.py
```

### Common Pipelines
| Pipeline | Purpose | Frequency |
|----------|---------|-----------|
| `unified_pipeline` | 18+ Danish govt sources | Weekly |
| `chr_pipeline` | Livestock tracking | Weekly |
| `svineflytning_pipeline` | Pig movements | Weekly |
| `drive_data_pipeline` | Regulatory compliance | On-demand |

## DuckDB for Large Files

DuckDB is excellent for querying large files without loading into memory:

```python
import duckdb

# Query CSV directly
result = duckdb.query("""
    SELECT cvr_number, SUM(area_ha) as total_area
    FROM 'large_file.csv'
    WHERE date >= '2024-01-01'
    GROUP BY cvr_number
""").df()

# Query Parquet files
result = duckdb.query("""
    SELECT *
    FROM 'data.parquet'
    WHERE cvr_number = '12345678'
""").df()

# Join multiple files
result = duckdb.query("""
    SELECT a.*, b.name
    FROM 'fields.parquet' a
    JOIN 'companies.csv' b ON a.cvr_number = b.cvr_number
    WHERE a.area_ha > 100
""").df()

# Aggregate on large datasets
result = duckdb.query("""
    SELECT
        cvr_number,
        COUNT(*) as field_count,
        SUM(area_ha) as total_area,
        AVG(area_ha) as avg_area
    FROM 'fields.parquet'
    GROUP BY cvr_number
    HAVING total_area > 1000
""").df()
```

### DuckDB Advantages
- **No memory limits**: Queries files directly without loading
- **SQL interface**: Use familiar SQL syntax
- **Fast**: Highly optimized columnar engine
- **Multiple formats**: CSV, Parquet, JSON
- **Joins**: Combine multiple files efficiently

## Troubleshooting

### "Module not found"
```bash
cd backend
source venv/bin/activate
pip install -e .
```

### GCS Authentication
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
```

### Memory Issues

**ALWAYS use DuckDB for large files - avoid Pandas:**

```python
# ✅ CORRECT: Use DuckDB
import duckdb
result = duckdb.query("""
    SELECT cvr_number, area_ha
    FROM 'large.csv'
    WHERE condition
""").df()

# ❌ AVOID: Pandas chunking (slow, complex)
# for chunk in pd.read_csv('large.csv', chunksize=10000):
#     process(chunk)

# ❌ AVOID: Pandas column selection (still loads into memory)
# df = pd.read_csv('large.csv', usecols=['cvr_number', 'area_ha'])
```

### When to Use Pandas vs DuckDB

**Use DuckDB (preferred):**
- Reading CSV/Parquet files
- Filtering, aggregating, joining data
- Any operation on data > 1GB
- Transformations that can be expressed in SQL

**Use Pandas only when:**
- Working with GeoPandas (spatial operations)
- Final result set is small (<100MB)
- Need very specific Python operations unavailable in SQL

**Use GeoPandas only for:**
- Geometry operations (ST_Transform, ST_Within, etc.)
- Spatial joins
- CRS transformations

## Quality Checklist

Before marking pipeline work complete:
- [ ] Bronze data preserved unchanged (native CRS, usually EPSG:25832)
- [ ] Silver data cleaned and validated (EPSG:25832)
- [ ] Gold data uploaded to Supabase (transformed to EPSG:4326 at upload)
- [ ] CVR/CHR/BFE formats validated
- [ ] No duplicate records
- [ ] Tests pass: `pytest tests/`
