# /validate-data - Data Quality Validation

Run data quality checks on pipeline outputs and database tables.

## Usage

```
/validate-data                    # Run all validations
/validate-data <table-name>       # Validate specific table
/validate-data --pipeline <name>  # Validate pipeline output
```

## Process

### 1. Identify Data Source

Determine what to validate:
- Supabase table
- GCS parquet file
- Pipeline output in memory

### 2. Run Validation Checks

Execute all applicable checks from the checklist below.

### 3. Report Results

Generate detailed validation report with pass/fail status.

## Validation Checks

### Danish Identifier Formats

**CVR Number (Company ID):**
```sql
-- Must be exactly 8 digits
SELECT
  COUNT(*) as total,
  COUNT(*) FILTER (WHERE cvr_number ~ '^\d{8}$') as valid,
  COUNT(*) FILTER (WHERE cvr_number !~ '^\d{8}$') as invalid
FROM [table];

-- Show invalid examples
SELECT DISTINCT cvr_number
FROM [table]
WHERE cvr_number !~ '^\d{8}$'
LIMIT 10;
```

**CHR Number (Herd ID):**
```sql
-- Must be exactly 6 digits
SELECT
  COUNT(*) as total,
  COUNT(*) FILTER (WHERE chr_number ~ '^\d{6}$') as valid,
  COUNT(*) FILTER (WHERE chr_number !~ '^\d{6}$') as invalid
FROM [table];
```

**BFE Number (Cadastral ID):**
```sql
-- Format varies, check for common patterns
SELECT
  COUNT(*) as total,
  COUNT(*) FILTER (WHERE bfe_number IS NOT NULL AND bfe_number != '') as valid
FROM [table];
```

### Geospatial Validation

**CRS Check:**
```sql
-- Should be EPSG:4326 (WGS84)
SELECT DISTINCT ST_SRID(geom) as srid
FROM [table]
WHERE geom IS NOT NULL;

-- Expected: 4326
```

**Geometry Validity:**
```sql
SELECT
  COUNT(*) as total,
  COUNT(*) FILTER (WHERE ST_IsValid(geom)) as valid,
  COUNT(*) FILTER (WHERE NOT ST_IsValid(geom)) as invalid
FROM [table];
```

**Bounding Box (Denmark):**
```sql
-- Denmark approximate bounds: 7.5-15.5 E, 54.5-58 N
SELECT COUNT(*)
FROM [table]
WHERE geom IS NOT NULL
  AND NOT ST_Within(
    geom,
    ST_MakeEnvelope(7.5, 54.5, 15.5, 58, 4326)
  );
-- Should be 0 or very few
```

**CRS Detection (coordinate range check):**
```sql
-- Check if coordinates are in expected range for CRS
SELECT
    MIN(ST_XMin(geom)) as min_x,
    MAX(ST_XMax(geom)) as max_x,
    MIN(ST_YMin(geom)) as min_y,
    MAX(ST_YMax(geom)) as max_y
FROM [table]
WHERE geom IS NOT NULL;

-- For EPSG:4326 (WGS84): X should be 7.5-15.5, Y should be 54.5-58
-- For EPSG:25832 (UTM): X should be 400k-900k, Y should be 6M-6.5M
-- If values are wrong CRS, data may be corrupted!
```

**Buffer Operation Check (DuckDB):**
```sql
-- CRITICAL: Verify ST_Buffer operations use UTM for meters
-- If you see ST_Buffer(geometry, 1000) on EPSG:4326 data,
-- that's 1000 DEGREES not meters - a critical bug!

-- Use common.crs_utils.sql_buffer_meters() instead
```

### Duplicate Detection

```sql
-- Check for duplicates by primary key
SELECT cvr_number, COUNT(*) as count
FROM [table]
GROUP BY cvr_number
HAVING COUNT(*) > 1
ORDER BY count DESC
LIMIT 10;

-- Check for exact row duplicates
SELECT *, COUNT(*) as count
FROM [table]
GROUP BY ALL
HAVING COUNT(*) > 1;
```

### Null/Empty Values

```sql
SELECT
  COUNT(*) as total_rows,
  COUNT(cvr_number) as cvr_not_null,
  COUNT(name) as name_not_null,
  COUNT(geom) as geom_not_null
FROM [table];

-- Percentage of nulls
SELECT
  column_name,
  ROUND(100.0 * COUNT(*) FILTER (WHERE value IS NULL) / COUNT(*), 2) as null_pct
FROM [table]
CROSS JOIN LATERAL (VALUES
  ('cvr', cvr_number),
  ('name', name),
  ('geom', geom::text)
) AS cols(column_name, value)
GROUP BY column_name;
```

### Data Freshness

```sql
-- Check last update time
SELECT
  MIN(created_at) as oldest,
  MAX(created_at) as newest,
  MAX(updated_at) as last_updated
FROM [table];
```

### Referential Integrity

```sql
-- Check foreign key references
SELECT t1.id, t1.related_id
FROM table1 t1
LEFT JOIN table2 t2 ON t1.related_id = t2.id
WHERE t2.id IS NULL AND t1.related_id IS NOT NULL;
-- Should return 0 rows
```

## Output Format

```
## Data Validation Report: [table/pipeline]

### Summary
| Check | Status | Details |
|-------|--------|---------|
| CVR Format | ✅ Pass | 12,345/12,345 valid (100%) |
| CHR Format | ✅ Pass | 8,234/8,234 valid (100%) |
| Geospatial CRS | ✅ Pass | SRID 4326 |
| Geometry Valid | ⚠️ Warning | 12,340/12,345 valid (99.96%) |
| Duplicates | ✅ Pass | 0 duplicates found |
| Null Values | ⚠️ Warning | cvr: 0%, name: 0.1%, geom: 2% |
| Bounding Box | ✅ Pass | All within Denmark |
| Data Freshness | ✅ Pass | Last updated: 2 hours ago |

### Detailed Findings

#### Invalid CVR Numbers (0)
No invalid CVR numbers found.

#### Invalid Geometries (5)
| ID | Issue |
|----|-------|
| abc-123 | Self-intersection |
| def-456 | Ring not closed |

#### Null Analysis
| Column | Null Count | Percentage |
|--------|------------|------------|
| cvr_number | 0 | 0% |
| name | 12 | 0.1% |
| geom | 245 | 2% |

### Recommendations
1. Fix 5 invalid geometries using ST_MakeValid()
2. Investigate 12 records with missing names
3. Consider geocoding 245 records without geometry

### Overall Status: ⚠️ Passed with Warnings
```

## Python Validation Script

For use in pipelines:

```python
import pandas as pd
import re

def validate_data(df: pd.DataFrame) -> dict:
    """Run all validation checks on DataFrame."""
    results = {}

    # CVR validation
    if 'cvr_number' in df.columns:
        valid_cvr = df['cvr_number'].str.match(r'^\d{8}$', na=False)
        results['cvr'] = {
            'total': len(df),
            'valid': valid_cvr.sum(),
            'invalid': (~valid_cvr).sum(),
            'pass': valid_cvr.all()
        }

    # CHR validation
    if 'chr_number' in df.columns:
        valid_chr = df['chr_number'].str.match(r'^\d{6}$', na=False)
        results['chr'] = {
            'total': len(df),
            'valid': valid_chr.sum(),
            'invalid': (~valid_chr).sum(),
            'pass': valid_chr.all()
        }

    # Duplicate check
    if 'cvr_number' in df.columns:
        dups = df.duplicated(subset=['cvr_number'], keep=False)
        results['duplicates'] = {
            'count': dups.sum(),
            'pass': dups.sum() == 0
        }

    # Null check
    results['nulls'] = {
        col: {
            'count': df[col].isna().sum(),
            'pct': round(100 * df[col].isna().mean(), 2)
        }
        for col in df.columns
    }

    return results
```

## Quick Commands

```bash
# Validate specific table via Supabase
supabase db query "SELECT COUNT(*) FROM [table] WHERE cvr_number !~ '^\d{8}$'"

# Check geospatial
supabase db query "SELECT ST_SRID(geom) FROM [table] LIMIT 1"

# Find duplicates
supabase db query "SELECT cvr_number, COUNT(*) FROM [table] GROUP BY cvr_number HAVING COUNT(*) > 1"
```
