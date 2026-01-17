# Backend Common Test Fixtures

This directory contains shared pytest fixtures used across all backend pipeline tests.

## Files

- `conftest.py` - Shared fixtures for all backend tests
- `test_conftest.py` - Tests validating the fixtures work correctly
- `__init__.py` - Package marker

## Available Fixtures

### File System & Storage

- **`temp_dir`** - Temporary directory for test data (function scope)
- **`mock_gcs_filesystem`** - Mock GCS filesystem for isolated testing (function scope)

### Database

- **`mock_duckdb_connection`** - In-memory DuckDB with spatial extension (function scope)
- **`mock_supabase_client`** - Mock Supabase client (function scope)

### Danish Data Validators

- **`sample_danish_geometries`** - Valid WKT geometries in EPSG:25832 and EPSG:4326 (session scope)
- **`valid_cvr_numbers`** - Sample valid 8-digit CVR numbers (session scope)
- **`valid_chr_numbers`** - Sample valid 6-digit CHR numbers (session scope)

### Date/Time

- **`sample_date_range`** - Sample date range for testing (function scope)

### Validation Functions

The following utility functions are available as fixtures:

- **`cvr_validator`** - Validates CVR number format (8 digits)
- **`chr_validator`** - Validates CHR number format (6 digits)
- **`cvr_formatter`** - Formats CVR with leading zeros
- **`chr_formatter`** - Formats CHR with leading zeros

You can also import these directly from `conftest.py`:
```python
from common.tests.conftest import validate_cvr_format, format_cvr
```

## Usage Examples

### Basic Fixture Usage

```python
# test_my_pipeline.py
import pytest

def test_cvr_processing(valid_cvr_numbers, cvr_validator):
    """Test CVR number processing."""
    for cvr in valid_cvr_numbers:
        assert cvr_validator(cvr)
        # Your test logic here
```

### Using Mock GCS

```python
def test_gcs_upload(mock_gcs_filesystem, temp_dir):
    """Test GCS upload logic without actual cloud storage."""
    # Your code that uses GCS
    mock_gcs_filesystem.put("local_file.txt", "gs://bucket/remote_file.txt")

    # Verify upload was called
    assert "gs://bucket/remote_file.txt" in mock_gcs_filesystem.uploaded_files
```

### Using DuckDB

```python
def test_duckdb_query(mock_duckdb_connection):
    """Test data transformation with DuckDB."""
    conn = mock_duckdb_connection

    # Create test data
    conn.execute("CREATE TABLE test_data (cvr VARCHAR, value INTEGER)")
    conn.execute("INSERT INTO test_data VALUES ('12345678', 100)")

    # Your query logic
    result = conn.execute("SELECT * FROM test_data WHERE value > 50").fetchall()
    assert len(result) == 1
```

### Using Danish Geometries

```python
def test_geometry_transformation(sample_danish_geometries, mock_duckdb_connection):
    """Test coordinate system transformation."""
    cph = sample_danish_geometries["copenhagen_point"]

    # Test with both coordinate systems
    wgs84_point = cph["epsg_4326"]  # "POINT(12.5683 55.6761)"
    utm32_point = cph["epsg_25832"]  # "POINT(723626.13 6176067.15)"

    # Your transformation logic
```

## Data Quality Standards

All fixtures follow the data quality rules from `.claude/rules/data-quality.md`:

- **CVR Numbers**: Exactly 8 digits, stored as string to preserve leading zeros
- **CHR Numbers**: Exactly 6 digits, stored as string
- **Geospatial Data**: Stored in EPSG:4326 (WGS84), input may be EPSG:25832 (Danish UTM)
- **Dates**: Use Python `date` or `datetime` objects, not strings

## Running Tests

```bash
# Test the fixtures themselves
cd backend
python3 -m pytest common/tests/test_conftest.py -v

# Run all backend tests (will use these fixtures)
python3 -m pytest -v
```

## Adding New Fixtures

When adding new shared fixtures:

1. Add the fixture to `conftest.py` with:
   - Clear docstring explaining purpose
   - Proper scope (function, module, session)
   - Type hints
   - Cleanup if needed (use `yield` for teardown)

2. Add tests to `test_conftest.py` validating the fixture works

3. Update this README with usage examples

## Scope Guidelines

- **function** - Each test gets its own instance (default, safest)
- **module** - Shared within a test file (use for expensive setup)
- **session** - Shared across entire test run (use for immutable reference data)

Choose the narrowest scope possible to avoid test interference.
