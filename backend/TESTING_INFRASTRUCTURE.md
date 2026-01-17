# Backend Test Infrastructure - Phase 1

This document provides an overview of the backend test infrastructure created for Phase 1 of the code quality standards implementation.

## Summary

**Total Lines of Code**: 1,645 lines
- Backend common tests: 651 lines (conftest + tests + docs)
- CHR pipeline tests: 994 lines (conftest + tests + docs)

**All Tests Passing**: ✅ 24/24 tests pass

## Directory Structure

```
backend/
├── common/
│   └── tests/
│       ├── __init__.py
│       ├── conftest.py          # Shared fixtures (334 lines)
│       ├── test_conftest.py     # Fixture validation tests (172 lines)
│       └── README.md            # Usage documentation (144 lines)
│
└── pipelines/
    └── chr_pipeline/
        └── tests/
            ├── __init__.py
            ├── conftest.py      # CHR-specific fixtures (469 lines)
            ├── test_conftest.py # CHR fixture tests (227 lines)
            └── README.md        # CHR testing guide (297 lines)
```

## Created Files

### Backend Common Tests (`/backend/common/tests/`)

1. **`conftest.py`** - Shared pytest fixtures for all backend pipelines
2. **`test_conftest.py`** - Tests validating the shared fixtures
3. **`__init__.py`** - Package marker
4. **`README.md`** - Comprehensive usage documentation

### CHR Pipeline Tests (`/backend/pipelines/chr_pipeline/tests/`)

1. **`conftest.py`** - CHR-specific pytest fixtures
2. **`test_conftest.py`** - Tests validating CHR fixtures
3. **`__init__.py`** - Package marker
4. **`README.md`** - CHR testing guide with examples

## Shared Fixtures (Backend Common)

### File System & Storage
- `temp_dir` - Temporary directory for test data
- `mock_gcs_filesystem` - Mock GCS for isolated testing

### Database
- `mock_duckdb_connection` - In-memory DuckDB with spatial extension
- `mock_supabase_client` - Mock Supabase client

### Danish Data Validators
- `sample_danish_geometries` - Valid WKT geometries (EPSG:25832 and EPSG:4326)
- `valid_cvr_numbers` - Sample 8-digit CVR numbers
- `valid_chr_numbers` - Sample 6-digit CHR numbers

### Date/Time
- `sample_date_range` - Date range for testing

### Validation Functions
- `cvr_validator` - Validates CVR format
- `chr_validator` - Validates CHR format
- `cvr_formatter` - Formats CVR with leading zeros
- `chr_formatter` - Formats CHR with leading zeros

## CHR Pipeline Fixtures

### SOAP/API Mocking
- `mock_soap_client` - Mock zeep SOAP client
- `configured_mock_soap_client` - Pre-configured with sample responses
- `mock_soap_response_factory` - Factory for custom responses
- `mock_chr_responses` - Sample API responses (session scope)

### Test Data
- `sample_herd_numbers` - Valid CHR herd numbers
- `mock_chr_credentials` - Mock credentials for auth testing
- `chr_date_range` - Date range for CHR queries (90 days default)
- `sample_animal_movements` - Realistic movement records
- `sample_veterinary_visits` - Realistic VetStat records

### Reference Data
- `chr_species_codes` - Species code mappings (Danish/English)
- `chr_movement_types` - Movement type mappings

## Test Results

All 24 fixture validation tests pass:

```bash
# Backend common tests: 12 tests
✅ test_temp_dir_fixture
✅ test_mock_gcs_filesystem
✅ test_mock_duckdb_connection
✅ test_sample_danish_geometries
✅ test_valid_cvr_numbers
✅ test_valid_chr_numbers
✅ test_cvr_validator
✅ test_chr_validator
✅ test_cvr_formatter
✅ test_chr_formatter
✅ test_sample_date_range
✅ test_mock_supabase_client

# CHR pipeline tests: 12 tests
✅ test_mock_soap_client
✅ test_mock_chr_responses
✅ test_chr_date_range
✅ test_sample_herd_numbers
✅ test_mock_chr_credentials
✅ test_mock_soap_response_factory
✅ test_configured_mock_soap_client
✅ test_sample_animal_movements
✅ test_sample_veterinary_visits
✅ test_chr_species_codes
✅ test_chr_movement_types
✅ test_vetstat_xml_response
```

## Running Tests

```bash
# Run all fixture tests
cd backend
python3 -m pytest common/tests/test_conftest.py pipelines/chr_pipeline/tests/test_conftest.py -v

# Run only common tests
python3 -m pytest common/tests/test_conftest.py -v

# Run only CHR tests
python3 -m pytest pipelines/chr_pipeline/tests/test_conftest.py -v

# Quick validation
python3 -m pytest common/tests/ pipelines/chr_pipeline/tests/ -q
```

## Key Features

### 1. Proper Fixture Scoping
- **Function scope** - Each test gets clean state (default)
- **Session scope** - Shared immutable reference data
- Clear docstrings explaining scope choices

### 2. Type Hints
All fixtures have complete type hints for better IDE support and documentation.

### 3. Comprehensive Documentation
- Detailed docstrings in conftest.py
- Usage examples in README files
- Common patterns and troubleshooting guides

### 4. Data Quality Standards Compliance
All fixtures follow `.claude/rules/data-quality.md`:
- CVR: 8 digits, string with leading zeros
- CHR: 6 digits, string with leading zeros
- Geospatial: EPSG:4326 for storage, EPSG:25832 for Danish input
- Dates: Python date/datetime objects, not strings

### 5. Mock Best Practices
- Isolated testing without external dependencies
- Configurable mock responses
- Proper cleanup/teardown
- Tracked state for assertions

### 6. Pytest Markers
CHR tests support categorization:
- `@pytest.mark.chr_bronze` - Bronze layer tests
- `@pytest.mark.chr_silver` - Silver layer tests
- `@pytest.mark.chr_gold` - Gold layer tests
- `@pytest.mark.chr_integration` - Integration tests
- `@pytest.mark.chr_api` - Requires API credentials
- `@pytest.mark.slow` - Long-running tests (>1 second)

## Usage Examples

### Using Common Fixtures

```python
def test_cvr_processing(valid_cvr_numbers, cvr_validator):
    """Test CVR number validation."""
    for cvr in valid_cvr_numbers:
        assert cvr_validator(cvr)
```

### Using CHR Fixtures

```python
def test_fetch_herd_data(configured_mock_soap_client, sample_herd_numbers):
    """Test CHR API data fetching."""
    client = configured_mock_soap_client
    herd_number = sample_herd_numbers[0]
    response = client.service.hentBesaetning(herd_number)
    assert response["herdNumber"] == herd_number
```

### Using GCS Mock

```python
def test_gcs_upload(mock_gcs_filesystem, temp_dir):
    """Test GCS upload without actual cloud storage."""
    mock_gcs_filesystem.put("local.txt", "gs://bucket/remote.txt")
    assert "gs://bucket/remote.txt" in mock_gcs_filesystem.uploaded_files
```

## Next Steps

This Phase 1 infrastructure provides the foundation for:

1. **Phase 2**: Writing actual pipeline tests using these fixtures
2. **Phase 3**: Adding more pipeline-specific fixtures as needed
3. **Phase 4**: Integration tests across pipelines
4. **Phase 5**: CI/CD integration with GitHub Actions

## Testing Philosophy

Following `.claude/rules/testing.md`:

1. **Test-Driven Development (TDD)** - Write tests first
2. **Isolation** - Tests don't depend on external services
3. **Fast Feedback** - Tests run quickly (<1s each)
4. **Clear Failures** - Descriptive assertions and error messages
5. **Maintainability** - DRY principle via shared fixtures

## File Locations

**Absolute Paths** (for reference):

```
/Users/martincollignon/conductor/landbruget.dk/.conductor/milan-v2/backend/common/tests/conftest.py
/Users/martincollignon/conductor/landbruget.dk/.conductor/milan-v2/backend/common/tests/test_conftest.py
/Users/martincollignon/conductor/landbruget.dk/.conductor/milan-v2/backend/common/tests/README.md

/Users/martincollignon/conductor/landbruget.dk/.conductor/milan-v2/backend/pipelines/chr_pipeline/tests/conftest.py
/Users/martincollignon/conductor/landbruget.dk/.conductor/milan-v2/backend/pipelines/chr_pipeline/tests/test_conftest.py
/Users/martincollignon/conductor/landbruget.dk/.conductor/milan-v2/backend/pipelines/chr_pipeline/tests/README.md
```

## Resources

- **Testing Rules**: `.claude/rules/testing.md`
- **Data Quality Rules**: `.claude/rules/data-quality.md`
- **Pytest Documentation**: https://docs.pytest.org/
- **Backend Guide**: `backend/CLAUDE.md`

---

**Status**: ✅ Phase 1 Complete - All tests passing, documentation comprehensive
**Created**: 2026-01-17
**Test Coverage**: 24 fixture validation tests, 100% passing
