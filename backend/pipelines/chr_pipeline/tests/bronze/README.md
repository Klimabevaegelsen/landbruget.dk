# Bronze Layer Tests - CHR Pipeline

## Overview

This directory contains comprehensive tests for the CHR pipeline's bronze layer (raw data ingestion), covering authentication, data export, herd data loading, and VetStat API integration.

## Test Coverage

**Total Tests**: 75
**Passing**: 38 (50.7%)
**Status**: Initial implementation complete

### Test Files

1. **test_auth.py** (22 tests)
   - SOAP client authentication
   - Certificate-based authentication
   - Credential management
   - Session handling
   - Endpoint-specific clients

2. **test_export.py** (20 tests)
   - Data buffering
   - GCS export functionality
   - Context data serialization
   - Batch operations
   - Error recovery
   - Memory management

3. **test_load_besaetning.py** (27 tests)
   - Herd list fetching
   - Herd details retrieval
   - Pagination handling
   - Date filtering
   - Error handling

4. **test_load_vetstat.py** (26 tests)
   - VetStat API authentication
   - XML request/response handling
   - Date range parameters
   - XML security elements
   - SOAP envelope creation

## Running Tests

### All Bronze Tests
```bash
cd backend/pipelines/chr_pipeline
source .venv/bin/activate
python -m pytest tests/bronze/ -v
```

### Specific Test File
```bash
python -m pytest tests/bronze/test_auth.py -v
```

### With Coverage
```bash
python -m pytest tests/bronze/ --cov=bronze --cov-report=html
```

### Run Specific Test Class
```bash
python -m pytest tests/bronze/test_auth.py::TestGetFvmCredentials -v
```

## Test Markers

Tests are marked with pytest markers for selective execution:

```bash
# Run only bronze layer tests
python -m pytest -m chr_bronze

# Run only slow tests
python -m pytest -m slow
```

## Known Issues

### Environment Variables

Some tests require environment variables to pass:

```bash
export LANDBRUGSDATA_UUID_NAMESPACE="$(python -c 'import uuid; print(uuid.uuid4())')"
```

### Mock Patching

Some tests have mock patching issues due to module import paths. These need adjustment:

- Tests that patch `bronze.export.gcs_access` may need to patch at the actual import location
- Some nested patches need proper context manager setup

## Test Data

### Fixtures

All test files use fixtures from `tests/conftest.py`:

- `mock_soap_client` - Mock SOAP client for CHR API
- `mock_chr_responses` - Sample CHR API responses
- `chr_date_range` - Date ranges for testing
- `sample_herd_numbers` - Valid 6-digit CHR numbers
- `mock_chr_credentials` - Fake credentials for auth testing

### Valid CHR Numbers

Tests use realistic 6-digit CHR numbers:
- `123456` - Standard test herd
- `654321` - Secondary test herd
- `111111`, `222222` - Additional test herds

## Test Patterns

### Authentication Tests

```python
@patch("bronze.auth.get_fvm_credentials")
def test_create_client(mock_get_creds):
    mock_get_creds.return_value = ("user", "pass", cert, key)
    client = create_chr_dyr_client()
    assert client is not None
```

### Export Tests

```python
@patch("bronze.export.USE_GCS", True)
@patch("bronze.export.gcs_access")
def test_save_to_gcs(mock_gcs):
    save_data_immediately("test_type", data, "test_id")
    mock_gcs.upload_json.assert_called_once()
```

### API Call Tests

```python
@patch("bronze.load_besaetning.save_raw_data")
def test_fetch_herd_list(mock_save, mock_client, mock_response):
    mock_client.service.listBesaetningerMedBrugsart.return_value = mock_response
    herd_list, has_more, last_herd = load_herd_list(mock_client, "user", 15, 11)
    assert len(herd_list) > 0
```

## Future Improvements

1. **Increase Coverage**: Add tests for edge cases and error paths
2. **Fix Mock Patches**: Resolve remaining mock patching issues
3. **Integration Tests**: Add tests that interact with actual (sandbox) APIs
4. **Performance Tests**: Add tests for memory and performance characteristics
5. **Documentation**: Expand inline documentation for complex test scenarios

## TDD Workflow

When adding new functionality to bronze layer:

1. **Write Test First**: Create failing test that describes expected behavior
2. **Run Test**: Confirm it fails with expected error
3. **Implement**: Write minimum code to pass the test
4. **Verify**: Run test to confirm it passes
5. **Refactor**: Clean up while keeping tests green

## Common Test Utilities

### Creating Mock Certificates

```python
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography import x509

private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
cert = CertificateBuilder()...sign(private_key, hashes.SHA256())
```

### Mock SOAP Responses

```python
response = Mock()
response.Response = Mock()
response.Response.BesaetningsnummerListe = Mock()
response.Response.BesaetningsnummerListe.BesNrListe = ["123456", "654321"]
```

### Mock GCS Access

```python
mock_gcs = MagicMock()
mock_gcs.fs.open.return_value.__enter__.return_value = MagicMock()
```

## Debugging Failed Tests

### View Full Traceback
```bash
python -m pytest tests/bronze/test_auth.py::test_name -vvs
```

### Run with PDB
```bash
python -m pytest tests/bronze/test_auth.py::test_name --pdb
```

### Show Print Statements
```bash
python -m pytest tests/bronze/test_auth.py::test_name -s
```

## Contributing

When adding new tests:

1. Follow existing naming conventions (`test_<function>_<scenario>`)
2. Use descriptive docstrings
3. Group related tests in classes
4. Use appropriate fixtures from `conftest.py`
5. Mark tests with appropriate pytest markers
6. Ensure tests are independent and can run in any order

## Contact

For questions about these tests, refer to:
- `tests/conftest.py` - Shared fixtures
- `docs/PIPELINE_INDEX.md` - Pipeline documentation
- `.claude/rules/testing.md` - Testing guidelines
