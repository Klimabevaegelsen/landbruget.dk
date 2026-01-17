# CHR Pipeline Test Suite

This directory contains tests for the CHR (Centrale Husdyrbrugsregister) pipeline.

## Files

- `conftest.py` - CHR-specific pytest fixtures
- `test_conftest.py` - Tests validating the CHR fixtures work correctly
- `__init__.py` - Package marker

## Available CHR Fixtures

### SOAP/API Mocking

- **`mock_soap_client`** - Mock zeep SOAP client for CHR API (function scope)
- **`configured_mock_soap_client`** - Pre-configured mock with sample responses (function scope)
- **`mock_soap_response_factory`** - Factory for creating custom mock SOAP responses (function scope)
- **`mock_chr_responses`** - Sample API responses for all CHR endpoints (session scope)

### Test Data

- **`sample_herd_numbers`** - Valid 6-digit CHR herd numbers (function scope)
- **`mock_chr_credentials`** - Mock credentials for testing auth logic (function scope)
- **`chr_date_range`** - Date range for CHR queries (default: last 90 days, function scope)
- **`sample_animal_movements`** - Realistic animal movement records (function scope)
- **`sample_veterinary_visits`** - Realistic VetStat visit records (function scope)

### Reference Data

- **`chr_species_codes`** - Species code mappings (Danish/English) (session scope)
- **`chr_movement_types`** - Movement type code mappings (session scope)

## CHR API Endpoints

The fixtures support mocking these CHR SOAP endpoints:

- `hentDyrListe` - Fetch animal list
- `hentBesaetning` - Fetch herd information
- `hentEjendom` - Fetch property information
- `hentStamdata` - Fetch animal master data
- `hentDiko` - Fetch DIKO movement data
- VetStat XML responses

## Usage Examples

### Testing Bronze Layer Data Fetching

```python
import pytest

def test_fetch_herd_data(configured_mock_soap_client, sample_herd_numbers):
    """Test fetching herd data from CHR API."""
    client = configured_mock_soap_client

    # Fetch data for first herd
    herd_number = sample_herd_numbers[0]
    response = client.service.hentBesaetning(herd_number)

    # Verify response structure
    assert response is not None
    assert "herdNumber" in response
    assert "cvrNumber" in response
```

### Testing with Custom Responses

```python
def test_handle_empty_response(mock_soap_client, mock_chr_responses):
    """Test handling of empty animal list."""
    # Configure mock with empty response
    empty_response = mock_chr_responses["hentDyrListe"]["empty"]
    mock_soap_client.service.hentDyrListe.return_value = empty_response

    # Your code that handles empty responses
    result = fetch_animals(mock_soap_client, "123456")
    assert result == []  # Should handle gracefully
```

### Testing Date Range Queries

```python
def test_date_range_filtering(chr_date_range, sample_animal_movements):
    """Test filtering movements by date range."""
    start_date = chr_date_range["start"]
    end_date = chr_date_range["end"]

    # Filter movements within date range
    filtered = [
        m for m in sample_animal_movements
        if start_date <= m["movement_date"] <= end_date
    ]

    # Your assertions
```

### Testing VetStat XML Parsing

```python
def test_parse_vetstat_xml(mock_chr_responses):
    """Test parsing VetStat XML responses."""
    xml_data = mock_chr_responses["vetstat"]["success"]

    # Your XML parsing logic
    import xml.etree.ElementTree as ET
    root = ET.fromstring(xml_data)

    visits = root.findall(".//Visit")
    assert len(visits) == 2
```

### Creating Custom Mock Responses

```python
def test_custom_response(mock_soap_response_factory):
    """Test with a custom response structure."""
    # Create custom response
    custom_data = {
        "herdNumber": "999999",
        "animalCount": 150,
        "location": "Test Farm",
    }

    response = mock_soap_response_factory(custom_data)

    # Access via attributes
    assert response.herdNumber == "999999"
    assert response.animalCount == 150
```

## Test Markers

Use these pytest markers to categorize CHR tests:

```python
@pytest.mark.chr_bronze
def test_bronze_layer():
    """Test bronze layer functionality."""
    pass

@pytest.mark.chr_silver
def test_silver_layer():
    """Test silver layer functionality."""
    pass

@pytest.mark.chr_gold
def test_gold_layer():
    """Test gold layer functionality."""
    pass

@pytest.mark.chr_integration
def test_integration():
    """Test full pipeline integration."""
    pass

@pytest.mark.chr_api
@pytest.mark.skipif(not os.getenv("CHR_API_KEY"), reason="No API credentials")
def test_real_api():
    """Test requiring actual CHR API access."""
    pass

@pytest.mark.slow
def test_long_running():
    """Test that takes >1 second."""
    pass
```

### Running Tests by Marker

```bash
# Run only bronze layer tests
pytest -m chr_bronze

# Run only fast tests (exclude slow)
pytest -m "not slow"

# Run integration tests
pytest -m chr_integration
```

## Common Test Patterns

### Testing Error Handling

```python
def test_handle_soap_fault(mock_soap_client):
    """Test handling of SOAP faults."""
    # Simulate SOAP fault
    mock_soap_client.service.hentBesaetning.side_effect = Exception("SOAP Fault")

    # Your error handling code
    with pytest.raises(Exception):
        fetch_herd_data(mock_soap_client, "123456")
```

### Testing Data Validation

```python
from common.tests.conftest import validate_chr_format

def test_herd_number_validation(sample_herd_numbers):
    """Test that all herd numbers are valid format."""
    for herd_num in sample_herd_numbers:
        assert validate_chr_format(herd_num)
        assert len(herd_num) == 6
        assert herd_num.isdigit()
```

## Running Tests

```bash
# Run all CHR tests
cd backend/pipelines/chr_pipeline
pytest tests/ -v

# Run specific test file
pytest tests/test_conftest.py -v

# Run with coverage
pytest tests/ --cov=. --cov-report=html

# Run and show print statements
pytest tests/ -v -s
```

## Adding New Tests

When adding new CHR pipeline tests:

1. **Import common fixtures** from `backend/common/tests/conftest.py`:
   ```python
   # These are automatically available
   def test_with_common_fixtures(temp_dir, valid_cvr_numbers):
       pass
   ```

2. **Use appropriate markers** to categorize tests

3. **Follow naming conventions**:
   - Test files: `test_*.py`
   - Test functions: `test_*`
   - Test classes: `Test*`

4. **Add docstrings** explaining what is being tested

5. **Use descriptive assertions**:
   ```python
   # Good
   assert len(results) == expected_count, f"Expected {expected_count} results, got {len(results)}"

   # Less helpful
   assert len(results) == expected_count
   ```

## CHR Data Validation Standards

All CHR tests should validate:

- **CHR Numbers**: Exactly 6 digits
- **CVR Numbers**: Exactly 8 digits (from common fixtures)
- **Dates**: Valid date objects, not strings
- **Species Codes**: Match official CHR codes
- **Movement Types**: Valid CHR movement type codes

See `.claude/rules/data-quality.md` for complete standards.

## Troubleshooting

### Import Errors

If you see `ModuleNotFoundError`:
```bash
# Ensure you're in the correct directory
cd backend/pipelines/chr_pipeline

# Check Python path
python3 -c "import sys; print(sys.path)"
```

### Fixture Not Found

Fixtures from `backend/common/tests/conftest.py` are automatically available:
```python
# This works automatically
def test_something(valid_cvr_numbers):
    pass
```

Fixtures from `chr_pipeline/tests/conftest.py` are only available within CHR tests.

### Mock Not Behaving Correctly

Reset mocks between tests by using function scope:
```python
@pytest.fixture(scope="function")  # Each test gets fresh mock
def my_mock():
    return MagicMock()
```
