# Google Drive Data Pipeline Test Suite

This directory contains the test suite for the Google Drive Data Pipeline. The tests are organized to match the pipeline's architecture, with dedicated tests for each component and integration tests for the full system.

## Test Organization

```
tests/
├── conftest.py               # Pytest fixtures and configuration
├── bronze/                   # Bronze layer tests
│   ├── test_processor.py     # Tests for Bronze processor
│   ├── test_metadata.py      # Tests for metadata management
│   └── drive/                # Google Drive integration tests
│       ├── test_auth.py      # Authentication tests
│       ├── test_fetcher.py   # File fetching tests
│       └── test_models.py    # Data model tests
├── silver/                   # Silver layer tests
│   ├── test_processor.py     # Tests for Silver processor
│   ├── transformers/         # Transformer tests
│   │   ├── test_excel_transformer.py
│   │   └── test_pdf_transformer.py
│   └── validators/           # Validator tests
│       ├── test_data_types.py
│       └── test_pii_validator.py
├── integration/              # Integration tests
│   ├── test_end_to_end.py    # Full pipeline tests
│   ├── test_bronze_process.py  # Bronze-specific integration
│   └── test_silver_process.py  # Silver-specific integration
└── utils/                    # Utility function tests
    ├── test_logging.py
    └── test_storage.py
```

## Running Tests

### Prerequisites

Before running tests, ensure you have:

1. Installed all development dependencies:
   ```bash
   pip install -e ".[dev]"
   ```
   
2. Set up the necessary environment variables (or use the test fixtures):
   ```bash
   export GOOGLE_DRIVE_FOLDER_ID="test_folder_id"
   export GOOGLE_APPLICATION_CREDENTIALS="path/to/test/credentials.json"
   ```

### Running All Tests

To run the entire test suite:

```bash
pytest
```

### Running Specific Test Categories

```bash
# Run only unit tests
pytest -k "not integration"

# Run only Bronze layer tests
pytest tests/bronze/

# Run only Silver layer tests
pytest tests/silver/

# Run only integration tests
pytest tests/integration/

# Run a specific test file
pytest tests/bronze/test_processor.py
```

### Test Tags

Tests are tagged with markers to allow selective execution:

```bash
# Run all integration tests
pytest -m integration

# Run all unit tests
pytest -m "not integration"

# Run Bronze layer tests
pytest -m bronze

# Run Silver layer tests
pytest -m silver
```

## Mock Services

The test suite uses mock objects for external services:

1. **MockDriveService**: Simulates Google Drive API responses
2. **MockStorageManager**: In-memory storage for testing
3. **MockTransformer**: Dummy transformation for testing

These mocks are defined in `conftest.py` and automatically injected into tests that need them.

## Test Data

Sample test data is stored in the `tests/fixtures/` directory:

- `sample_excel.xlsx`: Sample Excel file for transformer tests
- `sample_pdf.pdf`: Sample PDF file for transformer tests
- `drive_response.json`: Sample Google Drive API responses

## Integration Tests

Integration tests verify the interaction between components and the end-to-end functionality of the pipeline. These tests:

1. Mock external services like Google Drive API
2. Create temporary directories for Bronze and Silver data
3. Process sample files through the pipeline
4. Verify correct output in both layers

## Writing New Tests

When adding new functionality:

1. Create a unit test for each new component
2. Update integration tests if the component changes data flow
3. Follow the existing naming and organization patterns
4. Use the mock services and fixtures where appropriate

## Test Coverage

To generate a test coverage report:

```bash
pytest --cov=drive_data_pipeline
```

For a detailed HTML report:

```bash
pytest --cov=drive_data_pipeline --cov-report=html
```

This will create a `htmlcov/` directory with the coverage report.

## Continuous Integration

The test suite runs automatically as part of CI/CD workflows. The pipeline:

1. Runs all tests on pull requests
2. Generates coverage reports
3. Fails if coverage drops below the threshold
4. Verifies code quality with linters 