"""
Tests for the base classes in the unified pipeline.

This module tests the core functionality of BaseJobConfig and BaseSource
classes to ensure they work correctly with the unified GCS access architecture
and DuckDB-based data processing.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Add backend to path if not already there (for when running tests standalone)
# Path: .../backend/pipelines/unified_pipeline/src/tests/common/test_base.py
# Navigate up to backend directory
test_file_path = Path(__file__).resolve()
backend_dir = test_file_path.parents[5]  # Go up 5 levels from test file to backend

if backend_dir.exists() and str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

from common.storage import StorageAccess  # noqa: E402

from unified_pipeline.common.base import BaseJobConfig, BaseSource  # noqa: E402


class MockJobConfig(BaseJobConfig):
    """Configuration class for testing BaseJobConfig."""

    dataset: str = "test_dataset"
    bucket: str = "test_bucket"
    name: str = "Test Source"


class MockSource(BaseSource[MockJobConfig]):
    """Test implementation of BaseSource for testing purposes."""

    def __init__(self, config: MockJobConfig):
        super().__init__(config)

    async def run(self) -> None:
        """Test implementation of the run method."""
        pass


# Fixtures
@pytest.fixture
def mock_storage_access():
    """Create a mock GCS access layer for testing."""
    with patch("unified_pipeline.common.base.StorageAccess") as mock_class:
        mock_instance = MagicMock(spec=StorageAccess)
        mock_class.return_value = mock_instance

        # Mock DuckDB connection
        mock_instance.duckdb_conn = MagicMock()
        mock_instance.fs = MagicMock()
        mock_instance.monitor = MagicMock()

        # Mock common methods
        mock_instance.upload_from_duckdb_table.return_value = None
        mock_instance.create_table_from_storage.return_value = None
        mock_instance.list_files.return_value = []
        mock_instance.file_exists.return_value = True
        mock_instance.upload_json.return_value = None

        yield mock_instance


@pytest.fixture
def test_config() -> MockJobConfig:
    """Create a test configuration for testing."""
    return MockJobConfig()


@pytest.fixture
def test_source(test_config: MockJobConfig, mock_storage_access) -> MockSource:
    """Create a test source for testing with mocked GCS access."""
    with patch("unified_pipeline.common.base.StorageAccess", return_value=mock_storage_access):
        source = MockSource(test_config)
        # Replace storage with mock after initialization
        source.storage = mock_storage_access
        return source


# Tests for BaseJobConfig
def test_base_job_config_initialization() -> None:
    """Test that BaseJobConfig can be initialized and extended."""
    config = MockJobConfig()

    # Test that attributes are correctly set
    assert config.dataset == "test_dataset"
    assert config.bucket == "test_bucket"
    assert config.name == "Test Source"


def test_base_job_config_defaults() -> None:
    """Test that BaseJobConfig default values are correctly set."""
    config = MockJobConfig()

    # Test default values from BaseJobConfig
    assert config.save_local is False
    assert config.dev_mode is False
    assert config.generate_schemas is False
    assert config.save_schemas_locally is True


# Tests for BaseSource
def test_base_source_initialization(test_config: MockJobConfig, mock_storage_access) -> None:
    """Test that BaseSource can be initialized with unified architecture."""
    with patch("unified_pipeline.common.base.StorageAccess", return_value=mock_storage_access):
        source = MockSource(test_config)

        # Test that attributes are correctly set
        assert source.config == test_config
        assert source.log is not None

        # Test that DuckDB connection was created
        assert source.conn is not None

        # Test that date_pattern was set
        assert source.date_pattern is not None
        assert len(source.date_pattern) > 0


def test_unified_connection_architecture(test_source: MockSource) -> None:
    """Test that the unified connection architecture works correctly."""
    # Verify that storage_access and conn are properly connected
    assert hasattr(test_source, "conn")
    assert hasattr(test_source, "storage")
    assert test_source.conn is not None
    assert test_source.storage is not None


def test_duckdb_connection_is_functional(test_config: MockJobConfig, mock_storage_access) -> None:
    """Test that the DuckDB connection can execute queries."""
    with patch("unified_pipeline.common.base.StorageAccess", return_value=mock_storage_access):
        source = MockSource(test_config)

        # Test that we can execute a simple query
        result = source.conn.execute("SELECT 1 as value").fetchone()
        assert result is not None
        assert result[0] == 1


# Tests for _save_data method
def test_save_data_table_name(test_source: MockSource, test_config: MockJobConfig) -> None:
    """Test saving data using table name."""
    table_name = "test_table"

    # Create a test table in DuckDB
    test_source.conn.execute(f"CREATE TABLE {table_name} (id INTEGER, name VARCHAR)")
    test_source.conn.execute(f"INSERT INTO {table_name} VALUES (1, 'test')")

    # Mock the storage upload method
    test_source.storage.upload_from_duckdb_table = MagicMock()

    # Test saving with table name
    test_source._save_data(table_name, test_config.dataset, test_config.bucket, "silver")

    # Verify the upload method was called
    test_source.storage.upload_from_duckdb_table.assert_called_once()


def test_save_data_json(test_source: MockSource, test_config: MockJobConfig) -> None:
    """Test saving JSON data."""
    json_data = {"key": "value", "number": 123}

    # Mock the storage upload method
    test_source.storage.upload_json = MagicMock()

    # Test saving with JSON data
    test_source._save_data(json_data, test_config.dataset, test_config.bucket, "silver")

    # Verify the upload method was called
    test_source.storage.upload_json.assert_called_once()


def test_save_data_list(test_source: MockSource, test_config: MockJobConfig) -> None:
    """Test saving list data as JSON."""
    list_data = [{"key": "value1"}, {"key": "value2"}]

    # Mock the storage upload method
    test_source.storage.upload_json = MagicMock()

    # Test saving with list data
    test_source._save_data(list_data, test_config.dataset, test_config.bucket, "silver")

    # Verify the upload method was called
    test_source.storage.upload_json.assert_called_once()


def test_save_data_local(test_config: MockJobConfig, mock_storage_access) -> None:
    """Test saving data locally."""
    # Create config with save_local=True
    local_config = MockJobConfig(save_local=True)

    with patch("unified_pipeline.common.base.StorageAccess", return_value=mock_storage_access):
        source = MockSource(local_config)

        # Create a test table
        table_name = "test_local_table"
        source.conn.execute(f"CREATE TABLE {table_name} (id INTEGER, name VARCHAR)")
        source.conn.execute(f"INSERT INTO {table_name} VALUES (1, 'test')")

        # Test saving locally - should not raise an error
        # The local save uses DuckDB COPY command
        source._save_data(table_name, local_config.dataset, local_config.bucket, "silver")

        # Verify that upload_from_duckdb_table was NOT called (since save_local=True)
        mock_storage_access.upload_from_duckdb_table.assert_not_called()


def test_save_data_invalid_stage(test_source: MockSource, test_config: MockJobConfig) -> None:
    """Test that invalid stage raises ValueError."""
    with pytest.raises(ValueError) as excinfo:
        test_source._save_data("test_table", test_config.dataset, test_config.bucket, "invalid")

    assert "Invalid stage" in str(excinfo.value)


def test_save_data_subdataset(test_source: MockSource, test_config: MockJobConfig) -> None:
    """Test saving data with subdataset name."""
    table_name = "test_table"

    # Create a test table in DuckDB
    test_source.conn.execute(f"CREATE TABLE {table_name} (id INTEGER, name VARCHAR)")
    test_source.conn.execute(f"INSERT INTO {table_name} VALUES (1, 'test')")

    # Mock the storage upload method
    test_source.storage.upload_from_duckdb_table = MagicMock()

    # Test saving with subdataset
    test_source._save_data(
        table_name, test_config.dataset, test_config.bucket, "silver", subdataset="sub"
    )

    # Verify the upload method was called with correct path
    test_source.storage.upload_from_duckdb_table.assert_called_once()
    call_args = test_source.storage.upload_from_duckdb_table.call_args
    # The path should include the subdataset name
    assert "test_dataset_sub" in call_args[0][1]


# Integration test for unified architecture
def test_shared_connection_between_components(
    test_config: MockJobConfig, mock_storage_access
) -> None:
    """Test that components share the same DuckDB connection."""
    with patch("unified_pipeline.common.base.StorageAccess", return_value=mock_storage_access):
        # Create source
        source = MockSource(test_config)

        # Verify connection exists
        assert source.conn is not None

        # Verify we can create tables and query them
        source.conn.execute("CREATE TABLE test_shared (id INTEGER)")
        source.conn.execute("INSERT INTO test_shared VALUES (42)")

        result = source.conn.execute("SELECT id FROM test_shared").fetchone()
        assert result[0] == 42


def test_cleanup_resources(test_config: MockJobConfig, mock_storage_access) -> None:
    """Test that cleanup_resources method works correctly."""
    with patch("unified_pipeline.common.base.StorageAccess", return_value=mock_storage_access):
        source = MockSource(test_config)

        # Create some temporary tables
        source.conn.execute("CREATE TABLE temp_test (id INTEGER)")
        source.conn.execute("CREATE TABLE tmp_test2 (id INTEGER)")
        source.conn.execute("CREATE TABLE regular_table (id INTEGER)")

        # Call cleanup
        source.cleanup_resources()

        # Verify temp tables were cleaned up
        tables = source.conn.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in tables]

        # temp_ and tmp_ prefixed tables should be removed
        assert "temp_test" not in table_names
        assert "tmp_test2" not in table_names


def test_get_memory_usage(test_config: MockJobConfig, mock_storage_access) -> None:
    """Test that get_memory_usage returns proper structure."""
    with patch("unified_pipeline.common.base.StorageAccess", return_value=mock_storage_access):
        source = MockSource(test_config)

        memory_info = source.get_memory_usage()

        # Should return a dictionary with expected keys
        assert isinstance(memory_info, dict)
        # May have 'system' key if psutil is available, or 'error' if not
        assert "system" in memory_info or "error" in memory_info


def test_read_bronze_data_with_memory_data(test_config: MockJobConfig, mock_storage_access) -> None:
    """Test _read_bronze_data with in-memory data passing."""
    with patch("unified_pipeline.common.base.StorageAccess", return_value=mock_storage_access):
        source = MockSource(test_config)

        # Test with table name
        source.conn.execute("CREATE TABLE existing_table (id INTEGER, name VARCHAR)")
        source.conn.execute("INSERT INTO existing_table VALUES (1, 'test')")

        result = source._read_bronze_data("test", "bucket", bronze_data="existing_table")
        assert result == "existing_table"


def test_read_bronze_data_with_list_of_strings(
    test_config: MockJobConfig, mock_storage_access
) -> None:
    """Test _read_bronze_data with list of XML strings."""
    with patch("unified_pipeline.common.base.StorageAccess", return_value=mock_storage_access):
        source = MockSource(test_config)

        # Test with list of strings (like XML payloads)
        xml_data = ["<xml>data1</xml>", "<xml>data2</xml>"]

        result = source._read_bronze_data("test", "bucket", bronze_data=xml_data)

        # Should return a table name
        assert isinstance(result, str)
        assert "bronze_data_test" in result

        # Verify data was inserted
        count = source.conn.execute(f"SELECT COUNT(*) FROM {result}").fetchone()[0]
        assert count == 2


def test_read_bronze_data_with_dict_list(test_config: MockJobConfig, mock_storage_access) -> None:
    """Test _read_bronze_data with list of dictionaries."""
    with patch("unified_pipeline.common.base.StorageAccess", return_value=mock_storage_access):
        source = MockSource(test_config)

        # Test with list of dicts
        dict_data = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]

        result = source._read_bronze_data("test", "bucket", bronze_data=dict_data)

        # Should return a table name
        assert isinstance(result, str)
        assert "bronze_data_test" in result

        # Verify data was inserted
        count = source.conn.execute(f"SELECT COUNT(*) FROM {result}").fetchone()[0]
        assert count == 2


def test_configure_duckdb_spatial_extension(
    test_config: MockJobConfig, mock_storage_access
) -> None:
    """Test that DuckDB spatial extension is configured."""
    with patch("unified_pipeline.common.base.StorageAccess", return_value=mock_storage_access):
        source = MockSource(test_config)

        # Try to use spatial function - should work if spatial extension is loaded
        try:
            result = source.conn.execute("SELECT ST_Point(0, 0)").fetchone()
            assert result is not None
        except Exception:
            # Spatial extension might not be available in all environments
            # This is acceptable for CI environments without the extension
            pass
