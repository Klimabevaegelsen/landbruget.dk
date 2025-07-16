"""
Tests for the base classes in the unified pipeline.

This module tests the core functionality of BaseJobConfig and BaseSource
classes to ensure they work correctly with the unified GCS access architecture.
"""

from typing import Optional
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from shapely.geometry import Point

try:
    import geopandas as gpd
except ImportError:
    gpd = None

from unified_pipeline.common.base import BaseJobConfig, BaseSource
from unified_pipeline.util.gcs_access import GCSDataAccess


class TestJobConfig(BaseJobConfig):
    """Configuration class for testing BaseJobConfig."""

    dataset: str = "test_dataset"
    bucket: str = "test_bucket"
    name: str = "Test Source"


class TestSource(BaseSource[TestJobConfig]):
    """Test implementation of BaseSource for testing purposes."""

    def __init__(self, config: TestJobConfig):
        super().__init__(config)

    async def run(self) -> None:
        """Test implementation of the run method."""
        pass


# Fixtures
@pytest.fixture
def mock_gcs_access():
    """Create a mock GCS access layer for testing."""
    with patch("unified_pipeline.util.gcs_access.GCSDataAccess") as mock_class:
        mock_instance = MagicMock(spec=GCSDataAccess)
        mock_class.return_value = mock_instance

        # Mock DuckDB connection
        mock_instance.duckdb_conn = MagicMock()
        mock_instance.fs = MagicMock()
        mock_instance.monitor = MagicMock()

        # Mock common methods
        mock_instance.upload_from_duckdb_table.return_value = None
        mock_instance.create_table_from_gcs.return_value = None
        mock_instance.list_files.return_value = []
        mock_instance.file_exists.return_value = True

        yield mock_instance


@pytest.fixture
def mock_duckdb_connection():
    """Create a mock DuckDB connection."""
    with patch("duckdb.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        # Mock common operations
        mock_conn.execute.return_value = MagicMock()
        mock_conn.fetchone.return_value = [100]  # Default count
        mock_conn.fetchall.return_value = []

        yield mock_conn


@pytest.fixture
def test_config() -> TestJobConfig:
    """Create a test configuration for testing."""
    return TestJobConfig()


@pytest.fixture
def test_source(test_config: TestJobConfig, mock_gcs_access, mock_duckdb_connection) -> TestSource:
    """Create a test source for testing."""
    with patch("unified_pipeline.common.base.GCSDataAccess", return_value=mock_gcs_access):
        with patch("duckdb.connect", return_value=mock_duckdb_connection):
            return TestSource(test_config)


@pytest.fixture
def test_dataframe() -> pd.DataFrame:
    """Create a test DataFrame for testing."""
    return pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})


@pytest.fixture
def test_geodataframe() -> Optional[pd.DataFrame]:
    """Create a test GeoDataFrame for testing."""
    if gpd is None:
        pytest.skip("GeoPandas not available")

    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Point1", "Point2", "Point3"],
            "geometry": [Point(0, 0), Point(1, 1), Point(2, 2)],
        }
    )
    return gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")


# Tests for BaseJobConfig
def test_base_job_config_initialization() -> None:
    """Test that BaseJobConfig can be initialized and extended."""
    config = TestJobConfig()

    # Test that attributes are correctly set
    assert config.dataset == "test_dataset"
    assert config.bucket == "test_bucket"
    assert config.name == "Test Source"


# Tests for BaseSource
def test_base_source_initialization(
    test_config: TestJobConfig, mock_gcs_access, mock_duckdb_connection
) -> None:
    """Test that BaseSource can be initialized with unified architecture."""
    with patch("unified_pipeline.common.base.GCSDataAccess", return_value=mock_gcs_access):
        with patch("duckdb.connect", return_value=mock_duckdb_connection):
            source = TestSource(test_config)

            # Test that attributes are correctly set
            assert source.config == test_config
            assert source.log is not None
            assert source.gcs_access == mock_gcs_access
            assert source.conn == mock_duckdb_connection


def test_unified_connection_architecture(test_source: TestSource) -> None:
    """Test that the unified connection architecture works correctly."""
    # Verify that gcs_access and conn are properly connected
    assert hasattr(test_source, "conn")
    assert hasattr(test_source, "gcs_access")
    assert test_source.gcs_access is not None


# Tests for _save_data method
def test_save_data_table_name(test_source: TestSource, test_config: TestJobConfig) -> None:
    """Test saving data using table name."""
    table_name = "test_table"

    # Mock the gcs_access upload method
    test_source.gcs_access.upload_from_duckdb_table = MagicMock()

    # Test saving with table name
    test_source._save_data(table_name, test_config.dataset, test_config.bucket, "silver")

    # Verify the upload method was called
    test_source.gcs_access.upload_from_duckdb_table.assert_called_once()


def test_save_data_json(test_source: TestSource, test_config: TestJobConfig) -> None:
    """Test saving JSON data."""
    json_data = {"key": "value", "number": 123}

    # Mock the gcs_access upload method
    test_source.gcs_access.upload_json = MagicMock()

    # Test saving with JSON data
    test_source._save_data(json_data, test_config.dataset, test_config.bucket, "silver")

    # Verify the upload method was called
    test_source.gcs_access.upload_json.assert_called_once()


def test_save_data_local(test_source: TestSource, test_config: TestJobConfig) -> None:
    """Test saving data locally."""
    test_config.save_local = True
    table_name = "test_table"

    # Mock DuckDB execute
    test_source.conn.execute = MagicMock()

    # Test saving locally
    test_source._save_data(table_name, test_config.dataset, test_config.bucket, "silver")

    # Verify DuckDB COPY was called for local save
    test_source.conn.execute.assert_called()


# Integration test for unified architecture
def test_shared_connection_between_components(
    test_config: TestJobConfig, mock_gcs_access, mock_duckdb_connection
) -> None:
    """Test that components share the same DuckDB connection."""
    with patch("unified_pipeline.common.base.GCSDataAccess", return_value=mock_gcs_access):
        with patch("duckdb.connect", return_value=mock_duckdb_connection):
            # Create source
            source = TestSource(test_config)

            # Verify connection sharing
            assert source.conn == mock_duckdb_connection
            assert source.gcs_access == mock_gcs_access

            # Verify GCSDataAccess was initialized with the connection
            # (This would be the actual connection in real usage)
            assert source.gcs_access is not None
