"""
Tests for the base module in the unified pipeline.

This module contains tests for the base classes and methods defined in
unified_pipeline.common.base, including BaseJobConfig, BaseSource,
and utility methods for saving and reading data.
"""

import os
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Point

from unified_pipeline.common.base import BaseJobConfig, BaseSource
from unified_pipeline.util.gcs_util import GCSUtil


# Create test classes that inherit from the base classes (not test classes themselves)
class TestJobConfig(BaseJobConfig):
    """Configuration class for testing BaseJobConfig."""

    dataset: str = "test_dataset"
    bucket: str = "test_bucket"
    name: str = "Test Source"


class TestSource(BaseSource[TestJobConfig]):
    """Source class for testing BaseSource."""

    def __init__(self, config: TestJobConfig, gcs_util: GCSUtil):
        super().__init__(config, gcs_util)

    async def run(self) -> None:
        """Implement the abstract run method for testing."""
        pass


# Fixtures
@pytest.fixture
def mock_gcs_util() -> MagicMock:
    """Create a mock GCS utility for testing."""
    mock_util = MagicMock(spec=GCSUtil)
    mock_bucket = MagicMock()
    mock_blob = MagicMock()

    # Set up the mock chain for GCS operations
    mock_util.get_gcs_client.return_value.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob

    return mock_util


@pytest.fixture
def test_config() -> TestJobConfig:
    """Create a test configuration for testing."""
    return TestJobConfig()


@pytest.fixture
def test_source(test_config: TestJobConfig, mock_gcs_util: MagicMock) -> TestSource:
    """Create a test source for testing."""
    return TestSource(test_config, mock_gcs_util)


@pytest.fixture
def test_dataframe() -> pd.DataFrame:
    """Create a test DataFrame for testing."""
    return pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})


@pytest.fixture
def test_geodataframe() -> gpd.GeoDataFrame:
    """Create a test GeoDataFrame for testing."""
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
def test_base_source_initialization(test_config: TestJobConfig, mock_gcs_util: MagicMock) -> None:
    """Test that BaseSource can be initialized and extended."""
    source = TestSource(test_config, mock_gcs_util)

    # Test that attributes are correctly set
    assert source.config == test_config
    assert source.gcs_util == mock_gcs_util
    assert source.log is not None


# Tests for _save_raw_data method
@patch("unified_pipeline.common.base.pd.Timestamp")
@patch("unified_pipeline.common.base.os.makedirs")
def test_save_raw_data(
    mock_makedirs: MagicMock,
    mock_timestamp: MagicMock,
    test_source: TestSource,
    test_dataframe: pd.DataFrame,
    mock_gcs_util: MagicMock,
    test_config: TestJobConfig,
) -> None:
    """Test saving raw data to Google Cloud Storage."""
    mock_now = pd.Timestamp("2025-05-26")
    mock_timestamp.now.return_value = mock_now
    expected_date_str = mock_now.strftime("%Y-%m-%d")

    mock_bucket = mock_gcs_util.get_gcs_client.return_value.bucket.return_value
    mock_blob = mock_bucket.blob.return_value

    with patch("unified_pipeline.common.base.pd.DataFrame.to_parquet") as mock_to_parquet:
        test_source._save_raw_data(test_dataframe, test_config.dataset, test_config.bucket)

    mock_gcs_util.get_gcs_client.return_value.bucket.assert_called_once_with(test_config.bucket)

    temp_dir = f"/tmp/bronze/{test_config.dataset}"
    mock_makedirs.assert_called_once_with(temp_dir, exist_ok=True)

    temp_file = f"{temp_dir}/{expected_date_str}.parquet"
    expected_blob_path = f"bronze/{test_config.dataset}/{expected_date_str}.parquet"
    mock_bucket.blob.assert_called_once_with(expected_blob_path)

    mock_to_parquet.assert_called_once_with(temp_file)
    mock_blob.upload_from_filename.assert_called_once_with(temp_file)


# Tests for _save_data method
@patch("unified_pipeline.common.base.pd.Timestamp")
@patch("unified_pipeline.common.base.os.makedirs")
def test_save_data(
    mock_makedirs: MagicMock,
    mock_timestamp: MagicMock,
    test_source: TestSource,
    test_geodataframe: gpd.GeoDataFrame,
    mock_gcs_util: MagicMock,
    test_config: TestJobConfig,
) -> None:
    """Test saving processed data to Google Cloud Storage."""
    mock_now = pd.Timestamp("2025-05-26")
    mock_timestamp.now.return_value = mock_now
    expected_date_str = mock_now.strftime("%Y-%m-%d")

    mock_bucket = mock_gcs_util.get_gcs_client.return_value.bucket.return_value
    mock_blob = mock_bucket.blob.return_value

    with patch("unified_pipeline.common.base.gpd.GeoDataFrame.to_parquet") as mock_to_parquet:
        test_source._save_data(test_geodataframe, test_config.dataset, test_config.bucket)

    mock_gcs_util.get_gcs_client.return_value.bucket.assert_called_once_with(test_config.bucket)

    temp_dir = f"/tmp/silver/{test_config.dataset}"
    mock_makedirs.assert_called_once_with(temp_dir, exist_ok=True)

    temp_file = f"{temp_dir}/{expected_date_str}.parquet"
    expected_blob_path = f"silver/{test_config.dataset}/{expected_date_str}.parquet"
    mock_bucket.blob.assert_called_once_with(expected_blob_path)

    mock_to_parquet.assert_called_once_with(temp_file)
    mock_blob.upload_from_filename.assert_called_once_with(temp_file)


def test_save_data_with_empty_dataframe(
    test_source: TestSource, mock_gcs_util: MagicMock, test_config: TestJobConfig
) -> None:
    """Test saving an empty GeoDataFrame."""
    empty_gdf = gpd.GeoDataFrame(columns=["id", "geometry"], geometry="geometry")
    test_source._save_data(empty_gdf, test_config.dataset, test_config.bucket)
    mock_gcs_util.get_gcs_client.return_value.bucket.assert_not_called()


def test_save_data_with_none_dataframe(
    test_source: TestSource, mock_gcs_util: MagicMock, test_config: TestJobConfig
) -> None:
    """Test saving None as a GeoDataFrame."""
    test_source._save_data(None, test_config.dataset, test_config.bucket)
    mock_gcs_util.get_gcs_client.return_value.bucket.assert_not_called()


# Tests for _read_bronze_data method
@patch("unified_pipeline.common.base.pd.Timestamp")
@patch("unified_pipeline.common.base.os.makedirs")
def test_read_bronze_data_success(
    mock_makedirs: MagicMock,
    mock_timestamp: MagicMock,
    test_source: TestSource,
    test_dataframe: pd.DataFrame,
    mock_gcs_util: MagicMock,
    test_config: TestJobConfig,
) -> None:
    """Test reading bronze data from Google Cloud Storage."""
    mock_now = pd.Timestamp("2025-05-26")
    mock_timestamp.now.return_value = mock_now
    expected_date_str = mock_now.strftime("%Y-%m-%d")

    mock_bucket = mock_gcs_util.get_gcs_client.return_value.bucket.return_value
    mock_blob = mock_bucket.blob.return_value
    mock_blob.exists.return_value = True

    with patch(
        "unified_pipeline.common.base.pd.read_parquet", return_value=test_dataframe
    ) as mock_read_parquet:
        result = test_source._read_bronze_data(test_config.dataset, test_config.bucket)

    mock_gcs_util.get_gcs_client.return_value.bucket.assert_called_once_with(test_config.bucket)

    expected_blob_path = f"bronze/{test_config.dataset}/{expected_date_str}.parquet"
    mock_bucket.blob.assert_called_once_with(expected_blob_path)
    mock_blob.exists.assert_called_once()

    temp_dir = f"/tmp/bronze/{test_config.dataset}"
    mock_makedirs.assert_called_once_with(temp_dir, exist_ok=True)

    temp_file = f"{temp_dir}/{expected_date_str}.parquet"
    mock_blob.download_to_filename.assert_called_once_with(temp_file)

    mock_read_parquet.assert_called_once_with(temp_file)

    assert result is not None
    pd.testing.assert_frame_equal(result, test_dataframe)


def test_read_bronze_data_blob_not_exists(
    test_source: TestSource, mock_gcs_util: MagicMock, test_config: TestJobConfig
) -> None:
    """Test reading bronze data when the blob doesn't exist."""
    mock_bucket = mock_gcs_util.get_gcs_client.return_value.bucket.return_value
    mock_blob = mock_bucket.blob.return_value
    mock_blob.exists.return_value = False

    result = test_source._read_bronze_data(test_config.dataset, test_config.bucket)

    mock_gcs_util.get_gcs_client.return_value.bucket.assert_called_once_with(test_config.bucket)
    mock_bucket.blob.assert_called_once()
    mock_blob.exists.assert_called_once()

    mock_blob.download_to_filename.assert_not_called()

    assert result is None


# Integration tests with real file system (using temporary directory)
def test_save_and_read_real_files() -> None:
    """Integration test for saving and reading files using the real file system."""
    # Skip this test if we're not in a CI environment where file operations are safe
    # pytest.skip("Skipping real file operations in unit tests")

    # Use a temporary directory for testing
    with TemporaryDirectory() as temp_root:
        # Override the /tmp directory with our temporary directory
        tmp_bronze_dir = os.path.join(temp_root, "bronze", "test_dataset")
        tmp_silver_dir = os.path.join(temp_root, "silver", "test_dataset")
        os.makedirs(tmp_bronze_dir, exist_ok=True)
        os.makedirs(tmp_silver_dir, exist_ok=True)

        df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        gdf = gpd.GeoDataFrame(
            {"id": [1, 2], "geometry": [Point(0, 0), Point(1, 1)]},
            geometry="geometry",
            crs="EPSG:4326",
        )

        current_date = pd.Timestamp.now().strftime("%Y-%m-%d")
        bronze_file = os.path.join(tmp_bronze_dir, f"{current_date}.parquet")
        silver_file = os.path.join(tmp_silver_dir, f"{current_date}.parquet")

        df.to_parquet(bronze_file)
        gdf.to_parquet(silver_file)

        assert os.path.exists(bronze_file)
        assert os.path.exists(silver_file)

        df_read = pd.read_parquet(bronze_file)
        gdf_read = gpd.read_parquet(silver_file)

        # Verify data integrity
        pd.testing.assert_frame_equal(df, df_read)
        assert gdf_read.crs == gdf.crs
        assert len(gdf_read) == len(gdf)


# Test for timed decorator
@patch("unified_pipeline.util.timing.time.time")
def test_timed_decorator(mock_time: MagicMock) -> None:
    """Test that the timed decorator works correctly."""
    from unified_pipeline.util.timing import timed

    mock_time.side_effect = [0, 1]  # First call returns 0, second call returns 1 (1 second elapsed)

    # Define a test function with the timed decorator
    @timed(name="Test Timer")  # type: ignore
    def test_function() -> str:
        return "test_result"

    result = test_function()

    assert result == "test_result"

    # We cannot easily verify the log output with the current setup,
    # but we can verify that time.time() was called twice
    assert mock_time.call_count == 2
