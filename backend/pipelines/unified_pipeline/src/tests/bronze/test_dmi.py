"""
Unit tests for DMI Bronze layer implementation.

This module tests the bronze layer data ingestion for Danish Meteorological Institute data,
including configuration validation, API client functionality, and data processing.
"""

import os
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from unified_pipeline.bronze.dmi import DMIApiClient, DMIBronze, DMIBronzeConfig
from unified_pipeline.util.gcs_util import GCSUtil


class TestDMIBronzeConfig:
    """Test cases for DMIBronzeConfig."""

    def test_default_config(self):
        """Test default configuration values."""
        config = DMIBronzeConfig()

        assert config.name == "Danish Meteorological Institute"
        assert config.dataset == "dmi"
        assert config.type == "api"
        assert config.description == "DMI climate data from GovCloud API"
        assert config.frequency == "daily"
        assert config.bucket == "landbrugsdata-raw-data"
        assert config.parameters == ["pot_evaporation_makkink", "acc_precip"]
        assert config.api_base_url == "https://dmigw.govcloud.dk/v2/climateData"
        assert config.max_concurrent_fetches == 5
        assert config.days_back == 30

    def test_custom_config(self):
        """Test custom configuration values."""
        config = DMIBronzeConfig(
            parameters=["pot_evaporation_makkink"], max_concurrent_fetches=3, days_back=7
        )

        assert config.parameters == ["pot_evaporation_makkink"]
        assert config.max_concurrent_fetches == 3
        assert config.days_back == 7


class TestDMIApiClient:
    """Test cases for DMIApiClient."""

    @patch.dict(os.environ, {"DMI_GOV_CLOUD_API_KEY": "test_api_key"})
    def test_initialization_with_api_key(self):
        """Test API client initialization with valid API key."""
        config = DMIBronzeConfig()
        client = DMIApiClient(config)

        assert client.config == config
        assert client.api_key == "test_api_key"
        assert "X-Gravitee-Api-Key" in client.headers
        assert client.headers["X-Gravitee-Api-Key"] == "test_api_key"

    @patch.dict(os.environ, {}, clear=True)
    def test_initialization_without_api_key(self):
        """Test API client initialization without API key raises error."""
        config = DMIBronzeConfig()

        with pytest.raises(ValueError) as excinfo:
            DMIApiClient(config)

        assert "DMI_GOV_CLOUD_API_KEY environment variable is required" in str(excinfo.value)

    @patch.dict(os.environ, {"DMI_GOV_CLOUD_API_KEY": "test_api_key"})
    @pytest.mark.asyncio
    async def test_make_request_success(self):
        """Test successful API request."""
        config = DMIBronzeConfig()
        client = DMIApiClient(config)

        # Mock aiohttp session and response
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={"features": []})
        mock_response.text = AsyncMock(return_value='{"features": []}')
        mock_response.raise_for_status = MagicMock()

        mock_session = MagicMock()
        mock_session.get = MagicMock()
        mock_session.get.return_value.__aenter__ = AsyncMock(return_value=mock_response)
        mock_session.get.return_value.__aexit__ = AsyncMock(return_value=None)

        result = await client._make_request(mock_session, "test_endpoint", {"param": "value"})

        assert result == {"features": []}
        mock_session.get.assert_called_once()

    @patch.dict(os.environ, {"DMI_GOV_CLOUD_API_KEY": "test_api_key"})
    @pytest.mark.asyncio
    async def test_make_request_rate_limit(self):
        """Test API request with rate limit error."""
        config = DMIBronzeConfig()
        client = DMIApiClient(config)

        # Mock aiohttp session and response with rate limit
        mock_response = MagicMock()
        mock_response.status = 429

        mock_session = MagicMock()
        mock_session.get = MagicMock()
        mock_session.get.return_value.__aenter__ = AsyncMock(return_value=mock_response)
        mock_session.get.return_value.__aexit__ = AsyncMock(return_value=None)

        with pytest.raises(aiohttp.ClientError) as excinfo:
            await client._make_request(mock_session, "test_endpoint")

        assert "Rate limit exceeded" in str(excinfo.value)

    @patch.dict(os.environ, {"DMI_GOV_CLOUD_API_KEY": "test_api_key"})
    @pytest.mark.asyncio
    async def test_fetch_grid_data_success(self):
        """Test successful grid data fetching."""
        config = DMIBronzeConfig()
        client = DMIApiClient(config)

        # Mock successful API response
        mock_features = [{"properties": {"value": 1.5}, "geometry": {}}]
        client._make_request = AsyncMock(return_value={"features": mock_features})

        mock_session = MagicMock()
        start_time = datetime.now() - timedelta(days=1)
        end_time = datetime.now()

        result = await client.fetch_grid_data(
            mock_session, "pot_evaporation_makkink", start_time, end_time
        )

        assert "features" in result
        assert result["features"] == mock_features
        assert result["parameter_id"] == "pot_evaporation_makkink"
        assert result["feature_count"] == 1

    @patch.dict(os.environ, {"DMI_GOV_CLOUD_API_KEY": "test_api_key"})
    @pytest.mark.asyncio
    async def test_fetch_grid_data_no_data(self):
        """Test grid data fetching with no data returned."""
        config = DMIBronzeConfig()
        client = DMIApiClient(config)

        # Mock API response with no features
        client._make_request = AsyncMock(return_value={"features": []})

        mock_session = MagicMock()
        start_time = datetime.now() - timedelta(days=1)
        end_time = datetime.now()

        result = await client.fetch_grid_data(
            mock_session, "pot_evaporation_makkink", start_time, end_time
        )

        assert result["features"] == []
        assert result["message"] == "No data returned"

    @patch.dict(os.environ, {"DMI_GOV_CLOUD_API_KEY": "test_api_key"})
    @pytest.mark.asyncio
    async def test_fetch_grid_data_error(self):
        """Test grid data fetching with API error."""
        config = DMIBronzeConfig()
        client = DMIApiClient(config)

        # Mock API error
        client._make_request = AsyncMock(side_effect=Exception("API Error"))

        mock_session = MagicMock()
        start_time = datetime.now() - timedelta(days=1)
        end_time = datetime.now()

        result = await client.fetch_grid_data(
            mock_session, "pot_evaporation_makkink", start_time, end_time
        )

        assert result["features"] == []
        assert "error" in result
        assert "API Error" in result["error"]


class TestDMIBronze:
    """Test cases for DMIBronze."""

    @pytest.fixture
    def mock_gcs_util(self):
        """Create a mock GCS utility."""
        return MagicMock(spec=GCSUtil)

    @patch.dict(os.environ, {"DMI_GOV_CLOUD_API_KEY": "test_api_key"})
    @pytest.fixture
    def dmi_bronze(self, mock_gcs_util):
        """Create a DMIBronze instance for testing."""
        config = DMIBronzeConfig(parameters=["pot_evaporation_makkink"])
        return DMIBronze(config, mock_gcs_util)

    def test_initialization(self, dmi_bronze):
        """Test DMIBronze initialization."""
        assert dmi_bronze.config.dataset == "dmi"
        assert dmi_bronze.api_client is not None
        assert dmi_bronze.api_client.config == dmi_bronze.config

    def test_calculate_time_range(self, dmi_bronze):
        """Test time range calculation."""
        start_time, end_time = dmi_bronze._calculate_time_range()

        # Check that the time range is approximately correct
        time_diff = end_time - start_time
        expected_diff = timedelta(days=dmi_bronze.config.days_back)

        # Allow for small differences due to execution time
        assert abs(time_diff.total_seconds() - expected_diff.total_seconds()) < 60

    @pytest.mark.asyncio
    async def test_fetch_parameter_data_success(self, dmi_bronze):
        """Test successful parameter data fetching."""
        # Mock API client
        mock_session = MagicMock()
        mock_features = [{"properties": {"value": 1.5}, "geometry": {}}]
        dmi_bronze.api_client.fetch_grid_data = AsyncMock(
            return_value={
                "features": mock_features,
                "parameter_id": "pot_evaporation_makkink",
                "feature_count": 1,
            }
        )

        # Mock GCS access
        dmi_bronze.gcs_access = MagicMock()
        dmi_bronze.gcs_access.upload_json = MagicMock()

        start_time = datetime.now() - timedelta(days=1)
        end_time = datetime.now()

        result = await dmi_bronze._fetch_parameter_data(
            mock_session, "pot_evaporation_makkink", start_time, end_time
        )

        assert result is not None
        assert result["parameter_id"] == "pot_evaporation_makkink"
        assert "data" in result
        assert "metadata" in result
        assert result["data"]["features"] == mock_features

    @pytest.mark.asyncio
    async def test_fetch_parameter_data_failure(self, dmi_bronze):
        """Test parameter data fetching with API failure."""
        # Mock API client to return None
        mock_session = MagicMock()
        dmi_bronze.api_client.fetch_grid_data = AsyncMock(return_value=None)

        start_time = datetime.now() - timedelta(days=1)
        end_time = datetime.now()

        result = await dmi_bronze._fetch_parameter_data(
            mock_session, "pot_evaporation_makkink", start_time, end_time
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_run_success(self, dmi_bronze):
        """Test successful run execution."""
        # Mock _fetch_parameter_data to return success
        dmi_bronze._fetch_parameter_data = AsyncMock(
            return_value={
                "parameter_id": "pot_evaporation_makkink",
                "data": {"features": [{"properties": {"value": 1.5}}]},
                "metadata": {"feature_count": 1},
            }
        )

        result = await dmi_bronze.run()

        assert result is not None
        assert "pot_evaporation_makkink" in result
        assert result["pot_evaporation_makkink"]["parameter_id"] == "pot_evaporation_makkink"

    @pytest.mark.asyncio
    async def test_run_failure(self, dmi_bronze):
        """Test run execution with all parameters failing."""
        # Mock _fetch_parameter_data to return None (failure)
        dmi_bronze._fetch_parameter_data = AsyncMock(return_value=None)

        result = await dmi_bronze.run()

        assert result is None

    def test_save_parameter_data(self, dmi_bronze):
        """Test parameter data saving to GCS."""
        # Mock GCS access
        dmi_bronze.gcs_access = MagicMock()
        dmi_bronze.gcs_access.upload_json = MagicMock()

        parameter_data = {"features": [{"properties": {"value": 1.5}}]}
        metadata = {"feature_count": 1}

        dmi_bronze._save_parameter_data("pot_evaporation_makkink", parameter_data, metadata)

        # Verify both files were uploaded
        assert dmi_bronze.gcs_access.upload_json.call_count == 2

        # Check the calls
        calls = dmi_bronze.gcs_access.upload_json.call_args_list
        paths = [call[0][1] for call in calls]  # Second argument is the path

        # Verify paths use proper GCS format with bucket name
        assert any(
            "gs://landbrugsdata-raw-data/bronze/dmi/" in path
            and "pot_evaporation_makkink_data.json" in path
            for path in paths
        )
        assert any(
            "gs://landbrugsdata-raw-data/bronze/dmi/" in path
            and "pot_evaporation_makkink_metadata.json" in path
            for path in paths
        )
