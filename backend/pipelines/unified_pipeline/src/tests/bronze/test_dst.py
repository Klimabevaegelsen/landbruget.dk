"""
Unit tests for DST Bronze layer implementation.

This module tests the bronze layer data ingestion for Danish Statistics data,
including configuration validation, API client functionality, and data processing.
"""

from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from unified_pipeline.bronze.dst import DSTApiClient, DSTBronze, DSTBronzeConfig


class TestDSTBronzeConfig:
    """Test cases for DSTBronzeConfig."""

    def test_default_config(self):
        """Test default configuration values."""
        config = DSTBronzeConfig()

        assert config.name == "Danmarks Statistik API"
        assert config.dataset == "dst"
        assert config.type == "api"
        assert config.description == "Danish Statistics data from DST API"
        assert config.frequency == "monthly"
        assert config.bucket == "landbrugsdata-raw-data"
        assert config.table_ids == ["HST77", "GARTN1", "FRO", "HALM1"]
        assert config.lang == "da"
        assert config.api_base_url == "https://api.statbank.dk/v1"

    def test_custom_config(self):
        """Test custom configuration values."""
        config = DSTBronzeConfig(
            table_ids=["HST77", "GARTN1"],
            lang="en",
            api_base_url="https://custom-api.example.com/v1",
        )

        assert config.table_ids == ["HST77", "GARTN1"]
        assert config.lang == "en"
        assert config.api_base_url == "https://custom-api.example.com/v1"


class TestDSTApiClient:
    """Test cases for DSTApiClient."""

    def test_initialization(self):
        """Test API client initialization."""
        client = DSTApiClient(base_url="https://api.statbank.dk/v1", lang="da")

        assert client.base_url == "https://api.statbank.dk/v1"
        assert client.lang == "da"
        assert "User-Agent" in client.session.headers
        assert client.session.headers["User-Agent"] == "DanishStatsPipeline/1.0"

    @patch("requests.Session.get")
    def test_get_table_info_success(self, mock_get):
        """Test successful table info retrieval."""
        # Mock successful response
        mock_response = MagicMock()
        mock_response.json.return_value = {"table": "HST77", "variables": []}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        client = DSTApiClient()
        result = client.get_table_info("HST77")

        assert result is not None
        assert result["table"] == "HST77"
        mock_get.assert_called_once()

    @patch("requests.Session.get")
    def test_get_table_info_failure(self, mock_get):
        """Test table info retrieval failure."""
        # Mock failed response
        mock_get.side_effect = Exception("API Error")

        client = DSTApiClient()
        result = client.get_table_info("HST77")

        assert result is None

    def test_build_request_payload_hst77(self):
        """Test request payload building for HST77 table."""
        client = DSTApiClient()
        payload = client._build_request_payload("HST77")

        assert payload["table"] == "HST77"
        assert payload["lang"] == "da"
        assert payload["format"] == "JSONSTAT"
        assert "variables" in payload

        # Check HST77-specific variables
        variables = payload["variables"]
        assert len(variables) == 4
        assert any(var["code"] == "OMRÅDE" for var in variables)
        assert any(var["code"] == "AFGRØDE" for var in variables)

    def test_build_request_payload_gartn1(self):
        """Test request payload building for GARTN1 table."""
        client = DSTApiClient()
        payload = client._build_request_payload("GARTN1")

        assert payload["table"] == "GARTN1"
        variables = payload["variables"]
        assert any(var["code"] == "TAL" for var in variables)

    def test_build_request_payload_unknown_table(self):
        """Test request payload building for unknown table."""
        client = DSTApiClient()
        payload = client._build_request_payload("UNKNOWN")

        assert payload["table"] == "UNKNOWN"
        variables = payload["variables"]
        assert len(variables) == 1
        assert variables[0]["code"] == "*"


class TestDSTBronze:
    """Test cases for DSTBronze."""

    @pytest.fixture
    def mock_gcs_access(self):
        """Mock GCS access for testing."""
        return Mock()

    @pytest.fixture
    def dst_bronze(self):
        """Create a DSTBronze instance for testing."""
        config = DSTBronzeConfig(table_ids=["HST77"])
        return DSTBronze(config)

    def test_initialization(self, dst_bronze):
        """Test DSTBronze initialization."""
        assert dst_bronze.config.dataset == "dst"
        assert dst_bronze.api_client is not None
        assert dst_bronze.api_client.base_url == "https://api.statbank.dk/v1"

    @pytest.mark.asyncio
    async def test_fetch_table_data_success(self, dst_bronze):
        """Test successful table data fetching."""
        # Mock API client methods
        dst_bronze.api_client.get_table_info = MagicMock(return_value={"table": "HST77"})
        dst_bronze.api_client.get_table_data = MagicMock(return_value={"value": [1, 2, 3]})

        # Mock GCS access
        dst_bronze.gcs_access = MagicMock()
        dst_bronze.gcs_access.upload_json = MagicMock()

        result = await dst_bronze._fetch_table_data("HST77")

        assert result is not None
        assert result["table_id"] == "HST77"
        assert "data" in result
        assert "metadata" in result
        assert result["data"]["value"] == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_fetch_table_data_api_failure(self, dst_bronze):
        """Test table data fetching with API failure."""
        # Mock API client methods to return None (failure)
        dst_bronze.api_client.get_table_info = MagicMock(return_value=None)
        dst_bronze.api_client.get_table_data = MagicMock(return_value=None)

        result = await dst_bronze._fetch_table_data("HST77")

        assert result is None

    @pytest.mark.asyncio
    async def test_run_success(self, dst_bronze):
        """Test successful run execution."""
        # Mock _fetch_table_data to return success
        dst_bronze._fetch_table_data = AsyncMock(
            return_value={
                "table_id": "HST77",
                "data": {"value": [1, 2, 3]},
                "metadata": {"record_count": 3},
            }
        )

        result = await dst_bronze.run()

        assert result is not None
        assert "HST77" in result
        assert result["HST77"]["table_id"] == "HST77"

    @pytest.mark.asyncio
    async def test_run_failure(self, dst_bronze):
        """Test run execution with all tables failing."""
        # Mock _fetch_table_data to return None (failure)
        dst_bronze._fetch_table_data = AsyncMock(return_value=None)

        result = await dst_bronze.run()

        assert result is None

    def test_save_table_data(self, dst_bronze):
        """Test table data saving to GCS."""
        # Mock GCS access
        dst_bronze.gcs_access = MagicMock()
        dst_bronze.gcs_access.upload_json = MagicMock()

        table_data = {"value": [1, 2, 3]}
        table_info = {"table": "HST77"}
        metadata = {"record_count": 3}

        dst_bronze._save_table_data("HST77", table_data, table_info, metadata)

        # Verify all three files were uploaded
        assert dst_bronze.gcs_access.upload_json.call_count == 3

        # Check the calls
        calls = dst_bronze.gcs_access.upload_json.call_args_list
        paths = [call[0][1] for call in calls]  # Second argument is the path

        # Verify paths use proper GCS format with bucket name
        assert any(
            "gs://landbrugsdata-raw-data/bronze/dst/" in path and "HST77_data.json" in path
            for path in paths
        )
        assert any(
            "gs://landbrugsdata-raw-data/bronze/dst/" in path and "HST77_tableinfo.json" in path
            for path in paths
        )
        assert any(
            "gs://landbrugsdata-raw-data/bronze/dst/" in path and "HST77_metadata.json" in path
            for path in paths
        )
