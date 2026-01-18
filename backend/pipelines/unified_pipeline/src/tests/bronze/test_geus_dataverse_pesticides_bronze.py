"""
Tests for the GEUSDataversePesticidesBronze class.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from unified_pipeline.bronze.geus_dataverse_pesticides import (
    GEUSDataversePesticidesBronze,
    GEUSDataversePesticidesBronzeConfig,
)


@pytest.fixture
def config() -> GEUSDataversePesticidesBronzeConfig:
    """Return a test configuration."""
    return GEUSDataversePesticidesBronzeConfig(
        name="Test GEUS Dataverse Pesticides",
        dataset="test_geus_dataverse_pesticides",
        bucket="test-bucket",
        pest_url="https://test.example.com/dataverse/99149",
        pfas_url="https://test.example.com/dataverse/99150",
        request_timeout=60,
    )


@pytest.fixture
def geus_dataverse_bronze(
    config: GEUSDataversePesticidesBronzeConfig,
) -> GEUSDataversePesticidesBronze:
    """Return a test GEUSDataversePesticidesBronze instance."""
    source = GEUSDataversePesticidesBronze(config)
    source.log = MagicMock()
    return source


def test_config_defaults() -> None:
    """Test default configuration values."""
    config = GEUSDataversePesticidesBronzeConfig()

    assert config.name == "GEUS Dataverse Pesticides and PFAS"
    assert config.dataset == "geus_dataverse_pesticides"
    assert config.type == "dataverse"
    assert config.frequency == "annual"
    assert config.pest_url == "https://dataverse.geus.dk/api/access/datafile/99149"
    assert config.pfas_url == "https://dataverse.geus.dk/api/access/datafile/99150"
    assert config.source_crs == "EPSG:25832"
    assert config.request_timeout == 300


@pytest.mark.asyncio
async def test_download_rds_file_success(
    geus_dataverse_bronze: GEUSDataversePesticidesBronze,
) -> None:
    """Test successful download of .rds file."""
    mock_content = b"test rds content bytes"

    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.read = AsyncMock(return_value=mock_content)

    class MockSessionContextManager:
        async def __aenter__(self):
            return mock_response

        async def __aexit__(self, *args):
            pass

    mock_session = MagicMock()
    mock_session.get = MagicMock(return_value=MockSessionContextManager())

    result = await geus_dataverse_bronze._download_rds_file(
        mock_session, geus_dataverse_bronze.config.pest_url, "AM_pest.rds"
    )

    assert result == mock_content
    mock_session.get.assert_called_once_with(geus_dataverse_bronze.config.pest_url)


@pytest.mark.asyncio
async def test_download_rds_file_http_error(
    geus_dataverse_bronze: GEUSDataversePesticidesBronze,
) -> None:
    """Test HTTP error handling when downloading .rds file."""
    mock_response = AsyncMock()
    mock_response.status = 404

    class MockSessionContextManager:
        async def __aenter__(self):
            return mock_response

        async def __aexit__(self, *args):
            pass

    mock_session = MagicMock()
    mock_session.get = MagicMock(return_value=MockSessionContextManager())

    # Test that an exception is raised (retry error wraps the original)
    with pytest.raises(Exception):
        await geus_dataverse_bronze._download_rds_file(
            mock_session, geus_dataverse_bronze.config.pest_url, "AM_pest.rds"
        )


@patch("unified_pipeline.bronze.geus_dataverse_pesticides.Path")
def test_save_rds_to_gcs(
    mock_path: MagicMock,
    geus_dataverse_bronze: GEUSDataversePesticidesBronze,
) -> None:
    """Test saving .rds file to GCS."""
    mock_content = b"test rds content bytes"

    # Mock the Path.unlink call
    mock_path_instance = MagicMock()
    mock_path.return_value = mock_path_instance

    # Mock gcs_access
    geus_dataverse_bronze.gcs_access = MagicMock()
    geus_dataverse_bronze.date_pattern = "2024-01-15T10-00-00"

    with patch("tempfile.NamedTemporaryFile") as mock_temp:
        mock_temp_file = MagicMock()
        mock_temp_file.name = "/tmp/test_file.rds"
        mock_temp_file.__enter__ = MagicMock(return_value=mock_temp_file)
        mock_temp_file.__exit__ = MagicMock(return_value=False)
        mock_temp.return_value = mock_temp_file

        result = geus_dataverse_bronze._save_rds_to_gcs(mock_content, "AM_pest.rds")

    # Verify the path structure
    assert "bronze/test_geus_dataverse_pesticides" in result
    assert "AM_pest.rds" in result

    # Verify GCS upload was called
    geus_dataverse_bronze.gcs_access.upload_file.assert_called_once()


@patch("unified_pipeline.bronze.geus_dataverse_pesticides.Path")
def test_save_pfas_rds_to_gcs(
    mock_path: MagicMock,
    geus_dataverse_bronze: GEUSDataversePesticidesBronze,
) -> None:
    """Test saving PFAS .rds file to GCS."""
    mock_content = b"test pfas rds content bytes"

    # Mock the Path.unlink call
    mock_path_instance = MagicMock()
    mock_path.return_value = mock_path_instance

    # Mock gcs_access
    geus_dataverse_bronze.gcs_access = MagicMock()
    geus_dataverse_bronze.date_pattern = "2024-01-15T10-00-00"

    with patch("tempfile.NamedTemporaryFile") as mock_temp:
        mock_temp_file = MagicMock()
        mock_temp_file.name = "/tmp/test_file.rds"
        mock_temp_file.__enter__ = MagicMock(return_value=mock_temp_file)
        mock_temp_file.__exit__ = MagicMock(return_value=False)
        mock_temp.return_value = mock_temp_file

        result = geus_dataverse_bronze._save_rds_to_gcs(mock_content, "AM_pfas.rds")

    # Verify the path structure
    assert "bronze/test_geus_dataverse_pesticides" in result
    assert "AM_pfas.rds" in result

    # Verify GCS upload was called
    geus_dataverse_bronze.gcs_access.upload_file.assert_called_once()


@pytest.mark.asyncio
@patch(
    "unified_pipeline.bronze.geus_dataverse_pesticides.GEUSDataversePesticidesBronze._download_rds_file"
)
@patch(
    "unified_pipeline.bronze.geus_dataverse_pesticides.GEUSDataversePesticidesBronze._save_rds_to_gcs"
)
@patch("aiohttp.ClientSession")
@patch("aiohttp.TCPConnector")
async def test_run_success(
    mock_tcp_connector: MagicMock,
    mock_client_session: MagicMock,
    mock_save_rds: MagicMock,
    mock_download_rds: AsyncMock,
    geus_dataverse_bronze: GEUSDataversePesticidesBronze,
) -> None:
    """Test successful run of the pipeline downloading both pest and pfas files."""
    mock_pest_content = b"test pest rds content bytes"
    mock_pfas_content = b"test pfas rds content bytes"
    mock_download_rds.side_effect = [mock_pest_content, mock_pfas_content]
    mock_save_rds.side_effect = [
        "bronze/test_geus_dataverse_pesticides/2024-01-15/AM_pest.rds",
        "bronze/test_geus_dataverse_pesticides/2024-01-15/AM_pfas.rds",
    ]

    # Mock gcs_access
    geus_dataverse_bronze.gcs_access = MagicMock()
    geus_dataverse_bronze.date_pattern = "2024-01-15T10-00-00"

    result = await geus_dataverse_bronze.run()

    assert result is not None
    assert result["dataset"] == "test_geus_dataverse_pesticides"
    assert result["source_crs"] == "EPSG:25832"
    # Backwards compatible field
    assert "rds_path" in result
    # New fields for both files
    assert "pest_rds_path" in result
    assert "pfas_rds_path" in result
    assert result["pest_file_size_bytes"] == len(mock_pest_content)
    assert result["pfas_file_size_bytes"] == len(mock_pfas_content)

    assert mock_download_rds.call_count == 2
    assert mock_save_rds.call_count == 2


@pytest.mark.asyncio
@patch(
    "unified_pipeline.bronze.geus_dataverse_pesticides.GEUSDataversePesticidesBronze._download_rds_file"
)
@patch("aiohttp.ClientSession")
@patch("aiohttp.TCPConnector")
async def test_run_download_error(
    mock_tcp_connector: MagicMock,
    mock_client_session: MagicMock,
    mock_download_rds: AsyncMock,
    geus_dataverse_bronze: GEUSDataversePesticidesBronze,
) -> None:
    """Test run with error in downloading .rds file."""
    mock_download_rds.side_effect = Exception("Download failed")

    with pytest.raises(Exception) as excinfo:
        await geus_dataverse_bronze.run()

    assert "Download failed" in str(excinfo.value)


def test_source_crs_tracking(
    geus_dataverse_bronze: GEUSDataversePesticidesBronze,
) -> None:
    """Test that source CRS is properly tracked."""
    # Initially from config
    assert geus_dataverse_bronze.get_source_crs() == "EPSG:25832"

    # Can be set explicitly
    geus_dataverse_bronze.set_source_crs("EPSG:4326")
    assert geus_dataverse_bronze.get_source_crs() == "EPSG:4326"
