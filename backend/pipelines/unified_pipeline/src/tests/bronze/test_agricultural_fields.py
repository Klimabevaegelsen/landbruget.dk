"""
Tests for the AgriculturalFieldsBronze class.
"""

import json
from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from tenacity import stop_after_attempt

from unified_pipeline.bronze.agricultural_fields import (
    AgriculturalFieldsBronze,
    AgriculturalFieldsBronzeConfig,
)
from unified_pipeline.util.gcs_util import GCSUtil


@pytest.fixture
def mock_gcs_util() -> MagicMock:
    """Return a mock GCSUtil instance."""
    mock_gcs = MagicMock(spec=GCSUtil)
    mock_gcs.upload_blob = MagicMock()
    return mock_gcs


@pytest.fixture
def config() -> AgriculturalFieldsBronzeConfig:
    """Return a test configuration."""
    return AgriculturalFieldsBronzeConfig(
        name="Test Fields",
        type="test",
        description="Test data",
        fields_url="https://test.example/fields",
        blocks_url="https://test.example/blocks",
        fields_dataset="test_fields",
        blocks_dataset="test_blocks",
        bucket="test-bucket",
        batch_size=1000,
        max_concurrent=2,
    )


@pytest.fixture
def agricultural_fields_bronze(
    config: AgriculturalFieldsBronzeConfig, mock_gcs_util: MagicMock
) -> AgriculturalFieldsBronze:
    """Return a test AgriculturalFieldsBronze instance."""
    source = AgriculturalFieldsBronze(config, mock_gcs_util)
    source.log = MagicMock()
    return source


def get_async_mock_session(response: AsyncMock) -> MagicMock:
    """
    Create a mock aiohttp session.
    """

    class MockGetContextManager:
        async def __aenter__(self) -> AsyncMock:
            return response

        async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
            return None

    mock_session = AsyncMock()
    mock_session.get = MagicMock(return_value=MockGetContextManager())
    return mock_session


@pytest.mark.asyncio
async def test_get_total_count_success(
    agricultural_fields_bronze: AgriculturalFieldsBronze,
) -> None:
    """Test getting total count with successful response."""

    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.json = AsyncMock(return_value={"count": 5000})
    mock_session = get_async_mock_session(mock_response)

    result = await agricultural_fields_bronze._get_total_count(mock_session, "https://test.url")

    assert result == 5000
    mock_session.get.assert_called_once()


@pytest.mark.asyncio
async def test_get_total_count_error_status(
    agricultural_fields_bronze: AgriculturalFieldsBronze,
) -> None:
    """Test getting total count with error response status."""

    mock_response = AsyncMock()
    mock_response.status = 500
    mock_response.json = AsyncMock(return_value={"error": "Server error"})
    mock_session = get_async_mock_session(mock_response)

    with pytest.raises(Exception, match="Error getting count for"):
        await agricultural_fields_bronze._get_total_count(mock_session, "https://test.url")


@pytest.mark.asyncio
async def test_get_total_count_exception(
    agricultural_fields_bronze: AgriculturalFieldsBronze,
) -> None:
    """Test getting total count when an exception occurs."""

    mock_session = AsyncMock()
    mock_session.get.side_effect = Exception("Connection error")

    with pytest.raises(Exception, match="Error getting total count for"):
        await agricultural_fields_bronze._get_total_count(mock_session, "https://test.url")


@pytest.mark.asyncio
async def test_fetch_chunk_success(agricultural_fields_bronze: AgriculturalFieldsBronze) -> None:
    """Test fetching a chunk with successful response."""

    test_features: Dict[str, Any] = {
        "features": [
            {"id": 1, "attributes": {"name": "Field1"}, "geometry": {"rings": [[[1, 1], [2, 2]]]}},
            {"id": 2, "attributes": {"name": "Field2"}, "geometry": {"rings": [[[3, 3], [4, 4]]]}},
        ]
    }
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.json = AsyncMock(return_value=test_features)
    mock_response.text = AsyncMock(return_value=json.dumps(test_features))
    mock_session = get_async_mock_session(mock_response)

    result = await agricultural_fields_bronze._fetch_chunk(mock_session, "https://test.url", 0)

    assert result == json.dumps(test_features)
    mock_session.get.assert_called_once()


@pytest.mark.asyncio
@patch(
    "unified_pipeline.bronze.agricultural_fields.AgriculturalFieldsBronze._fetch_chunk.retry.stop",
    stop_after_attempt(1),
)
async def test_fetch_chunk_error(agricultural_fields_bronze: AgriculturalFieldsBronze) -> None:
    """Test fetching a chunk with error response."""

    mock_response = AsyncMock()
    mock_response.status = 500
    mock_response.json = AsyncMock(return_value={"error": "Server error"})
    mock_response.text = AsyncMock(return_value=json.dumps({"error": "Server error"}))
    mock_session = get_async_mock_session(mock_response)

    with pytest.raises(Exception) as excinfo:
        await agricultural_fields_bronze._fetch_chunk(mock_session, "https://test.url", 0)
        assert "Error response 500" in str(excinfo.value)


@pytest.mark.asyncio
@patch.object(AgriculturalFieldsBronze, "_get_total_count")
@patch.object(AgriculturalFieldsBronze, "_fetch_chunk")
async def test_process_data(
    mock_fetch_chunk: AsyncMock,
    mock_get_total_count: AsyncMock,
    agricultural_fields_bronze: AgriculturalFieldsBronze,
) -> None:
    """Test processing data with successful responses."""
    mock_get_total_count.return_value = 1500
    mock_fetch_chunk.side_effect = [
        json.dumps(
            {
                "features": [
                    {"attributes": {"name": "Field1"}, "geometry": {"rings": [[[1, 1], [2, 2]]]}},
                    {"attributes": {"name": "Field2"}, "geometry": {"rings": [[[3, 3], [4, 4]]]}},
                ]
            }
        ),
        json.dumps(
            {
                "features": [
                    {"attributes": {"name": "Field3"}, "geometry": {"rings": [[[5, 5], [6, 6]]]}},
                    {"attributes": {"name": "Field4"}, "geometry": {"rings": [[[7, 7], [8, 8]]]}},
                ]
            }
        ),
    ]

    mock_client_session = AsyncMock()
    mock_client_session.get = AsyncMock(return_value=mock_fetch_chunk)
    mock_client_session.get.return_value.__aenter__.return_value = mock_client_session
    mock_client_session.get.return_value.__aexit__.return_value = None
    mock_client_session.get.return_value.status = 200

    # Mock the save_raw_data method using patch.object
    with (
        patch.object(AgriculturalFieldsBronze, "_save_raw_data") as mock_save_raw_data,
        patch("aiohttp.ClientSession", return_value=mock_client_session),
    ):
        await agricultural_fields_bronze._process_data("https://test.url", "test_fields")

        mock_get_total_count.assert_called_once()

        assert mock_fetch_chunk.call_count == 2
        mock_save_raw_data.assert_called_once()


@pytest.mark.asyncio
async def test_process_data_when_total_count_is_zero(
    agricultural_fields_bronze: AgriculturalFieldsBronze,
) -> None:
    """Test processing data when total count is zero."""
    agricultural_fields_bronze._get_total_count = AsyncMock(return_value=0)  # type: ignore[method-assign]
    agricultural_fields_bronze._fetch_chunk = AsyncMock()  # type: ignore[method-assign]

    await agricultural_fields_bronze._process_data("https://test.url", "test_fields")

    agricultural_fields_bronze._fetch_chunk.assert_not_called()


@pytest.mark.asyncio
async def test_run_success(agricultural_fields_bronze: AgriculturalFieldsBronze) -> None:
    """Test running with successful processing."""
    agricultural_fields_bronze._process_data = AsyncMock()  # type: ignore[method-assign]

    await agricultural_fields_bronze.run()

    assert (
        agricultural_fields_bronze._process_data.call_count == 2
    )  # Should be called for fields and blocks


@pytest.mark.asyncio
async def test_run_with_exception(agricultural_fields_bronze: AgriculturalFieldsBronze) -> None:
    """Test run method handling exceptions."""
    agricultural_fields_bronze._process_data = AsyncMock(side_effect=Exception("Processing failed"))  # type: ignore[method-assign]

    # Act & Assert
    with pytest.raises(Exception, match="Processing failed"):
        await agricultural_fields_bronze.run()
