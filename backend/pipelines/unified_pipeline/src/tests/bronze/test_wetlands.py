"""
Tests for the WetlandsBronze class.
"""

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from tenacity import stop_after_attempt

from unified_pipeline.bronze.wetlands import WetlandsBronze, WetlandsBronzeConfig
from unified_pipeline.util.gcs_util import GCSUtil


@pytest.fixture
def mock_gcs_util() -> MagicMock:
    """Return a mock GCSUtil instance."""
    mock_gcs = MagicMock(spec=GCSUtil)
    return mock_gcs


@pytest.fixture
def config() -> WetlandsBronzeConfig:
    """Return a test configuration."""
    return WetlandsBronzeConfig(
        name="Test Wetlands Map",
        dataset="test_wetlands",
        bucket="test-bucket",
        url="https://test.example.com/wfs",
        batch_size=1000,
        max_concurrent=2,
        storage_batch_size=500,
    )


@pytest.fixture
def wetlands_bronze(config: WetlandsBronzeConfig, mock_gcs_util: MagicMock) -> WetlandsBronze:
    """Return a test WetlandsBronze instance."""
    source = WetlandsBronze(config, mock_gcs_util)
    source.log = MagicMock()
    return source


@pytest.fixture
def mock_response() -> MagicMock:
    """Return a mock aiohttp response."""
    mock_resp = MagicMock()
    mock_resp.status = 200
    mock_resp.text = AsyncMock(
        return_value="""
        <wfs:FeatureCollection 
            xmlns:wfs="http://www.opengis.net/wfs/2.0" 
            numberMatched="2000" 
            numberReturned="1000">
            <member>Feature 1</member>
            <member>Feature 2</member>
        </wfs:FeatureCollection>
        """
    )
    return mock_resp


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


def test_get_params(wetlands_bronze: WetlandsBronze) -> None:
    """Test generating request parameters."""
    params = wetlands_bronze._get_params(500)

    assert params["SERVICE"] == "WFS"
    assert params["REQUEST"] == "GetFeature"
    assert params["TYPENAMES"] == "natur:kulstof2022"
    assert params["STARTINDEX"] == "500"
    assert params["COUNT"] == str(wetlands_bronze.config.batch_size)
    assert params["SRSNAME"] == "urn:ogc:def:crs:EPSG::25832"


@pytest.mark.asyncio
async def test_fetch_chunck_success(wetlands_bronze: WetlandsBronze) -> None:
    xml_response = '<wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs/2.0" numberMatched="1" numberReturned="1"><wfs:member></wfs:member></wfs:FeatureCollection>'  # noqa: E501

    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.text.return_value = xml_response

    mock_session = get_async_mock_session(mock_response)

    result = await wetlands_bronze._fetch_chunck(mock_session, 0)

    assert result["text"] == xml_response
    assert result["start_index"] == 0
    assert result["total_features"] == 1
    assert result["returned_features"] == 1

    mock_session.get.assert_called_once_with(
        wetlands_bronze.config.url,
        params=wetlands_bronze._get_params(0),
    )


@pytest.mark.asyncio
@patch(
    "unified_pipeline.bronze.wetlands.WetlandsBronze._fetch_chunck.retry.stop",
    stop_after_attempt(1),
)
async def test_fetch_chunck_http_error(wetlands_bronze: WetlandsBronze) -> None:
    mock_response = AsyncMock()
    mock_response.status = 500

    mock_session = get_async_mock_session(mock_response)

    with pytest.raises(Exception) as excinfo:
        await wetlands_bronze._fetch_chunck(mock_session, 0)
        assert "Failed to fetch data. Status: 500" in str(excinfo.value)


@pytest.mark.asyncio
@patch(
    "unified_pipeline.bronze.wetlands.WetlandsBronze._fetch_chunck.retry.stop",
    stop_after_attempt(1),
)
async def test_fetch_chunck_xml_parse_error(wetlands_bronze: WetlandsBronze) -> None:
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.text = AsyncMock(return_value="<invalid_xml>")

    mock_session = get_async_mock_session(mock_response)

    with pytest.raises(Exception) as excinfo:
        await wetlands_bronze._fetch_chunck(mock_session, 0)
        assert "Failed to parse XML response" in str(excinfo.value)


@pytest.mark.asyncio
@patch("unified_pipeline.bronze.wetlands.WetlandsBronze._fetch_chunck")
@patch("aiohttp.ClientSession")
@patch("aiohttp.TCPConnector")
async def test_fetch_raw_data_success(
    mock_tcp_connector: MagicMock,
    mock_client_session: MagicMock,
    mock_fetch_chunck: AsyncMock,
    wetlands_bronze: WetlandsBronze,
) -> None:
    """Test successful fetching of all raw data."""
    # Mock the first chunk (initial request)
    mock_fetch_chunck.side_effect = [
        {
            "text": "<xml>chunk1</xml>",
            "start_index": 0,
            "total_features": 2000,
            "returned_features": 1000,
        },
        {
            "text": "<xml>chunk2</xml>",
            "start_index": 1000,
            "total_features": 2000,
            "returned_features": 1000,
        },
    ]

    result = await wetlands_bronze._fetch_raw_data()

    assert result is not None
    assert len(result) == 2
    assert result[0] == "<xml>chunk1</xml>"
    assert result[1] == "<xml>chunk2</xml>"


@pytest.mark.asyncio
@patch("unified_pipeline.bronze.wetlands.WetlandsBronze._fetch_chunck")
@patch("aiohttp.ClientSession")
@patch("aiohttp.TCPConnector")
async def test_fetch_raw_data_error(
    mock_tcp_connector: MagicMock,
    mock_client_session: MagicMock,
    mock_fetch_chunck: AsyncMock,
    wetlands_bronze: WetlandsBronze,
) -> None:
    """Test error handling when fetching raw data."""
    # First call succeeds, second call fails
    mock_fetch_chunck.side_effect = [
        {
            "text": "<xml>chunk1</xml>",
            "start_index": 0,
            "total_features": 2000,
            "returned_features": 1000,
        },
        Exception("Test error"),
    ]

    with pytest.raises(Exception):
        await wetlands_bronze._fetch_raw_data()


@pytest.mark.asyncio
@patch("unified_pipeline.bronze.wetlands.WetlandsBronze._fetch_raw_data")
async def test_run_success(mock_fetch_raw_data: AsyncMock, wetlands_bronze: WetlandsBronze) -> None:
    """Test successful run of the pipeline."""
    mock_fetch_raw_data.return_value = ["<xml>data</xml>"]
    wetlands_bronze._save_raw_data = MagicMock()

    await wetlands_bronze.run()

    mock_fetch_raw_data.assert_called_once()
    wetlands_bronze._save_raw_data.assert_called_once_with(
        ["<xml>data</xml>"],
        wetlands_bronze.config.dataset,
        wetlands_bronze.config.name,
        wetlands_bronze.config.bucket,
    )


@pytest.mark.asyncio
@patch("unified_pipeline.bronze.wetlands.WetlandsBronze._fetch_raw_data")
async def test_run_fetch_error(
    mock_fetch_raw_data: AsyncMock, wetlands_bronze: WetlandsBronze
) -> None:
    """Test run with error in fetching raw data."""
    mock_fetch_raw_data.return_value = None
    wetlands_bronze._save_raw_data = MagicMock()

    await wetlands_bronze.run()

    mock_fetch_raw_data.assert_called_once()
    wetlands_bronze._save_raw_data.assert_not_called()


@pytest.mark.asyncio
@patch("unified_pipeline.bronze.wetlands.WetlandsBronze._fetch_raw_data")
async def test_run_exception(
    mock_fetch_raw_data: AsyncMock, wetlands_bronze: WetlandsBronze
) -> None:
    """Test run with exception during fetching."""
    mock_fetch_raw_data.side_effect = Exception("Test error")
    wetlands_bronze._save_raw_data = MagicMock()

    with pytest.raises(Exception):
        await wetlands_bronze.run()

    mock_fetch_raw_data.assert_called_once()
    wetlands_bronze._save_raw_data.assert_not_called()
