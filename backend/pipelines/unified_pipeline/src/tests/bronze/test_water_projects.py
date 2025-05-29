import json
import xml.etree.ElementTree as ET
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pandas as pd
import pytest
from tenacity import stop_after_attempt

from unified_pipeline.bronze.water_projects import WaterProjectsBronze, WaterProjectsBronzeConfig
from unified_pipeline.util.gcs_util import GCSUtil


@pytest.fixture
def mock_gcs_util() -> MagicMock:
    return MagicMock(spec=GCSUtil)


@pytest.fixture
def config() -> WaterProjectsBronzeConfig:
    return WaterProjectsBronzeConfig()


@pytest.fixture
def water_projects_bronze(
    config: WaterProjectsBronzeConfig, mock_gcs_util: MagicMock
) -> WaterProjectsBronze:
    return WaterProjectsBronze(config, mock_gcs_util)


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


def test_water_projects_bronze_config() -> None:
    """Test the WaterProjectsBronzeConfig has expected default values."""
    config = WaterProjectsBronzeConfig()
    assert config.name == "Danish Water Projects Map"
    assert config.dataset == "water_projects"
    assert config.type == "wfs"
    assert config.url == "https://geodata.fvm.dk/geoserver/wfs"
    assert config.bucket == "landbrugsdata-raw-data"
    assert config.batch_size == 100
    assert config.max_concurrent == 3
    assert len(config.layers) > 0
    assert isinstance(config.url_mapping, dict)
    assert isinstance(config.service_types, dict)


def test_get_params(water_projects_bronze: WaterProjectsBronze) -> None:
    """Test the _get_params method returns correct parameter dictionary."""
    layer = "N2000_projekter:Hydrologi_E"
    params = water_projects_bronze._get_params(layer, start_index=10)

    assert params["SERVICE"] == "WFS"
    assert params["REQUEST"] == "GetFeature"
    assert params["VERSION"] == "2.0.0"
    assert params["TYPENAMES"] == layer
    assert params["STARTINDEX"] == "10"
    assert params["COUNT"] == str(water_projects_bronze.config.batch_size)
    assert params["SRSNAME"] == "urn:ogc:def:crs:EPSG::25832"


@pytest.mark.asyncio
async def test_fetch_chunk_success(water_projects_bronze: WaterProjectsBronze) -> None:
    """Test the _fetch_chunk method successfully fetches data."""
    xml_response = '<wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs/2.0" numberMatched="5" numberReturned="2"><wfs:member></wfs:member></wfs:FeatureCollection>'  # noqa: E501

    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.text = AsyncMock(return_value=xml_response)

    mock_session = get_async_mock_session(mock_response)
    layer = "N2000_projekter:Hydrologi_E"
    url = water_projects_bronze.config.url

    result = await water_projects_bronze._fetch_chunk(mock_session, layer, url, 0)

    assert result["text"] == xml_response
    assert result["start_index"] == 0
    assert result["total_features"] == 5
    assert result["returned_features"] == 2

    mock_session.get.assert_called_once_with(
        url,
        params=water_projects_bronze._get_params(layer, 0),
    )


@pytest.mark.asyncio
@patch(
    "unified_pipeline.bronze.water_projects.WaterProjectsBronze._fetch_chunk.retry.stop",
    stop_after_attempt(1),
)
async def test_fetch_chunk_http_error(water_projects_bronze: WaterProjectsBronze) -> None:
    """Test the _fetch_chunk method handles HTTP errors."""
    mock_response = AsyncMock()
    mock_response.status = 500
    mock_response.text = AsyncMock(return_value="Internal Server Error")

    mock_session = get_async_mock_session(mock_response)
    layer = "N2000_projekter:Hydrologi_E"
    url = water_projects_bronze.config.url

    # Patch the retry decorator for this test to prevent retries
    with patch("unified_pipeline.bronze.water_projects.retry", lambda **kwargs: lambda f: f):
        with pytest.raises(Exception) as excinfo:
            await water_projects_bronze._fetch_chunk(mock_session, layer, url, 0)
            assert "Failed to fetch data. Status: 500" in str(excinfo.value)


@pytest.mark.asyncio
@patch(
    "unified_pipeline.bronze.water_projects.WaterProjectsBronze._fetch_chunk.retry.stop",
    stop_after_attempt(1),
)
async def test_fetch_chunk_xml_parse_error(water_projects_bronze: WaterProjectsBronze) -> None:
    """Test the _fetch_chunk method handles XML parsing errors."""
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.text = AsyncMock(return_value="<invalid_xml>")

    mock_session = get_async_mock_session(mock_response)
    layer = "N2000_projekter:Hydrologi_E"
    url = water_projects_bronze.config.url

    # Patch the retry decorator for this test to prevent retries
    with patch("unified_pipeline.bronze.water_projects.retry", lambda **kwargs: lambda f: f):
        with pytest.raises(Exception) as excinfo:
            await water_projects_bronze._fetch_chunk(mock_session, layer, url, 0)
            assert "Failed to parse XML response" in str(excinfo.value)


@pytest.mark.asyncio
@patch(
    "unified_pipeline.bronze.water_projects.WaterProjectsBronze._fetch_chunk.retry.stop",
    stop_after_attempt(1),
)
async def test_fetch_chunk_unicode_decode_error(water_projects_bronze: WaterProjectsBronze) -> None:
    """Test the _fetch_chunk method handles Unicode decode errors."""
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.text = AsyncMock(
        side_effect=[UnicodeDecodeError("utf-8", b"", 0, 1, "invalid"), "<xml>replaced</xml>"]
    )

    mock_session = get_async_mock_session(mock_response)
    layer = "N2000_projekter:Hydrologi_E"
    url = water_projects_bronze.config.url

    result = await water_projects_bronze._fetch_chunk(mock_session, layer, url, 0)
    assert result["text"] == "<xml>replaced</xml>"


@pytest.mark.asyncio
async def test_fetch_arcgis_data_success(water_projects_bronze: WaterProjectsBronze) -> None:
    """Test the _fetch_arcgis_data method successfully fetches data."""
    json_data = {"features": [{"attributes": {"id": 1}}, {"attributes": {"id": 2}}]}

    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.json = AsyncMock(return_value=json_data)
    mock_response.text = AsyncMock(return_value=json.dumps(json_data))

    mock_session = get_async_mock_session(mock_response)
    layer = "Klima_lavbund_demarkation___offentlige_projekter:0"
    url = "https://gis.nst.dk/server/rest/services/autonom/Klima_lavbund_demarkation___offentlige_projekter/FeatureServer"

    result = await water_projects_bronze._fetch_arcgis_data(mock_session, layer, url)

    assert len(result) == 1
    assert json.loads(result[0]) == json_data

    # Verify the URL was constructed correctly with the layer ID
    mock_session.get.assert_called_once()
    args, kwargs = mock_session.get.call_args
    assert args[0] == f"{url}/0/query"
    assert kwargs["params"]["f"] == "json"


@pytest.mark.asyncio
@patch(
    "unified_pipeline.bronze.water_projects.WaterProjectsBronze._fetch_arcgis_data.retry.stop",
    stop_after_attempt(1),
)
async def test_fetch_arcgis_data_http_error(water_projects_bronze: WaterProjectsBronze) -> None:
    """Test the _fetch_arcgis_data method handles HTTP errors."""
    mock_response = AsyncMock()
    mock_response.status = 404
    mock_response.text = AsyncMock(return_value="Not Found")

    mock_session = get_async_mock_session(mock_response)
    layer = "Klima_lavbund_demarkation___offentlige_projekter:0"
    url = "https://gis.nst.dk/server/rest/services/autonom/Klima_lavbund_demarkation___offentlige_projekter/FeatureServer"

    with pytest.raises(Exception) as excinfo:
        await water_projects_bronze._fetch_arcgis_data(mock_session, layer, url)
        assert "Failed to fetch ArcGIS data. Status: 404" in str(excinfo.value)


@pytest.mark.asyncio
@patch(
    "unified_pipeline.bronze.water_projects.WaterProjectsBronze._fetch_arcgis_data.retry.stop",
    stop_after_attempt(1),
)
async def test_fetch_arcgis_data_unicode_decode_error(
    water_projects_bronze: WaterProjectsBronze,
) -> None:
    """Test the _fetch_arcgis_data method handles Unicode decode errors."""
    json_data = {"features": [{"attributes": {"id": 1}}]}

    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.json = AsyncMock(return_value=json_data)
    mock_response.text = AsyncMock(
        side_effect=[UnicodeDecodeError("utf-8", b"", 0, 1, "invalid"), json.dumps(json_data)]
    )

    mock_session = get_async_mock_session(mock_response)
    layer = "Klima_lavbund_demarkation___offentlige_projekter:0"
    url = "https://gis.nst.dk/server/rest/services/autonom/Klima_lavbund_demarkation___offentlige_projekter/FeatureServer"

    result = await water_projects_bronze._fetch_arcgis_data(mock_session, layer, url)

    assert len(result) == 1
    assert json.loads(result[0]) == json_data


@pytest.mark.asyncio
async def test_fetch_wfs_data_single_batch(water_projects_bronze: WaterProjectsBronze) -> None:
    """Test the _fetch_wfs_data method with a single batch of data."""
    # Modify the batch size to ensure we get a single batch
    small_config = water_projects_bronze.config.model_copy(update={"batch_size": 10})
    water_projects_bronze_small = WaterProjectsBronze(small_config, water_projects_bronze.gcs_util)

    # Set up mock for _fetch_chunk
    xml_content = (
        '<wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs/2.0" '
        'numberMatched="5" numberReturned="5"><wfs:member></wfs:member></wfs:FeatureCollection>'
    )

    mock_fetch_chunk = AsyncMock(
        return_value={
            "text": xml_content,
            "start_index": 0,
            "total_features": 5,
            "returned_features": 5,
        }
    )

    with patch.object(water_projects_bronze_small, "_fetch_chunk", mock_fetch_chunk):
        mock_session = AsyncMock()
        layer = "N2000_projekter:Hydrologi_E"
        url = water_projects_bronze.config.url

        result = await water_projects_bronze_small._fetch_wfs_data(mock_session, layer, url)

        assert result is not None
        assert len(result) == 1
        assert result[0] == xml_content
        mock_fetch_chunk.assert_called_once_with(mock_session, layer, url, 0)


@pytest.mark.asyncio
async def test_fetch_wfs_data_multiple_batches(water_projects_bronze: WaterProjectsBronze) -> None:
    """Test the _fetch_wfs_data method with multiple batches of data."""
    # Modify the batch size to ensure we get multiple batches
    small_config = water_projects_bronze.config.model_copy(update={"batch_size": 2})
    water_projects_bronze_small = WaterProjectsBronze(small_config, water_projects_bronze.gcs_util)

    # Define responses for each chunk
    chunk_responses = [
        {
            "text": "<wfs:FeatureCollection numberMatched=5 numberReturned=2><wfs:member>1</wfs:member><wfs:member>2</wfs:member></wfs:FeatureCollection>",  # noqa: E501
            "start_index": 0,
            "total_features": 5,
            "returned_features": 2,
        },
        {
            "text": "<wfs:FeatureCollection numberMatched=5 numberReturned=2><wfs:member>3</wfs:member><wfs:member>4</wfs:member></wfs:FeatureCollection>",  # noqa: E501
            "start_index": 2,
            "total_features": 5,
            "returned_features": 2,
        },
        {
            "text": "<wfs:FeatureCollection numberMatched=5 numberReturned=1><wfs:member>5</wfs:member></wfs:FeatureCollection>",  # noqa: E501
            "start_index": 4,
            "total_features": 5,
            "returned_features": 1,
        },
    ]

    # Create a mock implementation that returns the appropriate response based on start_index
    async def mock_fetch_chunk(
        session: aiohttp.ClientSession, layer: str, url: str, start_index: int
    ) -> dict:
        for response in chunk_responses:
            if response["start_index"] == start_index:
                return response
        return chunk_responses[0]  # Fallback

    with patch.object(water_projects_bronze_small, "_fetch_chunk", side_effect=mock_fetch_chunk):
        mock_session = AsyncMock()
        layer = "N2000_projekter:Hydrologi_E"
        url = water_projects_bronze.config.url

        result = await water_projects_bronze_small._fetch_wfs_data(mock_session, layer, url)

        assert result is not None
        assert len(result) == 3
        assert result[0] == chunk_responses[0]["text"]
        assert result[1] == chunk_responses[1]["text"]
        assert result[2] == chunk_responses[2]["text"]


@pytest.mark.asyncio
async def test_fetch_wfs_data_with_error(water_projects_bronze: WaterProjectsBronze) -> None:
    """Test the _fetch_wfs_data method handles errors during fetching."""
    # Set up mock for _fetch_chunk
    mock_fetch_chunk = AsyncMock(side_effect=Exception("Fetch error"))

    with patch.object(water_projects_bronze, "_fetch_chunk", mock_fetch_chunk):
        mock_session = AsyncMock()
        layer = "N2000_projekter:Hydrologi_E"
        url = water_projects_bronze.config.url

        with pytest.raises(Exception) as excinfo:
            await water_projects_bronze._fetch_wfs_data(mock_session, layer, url)
        assert "Fetch error" in str(excinfo.value)


@pytest.mark.asyncio
@patch("unified_pipeline.bronze.water_projects.aiohttp.ClientSession")
@patch("unified_pipeline.bronze.water_projects.aiohttp.TCPConnector")
async def test_fetch_raw_data_success(
    mock_tcp_connector: MagicMock,
    mock_client_session: MagicMock,
    water_projects_bronze: WaterProjectsBronze,
) -> None:
    """Test the _fetch_raw_data method successfully fetches data for all layers."""
    # Override layers to reduce test complexity
    test_config = water_projects_bronze.config.model_copy(
        update={
            "layers": [
                "N2000_projekter:Hydrologi_E",
                "Klima_lavbund_demarkation___offentlige_projekter:0",
            ],
            "service_types": {"Klima_lavbund_demarkation___offentlige_projekter:0": "arcgis"},
        }
    )
    water_projects_bronze_test = WaterProjectsBronze(test_config, water_projects_bronze.gcs_util)

    # Mock for WFS data
    wfs_data = ["<wfs:FeatureCollection>WFS Data</wfs:FeatureCollection>"]
    # Mock for ArcGIS data
    arcgis_data = ['{"features": [{"attributes": {"id": 1}}]}']

    # Mock the specific data fetching methods
    with (
        patch.object(
            water_projects_bronze_test, "_fetch_wfs_data", AsyncMock(return_value=wfs_data)
        ),
        patch.object(
            water_projects_bronze_test, "_fetch_arcgis_data", AsyncMock(return_value=arcgis_data)
        ),
    ):
        mock_session_instance = AsyncMock()
        mock_client_session.return_value.__aenter__.return_value = mock_session_instance

        result = await water_projects_bronze_test._fetch_raw_data()

        assert result is not None
        assert len(result) == 2
        # First layer with WFS data
        assert result[0] == ("N2000_projekter:Hydrologi_E", wfs_data[0])
        # Second layer with ArcGIS data
        assert result[1] == ("Klima_lavbund_demarkation___offentlige_projekter:0", arcgis_data[0])


@pytest.mark.asyncio
@patch("unified_pipeline.bronze.water_projects.aiohttp.ClientSession")
@patch("unified_pipeline.bronze.water_projects.aiohttp.TCPConnector")
async def test_fetch_raw_data_empty_result(
    mock_tcp_connector: MagicMock,
    mock_client_session: MagicMock,
    water_projects_bronze: WaterProjectsBronze,
) -> None:
    """Test the _fetch_raw_data method handles empty results."""
    # Override layers for test
    test_config = water_projects_bronze.config.model_copy(
        update={"layers": ["N2000_projekter:Hydrologi_E"]}
    )
    water_projects_bronze_test = WaterProjectsBronze(test_config, water_projects_bronze.gcs_util)

    # Mock the WFS data method to return empty data
    with patch.object(water_projects_bronze_test, "_fetch_wfs_data", AsyncMock(return_value=[])):
        mock_session_instance = AsyncMock()
        mock_client_session.return_value.__aenter__.return_value = mock_session_instance

        result = await water_projects_bronze_test._fetch_raw_data()

        assert result is None


@pytest.mark.asyncio
@patch("unified_pipeline.bronze.water_projects.aiohttp.ClientSession")
@patch("unified_pipeline.bronze.water_projects.aiohttp.TCPConnector")
async def test_fetch_raw_data_with_error(
    mock_tcp_connector: MagicMock,
    mock_client_session: MagicMock,
    water_projects_bronze: WaterProjectsBronze,
) -> None:
    """Test the _fetch_raw_data method handles errors."""
    # Override layers for test
    test_config = water_projects_bronze.config.model_copy(
        update={"layers": ["N2000_projekter:Hydrologi_E"]}
    )
    water_projects_bronze_test = WaterProjectsBronze(test_config, water_projects_bronze.gcs_util)

    # Mock the WFS data method to raise an exception
    with patch.object(
        water_projects_bronze_test,
        "_fetch_wfs_data",
        AsyncMock(side_effect=Exception("Fetch error")),
    ):
        mock_session_instance = AsyncMock()
        mock_client_session.return_value.__aenter__.return_value = mock_session_instance

        with pytest.raises(Exception) as excinfo:
            await water_projects_bronze_test._fetch_raw_data()
        assert "Fetch error" in str(excinfo.value)


def test_create_dataframe(water_projects_bronze: WaterProjectsBronze) -> None:
    """Test the create_dataframe method converts raw data to a DataFrame with metadata."""
    raw_data = [
        ("N2000_projekter:Hydrologi_E", "<xml>data1</xml>"),
        ("Vandprojekter:Fosfor_E_samlet", "<xml>data2</xml>"),
        ("Klima_lavbund_demarkation___offentlige_projekter:0", '{"features": [{"id": 1}]}'),
    ]

    result_df = water_projects_bronze.create_dataframe(raw_data)

    assert isinstance(result_df, pd.DataFrame)
    assert set(result_df.columns) == {"payload", "layer", "source", "created_at", "updated_at"}
    assert len(result_df) == 3

    # Check payload and layer columns match input data
    assert result_df["payload"].tolist() == [
        "<xml>data1</xml>",
        "<xml>data2</xml>",
        '{"features": [{"id": 1}]}',
    ]
    assert result_df["layer"].tolist() == [
        "N2000_projekter:Hydrologi_E",
        "Vandprojekter:Fosfor_E_samlet",
        "Klima_lavbund_demarkation___offentlige_projekter:0",
    ]

    # Check other metadata columns
    assert all(result_df["source"] == water_projects_bronze.config.name)
    assert all(isinstance(ts, pd.Timestamp) for ts in result_df["created_at"])
    assert all(isinstance(ts, pd.Timestamp) for ts in result_df["updated_at"])


@pytest.mark.asyncio
async def test_run_success(water_projects_bronze: WaterProjectsBronze) -> None:
    """Test the run method successfully completes the job."""
    raw_data = [("N2000_projekter:Hydrologi_E", "<xml>data</xml>")]

    # Mock the methods
    water_projects_bronze._fetch_raw_data = AsyncMock(return_value=raw_data)  # type: ignore[method-assign]
    water_projects_bronze._save_raw_data = MagicMock()  # type: ignore[method-assign]

    await water_projects_bronze.run()

    water_projects_bronze._fetch_raw_data.assert_called_once()
    water_projects_bronze._save_raw_data.assert_called_once()

    # Verify the DataFrame was created correctly
    args, kwargs = water_projects_bronze._save_raw_data.call_args
    df = args[0]
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df["payload"].iloc[0] == "<xml>data</xml>"
    assert df["layer"].iloc[0] == "N2000_projekter:Hydrologi_E"

    # Verify the correct dataset and bucket were used
    assert args[1] == water_projects_bronze.config.dataset
    assert args[2] == water_projects_bronze.config.bucket


@pytest.mark.asyncio
async def test_run_no_data(water_projects_bronze: WaterProjectsBronze) -> None:
    """Test the run method handles no data scenario."""
    # Mock the methods
    water_projects_bronze._fetch_raw_data = AsyncMock(return_value=None)  # type: ignore[method-assign]
    water_projects_bronze._save_raw_data = MagicMock()  # type: ignore[method-assign]

    await water_projects_bronze.run()

    water_projects_bronze._fetch_raw_data.assert_called_once()
    water_projects_bronze._save_raw_data.assert_not_called()


@pytest.mark.asyncio
async def test_run_with_fetch_error(water_projects_bronze: WaterProjectsBronze) -> None:
    """Test the run method handles fetch errors."""
    # Mock the methods to raise an exception
    water_projects_bronze._fetch_raw_data = AsyncMock(side_effect=Exception("Fetch error"))  # type: ignore[method-assign]
    water_projects_bronze._save_raw_data = MagicMock()  # type: ignore[method-assign]

    with pytest.raises(Exception) as excinfo:
        await water_projects_bronze.run()

    assert "Fetch error" in str(excinfo.value)
    water_projects_bronze._fetch_raw_data.assert_called_once()
    water_projects_bronze._save_raw_data.assert_not_called()


@pytest.mark.asyncio
@patch("unified_pipeline.bronze.water_projects.wait_exponential")
async def test_fetch_chunk_retry_behavior(
    mock_wait: MagicMock, water_projects_bronze: WaterProjectsBronze
) -> None:
    """Test the retry behavior of the _fetch_chunk method."""
    # Create a mock response that fails twice and succeeds on the third try
    mock_session = AsyncMock()

    # Create three different responses
    fail_response1 = AsyncMock()
    fail_response1.status = 500
    fail_response1.text = AsyncMock(return_value="Internal Server Error")

    success_response = AsyncMock()
    success_response.status = 200
    xml_response = (
        '<wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs/2.0" '
        'numberMatched="5" numberReturned="2"><wfs:member></wfs:member></wfs:FeatureCollection>'
    )
    success_response.text = AsyncMock(return_value=xml_response)

    # Set up mock session
    mock_session.get = MagicMock()

    # Create context managers for each response
    cm1 = AsyncMock()
    cm1.__aenter__.return_value = fail_response1
    cm1.__aexit__.return_value = None

    cm2 = AsyncMock()
    cm2.__aenter__.return_value = success_response
    cm2.__aexit__.return_value = None

    # Have session.get return different context managers on each call
    mock_session.get.side_effect = [cm1, cm2]

    # Replace the retry decorator to make it more predictable in tests
    with patch(
        "unified_pipeline.bronze.water_projects.WaterProjectsBronze._fetch_chunk.retry.stop",
        stop_after_attempt(4),  # Allow up to 4 attempts
    ):
        layer = "N2000_projekter:Hydrologi_E"
        url = water_projects_bronze.config.url
        result = await water_projects_bronze._fetch_chunk(mock_session, layer, url, 0)

        # Verify the result was successful on the third try
        assert result["text"] == xml_response
        assert result["total_features"] == 5
        assert result["returned_features"] == 2

        # Verify that session.get was called three times
        assert mock_session.get.call_count == 2

        # Verify the correct parameters were used each time
        for call_args in mock_session.get.call_args_list:
            args, kwargs = call_args
            assert args[0] == url
            assert kwargs["params"] == water_projects_bronze._get_params(layer, 0)


@pytest.mark.asyncio
async def test_fetch_wfs_data_with_mixed_results(
    water_projects_bronze: WaterProjectsBronze,
) -> None:
    """Test the _fetch_wfs_data method with mixed results (some successes, some errors)."""

    # We need to mock the asyncio.gather to simulate errors
    async def mock_gather(*args, return_exceptions: bool = False) -> list:  # type: ignore
        # Return mix of results and exceptions
        return [
            {"text": "result2", "start_index": 2, "total_features": 10, "returned_features": 2},
            Exception("Network error"),
            {"text": "result6", "start_index": 6, "total_features": 10, "returned_features": 2},
        ]

    # Create a patched version that returns mixed results for the first call
    success_chunk = {
        "text": "<wfs:FeatureCollection numberMatched=10 numberReturned=2>"
        "<wfs:member>1</wfs:member><wfs:member>2</wfs:member></wfs:FeatureCollection>",
        "start_index": 0,
        "total_features": 10,
        "returned_features": 2,
    }

    # Create a mock implementation for _fetch_chunk
    async def mock_fetch_chunk(
        session: aiohttp.ClientSession, layer: str, url: str, start_index: int
    ) -> dict:
        if start_index == 0:
            return success_chunk
        raise ValueError("Should not be called for other indices")

    # Use a patch for asyncio.gather to simulate mixed results
    with (
        patch.object(water_projects_bronze, "_fetch_chunk", side_effect=mock_fetch_chunk),
        patch("unified_pipeline.bronze.water_projects.asyncio.gather", side_effect=mock_gather),
    ):
        mock_session = AsyncMock()
        layer = "N2000_projekter:Hydrologi_E"
        url = water_projects_bronze.config.url

        # Test that the error is properly propagated
        with pytest.raises(Exception) as excinfo:
            await water_projects_bronze._fetch_wfs_data(mock_session, layer, url)
        assert "Network error" in str(excinfo.value)


@pytest.mark.asyncio
async def test_fetch_wfs_data_with_realistic_xml(
    water_projects_bronze: WaterProjectsBronze,
) -> None:
    """Test the _fetch_wfs_data method with a more realistic XML response."""
    # Create a more realistic XML response
    realistic_xml = """<?xml version="1.0" encoding="UTF-8"?>
    <wfs:FeatureCollection 
        xmlns:xs="http://www.w3.org/2001/XMLSchema" 
        xmlns:wfs="http://www.opengis.net/wfs/2.0" 
        xmlns:gml="http://www.opengis.net/gml/3.2"
        xmlns:N2000_projekter="https://geodata.fvm.dk/geoserver/N2000_projekter"
        numberMatched="5" numberReturned="2">
        <wfs:member>
            <N2000_projekter:Hydrologi_E gml:id="Hydrologi_E.1">
            <N2000_projekter:ID>123</N2000_projekter:ID>
            <N2000_projekter:PROJEKTNR>N123</N2000_projekter:PROJEKTNR>
            <N2000_projekter:NAVN>Test Project 1</N2000_projekter:NAVN>
            <N2000_projekter:STATUS>Implemented</N2000_projekter:STATUS>
            <N2000_projekter:SHAPE>
                <gml:MultiSurface>
                <gml:surfaceMember>
                    <gml:Polygon>
                    <gml:exterior>
                        <gml:LinearRing>
                        <gml:posList>10.1 56.1 10.2 56.1 10.2 56.2 10.1 56.2 10.1 56.1</gml:posList>
                        </gml:LinearRing>
                    </gml:exterior>
                    </gml:Polygon>
                </gml:surfaceMember>
                </gml:MultiSurface>
            </N2000_projekter:SHAPE>
            </N2000_projekter:Hydrologi_E>
            </wfs:member>
            <wfs:member>
            <N2000_projekter:Hydrologi_E gml:id="Hydrologi_E.2">
            <N2000_projekter:ID>456</N2000_projekter:ID>
            <N2000_projekter:PROJEKTNR>N456</N2000_projekter:PROJEKTNR>
            <N2000_projekter:NAVN>Test Project 2</N2000_projekter:NAVN>
            <N2000_projekter:STATUS>Planned</N2000_projekter:STATUS>
            <N2000_projekter:SHAPE>
                <gml:MultiSurface>
                <gml:surfaceMember>
                    <gml:Polygon>
                    <gml:exterior>
                        <gml:LinearRing>
                        <gml:posList>9.1 55.1 9.2 55.1 9.2 55.2 9.1 55.2 9.1 55.1</gml:posList>
                        </gml:LinearRing>
                    </gml:exterior>
                    </gml:Polygon>
                </gml:surfaceMember>
                </gml:MultiSurface>
            </N2000_projekter:SHAPE>
            </N2000_projekter:Hydrologi_E>
        </wfs:member>
    </wfs:FeatureCollection>
    """

    # Mock _fetch_chunk to return the realistic XML
    mock_fetch_chunk = AsyncMock(
        return_value={
            "text": realistic_xml,
            "start_index": 0,
            "total_features": 5,
            "returned_features": 2,
        }
    )

    # Mock asyncio.gather to return an empty list so no additional chunks are processed
    async def mock_gather(*args, return_exceptions: bool = False) -> list:  # type: ignore
        return []

    with (
        patch.object(water_projects_bronze, "_fetch_chunk", mock_fetch_chunk),
        patch("unified_pipeline.bronze.water_projects.asyncio.gather", side_effect=mock_gather),
    ):
        mock_session = AsyncMock()
        layer = "N2000_projekter:Hydrologi_E"
        url = water_projects_bronze.config.url

        result = await water_projects_bronze._fetch_wfs_data(mock_session, layer, url)

        # Verify the response was processed correctly
        assert result is not None
        assert len(result) == 1
        assert result[0] == realistic_xml

        # Verify we can parse the XML and extract data
        root = ET.fromstring(result[0])
        namespaces = {
            "wfs": "http://www.opengis.net/wfs/2.0",
            "N2000_projekter": "https://geodata.fvm.dk/geoserver/N2000_projekter",
        }

        members = root.findall(".//wfs:member", namespaces)
        assert len(members) == 2

        # Verify some properties from the XML
        first_member = members[0].find(".//N2000_projekter:Hydrologi_E", namespaces)
        assert first_member is not None
        project_id = first_member.find(".//N2000_projekter:ID", namespaces)
        assert project_id is not None
        assert project_id.text == "123"

        project_name = first_member.find(".//N2000_projekter:NAVN", namespaces)
        assert project_name is not None
        assert project_name.text == "Test Project 1"


@pytest.mark.asyncio
async def test_url_construction_for_different_services(
    water_projects_bronze: WaterProjectsBronze,
) -> None:
    """Test that URLs are correctly constructed for different service types."""
    # Create a test configuration with multiple layers and URLs
    test_config = water_projects_bronze.config.model_copy(
        update={
            "layers": [
                "N2000_projekter:Hydrologi_E",  # Default WFS
                "vandprojekter:kla_projektforslag",  # Custom WFS URL
                "Klima_lavbund_demarkation___offentlige_projekter:0",  # ArcGIS
            ],
            "url_mapping": {
                "vandprojekter:kla_projektforslag": "https://wfs2-miljoegis.mim.dk/vandprojekter/wfs",
                "Klima_lavbund_demarkation___offentlige_projekter:0": "https://gis.nst.dk/server/rest/services/autonom/Klima_lavbund_demarkation___offentlige_projekter/FeatureServer",
            },
            "service_types": {"Klima_lavbund_demarkation___offentlige_projekter:0": "arcgis"},
        }
    )
    # Create a new instance with our test config, using the same GCS util
    water_projects_test = WaterProjectsBronze(test_config, water_projects_bronze.gcs_util)

    # Create mock results for each service type
    wfs_data = ["<wfs:FeatureCollection>WFS Default Data</wfs:FeatureCollection>"]
    custom_wfs_data = ["<wfs:FeatureCollection>Custom WFS Data</wfs:FeatureCollection>"]
    arcgis_data = ['{"features": [{"attributes": {"id": 1}}]}']

    # Mock fetch methods to track which URL was used
    async def mock_fetch_wfs_data(session: aiohttp.ClientSession, layer: str, url: str) -> list:
        if url == water_projects_test.config.url:
            return wfs_data
        elif url == "https://wfs2-miljoegis.mim.dk/vandprojekter/wfs":
            return custom_wfs_data
        else:
            raise ValueError(f"Unexpected URL: {url}")

    async def mock_fetch_arcgis_data(session: aiohttp.ClientSession, layer: str, url: str) -> list:
        if "gis.nst.dk" in url:
            return arcgis_data
        else:
            raise ValueError(f"Unexpected URL: {url}")

    # Patch the fetch methods
    with (
        patch.object(water_projects_test, "_fetch_wfs_data", side_effect=mock_fetch_wfs_data),
        patch.object(water_projects_test, "_fetch_arcgis_data", side_effect=mock_fetch_arcgis_data),
        patch("unified_pipeline.bronze.water_projects.aiohttp.ClientSession"),
        patch("unified_pipeline.bronze.water_projects.aiohttp.TCPConnector"),
    ):
        # Run the fetch_raw_data method
        result = await water_projects_test._fetch_raw_data()

        # Verify the results
        assert result is not None
        assert len(result) == 3

        # Check that the correct data was returned for each layer
        assert result[0] == ("N2000_projekter:Hydrologi_E", wfs_data[0])
        assert result[1] == ("vandprojekter:kla_projektforslag", custom_wfs_data[0])
        assert result[2] == ("Klima_lavbund_demarkation___offentlige_projekter:0", arcgis_data[0])


@pytest.mark.asyncio
@patch(
    "unified_pipeline.bronze.water_projects.WaterProjectsBronze._fetch_chunk.retry.stop",
    stop_after_attempt(1),
)
async def test_handling_unexpected_data_structures(
    water_projects_bronze: WaterProjectsBronze,
) -> None:
    """Test handling of unexpected data structures in responses."""
    # Test with an unexpected XML structure (missing attributes)
    unexpected_xml = """<?xml version="1.0" encoding="UTF-8"?>
    <wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs/2.0" xmlns:N2000_projekter="https://geodata.fvm.dk/geoserver/N2000_projekter">
    <!-- Missing numberMatched and numberReturned attributes -->
    <wfs:member>
        <N2000_projekter:Hydrologi_E>
        <N2000_projekter:ID>123</N2000_projekter:ID>
        </N2000_projekter:Hydrologi_E>
    </wfs:member>
    </wfs:FeatureCollection>
    """

    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.text = AsyncMock(return_value=unexpected_xml)

    mock_session = get_async_mock_session(mock_response)
    layer = "N2000_projekter:Hydrologi_E"
    url = water_projects_bronze.config.url

    # Test that we can handle missing attributes gracefully
    result = await water_projects_bronze._fetch_chunk(mock_session, layer, url, 0)

    # Verify defaults are used for missing attributes
    assert result["text"] == unexpected_xml
    assert result["start_index"] == 0
    assert result["total_features"] == 0  # Default when numberMatched is missing
    assert result["returned_features"] == 0  # Default when numberReturned is missing

    # Test with malformed but technically valid XML
    malformed_xml = """<?xml version="1.0" encoding="UTF-8"?>
    <wfs:FeatureCollection 
        xmlns:wfs="http://www.opengis.net/wfs/2.0" 
        numberMatched="invalid" 
        numberReturned="not-a-number"
        xmlns:N2000_projekter="https://geodata.fvm.dk/geoserver/N2000_projekter">
        <wfs:member>
            <N2000_projekter:Hydrologi_E>
            <N2000_projekter:ID>123</N2000_projekter:ID>
            </N2000_projekter:Hydrologi_E>
        </wfs:member>
    </wfs:FeatureCollection>
    """

    mock_response.text = AsyncMock(return_value=malformed_xml)

    # This should raise a ValueError when trying to convert non-numeric strings to int
    with pytest.raises(Exception):
        await water_projects_bronze._fetch_chunk(mock_session, layer, url, 0)
