"""
Tests for the GEUSBoreholePesticidesBronze class.
"""

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from tenacity import stop_after_attempt

from unified_pipeline.bronze.geus_borehole_pesticides import (
    GEUSBoreholePesticidesBronze,
    GEUSBoreholePesticidesBronzeConfig,
)


@pytest.fixture
def config() -> GEUSBoreholePesticidesBronzeConfig:
    """Return a test configuration."""
    return GEUSBoreholePesticidesBronzeConfig(
        name="Test GEUS Borehole Pesticides",
        dataset="test_geus_borehole_pesticides",
        bucket="test-bucket",
        url="https://test.example.com/wfs",
        batch_size=1000,
        max_concurrent=2,
    )


@pytest.fixture
def geus_bronze(config: GEUSBoreholePesticidesBronzeConfig) -> GEUSBoreholePesticidesBronze:
    """Return a test GEUSBoreholePesticidesBronze instance."""
    source = GEUSBoreholePesticidesBronze(config)
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


def test_get_params_boreholes(geus_bronze: GEUSBoreholePesticidesBronze) -> None:
    """Test generating request parameters for boreholes layer."""
    params = geus_bronze._get_params("jupiter_boringer_ws", 500)

    assert params["SERVICE"] == "WFS"
    assert params["REQUEST"] == "GetFeature"
    assert params["TYPENAMES"] == "jupiter_boringer_ws"
    assert params["STARTINDEX"] == "500"
    assert params["COUNT"] == str(geus_bronze.config.batch_size)
    assert params["SRSNAME"] == "urn:ogc:def:crs:EPSG::25832"


def test_get_params_analyses(geus_bronze: GEUSBoreholePesticidesBronze) -> None:
    """Test generating request parameters for analyses layer."""
    params = geus_bronze._get_params("jupiter_anlaegsanalyser", 0)

    assert params["TYPENAMES"] == "jupiter_anlaegsanalyser"
    assert params["STARTINDEX"] == "0"


@pytest.mark.asyncio
async def test_fetch_chunk_success(geus_bronze: GEUSBoreholePesticidesBronze) -> None:
    """Test successful fetching of a chunk."""
    xml_response = """<wfs:FeatureCollection
        xmlns:wfs="http://www.opengis.net/wfs/2.0"
        numberMatched="5000"
        numberReturned="1000">
        <wfs:member></wfs:member>
    </wfs:FeatureCollection>"""

    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.text.return_value = xml_response

    mock_session = get_async_mock_session(mock_response)

    result = await geus_bronze._fetch_chunk(mock_session, "jupiter_boringer_ws", 0)

    assert result["text"] == xml_response
    assert result["typename"] == "jupiter_boringer_ws"
    assert result["start_index"] == 0
    assert result["total_features"] == 5000
    assert result["returned_features"] == 1000

    mock_session.get.assert_called_once_with(
        geus_bronze.config.url,
        params=geus_bronze._get_params("jupiter_boringer_ws", 0),
    )


@pytest.mark.asyncio
async def test_fetch_chunk_unknown_total(geus_bronze: GEUSBoreholePesticidesBronze) -> None:
    """Test handling of numberMatched='unknown' from GEUS WFS."""
    xml_response = """<wfs:FeatureCollection
        xmlns:wfs="http://www.opengis.net/wfs/2.0"
        numberMatched="unknown"
        numberReturned="1000">
        <wfs:member></wfs:member>
    </wfs:FeatureCollection>"""

    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.text.return_value = xml_response

    mock_session = get_async_mock_session(mock_response)

    result = await geus_bronze._fetch_chunk(mock_session, "jupiter_boringer_ws", 0)

    assert result["text"] == xml_response
    assert result["typename"] == "jupiter_boringer_ws"
    assert result["start_index"] == 0
    assert result["total_features"] is None  # Unknown total
    assert result["returned_features"] == 1000


@pytest.mark.asyncio
@patch(
    "unified_pipeline.bronze.geus_borehole_pesticides.GEUSBoreholePesticidesBronze._fetch_chunk.retry.stop",
    stop_after_attempt(1),
)
async def test_fetch_chunk_http_error(geus_bronze: GEUSBoreholePesticidesBronze) -> None:
    """Test HTTP error handling when fetching a chunk."""
    mock_response = AsyncMock()
    mock_response.status = 500

    mock_session = get_async_mock_session(mock_response)

    with pytest.raises(Exception) as excinfo:
        await geus_bronze._fetch_chunk(mock_session, "jupiter_boringer_ws", 0)
        assert "Failed to fetch" in str(excinfo.value)


@pytest.mark.asyncio
@patch(
    "unified_pipeline.bronze.geus_borehole_pesticides.GEUSBoreholePesticidesBronze._fetch_chunk.retry.stop",
    stop_after_attempt(1),
)
async def test_fetch_chunk_xml_parse_error(geus_bronze: GEUSBoreholePesticidesBronze) -> None:
    """Test XML parse error handling."""
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.text = AsyncMock(return_value="<invalid_xml>")

    mock_session = get_async_mock_session(mock_response)

    with pytest.raises(Exception) as excinfo:
        await geus_bronze._fetch_chunk(mock_session, "jupiter_boringer_ws", 0)
        assert "Failed to parse XML response" in str(excinfo.value)


@pytest.mark.asyncio
@patch("unified_pipeline.bronze.geus_borehole_pesticides.GEUSBoreholePesticidesBronze._fetch_chunk")
@patch("aiohttp.ClientSession")
@patch("aiohttp.TCPConnector")
async def test_fetch_layer_data_success(
    mock_tcp_connector: MagicMock,
    mock_client_session: MagicMock,
    mock_fetch_chunk: AsyncMock,
    geus_bronze: GEUSBoreholePesticidesBronze,
) -> None:
    """Test successful fetching of a complete layer."""
    mock_fetch_chunk.side_effect = [
        {
            "text": "<xml>chunk1</xml>",
            "typename": "jupiter_boringer_ws",
            "start_index": 0,
            "total_features": 2000,
            "returned_features": 1000,
        },
        {
            "text": "<xml>chunk2</xml>",
            "typename": "jupiter_boringer_ws",
            "start_index": 1000,
            "total_features": 2000,
            "returned_features": 1000,
        },
    ]

    mock_session = MagicMock()
    result = await geus_bronze._fetch_layer_data(mock_session, "jupiter_boringer_ws")

    assert result is not None
    assert len(result) == 2
    assert result[0] == "<xml>chunk1</xml>"
    assert result[1] == "<xml>chunk2</xml>"


@pytest.mark.asyncio
@patch("unified_pipeline.bronze.geus_borehole_pesticides.GEUSBoreholePesticidesBronze._fetch_chunk")
@patch("aiohttp.ClientSession")
@patch("aiohttp.TCPConnector")
async def test_fetch_layer_data_unknown_total(
    mock_tcp_connector: MagicMock,
    mock_client_session: MagicMock,
    mock_fetch_chunk: AsyncMock,
    geus_bronze: GEUSBoreholePesticidesBronze,
) -> None:
    """Test fetching layer when total_features is unknown (GEUS returns 'unknown')."""
    # Simulate unknown total - need to fetch until returned < batch_size (1000 in test config)
    mock_fetch_chunk.side_effect = [
        {
            "text": "<xml>chunk1</xml>",
            "typename": "jupiter_boringer_ws",
            "start_index": 0,
            "total_features": None,  # Unknown total
            "returned_features": 1000,  # Full batch
        },
        {
            "text": "<xml>chunk2</xml>",
            "typename": "jupiter_boringer_ws",
            "start_index": 1000,
            "total_features": None,
            "returned_features": 1000,  # Full batch
        },
        {
            "text": "<xml>chunk3</xml>",
            "typename": "jupiter_boringer_ws",
            "start_index": 2000,
            "total_features": None,
            "returned_features": 500,  # Partial batch = end of data
        },
    ]

    mock_session = MagicMock()
    result = await geus_bronze._fetch_layer_data(mock_session, "jupiter_boringer_ws")

    assert result is not None
    assert len(result) == 3
    assert result[0] == "<xml>chunk1</xml>"
    assert result[1] == "<xml>chunk2</xml>"
    assert result[2] == "<xml>chunk3</xml>"


@pytest.mark.asyncio
@patch(
    "unified_pipeline.bronze.geus_borehole_pesticides.GEUSBoreholePesticidesBronze._fetch_layer_data"
)
@patch("aiohttp.ClientSession")
@patch("aiohttp.TCPConnector")
async def test_fetch_raw_data_success(
    mock_tcp_connector: MagicMock,
    mock_client_session: MagicMock,
    mock_fetch_layer_data: AsyncMock,
    geus_bronze: GEUSBoreholePesticidesBronze,
) -> None:
    """Test successful fetching of all raw data (both layers)."""
    mock_fetch_layer_data.side_effect = [
        ["<xml>boreholes1</xml>", "<xml>boreholes2</xml>"],  # boreholes
        ["<xml>analyses1</xml>"],  # analyses
    ]

    result = await geus_bronze._fetch_raw_data()

    assert result is not None
    assert "boreholes" in result
    assert "analyses" in result
    assert len(result["boreholes"]) == 2
    assert len(result["analyses"]) == 1


@pytest.mark.asyncio
@patch(
    "unified_pipeline.bronze.geus_borehole_pesticides.GEUSBoreholePesticidesBronze._fetch_raw_data"
)
async def test_run_success(
    mock_fetch_raw_data: AsyncMock,
    geus_bronze: GEUSBoreholePesticidesBronze,
) -> None:
    """Test successful run of the pipeline."""
    mock_fetch_raw_data.return_value = {
        "boreholes": ["<xml>borehole data</xml>"],
        "analyses": ["<xml>analyses data</xml>"],
    }
    geus_bronze._save_data = MagicMock()

    result = await geus_bronze.run()

    assert result is not None
    assert "boreholes" in result
    assert "analyses" in result
    mock_fetch_raw_data.assert_called_once()


@pytest.mark.asyncio
@patch(
    "unified_pipeline.bronze.geus_borehole_pesticides.GEUSBoreholePesticidesBronze._fetch_raw_data"
)
async def test_run_fetch_error(
    mock_fetch_raw_data: AsyncMock,
    geus_bronze: GEUSBoreholePesticidesBronze,
) -> None:
    """Test run with error in fetching raw data."""
    mock_fetch_raw_data.return_value = None
    geus_bronze._save_data = MagicMock()

    result = await geus_bronze.run()

    assert result is None
    mock_fetch_raw_data.assert_called_once()
    geus_bronze._save_data.assert_not_called()


def test_create_dataframe(geus_bronze: GEUSBoreholePesticidesBronze) -> None:
    """Test creating a raw table from both layers."""
    data = {
        "boreholes": ["<wfs:FeatureCollection></wfs:FeatureCollection>"],
        "analyses": ["<wfs:FeatureCollection></wfs:FeatureCollection>"],
    }

    table_name = geus_bronze.create_dataframe(data)

    assert isinstance(table_name, str)
    assert table_name == "final_dataframe"

    # Verify the table exists and has correct structure
    result = geus_bronze.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    assert result[0] == 2  # Two rows, one for each layer

    # Check column structure
    columns = geus_bronze.conn.execute(f"DESCRIBE {table_name}").fetchall()
    column_names = {row[0] for row in columns}
    expected_columns = {"layer_type", "payload", "created_at", "source", "source_crs", "updated_at"}
    assert expected_columns.issubset(column_names)

    # Check layer types are correct
    layer_types = geus_bronze.conn.execute(
        f"SELECT DISTINCT layer_type FROM {table_name}"
    ).fetchall()
    layer_type_values = {row[0] for row in layer_types}
    assert layer_type_values == {"boreholes", "analyses"}


def test_config_defaults() -> None:
    """Test default configuration values."""
    config = GEUSBoreholePesticidesBronzeConfig()

    assert config.name == "GEUS Borehole Pesticides"
    assert config.dataset == "geus_borehole_pesticides"
    assert config.type == "wfs"
    assert config.frequency == "monthly"
    assert config.url == "https://data.geus.dk/geusmap/ows/25832.jsp"
    assert config.source_crs == "EPSG:25832"
    assert config.boreholes_typename == "jupiter_boringer_ws"
    assert config.analyses_typename == "jupiter_anlaegsanalyser"
    assert config.batch_size == 5000
    assert config.max_concurrent == 2
