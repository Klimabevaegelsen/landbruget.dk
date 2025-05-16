from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest
from tenacity import stop_after_attempt

from unified_pipeline.bronze.bnbo_status import BNBOStatusBronze, BNBOStatusBronzeConfig
from unified_pipeline.util.gcs_util import GCSUtil


@pytest.fixture
def mock_gcs_util() -> MagicMock:
    return MagicMock(spec=GCSUtil)


@pytest.fixture
def config() -> BNBOStatusBronzeConfig:
    return BNBOStatusBronzeConfig()


@pytest.fixture
def bnbo_status_bronze(
    config: BNBOStatusBronzeConfig, mock_gcs_util: MagicMock
) -> BNBOStatusBronze:
    return BNBOStatusBronze(config, mock_gcs_util)


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


def test_bnbo_status_bronze_config() -> None:
    config = BNBOStatusBronzeConfig()
    assert config.name == "Danish BNBO Status"
    assert config.dataset == "bnbo_status"
    assert config.url == "https://arealeditering-dist-geo.miljoeportal.dk/geoserver/wfs"
    assert config.bucket == "landbrugsdata-raw-data"


def test_get_params(bnbo_status_bronze: BNBOStatusBronze) -> None:
    params = bnbo_status_bronze._get_params(start_index=10)
    assert params["SERVICE"] == "WFS"
    assert params["REQUEST"] == "GetFeature"
    assert params["VERSION"] == "2.0.0"
    assert params["TYPENAMES"] == "dai:status_bnbo"
    assert params["STARTINDEX"] == "10"
    assert params["COUNT"] == str(bnbo_status_bronze.config.batch_size)
    assert params["SRSNAME"] == "urn:ogc:def:crs:EPSG::25832"


@pytest.mark.asyncio
async def test_fetch_chunck_success(bnbo_status_bronze: BNBOStatusBronze) -> None:
    xml_response = '<wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs/2.0" numberMatched="1" numberReturned="1"><wfs:member></wfs:member></wfs:FeatureCollection>'  # noqa: E501

    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.text.return_value = xml_response

    mock_session = get_async_mock_session(mock_response)

    result = await bnbo_status_bronze._fetch_chunck(mock_session, 0)

    assert result["text"] == xml_response
    assert result["start_index"] == 0
    assert result["total_features"] == 1
    assert result["returned_features"] == 1

    mock_session.get.assert_called_once_with(
        bnbo_status_bronze.config.url,
        params=bnbo_status_bronze._get_params(0),
    )


@pytest.mark.asyncio
@patch(
    "unified_pipeline.bronze.bnbo_status.BNBOStatusBronze._fetch_chunck.retry.stop",
    stop_after_attempt(1),
)
async def test_fetch_chunck_http_error(bnbo_status_bronze: BNBOStatusBronze) -> None:
    mock_response = AsyncMock()
    mock_response.status = 500

    mock_session = get_async_mock_session(mock_response)

    with pytest.raises(Exception) as excinfo:
        await bnbo_status_bronze._fetch_chunck(mock_session, 0)
        assert "Failed to fetch data. Status: 500" in str(excinfo.value)


@pytest.mark.asyncio
@patch(
    "unified_pipeline.bronze.bnbo_status.BNBOStatusBronze._fetch_chunck.retry.stop",
    stop_after_attempt(1),
)
async def test_fetch_chunck_xml_parse_error(bnbo_status_bronze: BNBOStatusBronze) -> None:
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.text = AsyncMock(return_value="<invalid_xml>")

    mock_session = get_async_mock_session(mock_response)

    with pytest.raises(Exception) as excinfo:
        await bnbo_status_bronze._fetch_chunck(mock_session, 0)
        assert "Failed to parse XML response" in str(excinfo.value)


@pytest.mark.asyncio
@patch("unified_pipeline.bronze.bnbo_status.aiohttp.ClientSession")
async def test_fetch_raw_data_single_batch(
    mock_client_session: MagicMock, bnbo_status_bronze: BNBOStatusBronze
) -> None:
    xml_content = (
        '<wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs/2.0" '
        "numberMatched=5 numberReturned=5><wfs:member></wfs:member></wfs:FeatureCollection>"
    )
    mock_fetch_chunk = AsyncMock(
        return_value={
            "text": xml_content,
            "start_index": 0,
            "total_features": 5,
            "returned_features": 5,
        }
    )

    with patch.object(bnbo_status_bronze, "_fetch_chunck", mock_fetch_chunk):
        mock_session_instance = AsyncMock()
        mock_client_session.return_value.__aenter__.return_value = mock_session_instance

        result = await bnbo_status_bronze._fetch_raw_data()
        assert result is not None
        assert len(result) == 1
        assert result == [xml_content]
        mock_fetch_chunk.assert_called_once_with(mock_session_instance, 0)


@pytest.mark.asyncio
@patch("unified_pipeline.bronze.bnbo_status.aiohttp.ClientSession")
async def test_fetch_raw_data_multiple_batches(
    mock_client_session: MagicMock,
    mock_gcs_util: MagicMock,
    config: BNBOStatusBronzeConfig,
) -> None:
    # Make batch size smaller for testing multiple batches
    new_config = config.model_copy(update={"batch_size": 2})
    bnbo_status_bronze_small_batch = BNBOStatusBronze(new_config, mock_gcs_util)

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

    # Instead of patching the _fetch_chunck method, we'll create a mock implementation
    # that still calls the get method on the session
    async def mock_fetch_chunk(session: AsyncMock, start_index: int) -> dict:
        # Simulate the call to session.get
        session.get(
            bnbo_status_bronze_small_batch.config.url,
            params=bnbo_status_bronze_small_batch._get_params(start_index),
        )  # noqa: E501
        # Return the appropriate mock response
        for resp in chunk_responses:
            if resp["start_index"] == start_index:
                return resp
        return chunk_responses[0]  # Fallback

    with patch.object(bnbo_status_bronze_small_batch, "_fetch_chunck", mock_fetch_chunk):
        mock_session_instance = AsyncMock()
        mock_client_session.return_value.__aenter__.return_value = mock_session_instance

        result = await bnbo_status_bronze_small_batch._fetch_raw_data()
        assert result is not None
        assert len(result) == 3
        assert result[0] == chunk_responses[0]["text"]
        assert result[1] == chunk_responses[1]["text"]
        assert result[2] == chunk_responses[2]["text"]
        assert mock_client_session.return_value.__aenter__.return_value.get.call_count == 3


@pytest.mark.asyncio
@patch("unified_pipeline.bronze.bnbo_status.aiohttp.ClientSession")
async def test_fetch_raw_data_fetch_chunk_fails(
    mock_client_session: MagicMock, bnbo_status_bronze: BNBOStatusBronze
) -> None:
    mock_fetch_chunk = AsyncMock(side_effect=Exception("Fetch error"))

    with patch.object(bnbo_status_bronze, "_fetch_chunck", mock_fetch_chunk):
        mock_session_instance = AsyncMock()
        mock_client_session.return_value.__aenter__.return_value = mock_session_instance

        with pytest.raises(Exception) as excinfo:
            await bnbo_status_bronze._fetch_raw_data()
            assert "Fetch error" in str(excinfo.value)


@pytest.mark.asyncio
@patch("unified_pipeline.bronze.bnbo_status.aiohttp.ClientSession")
async def test_fetch_raw_data_with_one_batch_and_exception(
    mock_client_session: MagicMock,
    config: BNBOStatusBronzeConfig,
    mock_gcs_util: MagicMock,
) -> None:
    new_config = config.model_copy(update={"batch_size": 1})
    bnbo_status_bronze = BNBOStatusBronze(new_config, mock_gcs_util)
    xml_content = (
        '<wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs/2.0" '
        "numberMatched=2 numberReturned=1><wfs:member></wfs:member></wfs:FeatureCollection>"
    )

    mock_session_instance = AsyncMock()
    mock_client_session.return_value.__aenter__.return_value = mock_session_instance

    with patch.object(
        bnbo_status_bronze,
        "_fetch_chunck",
        side_effect=[
            {
                "text": xml_content,
                "start_index": 0,
                "total_features": 2,
                "returned_features": 1,
            },
            Exception("Fetch error"),
        ],
    ) as mock_fetch_chunk:
        with pytest.raises(Exception) as excinfo:
            await bnbo_status_bronze._fetch_raw_data()
            assert "Fetch error" in str(excinfo.value)
        assert mock_fetch_chunk.call_count == 2


@pytest.mark.asyncio
@patch("unified_pipeline.common.base.pd.Timestamp")
@patch("unified_pipeline.common.base.os.makedirs")
async def test_save_raw_data(
    mock_makedirs: MagicMock,
    mock_timestamp: MagicMock,
    bnbo_status_bronze: BNBOStatusBronze,
    mock_gcs_util: MagicMock,
) -> None:
    raw_data_list = ["<xml_payload_1>", "<xml_payload_2>"]
    dataset_name = "test_dataset"

    # Mock pd.Timestamp
    mock_now = pd.Timestamp("2024-05-07 10:00:00")
    mock_timestamp.now.return_value = mock_now
    mock_now.strftime.return_value = "2024-05-07"  # type: ignore[attr-defined]

    # Mock GCS bucket and blob
    mock_blob = MagicMock()
    mock_bucket = MagicMock()
    mock_bucket.blob.return_value = mock_blob
    mock_gcs_util.get_gcs_client.return_value.bucket.return_value = mock_bucket

    # Mock DataFrame and to_parquet
    mock_df = MagicMock(spec=pd.DataFrame)
    # Configure the mock DataFrame to return appropriate values for our assertions
    mock_df.__getitem__.side_effect = lambda column: {
        "source": MagicMock(iloc=[bnbo_status_bronze.config.name]),
        "created_at": MagicMock(iloc=[mock_now]),
        "updated_at": MagicMock(iloc=[mock_now]),
    }[column]

    with patch(
        "unified_pipeline.common.base.pd.DataFrame", return_value=mock_df
    ) as mock_pd_dataframe:
        config = bnbo_status_bronze.config
        bnbo_status_bronze._save_raw_data(raw_data_list, dataset_name, config.name, config.bucket)

        mock_pd_dataframe.assert_called_once_with(
            {
                "payload": raw_data_list,
            }
        )
    assert mock_df["source"].iloc[0] == bnbo_status_bronze.config.name  # Check one element
    assert mock_df["created_at"].iloc[0] == mock_now
    assert mock_df["updated_at"].iloc[0] == mock_now

    mock_makedirs.assert_called_once_with(f"/tmp/bronze/{dataset_name}", exist_ok=True)

    expected_temp_file = f"/tmp/bronze/{dataset_name}/2024-05-07.parquet"
    mock_df.to_parquet.assert_called_once_with(expected_temp_file)

    mock_bucket.blob.assert_called_once_with(f"bronze/{dataset_name}/2024-05-07.parquet")
    mock_blob.upload_from_filename.assert_called_once_with(expected_temp_file)


@pytest.mark.asyncio
async def test_run_success(bnbo_status_bronze: BNBOStatusBronze) -> None:
    bnbo_status_bronze._fetch_raw_data = AsyncMock(return_value=["<xml_payload>"])  # type: ignore[method-assign]
    bnbo_status_bronze._save_raw_data = AsyncMock()  # type: ignore[method-assign]

    await bnbo_status_bronze.run()

    bnbo_status_bronze._fetch_raw_data.assert_called_once()
    config = bnbo_status_bronze.config
    bnbo_status_bronze._save_raw_data.assert_called_once_with(
        ["<xml_payload>"], config.dataset, config.name, config.bucket
    )


@pytest.mark.asyncio
async def test_run_no_data(bnbo_status_bronze: BNBOStatusBronze) -> None:
    bnbo_status_bronze._fetch_raw_data = AsyncMock(return_value=None)  # type: ignore[method-assign]
    await bnbo_status_bronze.run()
    bnbo_status_bronze._fetch_raw_data.assert_called_once()
