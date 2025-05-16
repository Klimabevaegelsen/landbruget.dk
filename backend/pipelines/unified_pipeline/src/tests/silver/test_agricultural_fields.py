"""
Tests for the AgriculturalFieldsSilver class.
"""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Polygon

from unified_pipeline.silver.agricultural_fields import (
    AgriculturalFieldsSilver,
    AgriculturalFieldsSilverConfig,
)
from unified_pipeline.util.gcs_util import GCSUtil


@pytest.fixture
def mock_gcs_util() -> MagicMock:
    """Return a mock GCSUtil instance."""
    mock_gcs = MagicMock(spec=GCSUtil)
    mock_gcs.read_parquet = MagicMock()
    mock_gcs.upload_blob = MagicMock()
    return mock_gcs


@pytest.fixture
def config() -> AgriculturalFieldsSilverConfig:
    """Return a test configuration."""
    return AgriculturalFieldsSilverConfig(
        fields_dataset="test_fields",
        blocks_dataset="test_blocks",
        bucket="test-bucket",
        storage_batch_size=1000,
        column_mapping={
            "Marknr": "field_id",
            "IMK_areal": "area_ha",
            "CVR": "cvr_number",
        },
    )


@pytest.fixture
def silver_source(
    config: AgriculturalFieldsSilverConfig, mock_gcs_util: MagicMock
) -> AgriculturalFieldsSilver:
    """Return a test AgriculturalFieldsSilver instance."""
    source = AgriculturalFieldsSilver(config, mock_gcs_util)
    source.log = MagicMock()
    return source


@pytest.fixture
def sample_payload() -> str:
    """Return a sample payload for testing."""
    return json.dumps(
        {
            "features": [
                {
                    "attributes": {"Marknr": "123", "IMK_areal": 5.5, "CVR": "12345678"},
                    "geometry": {
                        "rings": [
                            [[10.0, 55.0], [10.1, 55.0], [10.1, 55.1], [10.0, 55.1], [10.0, 55.0]]
                        ]
                    },
                },
                {
                    "attributes": {"Marknr": "456", "IMK_areal": 3.2, "CVR": "87654321"},
                    "geometry": {
                        "rings": [
                            [[11.0, 56.0], [11.1, 56.0], [11.1, 56.1], [11.0, 56.1], [11.0, 56.0]]
                        ]
                    },
                },
            ]
        }
    )


@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    """Return a sample DataFrame with payloads."""
    return pd.DataFrame(
        {
            "payload": [
                '{"features":[{"attributes":{"Marknr":"123","IMK_areal":5.5,"CVR":"12345678"},'
                '"geometry":{"rings":[[[10.0,55.0],[10.1,55.0],[10.1,55.1],[10.0,55.1],[10.0,55.0]]]}}]}',
                '{"features":[{"attributes":{"Marknr":"456","IMK_areal":3.2,"CVR":"87654321"},'
                '"geometry":{"rings":[[[11.0,56.0],[11.1,56.0],[11.1,56.1],[11.0,56.1],[11.0,56.0]]]}}]}',
            ]
        }
    )


@pytest.mark.asyncio
async def test_extract_geojson_from_payload_success(
    silver_source: AgriculturalFieldsSilver, sample_payload: str
) -> None:
    """Test successfully extracting GeoJSON from payload."""

    result = await silver_source.extract_geojson_from_payload(
        sample_payload, silver_source.config.column_mapping
    )

    assert not result.empty
    assert len(result) == 2
    assert "field_id" in result.columns
    assert "area_ha" in result.columns
    assert "cvr_number" in result.columns
    assert isinstance(result.geometry[0], Polygon)


@pytest.mark.asyncio
async def test_extract_geojson_from_payload_empty(silver_source: AgriculturalFieldsSilver) -> None:
    """Test extracting GeoJSON from empty payload."""

    empty_payload = json.dumps({"features": []})
    result = await silver_source.extract_geojson_from_payload(
        empty_payload, silver_source.config.column_mapping
    )
    assert result.empty


@pytest.mark.asyncio
async def test_extract_geojson_from_payload_error(silver_source: AgriculturalFieldsSilver) -> None:
    """Test error handling when extracting GeoJSON from invalid payload."""

    invalid_payload = "not a valid json"
    result = await silver_source.extract_geojson_from_payload(
        invalid_payload, silver_source.config.column_mapping
    )
    assert result.empty


@pytest.mark.asyncio
async def test_process_data_success(
    silver_source: AgriculturalFieldsSilver, sample_dataframe: pd.DataFrame
) -> None:
    """Test successfully processing data."""

    # Create a mock GeoDataFrame to return from validate_and_transform_geometries
    mock_gdf = gpd.GeoDataFrame(
        {
            "field_id": ["123", "456"],
            "area_ha": [5.5, 3.2],
            "cvr_number": ["12345678", "87654321"],
            "geometry": [
                Polygon([(10.0, 55.0), (10.1, 55.0), (10.1, 55.1), (10.0, 55.1), (10.0, 55.0)]),
                Polygon([(11.0, 56.0), (11.1, 56.0), (11.1, 56.1), (11.0, 56.1), (11.0, 56.0)]),
            ],
        }
    )
    result = await silver_source._process_data(sample_dataframe, "test_dataset")

    assert not result.empty
    assert len(result) == 2
    # Check that the dataframes have the same columns and values
    assert set(result.columns) == set(mock_gdf.columns)
    assert len(result) == len(mock_gdf)


@pytest.mark.asyncio
@patch("unified_pipeline.util.geometry_validator.validate_and_transform_geometries")
async def test_process_data_empty_result(
    mock_validate: MagicMock, silver_source: AgriculturalFieldsSilver
) -> None:
    """Test processing data with empty result."""

    empty_df = pd.DataFrame({"payload": []})
    result = await silver_source._process_data(empty_df, "test_dataset")
    assert result.empty
    mock_validate.assert_not_called()


@pytest.mark.asyncio
async def test_process_data_column_renaming(silver_source: AgriculturalFieldsSilver) -> None:
    """Test column renaming during data processing."""

    # Create a DataFrame with a column that needs renaming
    df_with_special_chars = pd.DataFrame(
        {
            "payload": [
                '{"features":[{"attributes":{"Marknr":"123","field.name":"Field(1)","field(test)":"test"},'
                '"geometry":{"rings":[[[10.0,55.0],[10.1,55.0],[10.1,55.1],[10.0,55.1],[10.0,55.0]]]}}]}'
            ]
        }
    )

    # Create a GeoDataFrame that would be created from the payload
    gdf = gpd.GeoDataFrame(
        {
            "field_id": ["123"],
            "field.name": ["Field(1)"],
            "field(test)": ["test"],
            "geometry": [
                Polygon([(10.0, 55.0), (10.1, 55.0), (10.1, 55.1), (10.0, 55.1), (10.0, 55.0)])
            ],
        },
        crs="EPSG:25832",
    )

    silver_source.extract_geojson_from_payload = AsyncMock(  # type: ignore[method-assign]
        return_value=gdf
    )

    result = await silver_source._process_data(df_with_special_chars, "test_dataset")

    assert "field_name" in result.columns
    assert "field_test_" in result.columns
    assert "field.name" not in result.columns
    assert "field(test)" not in result.columns


@pytest.mark.asyncio
async def test_run_success(silver_source: AgriculturalFieldsSilver) -> None:
    """Test successful execution of run method."""

    # Mock the read_bronze_data method
    silver_source._read_bronze_data = MagicMock(  # type: ignore[method-assign]
        return_value=pd.DataFrame({"payload": ["test_payload"]})
    )

    # Mock the _process_data method
    mock_geo_df = gpd.GeoDataFrame({"field_id": ["123"]})
    silver_source._process_data = AsyncMock(return_value=mock_geo_df)  # type: ignore[method-assign]

    # Mock the _save_data method
    silver_source._save_data = MagicMock()  # type: ignore[method-assign]

    # Act
    await silver_source.run()

    # Assert
    assert silver_source._read_bronze_data.call_count == 2  # Called for fields and blocks
    assert silver_source._process_data.call_count == 2  # Called for fields and blocks
    assert silver_source._save_data.call_count == 2  # Called for fields and blocks


@pytest.mark.asyncio
async def test_run_read_bronze_data_failure(silver_source: AgriculturalFieldsSilver) -> None:
    """Test run method when read_bronze_data fails."""

    # Mock the read_bronze_data method to return None (failure)
    silver_source._read_bronze_data = MagicMock(return_value=None)  # type: ignore[method-assign]

    # Mock the _process_data method
    silver_source._process_data = AsyncMock()  # type: ignore[method-assign]

    # Mock the _save_data method
    silver_source._save_data = MagicMock()  # type: ignore[method-assign]
    await silver_source.run()

    silver_source._read_bronze_data.assert_called_once()  # Only called once before failing
    silver_source._process_data.assert_not_called()  # Should not be called after failure
    silver_source._save_data.assert_not_called()  # Should not be called after failure


@pytest.mark.asyncio
async def test_run_process_data_failure(silver_source: AgriculturalFieldsSilver) -> None:
    """Test run method when process_data returns None."""

    # Mock the read_bronze_data method
    silver_source._read_bronze_data = MagicMock(  # type: ignore[method-assign]
        return_value=pd.DataFrame({"payload": ["test_payload"]})
    )

    # Mock the _process_data method to return None (failure)
    silver_source._process_data = AsyncMock(return_value=None)  # type: ignore[method-assign]

    # Mock the _save_data method
    silver_source._save_data = MagicMock()  # type: ignore[method-assign]

    await silver_source.run()

    silver_source._read_bronze_data.assert_called_once()  # Only called once before failing
    silver_source._process_data.assert_called_once()  # Called once before failing
    silver_source._save_data.assert_not_called()  # Should not be called after failure
