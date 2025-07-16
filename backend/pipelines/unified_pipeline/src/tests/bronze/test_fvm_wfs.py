"""
Tests for FVM WFS Bronze layer.
"""

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from unified_pipeline.bronze.fvm_wfs import FVMWFSBronze, FVMWFSBronzeConfig


@pytest.fixture
def config() -> FVMWFSBronzeConfig:
    """Create a test configuration."""
    return FVMWFSBronzeConfig(
        markblokke_years=[2024],  # Test with just one year
        marker_years=[2024],
        smaabiotoper_years=[2024],
        organic_areas_years=[2024],
        save_local=True,
    )


@pytest.fixture
def fvm_wfs_bronze(config: FVMWFSBronzeConfig) -> FVMWFSBronze:
    """Create a FVM WFS bronze instance."""
    return FVMWFSBronze(config)


def get_async_mock_session(response: AsyncMock) -> MagicMock:
    """Create a mock aiohttp session."""
    mock_session = MagicMock()

    class MockGetContextManager:
        async def __aenter__(self) -> AsyncMock:
            return response

        async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
            pass

    mock_session.get.return_value = MockGetContextManager()
    return mock_session


def test_fvm_wfs_bronze_config() -> None:
    """Test FVM WFS bronze configuration."""
    config = FVMWFSBronzeConfig()

    assert config.name == "Danish FVM WFS Agricultural Data"
    assert config.type == "wfs"
    assert config.wfs_url == "https://geodata.fvm.dk/geoserver/wfs"
    assert config.dataset_markblokke == "fvm_markblokke"
    assert config.dataset_marker == "fvm_marker"
    assert config.dataset_organic_areas == "fvm_organic_areas"
    assert len(config.markblokke_years) == 22  # 2005-2026
    assert len(config.marker_years) == 18  # 2008-2025
    assert len(config.smaabiotoper_years) == 3  # 2023-2025
    assert len(config.organic_areas_years) == 13  # 2012-2024


def test_get_layer_name(fvm_wfs_bronze: FVMWFSBronze) -> None:
    """Test layer name generation."""
    # Test Markblokke
    assert fvm_wfs_bronze._get_layer_name("Markblokke", 2024) == "Markblokke:Markblokke_2024"

    # Test Marker
    assert fvm_wfs_bronze._get_layer_name("Marker", 2024) == "Marker:Marker_2024"

    # Test Smaabiotoper
    assert fvm_wfs_bronze._get_layer_name("Smaabiotoper", 2024) == "Marker:Smaabiotoper_2024"

    # Test OrganicAreas
    assert (
        fvm_wfs_bronze._get_layer_name("OrganicAreas", 2024)
        == "Miljoe_og_oekologitilsagn:Oekologiske_arealer_2024"
    )


def test_get_wfs_params(fvm_wfs_bronze: FVMWFSBronze) -> None:
    """Test WFS parameter generation."""
    # Test count request
    params = fvm_wfs_bronze._get_wfs_params("Markblokke:Markblokke_2024", get_count_only=True)

    expected_params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeName": "Markblokke:Markblokke_2024",
        "srsName": "EPSG:25832",
        "resultType": "hits",
    }

    assert params == expected_params

    # Test data request
    params = fvm_wfs_bronze._get_wfs_params("Markblokke:Markblokke_2024", get_count_only=False)

    expected_params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeName": "Markblokke:Markblokke_2024",
        "srsName": "EPSG:25832",
        "outputFormat": "application/json",
    }

    assert params == expected_params


@pytest.mark.asyncio
async def test_get_total_count_success(fvm_wfs_bronze: FVMWFSBronze) -> None:
    """Test successful feature count retrieval."""
    # Mock response with numberMatched
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.text.return_value = (
        '<?xml version="1.0"?><wfs:FeatureCollection numberMatched="12345" />'
    )

    mock_session = get_async_mock_session(mock_response)

    count = await fvm_wfs_bronze._get_total_count(mock_session, "Markblokke:Markblokke_2024")
    assert count == 12345


@pytest.mark.asyncio
async def test_get_total_count_error_status(fvm_wfs_bronze: FVMWFSBronze) -> None:
    """Test feature count retrieval with error status."""
    mock_response = AsyncMock()
    mock_response.status = 400
    mock_response.text.return_value = "Bad Request"

    mock_session = get_async_mock_session(mock_response)

    with pytest.raises(Exception, match="Error getting count"):
        await fvm_wfs_bronze._get_total_count(mock_session, "Markblokke:Markblokke_2024")


@pytest.mark.asyncio
async def test_fetch_layer_data_success(fvm_wfs_bronze: FVMWFSBronze) -> None:
    """Test successful layer data fetching."""
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.text.return_value = '{"type": "FeatureCollection", "features": []}'

    mock_session = get_async_mock_session(mock_response)

    data = await fvm_wfs_bronze._fetch_layer_data(mock_session, "Markblokke:Markblokke_2024")
    assert data == '{"type": "FeatureCollection", "features": []}'


@pytest.mark.asyncio
async def test_fetch_layer_data_error(fvm_wfs_bronze: FVMWFSBronze) -> None:
    """Test layer data fetching with error."""
    mock_response = AsyncMock()
    mock_response.status = 500
    mock_response.text.return_value = "Internal Server Error"

    mock_session = get_async_mock_session(mock_response)

    with pytest.raises(Exception, match="Error response 500"):
        await fvm_wfs_bronze._fetch_layer_data(mock_session, "Markblokke:Markblokke_2024")


def test_create_dataframe(fvm_wfs_bronze: FVMWFSBronze) -> None:
    """Test  creation from raw data."""
    raw_data = ['{"test": "data1"}', '{"test": "data2"}']

    table_name = fvm_wfs_bronze.create_dataframe(raw_data, "Markblokke", 2024)

    # Verify it returns a table name
    assert isinstance(table_name, str)
    assert table_name == "final_dataframe"

    # Verify the table exists and has correct structure
    result = fvm_wfs_bronze.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    assert result[0] == 2

    # Check column structure
    columns = fvm_wfs_bronze.conn.execute(f"DESCRIBE {table_name}").fetchall()
    column_names = {row[0] for row in columns}
    expected_columns = {"payload", "source", "layer_type", "year", "created_at", "updated_at"}
    assert expected_columns.issubset(column_names)

    # Check data values
    data = fvm_wfs_bronze.conn.execute(
        f"SELECT layer_type, year, source FROM {table_name} LIMIT 1"
    ).fetchone()
    assert data[0] == "Markblokke"
    assert data[1] == 2024
    assert data[2] == "Danish FVM WFS Agricultural Data"
