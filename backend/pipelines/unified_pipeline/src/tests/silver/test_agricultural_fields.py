"""
Tests for Agricultural Fields Silver processing.

This module tests the silver layer processing for agricultural fields data,
including payload extraction, geometry validation, and data transformation.
"""

import json
from typing import Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

try:
    import geopandas as gpd
except ImportError:
    gpd = None

from unified_pipeline.silver.agricultural_fields import (
    AgriculturalFieldsSilver,
    AgriculturalFieldsSilverConfig,
)


@pytest.fixture
def config() -> AgriculturalFieldsSilverConfig:
    """Create a test configuration."""
    return AgriculturalFieldsSilverConfig(
        fields_dataset="test_fields",
        blocks_dataset="test_blocks",
        bucket="test-bucket",
        column_mapping={
            "Marknr": "field_id",
            "IMK_areal": "area_ha",
            "CVR": "cvr_number",
        },
    )


@pytest.fixture
def silver_source(
    config: AgriculturalFieldsSilverConfig,
) -> AgriculturalFieldsSilver:
    """Create a silver source for testing."""
    with patch("unified_pipeline.common.base.GCSDataAccess"):
        with patch("duckdb.connect"):
            return AgriculturalFieldsSilver(config)


@pytest.fixture
def sample_payload() -> str:
    """Return a sample GeoJSON payload."""
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
def sample_dataframe() -> Dict[str, List[str]]:
    """Return a sample dataframe with payloads."""
    return {
        "payload": [
            '{"features":[{"attributes":{"Marknr":"123","IMK_areal":5.5,"CVR":"12345678"},'
            '"geometry":{"rings":[[[10.0,55.0],[10.1,55.0],[10.1,55.1],[10.0,55.1],[10.0,55.0]]]}}]}',
            '{"features":[{"attributes":{"Marknr":"456","IMK_areal":3.2,"CVR":"87654321"},'
            '"geometry":{"rings":[[[11.0,56.0],[11.1,56.0],[11.1,56.1],[11.0,56.1],[11.0,56.0]]]}}]}',
        ]
    }


# Method extract_geojson_from_payload no longer exists - replaced with DuckDB-spatial processing


# Method extract_geojson_from_payload no longer exists - replaced with DuckDB-spatial processing


# Method extract_geojson_from_payload no longer exists - replaced with DuckDB-spatial processing


# Method _process_data no longer exists - replaced with _process_payloads_with_duckdb


# Method _process_data no longer exists - replaced with _process_payloads_with_duckdb


# Method _process_data no longer exists - replaced with _process_payloads_with_duckdb


@pytest.mark.asyncio
async def test_run_success(silver_source: AgriculturalFieldsSilver) -> None:
    """Test successful execution of run method."""
    # Mock the read_bronze_data method
    silver_source._read_bronze_data = MagicMock(  # type: ignore[method-assign]
        return_value={"payload": ["test_payload"]}
    )

    # Mock the _process_payloads_with_duckdb method
    silver_source._process_payloads_with_duckdb = AsyncMock(
        return_value=("test_table", silver_source.conn)
    )  # type: ignore[method-assign]

    # Mock the _save_data method
    silver_source._save_data = MagicMock()  # type: ignore[method-assign]

    # Act
    await silver_source.run()

    # Assert - Now expects 11 calls total (6 fields years 2020-2025 + 5 blocks years 2020-2024)
    assert silver_source._read_bronze_data.call_count == 11
    assert silver_source._process_payloads_with_duckdb.call_count == 11
    assert silver_source._save_data.call_count == 11


@pytest.mark.asyncio
async def test_run_read_bronze_data_failure(silver_source: AgriculturalFieldsSilver) -> None:
    """Test run method when read_bronze_data fails."""
    # Mock the read_bronze_data method to return None (failure)
    silver_source._read_bronze_data = MagicMock(return_value=None)  # type: ignore[method-assign]

    # Mock the _process_payloads_with_duckdb method
    silver_source._process_payloads_with_duckdb = AsyncMock()  # type: ignore[method-assign]

    # Mock the _save_data method
    silver_source._save_data = MagicMock()  # type: ignore[method-assign]

    await silver_source.run()

    # Expects 11 calls (will fail on first one, but continues to others)
    assert silver_source._read_bronze_data.call_count == 11
    # Should not be called after failure
    silver_source._process_payloads_with_duckdb.assert_not_called()
    silver_source._save_data.assert_not_called()  # Should not be called after failure


@pytest.mark.asyncio
async def test_run_process_data_failure(silver_source: AgriculturalFieldsSilver) -> None:
    """Test run method when process_payloads_with_duckdb returns None."""
    # Mock the read_bronze_data method
    silver_source._read_bronze_data = MagicMock(  # type: ignore[method-assign]
        return_value={"payload": ["test_payload"]}
    )

    # Mock the _process_payloads_with_duckdb method to return None (failure)
    silver_source._process_payloads_with_duckdb = AsyncMock(return_value=(None, None))  # type: ignore[method-assign]

    # Mock the _save_data method
    silver_source._save_data = MagicMock()  # type: ignore[method-assign]

    await silver_source.run()

    assert silver_source._read_bronze_data.call_count == 11  # Called for all datasets/years
    assert silver_source._process_payloads_with_duckdb.call_count == 11  # Called for all but fails
    silver_source._save_data.assert_not_called()  # Should not be called after failure
