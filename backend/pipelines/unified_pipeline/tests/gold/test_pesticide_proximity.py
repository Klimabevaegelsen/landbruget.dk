"""Tests for pesticide proximity matrix-year behavior."""

from unittest.mock import AsyncMock, Mock

import pytest

from unified_pipeline.gold.pesticide_proximity import (
    PesticideProximityGold,
    PesticideProximityGoldConfig,
)


@pytest.mark.asyncio
async def test_matrix_year_without_disaggregation_data_is_skipped():
    processor = object.__new__(PesticideProximityGold)
    processor.config = PesticideProximityGoldConfig(pesticide_year=2014)
    processor.log = Mock()
    processor._setup_duckdb = AsyncMock()
    processor._load_datasets = AsyncMock(return_value={"disaggregation": {2015: "path.parquet"}})
    processor._load_year_data = AsyncMock()
    processor._perform_proximity_analysis = AsyncMock()
    processor._save_year_results = AsyncMock()

    await processor.run()

    processor._setup_duckdb.assert_awaited_once()
    processor._load_datasets.assert_awaited_once()
    processor._load_year_data.assert_not_awaited()
    processor._perform_proximity_analysis.assert_not_awaited()
    processor._save_year_results.assert_not_awaited()
    processor.log.warning.assert_called_once()
