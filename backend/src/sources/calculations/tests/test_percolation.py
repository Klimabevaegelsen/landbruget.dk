import pytest
import pandas as pd
import geopandas as gpd
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock
from shapely.geometry import box
from src.sources.calculations.percolation import PercolationCalculator
from src.sources.parsers.dmi import DMIParser
import os
import json

@pytest.fixture
def dmi_parser():
    """Create a mock DMIParser instance"""
    return MagicMock(spec=DMIParser)

@pytest.fixture
def mock_precipitation_data():
    """Create mock precipitation data"""
    return gpd.GeoDataFrame({
        'geometry': [box(10.0, 55.0, 10.1, 55.1)],
        'value': [50.0]  # mm of precipitation
    })

@pytest.fixture
def mock_evaporation_data():
    """Create mock evaporation data"""
    return gpd.GeoDataFrame({
        'geometry': [box(10.0, 55.0, 10.1, 55.1)],
        'value': [30.0]  # mm of evaporation
    })

@pytest.fixture
def calculator(dmi_parser):
    """Create a PercolationCalculator instance"""
    return PercolationCalculator(dmi_parser)

@pytest.mark.asyncio
async def test_calculate_percolation(calculator, mock_precipitation_data, mock_evaporation_data):
    """Test percolation calculation with mock data"""
    calculator.dmi_parser.fetch_precipitation_data.return_value = mock_precipitation_data
    calculator.dmi_parser.fetch_evaporation_data.return_value = mock_evaporation_data

    start_time = datetime(2024, 3, 1)
    end_time = datetime(2024, 3, 20)
    bbox = [10.0, 55.0, 10.1, 55.1]

    result = await calculator.calculate_percolation(start_time, end_time, bbox)

    assert isinstance(result, gpd.GeoDataFrame)
    assert not result.empty
    assert 'percolation' in result.columns
    assert 'precipitation' in result.columns
    assert 'evaporation' in result.columns

    print("\nTest results with mock data:")
    print(f"Number of grid cells: {len(result)}")
    print(f"Average precipitation: {result['precipitation'].mean():.2f} mm")
    print(f"Average evaporation: {result['evaporation'].mean():.2f} mm")
    print(f"Average percolation: {result['percolation'].mean():.2f} mm")

@pytest.mark.asyncio
async def test_get_daily_percolation(calculator, mock_precipitation_data, mock_evaporation_data):
    """Test daily percolation calculation"""
    calculator.dmi_parser.fetch_precipitation_data.return_value = mock_precipitation_data
    calculator.dmi_parser.fetch_evaporation_data.return_value = mock_evaporation_data

    result = await calculator.get_daily_percolation(days=7)

    assert isinstance(result, gpd.GeoDataFrame)
    assert not result.empty
    assert 'percolation' in result.columns

@pytest.mark.asyncio
async def test_get_monthly_percolation(calculator, mock_precipitation_data, mock_evaporation_data):
    """Test monthly percolation calculation"""
    calculator.dmi_parser.fetch_precipitation_data.return_value = mock_precipitation_data
    calculator.dmi_parser.fetch_evaporation_data.return_value = mock_evaporation_data

    result = await calculator.get_monthly_percolation(months=1)

    assert isinstance(result, gpd.GeoDataFrame)
    assert not result.empty
    assert 'percolation' in result.columns

@pytest.mark.asyncio
async def test_empty_data_handling(calculator):
    """Test handling of empty data"""
    calculator.dmi_parser.fetch_precipitation_data.return_value = gpd.GeoDataFrame()
    calculator.dmi_parser.fetch_evaporation_data.return_value = gpd.GeoDataFrame()

    result = await calculator.calculate_percolation()

    assert isinstance(result, gpd.GeoDataFrame)
    assert result.empty

@pytest.mark.asyncio
async def test_error_handling(calculator):
    """Test error handling"""
    calculator.dmi_parser.fetch_precipitation_data.side_effect = Exception("Test error")

    result = await calculator.calculate_percolation()

    assert isinstance(result, gpd.GeoDataFrame)
    assert result.empty