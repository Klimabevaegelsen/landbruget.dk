import pytest
import geopandas as gpd
import os
from backend.pipelines.nles5.nles5 import NLES5Calculator
from src.sources.parsers.agricultural_fields import AgriculturalFields
from src.sources.parsers.dmi import DMIParser
from src.sources.static.fertilizer.parser import CatchCrops, FertilizerAccounts, FieldPlanFertilizer
from backend.pipelines.nles5.percolation import PercolationCalculator
from src.config import SOURCES
from google.cloud import storage

@pytest.mark.asyncio
async def test_nles5_calculation():
    """Test the NLES5 calculation with real data"""
    # Set PRODUCTION to false to avoid GCS initialization
    os.environ['PRODUCTION'] = 'false'

    # Get configuration from SOURCES
    config = SOURCES['agricultural_fields']

    # Initialize storage client for testing
    storage_client = storage.Client()
    config['storage_client'] = storage_client

    # Initialize parsers
    ag_fields = AgriculturalFields(config)
    dmi_parser = DMIParser()
    percolation_calculator = PercolationCalculator(dmi_parser)
    catch_crops = CatchCrops()
    fertilizer_accounts = FertilizerAccounts()
    field_plan = FieldPlanFertilizer()

    # Initialize calculator
    calculator = NLES5Calculator(
        agricultural_fields_parser=ag_fields,
        percolation_calculator=percolation_calculator,
        catch_crops_parser=catch_crops,
        fertilizer_accounts_parser=fertilizer_accounts,
        field_plan_parser=field_plan
    )

    # Run calculation for a specific field
    field_id = "1-0"  # Using a real field ID from the database
    result = await calculator.calculate_nitrogen_washout(field_id)

    # Print results
    print("\nNLES5 Calculation Results:")
    print(f"Field ID: {result['field_id'].iloc[0]}")
    print(f"Crop Type: {result['crop_type'].iloc[0]}")
    print(f"Soil Type: {result['soil_type'].iloc[0]}")
    print(f"Total Percolation: {result['percolation_total'].iloc[0]:.2f} mm")
    print(f"Nitrogen Washout: {result['nitrogen_washout'].iloc[0]:.2f} kg N/ha")
    print(f"Drainage Effect: {result['drainage_effect'].iloc[0]:.4f}")
    print(f"Soil Effect: {result['soil_effect'].iloc[0]:.4f}")
    print(f"Nitrogen Effect: {result['nitrogen_effect'].iloc[0]:.2f}")
    print(f"Crop Effect: {result['crop_effect'].iloc[0]:.2f}")
    print(f"Trend Effect: {result['trend_effect'].iloc[0]:.2f}")

    # Print nitrogen parameters
    print("\nNitrogen Parameters:")
    print(f"Total Nitrogen: {result['total_nitrogen'].iloc[0]:.2f} kg N/ha")
    print(f"Mineral N Spring: {result['mineral_n_spring'].iloc[0]:.2f} kg N/ha")
    print(f"Mineral N Autumn: {result['mineral_n_autumn'].iloc[0]:.2f} kg N/ha")
    print(f"Mineral N Applied: {result['mineral_n_applied'].iloc[0]:.2f} kg N/ha")
    print(f"Organic N: {result['organic_n'].iloc[0]:.2f} kg N/ha")

    # Print percolation periods
    print("\nPercolation Periods:")
    print(f"Period 1 (Sep-Nov): {result['percolation_period1'].iloc[0]:.2f} mm")
    print(f"Period 2 (Dec-Feb): {result['percolation_period2'].iloc[0]:.2f} mm")
    print(f"Period 3 (Mar-Aug): {result['percolation_period3'].iloc[0]:.2f} mm")

    return result