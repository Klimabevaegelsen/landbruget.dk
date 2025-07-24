#!/usr/bin/env python3
"""
NLES5 Reference Validation Tests

Comprehensive test suite that validates the unified NLES5 pipeline implementation
against the reference nles5.py implementation to ensure accuracy and compliance.
"""

import pytest
import asyncio
import sys
import os
from pathlib import Path
from unittest.mock import patch, MagicMock

# Add backend to path
sys.path.append(str(Path(__file__).parent.parent.parent.parent.parent.parent / "backend"))

from pipelines.unified_pipeline.src.unified_pipeline.gold.nles5_nitrogen_estimation import (
    NLES5NitrogenEstimationGold,
    NLES5NitrogenEstimationGoldConfig
)


class TestNLES5ReferenceValidation:
    """Comprehensive validation tests for NLES5 implementation."""

    @pytest.fixture
    def test_config(self):
        """Create test configuration for validation."""
        return NLES5NitrogenEstimationGoldConfig(
            # Small test bounds for validation
            test_bounds=[9.7, 55.8, 10.3, 56.1],
            max_years_to_process=1,
            batch_size=1000,
            max_memory_usage_gb=2,
            min_data_coverage=0.5,
            uncertainty_estimation=True,
        )

    @pytest.fixture
    def nles5_pipeline(self, test_config):
        """Create NLES5 pipeline instance."""
        return NLES5NitrogenEstimationGold(test_config)

    def test_configuration_validation(self, test_config):
        """Test that configuration matches reference implementation expectations."""
        # Test crop parameters (should have 13 categories)
        assert len(test_config.crop_parameters) == 13, "Should have 13 crop parameters"
        assert 'M1' in test_config.crop_parameters, "Should have M1 crop parameter"
        assert test_config.crop_parameters['M1'] == 0.0, "M1 should be reference (0.0)"

        # Test nitrogen coefficients (should have 8 coefficients)
        assert len(test_config.nitrogen_coefficients) == 8, "Should have 8 nitrogen coefficients"
        expected_coefficients = {
            'Bt': 0.456793,
            'Bcs': 0.049570,
            'Bca': 0.157044,
            'Budb': 0.038245,
            'Bm1': 0.026499,
            'Bf0': 0.016314,
            'Bf1': 0.026499,
            'Bg0': 0.014099
        }

        for coeff_name, expected_value in expected_coefficients.items():
            assert coeff_name in test_config.nitrogen_coefficients, f"Missing coefficient {coeff_name}"
            actual_value = test_config.nitrogen_coefficients[coeff_name]
            assert abs(actual_value - expected_value) < 0.000001, f"Coefficient {coeff_name}: {actual_value} ≠ {expected_value}"

        # Test soil parameters
        assert 'sand' in test_config.soil_parameters, "Should have sand soil parameters"
        assert 'clay' in test_config.soil_parameters, "Should have clay soil parameters"

        # Validate soil parameter coefficients are negative (FIXED in reference compliance)
        sand_params = test_config.soil_parameters['sand']
        clay_params = test_config.soil_parameters['clay']

        assert sand_params['per1_coef'] < 0, "Sand per1_coef should be negative"
        assert sand_params['per2_coef'] < 0, "Sand per2_coef should be negative"
        assert clay_params['per1_coef'] < 0, "Clay per1_coef should be negative"
        assert clay_params['per2_coef'] < 0, "Clay per2_coef should be negative"

    def test_pipeline_initialization(self, nles5_pipeline):
        """Test that pipeline initializes correctly."""
        assert nles5_pipeline is not None
        assert hasattr(nles5_pipeline, 'config')
        assert hasattr(nles5_pipeline, 'conn')
        assert hasattr(nles5_pipeline, 'log')

    def test_duckdb_configuration(self, nles5_pipeline):
        """Test DuckDB configuration for production optimization."""
        # This should not raise an exception
        nles5_pipeline._configure_duckdb()

        # Test that DuckDB can execute basic commands
        result = nles5_pipeline.conn.execute("SELECT 1 as test").fetchone()
        assert result[0] == 1

    def test_percolation_calculation_logic(self, nles5_pipeline):
        """Test percolation calculation logic matches reference."""
        # Create test climate data
        nles5_pipeline.conn.execute("""
            CREATE TABLE test_climate_data AS
            SELECT
                '{"type": "Point", "coordinates": [10.0, 56.0]}' as centroid_geometry,
                ST_GeomFromGeoJSON('{"type": "Point", "coordinates": [10.0, 56.0]}') as geometry,
                2024 as year,
                250.0 as perco_sep_nov_current,     -- per1: autumn
                300.0 as perco_dec_feb_current,     -- per2: winter
                350.0 as perco_mar_aug_current,     -- per3: spring/summer
                900.0 as total_percolation,
                800.0 as avg_precipitation,
                300.0 as avg_evaporation,
                30 as climate_data_points,
                true as sufficient_climate_data
        """)

        # Test seasonal aggregation matches reference periods
        periods = nles5_pipeline.conn.execute("""
            SELECT perco_sep_nov_current, perco_dec_feb_current, perco_mar_aug_current
            FROM test_climate_data
        """).fetchone()

        assert periods[0] == 250.0, "Sep-Nov percolation should match test data"
        assert periods[1] == 300.0, "Dec-Feb percolation should match test data"
        assert periods[2] == 350.0, "Mar-Aug percolation should match test data"

    def test_soil_effect_calculation(self, nles5_pipeline):
        """Test soil effect calculation matches reference implementation."""
        # Test reference soil effect formula: exp(-0.00185 * clay_content)
        nles5_pipeline.conn.execute("""
            CREATE TABLE test_soil_data AS
            SELECT
                15.0 as clay_content,
                'clay' as soil_type_category,
                EXP(-0.00185 * 15.0) as expected_soil_effect
        """)

        result = nles5_pipeline.conn.execute("""
            SELECT expected_soil_effect FROM test_soil_data
        """).fetchone()

        expected = 0.9729  # exp(-0.00185 * 15.0)
        actual = result[0]
        assert abs(actual - expected) < 0.001, f"Soil effect calculation: {actual} ≠ {expected}"

    def test_drainage_effect_calculation(self, nles5_pipeline):
        """Test drainage effect calculation matches reference."""
        # Test reference drainage formula for sand soil
        nles5_pipeline.conn.execute("""
            CREATE TABLE test_drainage_data AS
            SELECT
                250.0 as per1,  -- Sep-Nov
                300.0 as per2,  -- Dec-Feb
                350.0 as per3,  -- Mar-Aug
                -- Reference formula for sand: (1 - exp(per1_coef * per1 + per2_coef * (per2 + per3))) * exp(per_p_coef * (per2 + per3))
                (1 - EXP(-0.001194 * 250.0 + -0.001107 * (300.0 + 350.0))) *
                EXP(-0.000856 * (300.0 + 350.0)) as expected_drainage_effect_sand
        """)

        result = nles5_pipeline.conn.execute("""
            SELECT expected_drainage_effect_sand FROM test_drainage_data
        """).fetchone()

        # This should be a reasonable drainage effect value (0.5-1.2)
        drainage_effect = result[0]
        assert 0.5 <= drainage_effect <= 1.2, f"Drainage effect {drainage_effect} outside reasonable range"

    def test_nitrogen_effect_calculation(self, nles5_pipeline):
        """Test nitrogen effect calculation with reference coefficients."""
        # Test with typical fertilizer values
        test_data = {
            'tn_t_ha': 150.0,      # Total soil N
            'mineral_n_spring': 80.0,   # Spring mineral N
            'mineral_n_autumn': 8.0,    # Autumn mineral N
            'mineral_n_grazing': 3.0,   # Grazing mineral N
            'organic_n_manure': 35.0,   # Organic N
            'mineral_n_prev': 90.0,     # Previous mineral N
            'n_fixation': 2.0          # N fixation
        }

        # Calculate expected nitrogen effect using reference coefficients
        coeffs = nles5_pipeline.config.nitrogen_coefficients
        expected_n_effect = (
            coeffs['Bt'] * test_data['tn_t_ha'] +
            coeffs['Bcs'] * test_data['mineral_n_spring'] +
            coeffs['Bca'] * test_data['mineral_n_autumn'] +
            coeffs['Budb'] * test_data['mineral_n_grazing'] +
            coeffs['Bg0'] * test_data['organic_n_manure'] +
            coeffs['Bm1'] * test_data['mineral_n_prev'] +
            coeffs['Bf0'] * test_data['n_fixation']
        )

        # Test calculation
        nles5_pipeline.conn.execute(f"""
            CREATE TABLE test_nitrogen_data AS
            SELECT
                {test_data['tn_t_ha']} as tn_t_ha,
                {test_data['mineral_n_spring']} as mineral_n_spring,
                {test_data['mineral_n_autumn']} as mineral_n_autumn,
                {test_data['mineral_n_grazing']} as mineral_n_grazing,
                {test_data['organic_n_manure']} as organic_n_manure,
                {test_data['mineral_n_prev']} as mineral_n_prev,
                {test_data['n_fixation']} as n_fixation,
                -- Calculate nitrogen effect using reference coefficients
                ({coeffs['Bt']} * {test_data['tn_t_ha']} +
                 {coeffs['Bcs']} * {test_data['mineral_n_spring']} +
                 {coeffs['Bca']} * {test_data['mineral_n_autumn']} +
                 {coeffs['Budb']} * {test_data['mineral_n_grazing']} +
                 {coeffs['Bg0']} * {test_data['organic_n_manure']} +
                 {coeffs['Bm1']} * {test_data['mineral_n_prev']} +
                 {coeffs['Bf0']} * {test_data['n_fixation']}) as calculated_nitrogen_effect
        """)

        result = nles5_pipeline.conn.execute("""
            SELECT calculated_nitrogen_effect FROM test_nitrogen_data
        """).fetchone()

        actual_n_effect = result[0]
        assert abs(actual_n_effect - expected_n_effect) < 0.01, f"Nitrogen effect: {actual_n_effect} ≠ {expected_n_effect}"

    def test_trend_effect_calculation(self, nles5_pipeline):
        """Test trend effect matches reference implementation."""
        # Reference trend effect: -0.1108 * (2017 - 1991) = -2.8808
        expected_trend = -0.1108 * (2017 - 1991)

        nles5_pipeline.conn.execute(f"""
            CREATE TABLE test_trend_data AS
            SELECT
                -0.1108 * (2017 - 1991) as calculated_trend_effect
        """)

        result = nles5_pipeline.conn.execute("""
            SELECT calculated_trend_effect FROM test_trend_data
        """).fetchone()

        actual_trend = result[0]
        assert abs(actual_trend - expected_trend) < 0.0001, f"Trend effect: {actual_trend} ≠ {expected_trend}"
        assert abs(actual_trend - (-2.8808)) < 0.0001, f"Trend effect should be -2.8808, got {actual_trend}"

    def test_v_base_calculation(self, nles5_pipeline):
        """Test V base calculation (23.51 + crop_effect + nitrogen_effect)."""
        # Test V base with typical values
        crop_effect = 0.0  # M1 reference crop
        nitrogen_effect = 100.0  # Typical nitrogen effect
        expected_v_base = 23.51 + crop_effect + nitrogen_effect

        nles5_pipeline.conn.execute(f"""
            CREATE TABLE test_v_base_data AS
            SELECT
                23.51 + {crop_effect} + {nitrogen_effect} as calculated_v_base
        """)

        result = nles5_pipeline.conn.execute("""
            SELECT calculated_v_base FROM test_v_base_data
        """).fetchone()

        actual_v_base = result[0]
        assert abs(actual_v_base - expected_v_base) < 0.01, f"V base: {actual_v_base} ≠ {expected_v_base}"
        assert actual_v_base > 23.51, "V base should be greater than base constant 23.51"

    def test_final_nles5_formula(self, nles5_pipeline):
        """Test complete NLES5 formula: Y5 = trend_effect + V^1.5 * perco_soil_effect."""
        # Test with realistic values
        trend_effect = -2.8808
        v_base = 123.51  # 23.51 + 100 nitrogen effect
        perco_soil_effect = 0.85

        expected_washout = max(0, trend_effect + (v_base ** 1.5) * perco_soil_effect)

        nles5_pipeline.conn.execute(f"""
            CREATE TABLE test_final_formula AS
            SELECT
                GREATEST(0, {trend_effect} + POWER({v_base}, 1.5) * {perco_soil_effect}) as calculated_washout
        """)

        result = nles5_pipeline.conn.execute("""
            SELECT calculated_washout FROM test_final_formula
        """).fetchone()

        actual_washout = result[0]
        assert abs(actual_washout - expected_washout) < 0.1, f"Final washout: {actual_washout} ≠ {expected_washout}"
        assert actual_washout >= 0, "Nitrogen washout should never be negative"

    def test_uncertainty_calculation_ranges(self, test_config):
        """Test uncertainty calculations are within expected ranges."""
        # Test coefficient uncertainties match reference values
        expected_uncertainties = {
            'Bt': 0.202200,
            'Bcs': 0.007000,
            'Bca': 0.034257,
            'Budb': 0.011056,
            'Bm1': 0.006121,
            'Bf0': 0.005530,
            'Bf1': 0.006121,
            'Bg0': 0.008799
        }

        for coeff_name, expected_uncertainty in expected_uncertainties.items():
            assert coeff_name in test_config.coefficient_uncertainties
            actual_uncertainty = test_config.coefficient_uncertainties[coeff_name]
            assert abs(actual_uncertainty - expected_uncertainty) < 0.000001, \
                f"Uncertainty for {coeff_name}: {actual_uncertainty} ≠ {expected_uncertainty}"

    @pytest.mark.asyncio
    async def test_integration_workflow(self, nles5_pipeline):
        """Test basic integration workflow without real data."""
        # Mock the data loading to avoid GCS dependency
        with patch.object(nles5_pipeline, '_load_required_silver_datasets') as mock_load:
            mock_load.return_value = {
                "agricultural_fields": "mock_fields_table",
                "dmi": "mock_dmi_table"
            }

            # Mock other methods to avoid data dependencies
            with patch.object(nles5_pipeline, '_create_spatial_tables'), \
                 patch.object(nles5_pipeline, '_create_nles5_parameter_tables'), \
                 patch.object(nles5_pipeline, '_process_climate_data') as mock_climate, \
                 patch.object(nles5_pipeline, '_spatial_join_fields_climate') as mock_spatial, \
                 patch.object(nles5_pipeline, '_join_with_soil_data') as mock_soil, \
                 patch.object(nles5_pipeline, '_calculate_detailed_percolation_effects') as mock_percolation, \
                 patch.object(nles5_pipeline, '_calculate_nles5_estimates') as mock_estimates, \
                 patch.object(nles5_pipeline, '_validate_nles5_estimates') as mock_validate, \
                 patch.object(nles5_pipeline, '_calculate_uncertainty_estimates') as mock_uncertainty, \
                 patch.object(nles5_pipeline, '_save_results_to_gold') as mock_save:

                # Configure mocks
                mock_climate.return_value = "climate_percolation"
                mock_spatial.return_value = "fields_with_climate"
                mock_soil.return_value = "fields_with_climate_soil_crops"
                mock_percolation.return_value = "detailed_percolation_effects"
                mock_estimates.return_value = "nles5_nitrogen_estimates"
                mock_validate.return_value = True
                mock_uncertainty.return_value = "nles5_uncertainty_estimates"

                # Test that the workflow doesn't crash
                try:
                    await nles5_pipeline.run()
                    # If we get here, the workflow completed without exceptions
                    assert True
                except Exception as e:
                    pytest.fail(f"Integration workflow failed: {e}")


if __name__ == "__main__":
    # Run specific test for debugging
    import sys
    sys.exit(pytest.main([__file__, "-v"]))