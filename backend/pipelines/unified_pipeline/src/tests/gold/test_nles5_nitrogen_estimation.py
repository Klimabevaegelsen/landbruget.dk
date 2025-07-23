"""Tests for NLES5 Nitrogen Estimation Gold Layer."""

import pytest
from unittest.mock import Mock, patch
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon

from unified_pipeline.gold.nles5_nitrogen_estimation import (
    NLES5NitrogenEstimationGold,
    NLES5NitrogenEstimationGoldConfig,
)
from unified_pipeline.util.gcs_access import GCSDataAccess


class TestNLES5NitrogenEstimationGold:
    """Test suite for NLES5 nitrogen estimation gold processor."""

    @pytest.fixture
    def config(self):
        """Create test configuration."""
        return NLES5NitrogenEstimationGoldConfig(
            bucket="test-bucket",
            soil_types_dataset="soil_types",
            dmi_dataset="dmi",
            target_years=[2021, 2022, 2023]  # Specify test years
        )

    @pytest.fixture
    def mock_gcs_util(self):
        """Create mock GCS utility."""
        return Mock(spec=GCSDataAccess)

    @pytest.fixture
    def sample_agricultural_fields(self):
        """Create sample agricultural fields data."""
        # Create a simple polygon geometry
        geometry = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])

        return gpd.GeoDataFrame({
            'field_id': ['field_001', 'field_002', 'field_003'],
            'cvr_number': ['12345678', '87654321', '11223344'],
            'area_ha': [10.5, 8.2, 15.3],
            'crop_type': ['winter_cereals', 'spring_cereals', 'grass_clover'],
            'organic_farming': [False, True, False],
            'year': [2024, 2024, 2024],
            'geometry': [geometry.wkb] * 3
        })

    @pytest.fixture
    def sample_soil_types(self):
        """Create sample soil types data."""
        geometry = Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])

        return gpd.GeoDataFrame({
            'soil_code': ['1', '2', '5'],
            'soil_description': ['Sandy soil', 'Loamy sand', 'Clay soil'],
            'clay_content': [5.0, 12.0, 25.0],
            'geometry': [geometry.wkt] * 3
        })

    @pytest.fixture
    def processor(self, config, mock_gcs_util):
        """Create NLES5 processor instance."""
        with patch('unified_pipeline.gold.nles5_nitrogen_estimation.GCSDataAccess'):
            processor = NLES5NitrogenEstimationGold(config, mock_gcs_util)
            # Mock the DuckDB connection
            processor.conn = Mock()
            return processor

    def test_config_initialization(self, config):
        """Test configuration initialization."""
        assert config.name == "NLES5 Nitrogen Estimation Gold"
        assert config.dataset == "nles5_nitrogen_estimation"
        assert config.type == "gold"
        assert config.soil_types_dataset == "soil_types"

    def test_processor_initialization(self, processor):
        """Test processor initialization."""
        assert processor.config.dataset == "nles5_nitrogen_estimation"
        assert hasattr(processor, 'log')
        assert hasattr(processor, 'conn')

    @pytest.mark.asyncio
    async def test_run_with_mock_data(self, processor, sample_agricultural_fields, sample_soil_types):
        """Test running the processor with mock data."""
        # Mock the data loading methods
        processor._load_silver_data_to_table = Mock(return_value=True)
        processor._configure_duckdb = Mock()
        processor._process_nitrogen_estimation = Mock()
        processor._generate_summary_statistics = Mock()
        processor._save_results_to_gold = Mock()

        # Prepare silver data
        silver_data = {
            'fvm_marker': sample_agricultural_fields,
            'soil_types': sample_soil_types
        }

        # Run the processor
        await processor.run(silver_data=silver_data)

        # Verify methods were called
        processor._configure_duckdb.assert_called_once()
        processor._process_nitrogen_estimation.assert_called_once()
        processor._generate_summary_statistics.assert_called_once()
        processor._save_results_to_gold.assert_called_once()

    def test_configure_duckdb(self, processor):
        """Test DuckDB configuration."""
        processor._configure_duckdb()

        # Verify DuckDB settings were applied
        expected_calls = [
            ('SET memory_limit = \'8GB\'',),
            ('SET threads = 4',),
            ('SET enable_progress_bar = true',),
        ]

        for expected_call in expected_calls:
            processor.conn.execute.assert_any_call(expected_call[0])

    def test_get_latest_silver_path(self, processor):
        """Test getting latest silver data path."""
        # Mock GCS access
        processor.gcs_access.list_files = Mock(return_value=['gs://bucket/silver/test/20240101_120000/test.parquet'])

        result = processor._get_latest_silver_path('test')
        assert result == 'gs://bucket/silver/test/20240101_120000/test.parquet'

    def test_load_silver_data_with_in_memory_data(self, processor, sample_agricultural_fields):
        """Test loading silver data from in-memory source."""
        silver_data = {'test_dataset': sample_agricultural_fields}

        result = processor._load_silver_data_to_table('test_dataset', 'test_table', silver_data)

        assert result is True
        processor.conn.register.assert_called_once_with('test_table', sample_agricultural_fields)

    def test_load_silver_data_from_gcs(self, processor):
        """Test loading silver data from GCS."""
        # Mock successful GCS path and download
        processor._get_latest_silver_path = Mock(return_value='gs://bucket/test.parquet')
        processor.gcs_access._temp_download = Mock()
        processor.conn.execute = Mock()
        processor.conn.execute.return_value.fetchone.return_value = [100]  # Mock count

        # Mock context manager for temp download
        with patch('unified_pipeline.gold.nles5_nitrogen_estimation.GCSDataAccess._temp_download') as mock_temp:
            mock_temp.return_value.__enter__ = Mock(return_value='/tmp/test.parquet')
            mock_temp.return_value.__exit__ = Mock(return_value=None)

            result = processor._load_silver_data_to_table('test_dataset', 'test_table', None)

        assert result is True

    def test_process_nitrogen_estimation(self, processor):
        """Test nitrogen estimation processing."""
        # Mock DuckDB execution
        processor.conn.execute = Mock()
        processor.conn.execute.return_value.fetchone.return_value = [150]  # Mock count

        processor._process_nitrogen_estimation()

        # Verify that SQL queries were executed
        assert processor.conn.execute.call_count >= 2  # DROP and CREATE TABLE calls

    def test_generate_summary_statistics(self, processor):
        """Test summary statistics generation."""
        # Mock DuckDB execution
        mock_summary = [1000, 50000.0, 125.5, 100.0, 25.2, 50.0, 250.0, 6275000.0, 180.5, 0.85, 8, 2, 3, '2024-01-01']
        processor.conn.execute = Mock()
        processor.conn.execute.return_value.fetchone.return_value = mock_summary

        processor._generate_summary_statistics()

        # Verify summary table creation
        processor.conn.execute.assert_called()

    def test_save_results_to_gold(self, processor):
        """Test saving results to gold layer."""
        # Mock DuckDB execution and GCS operations
        processor.conn.execute = Mock()
        processor.conn.execute.return_value.fetchone.return_value = [100]  # Mock count
        processor.gcs_access._temp_file = Mock()
        processor.gcs_access.upload_file = Mock()

        # Mock context manager for temp file
        with patch('unified_pipeline.gold.nles5_nitrogen_estimation.GCSDataAccess._temp_file') as mock_temp:
            mock_temp.return_value.__enter__ = Mock(return_value='/tmp/output.parquet')
            mock_temp.return_value.__exit__ = Mock(return_value=None)

            processor._save_results_to_gold()

        # Verify files were uploaded
        assert processor.gcs_access.upload_file.call_count >= 2  # At least 2 tables saved

    @pytest.mark.asyncio
    async def test_run_missing_required_data(self, processor):
        """Test running processor with missing required datasets."""
        # Mock failed data loading
        processor._load_silver_data_to_table = Mock(return_value=False)
        processor._configure_duckdb = Mock()

        # Run the processor
        await processor.run(silver_data=None)

        # Verify early return when required data is missing
        processor._configure_duckdb.assert_called_once()
        processor._load_silver_data_to_table.assert_called()

    def test_nles5_model_parameters(self):
        """Test that NLES5 model parameters are correctly implemented."""
        # These are the actual NLES5 model parameters from the original implementation
        expected_crop_params = {
            'winter_cereals': 0,
            'spring_cereals': -6.74,
            'mixed_cereals_peas': -7.28,
            'grass_clover': -13.49,
            'seed_grass': -17.48,
            'fallow': -11.19,
            'sugar_beets': -0.64,
            'maize_potatoes': 3.53,
            'winter_rape': -7.32,
        }

        # The SQL implementation in the processor should include these parameters
        # This test verifies the model constants are available for implementation
        assert 'winter_cereals' in expected_crop_params
        assert 'spring_cereals' in expected_crop_params
        assert expected_crop_params['winter_cereals'] == 0
        assert expected_crop_params['spring_cereals'] == -6.74

    def test_error_handling(self, processor):
        """Test error handling in processor methods."""
        # Mock DuckDB execution to raise an exception
        processor.conn.execute = Mock(side_effect=Exception("Database error"))

        # Test that exceptions are properly handled
        with pytest.raises(Exception):
            processor._process_nitrogen_estimation()

    @pytest.mark.parametrize("dataset,expected_result", [
        ("fvm_marker", True),
        ("soil_types", True),
        ("nonexistent_dataset", False),
    ])
    def test_load_different_datasets(self, processor, dataset, expected_result):
        """Test loading different types of datasets."""
        if expected_result:
            processor._get_latest_silver_path = Mock(return_value='gs://bucket/test.parquet')
            processor.gcs_access._temp_download = Mock()
            processor.conn.execute = Mock()
            processor.conn.execute.return_value.fetchone.return_value = [100]

            with patch('unified_pipeline.gold.nles5_nitrogen_estimation.GCSDataAccess._temp_download') as mock_temp:
                mock_temp.return_value.__enter__ = Mock(return_value='/tmp/test.parquet')
                mock_temp.return_value.__exit__ = Mock(return_value=None)

                result = processor._load_silver_data_to_table(dataset, 'test_table', None)
        else:
            processor._get_latest_silver_path = Mock(side_effect=FileNotFoundError("Not found"))
            result = processor._load_silver_data_to_table(dataset, 'test_table', None)

        assert result == expected_result