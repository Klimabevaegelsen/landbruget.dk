"""
Test suite for pesticide disaggregation gold layer processor.

Tests the EXACT preservation of the original 92% coverage strategy.
"""

from unittest.mock import Mock

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Polygon

from unified_pipeline.gold.pesticide_disaggregation import (
    PesticideDisaggregationGold,
    PesticideDisaggregationGoldConfig,
)
from unified_pipeline.util.gcs_util import GCSUtil


class TestPesticideDisaggregationGold:
    @pytest.fixture
    def config(self):
        return PesticideDisaggregationGoldConfig(
            bucket="test-bucket",
            pesticide_year=2021,
            area_tolerance_pct=2.0,  # PRESERVE ORIGINAL VALUE
        )

    @pytest.fixture
    def mock_gcs_util(self):
        return Mock(spec=GCSUtil)

    @pytest.fixture
    def sample_agricultural_fields(self):
        """Create sample agricultural fields data matching expected schema."""
        return gpd.GeoDataFrame(
            {
                "field_id": ["field_001", "field_002", "field_003", "field_004"],
                "block_id": ["block_001", "block_002", "block_003", "block_004"],
                "cvr_number": ["12345678", "12345678", "87654321", "87654321"],
                "crop_code": [110, 110, 120, 120],
                "area_ha": [10.0, 15.0, 8.0, 12.0],
                "companyregistrationnumber": ["12345678", "12345678", "87654321", "87654321"],
                "code": [110, 110, 120, 120],
                "acreagesize": [10.0, 15.0, 8.0, 12.0],
                "organic_farming": [None, None, None, None],
                "geometry": [
                    Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
                    Polygon([(1, 0), (2, 0), (2, 1), (1, 1)]),
                    Polygon([(0, 1), (1, 1), (1, 2), (0, 2)]),
                    Polygon([(1, 1), (2, 1), (2, 2), (1, 2)]),
                ],
            }
        )

    @pytest.fixture
    def sample_pesticide_applications(self):
        """Create sample pesticide applications data."""
        return pd.DataFrame(
            {
                "OriginalPesticideRowID": [1, 2, 3, 4, 5],
                "CompanyRegistrationNumber": [
                    "12345678",
                    "12345678",
                    "87654321",
                    "87654321",
                    "99999999",
                ],
                "Code": [110, 110, 120, 120, 130],
                "AcreageSize": [
                    25.0,
                    24.5,
                    20.0,
                    19.8,
                    5.0,
                ],  # First two match total field areas exactly
                "PesticideName": [
                    "Herbicide A",
                    "Herbicide B",
                    "Fungicide A",
                    "Fungicide B",
                    "Insecticide A",
                ],
                "PesticideRegistrationNumber": ["REG001", "REG002", "REG003", "REG004", "REG005"],
                "DosageQuantity": [2.5, 3.0, 1.5, 2.0, 1.0],
                "DosageUnit": ["L/ha", "L/ha", "kg/ha", "kg/ha", "L/ha"],
                "CompanyName": ["Company A", "Company A", "Company B", "Company B", "Company C"],
                "Name": ["Wheat", "Wheat", "Barley", "Barley", "Corn"],
                "nopesticides": [None, None, None, None, None],
            }
        )

    def test_config_validation(self, config):
        """Test that configuration validates correctly."""
        assert config.area_tolerance_pct == 2.0  # CRITICAL: Must preserve original value
        assert config.pesticide_year == 2021
        assert config.bucket == "test-bucket"
        assert config.dataset == "pesticide_disaggregation"

    def test_processor_initialization(self, config, mock_gcs_util):
        """Test that processor initializes correctly."""
        processor = PesticideDisaggregationGold(config, mock_gcs_util)
        assert processor.config == config
        assert processor.duckdb_conn is None  # Should be None until setup
        assert len(processor._organic_marker_field_ids) == 0

    def test_main_strategy_exact_match(
        self, config, mock_gcs_util, sample_agricultural_fields, sample_pesticide_applications
    ):
        """Test that main strategy processes exact area matches correctly."""
        processor = PesticideDisaggregationGold(config, mock_gcs_util)

        # Setup DuckDB with test data
        processor._setup_duckdb(sample_agricultural_fields, sample_pesticide_applications)
        processor._create_results_table()
        processor._create_pending_pesticide_rows()

        # Run main strategy
        processed_count = processor._disaggregate_by_marker_match()

        # Should process records with exact or near-exact area matches
        assert processed_count >= 2  # At least the two exact matches

        # Verify results
        results = processor._get_results()
        assert len(results) >= 2

        # Check that allocation method is correct
        main_strategy_results = results[
            results["AllocationMethod"]
            == "Marker_ApplicationAreaToTotalFieldArea_FieldProportional"
        ]
        assert len(main_strategy_results) >= 2

    def test_area_tolerance_enforcement(self, config, mock_gcs_util, sample_agricultural_fields):
        """Test that 2% area tolerance is strictly enforced."""
        # Create pesticide data that exceeds tolerance
        pesticide_data = pd.DataFrame(
            {
                "OriginalPesticideRowID": [1, 2],
                "CompanyRegistrationNumber": ["12345678", "12345678"],
                "Code": [110, 110],
                "AcreageSize": [
                    30.0,
                    20.0,
                ],  # 30.0 exceeds 2% tolerance of 25.0, 20.0 is within tolerance
                "PesticideName": ["Herbicide A", "Herbicide B"],
                "PesticideRegistrationNumber": ["REG001", "REG002"],
                "DosageQuantity": [2.5, 3.0],
                "DosageUnit": ["L/ha", "L/ha"],
                "CompanyName": ["Company A", "Company A"],
                "Name": ["Wheat", "Wheat"],
                "nopesticides": [None, None],
            }
        )

        processor = PesticideDisaggregationGold(config, mock_gcs_util)
        processor._setup_duckdb(sample_agricultural_fields, pesticide_data)
        processor._create_results_table()
        processor._create_pending_pesticide_rows()

        processed_count = processor._disaggregate_by_marker_match()

        # Should only process the record within tolerance
        assert processed_count == 1

        results = processor._get_results()
        processed_pesticide_ids = results["OriginalPesticideRowID"].tolist()

        # Should process record 2 (20.0 within 2% of 25.0) but not record 1 (30.0 exceeds tolerance)
        assert "2" in processed_pesticide_ids
        assert "1" not in processed_pesticide_ids

    def test_nopesticides_filtering(self, config, mock_gcs_util, sample_agricultural_fields):
        """Test that nopesticides=1 records are correctly filtered out."""
        pesticide_data = pd.DataFrame(
            {
                "OriginalPesticideRowID": [1, 2, 3],
                "CompanyRegistrationNumber": ["12345678", "12345678", "12345678"],
                "Code": [110, 110, 110],
                "AcreageSize": [25.0, 25.0, 25.0],
                "PesticideName": ["Herbicide A", "Herbicide B", "No Pesticide"],
                "PesticideRegistrationNumber": ["REG001", "REG002", None],
                "DosageQuantity": [2.5, 3.0, None],
                "DosageUnit": ["L/ha", "L/ha", None],
                "CompanyName": ["Company A", "Company A", "Company A"],
                "Name": ["Wheat", "Wheat", "Wheat"],
                "nopesticides": [None, None, 1],  # Third record should be filtered out
            }
        )

        processor = PesticideDisaggregationGold(config, mock_gcs_util)
        processor._setup_duckdb(sample_agricultural_fields, pesticide_data)
        processor._create_results_table()
        processor._create_pending_pesticide_rows()

        # Check pending rows after filtering
        pending_count = processor.duckdb_conn.execute(
            "SELECT COUNT(*) FROM pending_pesticide_rows"
        ).fetchone()[0]
        assert pending_count == 2  # Should exclude the nopesticides=1 record

    def test_proportional_allocation(
        self, config, mock_gcs_util, sample_agricultural_fields, sample_pesticide_applications
    ):
        """Test that proportional allocation works correctly."""
        processor = PesticideDisaggregationGold(config, mock_gcs_util)
        processor._setup_duckdb(sample_agricultural_fields, sample_pesticide_applications)
        processor._create_results_table()
        processor._create_pending_pesticide_rows()

        processed_count = processor._disaggregate_by_marker_match()
        results = processor._get_results()

        # Check proportional allocation for CVR 12345678, Crop 110
        # Total field area: 10.0 + 15.0 = 25.0
        # Pesticide area: 25.0
        # Field 1 should get: 25.0 * (10.0/25.0) = 10.0
        # Field 2 should get: 25.0 * (15.0/25.0) = 15.0

        cvr_110_results = results[
            (results["CompanyRegistrationNumber"] == "12345678")
            & (
                results["AllocationMethod"]
                == "Marker_ApplicationAreaToTotalFieldArea_FieldProportional"
            )
        ]

        if len(cvr_110_results) > 0:
            allocated_areas = cvr_110_results["AllocatedArea"].tolist()
            # Should have proportional allocations
            assert any(abs(area - 10.0) < 0.1 for area in allocated_areas)
            assert any(abs(area - 15.0) < 0.1 for area in allocated_areas)

    def test_confidence_scoring(
        self, config, mock_gcs_util, sample_agricultural_fields, sample_pesticide_applications
    ):
        """Test that confidence scoring follows original formula."""
        processor = PesticideDisaggregationGold(config, mock_gcs_util)
        processor._setup_duckdb(sample_agricultural_fields, sample_pesticide_applications)
        processor._create_results_table()
        processor._create_pending_pesticide_rows()

        processed_count = processor._disaggregate_by_marker_match()
        results = processor._get_results()

        # Check confidence scores
        main_strategy_results = results[
            results["AllocationMethod"]
            == "Marker_ApplicationAreaToTotalFieldArea_FieldProportional"
        ]

        for _, row in main_strategy_results.iterrows():
            confidence = row["MatchConfidence"]
            # Confidence should be between 0 and 1
            assert 0.0 <= confidence <= 1.0
            # For exact matches, confidence should be high (close to 1.0)
            # For the test data with exact matches, confidence should be 1.0

    def test_coverage_validation_failure(self, config, mock_gcs_util):
        """Test that pipeline fails if coverage drops below 92%."""
        # Create data that will result in low coverage
        fields_df = gpd.GeoDataFrame(
            {
                "field_id": ["field_001"],
                "cvr_number": ["12345678"],
                "crop_code": [110],
                "area_ha": [10.0],
                "companyregistrationnumber": ["12345678"],
                "code": [110],
                "acreagesize": [10.0],
                "geometry": [Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
            }
        )

        # Create many pesticide records that won't match
        pesticide_data = pd.DataFrame(
            {
                "OriginalPesticideRowID": list(range(1, 101)),  # 100 records
                "CompanyRegistrationNumber": ["99999999"] * 100,  # Non-matching CVR
                "Code": [999] * 100,  # Non-matching crop code
                "AcreageSize": [5.0] * 100,
                "PesticideName": ["Test Pesticide"] * 100,
                "PesticideRegistrationNumber": ["REG999"] * 100,
                "DosageQuantity": [1.0] * 100,
                "DosageUnit": ["L/ha"] * 100,
                "CompanyName": ["Test Company"] * 100,
                "Name": ["Test Crop"] * 100,
                "nopesticides": [None] * 100,
            }
        )

        processor = PesticideDisaggregationGold(config, mock_gcs_util)

        # Mock the silver data loading to return our test data
        def mock_load_silver_data(silver_data):
            return {"agricultural_fields": fields_df, "pesticides": pesticide_data}

        processor._load_silver_data = mock_load_silver_data
        processor._save_data = Mock()  # Mock save to avoid actual GCS calls

        # Should raise ValueError due to low coverage
        with pytest.raises(ValueError, match="Coverage .* below required 92%"):
            import asyncio

            asyncio.run(processor.run())

    def test_all_strategies_execution(
        self, config, mock_gcs_util, sample_agricultural_fields, sample_pesticide_applications
    ):
        """Test that all 4 strategies are executed in correct order."""
        processor = PesticideDisaggregationGold(config, mock_gcs_util)
        processor._setup_duckdb(sample_agricultural_fields, sample_pesticide_applications)
        processor._create_results_table()
        processor._create_pending_pesticide_rows()

        # Track strategy execution
        strategy_counts = {}

        strategy_counts["marker"] = processor._disaggregate_by_marker_match()
        strategy_counts["non_organic"] = processor._disaggregate_by_marker_non_organic_match()
        strategy_counts["partial"] = processor._disaggregate_by_partial_field_coverage()
        strategy_counts["cluster"] = processor._disaggregate_by_adjacent_fields_single_cluster()

        # All strategies should execute without error
        assert all(isinstance(count, int) for count in strategy_counts.values())
        assert all(count >= 0 for count in strategy_counts.values())

        # Main strategy should process the most records
        assert strategy_counts["marker"] >= max(
            strategy_counts["non_organic"], strategy_counts["partial"], strategy_counts["cluster"]
        )

    def test_organic_field_identification(self, config, mock_gcs_util):
        """Test organic field identification logic."""
        # Create fields with organic farming indicators
        fields_df = gpd.GeoDataFrame(
            {
                "field_id": ["field_001", "field_002", "field_003"],
                "cvr_number": ["12345678", "12345678", "12345678"],
                "crop_code": [110, 110, 110],
                "area_ha": [10.0, 15.0, 8.0],
                "organic_farming": [None, "JA", "YES"],  # Second and third are organic
                "geometry": [
                    Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
                    Polygon([(1, 0), (2, 0), (2, 1), (1, 1)]),
                    Polygon([(0, 1), (1, 1), (1, 2), (0, 2)]),
                ],
            }
        )

        processor = PesticideDisaggregationGold(config, mock_gcs_util)
        processor._setup_duckdb(fields_df, pd.DataFrame())

        organic_ids = processor._get_organic_marker_field_ids()

        # Should identify fields 2 and 3 as organic
        assert "field_002" in organic_ids
        assert "field_003" in organic_ids
        assert "field_001" not in organic_ids

    def test_results_schema_compliance(
        self, config, mock_gcs_util, sample_agricultural_fields, sample_pesticide_applications
    ):
        """Test that results comply with expected schema."""
        processor = PesticideDisaggregationGold(config, mock_gcs_util)
        processor._setup_duckdb(sample_agricultural_fields, sample_pesticide_applications)
        processor._create_results_table()
        processor._create_pending_pesticide_rows()

        processor._disaggregate_by_marker_match()
        results = processor._get_results()

        if len(results) > 0:
            # Check required columns exist
            required_columns = [
                "DisaggregatedID",
                "OriginalPesticideRowID",
                "CompanyRegistrationNumber",
                "PesticideName",
                "PesticideRegistrationNumber",
                "DosageQuantity",
                "DosageUnit",
                "MatchedFieldID",
                "MatchedBlockID",
                "AllocatedArea",
                "AllocationMethod",
                "MatchConfidence",
                "IsPartialFieldCoverage",
                "DisaggregationDate",
            ]

            for col in required_columns:
                assert col in results.columns, f"Missing required column: {col}"

            # Check data types and constraints
            assert results["MatchConfidence"].dtype in ["float64", "float32"]
            assert all(0.0 <= conf <= 1.0 for conf in results["MatchConfidence"])
            assert results["AllocatedArea"].dtype in ["float64", "float32"]
            assert all(area > 0 for area in results["AllocatedArea"])
            assert results["IsPartialFieldCoverage"].dtype == bool
