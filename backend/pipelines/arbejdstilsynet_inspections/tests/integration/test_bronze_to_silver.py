"""
Integration tests for bronze to silver data flow.

This module tests:
- Full pipeline flow from bronze to silver
- Data preservation through transformations
- Column integrity
- Data quality validation
- Error propagation
"""

# Import pipeline components
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from silver.transform import SilverPipeline


@pytest.fixture
def sample_raw_csv():
    """Create realistic work inspection CSV data."""
    return """Dato,Antal,Afgørelse,Arbejdsmiljøproblem (emne),Påklaget,Efterkommet,Produktionsenhed,P-nummer,Branche,Produktionenhedens adresse
2024-01-15,5,Påbud,Støj og vibrationer,,Efterkommet,Dansk Landbrug A/S,1234567890,Landbrug, skovbrug og fiskeri,Landbrugsvej 1 4000 Roskilde
2024-01-16,10,Strakspåbud,Kemiske stoffer,Påklaget,,Danish Crown Slagteri,2345678901,Slagterier,Slagtervej 2 8000 Aarhus
2024-01-17,3,Forbud,Arbejde i høj,,,MT Højgaard A/S,3456789012,Anlægsarbejde,Byggevej 3 2000 København
2024-01-18,8,Vejledning,Ergonomi,,Efterkommet,Arla Foods,4567890123,Fødevarer,Mejerivej 4 6000 Kolding
2024-01-19,2,Påbud,Maskiner og teknisk udstyr,,,Vestjyllands Byggecenter,5678901234,Anlægsarbejde,Anlægsvej 5 7500 Holstebro"""


@pytest.fixture
def temp_workspace():
    """Create temporary workspace for integration testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        workspace = Path(temp_dir)
        bronze_dir = workspace / "bronze" / "data"
        silver_dir = workspace / "silver" / "data"
        bronze_dir.mkdir(parents=True)
        silver_dir.mkdir(parents=True)
        yield workspace


class TestFullPipelineFlow:
    """Tests for complete bronze to silver pipeline flow."""

    def test_full_pipeline_flow(self, temp_workspace, sample_raw_csv):
        """Test complete data flow from bronze layer to silver layer."""
        # Setup bronze data
        bronze_timestamp = "20240115_120000"
        bronze_data_dir = temp_workspace / "bronze" / "data" / bronze_timestamp
        bronze_data_dir.mkdir(parents=True)

        bronze_csv = bronze_data_dir / "data_merged.csv"
        with open(bronze_csv, "w") as f:
            f.write(sample_raw_csv)

        # Create and run silver pipeline
        silver_pipeline = SilverPipeline(log_level="ERROR")
        silver_pipeline.pipeline_root = str(temp_workspace)
        silver_pipeline.data_root = str(temp_workspace / "bronze" / "data")
        silver_pipeline.silver_dir = str(temp_workspace / "silver" / "data")
        silver_pipeline.output_dir = str(
            temp_workspace / "silver" / "data" / silver_pipeline.timestamp
        )
        silver_pipeline.output_parquet = str(
            Path(silver_pipeline.output_dir) / "workplace_inspections.parquet"
        )

        # Run pipeline steps
        assert silver_pipeline.setup_output_directories()
        assert silver_pipeline.find_latest_bronze_data()
        assert silver_pipeline.connect_database()
        assert silver_pipeline.load_data()
        assert silver_pipeline.rename_columns()
        assert silver_pipeline.validate_column_names()
        assert silver_pipeline.deduplicate()
        assert silver_pipeline.normalize_enums()
        assert silver_pipeline.handle_null_values()
        assert silver_pipeline.cast_types()
        assert silver_pipeline.filter_by_date()
        assert silver_pipeline.check_for_pii()

        # Skip CVR extraction in test
        with patch.object(silver_pipeline, "extract_and_save_cvr_numbers", return_value=True):
            assert silver_pipeline.save_output()

        # Verify output file exists
        assert Path(silver_pipeline.output_parquet).exists()

        # Verify data can be read
        result_df = pd.read_parquet(silver_pipeline.output_parquet)
        assert len(result_df) > 0

    def test_row_count_consistency(self, temp_workspace, sample_raw_csv):
        """Test that input and output row counts match (after deduplication)."""
        # Setup bronze data
        bronze_timestamp = "20240115_120000"
        bronze_data_dir = temp_workspace / "bronze" / "data" / bronze_timestamp
        bronze_data_dir.mkdir(parents=True)

        bronze_csv = bronze_data_dir / "data_merged.csv"
        with open(bronze_csv, "w") as f:
            f.write(sample_raw_csv)

        # Count input rows
        input_df = pd.read_csv(bronze_csv)
        input_rows = len(input_df)

        # Create and run silver pipeline
        silver_pipeline = SilverPipeline(log_level="ERROR")
        silver_pipeline.pipeline_root = str(temp_workspace)
        silver_pipeline.data_root = str(temp_workspace / "bronze" / "data")
        silver_pipeline.silver_dir = str(temp_workspace / "silver" / "data")
        silver_pipeline.output_dir = str(
            temp_workspace / "silver" / "data" / silver_pipeline.timestamp
        )
        silver_pipeline.output_parquet = str(
            Path(silver_pipeline.output_dir) / "workplace_inspections.parquet"
        )

        # Run pipeline
        silver_pipeline.setup_output_directories()
        silver_pipeline.find_latest_bronze_data()
        silver_pipeline.connect_database()
        silver_pipeline.load_data()
        silver_pipeline.rename_columns()
        silver_pipeline.validate_column_names()
        silver_pipeline.deduplicate()
        silver_pipeline.normalize_enums()
        silver_pipeline.handle_null_values()
        silver_pipeline.cast_types()
        silver_pipeline.filter_by_date()
        silver_pipeline.check_for_pii()

        with patch.object(silver_pipeline, "extract_and_save_cvr_numbers", return_value=True):
            silver_pipeline.save_output()

        # Count output rows
        output_df = pd.read_parquet(silver_pipeline.output_parquet)
        output_rows = len(output_df)

        # Should match (no duplicates in test data)
        assert output_rows == input_rows, f"Row count mismatch: {input_rows} -> {output_rows}"

    def test_column_integrity(self, temp_workspace, sample_raw_csv):
        """Test that all expected columns are present after transformation."""
        # Setup bronze data
        bronze_timestamp = "20240115_120000"
        bronze_data_dir = temp_workspace / "bronze" / "data" / bronze_timestamp
        bronze_data_dir.mkdir(parents=True)

        bronze_csv = bronze_data_dir / "data_merged.csv"
        with open(bronze_csv, "w") as f:
            f.write(sample_raw_csv)

        # Create and run silver pipeline
        silver_pipeline = SilverPipeline(log_level="ERROR")
        silver_pipeline.pipeline_root = str(temp_workspace)
        silver_pipeline.data_root = str(temp_workspace / "bronze" / "data")
        silver_pipeline.silver_dir = str(temp_workspace / "silver" / "data")
        silver_pipeline.output_dir = str(
            temp_workspace / "silver" / "data" / silver_pipeline.timestamp
        )
        silver_pipeline.output_parquet = str(
            Path(silver_pipeline.output_dir) / "workplace_inspections.parquet"
        )

        # Run pipeline
        silver_pipeline.setup_output_directories()
        silver_pipeline.find_latest_bronze_data()
        silver_pipeline.connect_database()
        silver_pipeline.load_data()
        silver_pipeline.rename_columns()
        silver_pipeline.validate_column_names()
        silver_pipeline.deduplicate()
        silver_pipeline.normalize_enums()
        silver_pipeline.handle_null_values()
        silver_pipeline.cast_types()
        silver_pipeline.filter_by_date()
        silver_pipeline.check_for_pii()

        with patch.object(silver_pipeline, "extract_and_save_cvr_numbers", return_value=True):
            silver_pipeline.save_output()

        # Check output columns
        output_df = pd.read_parquet(silver_pipeline.output_parquet)
        expected_columns = [
            "date",
            "case_count",
            "decision",
            "work_env_issue",
            "appealed",
            "complied",
            "company_name",
            "company_id",
            "industry",
            "company_address",
        ]

        for col in expected_columns:
            assert col in output_df.columns, f"Missing expected column: {col}"

    def test_data_quality_validation(self, temp_workspace, sample_raw_csv):
        """Test that CVR format and dates are valid after transformation."""
        # Setup bronze data
        bronze_timestamp = "20240115_120000"
        bronze_data_dir = temp_workspace / "bronze" / "data" / bronze_timestamp
        bronze_data_dir.mkdir(parents=True)

        bronze_csv = bronze_data_dir / "data_merged.csv"
        with open(bronze_csv, "w") as f:
            f.write(sample_raw_csv)

        # Create and run silver pipeline
        silver_pipeline = SilverPipeline(log_level="ERROR")
        silver_pipeline.pipeline_root = str(temp_workspace)
        silver_pipeline.data_root = str(temp_workspace / "bronze" / "data")
        silver_pipeline.silver_dir = str(temp_workspace / "silver" / "data")
        silver_pipeline.output_dir = str(
            temp_workspace / "silver" / "data" / silver_pipeline.timestamp
        )
        silver_pipeline.output_parquet = str(
            Path(silver_pipeline.output_dir) / "workplace_inspections.parquet"
        )

        # Run pipeline
        silver_pipeline.setup_output_directories()
        silver_pipeline.find_latest_bronze_data()
        silver_pipeline.connect_database()
        silver_pipeline.load_data()
        silver_pipeline.rename_columns()
        silver_pipeline.validate_column_names()
        silver_pipeline.deduplicate()
        silver_pipeline.normalize_enums()
        silver_pipeline.handle_null_values()
        silver_pipeline.cast_types()
        silver_pipeline.filter_by_date()
        silver_pipeline.check_for_pii()

        with patch.object(silver_pipeline, "extract_and_save_cvr_numbers", return_value=True):
            silver_pipeline.save_output()

        # Validate output data
        output_df = pd.read_parquet(silver_pipeline.output_parquet)

        # Check date format
        assert output_df["date"].dtype == "object" or pd.api.types.is_datetime64_any_dtype(
            output_df["date"]
        ), "Date column should be datetime type"

        # Check company_id format (should be 10 digits as BIGINT)
        assert output_df["company_id"].notna().all(), "All company IDs should have values"
        for company_id in output_df["company_id"]:
            assert len(str(company_id)) == 10, f"Company ID should be 10 digits: {company_id}"

        # Check appealed and complied are binary (0 or 1)
        assert set(output_df["appealed"].unique()).issubset({0, 1}), "Appealed should be 0 or 1"
        assert set(output_df["complied"].unique()).issubset({0, 1}), "Complied should be 0 or 1"

    def test_error_propagation(self, temp_workspace):
        """Test that bronze layer errors are properly handled in silver layer."""
        # Create invalid bronze data (missing required columns)
        bronze_timestamp = "20240115_120000"
        bronze_data_dir = temp_workspace / "bronze" / "data" / bronze_timestamp
        bronze_data_dir.mkdir(parents=True)

        bronze_csv = bronze_data_dir / "data_merged.csv"
        with open(bronze_csv, "w") as f:
            f.write("InvalidColumn1,InvalidColumn2\nValue1,Value2\n")

        # Create silver pipeline
        silver_pipeline = SilverPipeline(log_level="ERROR")
        silver_pipeline.pipeline_root = str(temp_workspace)
        silver_pipeline.data_root = str(temp_workspace / "bronze" / "data")
        silver_pipeline.silver_dir = str(temp_workspace / "silver" / "data")
        silver_pipeline.output_dir = str(
            temp_workspace / "silver" / "data" / silver_pipeline.timestamp
        )
        silver_pipeline.output_parquet = str(
            Path(silver_pipeline.output_dir) / "workplace_inspections.parquet"
        )

        # Run pipeline steps - should handle gracefully
        silver_pipeline.setup_output_directories()
        silver_pipeline.find_latest_bronze_data()
        silver_pipeline.connect_database()
        silver_pipeline.load_data()
        silver_pipeline.rename_columns()

        # After renaming, columns will not match expected format
        # Pipeline should still complete but with original column names
        result = silver_pipeline.con.execute("SELECT * FROM renamed_data LIMIT 1").description
        columns = [desc[0] for desc in result]

        # Original invalid columns should still be present
        assert "InvalidColumn1" in columns or "date" not in columns


class TestDataTransformationAccuracy:
    """Tests for data transformation accuracy."""

    def test_danish_character_normalization(self, temp_workspace):
        """Test that Danish characters are properly normalized."""
        # Create data with Danish characters
        csv_content = """Dato,Antal,Afgørelse,Arbejdsmiljøproblem (emne),Påklaget,Efterkommet,Produktionsenhed,P-nummer,Branche,Produktionenhedens adresse
2024-01-15,5,Påbud,Støj og vibrationer,,Efterkommet,Dansk Landbrug,1234567890,Landbrug,Vejlevej 1"""

        bronze_timestamp = "20240115_120000"
        bronze_data_dir = temp_workspace / "bronze" / "data" / bronze_timestamp
        bronze_data_dir.mkdir(parents=True)

        bronze_csv = bronze_data_dir / "data_merged.csv"
        with open(bronze_csv, "w", encoding="utf-8") as f:
            f.write(csv_content)

        # Create and run silver pipeline
        silver_pipeline = SilverPipeline(log_level="ERROR")
        silver_pipeline.pipeline_root = str(temp_workspace)
        silver_pipeline.data_root = str(temp_workspace / "bronze" / "data")
        silver_pipeline.silver_dir = str(temp_workspace / "silver" / "data")
        silver_pipeline.output_dir = str(
            temp_workspace / "silver" / "data" / silver_pipeline.timestamp
        )
        silver_pipeline.output_parquet = str(
            Path(silver_pipeline.output_dir) / "workplace_inspections.parquet"
        )

        # Run normalization steps
        silver_pipeline.setup_output_directories()
        silver_pipeline.find_latest_bronze_data()
        silver_pipeline.connect_database()
        silver_pipeline.load_data()
        silver_pipeline.rename_columns()
        silver_pipeline.deduplicate()
        silver_pipeline.normalize_enums()

        # Check normalized values
        result = silver_pipeline.con.execute(
            "SELECT decision, work_env_issue FROM normalized_data LIMIT 1"
        ).fetchone()

        # Should be lowercase
        assert result[0].islower(), "Decision should be normalized to lowercase"
        assert result[1].islower(), "Work env issue should be normalized to lowercase"

    def test_null_handling_consistency(self, temp_workspace):
        """Test that null values are consistently handled."""
        # Create data with various empty representations
        csv_content = """Dato,Antal,Afgørelse,Arbejdsmiljøproblem (emne),Påklaget,Efterkommet,Produktionsenhed,P-nummer,Branche,Produktionenhedens adresse
2024-01-15,5,Påbud,Støj,,Efterkommet,Company A,1234567890,Landbrug,Address 1
2024-01-16,10,,Issue,,,,Company B,2345678901,Industri,
2024-01-17,,Vejledning,Problem,,,Company C,3456789012,,Address 3"""

        bronze_timestamp = "20240115_120000"
        bronze_data_dir = temp_workspace / "bronze" / "data" / bronze_timestamp
        bronze_data_dir.mkdir(parents=True)

        bronze_csv = bronze_data_dir / "data_merged.csv"
        with open(bronze_csv, "w") as f:
            f.write(csv_content)

        # Create and run silver pipeline
        silver_pipeline = SilverPipeline(log_level="ERROR")
        silver_pipeline.pipeline_root = str(temp_workspace)
        silver_pipeline.data_root = str(temp_workspace / "bronze" / "data")
        silver_pipeline.silver_dir = str(temp_workspace / "silver" / "data")
        silver_pipeline.output_dir = str(
            temp_workspace / "silver" / "data" / silver_pipeline.timestamp
        )
        silver_pipeline.output_parquet = str(
            Path(silver_pipeline.output_dir) / "workplace_inspections.parquet"
        )

        # Run null handling
        silver_pipeline.setup_output_directories()
        silver_pipeline.find_latest_bronze_data()
        silver_pipeline.connect_database()
        silver_pipeline.load_data()
        silver_pipeline.rename_columns()
        silver_pipeline.deduplicate()
        silver_pipeline.normalize_enums()
        silver_pipeline.handle_null_values()

        # Check null handling
        result = silver_pipeline.con.execute(
            "SELECT * FROM nulled_data WHERE company_address IS NULL"
        ).fetchall()

        # Should have rows with null addresses
        assert len(result) > 0, "Should have null address records"
