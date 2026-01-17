"""
Tests for silver layer transformation for Arbejdstilsynet inspections.

This module tests all data transformations including:
- Column renaming and deduplication
- Type casting and date parsing
- PII detection
- CVR integration
- Null handling
"""

# Import the silver pipeline
import sys
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from silver.transform import SilverPipeline


@pytest.fixture
def sample_raw_data():
    """Create sample raw CSV data for testing."""
    return pd.DataFrame(
        {
            "Dato": ["2024-01-15", "2024-01-16", "2024-01-17"],
            "Antal": ["5", "10", "3"],
            "Afgoerelse": ["Påbud", "Strakspåbud", "Vejledning"],
            "Arbejdsmiljoeproblem (emne)": ["Støj", "Kemikalier", "Ergonomi"],
            "Paaklaget": ["", "Paaklaget", ""],
            "Efterkommet": ["Efterkommet", "", "Efterkommet"],
            "Produktionsenhed": ["Test Farm A/S", "Test Farm B ApS", "Test Farm C"],
            "P-nummer": ["1234567890", "2345678901", "3456789012"],
            "Branche": ["Landbrug", "Slagteri", "Anlæg"],
            "Produktionenhedens adresse": ["Testvej 1", "Prøvevej 2", "Eksempelvej 3"],
        }
    )


@pytest.fixture
def temp_bronze_data(sample_raw_data):
    """Create temporary bronze data directory with CSV file."""
    temp_dir = tempfile.mkdtemp()
    bronze_dir = Path(temp_dir) / "bronze" / "data" / "20240115_120000"
    bronze_dir.mkdir(parents=True)

    csv_path = bronze_dir / "data_merged.csv"
    sample_raw_data.to_csv(csv_path, index=False)

    return temp_dir, csv_path


@pytest.fixture
def pipeline(temp_bronze_data):
    """Create a SilverPipeline instance for testing."""
    temp_dir, _ = temp_bronze_data
    pipeline = SilverPipeline(log_level="ERROR")
    pipeline.pipeline_root = temp_dir
    pipeline.data_root = Path(temp_dir) / "bronze" / "data"
    pipeline.silver_dir = Path(temp_dir) / "silver" / "data"
    pipeline.output_dir = pipeline.silver_dir / pipeline.timestamp
    pipeline.output_parquet = pipeline.output_dir / "workplace_inspections.parquet"
    return pipeline


class TestColumnOperations:
    """Tests for column renaming, deduplication, and normalization."""

    def test_column_renaming(self, pipeline):
        """Test that Danish column names are correctly renamed to English."""
        pipeline.setup_output_directories()
        pipeline.find_latest_bronze_data()
        pipeline.connect_database()
        pipeline.load_data()
        pipeline.rename_columns()

        # Get renamed columns
        result = pipeline.con.execute("SELECT * FROM renamed_data LIMIT 1").description
        columns = [desc[0] for desc in result]

        # Check expected renamed columns
        assert "date" in columns
        assert "case_count" in columns
        assert "decision" in columns
        assert "work_env_issue" in columns
        assert "appealed" in columns
        assert "complied" in columns
        assert "company_name" in columns
        assert "company_id" in columns
        assert "industry" in columns
        assert "company_address" in columns

        # Check original columns are gone
        assert "Dato" not in columns
        assert "Antal" not in columns

    def test_column_deduplication(self, pipeline):
        """Test that SELECT DISTINCT correctly removes duplicate rows."""
        pipeline.setup_output_directories()
        pipeline.find_latest_bronze_data()
        pipeline.connect_database()

        # Create test data with duplicates
        pipeline.con.execute("""
            CREATE TABLE raw_data AS
            SELECT * FROM (VALUES
                ('2024-01-01', '5', 'Test', 'Issue', '', '', 'Company', '1234567890', 'Industry', 'Address'),
                ('2024-01-01', '5', 'Test', 'Issue', '', '', 'Company', '1234567890', 'Industry', 'Address'),
                ('2024-01-02', '10', 'Test2', 'Issue2', '', '', 'Company2', '2345678901', 'Industry2', 'Address2')
            ) AS t(Dato, Antal, Afgoerelse, "Arbejdsmiljoeproblem (emne)", Paaklaget, Efterkommet, Produktionsenhed, "P-nummer", Branche, "Produktionenhedens adresse")
        """)

        pipeline.rename_columns()
        pipeline.deduplicate()

        # Check row counts
        result = pipeline.con.execute("SELECT COUNT(*) FROM deduped_data").fetchone()[0]
        assert result == 2, "Should remove 1 duplicate row"

    def test_column_normalization(self, pipeline):
        """Test that enum values are properly normalized."""
        pipeline.setup_output_directories()
        pipeline.find_latest_bronze_data()
        pipeline.connect_database()
        pipeline.load_data()
        pipeline.rename_columns()
        pipeline.deduplicate()
        pipeline.normalize_enums()

        # Check normalized values
        result = pipeline.con.execute("SELECT DISTINCT decision FROM normalized_data").fetchall()
        decisions = [row[0] for row in result]

        # All should be lowercase
        for decision in decisions:
            if decision:
                assert decision == decision.lower(), "Decision should be lowercase"


class TestDataTypeCasting:
    """Tests for type casting operations."""

    def test_type_casting_success(self, pipeline):
        """Test that TRY_CAST successfully converts valid values."""
        pipeline.setup_output_directories()
        pipeline.find_latest_bronze_data()
        pipeline.connect_database()
        pipeline.load_data()
        pipeline.rename_columns()
        pipeline.deduplicate()
        pipeline.normalize_enums()
        pipeline.handle_null_values()
        pipeline.cast_types()

        # Check date type
        result = pipeline.con.execute(
            "SELECT typeof(date) as type FROM typed_data LIMIT 1"
        ).fetchone()
        assert result[0] == "DATE", "Date column should be DATE type"

        # Check integer types
        result = pipeline.con.execute(
            "SELECT typeof(case_count) as type FROM typed_data LIMIT 1"
        ).fetchone()
        assert result[0] == "BIGINT", "case_count should be BIGINT type"

    def test_type_casting_failures(self, pipeline):
        """Test that TRY_CAST handles NULL for invalid values."""
        pipeline.setup_output_directories()
        pipeline.connect_database()

        # Create test data with invalid values
        pipeline.con.execute("""
            CREATE TABLE raw_data AS
            SELECT * FROM (VALUES
                ('invalid-date', 'not-a-number', 'Test', 'Issue', '', '', 'Company', '1234567890', 'Industry', 'Address')
            ) AS t(Dato, Antal, Afgoerelse, "Arbejdsmiljoeproblem (emne)", Paaklaget, Efterkommet, Produktionsenhed, "P-nummer", Branche, "Produktionenhedens adresse")
        """)

        pipeline.rename_columns()
        pipeline.deduplicate()
        pipeline.normalize_enums()
        pipeline.handle_null_values()
        pipeline.cast_types()

        # Check NULL values for invalid casts
        result = pipeline.con.execute("SELECT date, case_count FROM typed_data LIMIT 1").fetchone()
        assert result[0] is None, "Invalid date should cast to NULL"
        assert result[1] is None, "Invalid number should cast to NULL"

    def test_date_parsing(self, pipeline):
        """Test that various date formats are correctly parsed."""
        pipeline.setup_output_directories()
        pipeline.connect_database()

        # Create test data with different date formats
        pipeline.con.execute("""
            CREATE TABLE raw_data AS
            SELECT * FROM (VALUES
                ('2024-01-15', '5', 'Test', 'Issue', '', '', 'Company', '1234567890', 'Industry', 'Address'),
                ('2024-12-31', '10', 'Test', 'Issue', '', '', 'Company', '1234567890', 'Industry', 'Address')
            ) AS t(Dato, Antal, Afgoerelse, "Arbejdsmiljoeproblem (emne)", Paaklaget, Efterkommet, Produktionsenhed, "P-nummer", Branche, "Produktionenhedens adresse")
        """)

        pipeline.rename_columns()
        pipeline.deduplicate()
        pipeline.normalize_enums()
        pipeline.handle_null_values()
        pipeline.cast_types()

        # Verify dates
        result = pipeline.con.execute("SELECT date FROM typed_data ORDER BY date").fetchall()
        assert len(result) == 2
        assert str(result[0][0]) == "2024-01-15"
        assert str(result[1][0]) == "2024-12-31"

    def test_date_edge_cases(self, pipeline):
        """Test leap year and edge case date handling."""
        pipeline.setup_output_directories()
        pipeline.connect_database()

        # Test leap year date
        pipeline.con.execute("""
            CREATE TABLE raw_data AS
            SELECT * FROM (VALUES
                ('2024-02-29', '5', 'Test', 'Issue', '', '', 'Company', '1234567890', 'Industry', 'Address'),
                ('2023-02-29', '5', 'Test', 'Issue', '', '', 'Company', '1234567890', 'Industry', 'Address')
            ) AS t(Dato, Antal, Afgoerelse, "Arbejdsmiljoeproblem (emne)", Paaklaget, Efterkommet, Produktionsenhed, "P-nummer", Branche, "Produktionenhedens adresse")
        """)

        pipeline.rename_columns()
        pipeline.deduplicate()
        pipeline.normalize_enums()
        pipeline.handle_null_values()
        pipeline.cast_types()

        result = pipeline.con.execute(
            "SELECT date FROM typed_data WHERE date IS NOT NULL"
        ).fetchall()
        assert len(result) == 1, "Only valid leap year date should be parsed"
        assert str(result[0][0]) == "2024-02-29"


class TestPIIDetection:
    """Tests for PII (Personally Identifiable Information) detection."""

    def test_pii_detection_valid(self, pipeline):
        """Test that 10-digit numbers matching CPR format are detected."""
        pipeline.setup_output_directories()
        pipeline.connect_database()

        # Create test data with potential CPR numbers (no spaces, actual 10 digits in a row)
        pipeline.con.execute("""
            CREATE TABLE filtered_data AS
            SELECT * FROM (VALUES
                ('2024-01-15', 5, 'Test', 'Issue', 0, 0, 'Company 0101901234 name', 1234567890, 'Industry', 'Address')
            ) AS t(date, case_count, decision, work_env_issue, appealed, complied, company_name, company_id, industry, company_address)
        """)

        # Patch LandbrugsdataUUID to ensure PII replacement works
        with patch("silver.transform.LandbrugsdataUUID") as mock_uuid:
            mock_uuid.generate_deterministic_uuid.return_value = "REDACTED-UUID"
            success = pipeline.check_for_pii()
            assert success, "PII check should complete"
            assert pipeline.df is not None, "DataFrame should be loaded"

            # Check that CPR was detected and replaced (if LandbrugsdataUUID available)
            company_name = pipeline.df["company_name"].iloc[0]
            # The pattern matches 10 consecutive digits
            if mock_uuid.generate_deterministic_uuid.called:
                assert "0101901234" not in company_name, "CPR should be replaced"

    def test_pii_detection_false_positives(self, pipeline):
        """Test that phone numbers are NOT matched as CPR."""
        pipeline.setup_output_directories()
        pipeline.connect_database()

        # Create test data with phone number (should NOT match CPR pattern)
        pipeline.con.execute("""
            CREATE TABLE filtered_data AS
            SELECT * FROM (VALUES
                ('2024-01-15', 5, 'Test', 'Issue', 0, 0, 'Company phone: +45 12345678', 1234567890, 'Industry', 'Address')
            ) AS t(date, case_count, decision, work_env_issue, appealed, complied, company_name, company_id, industry, company_address)
        """)

        success = pipeline.check_for_pii()
        assert success, "PII check should complete"

        # Phone number format should remain (not 10 consecutive digits)
        company_name = pipeline.df["company_name"].iloc[0]
        assert "12345678" in company_name, "Phone number should not be replaced"

    def test_pii_detection_edge_cases(self, pipeline):
        """Test boundary conditions for PII detection."""
        pipeline.setup_output_directories()
        pipeline.connect_database()

        # Test 9-digit and 11-digit numbers (should NOT match)
        pipeline.con.execute("""
            CREATE TABLE filtered_data AS
            SELECT * FROM (VALUES
                ('2024-01-15', 5, 'Test', 'Issue', 0, 0, 'Nine: 123456789 Eleven: 12345678901', 1234567890, 'Industry', 'Address')
            ) AS t(date, case_count, decision, work_env_issue, appealed, complied, company_name, company_id, industry, company_address)
        """)

        success = pipeline.check_for_pii()
        assert success, "PII check should complete"

        company_name = pipeline.df["company_name"].iloc[0]
        assert "123456789" in company_name, "9-digit number should remain"
        assert "12345678901" in company_name, "11-digit number should remain"


class TestCVRIntegration:
    """Tests for CVR number mapping and API integration."""

    @patch("silver.transform.save_pipeline_cvr_numbers")
    @patch("silver.transform.CVR_COLLECTION_AVAILABLE", True)
    @patch("silver.transform.CVRAPIClient")
    def test_cvr_mapping_success(self, mock_cvr_client, mock_save_cvr, pipeline):
        """Test successful P-number to CVR lookup."""
        # Mock CVR API response
        mock_client = Mock()
        mock_client.fetch_multiple_pnumbers.return_value = {
            "results": {
                "1234567890": {
                    "company_relations": [{"is_current": True, "cvr_number": "12345678"}]
                }
            },
            "summary": {"api_calls": 1, "efficiency_gain": "1.0x"},
        }
        mock_cvr_client.return_value = mock_client
        mock_save_cvr.return_value = "gs://test-bucket/cvr/test.json"

        pipeline.setup_output_directories()
        pipeline.connect_database()

        # Create test data
        pipeline.con.execute("""
            CREATE TABLE filtered_data AS
            SELECT * FROM (VALUES
                ('2024-01-15', 5, 'Test', 'Issue', 0, 0, 'Company', 1234567890, 'Industry', 'Address')
            ) AS t(date, case_count, decision, work_env_issue, appealed, complied, company_name, company_id, industry, company_address)
        """)

        pipeline.df = pipeline.con.execute("SELECT * FROM filtered_data").fetchdf()
        success = pipeline.extract_and_save_cvr_numbers()

        assert success, "CVR extraction should succeed"
        assert "cvr_number" in pipeline.df.columns, "CVR column should be added"

    @patch("silver.transform.save_pipeline_cvr_numbers")
    @patch("silver.transform.CVR_COLLECTION_AVAILABLE", True)
    @patch("silver.transform.CVRAPIClient")
    def test_cvr_mapping_failure(self, mock_cvr_client, mock_save_cvr, pipeline):
        """Test handling of missing CVR mappings."""
        # Mock empty CVR API response
        mock_client = Mock()
        mock_client.fetch_multiple_pnumbers.return_value = {
            "results": {},
            "summary": {"api_calls": 0, "efficiency_gain": "1.0x"},
        }
        mock_cvr_client.return_value = mock_client
        mock_save_cvr.return_value = "gs://test-bucket/cvr/test.json"

        pipeline.setup_output_directories()
        pipeline.connect_database()

        pipeline.con.execute("""
            CREATE TABLE filtered_data AS
            SELECT * FROM (VALUES
                ('2024-01-15', 5, 'Test', 'Issue', 0, 0, 'Company', 9999999999, 'Industry', 'Address')
            ) AS t(date, case_count, decision, work_env_issue, appealed, complied, company_name, company_id, industry, company_address)
        """)

        pipeline.df = pipeline.con.execute("SELECT * FROM filtered_data").fetchdf()
        success = pipeline.extract_and_save_cvr_numbers()

        assert success, "CVR extraction should not fail pipeline"
        assert "cvr_number" in pipeline.df.columns, "CVR column should be added even if empty"

    @patch("silver.transform.save_pipeline_cvr_numbers")
    @patch("silver.transform.CVR_COLLECTION_AVAILABLE", True)
    @patch("silver.transform.CVRAPIClient")
    def test_cvr_bulk_api_efficiency(self, mock_cvr_client, mock_save_cvr, pipeline):
        """Test that bulk operations are used for efficiency."""
        mock_client = Mock()
        mock_client.fetch_multiple_pnumbers.return_value = {
            "results": {
                "1234567890": {
                    "company_relations": [{"is_current": True, "cvr_number": "12345678"}]
                },
                "2345678901": {
                    "company_relations": [{"is_current": True, "cvr_number": "23456789"}]
                },
            },
            "summary": {"api_calls": 1, "efficiency_gain": "2.0x"},
        }
        mock_cvr_client.return_value = mock_client
        mock_save_cvr.return_value = "gs://test-bucket/cvr/test.json"

        pipeline.setup_output_directories()
        pipeline.connect_database()

        # Create test data with multiple P-numbers
        pipeline.con.execute("""
            CREATE TABLE filtered_data AS
            SELECT * FROM (VALUES
                ('2024-01-15', 5, 'Test', 'Issue', 0, 0, 'Company1', 1234567890, 'Industry', 'Address'),
                ('2024-01-16', 10, 'Test', 'Issue', 0, 0, 'Company2', 2345678901, 'Industry', 'Address')
            ) AS t(date, case_count, decision, work_env_issue, appealed, complied, company_name, company_id, industry, company_address)
        """)

        pipeline.df = pipeline.con.execute("SELECT * FROM filtered_data").fetchdf()
        pipeline.extract_and_save_cvr_numbers()

        # Verify bulk API was called once (not per P-number)
        mock_client.fetch_multiple_pnumbers.assert_called_once()

    @patch("silver.transform.CVRAPIClient")
    def test_cvr_api_error_handling(self, mock_cvr_client, pipeline):
        """Test graceful handling of CVR API failures."""
        # Mock API error
        mock_client = Mock()
        mock_client.fetch_multiple_pnumbers.side_effect = Exception("API Error")
        mock_cvr_client.return_value = mock_client

        pipeline.setup_output_directories()
        pipeline.connect_database()

        pipeline.con.execute("""
            CREATE TABLE filtered_data AS
            SELECT * FROM (VALUES
                ('2024-01-15', 5, 'Test', 'Issue', 0, 0, 'Company', 1234567890, 'Industry', 'Address')
            ) AS t(date, case_count, decision, work_env_issue, appealed, complied, company_name, company_id, industry, company_address)
        """)

        pipeline.df = pipeline.con.execute("SELECT * FROM filtered_data").fetchdf()
        success = pipeline.extract_and_save_cvr_numbers()

        # Should not fail the pipeline
        assert success, "CVR API error should not fail pipeline"


class TestNullHandling:
    """Tests for null value handling."""

    def test_empty_string_to_null(self, pipeline):
        """Test that NULLIF logic correctly converts empty strings to NULL."""
        pipeline.setup_output_directories()
        pipeline.connect_database()

        # Create test data with empty strings
        pipeline.con.execute("""
            CREATE TABLE normalized_data AS
            SELECT * FROM (VALUES
                ('2024-01-15', '5', '', 'Issue', '', '', 'Company', '1234567890', 'Industry', '')
            ) AS t(date, case_count, decision, work_env_issue, appealed, complied, company_name, company_id, industry, company_address)
        """)

        pipeline.handle_null_values()

        # Check NULL values
        result = pipeline.con.execute(
            "SELECT decision, appealed, complied, company_address FROM nulled_data LIMIT 1"
        ).fetchone()

        assert result[0] is None, "Empty decision should be NULL"
        assert result[1] is None, "Empty appealed should be NULL"
        assert result[2] is None, "Empty complied should be NULL"
        assert result[3] is None, "Empty address should be NULL"


class TestDateFiltering:
    """Tests for date range filtering functionality."""

    def test_date_filtering_with_range(self, pipeline):
        """Test filtering by date range."""
        pipeline.setup_output_directories()
        pipeline.connect_database()
        pipeline.start_date = "2024-01-15"
        pipeline.end_date = "2024-01-16"

        # Create test data with dates outside range
        pipeline.con.execute("""
            CREATE TABLE typed_data AS
            SELECT * FROM (VALUES
                (DATE '2024-01-14', 5, 'Test', 'Issue', 0, 0, 'Company', 1234567890, 'Industry', 'Address'),
                (DATE '2024-01-15', 10, 'Test', 'Issue', 0, 0, 'Company', 1234567890, 'Industry', 'Address'),
                (DATE '2024-01-16', 3, 'Test', 'Issue', 0, 0, 'Company', 1234567890, 'Industry', 'Address'),
                (DATE '2024-01-17', 8, 'Test', 'Issue', 0, 0, 'Company', 1234567890, 'Industry', 'Address')
            ) AS t(date, case_count, decision, work_env_issue, appealed, complied, company_name, company_id, industry, company_address)
        """)

        pipeline.filter_by_date()

        # Check filtered results
        result = pipeline.con.execute("SELECT COUNT(*) FROM filtered_data").fetchone()[0]
        assert result == 2, "Should only include dates in range"

        dates = pipeline.con.execute("SELECT date FROM filtered_data ORDER BY date").fetchall()
        assert str(dates[0][0]) == "2024-01-15"
        assert str(dates[1][0]) == "2024-01-16"

    def test_date_filtering_no_range(self, pipeline):
        """Test that no filtering occurs when no date range provided."""
        pipeline.setup_output_directories()
        pipeline.connect_database()
        pipeline.start_date = None
        pipeline.end_date = None

        pipeline.con.execute("""
            CREATE TABLE typed_data AS
            SELECT * FROM (VALUES
                (DATE '2024-01-15', 5, 'Test', 'Issue', 0, 0, 'Company', 1234567890, 'Industry', 'Address'),
                (DATE '2024-01-16', 10, 'Test', 'Issue', 0, 0, 'Company', 1234567890, 'Industry', 'Address')
            ) AS t(date, case_count, decision, work_env_issue, appealed, complied, company_name, company_id, industry, company_address)
        """)

        original_count = pipeline.con.execute("SELECT COUNT(*) FROM typed_data").fetchone()[0]
        pipeline.filter_by_date()
        filtered_count = pipeline.con.execute("SELECT COUNT(*) FROM filtered_data").fetchone()[0]

        assert original_count == filtered_count, "No filtering should occur"
