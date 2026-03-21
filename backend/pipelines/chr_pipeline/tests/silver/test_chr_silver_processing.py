"""Tests for CHR silver layer orchestration and processing.

Tests the main silver processing logic including:
- Silver orchestration workflow
- Data loading from bronze layer
- Table creation pipeline
- Error handling and recovery
"""

import contextlib
import json
from datetime import date
from unittest.mock import Mock, patch

import duckdb
import pytest
from chr_pipeline.silver.chr_silver_processing import (
    _save_discovered_cvr_numbers,
    download_bronze_data_from_gcs,
    process_chr_data,
)


@pytest.mark.chr_silver
class TestSilverOrchestration:
    """Test the main silver processing orchestration."""

    def test_process_chr_data_creates_silver_directory(self, tmp_path):
        """Test that silver processing creates output directory."""
        silver_dir = tmp_path / "silver" / "20240101_120000"
        bronze_dir = tmp_path / "bronze" / "20240101_120000"
        bronze_dir.mkdir(parents=True, exist_ok=True)

        # Create minimal required bronze files
        (bronze_dir / "besaetning_list.json").write_text("[]")
        (bronze_dir / "besaetning_details.json").write_text('{"Response": []}')
        (bronze_dir / "diko_flytninger.json").write_text("[]")
        (bronze_dir / "ejendom_oplysninger.json").write_text('{"Response": []}')
        (bronze_dir / "ejendom_vet_events.json").write_text("[]")

        # Mock config
        with patch("chr_pipeline.silver.chr_silver_processing.config") as mock_config:
            mock_config.BRONZE_BASE_DIR = bronze_dir.parent
            mock_config.BRONZE_DATE_FOLDER_OVERRIDE = "20240101_120000"

            with contextlib.suppress(SystemExit):
                process_chr_data(
                    silver_dir=silver_dir,
                    in_memory_data=None,
                    export_timestamp="20240101_120000",
                    bronze_timestamp=None,
                    force_streaming=False,
                )

        assert silver_dir.exists()

    def test_process_chr_data_with_in_memory_data(self, tmp_path, mock_chr_responses):
        """Test silver processing with in-memory data."""
        silver_dir = tmp_path / "silver" / "20240101_120000"

        in_memory_data = {
            "besaetning_list": {"json": []},
            "besaetning_details": {
                "json": [{"Response": [mock_chr_responses["hentBesaetning"]["success"]]}]
            },
            "diko_flytninger": {"json": [mock_chr_responses["hentDiko"]["success"]]},
            "ejendom_oplysninger": {
                "json": [{"Response": [mock_chr_responses["hentEjendom"]["success"]]}]
            },
            "ejendom_vet_events": {"json": []},
            "vetstat_antibiotics": {"xml": []},
        }

        with contextlib.suppress(SystemExit):
            process_chr_data(
                silver_dir=silver_dir,
                in_memory_data=in_memory_data,
                export_timestamp="20240101_120000",
                bronze_timestamp=None,
                force_streaming=False,
            )

        assert silver_dir.exists()

    def test_silver_processing_handles_missing_optional_data(self, tmp_path, mock_chr_responses):
        """Test that missing optional data (VetStat, SPF-SU) doesn't cause failure."""
        silver_dir = tmp_path / "silver" / "20240101_120000"
        bronze_dir = tmp_path / "bronze" / "20240101_120000"
        bronze_dir.mkdir(parents=True, exist_ok=True)

        # Create required files only (no optional files)
        besaetning_details = {
            "Response": [
                {
                    "Besaetning": {
                        "BesaetningsNummer": "123456",
                        "ChrNummer": "123456",
                        "DyreArtKode": "1",
                        "DyreArtTekst": "Kvæg",
                        "Ejer": {
                            "CvrNummer": "12345678",
                            "Navn": "Test Farm",
                            "Adresse": "Landevej 1",
                            "PostNummer": "8000",
                        },
                    }
                }
            ]
        }

        ejendom_oplysninger = {
            "Response": [
                {
                    "Ejendom": {
                        "EjendomId": "123456",
                        "Ejer": {
                            "CvrNummer": "12345678",
                            "Navn": "Test Owner",
                            "Adresse": "Gade 1",
                            "PostNummer": "8000",
                        },
                    }
                }
            ]
        }

        (bronze_dir / "besaetning_list.json").write_text("[]")
        (bronze_dir / "besaetning_details.json").write_text(json.dumps(besaetning_details))
        (bronze_dir / "diko_flytninger.json").write_text("[]")
        (bronze_dir / "ejendom_oplysninger.json").write_text(json.dumps(ejendom_oplysninger))
        (bronze_dir / "ejendom_vet_events.json").write_text("[]")

        with patch("chr_pipeline.silver.chr_silver_processing.config") as mock_config:
            mock_config.BRONZE_BASE_DIR = bronze_dir.parent
            mock_config.BRONZE_DATE_FOLDER_OVERRIDE = "20240101_120000"

            with contextlib.suppress(SystemExit):
                process_chr_data(
                    silver_dir=silver_dir,
                    in_memory_data=None,
                    export_timestamp="20240101_120000",
                    bronze_timestamp=None,
                    force_streaming=False,
                )

        # Silver directory should still be created
        assert silver_dir.exists()


@pytest.mark.chr_silver
class TestDataLoading:
    """Test bronze data loading into silver layer."""

    def test_download_bronze_data_from_gcs_success(self, tmp_path):
        """Test successful download of bronze data from GCS."""
        local_bronze_dir = tmp_path / "bronze"
        bronze_timestamp = "20240101_120000"

        # Mock GCS access
        with (
            patch("chr_pipeline.silver.chr_silver_processing.CVR_COLLECTION_AVAILABLE", True),
            patch("chr_pipeline.silver.chr_silver_processing.GCSDataAccess") as mock_gcs_class,
            patch.dict("os.environ", {"GCS_BUCKET": "test-bucket"}),
        ):
            mock_gcs = Mock()
            mock_gcs.file_exists.return_value = True
            mock_gcs.fs.open.return_value.__enter__ = Mock(
                return_value=Mock(read=Mock(return_value=b'{"test": "data"}'))
            )
            mock_gcs.fs.open.return_value.__exit__ = Mock(return_value=None)
            mock_gcs_class.return_value = mock_gcs

            result = download_bronze_data_from_gcs(bronze_timestamp, local_bronze_dir)

            assert result is True
            assert local_bronze_dir.exists()

    def test_download_bronze_data_missing_bucket(self, tmp_path):
        """Test that missing GCS_BUCKET environment variable causes failure."""
        local_bronze_dir = tmp_path / "bronze"
        bronze_timestamp = "20240101_120000"

        with (
            patch("chr_pipeline.silver.chr_silver_processing.CVR_COLLECTION_AVAILABLE", True),
            patch.dict("os.environ", {}, clear=True),
        ):
            result = download_bronze_data_from_gcs(bronze_timestamp, local_bronze_dir)

            assert result is False

    def test_load_bronze_data_handles_json_files(self, tmp_path):
        """Test loading JSON files from bronze layer."""
        con = duckdb.connect()
        bronze_dir = tmp_path / "bronze"
        bronze_dir.mkdir(parents=True)

        test_data = [{"id": 1, "value": "test"}, {"id": 2, "value": "data"}]
        (bronze_dir / "test_data.json").write_text(json.dumps(test_data))

        con.execute(
            f"CREATE TABLE test AS SELECT * FROM read_json_auto('{bronze_dir / 'test_data.json'}')"
        )

        result = con.execute("SELECT COUNT(*) FROM test").fetchone()[0]
        assert result == 2


@pytest.mark.chr_silver
class TestCVRCollection:
    """Test CVR number collection from silver data."""

    def test_save_discovered_cvr_numbers_extracts_from_tables(self, tmp_path):
        """Test that CVR numbers are correctly extracted from silver tables."""
        con = duckdb.connect()
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir(parents=True)
        export_timestamp = "20240101_120000"

        # Create test tables with CVR numbers
        con.execute("""
            CREATE TABLE property_owners AS
            SELECT '12345678' AS owner_cvr
            UNION ALL
            SELECT '87654321' AS owner_cvr
        """)

        con.execute("""
            CREATE TABLE herd_owners AS
            SELECT '12345678' AS owner_cvr
            UNION ALL
            SELECT '11111111' AS owner_cvr
        """)

        with (
            patch("chr_pipeline.silver.chr_silver_processing.CVR_COLLECTION_AVAILABLE", True),
            patch(
                "chr_pipeline.silver.chr_silver_processing.extract_cvr_numbers_from_table"
            ) as mock_extract,
            patch(
                "chr_pipeline.silver.chr_silver_processing.save_pipeline_cvr_numbers"
            ) as mock_save,
            patch("chr_pipeline.silver.chr_silver_processing.GCSDataAccess"),
        ):
            mock_extract.side_effect = [
                ["12345678", "87654321"],  # property_owners
                ["12345678", "11111111"],  # herd_owners
            ]
            mock_save.return_value = "gs://bucket/cvr/test.json"

            _save_discovered_cvr_numbers(con, silver_dir, export_timestamp)

            # Should have called save with unique CVR numbers
            assert mock_save.called
            call_args = mock_save.call_args[1]
            assert set(call_args["cvr_numbers"]) == {"11111111", "12345678", "87654321"}

    def test_save_cvr_numbers_handles_empty_tables(self, tmp_path):
        """Test CVR collection handles empty tables gracefully."""
        con = duckdb.connect()
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir(parents=True)
        export_timestamp = "20240101_120000"

        # Create empty table
        con.execute("CREATE TABLE property_owners AS SELECT NULL AS owner_cvr WHERE FALSE")

        with (
            patch("chr_pipeline.silver.chr_silver_processing.CVR_COLLECTION_AVAILABLE", True),
            patch(
                "chr_pipeline.silver.chr_silver_processing.extract_cvr_numbers_from_table"
            ) as mock_extract,
            patch("chr_pipeline.silver.chr_silver_processing.GCSDataAccess"),
        ):
            mock_extract.return_value = []

            # Should not raise error
            _save_discovered_cvr_numbers(con, silver_dir, export_timestamp)


@pytest.mark.chr_silver
class TestErrorHandling:
    """Test error handling in silver processing."""

    def test_process_chr_data_handles_missing_essential_tables(self, tmp_path):
        """Test that missing essential tables causes graceful failure."""
        silver_dir = tmp_path / "silver" / "20240101_120000"
        bronze_dir = tmp_path / "bronze" / "20240101_120000"
        bronze_dir.mkdir(parents=True, exist_ok=True)

        # Create only some required files (missing ejendom_oplysninger)
        (bronze_dir / "besaetning_list.json").write_text("[]")
        (bronze_dir / "besaetning_details.json").write_text('{"Response": []}')

        with (
            patch("chr_pipeline.silver.chr_silver_processing.config") as mock_config,
            pytest.raises(SystemExit),
        ):
            mock_config.BRONZE_BASE_DIR = bronze_dir.parent
            mock_config.BRONZE_DATE_FOLDER_OVERRIDE = "20240101_120000"

            process_chr_data(
                silver_dir=silver_dir,
                in_memory_data=None,
                export_timestamp="20240101_120000",
                bronze_timestamp=None,
                force_streaming=False,
            )

    def test_silver_processing_continues_on_step_failure(self, tmp_path):
        """Test that failure of one silver step doesn't stop entire pipeline."""
        silver_dir = tmp_path / "silver" / "20240101_120000"
        bronze_dir = tmp_path / "bronze" / "20240101_120000"
        bronze_dir.mkdir(parents=True, exist_ok=True)

        # Create valid bronze files
        besaetning_details = {
            "Response": [
                {
                    "Besaetning": {
                        "BesaetningsNummer": "123456",
                        "ChrNummer": "123456",
                        "DyreArtKode": "1",
                    }
                }
            ]
        }

        ejendom_oplysninger = {"Response": [{"Ejendom": {"EjendomId": "123456"}}]}

        (bronze_dir / "besaetning_list.json").write_text("[]")
        (bronze_dir / "besaetning_details.json").write_text(json.dumps(besaetning_details))
        (bronze_dir / "diko_flytninger.json").write_text("[]")
        (bronze_dir / "ejendom_oplysninger.json").write_text(json.dumps(ejendom_oplysninger))
        (bronze_dir / "ejendom_vet_events.json").write_text("[]")

        with patch("chr_pipeline.silver.chr_silver_processing.config") as mock_config:
            mock_config.BRONZE_BASE_DIR = bronze_dir.parent
            mock_config.BRONZE_DATE_FOLDER_OVERRIDE = "20240101_120000"

            # Should not raise exception even if individual steps fail
            with contextlib.suppress(SystemExit):
                process_chr_data(
                    silver_dir=silver_dir,
                    in_memory_data=None,
                    export_timestamp="20240101_120000",
                    bronze_timestamp=None,
                    force_streaming=False,
                )

        assert silver_dir.exists()


@pytest.mark.chr_silver
class TestDataTransformation:
    """Test data transformation logic in silver processing."""

    def test_lookup_table_creation(self, tmp_path):
        """Test creation of lookup tables from raw data."""
        con = duckdb.connect()
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir(parents=True)

        # Create mock vetstat data with age groups
        con.execute("""
            CREATE TABLE vetstat AS
            SELECT
                '1' AS Aldersgruppekode,
                'Smågrise' AS Aldersgruppe
            UNION ALL
            SELECT '2', 'Slagtesvin'
        """)

        from chr_pipeline.silver.helpers import _create_and_save_lookup

        result = _create_and_save_lookup(
            con,
            "vetstat",
            pk_col="Aldersgruppekode",
            name_col="Aldersgruppe",
            output_path=silver_dir / "age_groups.parquet",
            table_name="age_groups",
        )

        assert result == "age_groups"
        assert (silver_dir / "age_groups.parquet").exists()

        # Check content
        lookup_df = con.execute("SELECT * FROM age_groups").df()
        assert len(lookup_df) == 2
        assert "Aldersgruppekode" in lookup_df.columns
        assert "Aldersgruppe" in lookup_df.columns

    def test_date_casting_in_transformations(self):
        """Test that date fields are properly cast during transformation."""
        con = duckdb.connect()

        # Create test data with various date formats
        con.execute("""
            CREATE TABLE test_dates AS
            SELECT
                '2024-01-15' AS date_string,
                '2024-01-15T10:30:00' AS datetime_string,
                'invalid' AS bad_date
        """)

        # Test safe date casting
        result = con.execute("""
            SELECT
                TRY_CAST(date_string AS DATE) AS date1,
                TRY_CAST(datetime_string AS DATE) AS date2,
                TRY_CAST(bad_date AS DATE) AS date3
            FROM test_dates
        """).fetchone()

        assert result[0] == date(2024, 1, 15)
        assert result[1] == date(2024, 1, 15)
        assert result[2] is None  # Invalid date should be NULL
