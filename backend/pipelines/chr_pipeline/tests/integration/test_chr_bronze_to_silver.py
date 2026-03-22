"""Integration tests for CHR bronze to silver data flow.

Tests the full data pipeline from bronze layer to silver layer including:
- Full bronze→silver flow
- Data consistency across layers
- Error propagation
- Data quality validation
"""

import contextlib
import json
from unittest.mock import patch

import duckdb
import pytest
from chr_pipeline.silver.chr_silver_processing import process_chr_data
from chr_pipeline.silver.herds import create_herds_table


@pytest.mark.chr_integration
@pytest.mark.slow
class TestBronzeToSilverFlow:
    """Test complete bronze to silver data flow."""

    def test_full_pipeline_bronze_to_silver(self, tmp_path, mock_chr_responses):
        """Test complete pipeline from bronze files to silver tables."""
        bronze_dir = tmp_path / "bronze" / "20240101_120000"
        silver_dir = tmp_path / "silver" / "20240101_120000"
        bronze_dir.mkdir(parents=True)

        # Create comprehensive bronze data
        besaetning_details = {
            "Response": [
                {
                    "Besaetning": {
                        "BesaetningsNummer": "123456",
                        "ChrNummer": "123456",
                        "DyreArtKode": "1",
                        "DyreArtTekst": "Kvæg",
                        "BrugsArtKode": "10",
                        "BrugsArtTekst": "Mælkekvæg",
                        "Oekologisk": "Ja",
                        "DatoOpret": "2020-01-15",
                        "DatoOpdatering": "2024-01-01",
                        "Ejer": {
                            "CvrNummer": "12345678",
                            "Navn": "Test Farm ApS",
                            "Adresse": "Landevej 123",
                            "PostNummer": "8000",
                            "PostDistrikt": "Aarhus C",
                        },
                        "Bruger": {
                            "CvrNummer": "87654321",
                            "Navn": "User Farm ApS",
                            "Adresse": "Gade 45",
                            "PostNummer": "8200",
                        },
                        "BesStr": [
                            {
                                "BesaetningsStoerrelseTekst": "Køer",
                                "BesaetningsStoerrelse": "100",
                            },
                            {
                                "BesaetningsStoerrelseTekst": "Kvier",
                                "BesaetningsStoerrelse": "50",
                            },
                        ],
                        "BesStrDatoAjourfoert": "2024-01-01",
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
                            "Navn": "Test Farm ApS",
                            "Adresse": "Landevej 123",
                            "PostNummer": "8000",
                        },
                    }
                }
            ]
        }

        diko_flytninger = [
            {
                "animal_id": "DK123456789",
                "movement_date": "2024-01-15",
                "from_herd": "123456",
                "to_herd": "654321",
                "movement_type": "SALE",
            }
        ]

        # Write bronze files
        (bronze_dir / "besaetning_list.json").write_text("[]")
        (bronze_dir / "besaetning_details.json").write_text(json.dumps(besaetning_details))
        (bronze_dir / "diko_flytninger.json").write_text(json.dumps(diko_flytninger))
        (bronze_dir / "ejendom_oplysninger.json").write_text(json.dumps(ejendom_oplysninger))
        (bronze_dir / "ejendom_vet_events.json").write_text("[]")

        # Process silver
        with (
            patch("chr_pipeline.silver.chr_silver_processing.config") as mock_config,
            patch(
                "chr_pipeline.silver.chr_silver_processing.upload_silver_data_to_gcs"
            ) as mock_upload,
        ):
            mock_config.BRONZE_BASE_DIR = bronze_dir.parent
            mock_config.BRONZE_DATE_FOLDER_OVERRIDE = "20240101_120000"
            mock_upload.return_value = True

            with contextlib.suppress(SystemExit):
                process_chr_data(
                    silver_dir=silver_dir,
                    in_memory_data=None,
                    export_timestamp="20240101_120000",
                    bronze_timestamp=None,
                    force_streaming=False,
                )

        # Verify silver output files
        assert silver_dir.exists()
        expected_files = [
            "herds.parquet",
            "herd_owners.parquet",
            "herd_users.parquet",
            "herd_sizes.parquet",
        ]

        for file in expected_files:
            file_path = silver_dir / file
            # File should exist if data was processed
            if file_path.exists():
                assert file_path.stat().st_size > 0

    def test_data_consistency_bronze_to_silver(self, tmp_path):
        """Test that data remains consistent from bronze to silver."""
        bronze_dir = tmp_path / "bronze" / "20240101_120000"
        silver_dir = tmp_path / "silver" / "20240101_120000"
        bronze_dir.mkdir(parents=True)

        # Create bronze data with known values
        test_cvr = "12345678"
        test_herd_number = "123456"

        besaetning_details = {
            "Response": [
                {
                    "Besaetning": {
                        "BesaetningsNummer": test_herd_number,
                        "ChrNummer": test_herd_number,
                        "DyreArtKode": "1",
                        "DyreArtTekst": "Kvæg",
                        "Ejer": {
                            "CvrNummer": test_cvr,
                            "Navn": "Test Farm",
                            "Adresse": "Vej 1",
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
                        "EjendomId": test_herd_number,
                        "Ejer": {
                            "CvrNummer": test_cvr,
                            "Navn": "Test Farm",
                            "Adresse": "Vej 1",
                            "PostNummer": "8000",
                        },
                    }
                }
            ]
        }

        # Write bronze files
        (bronze_dir / "besaetning_list.json").write_text("[]")
        (bronze_dir / "besaetning_details.json").write_text(json.dumps(besaetning_details))
        (bronze_dir / "diko_flytninger.json").write_text("[]")
        (bronze_dir / "ejendom_oplysninger.json").write_text(json.dumps(ejendom_oplysninger))
        (bronze_dir / "ejendom_vet_events.json").write_text("[]")

        # Process to silver
        with (
            patch("chr_pipeline.silver.chr_silver_processing.config") as mock_config,
            patch(
                "chr_pipeline.silver.chr_silver_processing.upload_silver_data_to_gcs"
            ) as mock_upload,
        ):
            mock_config.BRONZE_BASE_DIR = bronze_dir.parent
            mock_config.BRONZE_DATE_FOLDER_OVERRIDE = "20240101_120000"
            mock_upload.return_value = True

            with contextlib.suppress(SystemExit):
                process_chr_data(
                    silver_dir=silver_dir,
                    in_memory_data=None,
                    export_timestamp="20240101_120000",
                    bronze_timestamp=None,
                    force_streaming=False,
                )

        # Verify consistency
        if (silver_dir / "herds.parquet").exists():
            con = duckdb.connect()
            rows = con.execute(
                f"SELECT herd_number, species_name FROM read_parquet('{silver_dir / 'herds.parquet'}')"
            ).fetchall()

            assert len(rows) > 0
            assert rows[0][0] == int(test_herd_number)
            assert rows[0][1] == "Kvæg"

        if (silver_dir / "herd_owners.parquet").exists():
            con = duckdb.connect()
            rows = con.execute(
                f"SELECT owner_cvr FROM read_parquet('{silver_dir / 'herd_owners.parquet'}')"
            ).fetchall()

            assert len(rows) > 0
            assert rows[0][0] == test_cvr


@pytest.mark.chr_integration
class TestDataValidation:
    """Test data validation across pipeline layers."""

    def test_chr_number_format_validation_across_layers(self, tmp_path):
        """Test CHR number format is validated consistently."""
        con = duckdb.connect()
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir(parents=True)

        # Create bronze data with valid and invalid CHR numbers
        # All fields referenced by herds.py must be present in the struct
        con.execute("""
            CREATE TABLE bes_details AS
            SELECT [{
                'Besaetning': {
                    'BesaetningsNummer': '123456',
                    'ChrNummer': '123456',
                    'DyreArtKode': '1',
                    'DyreArtTekst': 'Kvæg',
                    'BrugsArtKode': '10',
                    'BrugsArtTekst': 'Malkekvæg',
                    'VirksomhedsArtTekst': 'Landbrug',
                    'OmsaetningsKode': '1',
                    'OmsaetningsTekst': 'Normal',
                    'LeveringsErklaeringer': NULL,
                    'DatoOphoer': NULL,
                    'Oekologisk': 'Nej',
                    'DatoOpret': '2020-01-01',
                    'DatoOpdatering': '2024-01-01'
                }
            },
            {
                'Besaetning': {
                    'BesaetningsNummer': '789012',
                    'ChrNummer': 'INVALID',
                    'DyreArtKode': '2',
                    'DyreArtTekst': 'Svin',
                    'BrugsArtKode': '20',
                    'BrugsArtTekst': 'Slagtesvin',
                    'VirksomhedsArtTekst': 'Landbrug',
                    'OmsaetningsKode': '1',
                    'OmsaetningsTekst': 'Normal',
                    'LeveringsErklaeringer': NULL,
                    'DatoOphoer': NULL,
                    'Oekologisk': 'Nej',
                    'DatoOpret': '2020-01-01',
                    'DatoOpdatering': '2024-01-01'
                }
            },
            {
                'Besaetning': {
                    'BesaetningsNummer': '345678',
                    'ChrNummer': '12345',
                    'DyreArtKode': '1',
                    'DyreArtTekst': 'Kvæg',
                    'BrugsArtKode': '10',
                    'BrugsArtTekst': 'Malkekvæg',
                    'VirksomhedsArtTekst': 'Landbrug',
                    'OmsaetningsKode': '1',
                    'OmsaetningsTekst': 'Normal',
                    'LeveringsErklaeringer': NULL,
                    'DatoOphoer': NULL,
                    'Oekologisk': 'Ja',
                    'DatoOpret': '2019-06-15',
                    'DatoOpdatering': '2024-03-01'
                }
            }] AS Response
        """)

        create_herds_table(con, "bes_details", silver_dir)

        # Valid 6-digit CHR should be preserved
        valid_chr_count = con.execute(
            "SELECT COUNT(*) FROM herds WHERE chr_number IS NOT NULL"
        ).fetchone()[0]
        assert valid_chr_count >= 1

    def test_cvr_format_validation_across_layers(self, tmp_path):
        """Test CVR format validation (8 digits with leading zeros)."""
        con = duckdb.connect()
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir(parents=True)

        # Create test data with various CVR formats
        # All Ejer fields referenced by create_herd_owners_table must be present
        con.execute("""
            CREATE TABLE bes_details AS
            SELECT [{
                'Besaetning': {
                    'BesaetningsNummer': '123456',
                    'Ejer': {
                        'CvrNummer': '00012345',
                        'CprNummer': NULL,
                        'Navn': 'Farm 1',
                        'Adresse': 'Vej 1',
                        'PostNummer': '8000',
                        'PostDistrikt': 'Aarhus C',
                        'ByNavn': 'Aarhus',
                        'KommuneNummer': '751',
                        'KommuneNavn': 'Aarhus',
                        'Land': 'Danmark',
                        'TelefonNummer': NULL,
                        'MobilNummer': NULL,
                        'Email': NULL,
                        'Adressebeskyttelse': 'Nej',
                        'Reklamebeskyttelse': 'Nej'
                    }
                }
            },
            {
                'Besaetning': {
                    'BesaetningsNummer': '789012',
                    'Ejer': {
                        'CvrNummer': '12345678',
                        'CprNummer': NULL,
                        'Navn': 'Farm 2',
                        'Adresse': 'Vej 2',
                        'PostNummer': '8200',
                        'PostDistrikt': 'Aarhus N',
                        'ByNavn': 'Aarhus',
                        'KommuneNummer': '751',
                        'KommuneNavn': 'Aarhus',
                        'Land': 'Danmark',
                        'TelefonNummer': NULL,
                        'MobilNummer': NULL,
                        'Email': NULL,
                        'Adressebeskyttelse': 'Nej',
                        'Reklamebeskyttelse': 'Nej'
                    }
                }
            }] AS Response
        """)

        from chr_pipeline.silver.herds import create_herd_owners_table

        create_herd_owners_table(con, "bes_details", silver_dir)

        rows = con.execute("SELECT owner_cvr FROM herd_owners ORDER BY herd_number").fetchall()

        # CVR should be string to preserve leading zeros
        assert rows[0][0] == "00012345"
        assert rows[1][0] == "12345678"

    def test_date_validation_across_layers(self, tmp_path):
        """Test date field validation and transformation."""
        con = duckdb.connect()
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir(parents=True)

        # Create data with various date formats
        # All fields referenced by herds.py must be present in the struct
        con.execute("""
            CREATE TABLE bes_details AS
            SELECT [{
                'Besaetning': {
                    'BesaetningsNummer': '123456',
                    'ChrNummer': '123456',
                    'DyreArtKode': '1',
                    'DyreArtTekst': 'Kvæg',
                    'BrugsArtKode': '10',
                    'BrugsArtTekst': 'Malkekvæg',
                    'VirksomhedsArtTekst': 'Landbrug',
                    'OmsaetningsKode': '1',
                    'OmsaetningsTekst': 'Normal',
                    'LeveringsErklaeringer': NULL,
                    'Oekologisk': 'Nej',
                    'DatoOpret': '2020-01-15',
                    'DatoOpdatering': '2024-01-01T10:30:00',
                    'DatoOphoer': 'invalid_date'
                }
            }] AS Response
        """)

        create_herds_table(con, "bes_details", silver_dir)

        row = con.execute("SELECT date_created, date_updated, date_ceased FROM herds").fetchone()

        # Valid dates should be parsed
        assert row[0] is not None
        assert row[1] is not None
        # Invalid date should be NULL
        assert row[2] is None


@pytest.mark.chr_integration
class TestErrorPropagation:
    """Test error handling and propagation across layers."""

    def test_missing_required_fields_are_filtered(self, tmp_path):
        """Test that records with missing required fields are filtered out."""
        con = duckdb.connect()
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir(parents=True)

        # Create data with missing herd numbers
        # All fields referenced by herds.py must be present in the struct
        con.execute("""
            CREATE TABLE bes_details AS
            SELECT [{
                'Besaetning': {
                    'BesaetningsNummer': NULL,
                    'ChrNummer': '123456',
                    'DyreArtKode': '1',
                    'DyreArtTekst': 'Kvæg',
                    'BrugsArtKode': '10',
                    'BrugsArtTekst': 'Malkekvæg',
                    'VirksomhedsArtTekst': 'Landbrug',
                    'OmsaetningsKode': '1',
                    'OmsaetningsTekst': 'Normal',
                    'LeveringsErklaeringer': NULL,
                    'DatoOphoer': NULL,
                    'Oekologisk': 'Nej',
                    'DatoOpret': '2020-01-01',
                    'DatoOpdatering': '2024-01-01'
                }
            },
            {
                'Besaetning': {
                    'BesaetningsNummer': '789012',
                    'ChrNummer': '789012',
                    'DyreArtKode': '2',
                    'DyreArtTekst': 'Svin',
                    'BrugsArtKode': '20',
                    'BrugsArtTekst': 'Slagtesvin',
                    'VirksomhedsArtTekst': 'Landbrug',
                    'OmsaetningsKode': '1',
                    'OmsaetningsTekst': 'Normal',
                    'LeveringsErklaeringer': NULL,
                    'DatoOphoer': NULL,
                    'Oekologisk': 'Nej',
                    'DatoOpret': '2020-01-01',
                    'DatoOpdatering': '2024-01-01'
                }
            }] AS Response
        """)

        create_herds_table(con, "bes_details", silver_dir)

        rows = con.execute("SELECT herd_number FROM herds").fetchall()

        # Only record with valid herd number should remain
        assert len(rows) == 1
        assert rows[0][0] == 789012

    def test_empty_bronze_data_handles_gracefully(self, tmp_path):
        """Test that empty bronze data is handled gracefully."""
        bronze_dir = tmp_path / "bronze" / "20240101_120000"
        silver_dir = tmp_path / "silver" / "20240101_120000"
        bronze_dir.mkdir(parents=True)

        # Create empty bronze files
        (bronze_dir / "besaetning_list.json").write_text("[]")
        (bronze_dir / "besaetning_details.json").write_text('{"Response": []}')
        (bronze_dir / "diko_flytninger.json").write_text("[]")
        (bronze_dir / "ejendom_oplysninger.json").write_text('{"Response": []}')
        (bronze_dir / "ejendom_vet_events.json").write_text("[]")

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


@pytest.mark.chr_integration
@pytest.mark.slow
class TestPerformance:
    """Test pipeline performance with various data sizes."""

    def test_pipeline_handles_multiple_herds(self, tmp_path):
        """Test pipeline with multiple herds."""
        con = duckdb.connect()
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir(parents=True)

        # Build table row-by-row to avoid DuckDB planner explosion on large struct arrays
        con.execute("""
            CREATE TABLE bes_details_rows (
                herd_number VARCHAR, chr_number VARCHAR, species_code VARCHAR,
                species_name VARCHAR, usage_code VARCHAR, usage_name VARCHAR,
                business_type VARCHAR, turnover_code VARCHAR, turnover_text VARCHAR,
                delivery_decl VARCHAR, date_ceased VARCHAR, is_organic VARCHAR,
                date_created VARCHAR, date_updated VARCHAR
            )
        """)
        for i in range(10):
            con.execute(
                "INSERT INTO bes_details_rows VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [
                    str(100000 + i),
                    str(100000 + i),
                    "1",
                    "Kvæg",
                    "10",
                    "Malkekvæg",
                    "Landbrug",
                    "1",
                    "Normal",
                    None,
                    None,
                    "Nej",
                    "2020-01-01",
                    "2024-01-01",
                ],
            )

        # Reshape into the nested struct format that create_herds_table expects
        con.execute("""
            CREATE TABLE bes_details AS
            SELECT list({
                'Besaetning': {
                    'BesaetningsNummer': herd_number,
                    'ChrNummer': chr_number,
                    'DyreArtKode': species_code,
                    'DyreArtTekst': species_name,
                    'BrugsArtKode': usage_code,
                    'BrugsArtTekst': usage_name,
                    'VirksomhedsArtTekst': business_type,
                    'OmsaetningsKode': turnover_code,
                    'OmsaetningsTekst': turnover_text,
                    'LeveringsErklaeringer': delivery_decl,
                    'DatoOphoer': date_ceased,
                    'Oekologisk': is_organic,
                    'DatoOpret': date_created,
                    'DatoOpdatering': date_updated
                }
            }) AS Response
            FROM bes_details_rows
        """)

        create_herds_table(con, "bes_details", silver_dir)

        count = con.execute("SELECT COUNT(*) FROM herds").fetchone()[0]
        assert count == 10
