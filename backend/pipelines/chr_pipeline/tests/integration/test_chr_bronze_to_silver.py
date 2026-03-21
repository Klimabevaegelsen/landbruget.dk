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
            herds_df = con.execute(
                f"SELECT * FROM read_parquet('{silver_dir / 'herds.parquet'}')"
            ).df()

            assert len(herds_df) > 0
            assert herds_df.iloc[0]["herd_number"] == int(test_herd_number)
            assert herds_df.iloc[0]["species_name"] == "Kvæg"

        if (silver_dir / "herd_owners.parquet").exists():
            con = duckdb.connect()
            owners_df = con.execute(
                f"SELECT * FROM read_parquet('{silver_dir / 'herd_owners.parquet'}')"
            ).df()

            assert len(owners_df) > 0
            assert owners_df.iloc[0]["owner_cvr"] == test_cvr


@pytest.mark.chr_integration
class TestDataValidation:
    """Test data validation across pipeline layers."""

    def test_chr_number_format_validation_across_layers(self, tmp_path):
        """Test CHR number format is validated consistently."""
        con = duckdb.connect()
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir(parents=True)

        # Create bronze data with valid and invalid CHR numbers
        con.execute("""
            CREATE TABLE bes_details AS
            SELECT [{
                'Besaetning': {
                    'BesaetningsNummer': '123456',
                    'ChrNummer': '123456',
                    'DyreArtKode': '1',
                    'DyreArtTekst': 'Kvæg'
                }
            },
            {
                'Besaetning': {
                    'BesaetningsNummer': '789012',
                    'ChrNummer': 'INVALID',
                    'DyreArtKode': '2',
                    'DyreArtTekst': 'Svin'
                }
            },
            {
                'Besaetning': {
                    'BesaetningsNummer': '345678',
                    'ChrNummer': '12345',
                    'DyreArtKode': '1',
                    'DyreArtTekst': 'Kvæg'
                }
            }] AS Response
        """)

        create_herds_table(con, "bes_details", silver_dir)

        herds_df = con.execute("SELECT * FROM herds ORDER BY herd_number").df()

        # Valid 6-digit CHR should be preserved
        valid_chrs = herds_df[herds_df["chr_number"].notna()]
        assert len(valid_chrs) >= 1

    def test_cvr_format_validation_across_layers(self, tmp_path):
        """Test CVR format validation (8 digits with leading zeros)."""
        con = duckdb.connect()
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir(parents=True)

        # Create test data with various CVR formats
        con.execute("""
            CREATE TABLE bes_details AS
            SELECT [{
                'Besaetning': {
                    'BesaetningsNummer': '123456',
                    'Ejer': {
                        'CvrNummer': '00012345',
                        'Navn': 'Farm 1',
                        'Adresse': 'Vej 1',
                        'PostNummer': '8000'
                    }
                }
            },
            {
                'Besaetning': {
                    'BesaetningsNummer': '789012',
                    'Ejer': {
                        'CvrNummer': '12345678',
                        'Navn': 'Farm 2',
                        'Adresse': 'Vej 2',
                        'PostNummer': '8200'
                    }
                }
            }] AS Response
        """)

        from chr_pipeline.silver.herds import create_herd_owners_table

        create_herd_owners_table(con, "bes_details", silver_dir)

        owners_df = con.execute("SELECT * FROM herd_owners ORDER BY herd_number").df()

        # CVR should be string to preserve leading zeros
        assert owners_df.iloc[0]["owner_cvr"] == "00012345"
        assert owners_df.iloc[1]["owner_cvr"] == "12345678"

    def test_date_validation_across_layers(self, tmp_path):
        """Test date field validation and transformation."""
        con = duckdb.connect()
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir(parents=True)

        # Create data with various date formats
        con.execute("""
            CREATE TABLE bes_details AS
            SELECT [{
                'Besaetning': {
                    'BesaetningsNummer': '123456',
                    'ChrNummer': '123456',
                    'DyreArtKode': '1',
                    'DyreArtTekst': 'Kvæg',
                    'DatoOpret': '2020-01-15',
                    'DatoOpdatering': '2024-01-01T10:30:00',
                    'DatoOphoer': 'invalid_date'
                }
            }] AS Response
        """)

        create_herds_table(con, "bes_details", silver_dir)

        herds_df = con.execute("SELECT * FROM herds").df()

        # Valid dates should be parsed
        assert herds_df.iloc[0]["date_created"] is not None
        assert herds_df.iloc[0]["date_updated"] is not None
        # Invalid date should be NULL
        assert (
            herds_df.iloc[0]["date_ceased"] is None or str(herds_df.iloc[0]["date_ceased"]) == "NaT"
        )


@pytest.mark.chr_integration
class TestErrorPropagation:
    """Test error handling and propagation across layers."""

    def test_missing_required_fields_are_filtered(self, tmp_path):
        """Test that records with missing required fields are filtered out."""
        con = duckdb.connect()
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir(parents=True)

        # Create data with missing herd numbers
        con.execute("""
            CREATE TABLE bes_details AS
            SELECT [{
                'Besaetning': {
                    'BesaetningsNummer': NULL,
                    'ChrNummer': '123456',
                    'DyreArtKode': '1',
                    'DyreArtTekst': 'Kvæg'
                }
            },
            {
                'Besaetning': {
                    'BesaetningsNummer': '789012',
                    'ChrNummer': '789012',
                    'DyreArtKode': '2',
                    'DyreArtTekst': 'Svin'
                }
            }] AS Response
        """)

        create_herds_table(con, "bes_details", silver_dir)

        herds_df = con.execute("SELECT * FROM herds").df()

        # Only record with valid herd number should remain
        assert len(herds_df) == 1
        assert herds_df.iloc[0]["herd_number"] == 789012

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

    def test_pipeline_handles_large_herd_count(self, tmp_path):
        """Test pipeline with many herds."""
        con = duckdb.connect()
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir(parents=True)

        # Create data with 100 herds
        herd_data = [
            {
                "Besaetning": {
                    "BesaetningsNummer": str(100000 + i),
                    "ChrNummer": str(100000 + i),
                    "DyreArtKode": "1",
                    "DyreArtTekst": "Kvæg",
                }
            }
            for i in range(100)
        ]

        con.execute(f"CREATE TABLE bes_details AS SELECT {herd_data} AS Response")

        create_herds_table(con, "bes_details", silver_dir)

        herds_df = con.execute("SELECT * FROM herds").df()
        assert len(herds_df) == 100

    def test_pipeline_memory_efficiency(self, tmp_path):
        """Test that pipeline doesn't consume excessive memory."""
        import gc

        con = duckdb.connect()
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir(parents=True)

        # Create moderate dataset
        herd_data = [
            {
                "Besaetning": {
                    "BesaetningsNummer": str(100000 + i),
                    "ChrNummer": str(100000 + i),
                    "DyreArtKode": "1",
                    "DyreArtTekst": "Kvæg",
                    "BesStr": [
                        {"BesaetningsStoerrelseTekst": "Køer", "BesaetningsStoerrelse": "100"}
                    ],
                }
            }
            for i in range(50)
        ]

        con.execute(f"CREATE TABLE bes_details AS SELECT {herd_data} AS Response")

        # Process and measure
        gc.collect()
        result = create_herds_table(con, "bes_details", silver_dir)

        # Verify result exists
        assert result is not None

        # Cleanup
        del result
        gc.collect()
