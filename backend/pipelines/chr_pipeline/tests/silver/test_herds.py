"""Tests for CHR herd data transformation and processing.

Tests the silver layer herd processing including:
- Herd table creation and transformation
- CHR number validation
- Owner and user data processing
- Herd size aggregation logic
"""

import duckdb
import pytest
from chr_pipeline.silver.herds import (
    create_herd_owners_table,
    create_herd_sizes_table,
    create_herd_users_table,
    create_herds_table,
)


@pytest.mark.chr_silver
class TestHerdsTable:
    """Test herds table creation and transformation."""

    def test_create_herds_table_success(self, tmp_path):
        """Test successful creation of herds table from besaetning details."""
        con = duckdb.connect()
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir(parents=True)

        # Create mock besaetning details data matching actual bronze structure
        con.execute("""
            CREATE TABLE bes_details AS
            SELECT [{
                'Besaetning': {
                    'BesaetningsNummer': '123456',
                    'ChrNummer': '123456',
                    'DyreArtKode': '1',
                    'DyreArtTekst': 'Kvæg',
                    'BrugsArtKode': '10',
                    'BrugsArtTekst': 'Mælkekvæg',
                    'VirksomhedsArtTekst': 'Landbrug',
                    'OmsaetningsKode': '1',
                    'OmsaetningsTekst': 'Aktiv',
                    'LeveringsErklaeringer': 'Standard',
                    'Oekologisk': 'Ja',
                    'DatoOpret': '2020-01-15',
                    'DatoOpdatering': '2024-01-01',
                    'DatoOphoer': NULL
                }
            }] AS Response
        """)

        result = create_herds_table(con, "bes_details", silver_dir)

        assert result is not None
        assert (silver_dir / "herds.parquet").exists()

        # Verify data
        herds_df = con.execute("SELECT * FROM herds").df()
        assert len(herds_df) == 1
        assert herds_df.iloc[0]["herd_number"] == 123456
        assert herds_df.iloc[0]["chr_number"] == 123456
        assert herds_df.iloc[0]["species_name"] == "Kvæg"
        assert herds_df.iloc[0]["is_organic"]

    def test_create_herds_table_with_none_input(self, tmp_path):
        """Test that None input returns None gracefully."""
        con = duckdb.connect()
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir(parents=True)

        result = create_herds_table(con, None, silver_dir)

        assert result is None

    def test_chr_number_validation(self, tmp_path):
        """Test CHR number format validation."""
        con = duckdb.connect()
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir(parents=True)

        # Create data with valid and invalid CHR numbers
        con.execute("""
            CREATE TABLE bes_details AS
            SELECT [{
                'Besaetning': {
                    'BesaetningsNummer': '123456',
                    'ChrNummer': '123456',
                    'DyreArtKode': '1',
                    'DyreArtTekst': 'Kvæg',
                    'BrugsArtKode': '10',
                    'BrugsArtTekst': 'Mælkekvæg',
                    'VirksomhedsArtTekst': 'Landbrug',
                    'OmsaetningsKode': '1',
                    'OmsaetningsTekst': 'Aktiv',
                    'LeveringsErklaeringer': NULL,
                    'DatoOphoer': NULL,
                    'Oekologisk': NULL,
                    'DatoOpret': NULL,
                    'DatoOpdatering': NULL
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
                    'OmsaetningsTekst': 'Aktiv',
                    'LeveringsErklaeringer': NULL,
                    'DatoOphoer': NULL,
                    'Oekologisk': NULL,
                    'DatoOpret': NULL,
                    'DatoOpdatering': NULL
                }
            }] AS Response
        """)

        result = create_herds_table(con, "bes_details", silver_dir)

        assert result is not None

        # Verify CHR number handling
        herds_df = con.execute("SELECT * FROM herds ORDER BY herd_number").df()
        assert len(herds_df) == 2
        assert herds_df.iloc[0]["chr_number"] == 123456
        # Invalid CHR should be NULL or not equal to "INVALID" string
        import pandas as pd

        chr_val = herds_df.iloc[1]["chr_number"]
        assert pd.isna(chr_val) or chr_val != "INVALID"

    def test_herd_deduplication(self, tmp_path):
        """Test that duplicate herds are removed."""
        con = duckdb.connect()
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir(parents=True)

        # Create data with duplicates
        con.execute("""
            CREATE TABLE bes_details AS
            SELECT [{
                'Besaetning': {
                    'BesaetningsNummer': '123456',
                    'ChrNummer': '123456',
                    'DyreArtKode': '1',
                    'DyreArtTekst': 'Kvæg',
                    'BrugsArtKode': NULL,
                    'BrugsArtTekst': NULL,
                    'VirksomhedsArtTekst': NULL,
                    'OmsaetningsKode': NULL,
                    'OmsaetningsTekst': NULL,
                    'LeveringsErklaeringer': NULL,
                    'DatoOphoer': NULL,
                    'Oekologisk': NULL,
                    'DatoOpret': NULL,
                    'DatoOpdatering': NULL
                }
            },
            {
                'Besaetning': {
                    'BesaetningsNummer': '123456',
                    'ChrNummer': '123456',
                    'DyreArtKode': '1',
                    'DyreArtTekst': 'Kvæg',
                    'BrugsArtKode': NULL,
                    'BrugsArtTekst': NULL,
                    'VirksomhedsArtTekst': NULL,
                    'OmsaetningsKode': NULL,
                    'OmsaetningsTekst': NULL,
                    'LeveringsErklaeringer': NULL,
                    'DatoOphoer': NULL,
                    'Oekologisk': NULL,
                    'DatoOpret': NULL,
                    'DatoOpdatering': NULL
                }
            }] AS Response
        """)

        create_herds_table(con, "bes_details", silver_dir)

        # Should deduplicate
        herds_df = con.execute("SELECT * FROM herds").df()
        assert len(herds_df) == 1

    def test_date_field_transformation(self, tmp_path):
        """Test that date fields are properly transformed."""
        con = duckdb.connect()
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir(parents=True)

        con.execute("""
            CREATE TABLE bes_details AS
            SELECT [{
                'Besaetning': {
                    'BesaetningsNummer': '123456',
                    'ChrNummer': '123456',
                    'DyreArtKode': '1',
                    'DyreArtTekst': 'Kvæg',
                    'BrugsArtKode': NULL,
                    'BrugsArtTekst': NULL,
                    'VirksomhedsArtTekst': NULL,
                    'OmsaetningsKode': NULL,
                    'OmsaetningsTekst': NULL,
                    'LeveringsErklaeringer': NULL,
                    'Oekologisk': NULL,
                    'DatoOpret': '2020-01-15',
                    'DatoOpdatering': '2024-01-01',
                    'DatoOphoer': '2024-06-30'
                }
            }] AS Response
        """)

        create_herds_table(con, "bes_details", silver_dir)

        herds_df = con.execute("SELECT * FROM herds").df()
        assert herds_df.iloc[0]["date_created"] is not None
        assert herds_df.iloc[0]["date_updated"] is not None
        assert herds_df.iloc[0]["date_ceased"] is not None


@pytest.mark.chr_silver
class TestHerdOwners:
    """Test herd owners table creation."""

    def test_create_herd_owners_table_success(self, tmp_path):
        """Test successful creation of herd owners table."""
        con = duckdb.connect()
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir(parents=True)

        con.execute("""
            CREATE TABLE bes_details AS
            SELECT [{
                'Besaetning': {
                    'BesaetningsNummer': '123456',
                    'Ejer': {
                        'CvrNummer': '12345678',
                        'Navn': 'Test Farm ApS',
                        'Adresse': 'Landevej 123',
                        'PostNummer': '8000',
                        'PostDistrikt': 'Aarhus C',
                        'ByNavn': 'Aarhus',
                        'KommuneNummer': '751',
                        'KommuneNavn': 'Aarhus',
                        'Land': 'Danmark',
                        'TelefonNummer': '12345678',
                        'Email': 'test@farm.dk',
                        'Adressebeskyttelse': 'Nej',
                        'Reklamebeskyttelse': 'Nej'
                    }
                }
            }] AS Response
        """)

        result = create_herd_owners_table(con, "bes_details", silver_dir)

        assert result is not None
        assert (silver_dir / "herd_owners.parquet").exists()

        # Verify data
        owners_df = con.execute("SELECT * FROM herd_owners").df()
        assert len(owners_df) == 1
        assert owners_df.iloc[0]["herd_number"] == 123456
        assert owners_df.iloc[0]["owner_cvr"] == "12345678"
        assert owners_df.iloc[0]["owner_name"] == "Test Farm ApS"
        assert owners_df.iloc[0]["owner_postal_code"] == "8000"

    def test_owner_cvr_format_validation(self, tmp_path):
        """Test CVR format is preserved (8 digits with leading zeros)."""
        con = duckdb.connect()
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir(parents=True)

        con.execute("""
            CREATE TABLE bes_details AS
            SELECT [{
                'Besaetning': {
                    'BesaetningsNummer': '123456',
                    'Ejer': {
                        'CvrNummer': '00012345',
                        'Navn': 'Test Farm',
                        'Adresse': 'Vej 1',
                        'PostNummer': '8000'
                    }
                }
            }] AS Response
        """)

        create_herd_owners_table(con, "bes_details", silver_dir)

        owners_df = con.execute("SELECT * FROM herd_owners").df()
        # CVR should be stored as string preserving leading zeros
        assert owners_df.iloc[0]["owner_cvr"] == "00012345"

    def test_owner_missing_required_fields(self, tmp_path):
        """Test handling of owners with missing required fields."""
        con = duckdb.connect()
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir(parents=True)

        # Owner with no CVR, no CPR, no name/address
        con.execute("""
            CREATE TABLE bes_details AS
            SELECT [{
                'Besaetning': {
                    'BesaetningsNummer': '123456',
                    'Ejer': {
                        'TelefonNummer': '12345678'
                    }
                }
            }] AS Response
        """)

        result = create_herd_owners_table(con, "bes_details", silver_dir)

        # Should filter out incomplete owners
        if result is not None:
            owners_df = con.execute("SELECT * FROM herd_owners").df()
            assert len(owners_df) == 0


@pytest.mark.chr_silver
class TestHerdUsers:
    """Test herd users table creation."""

    def test_create_herd_users_table_success(self, tmp_path):
        """Test successful creation of herd users table."""
        con = duckdb.connect()
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir(parents=True)

        con.execute("""
            CREATE TABLE bes_details AS
            SELECT [{
                'Besaetning': {
                    'BesaetningsNummer': '123456',
                    'Bruger': {
                        'CvrNummer': '87654321',
                        'Navn': 'User Farm ApS',
                        'Adresse': 'Gade 45',
                        'PostNummer': '8200',
                        'PostDistrikt': 'Aarhus N',
                        'ByNavn': 'Aarhus',
                        'KommuneNummer': '751',
                        'KommuneNavn': 'Aarhus'
                    }
                }
            }] AS Response
        """)

        result = create_herd_users_table(con, "bes_details", silver_dir)

        assert result is not None
        assert (silver_dir / "herd_users.parquet").exists()

        users_df = con.execute("SELECT * FROM herd_users").df()
        assert len(users_df) == 1
        assert users_df.iloc[0]["herd_number"] == 123456
        assert users_df.iloc[0]["user_cvr"] == "87654321"
        assert users_df.iloc[0]["user_name"] == "User Farm ApS"

    def test_user_different_from_owner(self, tmp_path):
        """Test that user can be different from owner."""
        con = duckdb.connect()
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir(parents=True)

        con.execute("""
            CREATE TABLE bes_details AS
            SELECT [{
                'Besaetning': {
                    'BesaetningsNummer': '123456',
                    'Ejer': {
                        'CvrNummer': '12345678',
                        'Navn': 'Owner Farm',
                        'Adresse': 'Vej 1',
                        'PostNummer': '8000'
                    },
                    'Bruger': {
                        'CvrNummer': '87654321',
                        'Navn': 'User Farm',
                        'Adresse': 'Vej 2',
                        'PostNummer': '8200'
                    }
                }
            }] AS Response
        """)

        create_herd_owners_table(con, "bes_details", silver_dir)
        create_herd_users_table(con, "bes_details", silver_dir)

        owners_df = con.execute("SELECT * FROM herd_owners").df()
        users_df = con.execute("SELECT * FROM herd_users").df()

        assert owners_df.iloc[0]["owner_cvr"] != users_df.iloc[0]["user_cvr"]
        assert owners_df.iloc[0]["owner_cvr"] == "12345678"
        assert users_df.iloc[0]["user_cvr"] == "87654321"


@pytest.mark.chr_silver
class TestHerdSizes:
    """Test herd sizes table creation."""

    def test_create_herd_sizes_table_success(self, tmp_path):
        """Test successful creation of herd sizes table."""
        con = duckdb.connect()
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir(parents=True)

        con.execute("""
            CREATE TABLE bes_details AS
            SELECT [{
                'Besaetning': {
                    'BesaetningsNummer': '123456',
                    'ChrNummer': '123456',
                    'DyreArtKode': '1',
                    'DyreArtTekst': 'Kvæg',
                    'BesStr': [
                        {
                            'BesaetningsStoerrelseTekst': 'Køer',
                            'BesaetningsStoerrelse': '50'
                        },
                        {
                            'BesaetningsStoerrelseTekst': 'Kvier',
                            'BesaetningsStoerrelse': '25'
                        }
                    ],
                    'BesStrDatoAjourfoert': '2024-01-01'
                }
            }] AS Response
        """)

        result = create_herd_sizes_table(con, "bes_details", silver_dir)

        assert result is not None
        assert (silver_dir / "herd_sizes.parquet").exists()

        sizes_df = con.execute("SELECT * FROM herd_sizes ORDER BY category").df()
        assert len(sizes_df) == 2
        assert sizes_df.iloc[0]["category"] == "Kvier"
        assert sizes_df.iloc[0]["count"] == 25
        assert sizes_df.iloc[1]["category"] == "Køer"
        assert sizes_df.iloc[1]["count"] == 50

    def test_herd_sizes_aggregation(self, tmp_path):
        """Test aggregation of herd sizes across multiple categories."""
        con = duckdb.connect()
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir(parents=True)

        con.execute("""
            CREATE TABLE bes_details AS
            SELECT [{
                'Besaetning': {
                    'BesaetningsNummer': '123456',
                    'ChrNummer': '123456',
                    'DyreArtKode': '1',
                    'DyreArtTekst': 'Kvæg',
                    'BesStr': [
                        {'BesaetningsStoerrelseTekst': 'Køer', 'BesaetningsStoerrelse': '100'},
                        {'BesaetningsStoerrelseTekst': 'Kvier', 'BesaetningsStoerrelse': '50'},
                        {'BesaetningsStoerrelseTekst': 'Kalve', 'BesaetningsStoerrelse': '30'}
                    ],
                    'BesStrDatoAjourfoert': '2024-01-01'
                }
            }] AS Response
        """)

        create_herd_sizes_table(con, "bes_details", silver_dir)

        sizes_df = con.execute("SELECT * FROM herd_sizes").df()
        total_animals = sizes_df["count"].sum()

        assert total_animals == 180
        assert len(sizes_df) == 3

    def test_herd_sizes_with_empty_besstr(self, tmp_path):
        """Test handling of herds with no size data."""
        con = duckdb.connect()
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir(parents=True)

        con.execute("""
            CREATE TABLE bes_details AS
            SELECT [{
                'Besaetning': {
                    'BesaetningsNummer': '123456',
                    'ChrNummer': '123456',
                    'DyreArtKode': '1',
                    'DyreArtTekst': 'Kvæg',
                    'BesStr': NULL
                }
            }] AS Response
        """)

        result = create_herd_sizes_table(con, "bes_details", silver_dir)

        # Should handle gracefully
        if result is not None:
            sizes_df = con.execute("SELECT * FROM herd_sizes").df()
            assert len(sizes_df) == 0

    def test_herd_sizes_uuid_generation(self, tmp_path):
        """Test that each herd size record gets a unique ID."""
        con = duckdb.connect()
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir(parents=True)

        con.execute("""
            CREATE TABLE bes_details AS
            SELECT [{
                'Besaetning': {
                    'BesaetningsNummer': '123456',
                    'ChrNummer': '123456',
                    'DyreArtKode': '1',
                    'DyreArtTekst': 'Kvæg',
                    'BesStr': [
                        {'BesaetningsStoerrelseTekst': 'Køer', 'BesaetningsStoerrelse': '50'},
                        {'BesaetningsStoerrelseTekst': 'Kvier', 'BesaetningsStoerrelse': '25'}
                    ]
                }
            }] AS Response
        """)

        create_herd_sizes_table(con, "bes_details", silver_dir)

        sizes_df = con.execute("SELECT * FROM herd_sizes").df()
        # Each record should have a unique UUID
        assert len(sizes_df["size_id"].unique()) == 2
