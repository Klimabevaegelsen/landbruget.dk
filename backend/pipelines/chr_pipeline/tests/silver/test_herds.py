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
        row = con.execute(
            "SELECT herd_number, chr_number, species_name, is_organic FROM herds"
        ).fetchone()
        assert row is not None
        assert row[0] == 123456
        assert row[1] == 123456
        assert row[2] == "Kvæg"
        assert row[3] is True

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
        rows = con.execute(
            "SELECT herd_number, chr_number FROM herds ORDER BY herd_number"
        ).fetchall()
        assert len(rows) == 2
        assert rows[0][1] == 123456
        # Invalid CHR should be NULL (TRY_CAST of 'INVALID' to BIGINT yields NULL)
        assert rows[1][1] is None

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
        count = con.execute("SELECT COUNT(*) FROM herds").fetchone()[0]
        assert count == 1

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

        row = con.execute("SELECT date_created, date_updated, date_ceased FROM herds").fetchone()
        assert row[0] is not None
        assert row[1] is not None
        assert row[2] is not None


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
                        'CprNummer': NULL,
                        'Navn': 'Test Farm ApS',
                        'Adresse': 'Landevej 123',
                        'PostNummer': '8000',
                        'PostDistrikt': 'Aarhus C',
                        'ByNavn': 'Aarhus',
                        'KommuneNummer': '751',
                        'KommuneNavn': 'Aarhus',
                        'Land': 'Danmark',
                        'TelefonNummer': '12345678',
                        'MobilNummer': NULL,
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
        row = con.execute(
            "SELECT herd_number, owner_cvr, owner_name, owner_postal_code FROM herd_owners"
        ).fetchone()
        assert row is not None
        assert row[0] == 123456
        assert row[1] == "12345678"
        assert row[2] == "Test Farm ApS"
        assert row[3] == "8000"

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
                        'CprNummer': NULL,
                        'Navn': 'Test Farm',
                        'Adresse': 'Vej 1',
                        'PostNummer': '8000',
                        'PostDistrikt': NULL,
                        'ByNavn': NULL,
                        'KommuneNummer': NULL,
                        'KommuneNavn': NULL,
                        'Land': NULL,
                        'TelefonNummer': NULL,
                        'MobilNummer': NULL,
                        'Email': NULL,
                        'Adressebeskyttelse': NULL,
                        'Reklamebeskyttelse': NULL
                    }
                }
            }] AS Response
        """)

        create_herd_owners_table(con, "bes_details", silver_dir)

        row = con.execute("SELECT owner_cvr FROM herd_owners").fetchone()
        # CVR should be stored as string preserving leading zeros
        assert row[0] == "00012345"

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
                        'CvrNummer': NULL,
                        'CprNummer': NULL,
                        'Navn': NULL,
                        'Adresse': NULL,
                        'PostNummer': NULL,
                        'PostDistrikt': NULL,
                        'ByNavn': NULL,
                        'KommuneNummer': NULL,
                        'KommuneNavn': NULL,
                        'Land': NULL,
                        'TelefonNummer': '12345678',
                        'MobilNummer': NULL,
                        'Email': NULL,
                        'Adressebeskyttelse': NULL,
                        'Reklamebeskyttelse': NULL
                    }
                }
            }] AS Response
        """)

        result = create_herd_owners_table(con, "bes_details", silver_dir)

        # Should filter out incomplete owners
        if result is not None:
            count = con.execute("SELECT COUNT(*) FROM herd_owners").fetchone()[0]
            assert count == 0


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
                        'CprNummer': NULL,
                        'Navn': 'User Farm ApS',
                        'Adresse': 'Gade 45',
                        'PostNummer': '8200',
                        'PostDistrikt': 'Aarhus N',
                        'ByNavn': 'Aarhus',
                        'KommuneNummer': '751',
                        'KommuneNavn': 'Aarhus',
                        'Land': NULL,
                        'TelefonNummer': NULL,
                        'MobilNummer': NULL,
                        'Email': NULL,
                        'Adressebeskyttelse': NULL,
                        'Reklamebeskyttelse': NULL
                    }
                }
            }] AS Response
        """)

        result = create_herd_users_table(con, "bes_details", silver_dir)

        assert result is not None
        assert (silver_dir / "herd_users.parquet").exists()

        row = con.execute("SELECT herd_number, user_cvr, user_name FROM herd_users").fetchone()
        assert row is not None
        assert row[0] == 123456
        assert row[1] == "87654321"
        assert row[2] == "User Farm ApS"

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
                        'CprNummer': NULL,
                        'Navn': 'Owner Farm',
                        'Adresse': 'Vej 1',
                        'PostNummer': '8000',
                        'PostDistrikt': NULL,
                        'ByNavn': NULL,
                        'KommuneNummer': NULL,
                        'KommuneNavn': NULL,
                        'Land': NULL,
                        'TelefonNummer': NULL,
                        'MobilNummer': NULL,
                        'Email': NULL,
                        'Adressebeskyttelse': NULL,
                        'Reklamebeskyttelse': NULL
                    },
                    'Bruger': {
                        'CvrNummer': '87654321',
                        'CprNummer': NULL,
                        'Navn': 'User Farm',
                        'Adresse': 'Vej 2',
                        'PostNummer': '8200',
                        'PostDistrikt': NULL,
                        'ByNavn': NULL,
                        'KommuneNummer': NULL,
                        'KommuneNavn': NULL,
                        'Land': NULL,
                        'TelefonNummer': NULL,
                        'MobilNummer': NULL,
                        'Email': NULL,
                        'Adressebeskyttelse': NULL,
                        'Reklamebeskyttelse': NULL
                    }
                }
            }] AS Response
        """)

        create_herd_owners_table(con, "bes_details", silver_dir)
        create_herd_users_table(con, "bes_details", silver_dir)

        owner_cvr = con.execute("SELECT owner_cvr FROM herd_owners").fetchone()[0]
        user_cvr = con.execute("SELECT user_cvr FROM herd_users").fetchone()[0]

        assert owner_cvr != user_cvr
        assert owner_cvr == "12345678"
        assert user_cvr == "87654321"


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

        rows = con.execute("SELECT category, count FROM herd_sizes ORDER BY category").fetchall()
        assert len(rows) == 2
        assert rows[0][0] == "Kvier"
        assert rows[0][1] == 25
        assert rows[1][0] == "Køer"
        assert rows[1][1] == 50

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

        result = con.execute(
            "SELECT COUNT(*) AS num_rows, SUM(count) AS total FROM herd_sizes"
        ).fetchone()
        assert result[1] == 180
        assert result[0] == 3

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
            count = con.execute("SELECT COUNT(*) FROM herd_sizes").fetchone()[0]
            assert count == 0

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
                    ],
                    'BesStrDatoAjourfoert': '2024-01-01'
                }
            }] AS Response
        """)

        create_herd_sizes_table(con, "bes_details", silver_dir)

        # Each record should have a unique UUID
        unique_count = con.execute("SELECT COUNT(DISTINCT size_id) FROM herd_sizes").fetchone()[0]
        assert unique_count == 2
