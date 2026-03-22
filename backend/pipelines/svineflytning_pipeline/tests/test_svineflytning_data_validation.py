"""Data validation tests for svineflytning pipeline.

Great Expectations-style checks: identifier formats, date ranges,
unique keys, required fields not null.
"""

from datetime import date

import duckdb


class TestCHRNumberValidation:
    """CHR numbers must be 6-digit integers."""

    def test_sender_chr_is_positive_integer(self, sample_bronze_chunk):
        movement = sample_bronze_chunk["response"]["Response"]["SvineflytningListe"][
            "Svineflytning"
        ][0]
        chr_number = movement["Afsender"]["ChrNummer"]
        assert isinstance(chr_number, int)
        assert chr_number > 0

    def test_sender_chr_is_6_digits(self, sample_bronze_chunk):
        movement = sample_bronze_chunk["response"]["Response"]["SvineflytningListe"][
            "Svineflytning"
        ][0]
        chr_str = str(movement["Afsender"]["ChrNummer"])
        assert len(chr_str) == 6, f"CHR should be 6 digits, got {len(chr_str)}: {chr_str}"

    def test_receiver_chr_is_6_digits(self, sample_bronze_chunk):
        movement = sample_bronze_chunk["response"]["Response"]["SvineflytningListe"][
            "Svineflytning"
        ][0]
        chr_str = str(movement["Modtager"]["ChrNummer"])
        assert len(chr_str) == 6

    def test_chr_numbers_are_distinct_for_sender_receiver(self, sample_bronze_chunk):
        """Sender and receiver CHR should differ (can't move to self)."""
        movement = sample_bronze_chunk["response"]["Response"]["SvineflytningListe"][
            "Svineflytning"
        ][0]
        assert movement["Afsender"]["ChrNummer"] != movement["Modtager"]["ChrNummer"]


class TestMovementDateValidation:
    """Movement dates must be within reasonable ranges."""

    def test_movement_date_is_valid_date_string(self, sample_bronze_chunk):
        movement = sample_bronze_chunk["response"]["Response"]["SvineflytningListe"][
            "Svineflytning"
        ][0]
        date_str = movement["FlytteTidspunkt"]["SvineflytDato"]
        # Should parse as a date
        parsed = date.fromisoformat(date_str)
        assert parsed.year >= 2000
        assert parsed.year <= 2030

    def test_movement_date_not_in_future(self, sample_bronze_chunk):
        """Movement dates should not be far in the future."""
        movement = sample_bronze_chunk["response"]["Response"]["SvineflytningListe"][
            "Svineflytning"
        ][0]
        date_str = movement["FlytteTidspunkt"]["SvineflytDato"]
        parsed = date.fromisoformat(date_str)
        # Allow a 1-year buffer for test fixtures
        assert parsed.year <= date.today().year + 1

    def test_chunk_date_range_is_valid(self, sample_bronze_chunk):
        """Chunk start_date must be before or equal to end_date."""
        start = date.fromisoformat(sample_bronze_chunk["start_date"])
        end = date.fromisoformat(sample_bronze_chunk["end_date"])
        assert start <= end


class TestAnimalCountValidation:
    """Animal counts must be non-negative and consistent."""

    def test_total_animals_non_negative(self, sample_bronze_chunk):
        movement = sample_bronze_chunk["response"]["Response"]["SvineflytningListe"][
            "Svineflytning"
        ][0]
        total = movement["AntalDyr"]["AntalDyrIAlt"]
        assert total >= 0

    def test_subcounts_do_not_exceed_total(self, sample_bronze_chunk):
        """Sum of sow + slaughter pig should not exceed total."""
        movement = sample_bronze_chunk["response"]["Response"]["SvineflytningListe"][
            "Svineflytning"
        ][0]
        antal = movement["AntalDyr"]
        total = antal["AntalDyrIAlt"]
        sows = antal.get("AntalSoer") or 0
        slaughter = antal.get("AntalSlagtesvin") or 0
        assert sows + slaughter <= total

    def test_total_animals_positive_for_non_deleted(self, sample_bronze_chunk):
        """Non-deleted movements should have at least 1 animal."""
        movement = sample_bronze_chunk["response"]["Response"]["SvineflytningListe"][
            "Svineflytning"
        ][0]
        assert movement["Handling"] != "slet"
        assert movement["AntalDyr"]["AntalDyrIAlt"] > 0


class TestPrimaryKeyUniqueness:
    """Movement IDs must be unique within a chunk."""

    def test_movement_ids_unique_in_chunk(self, sample_bronze_chunk):
        movements = sample_bronze_chunk["response"]["Response"]["SvineflytningListe"][
            "Svineflytning"
        ]
        ids = [m["Id"] for m in movements]
        assert len(ids) == len(set(ids)), f"Duplicate IDs: {ids}"

    def test_movement_ids_are_positive(self, sample_bronze_chunk):
        movements = sample_bronze_chunk["response"]["Response"]["SvineflytningListe"][
            "Svineflytning"
        ]
        for m in movements:
            assert m["Id"] > 0


class TestRequiredFieldsNotNull:
    """Critical fields must not be null for valid (non-deleted) movements."""

    def test_id_not_null(self, sample_bronze_chunk):
        for m in sample_bronze_chunk["response"]["Response"]["SvineflytningListe"]["Svineflytning"]:
            assert m["Id"] is not None

    def test_handling_not_null(self, sample_bronze_chunk):
        for m in sample_bronze_chunk["response"]["Response"]["SvineflytningListe"]["Svineflytning"]:
            assert m["Handling"] is not None

    def test_oprindelse_not_null(self, sample_bronze_chunk):
        for m in sample_bronze_chunk["response"]["Response"]["SvineflytningListe"]["Svineflytning"]:
            assert m["Oprindelse"] is not None

    def test_valid_movement_has_sender(self, sample_bronze_chunk):
        """Non-deleted movements must have a sender."""
        movement = sample_bronze_chunk["response"]["Response"]["SvineflytningListe"][
            "Svineflytning"
        ][0]
        assert movement["Handling"] != "slet"
        assert movement["Afsender"] is not None
        assert movement["Afsender"]["ChrNummer"] is not None


class TestPostalCodeValidation:
    """Danish postal codes are 4-digit numbers between 1000-9999."""

    def test_sender_postal_code_valid(self, sample_bronze_chunk):
        movement = sample_bronze_chunk["response"]["Response"]["SvineflytningListe"][
            "Svineflytning"
        ][0]
        postal = movement["Afsender"]["Ejendom"]["PostNummer"]
        assert 1000 <= postal <= 9999

    def test_receiver_postal_code_valid(self, sample_bronze_chunk):
        movement = sample_bronze_chunk["response"]["Response"]["SvineflytningListe"][
            "Svineflytning"
        ][0]
        postal = movement["Modtager"]["Ejendom"]["PostNummer"]
        assert 1000 <= postal <= 9999


class TestSilverDataQualityFlags:
    """Verify silver's data quality flags are derived correctly."""

    def test_is_deleted_flag_for_slet_handling(self):
        """Records with Handling='slet' should be flagged as deleted."""
        conn = duckdb.connect(":memory:")
        conn.execute("""
            CREATE TABLE test_movements AS
            SELECT 'slet' AS Handling, 100 AS Id, 50 AS AntalDyr_AntalDyrIAlt
            UNION ALL
            SELECT 'ret', 101, 150
        """)
        conn.execute("""
            CREATE TABLE flagged AS
            SELECT *,
                CASE WHEN Handling = 'slet' THEN true ELSE false END AS is_deleted
            FROM test_movements
        """)
        deleted = conn.execute("SELECT COUNT(*) FROM flagged WHERE is_deleted = true").fetchone()[0]
        not_deleted = conn.execute(
            "SELECT COUNT(*) FROM flagged WHERE is_deleted = false"
        ).fetchone()[0]
        conn.close()
        assert deleted == 1
        assert not_deleted == 1

    def test_missing_animal_count_flag(self):
        """Records with null or <=0 animal count should be flagged."""
        conn = duckdb.connect(":memory:")
        conn.execute("""
            CREATE TABLE test AS
            SELECT NULL AS AntalDyr_AntalDyrIAlt
            UNION ALL SELECT 0
            UNION ALL SELECT -1
            UNION ALL SELECT 150
        """)
        conn.execute("""
            CREATE TABLE flagged AS
            SELECT *,
                CASE WHEN AntalDyr_AntalDyrIAlt IS NULL OR AntalDyr_AntalDyrIAlt <= 0
                    THEN true ELSE false
                END AS missing_animal_count
            FROM test
        """)
        flagged = conn.execute(
            "SELECT COUNT(*) FROM flagged WHERE missing_animal_count = true"
        ).fetchone()[0]
        conn.close()
        assert flagged == 3
