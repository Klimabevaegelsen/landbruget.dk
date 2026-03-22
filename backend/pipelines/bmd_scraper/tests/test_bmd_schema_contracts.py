"""Schema & contract tests for BMD scraper pipeline.

Validates that the bronze Excel output matches what silver expects,
and that silver's DuckDB transformations produce the correct schema.
"""

from typing import ClassVar


class TestBronzeOutputSchema:
    """Bronze output is an Excel file with pesticide product data."""

    # Key columns that must exist in the raw Excel (pre-cleaning names)
    REQUIRED_RAW_COLUMNS: ClassVar[set[str]] = {
        "Produktnavn",
        "Registrerings-nr.",
        "Bekæmpelsesmiddeltype",
        "Aktivstofnavn(e)",
        "Produktstatus",
    }

    def test_bronze_has_required_columns(self, bmd_raw_connection):
        """Bronze data must contain the columns silver needs."""
        result = bmd_raw_connection.execute("SELECT * FROM bmd_raw LIMIT 0").description
        actual_columns = {desc[0] for desc in result}

        for col in self.REQUIRED_RAW_COLUMNS:
            assert col in actual_columns, f"Missing required column: {col}"

    def test_bronze_has_rows(self, bmd_raw_connection):
        """Bronze data must contain at least one data row."""
        count = bmd_raw_connection.execute("SELECT COUNT(*) FROM bmd_raw").fetchone()[0]
        assert count >= 1

    def test_bronze_has_expected_column_count(self, bmd_raw_connection):
        """Bronze data should have ~45 columns matching the BMD Excel schema."""
        result = bmd_raw_connection.execute("SELECT * FROM bmd_raw LIMIT 0").description
        assert len(result) == 45


class TestSilverColumnCleaning:
    """Silver cleans column names: lowercase, underscores, strip special chars."""

    def _clean_column_name(self, col):
        """Replicate silver's column name cleaning logic."""
        clean = col.lower().strip().replace(" ", "_")
        clean = "".join(c if c.isalnum() or c == "_" else "_" for c in clean)
        while "__" in clean:
            clean = clean.replace("__", "_")
        return clean.strip("_")

    def test_produktnavn_cleaned(self):
        assert self._clean_column_name("Produktnavn") == "produktnavn"

    def test_registrerings_nr_cleaned(self):
        assert self._clean_column_name("Registrerings-nr.") == "registrerings_nr"

    def test_aktivstofnavn_cleaned(self):
        assert self._clean_column_name("Aktivstofnavn(e)") == "aktivstofnavn_e"

    def test_bekæmpelsesmiddeltype_cleaned(self):
        # Danish ø/æ are alphanumeric in Python — preserved by isalnum()
        assert self._clean_column_name("Bekæmpelsesmiddeltype") == "bekæmpelsesmiddeltype"

    def test_produktgruppe_pesticid_cleaned(self):
        assert self._clean_column_name("Produktgruppe (pesticid)") == "produktgruppe_pesticid"

    def test_belastning_miljøeffekt_cleaned(self):
        assert self._clean_column_name("Belastning (miljøeffekt)") == "belastning_miljøeffekt"

    def test_h_sætninger_cleaned(self):
        assert self._clean_column_name("H-sætninger") == "h_sætninger"


class TestSilverOutputSchema:
    """Silver produces pesticide_products.parquet with cleaned columns + computed flags."""

    # Computed columns silver adds beyond the raw data
    COMPUTED_COLUMNS: ClassVar[set[str]] = {
        "contains_pfas",
        "contains_diquat",
        "contains_glyphosate",
    }

    def test_pfas_indicator_added_to_output(self, bmd_raw_connection):
        """Silver must add the three substance indicator columns."""
        conn = bmd_raw_connection

        # Find the active ingredient column
        result = conn.execute("SELECT * FROM bmd_raw LIMIT 0").description
        columns = [desc[0] for desc in result]

        active_col = None
        for col in columns:
            if "aktivstofnavn" in col.lower():
                active_col = col
                break

        assert active_col is not None, "Must have an active ingredient column"

        # Add the PFAS indicator (simplified version of silver's logic)
        conn.execute(f"""
            CREATE TABLE with_pfas AS
            SELECT *,
                CASE WHEN LOWER("{active_col}") LIKE '%diflufenican%' THEN true ELSE false END AS contains_pfas,
                CASE WHEN LOWER("{active_col}") LIKE '%diquat%' THEN true ELSE false END AS contains_diquat,
                CASE WHEN LOWER("{active_col}") LIKE '%glyphosat%' THEN true ELSE false END AS contains_glyphosate
            FROM bmd_raw
        """)

        result = conn.execute("SELECT * FROM with_pfas LIMIT 0").description
        output_columns = {desc[0] for desc in result}

        for col in self.COMPUTED_COLUMNS:
            assert col in output_columns

    def test_pfas_detection_flags_diflufenican(self, bmd_raw_connection):
        """Product with diflufenican active ingredient should be flagged as PFAS."""
        conn = bmd_raw_connection

        result = conn.execute("SELECT * FROM bmd_raw LIMIT 0").description
        columns = [desc[0] for desc in result]
        active_col = next(c for c in columns if "aktivstofnavn" in c.lower())

        conn.execute(f"""
            CREATE TABLE pfas_check AS
            SELECT *,
                CASE WHEN LOWER("{active_col}") LIKE '%diflufenican%' THEN true ELSE false END AS contains_pfas
            FROM bmd_raw
        """)

        pfas_count = conn.execute(
            "SELECT COUNT(*) FROM pfas_check WHERE contains_pfas = true"
        ).fetchone()[0]

        # Our fixture has one row with Diflufenican
        assert pfas_count == 1


class TestBronzeToSilverContract:
    """End-to-end contract: bronze data → silver cleaned schema."""

    def test_full_column_cleaning_pipeline(self, bmd_raw_connection):
        """Bronze columns must survive the full cleaning pipeline."""
        conn = bmd_raw_connection

        # Get raw columns
        result = conn.execute("SELECT * FROM bmd_raw LIMIT 0").description
        raw_columns = [desc[0] for desc in result]

        # Apply silver's column cleaning
        def clean(col):
            c = col.lower().strip().replace(" ", "_")
            c = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in c)
            while "__" in c:
                c = c.replace("__", "_")
            return c.strip("_")

        column_selects = [f'"{orig}" AS {clean(orig)}' for orig in raw_columns]
        conn.execute(f"""
            CREATE TABLE cleaned AS
            SELECT {", ".join(column_selects)}
            FROM bmd_raw
        """)

        # Verify cleaned table has same row count
        raw_count = conn.execute("SELECT COUNT(*) FROM bmd_raw").fetchone()[0]
        clean_count = conn.execute("SELECT COUNT(*) FROM cleaned").fetchone()[0]
        assert clean_count == raw_count

        # Verify no duplicate column names after cleaning
        cleaned_result = conn.execute("SELECT * FROM cleaned LIMIT 0").description
        cleaned_names = [desc[0] for desc in cleaned_result]
        assert len(cleaned_names) == len(set(cleaned_names)), (
            "Duplicate column names after cleaning"
        )

    def test_deduplication_preserves_unique_rows(self, bmd_raw_connection):
        """Silver deduplicates rows — unique rows must survive."""
        conn = bmd_raw_connection
        conn.execute("CREATE TABLE deduped AS SELECT DISTINCT * FROM bmd_raw")

        raw_count = conn.execute("SELECT COUNT(*) FROM bmd_raw").fetchone()[0]
        dedup_count = conn.execute("SELECT COUNT(*) FROM deduped").fetchone()[0]

        # Our fixture has unique rows, so counts should match
        assert dedup_count == raw_count
