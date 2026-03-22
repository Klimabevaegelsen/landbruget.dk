"""Silver transform tests for BMD scraper pipeline.

Tests the DuckDB SQL transformations: column cleaning, data cleaning,
date parsing, PFAS detection, and deduplication.
"""

from typing import ClassVar

import duckdb


def _clean_column_name(col):
    """Replicate BMDTransformer.clean_column_names logic."""
    clean = col.lower().strip().replace(" ", "_")
    clean = "".join(c if c.isalnum() or c == "_" else "_" for c in clean)
    while "__" in clean:
        clean = clean.replace("__", "_")
    return clean.strip("_")


class TestColumnCleaning:
    """Test the column name cleaning transform."""

    def test_all_columns_cleaned(self, bmd_raw_connection):
        conn = bmd_raw_connection
        result = conn.execute("SELECT * FROM bmd_raw LIMIT 0").description
        raw_columns = [desc[0] for desc in result]

        column_selects = [f'"{orig}" AS {_clean_column_name(orig)}' for orig in raw_columns]
        conn.execute(f"""
            CREATE TABLE cleaned AS
            SELECT {", ".join(column_selects)} FROM bmd_raw
        """)

        cleaned_result = conn.execute("SELECT * FROM cleaned LIMIT 0").description
        for desc in cleaned_result:
            name = desc[0]
            # All lowercase
            assert name == name.lower()
            # No spaces
            assert " " not in name
            # No leading/trailing underscores
            assert not name.startswith("_")
            assert not name.endswith("_")

    def test_no_duplicate_cleaned_names(self, bmd_raw_connection):
        conn = bmd_raw_connection
        result = conn.execute("SELECT * FROM bmd_raw LIMIT 0").description
        raw_columns = [desc[0] for desc in result]
        cleaned = [_clean_column_name(c) for c in raw_columns]
        assert len(cleaned) == len(set(cleaned))

    def test_row_count_preserved(self, bmd_raw_connection):
        conn = bmd_raw_connection
        result = conn.execute("SELECT * FROM bmd_raw LIMIT 0").description
        raw_columns = [desc[0] for desc in result]

        column_selects = [f'"{orig}" AS {_clean_column_name(orig)}' for orig in raw_columns]
        conn.execute(f"""
            CREATE TABLE cleaned_count AS
            SELECT {", ".join(column_selects)} FROM bmd_raw
        """)
        raw = conn.execute("SELECT COUNT(*) FROM bmd_raw").fetchone()[0]
        clean = conn.execute("SELECT COUNT(*) FROM cleaned_count").fetchone()[0]
        assert raw == clean


class TestDataCleaning:
    """Test trimming and deduplication."""

    def test_trim_removes_whitespace(self):
        conn = duckdb.connect(":memory:")
        conn.execute("""
            CREATE TABLE test AS
            SELECT '  trimmed  ' AS name, 'normal' AS other
        """)
        conn.execute("""
            CREATE TABLE trimmed AS
            SELECT TRIM(name) AS name, other FROM test
        """)
        row = conn.execute("SELECT name FROM trimmed").fetchone()
        conn.close()
        assert row[0] == "trimmed"

    def test_deduplication_removes_exact_duplicates(self):
        conn = duckdb.connect(":memory:")
        conn.execute("""
            CREATE TABLE test AS
            SELECT 'Product A' AS name, 'REG-001' AS reg
            UNION ALL SELECT 'Product A', 'REG-001'
            UNION ALL SELECT 'Product B', 'REG-002'
        """)
        conn.execute("CREATE TABLE deduped AS SELECT DISTINCT * FROM test")
        count = conn.execute("SELECT COUNT(*) FROM deduped").fetchone()[0]
        conn.close()
        assert count == 2

    def test_deduplication_preserves_unique_rows(self, bmd_raw_connection):
        conn = bmd_raw_connection
        conn.execute("CREATE TABLE deduped AS SELECT DISTINCT * FROM bmd_raw")
        raw = conn.execute("SELECT COUNT(*) FROM bmd_raw").fetchone()[0]
        deduped = conn.execute("SELECT COUNT(*) FROM deduped").fetchone()[0]
        assert deduped == raw


class TestDateParsing:
    """Test date column parsing with TRY_CAST."""

    def test_valid_date_parsed(self):
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE test AS SELECT '2020-01-15' AS dato")
        conn.execute("CREATE TABLE parsed AS SELECT TRY_CAST(dato AS DATE) AS dato FROM test")
        row = conn.execute("SELECT dato FROM parsed").fetchone()
        conn.close()
        assert row[0] is not None
        assert str(row[0]) == "2020-01-15"

    def test_invalid_date_returns_null(self):
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE test AS SELECT 'not-a-date' AS dato")
        conn.execute("CREATE TABLE parsed AS SELECT TRY_CAST(dato AS DATE) AS dato FROM test")
        row = conn.execute("SELECT dato FROM parsed").fetchone()
        conn.close()
        assert row[0] is None

    def test_null_date_stays_null(self):
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE test AS SELECT NULL::VARCHAR AS dato")
        conn.execute("CREATE TABLE parsed AS SELECT TRY_CAST(dato AS DATE) AS dato FROM test")
        row = conn.execute("SELECT dato FROM parsed").fetchone()
        conn.close()
        assert row[0] is None


class TestPFASDetection:
    """Test the PFAS indicator column logic."""

    PFAS_INGREDIENTS: ClassVar[set[str]] = {
        "diflufenican",
        "flonicamid",
        "fluazinam",
        "fludioxonil",
        "fluopyram",
        "gamma-cyhalothrin",
        "lambda-cyhalothrin",
        "mefentrifluconazol",
        "oxathiapiprolin",
        "picolinafen",
        "pyroxsulam",
        "tau-fluvalinat",
        "tefluthrin",
        "triflusulfuron-methyl",
    }

    def test_pfas_detected_for_known_ingredient(self):
        conn = duckdb.connect(":memory:")
        conn.execute("""
            CREATE TABLE test AS
            SELECT 'Diflufenican' AS aktivstofnavn
        """)
        pfas_conditions = " OR ".join(
            f"LOWER(aktivstofnavn) LIKE '%{i}%'" for i in self.PFAS_INGREDIENTS
        )
        conn.execute(f"""
            CREATE TABLE result AS
            SELECT *,
                CASE WHEN {pfas_conditions} THEN true ELSE false END AS contains_pfas
            FROM test
        """)
        row = conn.execute("SELECT contains_pfas FROM result").fetchone()
        conn.close()
        assert row[0] is True

    def test_pfas_not_detected_for_safe_ingredient(self):
        conn = duckdb.connect(":memory:")
        conn.execute("""
            CREATE TABLE test AS SELECT 'Glyphosate' AS aktivstofnavn
        """)
        pfas_conditions = " OR ".join(
            f"LOWER(aktivstofnavn) LIKE '%{i}%'" for i in self.PFAS_INGREDIENTS
        )
        conn.execute(f"""
            CREATE TABLE result AS
            SELECT *,
                CASE WHEN {pfas_conditions} THEN true ELSE false END AS contains_pfas
            FROM test
        """)
        row = conn.execute("SELECT contains_pfas FROM result").fetchone()
        conn.close()
        assert row[0] is False

    def test_null_ingredient_returns_null(self):
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE test AS SELECT NULL::VARCHAR AS aktivstofnavn")
        conn.execute("""
            CREATE TABLE result AS
            SELECT *,
                CASE
                    WHEN aktivstofnavn IS NULL OR aktivstofnavn = '' THEN NULL
                    WHEN LOWER(aktivstofnavn) LIKE '%diflufenican%' THEN true
                    ELSE false
                END AS contains_pfas
            FROM test
        """)
        row = conn.execute("SELECT contains_pfas FROM result").fetchone()
        conn.close()
        assert row[0] is None

    def test_diquat_detection(self):
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE test AS SELECT 'Diquat dibromid' AS aktivstofnavn")
        conn.execute("""
            CREATE TABLE result AS
            SELECT *,
                CASE WHEN LOWER(aktivstofnavn) LIKE '%diquat%' THEN true ELSE false END AS contains_diquat
            FROM test
        """)
        row = conn.execute("SELECT contains_diquat FROM result").fetchone()
        conn.close()
        assert row[0] is True

    def test_glyphosate_detection_danish_spelling(self):
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE test AS SELECT 'Glyphosat' AS aktivstofnavn")
        conn.execute("""
            CREATE TABLE result AS
            SELECT *,
                CASE
                    WHEN LOWER(aktivstofnavn) LIKE '%glyphosat%'
                        OR LOWER(aktivstofnavn) LIKE '%glyphosate%'
                    THEN true ELSE false
                END AS contains_glyphosate
            FROM test
        """)
        row = conn.execute("SELECT contains_glyphosate FROM result").fetchone()
        conn.close()
        assert row[0] is True


class TestSemicolonListConversion:
    """Test semicolon-separated value to array conversion."""

    def test_semicolon_split(self):
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE test AS SELECT 'A;B;C' AS val")
        conn.execute("""
            CREATE TABLE result AS
            SELECT string_split(val, ';') AS val_array FROM test
        """)
        row = conn.execute("SELECT val_array FROM result").fetchone()
        conn.close()
        assert row[0] == ["A", "B", "C"]

    def test_null_value_stays_null(self):
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE test AS SELECT NULL::VARCHAR AS val")
        conn.execute("""
            CREATE TABLE result AS
            SELECT CASE WHEN val IS NULL OR val = '' THEN NULL
                        ELSE string_split(val, ';')
                   END AS val_array
            FROM test
        """)
        row = conn.execute("SELECT val_array FROM result").fetchone()
        conn.close()
        assert row[0] is None

    def test_no_semicolon_returns_single_element(self):
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE test AS SELECT 'SingleValue' AS val")
        conn.execute("""
            CREATE TABLE result AS
            SELECT string_split(val, ';') AS val_array FROM test
        """)
        row = conn.execute("SELECT val_array FROM result").fetchone()
        conn.close()
        assert row[0] == ["SingleValue"]
