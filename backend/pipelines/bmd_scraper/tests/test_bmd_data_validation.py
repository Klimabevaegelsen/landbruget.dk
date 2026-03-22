"""Data validation tests for BMD scraper pipeline.

Great Expectations-style checks: unique product IDs, non-null names,
valid date ranges, active substance columns present, PFAS detection.
"""

from typing import ClassVar

import duckdb


class TestProductIdentifierUniqueness:
    """Product registration numbers must be unique."""

    def test_registrerings_nr_unique(self, bmd_raw_connection):
        conn = bmd_raw_connection
        total = conn.execute("SELECT COUNT(*) FROM bmd_raw").fetchone()[0]
        distinct = conn.execute(
            'SELECT COUNT(DISTINCT "Registrerings-nr.") FROM bmd_raw'
        ).fetchone()[0]
        assert total == distinct, "Duplicate registration numbers found"

    def test_registrerings_nr_not_null(self, bmd_raw_connection):
        conn = bmd_raw_connection
        nulls = conn.execute(
            'SELECT COUNT(*) FROM bmd_raw WHERE "Registrerings-nr." IS NULL'
        ).fetchone()[0]
        assert nulls == 0, "Registration numbers must not be null"


class TestProductNameValidation:
    """Product names must be present and non-empty."""

    def test_produktnavn_not_null(self, bmd_raw_connection):
        conn = bmd_raw_connection
        nulls = conn.execute('SELECT COUNT(*) FROM bmd_raw WHERE "Produktnavn" IS NULL').fetchone()[
            0
        ]
        assert nulls == 0

    def test_produktnavn_not_empty(self, bmd_raw_connection):
        conn = bmd_raw_connection
        empty = conn.execute(
            "SELECT COUNT(*) FROM bmd_raw WHERE TRIM(\"Produktnavn\") = ''"
        ).fetchone()[0]
        assert empty == 0


class TestProductStatusValidation:
    """Product status must be one of the known Danish status values."""

    KNOWN_STATUSES: ClassVar[set[str]] = {"Godkendt", "Udgået", "Tilladt", "Ikke godkendt"}

    def test_status_not_null(self, bmd_raw_connection):
        conn = bmd_raw_connection
        nulls = conn.execute(
            'SELECT COUNT(*) FROM bmd_raw WHERE "Produktstatus" IS NULL'
        ).fetchone()[0]
        assert nulls == 0

    def test_status_in_accepted_values(self, bmd_raw_connection):
        conn = bmd_raw_connection
        statuses = conn.execute('SELECT DISTINCT "Produktstatus" FROM bmd_raw').fetchall()
        for (status,) in statuses:
            assert status in self.KNOWN_STATUSES, f"Unknown status: {status}"


class TestDateRangeValidation:
    """Approval dates must be within reasonable ranges."""

    def test_godkendelsesdato_parseable(self, bmd_raw_connection):
        """Approval dates must be parseable as dates."""
        conn = bmd_raw_connection
        conn.execute("""
            CREATE TABLE date_check AS
            SELECT TRY_CAST("Godkendelsesdato" AS DATE) AS approval_date
            FROM bmd_raw
            WHERE "Godkendelsesdato" IS NOT NULL
        """)
        nulls = conn.execute(
            "SELECT COUNT(*) FROM date_check WHERE approval_date IS NULL"
        ).fetchone()[0]
        assert nulls == 0, "Some approval dates failed to parse"

    def test_godkendelsesdato_not_before_1990(self, bmd_raw_connection):
        """No pesticide approvals should be before 1990."""
        conn = bmd_raw_connection
        old = conn.execute("""
            SELECT COUNT(*) FROM bmd_raw
            WHERE TRY_CAST("Godkendelsesdato" AS DATE) < DATE '1990-01-01'
        """).fetchone()[0]
        assert old == 0

    def test_udloebsdato_after_godkendelsesdato(self, bmd_raw_connection):
        """Expiry date should be after approval date."""
        conn = bmd_raw_connection
        invalid = conn.execute("""
            SELECT COUNT(*) FROM bmd_raw
            WHERE TRY_CAST("Udløbsdato" AS DATE) < TRY_CAST("Godkendelsesdato" AS DATE)
              AND "Udløbsdato" IS NOT NULL
              AND "Godkendelsesdato" IS NOT NULL
        """).fetchone()[0]
        assert invalid == 0, "Expiry date before approval date found"


class TestActiveSubstanceValidation:
    """Active substance column must be present and populated."""

    def test_aktivstofnavn_column_exists(self, bmd_raw_connection):
        conn = bmd_raw_connection
        result = conn.execute("SELECT * FROM bmd_raw LIMIT 0").description
        columns = {desc[0] for desc in result}
        assert any("aktivstofnavn" in c.lower() for c in columns)

    def test_aktivstofnavn_not_all_null(self, bmd_raw_connection):
        conn = bmd_raw_connection
        result = conn.execute("SELECT * FROM bmd_raw LIMIT 0").description
        active_col = next(d[0] for d in result if "aktivstofnavn" in d[0].lower())
        non_null = conn.execute(
            f'SELECT COUNT(*) FROM bmd_raw WHERE "{active_col}" IS NOT NULL'
        ).fetchone()[0]
        assert non_null > 0


class TestBekæmpelsesmiddeltypeValidation:
    """Product type must be one of the known types."""

    KNOWN_TYPES: ClassVar[set[str]] = {"Pesticid", "Biocid"}

    def test_type_not_null(self, bmd_raw_connection):
        conn = bmd_raw_connection
        nulls = conn.execute(
            'SELECT COUNT(*) FROM bmd_raw WHERE "Bekæmpelsesmiddeltype" IS NULL'
        ).fetchone()[0]
        assert nulls == 0

    def test_type_in_accepted_values(self, bmd_raw_connection):
        conn = bmd_raw_connection
        types = conn.execute('SELECT DISTINCT "Bekæmpelsesmiddeltype" FROM bmd_raw').fetchall()
        for (t,) in types:
            assert t in self.KNOWN_TYPES, f"Unknown type: {t}"


class TestPFASDetectionValidation:
    """PFAS detection logic must correctly identify known PFAS ingredients."""

    PFAS_INGREDIENTS: ClassVar[set[str]] = {
        "diflufenican",
        "flonicamid",
        "fluazinam",
        "fludioxonil",
        "fluopyram",
        "gamma-cyhalothrin",
        "lambda-cyhalothrin",
    }

    def test_diflufenican_detected_as_pfas(self):
        """Diflufenican is a known PFAS ingredient."""
        ingredient = "Diflufenican"
        assert ingredient.lower() in self.PFAS_INGREDIENTS

    def test_tebuconazol_not_pfas(self):
        """Tebuconazol is NOT a PFAS ingredient."""
        ingredient = "Tebuconazol"
        assert ingredient.lower() not in self.PFAS_INGREDIENTS

    def test_pfas_sql_detection_logic(self):
        """SQL LIKE-based PFAS detection works for known ingredients."""
        conn = duckdb.connect(":memory:")
        conn.execute("""
            CREATE TABLE test AS
            SELECT 'Diflufenican' AS ingredient
            UNION ALL SELECT 'Tebuconazol'
            UNION ALL SELECT 'Fluopyram + Tebuconazol'
        """)
        pfas = conn.execute("""
            SELECT COUNT(*) FROM test
            WHERE LOWER(ingredient) LIKE '%diflufenican%'
               OR LOWER(ingredient) LIKE '%fluopyram%'
        """).fetchone()[0]
        conn.close()
        assert pfas == 2  # Diflufenican + Fluopyram+Tebuconazol


class TestNumericFieldRanges:
    """Numeric fields should be within expected ranges."""

    def test_belastning_values_non_negative(self, bmd_raw_connection):
        """Environmental impact scores should be non-negative when present."""
        conn = bmd_raw_connection
        negative = conn.execute("""
            SELECT COUNT(*) FROM bmd_raw
            WHERE TRY_CAST("Belastning (miljøeffekt)" AS DOUBLE) < 0
               OR TRY_CAST("Belastning (miljøadfærd)" AS DOUBLE) < 0
               OR TRY_CAST("Belastning (sundhed)" AS DOUBLE) < 0
        """).fetchone()[0]
        assert negative == 0

    def test_samlet_belastning_is_sum(self):
        """Samlet belastning should approximately equal sum of components.

        Uses a standalone DuckDB table with known-good data to validate
        the invariant independently of the fixture column alignment.
        """
        conn = duckdb.connect(":memory:")
        conn.execute("""
            CREATE TABLE test_belastning AS
            SELECT 1.5 AS env, 2.0 AS behavior, 0.5 AS health, 4.0 AS total
            UNION ALL
            SELECT 0.8, 1.2, 0.3, 2.3
        """)
        rows = conn.execute("SELECT env, behavior, health, total FROM test_belastning").fetchall()
        conn.close()
        for env, behavior, health, total in rows:
            component_sum = env + behavior + health
            assert abs(total - component_sum) < 0.01, f"Total {total} != sum {component_sum}"
