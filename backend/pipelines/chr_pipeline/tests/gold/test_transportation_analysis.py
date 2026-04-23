"""Regression tests for CHR transportation analysis fallbacks."""

import duckdb
import pytest

try:
    from gold.transportation_analysis import (
        create_comprehensive_certificate_matching,
        load_transportation_data_sources,
    )

    GOLD_TRANSPORT_MODULE_AVAILABLE = True
except (ImportError, TypeError):
    GOLD_TRANSPORT_MODULE_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not GOLD_TRANSPORT_MODULE_AVAILABLE,
    reason="Transportation analysis module not available",
)


class _StubStorageAccess:
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.duckdb_conn = conn

    def list_files(self, pattern: str) -> list[str]:
        return []

    def query_parquet_native(self, latest_file: str, query: str, table_name: str) -> None:
        raise AssertionError("query_parquet_native should not be called when files are absent")


@pytest.fixture
def conn() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect(":memory:")
    yield connection
    connection.close()


def _create_empty_svineflytning(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE OR REPLACE TABLE svineflytning AS
        SELECT
            CAST(NULL AS VARCHAR) AS traces_document,
            CAST(NULL AS VARCHAR) AS sender_chr_number,
            CAST(NULL AS VARCHAR) AS receiver_chr_number,
            CAST(NULL AS DATE) AS movement_date,
            CAST(NULL AS BIGINT) AS total_animals,
            CAST(NULL AS VARCHAR) AS receiver_country_code,
            CAST(NULL AS VARCHAR) AS movement_time,
            CAST(NULL AS BIGINT) AS movement_sequence,
            CAST(NULL AS VARCHAR) AS sender_city_name,
            CAST(NULL AS VARCHAR) AS sender_postal_code,
            CAST(NULL AS VARCHAR) AS sender_postal_district,
            CAST(NULL AS VARCHAR) AS sender_municipality_code,
            CAST(NULL AS VARCHAR) AS sender_municipality_name,
            CAST(NULL AS VARCHAR) AS sender_property_created,
            CAST(NULL AS VARCHAR) AS sender_property_updated,
            CAST(NULL AS BOOLEAN) AS sender_foreign_property,
            CAST(NULL AS VARCHAR) AS receiver_city_name,
            CAST(NULL AS VARCHAR) AS receiver_postal_code,
            CAST(NULL AS VARCHAR) AS receiver_postal_district,
            CAST(NULL AS VARCHAR) AS receiver_municipality_code,
            CAST(NULL AS VARCHAR) AS receiver_municipality_name,
            CAST(NULL AS VARCHAR) AS receiver_property_created,
            CAST(NULL AS VARCHAR) AS receiver_property_updated,
            CAST(NULL AS BOOLEAN) AS receiver_foreign_property,
            CAST(NULL AS BIGINT) AS sow_count,
            CAST(NULL AS BIGINT) AS slaughter_pig_count,
            CAST(NULL AS BIGINT) AS containers_190l,
            CAST(NULL AS BIGINT) AS containers_240l,
            CAST(NULL AS VARCHAR) AS vehicle_country_code,
            CAST(NULL AS VARCHAR) AS vehicle_registration,
            CAST(NULL AS VARCHAR) AS trailer_country_code,
            CAST(NULL AS VARCHAR) AS trailer_registration,
            CAST(NULL AS VARCHAR) AS transshipment_info,
            CAST(NULL AS VARCHAR) AS health_certificate,
            CAST(NULL AS VARCHAR) AS reporter_login,
            CAST(NULL AS TIMESTAMP) AS report_timestamp,
            CAST(NULL AS TIMESTAMP) AS processed_timestamp,
            CAST(NULL AS TIMESTAMP) AS source_chunk_timestamp,
            CAST(NULL AS DATE) AS source_period_start,
            CAST(NULL AS DATE) AS source_period_end,
            CAST(NULL AS BOOLEAN) AS is_deleted,
            CAST(NULL AS BOOLEAN) AS is_invalid,
            CAST(NULL AS BIGINT) AS missing_animal_count
        WHERE FALSE
    """)


def test_missing_optional_transport_tables_get_schema_aware_fallbacks(
    conn: duckdb.DuckDBPyConnection,
) -> None:
    storage_access = _StubStorageAccess(conn)

    loaded_tables = load_transportation_data_sources(storage_access)

    assert loaded_tables["intl_pig_nt"] is False
    assert loaded_tables["intl_pig_cl"] is False
    assert loaded_tables["intl_pig_2024_2025"] is False

    conn.execute("SELECT i_2_imsoc_reference FROM intl_pig_nt").fetchall()
    conn.execute("SELECT i_2_certificate_reference_number FROM intl_pig_cl").fetchall()
    conn.execute("SELECT i_2_imsoc_reference FROM intl_pig_2024_2025").fetchall()


def test_certificate_matching_compiles_when_optional_pig_exports_are_missing(
    conn: duckdb.DuckDBPyConnection,
) -> None:
    storage_access = _StubStorageAccess(conn)

    load_transportation_data_sources(storage_access)
    _create_empty_svineflytning(conn)
    create_comprehensive_certificate_matching(conn)

    total_matches = conn.execute("SELECT COUNT(*) FROM certificate_matched_movements").fetchone()[0]
    assert total_matches == 0
