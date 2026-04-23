"""Tests for CHR production sites source loading."""

import duckdb
import pytest

try:
    from gold.production_sites import _load_silver_tables

    PRODUCTION_SITES_MODULE_AVAILABLE = True
except (ImportError, TypeError):
    PRODUCTION_SITES_MODULE_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not PRODUCTION_SITES_MODULE_AVAILABLE,
    reason="Production sites module not available",
)


class _StubStorageAccess:
    def __init__(self) -> None:
        self.duckdb_conn = duckdb.connect(":memory:")
        self.patterns: list[str] = []

    def list_files(self, pattern: str) -> list[str]:
        self.patterns.append(pattern)
        return []

    def query_parquet_native(self, latest_file: str, query: str, table_name: str) -> None:
        raise AssertionError("query_parquet_native should not run when no files are found")


def test_load_silver_tables_uses_single_glob_pattern_per_source() -> None:
    storage = _StubStorageAccess()

    try:
        assert _load_silver_tables(storage) is True
        assert storage.patterns == [
            "landbruget-data/silver/chr/*/properties*.parquet",
            "landbruget-data/silver/chr/*/property_owners*.parquet",
            "landbruget-data/silver/chr/*/herds*.parquet",
            "landbruget-data/silver/chr/*/herd_sizes*.parquet",
        ]
    finally:
        storage.duckdb_conn.close()
