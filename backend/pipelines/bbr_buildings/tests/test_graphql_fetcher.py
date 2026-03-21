"""Tests for BulkGeoDanmarkGraphQLFetcher intermediate file handling."""

import duckdb
import pytest

from bronze.bulk_geodanmark_graphql_fetcher import BulkGeoDanmarkGraphQLFetcher


@pytest.fixture
def fetcher(tmp_path, monkeypatch):
    """Create a fetcher with a temp output directory and in-memory DuckDB."""
    monkeypatch.setattr(BulkGeoDanmarkGraphQLFetcher, "__init__", lambda self, api_key: None)
    f = BulkGeoDanmarkGraphQLFetcher.__new__(BulkGeoDanmarkGraphQLFetcher)
    f.api_key = "test"
    f.endpoint = "https://example.com"
    f.output_dir = tmp_path
    f.conn = duckdb.connect()
    f.conn.execute("INSTALL spatial")
    f.conn.execute("LOAD spatial")
    return f


def _create_test_table(fetcher, table_name, rows=5):
    """Create a simple DuckDB table with test data."""
    fetcher.conn.execute(f"""
        CREATE TABLE {table_name} AS
        SELECT i AS id_lokalId, 'type_' || i AS bygningstype
        FROM range({rows}) t(i)
    """)


def _create_valid_parquet(fetcher, filename, rows=10):
    """Write a valid parquet file to the fetcher's output directory."""
    path = fetcher.output_dir / filename
    fetcher.conn.execute(f"""
        COPY (
            SELECT i AS id_lokalId, 'type_' || i AS bygningstype
            FROM range({rows}) t(i)
        ) TO '{path}' (FORMAT PARQUET)
    """)
    return path


def _create_corrupt_parquet(fetcher, filename):
    """Write a tiny corrupt file to the fetcher's output directory."""
    path = fetcher.output_dir / filename
    path.write_bytes(b"not a parquet file")
    return path


class TestCombineIntermediateFiles:
    def test_skips_corrupted_files(self, fetcher):
        _create_valid_parquet(fetcher, "geodanmark_buildings_batch_0000.geoparquet", rows=5)
        _create_corrupt_parquet(fetcher, "geodanmark_buildings_batch_0001.geoparquet")
        _create_valid_parquet(fetcher, "geodanmark_buildings_batch_0002.geoparquet", rows=3)

        fetcher._combine_intermediate_files()

        result = fetcher.conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{fetcher.output_dir}/geodanmark_buildings_complete.geoparquet')"
        ).fetchone()[0]
        assert result == 8  # 5 + 3, skipping the corrupt file

    def test_fails_if_all_files_corrupted(self, fetcher):
        _create_corrupt_parquet(fetcher, "geodanmark_buildings_batch_0000.geoparquet")
        _create_corrupt_parquet(fetcher, "geodanmark_buildings_batch_0001.geoparquet")

        with pytest.raises(RuntimeError, match="All intermediate files are corrupted"):
            fetcher._combine_intermediate_files()

    def test_no_intermediate_files(self, fetcher):
        # Should return without error
        fetcher._combine_intermediate_files()

    def test_cleans_up_all_files_including_skipped(self, fetcher):
        _create_valid_parquet(fetcher, "geodanmark_buildings_batch_0000.geoparquet")
        _create_corrupt_parquet(fetcher, "geodanmark_buildings_batch_0001.geoparquet")

        fetcher._combine_intermediate_files()

        remaining = list(fetcher.output_dir.glob("geodanmark_buildings_batch_*.geoparquet"))
        assert remaining == []


class TestSaveIntermediateResults:
    def test_saves_and_drops_tables(self, fetcher):
        _create_test_table(fetcher, "page_0", rows=3)
        _create_test_table(fetcher, "page_1", rows=2)

        fetcher._save_intermediate_results(["page_0", "page_1"], batch_num=0)

        # File should exist with correct row count
        output = fetcher.output_dir / "geodanmark_buildings_batch_0000.geoparquet"
        assert output.exists()
        count = fetcher.conn.execute(f"SELECT COUNT(*) FROM read_parquet('{output}')").fetchone()[0]
        assert count == 5

        # Tables should be dropped
        tables = fetcher.conn.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in tables]
        assert "page_0" not in table_names
        assert "page_1" not in table_names

    def test_always_frees_memory_on_failure(self, fetcher):
        _create_test_table(fetcher, "page_0", rows=3)

        # Make the output directory read-only so COPY fails
        original_conn = fetcher.conn

        class FailingConn:
            """Wrapper that fails on COPY but delegates everything else."""

            def execute(self, sql, *args, **kwargs):
                if "COPY" in sql:
                    raise RuntimeError("Simulated disk error")
                return original_conn.execute(sql, *args, **kwargs)

        fetcher.conn = FailingConn()

        with pytest.raises(RuntimeError, match="Failed to save batch"):
            fetcher._save_intermediate_results(["page_0"], batch_num=0)

        # Table should still be dropped despite save failure
        fetcher.conn = original_conn
        tables = fetcher.conn.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in tables]
        assert "page_0" not in table_names

    def test_cleans_up_corrupt_file_on_failure(self, fetcher):
        _create_test_table(fetcher, "page_0", rows=3)
        output = fetcher.output_dir / "geodanmark_buildings_batch_0000.geoparquet"

        original_conn = fetcher.conn

        class FailingConn:
            """Wrapper that fails on COPY and writes a corrupt partial file."""

            def execute(self, sql, *args, **kwargs):
                if "COPY" in sql:
                    output.write_bytes(b"partial")
                    raise RuntimeError("Simulated disk error")
                return original_conn.execute(sql, *args, **kwargs)

        fetcher.conn = FailingConn()

        with pytest.raises(RuntimeError):
            fetcher._save_intermediate_results(["page_0"], batch_num=0)

        # Corrupted file should be cleaned up
        assert not output.exists()

        # Restore real conn so __del__ doesn't error
        fetcher.conn = original_conn


class TestSchemaConsistency:
    """Reproduce schema mismatch bugs that crash the pipeline after hours of fetching."""

    def test_combine_with_varchar_vs_json_column(self, fetcher):
        """BBRaktion as VARCHAR in one file and JSON in another should not crash."""
        # Batch 0: BBRaktion is a plain string → VARCHAR
        fetcher.conn.execute(f"""
            COPY (
                SELECT 1 AS id_lokalId, 'Mangler afklaring' AS BBRaktion
            ) TO '{fetcher.output_dir}/geodanmark_buildings_batch_0000.geoparquet' (FORMAT PARQUET)
        """)
        # Batch 1: BBRaktion is JSON
        fetcher.conn.execute(f"""
            COPY (
                SELECT 2 AS id_lokalId, '{{"aktion": "value"}}'::JSON AS BBRaktion
            ) TO '{fetcher.output_dir}/geodanmark_buildings_batch_0001.geoparquet' (FORMAT PARQUET)
        """)

        fetcher._combine_intermediate_files()

        result = fetcher.conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{fetcher.output_dir}/geodanmark_buildings_complete.geoparquet')"
        ).fetchone()[0]
        assert result == 2

    def test_combine_with_timestamp_vs_varchar_column(self, fetcher):
        """registreringTil as TIMESTAMP in one file and VARCHAR in another should not crash."""
        # Batch 0: registreringTil is TIMESTAMP
        fetcher.conn.execute(f"""
            COPY (
                SELECT 1 AS id_lokalId, TIMESTAMP '2026-03-20 13:15:15' AS registreringTil
            ) TO '{fetcher.output_dir}/geodanmark_buildings_batch_0000.geoparquet' (FORMAT PARQUET)
        """)
        # Batch 1: registreringTil is VARCHAR
        fetcher.conn.execute(f"""
            COPY (
                SELECT 2 AS id_lokalId, '2026-03-20T13:15:15.195000Z' AS registreringTil
            ) TO '{fetcher.output_dir}/geodanmark_buildings_batch_0001.geoparquet' (FORMAT PARQUET)
        """)

        fetcher._combine_intermediate_files()

        result = fetcher.conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{fetcher.output_dir}/geodanmark_buildings_complete.geoparquet')"
        ).fetchone()[0]
        assert result == 2

    def test_nodes_to_duckdb_forces_varchar_types(self, fetcher):
        """All columns from _nodes_to_duckdb_table should be VARCHAR (not auto-inferred)."""
        nodes = [
            {
                "id_lokalId": "abc-123",
                "BBRUUID": None,
                "bygningstype": "Bygning",
                "status": None,
                "geometristatus": None,
                "BBRaktion": "Mangler afklaring",
                "maalestedBygning": None,
                "metode3D": None,
                "overlapBygning": None,
                "synligBygning": None,
                "underMinimumBygning": None,
                "dataansvar": None,
                "registreringFra": "2026-03-20T13:15:15.195000Z",
                "registreringTil": None,
                "virkningFra": "2026-03-20T13:15:15.195000Z",
                "geometri": {
                    "wkt": "POLYGON Z((550000 6200000 0, 550010 6200000 0, 550010 6200010 0, 550000 6200010 0, 550000 6200000 0))"
                },
            }
        ]

        fetcher._nodes_to_duckdb_table(nodes, "test_page")

        # Check column types — all should be VARCHAR except geometri (GEOMETRY)
        columns = fetcher.conn.execute(
            "SELECT column_name, column_type FROM (DESCRIBE test_page)"
        ).fetchall()
        col_types = dict(columns)

        for col in [
            "id_lokalId",
            "BBRUUID",
            "BBRaktion",
            "registreringFra",
            "registreringTil",
            "virkningFra",
        ]:
            assert col_types[col] == "VARCHAR", f"{col} should be VARCHAR, got {col_types[col]}"
