import duckdb

from exporters.base import BaseExporter


class _StubExporter(BaseExporter):
    def export(self) -> dict:
        return {}


def test_latest_r2_parquet_prefers_latest_timestamped_file() -> None:
    exporter = _StubExporter(conn=duckdb.connect(":memory:"), output_dir="/tmp")

    class _StubFS:
        def exists(self, path: str) -> bool:
            return False

        def glob(self, pattern: str) -> list[str]:
            assert pattern == "landbruget-data/gold/cvr_enrichment_companies/*/data.parquet"
            return [
                "landbruget-data/gold/cvr_enrichment_companies/20260216_045434/data.parquet",
                "landbruget-data/gold/cvr_enrichment_companies/20260420_044807/data.parquet",
            ]

    exporter._r2_fs = _StubFS()

    assert (
        exporter.latest_r2_parquet("gold/cvr_enrichment_companies")
        == "r2://landbruget-data/gold/cvr_enrichment_companies/20260420_044807/data.parquet"
    )


def test_latest_r2_parquet_can_fall_back_to_unversioned_path() -> None:
    exporter = _StubExporter(conn=duckdb.connect(":memory:"), output_dir="/tmp")

    class _StubFS:
        def exists(self, path: str) -> bool:
            return path == "landbruget-data/gold/example/data.parquet"

        def glob(self, pattern: str) -> list[str]:
            return []

    exporter._r2_fs = _StubFS()

    assert (
        exporter.latest_r2_parquet("gold/example", include_unversioned=True)
        == "r2://landbruget-data/gold/example/data.parquet"
    )


def test_latest_r2_match_returns_latest_object() -> None:
    exporter = _StubExporter(conn=duckdb.connect(":memory:"), output_dir="/tmp")

    class _StubFS:
        def glob(self, pattern: str) -> list[str]:
            assert pattern == "landbruget-data/silver/chr/*/herd_sizes*.parquet"
            return [
                "landbruget-data/silver/chr/20260216_045434/herd_sizes.parquet",
                "landbruget-data/silver/chr/20260420_044807/herd_sizes.parquet",
            ]

    exporter._r2_fs = _StubFS()

    assert (
        exporter.latest_r2_match("silver/chr/*/herd_sizes*.parquet")
        == "r2://landbruget-data/silver/chr/20260420_044807/herd_sizes.parquet"
    )


def test_count_parquet_rows_uses_footer_metadata(tmp_path) -> None:
    conn = duckdb.connect(":memory:")
    exporter = _StubExporter(conn=conn, output_dir="/tmp")

    first = tmp_path / "first.parquet"
    second = tmp_path / "second.parquet"

    conn.execute("CREATE TABLE first_table AS SELECT * FROM range(3)")
    conn.execute(f"COPY first_table TO '{first}' (FORMAT PARQUET)")
    conn.execute("CREATE TABLE second_table AS SELECT * FROM range(5)")
    conn.execute(f"COPY second_table TO '{second}' (FORMAT PARQUET)")

    assert exporter.count_parquet_rows([str(first), str(second)]) == 8
