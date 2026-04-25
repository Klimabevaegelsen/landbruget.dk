"""Silver transform tests for dyrenesdetektiv_scraper."""

import json

import duckdb

from silver.transform import run_silver


def test_run_silver_handles_empty_details_directory(tmp_path):
    bronze_dir = tmp_path / "bronze"
    details_dir = bronze_dir / "details"
    silver_dir = tmp_path / "silver"
    details_dir.mkdir(parents=True)
    silver_dir.mkdir(parents=True)

    (bronze_dir / "index.json").write_text("[]", encoding="utf-8")
    (bronze_dir / "kontrol_tag.json").write_text("[]", encoding="utf-8")

    summary = run_silver(bronze_dir, silver_dir)

    assert summary["record_count"] == 0
    assert summary["valid_chr_count"] == 0
    assert summary["valid_cvr_count"] == 0

    parquet_path = silver_dir / "dyrenesdetektiv_kontrol.parquet"
    assert parquet_path.exists()

    count = duckdb.sql(f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')").fetchone()[0]
    assert count == 0

    metadata = json.loads((silver_dir / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["record_count"] == 0
