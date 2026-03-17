"""Tests for StoragePath helper in storage_interface."""


def test_storage_path_prefers_r2_bucket(monkeypatch):
    from common.storage_interface import StoragePath

    monkeypatch.setenv("R2_BUCKET", "r2-bucket")
    monkeypatch.setenv("GCS_BUCKET", "gcs-bucket")

    paths = StoragePath()
    assert paths.silver("unified_pipeline", "dataset.parquet") == (
        "gs://r2-bucket/silver/unified_pipeline/dataset.parquet"
    )


def test_storage_path_falls_back_to_gcs_bucket(monkeypatch):
    from common.storage_interface import StoragePath

    monkeypatch.delenv("R2_BUCKET", raising=False)
    monkeypatch.setenv("GCS_BUCKET", "legacy-gcs-bucket")

    paths = StoragePath()
    assert paths.bronze("chr_pipeline", "20260307_120000", "raw.parquet") == (
        "gs://legacy-gcs-bucket/bronze/chr_pipeline/20260307_120000/raw.parquet"
    )


def test_storage_path_uses_default_bucket_when_env_missing(monkeypatch):
    from common.storage_interface import DEFAULT_BUCKET, StoragePath

    monkeypatch.delenv("R2_BUCKET", raising=False)
    monkeypatch.delenv("GCS_BUCKET", raising=False)

    paths = StoragePath()
    assert (
        paths.raw("gold", "example.parquet")
        == f"gs://{DEFAULT_BUCKET}/gold/example.parquet"
    )
