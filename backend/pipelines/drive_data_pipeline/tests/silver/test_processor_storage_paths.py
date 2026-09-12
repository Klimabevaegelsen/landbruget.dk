"""Tests for Silver processor cloud-storage path handling."""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from drive_data_pipeline.silver.processor import SilverProcessor


@pytest.mark.parametrize(
    ("output_path", "expected"),
    [
        (
            "gr_2025/20260912_131417/register.parquet",
            "landbruget-data/silver/gr_2025/20260912_131417/register.parquet",
        ),
        (
            "silver/gr_2025/20260912_131417/register.parquet",
            "landbruget-data/silver/gr_2025/20260912_131417/register.parquet",
        ),
        (
            "landbruget-data/silver/gr_2025/20260912_131417/register.parquet",
            "landbruget-data/silver/gr_2025/20260912_131417/register.parquet",
        ),
        (
            "r2://landbruget-data/silver/gr_2025/20260912_131417/register.parquet",
            "r2://landbruget-data/silver/gr_2025/20260912_131417/register.parquet",
        ),
    ],
)
def test_get_cloud_storage_path(output_path: str, expected: str) -> None:
    """Relative and already-qualified paths resolve to one canonical R2 path."""
    assert SilverProcessor._get_cloud_storage_path(output_path, "landbruget-data") == expected


def test_handle_pii_uses_bucket_qualified_relative_path() -> None:
    """PII validation reads the same bucket/key that the R2 save operation writes."""
    processor = object.__new__(SilverProcessor)
    processor.storage_manager = SimpleNamespace(
        storage_type="r2",
        bucket_name="landbruget-data",
    )
    processor._shared_storage_access = MagicMock()
    processor._shared_storage_access.query_parquet_native.return_value = "pii_validation_table"
    processor.pii_validator = MagicMock()
    processor.pii_validator.validate.return_value.is_valid = True

    output_path = Path("gr_2025/20260912_131417/register.parquet")
    assert processor._handle_pii_in_file(output_path, Path()) is None

    processor._shared_storage_access.query_parquet_native.assert_called_once_with(
        "landbruget-data/silver/gr_2025/20260912_131417/register.parquet",
        "SELECT *",
        "pii_validation_table",
    )
