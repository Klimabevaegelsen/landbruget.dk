"""
Cloud Object Storage Access Module (Cloudflare R2 via s3fs).

This module is THE single source of truth for all cloud storage operations across
the codebase. All pipelines should import from here instead of using s3fs or
google-cloud-storage directly.

Usage:
    from common.storage import StorageAccess

    storage = StorageAccess()

    # Read parquet from R2
    storage.query_parquet_direct("r2://bucket/file.parquet", "SELECT *", "my_table")

    # Write parquet to R2
    storage.upload_from_duckdb_table("my_table", "r2://bucket/output.parquet")

    # Stream JSON
    storage.upload_json(data, "r2://bucket/data.json")
    storage.download_json("r2://bucket/data.json")

    # r2:// and s3:// prefixed paths are also accepted and converted automatically.

Performance:
    - Streaming operations (no temp file overhead for JSON)
    - DuckDB native r2:// integration for efficient parquet processing
    - s3fs-backed filesystem with connection caching
"""

from common.storage.core import StorageAccess
from common.storage.filesystem import (
    get_duckdb_with_r2,
    get_r2_filesystem,
    setup_duckdb_cloud_auth,
)
from common.storage.monitoring import ResourceMonitor

__all__ = [
    "ResourceMonitor",
    "StorageAccess",
    "get_duckdb_with_r2",
    "get_r2_filesystem",
    "setup_duckdb_cloud_auth",
]
