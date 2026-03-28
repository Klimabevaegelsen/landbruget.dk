"""
Cloud Object Storage Access Module (Cloudflare R2 via s3fs).

This module is THE single source of truth for all cloud storage operations across
the codebase. All pipelines should import from here instead of using s3fs or
google-cloud-storage directly.

Usage:
    from common.gcs import GCSDataAccess

    gcs = GCSDataAccess()

    # Read parquet from R2
    gcs.query_parquet_direct("r2://bucket/file.parquet", "SELECT *", "my_table")

    # Write parquet to R2
    gcs.upload_from_duckdb_table("my_table", "r2://bucket/output.parquet")

    # Stream JSON
    gcs.upload_json(data, "r2://bucket/data.json")
    gcs.download_json("r2://bucket/data.json")

    # Legacy gs:// paths are also accepted and converted automatically.

Performance:
    - Streaming operations (no temp file overhead for JSON)
    - DuckDB native r2:// integration for efficient parquet processing
    - s3fs-backed filesystem with connection caching
"""

from common.gcs.core import GCSDataAccess
from common.gcs.filesystem import (
    get_duckdb_with_gcs,
    get_gcs_filesystem,
    setup_duckdb_cloud_auth,
)
from common.gcs.monitoring import ResourceMonitor

__all__ = [
    "GCSDataAccess",
    "ResourceMonitor",
    "get_duckdb_with_gcs",
    "get_gcs_filesystem",
    "setup_duckdb_cloud_auth",
]
