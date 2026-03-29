"""
Migration helper utilities for transitioning to the optimized StorageAccess architecture.

This module provides utility functions to help with the migration from the legacy
system to the optimized StorageAccess architecture. These helpers ensure
consistent patterns and reduce migration complexity.
"""

import duckdb
from common.storage import StorageAccess

from unified_pipeline.util.log_util import Logger


class GCSMigrationHelper:
    """Helper utilities for migrating to StorageAccess."""

    def __init__(self, logger: Logger | None = None):
        self.log = logger or Logger.get_logger()

    @staticmethod
    def upload_table_to_storage(
        storage_access: StorageAccess, table_name: str, bucket: str, path: str
    ) -> None:
        """
        Upload DuckDB table to storage using optimized path.

        Replaces: gcs_util.upload_file() patterns for table data

        Args:
            storage_access: StorageAccess instance
            table_name: Name of DuckDB table to upload
            bucket: Storage bucket name
            path: Path within bucket (bare bucket/path)
        """
        storage_path = f"{bucket}/{path}"
        storage_access.upload_from_duckdb_table(table_name, storage_path)

    @staticmethod
    def download_table_from_storage(
        storage_access: StorageAccess, storage_path: str, table_name: str
    ) -> None:
        """
        Download storage file and create DuckDB table.

        Replaces: gcs_util.download_file() + manual table creation patterns

        Args:
            storage_access: StorageAccess instance
            storage_path: Full storage path (bucket/path)
            table_name: Name for the created DuckDB table
        """
        storage_access.create_table_from_storage(table_name, storage_path)

    @staticmethod
    def list_storage_files(storage_access: StorageAccess, bucket: str, prefix: str) -> list[str]:
        """
        List files in storage bucket with prefix.

        Replaces: gcs_util.list_files()

        Args:
            storage_access: StorageAccess instance
            bucket: Storage bucket name
            prefix: File prefix to filter by

        Returns:
            List of full storage paths (bucket/path format)
        """
        pattern = f"{bucket}/{prefix}*"
        return storage_access.list_files(pattern)

    @staticmethod
    def download_json_from_storage(
        storage_access: StorageAccess, bucket: str, blob_name: str
    ) -> dict:
        """
        Download JSON data from storage.

        Replaces: gcs_util.download_json_from_gcs()

        Args:
            storage_access: StorageAccess instance
            bucket: Storage bucket name
            blob_name: Path to JSON file within bucket

        Returns:
            Parsed JSON data as dict or list
        """
        storage_path = f"{bucket}/{blob_name}"
        return storage_access.download_json(storage_path)

    @staticmethod
    def upload_json_to_storage(
        storage_access: StorageAccess, data: dict, bucket: str, blob_name: str
    ) -> None:
        """
        Upload JSON data to storage.

        Replaces: gcs_util.upload_json_to_gcs() patterns

        Args:
            storage_access: StorageAccess instance
            data: JSON data to upload
            bucket: Storage bucket name
            blob_name: Path within bucket for the JSON file
        """
        storage_path = f"{bucket}/{blob_name}"
        storage_access.upload_json(data, storage_path)

    def validate_connection_sharing(
        self, base_conn: duckdb.DuckDBPyConnection, storage_access: StorageAccess
    ) -> bool:
        """
        Validate that BaseSource and StorageAccess are sharing the same connection.

        Args:
            base_conn: BaseSource DuckDB connection
            storage_access: StorageAccess instance

        Returns:
            True if connections are properly shared, False otherwise
        """
        try:
            # Create a test table in base connection
            test_table = "migration_test_table"
            base_conn.execute(f"CREATE TABLE {test_table} AS SELECT 1 as test_col")

            # Try to access it from storage_access connection
            result = storage_access.duckdb_conn.execute(
                f"SELECT COUNT(*) FROM {test_table}"
            ).fetchone()

            # Clean up
            base_conn.execute(f"DROP TABLE IF EXISTS {test_table}")

            if result and result[0] == 1:
                self.log.info("✅ Connection sharing validation passed")
                return True
            self.log.error("❌ Connection sharing validation failed - connections are separate")
            return False

        except Exception as e:
            self.log.error(f"❌ Connection sharing validation failed with error: {e}")
            return False

    def create_unified_connection_setup(self) -> tuple[duckdb.DuckDBPyConnection, StorageAccess]:
        """
        Create a unified connection setup for testing migration patterns.

        Returns:
            Tuple of (DuckDB connection, StorageAccess instance) that share the same connection
        """
        # Create base connection
        conn = duckdb.connect()

        # Configure it properly
        try:
            conn.execute("SET memory_limit = '12GB'")
            conn.execute("SET max_memory = '12GB'")
            conn.execute("SET threads = 4")

            # Register s3fs filesystem
            import os

            from fsspec import filesystem

            # Use HMAC authentication if available (same pattern as storage_access.py)
            storage_access_key = os.getenv("GCS_ACCESS_KEY_ID")
            gcs_secret_key = os.getenv("GCS_SECRET_ACCESS_KEY")

            if storage_access_key and gcs_secret_key:
                fs = filesystem(
                    "gs", access_key_id=storage_access_key, secret_access_key=gcs_secret_key
                )
            else:
                fs = filesystem("gs")

            conn.register_filesystem(fs)

            self.log.info("✅ Unified connection configured successfully")
        except Exception as e:
            self.log.warning(f"Connection configuration warning: {e}")

        # Create StorageAccess with shared connection
        storage_access = StorageAccess(connection=conn)

        return conn, storage_access


# Convenience functions for common migration patterns
def migrate_save_data_pattern(
    storage_access: StorageAccess,
    table_name: str,
    dataset: str,
    bucket: str,
    stage: str,
    timestamp: str,
    subdataset: str | None = None,
) -> None:
    """
    Migrate the common _save_data pattern to StorageAccess.

    This replaces the pattern:
    ```python
    # OLD
    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
        conn.execute(f"COPY {table_name} TO '{tmp.name}' (FORMAT PARQUET)")
        # Use storage_access instead of gcs_util
        storage_path = f"{bucket}/{path}"
        with open(tmp.name, "rb") as src:
            with storage_access.fs.open(storage_path, "wb") as dst:
                import shutil
                shutil.copyfileobj(src, dst)

    # NEW
    migrate_save_data_pattern(self.storage, table_name, dataset, bucket, stage, timestamp)
    ```
    """
    # Determine final dataset name
    final_dataset = f"{dataset}_{subdataset}" if subdataset else dataset

    # Create path with timestamp
    filename = f"{final_dataset}.parquet"
    path = f"{stage}/{final_dataset}/{timestamp}/{filename}"
    storage_path = f"{bucket}/{path}"

    # 🚀 ENHANCED: Try native HMAC export first, fallback to existing method
    if hasattr(storage_access, "export_to_storage_native"):
        native_used = storage_access.export_to_storage_native(
            table_name,
            storage_path,
            compression="zstd",  # Optimal compression
            row_group_size=100000,
        )
        if not native_used:
            # Fallback to existing method
            storage_access.upload_from_duckdb_table(table_name, storage_path)
    else:
        # Original method for backward compatibility
        storage_access.upload_from_duckdb_table(table_name, storage_path)


def migrate_read_data_pattern(
    storage_access: StorageAccess, bucket: str, blob_name: str, table_name: str
) -> None:
    """
    Migrate the common data reading pattern to StorageAccess.

    This replaces the pattern:
    ```python
    # OLD
    # self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{temp_file}')")

    # NEW
    migrate_read_data_pattern(self.storage, bucket, blob_name, table_name)
    ```
    """
    storage_path = f"{bucket}/{blob_name}"

    # 🚀 ENHANCED: Try native HMAC loading first, fallback to existing method
    if hasattr(storage_access, "query_parquet_native"):
        try:
            storage_access.query_parquet_native(storage_path, "SELECT *", table_name)
        except Exception:
            # Fallback to existing method
            storage_access.create_table_from_storage(table_name, storage_path)
    else:
        # Original method for backward compatibility
        storage_access.create_table_from_storage(table_name, storage_path)
