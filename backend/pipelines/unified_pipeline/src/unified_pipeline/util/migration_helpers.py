"""
Migration helper utilities for transitioning to the optimized GCSDataAccess architecture.

This module provides utility functions to help with the migration from the legacy
system to the optimized GCSDataAccess architecture. These helpers ensure
consistent patterns and reduce migration complexity.
"""

from typing import List, Optional

import duckdb

from unified_pipeline.util.gcs_access import GCSDataAccess
from unified_pipeline.util.log_util import Logger


class GCSMigrationHelper:
    """Helper utilities for migrating to GCSDataAccess."""

    def __init__(self, logger: Optional[Logger] = None):
        self.log = logger or Logger.get_logger()

    @staticmethod
    def upload_table_to_gcs(
        gcs_access: GCSDataAccess, table_name: str, bucket: str, path: str
    ) -> None:
        """
        Upload DuckDB table to GCS using optimized path.

        Replaces: gcs_util.upload_file() patterns for table data

        Args:
            gcs_access: GCSDataAccess instance
            table_name: Name of DuckDB table to upload
            bucket: GCS bucket name
            path: Path within bucket (without gs:// prefix)
        """
        gcs_path = f"gs://{bucket}/{path}"
        gcs_access.upload_from_duckdb_table(table_name, gcs_path)

    @staticmethod
    def download_table_from_gcs(gcs_access: GCSDataAccess, gcs_path: str, table_name: str) -> None:
        """
        Download GCS file and create DuckDB table.

        Replaces: gcs_util.download_file() + manual table creation patterns

        Args:
            gcs_access: GCSDataAccess instance
            gcs_path: Full GCS path (gs://bucket/path)
            table_name: Name for the created DuckDB table
        """
        gcs_access.create_table_from_gcs(table_name, gcs_path)

    @staticmethod
    def list_gcs_files(gcs_access: GCSDataAccess, bucket: str, prefix: str) -> List[str]:
        """
        List files in GCS bucket with prefix.

        Replaces: gcs_util.list_files()

        Args:
            gcs_access: GCSDataAccess instance
            bucket: GCS bucket name
            prefix: File prefix to filter by

        Returns:
            List of full GCS paths (gs://bucket/path format)
        """
        pattern = f"gs://{bucket}/{prefix}*"
        return gcs_access.list_files(pattern)

    @staticmethod
    def download_json_from_gcs(gcs_access: GCSDataAccess, bucket: str, blob_name: str) -> dict:
        """
        Download JSON data from GCS.

        Replaces: gcs_util.download_json_from_gcs()

        Args:
            gcs_access: GCSDataAccess instance
            bucket: GCS bucket name
            blob_name: Path to JSON file within bucket

        Returns:
            Parsed JSON data as dict or list
        """
        gcs_path = f"gs://{bucket}/{blob_name}"
        return gcs_access.download_json(gcs_path)

    @staticmethod
    def upload_json_to_gcs(
        gcs_access: GCSDataAccess, data: dict, bucket: str, blob_name: str
    ) -> None:
        """
        Upload JSON data to GCS.

        Replaces: gcs_util.upload_json_to_gcs() patterns

        Args:
            gcs_access: GCSDataAccess instance
            data: JSON data to upload
            bucket: GCS bucket name
            blob_name: Path within bucket for the JSON file
        """
        gcs_path = f"gs://{bucket}/{blob_name}"
        gcs_access.upload_json(data, gcs_path)

    def validate_connection_sharing(
        self, base_conn: duckdb.DuckDBPyConnection, gcs_access: GCSDataAccess
    ) -> bool:
        """
        Validate that BaseSource and GCSDataAccess are sharing the same connection.

        Args:
            base_conn: BaseSource DuckDB connection
            gcs_access: GCSDataAccess instance

        Returns:
            True if connections are properly shared, False otherwise
        """
        try:
            # Create a test table in base connection
            test_table = "migration_test_table"
            base_conn.execute(f"CREATE TABLE {test_table} AS SELECT 1 as test_col")

            # Try to access it from gcs_access connection
            result = gcs_access.duckdb_conn.execute(f"SELECT COUNT(*) FROM {test_table}").fetchone()

            # Clean up
            base_conn.execute(f"DROP TABLE IF EXISTS {test_table}")

            if result and result[0] == 1:
                self.log.info("✅ Connection sharing validation passed")
                return True
            else:
                self.log.error("❌ Connection sharing validation failed - connections are separate")
                return False

        except Exception as e:
            self.log.error(f"❌ Connection sharing validation failed with error: {e}")
            return False

    def create_unified_connection_setup(self) -> tuple[duckdb.DuckDBPyConnection, GCSDataAccess]:
        """
        Create a unified connection setup for testing migration patterns.

        Returns:
            Tuple of (DuckDB connection, GCSDataAccess instance) that share the same connection
        """
        # Create base connection
        conn = duckdb.connect()

        # Configure it properly
        try:
            conn.execute("SET memory_limit = '12GB'")
            conn.execute("SET max_memory = '12GB'")
            conn.execute("SET threads = 4")
            conn.execute("INSTALL spatial; LOAD spatial;")

            # Register gcsfs filesystem
            from fsspec import filesystem

            fs = filesystem("gs")
            conn.register_filesystem(fs)

            self.log.info("✅ Unified connection configured successfully")
        except Exception as e:
            self.log.warning(f"Connection configuration warning: {e}")

        # Create GCSDataAccess with shared connection
        gcs_access = GCSDataAccess(connection=conn)

        return conn, gcs_access


# Convenience functions for common migration patterns
def migrate_save_data_pattern(
    gcs_access: GCSDataAccess,
    table_name: str,
    dataset: str,
    bucket: str,
    stage: str,
    timestamp: str,
    subdataset: str = None,
) -> None:
    """
    Migrate the common _save_data pattern to GCSDataAccess.

    This replaces the pattern:
    ```python
    # OLD
    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
        conn.execute(f"COPY {table_name} TO '{tmp.name}' (FORMAT PARQUET)")
        # Use gcs_access instead of gcs_util
        gcs_path = f"gs://{bucket}/{path}"
        with open(tmp.name, "rb") as src:
            with gcs_access.fs.open(gcs_path, "wb") as dst:
                import shutil
                shutil.copyfileobj(src, dst)

    # NEW
    migrate_save_data_pattern(self.gcs_access, table_name, dataset, bucket, stage, timestamp)
    ```
    """
    # Determine final dataset name
    final_dataset = f"{dataset}_{subdataset}" if subdataset else dataset

    # Create path with timestamp
    filename = f"{final_dataset}.parquet"
    path = f"{stage}/{final_dataset}/{timestamp}/{filename}"
    gcs_path = f"gs://{bucket}/{path}"

    # 🚀 ENHANCED: Try native HMAC export first, fallback to existing method
    if hasattr(gcs_access, "export_to_gcs_native"):
        native_used = gcs_access.export_to_gcs_native(
            table_name,
            gcs_path,
            compression="zstd",  # Optimal compression
            row_group_size=100000,
        )
        if not native_used:
            # Fallback to existing method
            gcs_access.upload_from_duckdb_table(table_name, gcs_path)
    else:
        # Original method for backward compatibility
        gcs_access.upload_from_duckdb_table(table_name, gcs_path)


def migrate_read_data_pattern(
    gcs_access: GCSDataAccess, bucket: str, blob_name: str, table_name: str
) -> None:
    """
    Migrate the common data reading pattern to GCSDataAccess.

    This replaces the pattern:
    ```python
    # OLD
    # temp_file = self.gcs_util.download_file(bucket, blob_name, local_path)
    # self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{temp_file}')")

    # NEW
    migrate_read_data_pattern(self.gcs_access, bucket, blob_name, table_name)
    ```
    """
    gcs_path = f"gs://{bucket}/{blob_name}"

    # 🚀 ENHANCED: Try native HMAC loading first, fallback to existing method
    if hasattr(gcs_access, "query_parquet_native"):
        try:
            gcs_access.query_parquet_native(gcs_path, "SELECT *", table_name)
        except Exception:
            # Fallback to existing method
            gcs_access.create_table_from_gcs(table_name, gcs_path)
    else:
        # Original method for backward compatibility
        gcs_access.create_table_from_gcs(table_name, gcs_path)
