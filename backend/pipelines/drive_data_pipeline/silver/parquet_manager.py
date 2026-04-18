"""Parquet and GeoParquet output management for Silver layer."""

import json
import os
import tempfile
import time
from pathlib import Path
from typing import Any

from common.storage.core import _patch_parquet_kv_metadata

# Handle imports for both standalone and package usage
try:
    from ..utils.logging import get_logger
    from ..utils.storage import DriveStorageManager
except ImportError:
    # Fallback for standalone usage
    import logging

    def get_logger() -> logging.Logger:
        return logging.getLogger(__name__)

    DriveStorageManager = None
from .duckdb_base import DuckDBProcessor

# Get logger
logger = get_logger()


class ParquetManager(DuckDBProcessor):
    """Manager for Parquet and GeoParquet output using DuckDB."""

    def __init__(
        self,
        storage_manager: "DriveStorageManager",
        compression: str = "snappy",
        partition_by: list[str] | None = None,
    ) -> None:
        """Initialize the Parquet manager.

        Args:
            storage_manager: Storage manager for file operations
            compression: Compression algorithm to use
            partition_by: List of columns to partition by
        """
        super().__init__(dataset_name="parquet_manager")
        self.storage_manager = storage_manager
        self.compression = compression
        self.partition_by = partition_by
        logger.info(f"Initialized DuckDB-based ParquetManager with compression={compression}")

    def save_table_to_parquet(
        self,
        table_name: str,
        output_path: Path,
        schema_metadata: dict | None = None,
        row_group_size: int = 100000,
    ) -> Path:
        """Save a DuckDB table to Parquet format.

        Args:
            table_name: Name of DuckDB table to save
            output_path: Path to save to
            schema_metadata: Optional schema metadata to include
            row_group_size: Number of rows per group

        Returns:
            Path to the saved file
        """
        try:
            logger.info(f"Saving DuckDB table '{table_name}' to Parquet: {output_path}")

            # Check if we're using R2 cloud storage
            if self.storage_manager.storage_type.lower() == "r2":
                # Cloud storage - write to temp file first, then upload
                with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as temp_file:
                    temp_path = temp_file.name

                try:
                    # Export from DuckDB table to parquet
                    self.conn.execute(f"""
                        COPY {table_name} TO '{temp_path}'
                        (FORMAT PARQUET, COMPRESSION {self.compression},
                         ROW_GROUP_SIZE {row_group_size})
                    """)

                    if schema_metadata:
                        _patch_parquet_kv_metadata(
                            temp_path,
                            {b"schema": json.dumps(schema_metadata).encode("utf-8")},
                            log=logger,
                            compression=self.compression,
                        )

                    # Upload to cloud storage
                    with open(temp_path, "rb") as f:
                        self.storage_manager.save_file(f.read(), output_path)

                    logger.info(f"✅ Saved Parquet file to cloud storage: {output_path}")
                    return output_path

                finally:
                    # Clean up temp file
                    if os.path.exists(temp_path):
                        os.unlink(temp_path)
            else:
                # Local storage - direct export
                # CRITICAL FIX: Always ensure directory exists before DuckDB writes
                # This prevents "No such file or directory" errors in any environment
                try:
                    # First try to use the storage manager's directory creation
                    self.storage_manager.ensure_directory_exists(output_path.parent)
                except Exception as e:
                    logger.warning(f"Storage manager directory creation failed: {e}")
                    # Fallback to direct directory creation
                    output_path.parent.mkdir(parents=True, exist_ok=True)

                # Additional safety check - ensure parent directory exists
                if not output_path.parent.exists():
                    logger.warning(f"Directory still doesn't exist, creating: {output_path.parent}")
                    output_path.parent.mkdir(parents=True, exist_ok=True)

                logger.info(f"Writing DuckDB table to path: {output_path}")

                # Export directly from DuckDB
                self.conn.execute(f"""
                    COPY {table_name} TO '{output_path}'
                    (FORMAT PARQUET, COMPRESSION {self.compression},
                     ROW_GROUP_SIZE {row_group_size})
                """)

                if schema_metadata:
                    _patch_parquet_kv_metadata(
                        str(output_path),
                        {b"schema": json.dumps(schema_metadata).encode("utf-8")},
                        log=logger,
                        compression=self.compression,
                    )

                logger.info(f"Saved Parquet file to {output_path}")
                return output_path

        except Exception as e:
            error_msg = f"Failed to save DuckDB table to Parquet: {e!s}"
            logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    def save_dataframe_to_parquet(
        self,
        df: Any,
        output_path: Path,
        schema_metadata: dict | None = None,
        row_group_size: int = 100000,
    ) -> Path:
        """Save a pandas DataFrame or a DuckDB table name to Parquet."""
        if isinstance(df, str):
            return self.save_table_to_parquet(df, output_path, schema_metadata, row_group_size)

        table_name = f"temp_df_{int(time.time())}"
        self.register_dataframe(df, table_name)
        return self.save_table_to_parquet(table_name, output_path, schema_metadata, row_group_size)

    def create_spatial_table_from_coords(
        self,
        table_name: str,
        latitude_col: str,
        longitude_col: str,
        target_crs: str = "EPSG:4326",
    ) -> str:
        """Create spatial table from coordinate columns using DuckDB-spatial.

        Args:
            table_name: Name of source table
            latitude_col: Name of latitude column
            longitude_col: Name of longitude column
            target_crs: Target coordinate reference system

        Returns:
            Name of spatial table
        """
        try:
            logger.info(f"Creating spatial table from coordinates in '{table_name}'")

            spatial_table = f"{table_name}_spatial"

            # Create spatial table using DuckDB-spatial
            self.conn.execute(f"""
                CREATE TABLE {spatial_table} AS
                SELECT
                    * EXCLUDE ({latitude_col}, {longitude_col}),
                    ST_Point({longitude_col}, {latitude_col}) as geometry
                FROM {table_name}
                WHERE {latitude_col} IS NOT NULL
                  AND {longitude_col} IS NOT NULL
                  AND {latitude_col} BETWEEN -90 AND 90
                  AND {longitude_col} BETWEEN -180 AND 180
            """)

            count = self.conn.execute(f"SELECT COUNT(*) FROM {spatial_table}").fetchone()[0]
            logger.info(f"Created spatial table '{spatial_table}' with {count:,} features")
            return spatial_table

        except Exception as e:
            error_msg = f"Failed to create spatial table: {e!s}"
            logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    def save_spatial_table_to_geoparquet(
        self,
        table_name: str,
        output_path: Path,
        geometry_column: str = "geometry",
        schema_metadata: dict | None = None,
    ) -> Path:
        """Save a spatial DuckDB table to GeoParquet.

        DuckDB's spatial extension writes GeoParquet 1.1 metadata but omits the
        CRS field — callers must standardize geometries to EPSG:4326 upstream
        (handled here by GeospatialValidator.standardize()).
        """
        logger.info(f"Saving spatial table '{table_name}' to GeoParquet: {output_path}")

        if self.storage_manager.storage_type.lower() == "r2":
            with tempfile.NamedTemporaryFile(suffix=".geoparquet", delete=False) as temp_file:
                temp_path = Path(temp_file.name)
            try:
                self._copy_to_parquet(table_name, temp_path)
                if schema_metadata:
                    self._splice_schema_metadata(temp_path, schema_metadata)
                with open(temp_path, "rb") as f:
                    self.storage_manager.save_file(f.read(), output_path)
                logger.info(f"Saved GeoParquet file to R2: {output_path}")
                return output_path
            finally:
                if temp_path.exists():
                    os.unlink(temp_path)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        self._copy_to_parquet(table_name, output_path)
        if schema_metadata:
            self._splice_schema_metadata(output_path, schema_metadata)
        logger.info(f"Saved GeoParquet file to {output_path}")
        return output_path

    def _copy_to_parquet(self, table_name: str, path: Path) -> None:
        self.conn.execute(
            f"COPY {table_name} TO '{path}' (FORMAT PARQUET, COMPRESSION {self.compression})"
        )

    def _splice_schema_metadata(self, path: Path, schema_metadata: dict) -> None:
        _patch_parquet_kv_metadata(
            str(path),
            {b"schema": json.dumps(schema_metadata).encode("utf-8")},
            log=logger,
            compression=self.compression,
        )
