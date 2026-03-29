"""
Shared DuckDB processor base class for all pipelines.

This module provides a standardized interface for DuckDB operations across
all agricultural data pipelines, ensuring consistency in connection management,
extension loading, and common operations.
"""

import contextlib
import time
from pathlib import Path
from typing import Any

import duckdb
from common.storage.filesystem import setup_duckdb_cloud_auth


class SharedDuckDBProcessor:
    """
    Shared base class for DuckDB-based data processing across all pipelines.

    This class consolidates common DuckDB operations that were previously
    duplicated across multiple pipeline implementations.
    """

    def __init__(self, db_path: str = ":memory:", dataset_name: str = "data") -> None:
        """
        Initialize DuckDB processor.

        Args:
            db_path: Path to DuckDB database file or ":memory:" for in-memory
            dataset_name: Default name prefix for generated tables
        """
        self.conn = duckdb.connect(db_path)
        self.dataset_name = dataset_name
        self._setup_extensions()

    def _setup_extensions(self) -> None:
        """Setup commonly required DuckDB extensions and cloud auth."""
        with contextlib.suppress(Exception):
            self.conn.execute("INSTALL spatial")
            self.conn.execute("LOAD spatial")

        # Setup cloud storage auth (also installs/loads httpfs)
        setup_duckdb_cloud_auth(self.conn)

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists."""
        try:
            self.conn.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
            return True
        except Exception:
            return False

    def create_table_from_parquet(self, parquet_path: str | Path, table_name: str | None = None) -> str:
        """Create a table from parquet file."""
        if table_name is None:
            table_name = f"{self.dataset_name}_{int(time.time())}"

        self.conn.execute(f"""
            CREATE TABLE {table_name} AS
            SELECT * FROM read_parquet('{parquet_path}')
        """)
        return table_name

    def create_table_from_csv(self, csv_path: str | Path, table_name: str | None = None) -> str:
        """Create a table from CSV file."""
        if table_name is None:
            table_name = f"{self.dataset_name}_{int(time.time())}"

        self.conn.execute(f"""
            CREATE TABLE {table_name} AS
            SELECT * FROM read_csv('{csv_path}', AUTO_DETECT=TRUE, HEADER=TRUE)
        """)
        return table_name

    def create_spatial_table(self, geospatial_path: str | Path, table_name: str | None = None) -> str:
        """Create a table from geospatial file."""
        if table_name is None:
            table_name = f"{self.dataset_name}_geo_{int(time.time())}"

        self.conn.execute(f"""
            CREATE TABLE {table_name} AS
            SELECT * FROM ST_Read('{geospatial_path}')
        """)
        return table_name

    def save_table_to_parquet(self, table_name: str, output_path: str | Path) -> None:
        """Save table to parquet file."""
        self.conn.execute(f"""
            COPY {table_name} TO '{output_path}' (FORMAT PARQUET)
        """)

    def save_table_to_csv(self, table_name: str, output_path: str | Path) -> None:
        """Save table to CSV file."""
        self.conn.execute(f"""
            COPY {table_name} TO '{output_path}' (FORMAT CSV, HEADER)
        """)

    def create_table_from_storage_parquet(self, storage_path: str, table_name: str | None = None) -> str:
        """Create a table directly from storage parquet file using native DuckDB access."""
        if table_name is None:
            table_name = f"{self.dataset_name}_storage_{int(time.time())}"

        self.conn.execute(f"""
            CREATE TABLE {table_name} AS
            SELECT * FROM read_parquet('{storage_path}')
        """)
        return table_name

    def save_table_to_storage_parquet(
        self, table_name: str, storage_path: str, compression: str = "zstd", **options
    ) -> None:
        """Save table directly to storage parquet file using native DuckDB access."""
        copy_options = ["FORMAT PARQUET", f"COMPRESSION {compression}"]

        if "row_group_size" in options:
            copy_options.append(f"ROW_GROUP_SIZE {options['row_group_size']}")

        options_str = ", ".join(copy_options)
        self.conn.execute(f"COPY {table_name} TO '{storage_path}' ({options_str})")

    def get_table_info(self, table_name: str) -> list[dict[str, Any]]:
        """Get information about a table."""
        return [
            {
                "column_name": row[0],
                "column_type": row[1],
                "null": row[2],
                "key": row[3],
                "default": row[4],
                "extra": row[5],
            }
            for row in self.conn.execute(f"DESCRIBE {table_name}").fetchall()
        ]

    def get_row_count(self, table_name: str) -> int:
        """Get row count for a table."""
        result = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        return result[0] if result else 0

    def execute_query(self, query: str) -> list[Any]:
        """Execute a query and return results."""
        return self.conn.execute(query).fetchall()

    def create_spatial_index(
        self,
        table_name: str,
        geometry_column: str = "geometry",
        index_name: str | None = None,
    ) -> bool:
        """
        Create an R-tree spatial index on a geometry column.

        Best practice: Call AFTER populating the table (bulk-load algorithm is faster).
        The index accelerates ST_Intersects, ST_Within, ST_Contains, ST_Covers, etc.

        Args:
            table_name: Table to index
            geometry_column: Name of the geometry column
            index_name: Optional custom index name (auto-generated if None)

        Returns:
            True if index was created successfully, False otherwise
        """
        if index_name is None:
            index_name = f"idx_{table_name}_{geometry_column}_rtree"
        try:
            self.conn.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} USING RTREE({geometry_column})")
            return True
        except Exception:
            return False

    def drop_table_if_exists(self, table_name: str) -> None:
        """Drop a table if it exists."""
        with contextlib.suppress(Exception):
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")

    def close(self) -> None:
        """Close database connection."""
        if self.conn:
            with contextlib.suppress(Exception):
                self.conn.close()

    def __enter__(self) -> "SharedDuckDBProcessor":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


class PipelineProcessor(SharedDuckDBProcessor):
    """
    Extended processor for pipeline-specific operations.

    This class adds pipeline-specific functionality on top of the base
    SharedDuckDBProcessor, such as logging integration and error handling.
    """

    def __init__(
        self,
        db_path: str = ":memory:",
        dataset_name: str = "data",
        logger=None,
    ) -> None:
        """
        Initialize pipeline processor.

        Args:
            db_path: Path to DuckDB database file or ":memory:" for in-memory
            dataset_name: Default name prefix for generated tables
            logger: Logger instance for pipeline operations
        """
        super().__init__(db_path, dataset_name)
        self.logger = logger

    def log_info(self, message: str) -> None:
        """Log info message if logger available."""
        if self.logger:
            self.logger.info(message)
        else:
            pass

    def log_warning(self, message: str) -> None:
        """Log warning message if logger available."""
        if self.logger:
            self.logger.warning(message)
        else:
            pass

    def log_error(self, message: str) -> None:
        """Log error message if logger available."""
        if self.logger:
            self.logger.error(message)
        else:
            pass

    def safe_execute(self, query: str, description: str = "Query") -> list[Any] | None:
        """
        Execute query with error handling and logging.

        Args:
            query: SQL query to execute
            description: Description of the operation for logging

        Returns:
            Query results or None if error occurred
        """
        try:
            self.log_info(f"Executing {description}")
            result = self.conn.execute(query).fetchall()
            self.log_info(f"✅ {description} completed successfully")
            return result
        except Exception as e:
            self.log_error(f"❌ {description} failed: {e!s}")
            return None

    def process_with_memory_monitoring(self, operation_func, description: str = "Operation") -> Any:
        """
        Execute an operation with memory monitoring.

        Args:
            operation_func: Function to execute
            description: Description for logging
        """
        try:
            import psutil

            process = psutil.Process()
            memory_before = process.memory_info().rss / 1024 / 1024  # MB

            self.log_info(f"🔄 Starting {description} (Memory: {memory_before:.1f}MB)")

            result = operation_func()

            memory_after = process.memory_info().rss / 1024 / 1024  # MB
            memory_diff = memory_after - memory_before

            self.log_info(f"✅ {description} completed (Memory: {memory_after:.1f}MB, Change: {memory_diff:+.1f}MB)")

            return result

        except ImportError:
            self.log_warning("psutil not available, memory monitoring disabled")
            return operation_func()
        except Exception as e:
            self.log_error(f"❌ {description} failed: {e!s}")
            raise
