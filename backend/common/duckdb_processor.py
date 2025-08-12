"""
Shared DuckDB processor base class for all pipelines.

This module provides a standardized interface for DuckDB operations across
all agricultural data pipelines, ensuring consistency in connection management,
extension loading, and common operations.
"""

import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import duckdb


class SharedDuckDBProcessor:
    """
    Shared base class for DuckDB-based data processing across all pipelines.
    
    This class consolidates common DuckDB operations that were previously
    duplicated across multiple pipeline implementations.
    """

    def __init__(self, db_path: str = ":memory:", dataset_name: str = "data"):
        """
        Initialize DuckDB processor.
        
        Args:
            db_path: Path to DuckDB database file or ":memory:" for in-memory
            dataset_name: Default name prefix for generated tables
        """
        self.conn = duckdb.connect(db_path)
        self.dataset_name = dataset_name
        self._setup_extensions()

    def _setup_extensions(self):
        """Setup commonly required DuckDB extensions."""
        extensions = [
            ("spatial", "Geospatial operations"),
            ("httpfs", "HTTP/S3 file system access"),
        ]
        
        for ext_name, description in extensions:
            try:
                self.conn.execute(f"INSTALL {ext_name}")
                self.conn.execute(f"LOAD {ext_name}")
            except Exception as e:
                print(f"Warning: Could not load {ext_name} extension ({description}): {e}")

        # Setup GCS HMAC authentication if available
        self._setup_gcs_auth()

    def _setup_gcs_auth(self):
        """Setup Google Cloud Storage HMAC authentication for native DuckDB access."""
        try:
            gcs_access_key = os.getenv("GCS_ACCESS_KEY_ID")
            gcs_secret_key = os.getenv("GCS_SECRET_ACCESS_KEY")

            if gcs_access_key and gcs_secret_key:
                self.conn.execute(f"""
                    CREATE OR REPLACE PERSISTENT SECRET gcs_hmac (
                        TYPE GCS,
                        KEY_ID '{gcs_access_key}',
                        SECRET '{gcs_secret_key}'
                    );
                """)
                print("✅ DuckDB GCS HMAC authentication configured")
            else:
                print("ℹ️  GCS HMAC credentials not found")
        except Exception as e:
            print(f"Warning: Could not setup GCS HMAC authentication: {e}")

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists."""
        try:
            self.conn.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
            return True
        except Exception:
            return False

    def create_table_from_parquet(
        self, parquet_path: Union[str, Path], table_name: Optional[str] = None
    ) -> str:
        """Create a table from parquet file."""
        if table_name is None:
            table_name = f"{self.dataset_name}_{int(time.time())}"

        self.conn.execute(f"""
            CREATE TABLE {table_name} AS 
            SELECT * FROM read_parquet('{parquet_path}')
        """)
        return table_name

    def create_table_from_csv(
        self, csv_path: Union[str, Path], table_name: Optional[str] = None
    ) -> str:
        """Create a table from CSV file."""
        if table_name is None:
            table_name = f"{self.dataset_name}_{int(time.time())}"

        self.conn.execute(f"""
            CREATE TABLE {table_name} AS 
            SELECT * FROM read_csv('{csv_path}', AUTO_DETECT=TRUE, HEADER=TRUE)
        """)
        return table_name

    def create_spatial_table(
        self, geospatial_path: Union[str, Path], table_name: Optional[str] = None
    ) -> str:
        """Create a table from geospatial file."""
        if table_name is None:
            table_name = f"{self.dataset_name}_geo_{int(time.time())}"

        self.conn.execute(f"""
            CREATE TABLE {table_name} AS 
            SELECT * FROM ST_Read('{geospatial_path}')
        """)
        return table_name

    def save_table_to_parquet(self, table_name: str, output_path: Union[str, Path]):
        """Save table to parquet file."""
        self.conn.execute(f"""
            COPY {table_name} TO '{output_path}' (FORMAT PARQUET)
        """)

    def save_table_to_csv(self, table_name: str, output_path: Union[str, Path]):
        """Save table to CSV file."""
        self.conn.execute(f"""
            COPY {table_name} TO '{output_path}' (FORMAT CSV, HEADER)
        """)

    def create_table_from_gcs_parquet(
        self, gcs_path: str, table_name: Optional[str] = None
    ) -> str:
        """Create a table directly from GCS parquet file using native DuckDB access."""
        if table_name is None:
            table_name = f"{self.dataset_name}_gcs_{int(time.time())}"

        self.conn.execute(f"""
            CREATE TABLE {table_name} AS 
            SELECT * FROM read_parquet('{gcs_path}')
        """)
        return table_name

    def save_table_to_gcs_parquet(
        self, table_name: str, gcs_path: str, compression: str = "zstd", **options
    ):
        """Save table directly to GCS parquet file using native DuckDB access."""
        copy_options = [f"FORMAT PARQUET", f"COMPRESSION {compression}"]

        if "row_group_size" in options:
            copy_options.append(f"ROW_GROUP_SIZE {options['row_group_size']}")

        options_str = ", ".join(copy_options)
        self.conn.execute(f"COPY {table_name} TO '{gcs_path}' ({options_str})")

    def get_table_info(self, table_name: str) -> List[Dict[str, Any]]:
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

    def execute_query(self, query: str) -> List[Any]:
        """Execute a query and return results."""
        return self.conn.execute(query).fetchall()

    def drop_table_if_exists(self, table_name: str):
        """Drop a table if it exists."""
        try:
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        except Exception:
            pass  # Table doesn't exist or other error

    def close(self):
        """Close database connection."""
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
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
    ):
        """
        Initialize pipeline processor.
        
        Args:
            db_path: Path to DuckDB database file or ":memory:" for in-memory
            dataset_name: Default name prefix for generated tables
            logger: Logger instance for pipeline operations
        """
        super().__init__(db_path, dataset_name)
        self.logger = logger

    def log_info(self, message: str):
        """Log info message if logger available."""
        if self.logger:
            self.logger.info(message)
        else:
            print(f"INFO: {message}")

    def log_warning(self, message: str):
        """Log warning message if logger available."""
        if self.logger:
            self.logger.warning(message)
        else:
            print(f"WARNING: {message}")

    def log_error(self, message: str):
        """Log error message if logger available."""
        if self.logger:
            self.logger.error(message)
        else:
            print(f"ERROR: {message}")

    def safe_execute(self, query: str, description: str = "Query") -> Optional[List[Any]]:
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
            self.log_error(f"❌ {description} failed: {str(e)}")
            return None

    def process_with_memory_monitoring(self, operation_func, description: str = "Operation"):
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
            
            self.log_info(
                f"✅ {description} completed "
                f"(Memory: {memory_after:.1f}MB, Change: {memory_diff:+.1f}MB)"
            )
            
            return result
            
        except ImportError:
            self.log_warning("psutil not available, memory monitoring disabled")
            return operation_func()
        except Exception as e:
            self.log_error(f"❌ {description} failed: {str(e)}")
            raise