"""Base class for DuckDB operations in migrated pipelines."""

import time
from pathlib import Path
from typing import Any

import duckdb
from common.gcs.filesystem import setup_duckdb_cloud_auth


class DuckDBProcessor:
    """Base class for DuckDB-based data processing."""

    def __init__(self, db_path: str = ":memory:", dataset_name: str = "data"):
        self.conn = duckdb.connect(db_path)
        self.dataset_name = dataset_name
        self._setup_extensions()

    def _setup_extensions(self):
        """Setup required DuckDB extensions and cloud auth."""
        try:
            self.conn.execute("INSTALL spatial")
            self.conn.execute("LOAD spatial")
        except Exception as e:
            print(f"Warning: Could not load spatial extension: {e}")

        # Setup cloud storage auth (also installs/loads httpfs)
        setup_duckdb_cloud_auth(self.conn)

    def create_table_from_parquet(
        self, parquet_path: str | Path, table_name: str | None = None
    ) -> str:
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

    def create_spatial_table(
        self, geospatial_path: str | Path, table_name: str | None = None
    ) -> str:
        """Create a table from geospatial file."""
        if table_name is None:
            table_name = f"{self.dataset_name}_geo_{int(time.time())}"

        self.conn.execute(f"""
            CREATE TABLE {table_name} AS
            SELECT * FROM ST_Read('{geospatial_path}')
        """)
        return table_name

    def save_table_to_parquet(self, table_name: str, output_path: str | Path):
        """Save table to parquet file."""
        self.conn.execute(f"""
            COPY {table_name} TO '{output_path}' (FORMAT PARQUET)
        """)

    def save_table_to_csv(self, table_name: str, output_path: str | Path):
        """Save table to CSV file."""
        self.conn.execute(f"""
            COPY {table_name} TO '{output_path}' (FORMAT CSV, HEADER)
        """)

    def create_table_from_gcs_parquet(self, gcs_path: str, table_name: str | None = None) -> str:
        """
        Create a table directly from GCS parquet file using native DuckDB access.

        Args:
            gcs_path: GCS path (gs://bucket/path/file.parquet)
            table_name: Optional table name

        Returns:
            Table name
        """
        if table_name is None:
            table_name = f"{self.dataset_name}_gcs_{int(time.time())}"

        self.conn.execute(f"""
            CREATE TABLE {table_name} AS
            SELECT * FROM read_parquet('{gcs_path}')
        """)
        return table_name

    def save_table_to_gcs_parquet(self, table_name: str, gcs_path: str, **options):
        """
        Save table directly to GCS parquet file using native DuckDB access.

        Args:
            table_name: Name of the table to save
            gcs_path: GCS path (gs://bucket/path/file.parquet)
            **options: Additional parquet options (compression, etc.)
        """
        copy_options = ["FORMAT PARQUET"]

        if "compression" in options:
            copy_options.append(f"COMPRESSION {options['compression']}")
        else:
            copy_options.append("COMPRESSION zstd")  # Default to zstd

        if "row_group_size" in options:
            copy_options.append(f"ROW_GROUP_SIZE {options['row_group_size']}")

        options_str = ", ".join(copy_options)

        self.conn.execute(f"""
            COPY {table_name} TO '{gcs_path}' ({options_str})
        """)

    def query_gcs_parquet(
        self, gcs_path: str, query: str = "SELECT *", table_name: str | None = None
    ) -> str:
        """
        Query GCS parquet file directly and create a table.

        Args:
            gcs_path: GCS path (gs://bucket/path/file.parquet)
            query: SQL query to apply (default: SELECT *)
            table_name: Optional table name for result

        Returns:
            Table name containing query results
        """
        if table_name is None:
            table_name = f"{self.dataset_name}_query_{int(time.time())}"

        self.conn.execute(f"""
            CREATE TABLE {table_name} AS
            {query}
            FROM read_parquet('{gcs_path}')
        """)
        return table_name

    def get_table_info(self, table_name: str) -> list[dict[str, Any]]:
        """Get information about a table."""
        return self.conn.execute(f"DESCRIBE {table_name}").fetchall()

    def execute_query(self, query: str) -> list[Any]:
        """Execute a query and return results."""
        return self.conn.execute(query).fetchall()

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
