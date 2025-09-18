"""Base class for DuckDB operations in drive data pipeline."""

import time
from pathlib import Path
from typing import Any

import duckdb

# Handle imports for both standalone and package usage
try:
    # Use unified pipeline's optimized DuckDB connection
    from unified_pipeline.util.gcs_access import get_duckdb_with_gcs

    from ..utils.logging import get_logger

    logger = get_logger()
except ImportError:
    # Fallback for standalone usage
    import logging

    logger = logging.getLogger(__name__)

    # Fallback connection for standalone usage
    def get_duckdb_with_gcs() -> duckdb.DuckDBPyConnection:
        conn = duckdb.connect()
        try:
            conn.execute("INSTALL spatial")
            conn.execute("LOAD spatial")
        except Exception:
            pass
        return conn


class DuckDBProcessor:
    """Base class for DuckDB-based data processing in drive data pipeline."""

    # Class-level shared connection
    _shared_conn: duckdb.DuckDBPyConnection = None
    _conn_ref_count: int = 0

    def __init__(
        self,
        db_path: str = ":memory:",
        dataset_name: str = "drive_data",
        connection: duckdb.DuckDBPyConnection = None,
    ) -> None:
        self.dataset_name = dataset_name

        if connection:
            # Use provided connection (for sharing)
            self.conn = connection
            self._owns_connection = False
            logger.info(f"🔗 Using provided DuckDB connection for {dataset_name}")
        else:
            # Use shared connection or create new one
            if DuckDBProcessor._shared_conn is None:
                DuckDBProcessor._shared_conn = get_duckdb_with_gcs()
                logger.info("🔗 Created new shared DuckDB connection with GCS support")

            self.conn = DuckDBProcessor._shared_conn
            DuckDBProcessor._conn_ref_count += 1
            self._owns_connection = True
            logger.info(
                f"🔗 Using shared DuckDB connection for {dataset_name} "
                f"(ref count: {DuckDBProcessor._conn_ref_count})"
            )

    def register_table(self, data: Any, table_name: str) -> str:
        """Register data (DataFrame, etc.) as a DuckDB table."""
        self.conn.register(table_name, data)
        logger.debug(f"Registered data as table: {table_name}")
        return table_name

    def register_dataframe(self, df: Any, table_name: str) -> str:
        """Register a pandas DataFrame as a DuckDB table (legacy method)."""
        return self.register_table(df, table_name)

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
        logger.info(f"Created table {table_name} from parquet: {parquet_path}")
        return table_name

    def create_table_from_csv(self, csv_path: str | Path, table_name: str | None = None) -> str:
        """Create a table from CSV file."""
        if table_name is None:
            table_name = f"{self.dataset_name}_{int(time.time())}"

        self.conn.execute(f"""
            CREATE TABLE {table_name} AS
            SELECT * FROM read_csv('{csv_path}', AUTO_DETECT=TRUE, HEADER=TRUE)
        """)
        logger.info(f"Created table {table_name} from CSV: {csv_path}")
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
        logger.info(f"Created spatial table {table_name} from: {geospatial_path}")
        return table_name

    def export_to_parquet(self, table_name: str, output_path: str | Path) -> None:
        """Export table to parquet file."""
        # Ensure the output directory exists before saving
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        self.conn.execute(f"""
            COPY {table_name} TO '{output_path}' (FORMAT PARQUET, COMPRESSION snappy)
        """)
        logger.info(f"Exported table {table_name} to parquet: {output_path}")

    def export_to_geoparquet(self, table_name: str, output_path: str | Path) -> None:
        """Export spatial table to GeoParquet file."""
        # Ensure the output directory exists before saving
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        self.conn.execute(f"""
            COPY {table_name} TO '{output_path}' (FORMAT PARQUET, COMPRESSION snappy)
        """)
        logger.info(f"Exported spatial table {table_name} to GeoParquet: {output_path}")

    def save_table_to_parquet(self, table_name: str, output_path: str | Path) -> None:
        """Save table to parquet file (legacy method)."""
        # Ensure the output directory exists before saving
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        return self.export_to_parquet(table_name, output_path)

    def save_table_to_csv(self, table_name: str, output_path: str | Path) -> None:
        """Save table to CSV file."""
        # Ensure the output directory exists before saving
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        self.conn.execute(f"""
            COPY {table_name} TO '{output_path}' (FORMAT CSV, HEADER)
        """)
        logger.info(f"Saved table {table_name} to CSV: {output_path}")

    def export_table_to_dataframe(self, table_name: str) -> Any:
        """Export DuckDB table to pandas DataFrame (when needed for compatibility)."""
        return self.conn.execute(f"SELECT * FROM {table_name}").df()

    def get_table_info(self, table_name: str) -> list[dict[str, Any]]:
        """Get information about a table."""
        return self.conn.execute(f"DESCRIBE {table_name}").fetchall()

    def execute_query(self, query: str) -> list[Any]:
        """Execute a query and return results."""
        return self.conn.execute(query).fetchall()

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists."""
        try:
            self.conn.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
            return True
        except Exception:
            return False

    def drop_table(self, table_name: str) -> None:
        """Drop a table or view if it exists."""
        try:
            # Check if it's a table or view by querying information schema
            result = self.conn.execute(f"""
                SELECT table_type FROM information_schema.tables
                WHERE table_name = '{table_name}'
                UNION ALL
                SELECT 'VIEW' as table_type FROM information_schema.views
                WHERE table_name = '{table_name}'
            """).fetchall()

            if result:
                object_type = result[0][0]
                if object_type == "VIEW":
                    self.conn.execute(f"DROP VIEW IF EXISTS {table_name}")
                    logger.debug(f"Dropped view: {table_name}")
                else:
                    self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                    logger.debug(f"Dropped table: {table_name}")
            else:
                # Object doesn't exist, try both just in case
                try:
                    self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                except Exception:
                    self.conn.execute(f"DROP VIEW IF EXISTS {table_name}")
                logger.debug(f"Dropped object: {table_name}")

        except Exception as e:
            # Fallback: try both drop commands
            try:
                self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                logger.debug(f"Dropped table: {table_name}")
            except Exception:
                try:
                    self.conn.execute(f"DROP VIEW IF EXISTS {table_name}")
                    logger.debug(f"Dropped view: {table_name}")
                except Exception:
                    logger.warning(f"Failed to drop table/view {table_name}: {str(e)}")
                    # Don't raise exception since this is cleanup

    def close(self) -> None:
        """Close database connection with reference counting."""
        if self.conn and self._owns_connection:
            DuckDBProcessor._conn_ref_count -= 1
            logger.debug(f"Decremented connection ref count to {DuckDBProcessor._conn_ref_count}")

            # Only close if this is the last reference
            if DuckDBProcessor._conn_ref_count <= 0:
                self.conn.close()
                DuckDBProcessor._shared_conn = None
                DuckDBProcessor._conn_ref_count = 0
                logger.debug("Closed shared DuckDB connection (last reference)")
            else:
                logger.debug("Keeping shared DuckDB connection alive (other references exist)")
        elif self.conn and not self._owns_connection:
            # Don't close connections we don't own
            logger.debug("Not closing provided DuckDB connection (not owned)")

    def __enter__(self) -> "DuckDBProcessor":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
