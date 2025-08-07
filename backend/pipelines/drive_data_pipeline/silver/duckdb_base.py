"""Base class for DuckDB operations in drive data pipeline."""

import time
from pathlib import Path
from typing import Any

import duckdb

# Handle imports for both standalone and package usage
try:
    from ..utils.logging import get_logger

    logger = get_logger()
except ImportError:
    # Fallback for standalone usage
    import logging

    logger = logging.getLogger(__name__)


class DuckDBProcessor:
    """Base class for DuckDB-based data processing in drive data pipeline."""

    def __init__(self, db_path: str = ":memory:", dataset_name: str = "drive_data"):
        self.conn = duckdb.connect(db_path)
        self.dataset_name = dataset_name
        self._setup_extensions()
        logger.info(f"Initialized DuckDB processor for {dataset_name}")

    def _setup_extensions(self):
        """Setup required DuckDB extensions."""
        try:
            self.conn.execute("INSTALL spatial")
            self.conn.execute("LOAD spatial")
            logger.info("✅ DuckDB-spatial loaded successfully")
        except Exception as e:
            logger.warning(f"Could not load spatial extension: {e}")

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

    def export_to_parquet(self, table_name: str, output_path: str | Path):
        """Export table to parquet file."""
        # Ensure the output directory exists before saving
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        self.conn.execute(f"""
            COPY {table_name} TO '{output_path}' (FORMAT PARQUET, COMPRESSION snappy)
        """)
        logger.info(f"Exported table {table_name} to parquet: {output_path}")

    def export_to_geoparquet(self, table_name: str, output_path: str | Path):
        """Export spatial table to GeoParquet file."""
        # Ensure the output directory exists before saving
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        self.conn.execute(f"""
            COPY {table_name} TO '{output_path}' (FORMAT PARQUET, COMPRESSION snappy)
        """)
        logger.info(f"Exported spatial table {table_name} to GeoParquet: {output_path}")

    def save_table_to_parquet(self, table_name: str, output_path: str | Path):
        """Save table to parquet file (legacy method)."""
        # Ensure the output directory exists before saving
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        return self.export_to_parquet(table_name, output_path)

    def save_table_to_csv(self, table_name: str, output_path: str | Path):
        """Save table to CSV file."""
        # Ensure the output directory exists before saving
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        self.conn.execute(f"""
            COPY {table_name} TO '{output_path}' (FORMAT CSV, HEADER)
        """)
        logger.info(f"Saved table {table_name} to CSV: {output_path}")

    def export_table_to_dataframe(self, table_name: str):
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
        except:
            return False

    def drop_table(self, table_name: str):
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

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logger.debug("Closed DuckDB connection")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
