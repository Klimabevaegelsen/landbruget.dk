import logging
from typing import Dict, List, Optional, Tuple

import duckdb

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages DuckDB connection and operations."""

    def __init__(self):
        """Initialize DuckDB connection with spatial extension."""
        self.con = duckdb.connect(":memory:")
        self.con.install_extension("spatial")
        self.con.load_extension("spatial")
        logger.info("DuckDB connection initialized with spatial extension")

    def execute_query(self, query: str, params: Optional[Dict] = None) -> List[Tuple]:
        """Execute a SQL query with optional parameters."""
        try:
            if params:
                return self.con.execute(query, params).fetchall()
            return self.con.execute(query).fetchall()
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise

    def executemany(self, query: str, params_list: List[Tuple]) -> None:
        """Execute a SQL query with a list of parameters (for bulk inserts)."""
        try:
            self.con.executemany(query, params_list)
            logger.debug(f"Executed query with {len(params_list)} parameter sets.")
        except Exception as e:
            logger.error(f"Error executing query with executemany: {str(e)}")
            raise

    def create_table(
        self, table_name: str, file_path: str
    ) -> None:  # Changed Path to str for file_path for now
        """Create a table from a parquet file."""
        try:
            if table_name == "pesticide":
                self.con.execute(
                    f"CREATE TABLE {table_name}_temp AS SELECT * FROM read_parquet('{file_path}')"
                )
                self.con.execute(
                    f"CREATE TABLE {table_name} AS SELECT rowid as OriginalPesticideRowID, * FROM {table_name}_temp"
                )
                self.con.execute(f"DROP TABLE {table_name}_temp")
            else:
                self.con.execute(
                    f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{file_path}')"
                )
            logger.info(f"Created table {table_name} from {file_path}")
        except Exception as e:
            logger.error(f"Error creating table {table_name}: {str(e)}")
            raise
