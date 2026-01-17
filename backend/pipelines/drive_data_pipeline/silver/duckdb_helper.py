"""DuckDB helper for Silver layer (pure DuckDB, no Ibis)."""

from pathlib import Path
from typing import Any

import duckdb

# pandas import needed for DataFrame type annotation
try:
    import pandas as pd

    DataFrame = pd.DataFrame
except ImportError:
    DataFrame = Any
from ..utils.logging import get_logger

# Get logger
logger = get_logger()


class DuckDBHelper:
    """Helper class for DuckDB operations."""

    def __init__(self, database_path: Path | None = None) -> None:
        """Initialize the DuckDB helper.

        Args:
            database_path: Path to the DuckDB database file (optional)
                           If None, an in-memory database will be used
        """
        # Create DuckDB connection
        if database_path:
            # Use file-based database
            self.db_path = str(database_path)
            self.conn = duckdb.connect(self.db_path)
            logger.info(f"Connected to DuckDB database: {database_path}")
        else:
            # Use in-memory database
            self.db_path = ":memory:"
            self.conn = duckdb.connect(self.db_path)
            logger.info("Connected to in-memory DuckDB database")

    def register_dataframe(self, df: DataFrame, table_name: str) -> str:
        """Register a pandas DataFrame as a DuckDB table.

        Args:
            df: pandas DataFrame
            table_name: Name for the table in DuckDB

        Returns:
            The table name string
        """
        try:
            # Register the DataFrame with DuckDB
            self.conn.register(table_name, df)

            logger.debug(f"Registered DataFrame as table: {table_name}")
            return table_name

        except Exception as e:
            logger.error(f"Failed to register DataFrame as table: {e!s}")
            raise

    def table_to_dataframe(self, table_name: str) -> Any:
        """Convert a DuckDB table to pandas DataFrame.

        Args:
            table_name: Name of the table in DuckDB

        Returns:
            pandas DataFrame
        """
        try:
            df = self.conn.sql(f"SELECT * FROM {table_name}").df()
            logger.debug(f"Converted table {table_name} to DataFrame")
            return df

        except Exception as e:
            logger.error(f"Failed to convert table to DataFrame: {e!s}")
            raise

    def save_to_parquet(
        self, table_name: str, output_path: Path, compression: str = "snappy"
    ) -> Path:
        """Save a DuckDB table to Parquet format.

        Args:
            table_name: Name of the table in DuckDB
            output_path: Path where the Parquet file will be saved
            compression: Compression algorithm (default: snappy)

        Returns:
            Path to the saved file
        """
        try:
            # Use DuckDB's COPY statement to write the Parquet file
            self.conn.execute(
                f"""
                COPY {table_name}
                TO '{output_path}' (FORMAT 'PARQUET', COMPRESSION '{compression}')
                """
            )

            logger.info(f"Saved table to Parquet: {output_path}")
            return output_path

        except Exception as e:
            logger.error(f"Failed to save table to Parquet: {e!s}")
            raise

    def get_schema(self, table_name: str) -> dict[str, str]:
        """Get the schema of a DuckDB table.

        Args:
            table_name: Name of the table in DuckDB

        Returns:
            Dictionary mapping column names to data types
        """
        try:
            result = self.conn.sql(f"DESCRIBE {table_name}").fetchall()
            schema = {row[0]: row[1] for row in result}

            logger.debug(f"Retrieved schema with {len(schema)} columns")
            return schema

        except Exception as e:
            logger.error(f"Failed to get schema: {e!s}")
            raise

    def cast_column_types(self, table_name: str, type_mapping: dict[str, str]) -> str:
        """Cast columns to specified types by creating a new table.

        Args:
            table_name: Name of the table in DuckDB
            type_mapping: Dictionary mapping column names to target DuckDB data types

        Returns:
            The table name (table is modified in place via replacement)
        """
        try:
            # Get current schema to know all columns
            current_schema = self.get_schema(table_name)

            # Build SELECT clause with casts
            select_columns = []
            for col_name in current_schema:
                if col_name in type_mapping:
                    target_type = type_mapping[col_name]
                    select_columns.append(f'CAST("{col_name}" AS {target_type}) AS "{col_name}"')
                else:
                    select_columns.append(f'"{col_name}"')

            select_clause = ", ".join(select_columns)

            # Create a new table with the casted columns and replace the old one
            temp_table = f"{table_name}_temp"
            self.conn.execute(
                f"CREATE TABLE {temp_table} AS SELECT {select_clause} FROM {table_name}"
            )
            self.conn.execute(f"DROP TABLE {table_name}")
            self.conn.execute(f"ALTER TABLE {temp_table} RENAME TO {table_name}")

            logger.debug(f"Cast {len(type_mapping)} columns to specified types")
            return table_name

        except Exception as e:
            logger.error(f"Failed to cast column types: {e!s}")
            raise

    def close(self) -> None:
        """Close the DuckDB connection."""
        try:
            self.conn.close()
            logger.info("Closed DuckDB connection")
        except Exception as e:
            logger.error(f"Error closing DuckDB connection: {e!s}")
            raise
