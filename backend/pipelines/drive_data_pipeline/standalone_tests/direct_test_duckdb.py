#!/usr/bin/env python3
"""
Direct test for the DuckDBHelper class without going through __init__.py.
"""

# Standard library imports
import sys
from pathlib import Path
from typing import Any

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Third-party imports
import duckdb  # noqa: E402

# Import the logger directly
from utils.logging import get_logger  # noqa: E402

logger = get_logger()


class DuckDBHelper:
    """Helper class for DuckDB operations."""

    def __init__(self, database_path: Path | str | None = None) -> None:
        """Initialize the DuckDB helper."""
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

    def register_dataframe(self, df: Any, table_name: str) -> str:
        """Register a pandas DataFrame as a DuckDB table."""
        try:
            # Register the DataFrame with DuckDB
            self.conn.register(table_name, df)

            logger.debug(f"Registered DataFrame as table: {table_name}")
            return table_name

        except Exception as e:
            logger.error(f"Failed to register DataFrame as table: {e!s}")
            raise

    def table_to_dataframe(self, table_name: str) -> Any:
        """Convert a DuckDB table to pandas DataFrame."""
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
        """Save a DuckDB table to Parquet format."""
        try:
            # Ensure parent directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)

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
        """Get the schema of a DuckDB table."""
        try:
            result = self.conn.sql(f"DESCRIBE {table_name}").fetchall()
            schema = {row[0]: row[1] for row in result}

            logger.debug(f"Retrieved schema with {len(schema)} columns")
            return schema

        except Exception as e:
            logger.error(f"Failed to get schema: {e!s}")
            raise

    def cast_column_types(self, table_name: str, type_mapping: dict[str, str]) -> str:
        """Cast columns to specified types by creating a new table."""
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


def test_duckdb_helper() -> bool:
    """Test the DuckDBHelper class."""
    print("Testing DuckDBHelper class...")

    # Create a DuckDBHelper instance (using in-memory database)
    helper = DuckDBHelper()

    # Create a test DataFrame using pandas
    import pandas as pd

    df = pd.DataFrame(
        {"id": [1, 2, 3], "name": ["test1", "test2", "test3"], "value": [10.5, 20.5, 30.5]}
    )

    print("Created test DataFrame:")
    print(df.head())

    # Register DataFrame as DuckDB table
    table_name = "test_table"
    result_name = helper.register_dataframe(df, table_name)
    print(f"Registered DataFrame as table: {result_name}")

    # Get schema
    schema = helper.get_schema(table_name)
    print("Table schema:")
    for col, dtype in schema.items():
        print(f"  {col}: {dtype}")

    # Cast column types
    type_mapping = {"value": "INTEGER"}
    helper.cast_column_types(table_name, type_mapping)

    # Convert back to DataFrame
    result_df = helper.table_to_dataframe(table_name)
    print("Result DataFrame after casting:")
    print(result_df.head())
    print(f"Value column dtype: {result_df['value'].dtype}")

    # Create a directory for test output
    output_dir = project_root / "test_output"
    output_dir.mkdir(exist_ok=True)

    # Save to Parquet
    output_path = output_dir / "test_table.parquet"
    helper.save_to_parquet(table_name, output_path)
    print(f"Saved table to Parquet: {output_path}")

    # Close the connection
    helper.close()

    print("DuckDBHelper test completed successfully!")
    return True


if __name__ == "__main__":
    test_duckdb_helper()
