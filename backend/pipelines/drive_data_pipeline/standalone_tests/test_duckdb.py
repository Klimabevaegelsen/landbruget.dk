#!/usr/bin/env python3
"""
Standalone test for the DuckDBHelper class.
"""

# Standard library imports
import sys
from pathlib import Path

# Third-party imports

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Direct imports from the codebase
from silver.duckdb_helper import DuckDBHelper  # noqa: E402
from utils.logging import get_logger  # noqa: E402

logger = get_logger()


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
