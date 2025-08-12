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
from silver.duckdb_helper import DuckDBHelper
from utils.logging import get_logger

logger = get_logger()


def test_duckdb_helper() -> bool:
    """Test the DuckDBHelper class."""
    print("Testing DuckDBHelper class...")
    
    # Create a DuckDBHelper instance (using in-memory database)
    helper = DuckDBHelper()
    
    # Create a test DataFrame
    df = self.conn.execute("CREATE TABLE temp_table AS SELECT ...")
    
    print("Created test DataFrame:")
    print(df.head())
    
    # Convert DataFrame to Ibis table
    table_name = "test_table"
    table = helper.dataframe_to_ibis(df, table_name)
    print(f"Converted DataFrame to Ibis table: {table_name}")
    
    # Get schema
    schema = helper.get_schema(table)
    print("Table schema:")
    for col, dtype in schema.items():
        print(f"  {col}: {dtype}")
    
    # Cast column types
    type_mapping = {'value': 'int32'}
    table = helper.cast_column_types(table, type_mapping)
    
    # Convert back to DataFrame
    result_df = helper.ibis_to_dataframe(table)
    print("Result DataFrame after casting:")
    print(result_df.head())
    print(f"Value column dtype: {result_df['value'].dtype}")
    
    # Create a directory for test output
    output_dir = project_root / "test_output"
    output_dir.mkdir(exist_ok=True)
    
    # Save to Parquet
    output_path = output_dir / "test_table.parquet"
    helper.save_to_parquet(table, output_path)
    print(f"Saved table to Parquet: {output_path}")
    
    # Close the connection
    helper.close()
    
    print("DuckDBHelper test completed successfully!")
    return True


if __name__ == "__main__":
    test_duckdb_helper() 