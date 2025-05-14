#!/usr/bin/env python3
"""
Test script for the DuckDBHelper class.
This script tests the DuckDBHelper class to ensure it works properly.
"""

import sys
from pathlib import Path

import pandas as pd

# Add the parent directory to sys.path
sys.path.append(str(Path(__file__).parent))

from drive_data_pipeline.silver.duckdb_helper import DuckDBHelper
from drive_data_pipeline.utils.logging import get_logger

# Get logger
logger = get_logger()


def test_duckdb_helper():
    """Test the DuckDBHelper class."""
    print("Testing DuckDBHelper class...")
    
    # Create a DuckDBHelper instance
    helper = DuckDBHelper()
    
    # Create a test DataFrame
    df = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'value': [10.5, 20.1, 30.7, 40.2, 50.9]
    })
    
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
    output_dir = Path("./test_output")
    output_dir.mkdir(exist_ok=True)
    
    # Save to Parquet
    output_path = output_dir / "test_table.parquet"
    helper.save_to_parquet(table, output_path)
    print(f"Saved table to Parquet: {output_path}")
    
    # Close the connection
    helper.close()
    
    print("DuckDBHelper test completed successfully!")


if __name__ == "__main__":
    test_duckdb_helper() 