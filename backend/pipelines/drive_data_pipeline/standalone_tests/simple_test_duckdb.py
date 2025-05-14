#!/usr/bin/env python3
"""
Simple test for DuckDB functionality.
"""

import duckdb
import pandas as pd


def test_duckdb():
    """Test basic DuckDB functionality."""
    print("Testing basic DuckDB functionality...")
    
    # Create a test DataFrame
    df = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'value': [10.5, 20.1, 30.7, 40.2, 50.9]
    })
    
    print("Created test DataFrame:")
    print(df.head())
    
    # Create a DuckDB connection
    conn = duckdb.connect(":memory:")
    
    # Register the DataFrame with DuckDB
    conn.register("test_df", df)
    
    # Run a SQL query
    result = conn.execute("SELECT * FROM test_df WHERE value > 30").fetchdf()
    
    print("Query result (value > 30):")
    print(result)
    
    # Calculate average
    avg_value = conn.execute("SELECT AVG(value) AS avg_value FROM test_df").fetchone()[0]
    print(f"Average value: {avg_value}")
    
    # Cast value to integer
    result_int = conn.execute("SELECT id, name, CAST(value AS INTEGER) AS value_int FROM test_df").fetchdf()
    print("Values cast to integer:")
    print(result_int)
    
    # Close the connection
    conn.close()
    
    print("DuckDB test completed successfully!")
    return True


if __name__ == "__main__":
    test_duckdb() 