#!/usr/bin/env python3
"""
Simple test for DuckDB functionality.
"""

import duckdb


def test_duckdb() -> bool:
    """Test basic DuckDB functionality."""
    print("Testing basic DuckDB functionality...")
    
    # Create a DuckDB connection
    conn = duckdb.connect(":memory:")
    
    # Create a test DataFrame
    df = conn.execute("CREATE TABLE temp_table AS SELECT 1 as id, 'test' as name").fetchdf()
    
    print("Created test DataFrame:")
    print(df.head())
    
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