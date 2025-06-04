#!/usr/bin/env python3
"""
Direct test for the DuckDBHelper class without going through __init__.py.
"""

# Standard library imports
import sys
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Third-party imports
import duckdb
import ibis
import pandas as pd

# Import the logger directly
from utils.logging import get_logger

logger = get_logger()


class DuckDBHelper:
    """Helper class for DuckDB and Ibis operations."""
    
    def __init__(self, database_path=None):
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
        
        # Initialize Ibis connection - use the path string, not the connection object
        self.ibis_conn = ibis.duckdb.connect(self.db_path)
        
    def dataframe_to_ibis(self, df, table_name):
        """Convert a pandas DataFrame to an Ibis table."""
        try:
            # Register the DataFrame with DuckDB
            self.conn.register(table_name, df)
            
            # Create a table in DuckDB from the DataFrame
            self.conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
            
            # Get the table as an Ibis expression
            table = self.ibis_conn.table(table_name)
            
            logger.debug(f"Converted DataFrame to Ibis table: {table_name}")
            return table
        
        except Exception as e:
            logger.error(f"Failed to convert DataFrame to Ibis table: {str(e)}")
            raise
    
    def ibis_to_dataframe(self, table):
        """Convert an Ibis table to pandas DataFrame."""
        try:
            df = table.execute()
            logger.debug("Converted Ibis table to DataFrame")
            return df
        
        except Exception as e:
            logger.error(f"Failed to convert Ibis table to DataFrame: {str(e)}")
            raise
    
    def save_to_parquet(self, table, output_path, compression="snappy"):
        """Save an Ibis table to Parquet format."""
        try:
            # Execute the Ibis expression and convert to a DataFrame
            df = self.ibis_to_dataframe(table)
            
            # Ensure parent directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Use pandas to save to Parquet (simplified for test)
            df.to_parquet(output_path, compression=compression)
            
            logger.info(f"Saved table to Parquet: {output_path}")
            return output_path
        
        except Exception as e:
            logger.error(f"Failed to save table to Parquet: {str(e)}")
            raise
    
    def get_schema(self, table):
        """Get the schema of an Ibis table."""
        try:
            schema = {}
            for col_name, dtype in zip(table.columns, table.dtypes, strict=False):
                schema[col_name] = str(dtype)
            
            logger.debug(f"Retrieved schema with {len(schema)} columns")
            return schema
        
        except Exception as e:
            logger.error(f"Failed to get schema: {str(e)}")
            raise
    
    def cast_column_types(self, table, type_mapping):
        """Cast columns to specified types."""
        try:
            for col_name, dtype in type_mapping.items():
                if col_name in table.columns:
                    # Cast the column to the specified type
                    table = table.mutate(
                        **{col_name: table[col_name].cast(dtype)}
                    )
            
            logger.debug(f"Cast {len(type_mapping)} columns to specified types")
            return table
        
        except Exception as e:
            logger.error(f"Failed to cast column types: {str(e)}")
            raise
    
    def close(self):
        """Close the DuckDB connection."""
        try:
            self.conn.close()
            logger.info("Closed DuckDB connection")
        except Exception as e:
            logger.error(f"Error closing DuckDB connection: {str(e)}")
            raise 


def test_duckdb_helper():
    """Test the DuckDBHelper class."""
    print("Testing DuckDBHelper class...")
    
    # Create a DuckDBHelper instance (using in-memory database)
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