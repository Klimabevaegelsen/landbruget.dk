"""DuckDB and Ibis integration for Silver layer."""

from pathlib import Path

import duckdb
import ibis
import pandas as pd

from ..utils.logging import get_logger

# Get logger
logger = get_logger()


class DuckDBHelper:
    """Helper class for DuckDB and Ibis operations."""
    
    def __init__(self, database_path: Path | None = None):
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
        
        # Initialize Ibis connection - use the path string, not the connection object
        self.ibis_conn = ibis.duckdb.connect(self.db_path)
        
    def dataframe_to_ibis(self, df: pd.DataFrame, table_name: str) -> ibis.expr.types.Table:
        """Convert a pandas DataFrame to an Ibis table.
        
        Args:
            df: pandas DataFrame
            table_name: Name for the table in DuckDB
            
        Returns:
            Ibis Table object
        """
        try:
            # Register the DataFrame with DuckDB
            self.conn.register(table_name, df)
            
            # Get the table as an Ibis expression
            table = self.ibis_conn.table(table_name)
            
            logger.debug(f"Converted DataFrame to Ibis table: {table_name}")
            return table
        
        except Exception as e:
            logger.error(f"Failed to convert DataFrame to Ibis table: {str(e)}")
            raise
    
    def ibis_to_dataframe(self, table: ibis.expr.types.Table) -> pd.DataFrame:
        """Convert an Ibis table to pandas DataFrame.
        
        Args:
            table: Ibis Table object
            
        Returns:
            pandas DataFrame
        """
        try:
            df = table.execute()
            logger.debug("Converted Ibis table to DataFrame")
            return df
        
        except Exception as e:
            logger.error(f"Failed to convert Ibis table to DataFrame: {str(e)}")
            raise
    
    def save_to_parquet(
        self, table: ibis.expr.types.Table, output_path: Path, compression: str = "snappy"
    ) -> Path:
        """Save an Ibis table to Parquet format.
        
        Args:
            table: Ibis Table object
            output_path: Path where the Parquet file will be saved
            compression: Compression algorithm (default: snappy)
            
        Returns:
            Path to the saved file
        """
        try:
            # Execute the Ibis expression and convert to a DataFrame
            df = self.ibis_to_dataframe(table)
            
            # Use DuckDB's COPY statement to write the Parquet file
            # This performs better than pandas to_parquet for large datasets
            self.conn.execute(
                f"""
                COPY (SELECT * FROM df)
                TO '{output_path}' (FORMAT 'PARQUET', COMPRESSION '{compression}')
                """
            )
            
            logger.info(f"Saved table to Parquet: {output_path}")
            return output_path
        
        except Exception as e:
            logger.error(f"Failed to save table to Parquet: {str(e)}")
            raise
    
    def get_schema(self, table: ibis.expr.types.Table) -> dict[str, str]:
        """Get the schema of an Ibis table.
        
        Args:
            table: Ibis Table object
            
        Returns:
            Dictionary mapping column names to data types
        """
        try:
            schema = {}
            for col_name, dtype in zip(table.columns, table.dtypes, strict=False):
                schema[col_name] = str(dtype)
            
            logger.debug(f"Retrieved schema with {len(schema)} columns")
            return schema
        
        except Exception as e:
            logger.error(f"Failed to get schema: {str(e)}")
            raise
    
    def cast_column_types(
        self, table: ibis.expr.types.Table, type_mapping: dict[str, str]
    ) -> ibis.expr.types.Table:
        """Cast columns to specified types.
        
        Args:
            table: Ibis Table object
            type_mapping: Dictionary mapping column names to target data types
            
        Returns:
            Ibis Table with recast columns
        """
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