"""Base class for DuckDB operations in migrated pipelines."""

import duckdb
import time
from pathlib import Path
from typing import Optional, Union, List, Dict, Any

class DuckDBProcessor:
    """Base class for DuckDB-based data processing."""
    
    def __init__(self, db_path: str = ":memory:", dataset_name: str = "data"):
        self.conn = duckdb.connect(db_path)
        self.dataset_name = dataset_name
        self._setup_extensions()
    
    def _setup_extensions(self):
        """Setup required DuckDB extensions."""
        try:
            self.conn.execute("INSTALL spatial")
            self.conn.execute("LOAD spatial")
        except Exception as e:
            print(f"Warning: Could not load spatial extension: {e}")
    
    def create_table_from_parquet(self, parquet_path: Union[str, Path], table_name: Optional[str] = None) -> str:
        """Create a table from parquet file."""
        if table_name is None:
            table_name = f"{self.dataset_name}_{int(time.time())}"
        
        self.conn.execute(f"""
            CREATE TABLE {table_name} AS 
            SELECT * FROM read_parquet('{parquet_path}')
        """)
        return table_name
    
    def create_table_from_csv(self, csv_path: Union[str, Path], table_name: Optional[str] = None) -> str:
        """Create a table from CSV file."""
        if table_name is None:
            table_name = f"{self.dataset_name}_{int(time.time())}"
        
        self.conn.execute(f"""
            CREATE TABLE {table_name} AS 
            SELECT * FROM read_csv('{csv_path}', AUTO_DETECT=TRUE, HEADER=TRUE)
        """)
        return table_name
    
    def create_spatial_table(self, geospatial_path: Union[str, Path], table_name: Optional[str] = None) -> str:
        """Create a table from geospatial file."""
        if table_name is None:
            table_name = f"{self.dataset_name}_geo_{int(time.time())}"
        
        self.conn.execute(f"""
            CREATE TABLE {table_name} AS 
            SELECT * FROM ST_Read('{geospatial_path}')
        """)
        return table_name
    
    def save_table_to_parquet(self, table_name: str, output_path: Union[str, Path]):
        """Save table to parquet file."""
        self.conn.execute(f"""
            COPY {table_name} TO '{output_path}' (FORMAT PARQUET)
        """)
    
    def save_table_to_csv(self, table_name: str, output_path: Union[str, Path]):
        """Save table to CSV file."""
        self.conn.execute(f"""
            COPY {table_name} TO '{output_path}' (FORMAT CSV, HEADER)
        """)
    
    def get_table_info(self, table_name: str) -> List[Dict[str, Any]]:
        """Get information about a table."""
        return self.conn.execute(f"DESCRIBE {table_name}").fetchall()
    
    def execute_query(self, query: str) -> List[Any]:
        """Execute a query and return results."""
        return self.conn.execute(query).fetchall()
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
