"""Base class for bronze layer processing."""

from .duckdb_processor import DuckDBProcessor
from typing import Union, Optional
from pathlib import Path

class BronzeBase(DuckDBProcessor):
    """Base class for bronze layer data ingestion."""
    
    def __init__(self, dataset_name: str, db_path: str = ":memory:"):
        super().__init__(db_path, dataset_name)
    
    def ingest_data(self, input_path: Union[str, Path], **kwargs) -> str:
        """
        Ingest raw data into bronze layer.
        
        Args:
            input_path: Path to input data file
            **kwargs: Additional processing parameters
            
        Returns:
            str: Name of created bronze table
        """
        raise NotImplementedError("Subclasses must implement ingest_data method")
    
    def validate_bronze_data(self, table_name: str) -> bool:
        """
        Validate bronze layer data quality.
        
        Args:
            table_name: Name of bronze table to validate
            
        Returns:
            bool: True if validation passes
        """
        try:
            # Basic validation - check if table exists and has data
            count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            return count > 0
        except Exception as e:
            print(f"Bronze validation failed: {e}")
            return False
