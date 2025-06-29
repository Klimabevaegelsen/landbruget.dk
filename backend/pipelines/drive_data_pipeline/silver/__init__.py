"""Silver layer module for Google Drive data pipeline."""

from .duckdb_base import DuckDBProcessor
from .parquet_manager import ParquetManager
from .processor import SilverProcessor
from .storage import SilverStorageManager

__all__ = [
    "SilverProcessor",
    "SilverStorageManager",
    "DuckDBProcessor",
    "ParquetManager",
]
