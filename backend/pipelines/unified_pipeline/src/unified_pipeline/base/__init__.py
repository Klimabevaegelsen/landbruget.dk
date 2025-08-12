"""Base classes for DuckDB-based pipeline processing."""

from .bronze_base import BronzeBase
from .duckdb_processor import DuckDBProcessor
from .gold_base import GoldBase
from .silver_base import SilverBase

__all__ = ['DuckDBProcessor', 'BronzeBase', 'SilverBase', 'GoldBase']
