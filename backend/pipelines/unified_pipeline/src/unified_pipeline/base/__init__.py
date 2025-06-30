"""Base classes for DuckDB-based pipeline processing."""

from .duckdb_processor import DuckDBProcessor
from .bronze_base import BronzeBase
from .silver_base import SilverBase
from .gold_base import GoldBase

__all__ = ['DuckDBProcessor', 'BronzeBase', 'SilverBase', 'GoldBase']
