"""
Bronze stage processing for BMD data extraction.
This stage is responsible for extracting raw data from the BMD portal.
"""

from .export import BMDScraper

__all__ = ["BMDScraper"]
