"""Bronze layer for Google Drive Data Pipeline."""

from .metadata import MetadataManager
from .processor import BronzeProcessor

__all__ = ["BronzeProcessor", "MetadataManager"] 