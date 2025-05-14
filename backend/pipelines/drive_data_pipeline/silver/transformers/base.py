"""Base transformer for Silver layer."""

import abc
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ...bronze.metadata import FileMetadata
from ...utils.logging import get_logger

# Get logger
logger = get_logger()


@dataclass
class TransformResult:
    """Result of a transformation operation."""

    success: bool
    output_path: Path | None = None
    error: str | None = None
    row_count: int | None = None
    schema: dict[str, str] | None = None
    metadata: dict[str, Any] | None = None


class BaseTransformer(abc.ABC):
    """Base class for all transformers."""

    def __init__(self):
        """Initialize the transformer."""
        logger.debug(f"Initialized {self.__class__.__name__}")
    
    @abc.abstractmethod
    def transform(
        self,
        file_path: Path,
        metadata: FileMetadata,
        output_dir: Path,
    ) -> TransformResult:
        """Transform the file from Bronze to Silver format.

        Args:
            file_path: Path to the file in Bronze layer
            metadata: Metadata for the file
            output_dir: Directory to save the transformed file

        Returns:
            TransformResult with the result of the transformation
        """
        pass
    
    def _standardize_column_names(self, columns: list[str]) -> list[str]:
        """Standardize column names according to project conventions.

        Args:
            columns: Original column names

        Returns:
            Standardized column names
        """
        standardized = []
        for col in columns:
            # Convert to lowercase
            col = col.lower()
            
            # Replace spaces and special chars with underscores
            col = col.replace(" ", "_")
            
            # Replace multiple underscores with a single one
            while "__" in col:
                col = col.replace("__", "_")
            
            # Replace special characters (æ, ø, å)
            col = col.replace("æ", "ae")
            col = col.replace("ø", "oe")
            col = col.replace("å", "aa")
            
            # Remove other special characters
            col = "".join(c if c.isalnum() or c == "_" else "_" for c in col)
            
            # Remove leading/trailing underscores
            col = col.strip("_")
            
            # Ensure the name is not empty
            if not col:
                col = "column"
            
            standardized.append(col)
        
        return standardized
    
    def _create_schema_dict(self, df: Any) -> dict[str, str]:
        """Create a schema dictionary from a dataframe.

        Args:
            df: DuckDB/Ibis dataframe

        Returns:
            Dictionary mapping column names to data types
        """
        # This is a placeholder - actual implementation will depend on
        # whether we're using DuckDB, Ibis, or another library
        schema = {}
        
        # Example implementation if using Ibis
        # for col in df.columns:
        #     dtype = str(df[col].type())
        #     schema[col] = dtype
        
        return schema 