"""Base transformer for Silver layer."""

import abc
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

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

    def transform_from_content(
        self,
        file_content: bytes,
        filename: str,
        metadata_dict: dict,
    ) -> pd.DataFrame | None:
        """Transform file content directly from memory.

        Args:
            file_content: Raw file content in bytes
            filename: Original filename
            metadata_dict: File metadata dictionary

        Returns:
            Transformed DataFrame or None if transformation failed
        """
        # Default implementation: write to temp file and use transform method
        import os
        import tempfile

        from ...bronze.metadata import FileMetadata

        try:
            # Create a temporary file
            with tempfile.NamedTemporaryFile(
                suffix=Path(filename).suffix, delete=False
            ) as temp_file:
                temp_file.write(file_content)
                temp_file.flush()
                temp_path = Path(temp_file.name)

            # Create metadata object
            metadata = FileMetadata(**metadata_dict)

            # Create temporary output directory
            with tempfile.TemporaryDirectory() as temp_output_dir:
                # Use the transform method
                result = self.transform(temp_path, metadata, Path(temp_output_dir))

                if result.success:
                    # Handle multiple output files (e.g., Excel with multiple sheets)
                    if result.output_path:
                        # Single output file
                        df = pd.read_parquet(result.output_path)
                        return df
                    elif result.metadata and "output_paths" in result.metadata:
                        # Multiple output files - combine them
                        output_paths = result.metadata["output_paths"]
                        if output_paths:
                            # Read and combine all output files
                            dfs = []
                            for path_str in output_paths:
                                path = Path(path_str)
                                if path.exists():
                                    df = pd.read_parquet(path)
                                    # Add sheet identifier column
                                    df["sheet_name"] = path.stem.split("_")[-1]
                                    dfs.append(df)

                            if dfs:
                                # Combine all sheets into one DataFrame
                                combined_df = pd.concat(dfs, ignore_index=True)
                                return combined_df

                    logger.error("Transform succeeded but no output files found")
                    return None
                else:
                    logger.error(f"Transform failed: {result.error}")
                    return None

        except Exception as e:
            logger.error(f"Failed to transform content for {filename}: {str(e)}")
            return None
        finally:
            # Clean up temporary file
            try:
                if temp_path.exists():
                    os.unlink(temp_path)
            except Exception:
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
