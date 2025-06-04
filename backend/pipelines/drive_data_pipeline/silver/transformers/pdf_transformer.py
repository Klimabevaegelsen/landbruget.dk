"""PDF transformer for Silver layer."""

from pathlib import Path

import pandas as pd
import tabula

from ...bronze.metadata import FileMetadata
from ...utils.logging import get_logger, set_context
from ..storage import SilverStorageManager
from .base import BaseTransformer, TransformResult

# Get logger
logger = get_logger()


class PDFTransformer(BaseTransformer):
    """Transformer for PDF files."""

    def transform(
        self,
        file_path: Path,
        metadata: FileMetadata,
        output_dir: Path,
    ) -> TransformResult:
        """Transform PDF file to Parquet format.

        Args:
            file_path: Path to the PDF file
            metadata: Metadata for the file
            output_dir: Directory to save the transformed file

        Returns:
            TransformResult with the result of the transformation
        """
        try:
            set_context(
                file_id=metadata.file_id,
                file_name=metadata.original_filename,
            )
            logger.info(f"Transforming PDF file: {file_path}")
            
            # Extract tables from PDF
            tables = self._extract_tables(file_path)
            if not tables:
                return TransformResult(
                    success=False,
                    error="PDF file has no extractable tables",
                )
            
            # Create output directory for this file
            storage_manager = SilverStorageManager(
                storage_manager=None,  # Not needed for this operation
                base_path=output_dir,
            )
            file_output_dir = storage_manager.create_output_directory(
                run_dir=output_dir,
                source_subfolder=metadata.original_subfolder,
                content_type="PDF",
            )
            
            # Process each table
            output_paths = []
            total_rows = 0
            
            for i, df in enumerate(tables):
                # Clean column names
                df.columns = self._standardize_column_names(df.columns.tolist())
                
                # Apply data type standardization
                df = self._standardize_data_types(df)
                
                # Generate output filename
                base_filename = Path(metadata.original_filename).stem
                table_filename = f"{base_filename}_table_{i+1}"
                
                # Save as Parquet
                output_path = storage_manager.save_parquet(
                    df=df,
                    output_dir=file_output_dir,
                    filename=table_filename,
                )
                
                output_paths.append(output_path)
                total_rows += len(df)
            
            # Create schema dictionary from the last table
            schema = self._create_schema_dict(df)
            
            return TransformResult(
                success=True,
                output_path=output_paths[0] if len(output_paths) == 1 else None,
                row_count=total_rows,
                schema=schema,
                metadata={
                    "table_count": len(tables),
                    "output_paths": [str(p) for p in output_paths],
                }
            )
        
        except Exception as e:
            error_msg = f"Failed to transform PDF file {file_path}: {str(e)}"
            logger.error(error_msg)
            return TransformResult(
                success=False,
                error=error_msg,
            )

    def _extract_tables(self, file_path: Path) -> list[pd.DataFrame]:
        """Extract tables from PDF file.

        Args:
            file_path: Path to the PDF file

        Returns:
            List of dataframes, each representing a table
        """
        try:
            logger.debug(f"Extracting tables from PDF: {file_path}")
            
            # Use tabula-py to extract tables
            # This extracts all tables from all pages
            tables = tabula.read_pdf(
                str(file_path),
                pages='all',
                multiple_tables=True,
                guess=True,
            )
            
            # Filter out empty tables
            tables = [df for df in tables if not df.empty]
            
            logger.info(f"Extracted {len(tables)} tables from {file_path}")
            return tables
        
        except Exception as e:
            logger.error(f"Failed to extract tables from PDF {file_path}: {str(e)}")
            return []

    def _standardize_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize data types in the dataframe.

        Args:
            df: Pandas dataframe

        Returns:
            Dataframe with standardized data types
        """
        # Create a copy to avoid modifying the original
        df_clean = df.copy()
        
        for col in df_clean.columns:
            # Handle date columns
            if df_clean[col].dtype == 'object':
                # Try to convert to datetime if it looks like a date
                try:
                    if df_clean[col].str.contains(r'\d{2}[/.-]\d{2}[/.-]\d{4}').any() or \
                       df_clean[col].str.contains(r'\d{4}[/.-]\d{2}[/.-]\d{2}').any():
                        df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
                except:
                    pass
            
            # Handle boolean columns (yes/no, true/false)
            if df_clean[col].dtype == 'object':
                try:
                    # Check for boolean-like values
                    bool_map = {'yes': 1, 'no': 0, 'true': 1, 'false': 0}
                    if df_clean[col].str.lower().isin(bool_map.keys()).all():
                        df_clean[col] = df_clean[col].str.lower().map(bool_map)
                except:
                    pass
            
            # Handle numeric columns that may be strings
            if df_clean[col].dtype == 'object':
                try:
                    # Try to convert strings with numbers to numeric
                    df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
                except:
                    pass
        
        return df_clean 