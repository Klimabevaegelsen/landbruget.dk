"""Advanced PDF transformer with enhanced extraction capabilities."""

from pathlib import Path

import numpy as np
import pandas as pd
import pdfplumber
import pytesseract
import tabula
from pdf2image import convert_from_path

from ...utils.logging import get_logger
from .pdf_transformer import PDFTransformer

# Get logger
logger = get_logger()


class AdvancedPDFTransformer(PDFTransformer):
    """Advanced transformer for PDF files with enhanced extraction capabilities."""

    def __init__(
        self,
        use_ocr: bool = False,
        ocr_language: str = "eng",
        min_table_size: int = 3,
        extraction_methods: list[str] = None,
    ):
        """Initialize the advanced PDF transformer.

        Args:
            use_ocr: Whether to use OCR for scanned PDFs
            ocr_language: Language for OCR (e.g., 'eng', 'dan', 'eng+dan')
            min_table_size: Minimum number of rows/columns to consider a table
            extraction_methods: List of extraction methods to try, in order
                                (default: ['tabula', 'pdfplumber', 'ocr'])
        """
        super().__init__()
        self.use_ocr = use_ocr
        self.ocr_language = ocr_language
        self.min_table_size = min_table_size
        self.extraction_methods = extraction_methods or ["tabula", "pdfplumber", "ocr"]
        logger.info(f"Initialized AdvancedPDFTransformer with {len(self.extraction_methods)} methods")

    def _extract_tables(self, file_path: Path) -> list[pd.DataFrame]:
        """Extract tables from PDF file using multiple methods.

        Args:
            file_path: Path to the PDF file

        Returns:
            List of dataframes, each representing a table
        """
        all_tables = []
        
        logger.debug(f"Extracting tables from PDF: {file_path}")
        
        # Try each extraction method in order
        for method in self.extraction_methods:
            try:
                if method == "tabula":
                    tables = self._extract_with_tabula(file_path)
                elif method == "pdfplumber":
                    tables = self._extract_with_pdfplumber(file_path)
                elif method == "ocr" and self.use_ocr:
                    tables = self._extract_with_ocr(file_path)
                else:
                    continue
                
                # Add tables to results if they're not empty
                for table in tables:
                    if not table.empty and not self._is_duplicate_table(table, all_tables):
                        all_tables.append(table)
                
                logger.debug(f"Extracted {len(tables)} tables using {method}")
                
                # If we found tables, we might not need to try other methods
                if len(all_tables) > 0 and method != "pdfplumber":
                    break
                    
            except Exception as e:
                logger.warning(f"Failed to extract tables with {method}: {str(e)}")
        
        logger.info(f"Extracted {len(all_tables)} total tables from {file_path}")
        return all_tables

    def _extract_with_tabula(self, file_path: Path) -> list[pd.DataFrame]:
        """Extract tables using tabula-py.

        Args:
            file_path: Path to the PDF file

        Returns:
            List of dataframes
        """
        try:
            # Try different tabula modes for better extraction
            # Lattice mode is good for tables with lines/borders
            lattice_tables = tabula.read_pdf(
                str(file_path),
                pages="all",
                multiple_tables=True,
                lattice=True,
            )
            
            # Stream mode is good for tables without clear borders
            stream_tables = tabula.read_pdf(
                str(file_path),
                pages="all",
                multiple_tables=True,
                stream=True,
            )
            
            # Combine results, filter empty tables
            tables = lattice_tables + stream_tables
            tables = [df for df in tables if not df.empty]
            
            # Filter small tables that might be noise
            tables = [
                df for df in tables
                if df.shape[0] >= self.min_table_size and df.shape[1] >= 2
            ]
            
            # Clean up tables
            for i in range(len(tables)):
                # Drop completely empty rows and columns
                tables[i] = tables[i].dropna(how="all").dropna(axis=1, how="all")
                
                # Attempt to use first row as header if it looks like one
                if tables[i].shape[0] > 1:
                    first_row = tables[i].iloc[0]
                    rest_rows = tables[i].iloc[1:]
                    if not first_row.equals(rest_rows.iloc[0]):
                        # First row appears to be a header
                        tables[i].columns = first_row
                        tables[i] = tables[i].iloc[1:].reset_index(drop=True)
            
            return tables
        
        except Exception as e:
            logger.warning(f"Tabula extraction failed: {str(e)}")
            return []

    def _extract_with_pdfplumber(self, file_path: Path) -> list[pd.DataFrame]:
        """Extract tables using pdfplumber, which works better for complex layouts.

        Args:
            file_path: Path to the PDF file

        Returns:
            List of dataframes
        """
        try:
            all_tables = []
            
            with pdfplumber.open(file_path) as pdf:
                for page_num, page in enumerate(pdf.pages):
                    # Extract tables from the page
                    tables = page.extract_tables()
                    
                    for table in tables:
                        if table and len(table) >= self.min_table_size:
                            # Convert to DataFrame
                            df = pd.DataFrame(table[1:], columns=table[0])
                            
                            # Clean up the table
                            df = df.replace(r'^\s*$', np.nan, regex=True)
                            df = df.dropna(how="all").dropna(axis=1, how="all")
                            
                            if not df.empty:
                                all_tables.append(df)
            
            return all_tables
        
        except Exception as e:
            logger.warning(f"PDFPlumber extraction failed: {str(e)}")
            return []

    def _extract_with_ocr(self, file_path: Path) -> list[pd.DataFrame]:
        """Extract tables using OCR for scanned PDFs.

        Args:
            file_path: Path to the PDF file

        Returns:
            List of dataframes
        """
        if not self.use_ocr:
            return []
            
        try:
            all_tables = []
            
            # Convert PDF to images
            images = convert_from_path(file_path)
            
            for i, image in enumerate(images):
                # Perform OCR
                text = pytesseract.image_to_string(
                    image, lang=self.ocr_language, config="--psm 6"
                )
                
                # Perform OCR with table detection
                tables_data = pytesseract.image_to_data(
                    image, lang=self.ocr_language, config="--psm 6", output_type="data.frame"
                )
                
                # This is a simplified approach - real implementation would need
                # more complex logic to reconstruct tables from OCR output
                
                # For simplicity, use tabula to extract tables from the image
                # Convert image to temporary PDF
                temp_image_path = f"{file_path.stem}_page_{i}.png"
                image.save(temp_image_path)
                
                try:
                    # Use tabula on the image (will only work if the image has clear tables)
                    temp_tables = tabula.read_pdf(
                        temp_image_path,
                        pages="1",
                        multiple_tables=True,
                        guess=True,
                    )
                    
                    for table in temp_tables:
                        if not table.empty:
                            all_tables.append(table)
                    
                except Exception as e:
                    logger.warning(f"OCR table extraction failed for page {i}: {str(e)}")
                
                finally:
                    # Clean up temporary file
                    Path(temp_image_path).unlink(missing_ok=True)
            
            return all_tables
        
        except Exception as e:
            logger.warning(f"OCR extraction failed: {str(e)}")
            return []

    def _is_duplicate_table(self, new_table: pd.DataFrame, existing_tables: list[pd.DataFrame]) -> bool:
        """Check if a table is a duplicate of an existing table.

        Args:
            new_table: Table to check
            existing_tables: List of existing tables

        Returns:
            True if the table is likely a duplicate
        """
        if not existing_tables:
            return False
            
        # Check for exact duplicates
        for table in existing_tables:
            # If dimensions match, check for content similarity
            if table.shape == new_table.shape:
                # Check if columns are similar
                if set(table.columns) == set(new_table.columns):
                    # Sample a few rows to compare
                    sample_size = min(5, table.shape[0])
                    if table.shape[0] > 0 and new_table.shape[0] > 0:
                        sample_rows = min(table.shape[0], new_table.shape[0])
                        if np.array_equal(
                            table.iloc[:sample_rows, :].values, 
                            new_table.iloc[:sample_rows, :].values
                        ):
                            return True
        
        return False 