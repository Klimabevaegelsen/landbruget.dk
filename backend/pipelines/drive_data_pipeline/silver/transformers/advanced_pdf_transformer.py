"""Advanced PDF transformer with enhanced extraction capabilities.

Refactored to use vanilla DuckDB for table processing instead of pandas/numpy.
"""

from pathlib import Path
from typing import Any

import pdfplumber
import pytesseract
import tabula
from pdf2image import convert_from_path

# Handle imports for both standalone and package usage
try:
    from ...utils.logging import get_logger
except ImportError:
    # Fallback for standalone usage
    import logging

    from common.logging_utils import get_pipeline_logger

    def get_logger() -> logging.Logger:
        return get_pipeline_logger(__name__)


from .pdf_transformer import PDFTransformer

# Get logger
logger = get_logger()


class AdvancedPDFTransformer(PDFTransformer):
    """Advanced transformer for PDF files with enhanced extraction capabilities using DuckDB."""

    def __init__(
        self,
        use_ocr: bool = False,
        ocr_language: str = "eng",
        min_table_size: int = 3,
        extraction_methods: list[str] | None = None,
    ) -> None:
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
        logger.info(
            f"Initialized AdvancedPDFTransformer with {len(self.extraction_methods)} methods"
        )

    def _extract_tables(self, file_path: Path) -> list[Any]:
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
                    if (
                        table is not None
                        and not self._is_empty_table(table)
                        and not self._is_duplicate_table(table, all_tables)
                    ):
                        all_tables.append(table)

                logger.debug(f"Extracted {len(tables)} tables using {method}")

                # If we found tables, we might not need to try other methods
                if len(all_tables) > 0 and method != "pdfplumber":
                    break

            except Exception as e:
                logger.warning(f"Failed to extract tables with {method}: {e!s}")

        logger.info(f"Extracted {len(all_tables)} total tables from {file_path}")
        return all_tables

    def _is_empty_table(self, table: Any) -> bool:
        """Check if a table (DataFrame or DuckDB relation) is empty."""
        if table is None:
            return True
        # Handle pandas DataFrame
        if hasattr(table, "empty"):
            return table.empty
        # Handle DuckDB relation
        if hasattr(table, "fetchone"):
            return table.fetchone() is None
        return True

    def _extract_with_tabula(self, file_path: Path) -> list[Any]:
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

            # Filter small tables that might be noise using DuckDB
            filtered_tables = []
            for df in tables:
                if df.shape[0] >= self.min_table_size and df.shape[1] >= 2:
                    # Clean up table using DuckDB
                    cleaned_df = self._clean_table_with_duckdb(df)
                    if cleaned_df is not None and not cleaned_df.empty:
                        filtered_tables.append(cleaned_df)

            return filtered_tables

        except Exception as e:
            logger.warning(f"Tabula extraction failed: {e!s}")
            return []

    def _clean_table_with_duckdb(self, df: Any) -> Any:
        """Clean a table using DuckDB operations.

        Args:
            df: Input DataFrame

        Returns:
            Cleaned DataFrame
        """
        try:
            # Create a unique table name
            table_name = f"temp_clean_{id(df)}"

            # Register DataFrame with DuckDB
            self.conn.register("temp_df", df)

            # Get column names
            column_count = len(df.columns)
            [f"column{i}" for i in range(column_count)]

            # Create table with original data
            self.conn.execute(f"""
                CREATE TABLE {table_name} AS
                SELECT * FROM temp_df
            """)

            # Get actual column names
            col_info = self.conn.execute(f"DESCRIBE {table_name}").fetchall()
            actual_columns = [col[0] for col in col_info]

            # Build a WHERE clause to filter rows where ALL columns are NULL
            null_checks = " AND ".join([f'"{col}" IS NULL' for col in actual_columns])

            # Filter out completely empty rows using DuckDB
            self.conn.execute(f"""
                CREATE TABLE {table_name}_clean AS
                SELECT * FROM {table_name}
                WHERE NOT ({null_checks})
            """)

            # Convert back to DataFrame
            result = self.conn.execute(f"SELECT * FROM {table_name}_clean").df()

            # Check if first row looks like a header and handle it
            if len(result) > 1:
                first_row = result.iloc[0]
                # If first row values are different from second row, it might be a header
                # This is a heuristic - keep the pandas logic for this part as it's complex
                try:
                    if not first_row.equals(result.iloc[1]):
                        # First row appears to be a header
                        result.columns = first_row.values
                        result = result.iloc[1:].reset_index(drop=True)
                except Exception:
                    pass

            # Clean up temporary tables
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}_clean")
            self.conn.unregister("temp_df")

            return result

        except Exception as e:
            logger.warning(f"DuckDB table cleaning failed: {e!s}")
            return df  # Return original if cleaning fails

    def _extract_with_pdfplumber(self, file_path: Path) -> list[Any]:
        """Extract tables using pdfplumber, which works better for complex layouts.

        Args:
            file_path: Path to the PDF file

        Returns:
            List of dataframes
        """
        try:
            all_tables = []

            with pdfplumber.open(file_path) as pdf:
                for _page_num, page in enumerate(pdf.pages):
                    # Extract tables from the page
                    tables = page.extract_tables()

                    for table in tables:
                        if table and len(table) >= self.min_table_size:
                            # Process table using DuckDB
                            df = self._process_pdfplumber_table_with_duckdb(table)

                            if df is not None and not df.empty:
                                all_tables.append(df)

            return all_tables

        except Exception as e:
            logger.warning(f"PDFPlumber extraction failed: {e!s}")
            return []

    def _process_pdfplumber_table_with_duckdb(self, table: list[list[Any]]) -> Any:
        """Process a pdfplumber table using DuckDB.

        Args:
            table: Raw table from pdfplumber (list of lists)

        Returns:
            Cleaned DataFrame
        """
        try:
            if not table or len(table) < 2:
                return None

            # Get headers from first row
            headers = []
            for i, h in enumerate(table[0]):
                header = str(h).strip() if h else f"column_{i}"
                # Clean header name
                header = header.replace('"', "'").replace("\n", " ").replace("\r", " ")
                if not header or header.isspace():
                    header = f"column_{i}"
                headers.append(header)

            # Create unique table name
            table_name = f"pdfplumber_temp_{id(table)}"

            # Create table with all columns as VARCHAR
            columns_def = ", ".join([f'"{h}" VARCHAR' for h in headers])
            self.conn.execute(f"CREATE TABLE {table_name} ({columns_def})")

            # Insert data rows
            for row in table[1:]:
                # Skip completely empty rows
                if all(v is None or str(v).strip() == "" for v in row):
                    continue

                values = []
                for i, v in enumerate(row):
                    if i >= len(headers):
                        break
                    val = str(v) if v is not None else ""
                    # Clean value: replace empty/whitespace with empty string
                    val = val.strip() if val else ""
                    # Escape single quotes for SQL
                    val = val.replace("'", "''")
                    values.append(f"'{val}'")

                # Pad values if row is shorter than headers
                while len(values) < len(headers):
                    values.append("''")

                values_str = ", ".join(values)
                self.conn.execute(f"INSERT INTO {table_name} VALUES ({values_str})")

            # Filter out rows where all columns are empty
            null_checks = " AND ".join([f'("{h}" IS NULL OR TRIM("{h}") = \'\')' for h in headers])
            self.conn.execute(f"""
                CREATE TABLE {table_name}_clean AS
                SELECT * FROM {table_name}
                WHERE NOT ({null_checks})
            """)

            # Get column info for filtering empty columns
            col_info = self.conn.execute(f"DESCRIBE {table_name}_clean").fetchall()

            # Find non-empty columns
            non_empty_columns = []
            for col_name, *_ in col_info:
                count_result = self.conn.execute(f"""
                    SELECT COUNT(*) FROM {table_name}_clean
                    WHERE "{col_name}" IS NOT NULL AND TRIM("{col_name}") != ''
                """).fetchone()
                if count_result[0] > 0:
                    non_empty_columns.append(col_name)

            if non_empty_columns:
                # Select only non-empty columns
                cols_str = ", ".join([f'"{c}"' for c in non_empty_columns])
                result = self.conn.execute(f"SELECT {cols_str} FROM {table_name}_clean").df()
            else:
                result = self.conn.execute(f"SELECT * FROM {table_name}_clean").df()

            # Clean up
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}_clean")

            return result

        except Exception as e:
            logger.warning(f"Failed to process pdfplumber table with DuckDB: {e!s}")
            return None

    def _extract_with_ocr(self, file_path: Path) -> list[Any]:
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
                pytesseract.image_to_string(image, lang=self.ocr_language, config="--psm 6")

                # Perform OCR with table detection
                pytesseract.image_to_data(
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
                            # Clean the table using DuckDB
                            cleaned = self._clean_table_with_duckdb(table)
                            if cleaned is not None and not cleaned.empty:
                                all_tables.append(cleaned)

                except Exception as e:
                    logger.warning(f"OCR table extraction failed for page {i}: {e!s}")

                finally:
                    # Clean up temporary file
                    Path(temp_image_path).unlink(missing_ok=True)

            return all_tables

        except Exception as e:
            logger.warning(f"OCR extraction failed: {e!s}")
            return []

    def _is_duplicate_table(self, new_table: Any, existing_tables: list[Any]) -> bool:
        """Check if a table is a duplicate of an existing table using DuckDB.

        Args:
            new_table: Table to check (DataFrame)
            existing_tables: List of existing tables

        Returns:
            True if the table is likely a duplicate
        """
        if not existing_tables:
            return False

        try:
            # Get shape of new table
            new_rows = len(new_table) if hasattr(new_table, "__len__") else 0
            new_cols = len(new_table.columns) if hasattr(new_table, "columns") else 0

            if new_rows == 0:
                return False

            # Check for exact duplicates
            for table in existing_tables:
                existing_rows = len(table) if hasattr(table, "__len__") else 0
                existing_cols = len(table.columns) if hasattr(table, "columns") else 0

                # If dimensions match and columns are similar, check for content similarity
                if (
                    existing_rows == new_rows
                    and existing_cols == new_cols
                    and set(table.columns) == set(new_table.columns)
                ):
                    # Use DuckDB to compare tables
                    try:
                        self.conn.register("table_a", table)
                        self.conn.register("table_b", new_table)

                        # Sample first few rows for comparison
                        sample_size = min(5, existing_rows)

                        # Compare samples using EXCEPT
                        diff_count = self.conn.execute(f"""
                            SELECT COUNT(*) FROM (
                                SELECT * FROM table_a LIMIT {sample_size}
                                EXCEPT
                                SELECT * FROM table_b LIMIT {sample_size}
                            )
                        """).fetchone()[0]

                        self.conn.unregister("table_a")
                        self.conn.unregister("table_b")

                        if diff_count == 0:
                            return True

                    except Exception:
                        # Fall back to simple comparison if DuckDB comparison fails
                        pass

            return False

        except Exception as e:
            logger.debug(f"Duplicate check failed: {e!s}")
            return False
