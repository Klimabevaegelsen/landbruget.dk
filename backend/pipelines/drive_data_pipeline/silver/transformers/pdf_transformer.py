"""PDF transformer for Silver layer using DuckDB."""

from pathlib import Path
from typing import Any

import tabula

# Handle imports for both standalone and package usage
try:
    from ...utils.logging import get_logger
    from ..duckdb_base import DuckDBProcessor
except ImportError:
    # Fallback for standalone usage
    import logging

    def get_logger() -> logging.Logger:
        return logging.getLogger(__name__)

    from silver.duckdb_base import DuckDBProcessor
from .base import BaseTransformer, TransformResult

# Get logger
logger = get_logger()


class PDFTransformer(BaseTransformer, DuckDBProcessor):
    """Transformer for PDF files using DuckDB."""

    def __init__(self) -> None:
        """Initialize the PDF transformer."""
        BaseTransformer.__init__(self)
        DuckDBProcessor.__init__(self)
        logger.info("Initialized PDFTransformer with DuckDB")

    def transform(
        self,
        file_path: Path,
        metadata: Any,  # FileMetadata
        output_dir: Path,
    ) -> TransformResult:
        """Transform PDF file to structured data using DuckDB.

        Args:
            file_path: Path to the PDF file
            metadata: File metadata
            output_dir: Directory to save output files

        Returns:
            TransformResult with transformation results
        """
        try:
            logger.info(f"Transforming PDF file: {file_path}")

            # Extract tables from PDF
            tables = self._extract_tables(file_path)
            if not tables:
                logger.warning(f"PDF file {file_path} has no extractable tables")
                return TransformResult(
                    success=False,
                    error="No extractable tables found in PDF",
                )

            # Process tables using DuckDB
            output_paths = []
            total_rows = 0

            for i, df in enumerate(tables):
                # Register table in DuckDB
                table_name = f"pdf_table_{i}"
                self.register_table(df, table_name)

                # Clean and standardize using DuckDB
                clean_table = self._standardize_table_with_duckdb(table_name, i, file_path.name)

                # Count rows
                row_count = self.conn.execute(f"SELECT COUNT(*) FROM {clean_table}").fetchone()[0]
                total_rows += row_count

                # Save to parquet
                output_filename = f"{file_path.stem}_table_{i}.parquet"
                output_path = output_dir / output_filename
                self.export_to_parquet(clean_table, output_path)
                output_paths.append(output_path)

                logger.info(f"Processed table {i} with {row_count} rows")

            # Create schema from the last table
            schema = self._create_schema_dict_from_table(clean_table)

            return TransformResult(
                success=True,
                output_path=output_paths[0] if len(output_paths) == 1 else None,
                row_count=total_rows,
                schema=schema,
                metadata={
                    "table_count": len(tables),
                    "output_paths": [str(p) for p in output_paths],
                },
            )

        except Exception as e:
            error_msg = f"Failed to transform PDF file {file_path}: {str(e)}"
            logger.error(error_msg)
            return TransformResult(
                success=False,
                error=error_msg,
            )

    def transform_from_content(
        self,
        file_content: bytes,
        filename: str,
        metadata_dict: dict,
    ) -> str | None:
        """Transform PDF content directly from memory, returning DuckDB table name.

        Args:
            file_content: Raw PDF content in bytes
            filename: Original filename
            metadata_dict: File metadata dictionary

        Returns:
            DuckDB table name with transformed data or None if transformation failed
        """
        import os
        import tempfile

        try:
            logger.info(f"Transforming PDF file: {filename}")

            # Create a temporary file for the PDF content
            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as temp_file:
                temp_file.write(file_content)
                temp_file.flush()
                temp_path = Path(temp_file.name)

            # Extract tables from the temporary PDF
            tables = self._extract_tables(temp_path)
            if not tables:
                logger.warning(f"PDF file {filename} has no extractable tables")
                return None

            # Process tables using DuckDB
            processed_tables = []
            for i, df in enumerate(tables):
                # Register table in DuckDB
                temp_table = f"pdf_temp_{i}"
                self.register_table(df, temp_table)

                # Clean and standardize
                clean_table = self._standardize_table_with_duckdb(temp_table, i, filename)
                processed_tables.append(clean_table)

            # Combine all tables into one if multiple exist
            if len(processed_tables) == 1:
                final_table = processed_tables[0]
            else:
                # Union all tables - but handle different column structures
                final_table = f"pdf_combined_{filename.replace('.', '_')}"

                # Find common columns across all tables
                all_columns = {}
                for table in processed_tables:
                    columns = self.conn.execute(f"DESCRIBE {table}").fetchall()
                    all_columns[table] = [col[0] for col in columns]

                # Find intersection of all column sets
                common_columns = set(all_columns[processed_tables[0]])
                for table in processed_tables[1:]:
                    common_columns = common_columns.intersection(set(all_columns[table]))

                if common_columns:
                    # Union with common columns only
                    common_cols_list = sorted(list(common_columns))  # Sort for consistency
                    common_cols_str = ", ".join([f'"{col}"' for col in common_cols_list])

                    union_query = " UNION ALL ".join(
                        [f"SELECT {common_cols_str} FROM {table}" for table in processed_tables]
                    )
                    self.conn.execute(f"CREATE TABLE {final_table} AS {union_query}")

                    logger.info(
                        f"Combined {len(processed_tables)} tables using "
                        f"{len(common_columns)} common columns"
                    )
                else:
                    # No common columns - just use the first table as fallback
                    logger.warning(
                        f"No common columns found across {len(processed_tables)} tables, "
                        f"using first table only"
                    )
                    final_table = processed_tables[0]

            logger.info(f"Successfully transformed PDF {filename} into table {final_table}")
            return final_table

        except Exception as e:
            logger.error(f"Failed to transform PDF content for {filename}: {str(e)}")
            return None
        finally:
            # Clean up temporary file
            try:
                if temp_path.exists():
                    os.unlink(temp_path)
            except Exception:
                pass

    def _extract_tables(self, file_path: Path) -> list[Any]:
        """Extract tables from PDF file using tabula.

        Args:
            file_path: Path to the PDF file

        Returns:
            List of pandas dataframes, each representing a table
        """
        try:
            logger.debug(f"Extracting tables from PDF: {file_path}")

            # Use tabula-py to extract tables
            # This extracts all tables from all pages
            tables = tabula.read_pdf(
                str(file_path),
                pages="all",
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

    def _standardize_table_with_duckdb(
        self, table_name: str, table_number: int, source_file: str
    ) -> str:
        """Standardize a table using DuckDB operations.

        Args:
            table_name: Name of the DuckDB table to standardize
            table_number: Table number identifier
            source_file: Source filename

        Returns:
            Name of the standardized table
        """
        clean_table = f"{table_name}_clean"

        try:
            # Get column information
            columns_info = self.conn.execute(f"DESCRIBE {table_name}").fetchall()

            # Build standardization query
            select_parts = []

            for col_name, col_type, *_ in columns_info:
                # Properly escape column names with double quotes
                escaped_col_name = f'"{col_name}"'

                # Standardize column names (convert to snake_case)
                clean_col_name = self._standardize_column_name(col_name)

                # Apply data type standardization based on column type
                if "VARCHAR" in col_type.upper() or "TEXT" in col_type.upper():
                    # Handle string columns - try to detect dates, booleans, numbers
                    select_parts.append(f"""
                        CASE
                            WHEN {escaped_col_name} ~ '^\\d{{2}}[/.-]\\d{{2}}[/.-]\\d{{4}}$' OR
                                 {escaped_col_name} ~ '^\\d{{4}}[/.-]\\d{{2}}[/.-]\\d{{2}}$'
                            THEN TRY_CAST({escaped_col_name} AS DATE)::VARCHAR
                            WHEN LOWER({escaped_col_name}) IN (
                                'yes', 'no', 'true', 'false', 'ja', 'nej'
                            )
                            THEN CASE LOWER({escaped_col_name})
                                     WHEN 'yes' THEN '1'
                                     WHEN 'true' THEN '1'
                                     WHEN 'ja' THEN '1'
                                     WHEN 'no' THEN '0'
                                     WHEN 'false' THEN '0'
                                     WHEN 'nej' THEN '0'
                                     ELSE {escaped_col_name}
                                 END
                            WHEN {escaped_col_name} ~ '^-?\\d+\\.?\\d*$'
                            THEN TRY_CAST({escaped_col_name} AS DOUBLE)::VARCHAR
                            ELSE {escaped_col_name}
                        END AS {clean_col_name}
                    """)
                elif "INTEGER" in col_type.upper() or "BIGINT" in col_type.upper():
                    # Handle integer columns - cast to string first, then apply date
                    # pattern detection
                    select_parts.append(f"""
                        CASE
                            WHEN CAST({escaped_col_name} AS VARCHAR) ~ 
                                    '^\\d{{2}}[/.-]\\d{{2}}[/.-]\\d{{4}}$' OR
                                 CAST({escaped_col_name} AS VARCHAR) ~ 
                                    '^\\d{{4}}[/.-]\\d{{2}}[/.-]\\d{{2}}$'
                            THEN TRY_CAST(CAST({escaped_col_name} AS VARCHAR) AS DATE)::VARCHAR
                            ELSE CAST({escaped_col_name} AS VARCHAR)
                        END AS {clean_col_name}
                    """)
                elif "DOUBLE" in col_type.upper() or "FLOAT" in col_type.upper():
                    # Handle float columns - convert to string
                    select_parts.append(f"CAST({escaped_col_name} AS VARCHAR) AS {clean_col_name}")
                else:
                    # Keep other column types as-is but with clean names, cast to string
                    # for consistency
                    select_parts.append(f"CAST({escaped_col_name} AS VARCHAR) AS {clean_col_name}")

            # Add metadata columns
            select_parts.append(f"{table_number} AS table_number")
            select_parts.append(f"'{source_file}' AS source_file")

            # Create the cleaned table - also properly escape column names in WHERE clause
            escaped_column_nulls = [f'"{col[0]}" IS NULL' for col in columns_info]

            self.conn.execute(f"""
                CREATE TABLE {clean_table} AS
                SELECT {", ".join(select_parts)}
                FROM {table_name}
                WHERE NOT (
                    -- Remove rows where all original columns are null
                    {" AND ".join(escaped_column_nulls)}
                )
            """)

            logger.debug(f"Standardized table {table_name} -> {clean_table}")
            return clean_table

        except Exception as e:
            logger.warning(f"Failed to standardize table {table_name}: {str(e)}")
            # Return original table if standardization fails
            return table_name

    def _standardize_column_name(self, col_name: str) -> str:
        """Convert column name to snake_case with domain-specific mappings.

        Args:
            col_name: Original column name

        Returns:
            Standardized column name
        """
        import re

        # First try domain-specific mappings
        mapped_name = self._apply_domain_specific_mappings(col_name)
        if mapped_name:
            return mapped_name

        # Convert to lowercase and replace spaces/special chars with underscores
        clean_name = re.sub(r"[^a-zA-Z0-9]", "_", str(col_name).lower())

        # Remove consecutive underscores
        clean_name = re.sub(r"_+", "_", clean_name)

        # Remove leading/trailing underscores
        clean_name = clean_name.strip("_")

        # Ensure it starts with a letter
        if clean_name and clean_name[0].isdigit():
            clean_name = f"col_{clean_name}"

        return clean_name or "unnamed_column"

    def _apply_domain_specific_mappings(self, col_name: str) -> str:
        """Apply domain-specific column name mappings for known data types.

        Args:
            col_name: Original column name

        Returns:
            Mapped column name or None if no mapping applies
        """
        # Normalize column name for comparison (lowercase, no spaces)
        normalized_name = col_name.lower().replace(" ", "").replace("_", "").replace("-", "")

        # VISA-specific column mappings (Danish and English)
        visa_mappings = {
            # Year variations
            "aar": "year",
            "år": "year",
            "year": "year",
            "aarig": "year",
            "årig": "year",
            # Nationality variations
            "nationalitet": "nationality",
            "nationality": "nationality",
            "land": "nationality",
            "country": "nationality",
            "oprindelsesland": "nationality",
            "statsborger": "nationality",
            "citizenship": "nationality",
            # First permits count variations
            "foerstetilladelser": "first_permits_count",
            "førstegangsarbejdstilladelser": "first_permits_count",
            "firstpermits": "first_permits_count",
            "foerstetilladelse": "first_permits_count",
            "førstegangs": "first_permits_count",
            "initialpermits": "first_permits_count",
            "nyetilladelser": "first_permits_count",
            "newtilladelser": "first_permits_count",
            "antal": "count",
            "count": "count",
            "tael": "count",
            "tal": "count",
            # CVR mappings (for company identification)
            "cvr": "cvr_number",
            "cvrno": "cvr_number",
            "cvrnr": "cvr_number",
            "cvrnummer": "cvr_number",
            "virksomhedsnummer": "cvr_number",
            "companyid": "cvr_number",
            "company_id": "cvr_number",
            "firmaid": "cvr_number",
            "firma_id": "cvr_number",
            # Company name variations
            "virksomhedsnavn": "company_name",
            "firmanavn": "company_name",
            "companyname": "company_name",
            "arbejdsgiver": "company_name",
            "employer": "company_name",
        }

        # Check if this matches any VISA column
        if normalized_name in visa_mappings:
            return visa_mappings[normalized_name]

        # Check for partial matches on common VISA terms
        visa_partial_patterns = {
            "tilladelse": "permits",
            "permit": "permits",
            "arbejds": "work",
            "work": "work",
            "visa": "visa",
            "visum": "visa",
        }

        for pattern, mapped in visa_partial_patterns.items():
            if pattern in normalized_name:
                # If it contains count/number indicators, add _count suffix
                if any(
                    count_term in normalized_name
                    for count_term in ["antal", "count", "tal", "tael", "number"]
                ):
                    return f"{mapped}_count"
                return mapped

        return None

    def _create_schema_dict_from_table(self, table_name: str) -> dict:
        """Create schema dictionary from DuckDB table.

        Args:
            table_name: Name of the DuckDB table

        Returns:
            Schema dictionary
        """
        try:
            columns_info = self.conn.execute(f"DESCRIBE {table_name}").fetchall()
            schema = {}

            for col_name, col_type, *_ in columns_info:
                # Map DuckDB types to schema types
                if "VARCHAR" in col_type.upper() or "TEXT" in col_type.upper():
                    schema[col_name] = "string"
                elif "INTEGER" in col_type.upper() or "BIGINT" in col_type.upper():
                    schema[col_name] = "integer"
                elif "DOUBLE" in col_type.upper() or "FLOAT" in col_type.upper():
                    schema[col_name] = "float"
                elif "DATE" in col_type.upper():
                    schema[col_name] = "date"
                elif "TIMESTAMP" in col_type.upper():
                    schema[col_name] = "timestamp"
                elif "BOOLEAN" in col_type.upper():
                    schema[col_name] = "boolean"
                else:
                    schema[col_name] = "string"  # Default to string

            return schema

        except Exception as e:
            logger.warning(f"Failed to create schema from table {table_name}: {str(e)}")
            return {}
