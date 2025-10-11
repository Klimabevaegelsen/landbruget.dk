"""CSV file transformer using DuckDB for reading and processing."""

import time
from pathlib import Path

# Handle imports for both standalone and package usage
try:
    from ...utils.logging import get_logger
    from ..duckdb_base import DuckDBProcessor
    from .base import BaseTransformer, FileMetadata, TransformResult
except ImportError:
    # Fallback for standalone usage
    import logging

    def get_logger() -> logging.Logger:
        return logging.getLogger(__name__)

    from silver.duckdb_base import DuckDBProcessor
    from silver.transformers.base import BaseTransformer, FileMetadata, TransformResult

logger = get_logger()


class CSVTransformer(BaseTransformer, DuckDBProcessor):
    """Transform CSV files using DuckDB for reading and processing."""

    def __init__(self) -> None:
        BaseTransformer.__init__(self)
        DuckDBProcessor.__init__(self)
        logger.info("Initialized CSVTransformer with DuckDB")

    def transform(
        self,
        file_path: Path,
        metadata: FileMetadata,
        output_dir: Path,
    ) -> TransformResult:
        """Transform CSV file to standardized format using DuckDB.

        Args:
            file_path: Path to the CSV file
            metadata: File metadata
            output_dir: Output directory for transformed files

        Returns:
            TransformResult with success status and metadata
        """
        try:
            logger.info(f"Transforming CSV file using DuckDB: {file_path}")

            # Create output directory for this file
            file_output_dir = output_dir / "CSV"
            file_output_dir.mkdir(parents=True, exist_ok=True)

            # Read CSV file using DuckDB
            table_name = self._read_csv_with_duckdb(file_path)

            if not table_name:
                logger.warning(f"No valid data found in CSV file: {file_path}")
                return TransformResult(
                    success=False,
                    error="No valid data found in CSV file",
                )

            # Clean column names using DuckDB
            cleaned_table = self._standardize_column_names_duckdb(table_name)

            # Apply data type standardization using DuckDB
            standardized_table = self._standardize_data_types_duckdb(cleaned_table)

            # Generate output filename
            base_filename = Path(metadata.original_filename).stem
            output_filename = f"{base_filename}.parquet"
            output_path = file_output_dir / output_filename

            # Save as Parquet using DuckDB
            self.save_table_to_parquet(standardized_table, output_path)

            # Get row count
            row_count = self.conn.execute(f"SELECT COUNT(*) FROM {standardized_table}").fetchone()[
                0
            ]

            # Create schema dictionary
            schema = {"columns": [], "data_types": []}
            try:
                table_info = self.get_table_info(standardized_table)
                schema = {
                    "columns": [col[0] for col in table_info],
                    "data_types": [col[1] for col in table_info],
                }
            except Exception as e:
                logger.warning(f"Failed to get schema from table {standardized_table}: {str(e)}")
                schema = {"columns": [], "data_types": []}

            # Clean up intermediate tables
            self.drop_table(table_name)
            if cleaned_table != standardized_table:
                self.drop_table(cleaned_table)
            self.drop_table(standardized_table)

            return TransformResult(
                success=True,
                output_path=output_path,
                row_count=row_count,
                schema=schema,
                metadata={
                    "file_type": "CSV",
                    "output_path": str(output_path),
                },
            )

        except Exception as e:
            error_msg = f"Failed to transform CSV file {file_path}: {str(e)}"
            logger.error(error_msg)
            return TransformResult(
                success=False,
                error=error_msg,
            )

    def _read_csv_with_duckdb(self, file_path: Path) -> str | None:
        """Read CSV file using DuckDB with auto-detection.

        Args:
            file_path: Path to the CSV file

        Returns:
            Table name in DuckDB or None if failed
        """
        try:
            logger.debug(f"Reading CSV file using DuckDB: {file_path}")

            # Generate unique table name
            table_name = f"csv_data_{int(time.time())}"

            # Use DuckDB's read_csv with auto-detection
            # This handles various CSV formats, delimiters, and encodings automatically
            self.conn.execute(f"""
                CREATE TABLE {table_name} AS
                SELECT * FROM read_csv('{file_path}', AUTO_DETECT=TRUE, HEADER=TRUE)
            """)

            # Check if table has data
            row_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

            if row_count == 0:
                logger.warning(f"CSV file {file_path} contains no data")
                self.drop_table(table_name)
                return None

            logger.info(f"Successfully read CSV file with {row_count} rows")
            return table_name

        except Exception as e:
            logger.error(f"Failed to read CSV file with DuckDB: {str(e)}")
            return None

    def _standardize_column_names_duckdb(self, table_name: str) -> str:
        """Standardize column names using DuckDB operations.

        Args:
            table_name: Name of source table

        Returns:
            Name of table with standardized column names
        """
        try:
            # Get current column info
            columns_info = self.get_table_info(table_name)

            if not columns_info:
                logger.warning(f"No columns found in table {table_name}")
                return table_name

            # Create column mapping for renaming
            column_mapping = []
            used_names = set()  # Track used standardized names to avoid duplicates

            for i, col_info in enumerate(columns_info):
                # DuckDB DESCRIBE returns: (col_name, col_type, nullable, key, default, extra)
                col_name = col_info[0]
                col_info[1]

                # Apply domain-specific column name mappings first
                mapped_name = self._apply_domain_specific_mappings(col_name)

                if mapped_name:
                    # Use domain-specific mapping
                    standardized_name = mapped_name
                else:
                    # Standardize column name: lowercase, replace spaces/special chars
                    # Handle Danish characters properly
                    standardized_name = str(col_name).lower()
                    # Replace Danish characters
                    standardized_name = standardized_name.replace("æ", "ae")
                    standardized_name = standardized_name.replace("ø", "oe")
                    standardized_name = standardized_name.replace("å", "aa")
                    # Replace other special characters
                    standardized_name = standardized_name.replace("é", "e")
                    standardized_name = standardized_name.replace("è", "e")
                    standardized_name = standardized_name.replace("ê", "e")
                    standardized_name = standardized_name.replace("ë", "e")
                    standardized_name = standardized_name.replace("á", "a")
                    standardized_name = standardized_name.replace("à", "a")
                    standardized_name = standardized_name.replace("â", "a")
                    standardized_name = standardized_name.replace("ä", "a")
                    standardized_name = standardized_name.replace("ó", "o")
                    standardized_name = standardized_name.replace("ò", "o")
                    standardized_name = standardized_name.replace("ô", "o")
                    standardized_name = standardized_name.replace("ö", "o")
                    # Replace spaces and special chars with underscores
                    standardized_name = "".join(
                        c if c.isalnum() else "_" for c in standardized_name
                    ).strip("_")

                    # Remove consecutive underscores
                    while "__" in standardized_name:
                        standardized_name = standardized_name.replace("__", "_")

                    # Ensure it doesn't start with a number
                    if standardized_name and standardized_name[0].isdigit():
                        standardized_name = f"col_{standardized_name}"

                    # Handle empty names or very short names
                    if not standardized_name or len(standardized_name) < 2:
                        standardized_name = f"column_{i}"

                # Ensure uniqueness
                original_standardized = standardized_name
                counter = 1
                while standardized_name in used_names:
                    standardized_name = f"{original_standardized}_{counter}"
                    counter += 1

                used_names.add(standardized_name)

                # Properly quote both original and standardized column names
                # Escape any quotes in the original column name
                escaped_col_name = str(col_name).replace('"', '""')
                column_mapping.append(f'"{escaped_col_name}" AS "{standardized_name}"')

            # Create new table with standardized column names
            result_table = f"{table_name}_clean_cols"
            columns_sql = ", ".join(column_mapping)

            self.conn.execute(f"""
                CREATE TABLE {result_table} AS
                SELECT {columns_sql}
                FROM {table_name}
            """)

            # Add backward compatibility columns for pesticide data
            self._add_backward_compatibility_columns(result_table)

            logger.debug(f"Standardized column names for table {table_name}")
            return result_table

        except Exception as e:
            logger.error(f"Failed to standardize column names: {str(e)}")
            # Return original table if standardization fails
            return table_name

    def _apply_domain_specific_mappings(self, col_name: str) -> str | None:
        """Apply domain-specific column name mappings for known data types.

        Args:
            col_name: Original column name

        Returns:
            Mapped column name or None if no mapping applies
        """
        # Normalize column name for comparison (lowercase, no spaces)
        normalized_name = col_name.lower().replace(" ", "").replace("_", "")

        # Pesticide data column mappings
        pesticide_mappings = {
            "acreagesize": "area_ha",
            "companyregistrationnumber": "cvr_number",
            "code": "crop_code",
            "pesticidename": "pesticide_name",
            "pesticideregistrationnumber": "pesticide_registration_number",
            "dosagequantity": "dosage_quantity",
            "dosageunit": "dosage_unit",
            "nopesticides": "no_pesticides",
        }

        # General CVR column mappings (for any document type)
        cvr_mappings = {
            "cvr": "cvr_number",
            "cvrno": "cvr_number",
            "cvrnr": "cvr_number",
            "cvrnummer": "cvr_number",
            "virksomhedsnummer": "cvr_number",
            "companyid": "cvr_number",
            "company_id": "cvr_number",
            "firmaid": "cvr_number",
            "firma_id": "cvr_number",
        }

        # Check if this matches any pesticide column
        if normalized_name in pesticide_mappings:
            return pesticide_mappings[normalized_name]

        # Check if this matches any CVR column variation
        if normalized_name in cvr_mappings:
            return cvr_mappings[normalized_name]

        # Add other domain-specific mappings here as needed

        return None

    def _add_backward_compatibility_columns(self, table_name: str) -> None:
        """Add backward compatibility columns for pesticide data to maintain compatibility.

        Args:
            table_name: Name of the table to add compatibility columns to
        """
        try:
            # Get current columns to check what we're working with
            columns_info = self.get_table_info(table_name)
            column_names = [col[0] for col in columns_info]

            # Define backward compatibility mappings (new_name -> old_name)
            backward_compatibility_mappings = {
                "area_ha": "acreagesize",
                "cvr_number": "companyregistrationnumber",
                "crop_code": "code",
                "pesticide_name": "pesticidename",
                "pesticide_registration_number": "pesticideregistrationnumber",
                "dosage_quantity": "dosagequantity",
                "dosage_unit": "dosageunit",
                "no_pesticides": "nopesticides",
            }

            # Add backward compatibility columns if the new columns exist
            alter_statements = []
            for new_col, old_col in backward_compatibility_mappings.items():
                if new_col in column_names and old_col not in column_names:
                    alter_statements.append(
                        f"ALTER TABLE {table_name} ADD COLUMN {old_col} AS {new_col}"
                    )

            # Execute all alter statements
            for statement in alter_statements:
                self.conn.execute(statement)

            if alter_statements:
                logger.debug(
                    f"Added {len(alter_statements)} backward compatibility columns to {table_name}"
                )

        except Exception as e:
            logger.warning(
                f"Failed to add backward compatibility columns to {table_name}: {str(e)}"
            )

    def _standardize_data_types_duckdb(self, table_name: str) -> str:
        """Apply data type standardization using DuckDB operations.

        Args:
            table_name: Name of source table

        Returns:
            Name of table with standardized data types
        """
        try:
            # Get column information
            columns_info = self.get_table_info(table_name)

            if not columns_info:
                logger.warning(f"No columns found in table {table_name}")
                return table_name

            # Build column transformations
            column_transformations = []

            for col_info in columns_info:
                # DuckDB DESCRIBE returns: (col_name, col_type, nullable, key, default, extra)
                col_name = col_info[0]
                col_info[1]

                # Properly quote column name for DuckDB and escape quotes
                escaped_col_name = str(col_name).replace('"', '""')
                quoted_col_name = f'"{escaped_col_name}"'

                # Apply explicit type casting for known columns
                if col_name in [
                    "area_ha",
                    "block_area_ha",
                    "applied_area_ha",
                    "reported_area_ha",
                    "dosage_quantity",
                ]:
                    # Force area and dosage columns to DOUBLE with safe casting
                    transformation = (
                        f'TRY_CAST({quoted_col_name} AS DOUBLE) AS "{escaped_col_name}"'
                    )
                elif col_name in ["cvr_number", "crop_code", "pesticide_registration_number"]:
                    # Force ID columns to VARCHAR
                    transformation = f'CAST({quoted_col_name} AS VARCHAR) AS "{escaped_col_name}"'
                else:
                    # For all other columns, cast to VARCHAR to ensure consistency
                    # This avoids type inference issues and makes the data more predictable
                    transformation = f'CAST({quoted_col_name} AS VARCHAR) AS "{escaped_col_name}"'

                column_transformations.append(transformation)

            # Create new table with type standardization
            result_table = f"{table_name}_typed"
            transformations_sql = ",\n                ".join(column_transformations)

            # Simplified query without complex row filtering to avoid parsing issues
            self.conn.execute(f"""
                CREATE TABLE {result_table} AS
                SELECT
                    {transformations_sql}
                FROM {table_name}
                WHERE 1=1
            """)

            logger.debug(f"Applied data type standardization to table {table_name}")
            return result_table

        except Exception as e:
            logger.error(f"Failed to standardize data types: {str(e)}")
            # Return original table if standardization fails
            return table_name
