"""Excel transformer for Silver layer - DuckDB optimized."""

import time
from pathlib import Path

import pandas as pd

# Handle imports for both standalone and package usage
try:
    from ...bronze.metadata import FileMetadata
    from ...utils.logging import get_logger, set_context
    from ...utils.storage import get_storage_manager
    from ..duckdb_base import DuckDBProcessor
    from ..storage import SilverStorageManager
except ImportError:
    # Fallback for standalone usage
    import logging

    get_logger = lambda: logging.getLogger(__name__)
    set_context = lambda x: None
    get_storage_manager = lambda: None
    from silver.duckdb_base import DuckDBProcessor
    from silver.storage import SilverStorageManager

    FileMetadata = None
from .base import BaseTransformer, TransformResult

# Get logger
logger = get_logger()


class ExcelTransformer(BaseTransformer, DuckDBProcessor):
    """Transformer for Excel files using DuckDB."""

    def __init__(self):
        """Initialize the Excel transformer with DuckDB support."""
        DuckDBProcessor.__init__(self, dataset_name="excel_transformer")
        logger.info("Initialized DuckDB-based Excel transformer")

    def transform(
        self,
        file_path: Path,
        metadata: FileMetadata,
        output_dir: Path,
    ) -> TransformResult:
        """Transform Excel file to Parquet format using DuckDB.

        Args:
            file_path: Path to the Excel file
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
            logger.info(f"Transforming Excel file using DuckDB: {file_path}")

            # Read Excel file and convert to DuckDB tables
            sheets_data = self._read_excel_to_duckdb(file_path)
            if not sheets_data:
                return TransformResult(
                    success=False,
                    error="Excel file has no valid sheets",
                )

            # Create a storage manager instance
            storage_manager = get_storage_manager("local")

            # Create output directory for this file
            silver_storage = SilverStorageManager(
                storage_manager=storage_manager,
                base_path=output_dir,
            )
            file_output_dir = silver_storage.create_output_directory(
                run_dir=output_dir,
                source_subfolder=metadata.original_subfolder,
                content_type="Excel",
            )

            # Process each sheet
            output_paths = []
            total_rows = 0

            for sheet_name, table_name in sheets_data:
                # Clean column names using DuckDB
                cleaned_table = self._standardize_column_names_duckdb(table_name)

                # Apply data type standardization using DuckDB
                standardized_table = self._standardize_data_types_duckdb(cleaned_table)

                # Generate output filename
                base_filename = Path(metadata.original_filename).stem
                sheet_filename = f"{base_filename}_{sheet_name}"

                # Save as Parquet using DuckDB
                output_path = file_output_dir / f"{sheet_filename}.parquet"
                self.save_table_to_parquet(standardized_table, output_path)

                output_paths.append(output_path)

                # Get row count
                row_count = self.conn.execute(
                    f"SELECT COUNT(*) FROM {standardized_table}"
                ).fetchone()[0]
                total_rows += row_count

                # Clean up intermediate tables
                self.drop_table(table_name)
                self.drop_table(cleaned_table)
                self.drop_table(standardized_table)

            # Create schema dictionary from the last table (for compatibility)
            schema = {"columns": [], "data_types": []}
            if sheets_data:
                # Get schema from last processed table before cleanup
                last_table = f"temp_schema_{int(time.time())}"
                self.conn.execute(
                    f"CREATE TABLE {last_table} AS SELECT * FROM {standardized_table} LIMIT 0"
                )
                table_info = self.get_table_info(last_table)
                schema = {
                    "columns": [col[0] for col in table_info],
                    "data_types": [col[1] for col in table_info],
                }
                self.drop_table(last_table)

            return TransformResult(
                success=True,
                output_path=output_paths[0] if len(output_paths) == 1 else None,
                row_count=total_rows,
                schema=schema,
                metadata={
                    "sheet_count": len(sheets_data),
                    "output_paths": [str(p) for p in output_paths],
                },
            )

        except Exception as e:
            error_msg = f"Failed to transform Excel file {file_path}: {str(e)}"
            logger.error(error_msg)
            return TransformResult(
                success=False,
                error=error_msg,
            )

    def _read_excel_to_duckdb(self, file_path: Path) -> list[tuple[str, str]]:
        """Read Excel file and convert to DuckDB tables.

        Args:
            file_path: Path to the Excel file

        Returns:
            List of tuples containing (sheet_name, table_name)
        """
        try:
            logger.debug(f"Reading Excel file to DuckDB: {file_path}")

            # Read all sheets using pandas (DuckDB doesn't have native Excel support yet)
            excel_file = pd.ExcelFile(file_path)
            sheets_data = []

            for sheet_name in excel_file.sheet_names:
                try:
                    # First, read the sheet without any header processing to detect structure
                    df_raw = pd.read_excel(excel_file, sheet_name=sheet_name, header=None)

                    # Skip empty sheets
                    if df_raw.empty:
                        logger.debug(f"Skipping empty sheet: {sheet_name}")
                        continue

                    # Auto-detect header row by finding the first row that isn't mostly empty/NaN
                    header_row = 0
                    for i in range(min(5, len(df_raw))):  # Check first 5 rows max
                        row = df_raw.iloc[i]
                        # Count non-null, non-empty values
                        valid_values = row.dropna()
                        valid_values = valid_values[valid_values.astype(str).str.strip() != ""]

                        # If row has at least 3 valid values, consider it a potential header
                        if len(valid_values) >= 3:
                            # Check if this looks like a header row (contains text, not mostly numbers)
                            text_count = sum(
                                1
                                for val in valid_values
                                if isinstance(val, str)
                                or (
                                    pd.notna(val)
                                    and not str(val).replace(".", "").replace("-", "").isdigit()
                                )
                            )

                            if text_count >= len(valid_values) * 0.6:  # At least 60% text values
                                header_row = i
                                break

                    # Now read with the detected header row
                    if header_row > 0:
                        logger.debug(
                            f"Auto-detected header row at position {header_row} for sheet {sheet_name}"
                        )
                        # Skip rows before header and use the detected row as header
                        df = pd.read_excel(
                            excel_file,
                            sheet_name=sheet_name,
                            header=header_row,
                            skiprows=list(range(header_row)),
                        )
                    else:
                        # Standard parsing if header is in row 0
                        df = pd.read_excel(excel_file, sheet_name=sheet_name)

                    # Skip empty sheets after processing
                    if df.empty:
                        logger.debug(f"Skipping empty sheet after processing: {sheet_name}")
                        continue

                    # Clean sheet name for table name
                    clean_sheet_name = "".join(
                        c if c.isalnum() else "_" for c in sheet_name
                    ).lower()

                    # Register DataFrame as DuckDB table
                    table_name = f"excel_{clean_sheet_name}_{int(time.time())}"
                    self.register_dataframe(df, table_name)

                    # Append sheet data
                    sheets_data.append((clean_sheet_name, table_name))
                    logger.debug(
                        f"Registered sheet {sheet_name} as table {table_name} with {len(df)} rows"
                    )

                except Exception as e:
                    logger.warning(f"Failed to read sheet {sheet_name}: {str(e)}")
                    continue

            logger.info(f"Successfully read {len(sheets_data)} sheets from Excel file")
            return sheets_data

        except Exception as e:
            logger.error(f"Failed to read Excel file: {str(e)}")
            return []

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

            # Create column mapping for renaming
            column_mapping = []
            for col_name, col_type in columns_info:
                # Apply domain-specific column name mappings first
                mapped_name = self._apply_domain_specific_mappings(col_name)

                if mapped_name:
                    # Use domain-specific mapping
                    standardized_name = mapped_name
                else:
                    # Standardize column name: lowercase, replace spaces/special chars with underscores
                    standardized_name = "".join(
                        c.lower() if c.isalnum() else "_" for c in str(col_name)
                    ).strip("_")

                    # Ensure it doesn't start with a number
                    if standardized_name and standardized_name[0].isdigit():
                        standardized_name = f"col_{standardized_name}"

                    # Handle empty names
                    if not standardized_name:
                        standardized_name = f"column_{len(column_mapping)}"

                column_mapping.append(f'"{col_name}" AS {standardized_name}')

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

    def _apply_domain_specific_mappings(self, col_name: str) -> str:
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

        # Check if this matches any pesticide column
        if normalized_name in pesticide_mappings:
            return pesticide_mappings[normalized_name]

        # Add other domain-specific mappings here as needed

        return None

    def _add_backward_compatibility_columns(self, table_name: str) -> None:
        """Add backward compatibility columns for pesticide data to maintain downstream compatibility.

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

            # Build column transformations
            column_transformations = []

            for col_name, col_type in columns_info:
                # Apply explicit type casting for known columns
                if col_name in [
                    "area_ha",
                    "block_area_ha",
                    "applied_area_ha",
                    "reported_area_ha",
                    "dosage_quantity",
                ]:
                    # Force area and dosage columns to DOUBLE
                    transformation = f"CAST({col_name} AS DOUBLE) AS {col_name}"
                elif col_name in ["cvr_number", "crop_code", "pesticide_registration_number"]:
                    # Force ID columns to VARCHAR
                    transformation = f"CAST({col_name} AS VARCHAR) AS {col_name}"
                elif col_type.upper() in ["VARCHAR", "TEXT"]:
                    # For string columns, try to detect and convert dates/numbers
                    transformation = f"""
                        CASE 
                            WHEN TRY_CAST({col_name} AS TIMESTAMP) IS NOT NULL 
                            THEN TRY_CAST({col_name} AS TIMESTAMP)
                            WHEN TRY_CAST({col_name} AS DOUBLE) IS NOT NULL 
                            THEN TRY_CAST({col_name} AS DOUBLE)
                            ELSE {col_name}
                        END AS {col_name}
                    """
                else:
                    # Keep existing type
                    transformation = f"{col_name}"

                column_transformations.append(transformation)

            # Create new table with type standardization
            result_table = f"{table_name}_typed"
            transformations_sql = ",\n                ".join(column_transformations)

            self.conn.execute(f"""
                CREATE TABLE {result_table} AS
                SELECT 
                    {transformations_sql}
                FROM {table_name}
                WHERE NOT (
                    -- Remove completely empty rows
                    SELECT bool_and(column_value IS NULL OR TRIM(CAST(column_value AS VARCHAR)) = '') 
                    FROM unnest([{", ".join([col[0] for col in columns_info])}]) AS t(column_value)
                )
            """)

            logger.debug(f"Applied data type standardization to table {table_name}")
            return result_table

        except Exception as e:
            logger.error(f"Failed to standardize data types: {str(e)}")
            # Return original table if standardization fails
            return table_name
