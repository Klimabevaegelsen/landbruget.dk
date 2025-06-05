"""Excel transformer for Silver layer."""

from pathlib import Path

import pandas as pd

from ...bronze.metadata import FileMetadata
from ...utils.logging import get_logger, set_context
from ...utils.storage import get_storage_manager
from ..storage import SilverStorageManager
from .base import BaseTransformer, TransformResult

# Get logger
logger = get_logger()


class ExcelTransformer(BaseTransformer):
    """Transformer for Excel files."""

    def transform(
        self,
        file_path: Path,
        metadata: FileMetadata,
        output_dir: Path,
    ) -> TransformResult:
        """Transform Excel file to Parquet format.

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
            logger.info(f"Transforming Excel file: {file_path}")

            # Read Excel file
            sheets_data = self._read_excel(file_path)
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

            for sheet_name, df in sheets_data:
                # Clean column names
                df.columns = self._standardize_column_names(df.columns.tolist())

                # Apply data type standardization
                df = self._standardize_data_types(df)

                # Generate output filename
                base_filename = Path(metadata.original_filename).stem
                sheet_filename = f"{base_filename}_{sheet_name}"

                # Convert to DuckDB/Ibis for processing
                # This is a placeholder - actual implementation will use
                # DuckDB/Ibis
                # duckdb_conn = duckdb.connect(database=":memory:")
                # duckdb_conn.register("df_table", df)
                # ibis_table = ibis.duckdb.api.connect(duckdb_conn)
                #     .table("df_table")

                # Save as Parquet
                output_path = silver_storage.save_parquet(
                    df=df,  # Replace with ibis_table when implemented
                    output_dir=file_output_dir,
                    filename=sheet_filename,
                )

                output_paths.append(output_path)
                total_rows += len(df)

            # Create schema dictionary
            schema = self._create_schema_dict(df)

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

    def _read_excel(self, file_path: Path) -> list[tuple[str, pd.DataFrame]]:
        """Read Excel file and returns data from all sheets.

        Args:
            file_path: Path to the Excel file

        Returns:
            List of tuples containing (sheet_name, dataframe)
        """
        try:
            logger.debug(f"Reading Excel file: {file_path}")

            # Read all sheets
            excel_file = pd.ExcelFile(file_path)
            sheets_data = []

            for sheet_name in excel_file.sheet_names:
                try:
                    # Read the sheet
                    df = pd.read_excel(excel_file, sheet_name=sheet_name)

                    # Skip empty sheets
                    if df.empty:
                        logger.debug(f"Skipping empty sheet: {sheet_name}")
                        continue

                    # Clean sheet name for filename
                    clean_sheet_name = "".join(
                        c if c.isalnum() else "_" for c in sheet_name
                    ).lower()

                    # Append sheet data
                    sheets_data.append((clean_sheet_name, df))
                    logger.debug(f"Read sheet {sheet_name} with {len(df)} rows")

                except Exception as e:
                    logger.warning(f"Failed to read sheet {sheet_name}: {str(e)}")

            logger.info(f"Read {len(sheets_data)} sheets from {file_path}")
            return sheets_data

        except Exception as e:
            logger.error(f"Failed to read Excel file {file_path}: {str(e)}")
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
            if df_clean[col].dtype == "object":
                # Try to convert to datetime if it looks like a date
                try:
                    if (
                        df_clean[col].str.contains(r"\d{2}[/.-]\d{2}[/.-]\d{4}").any()
                        or df_clean[col].str.contains(r"\d{4}[/.-]\d{2}[/.-]\d{2}").any()
                    ):
                        df_clean[col] = pd.to_datetime(df_clean[col], errors="coerce")
                except:
                    pass

            # Handle boolean columns
            if df_clean[col].dtype == "bool":
                df_clean[col] = df_clean[col].astype(int)

            # Handle float columns
            if df_clean[col].dtype == "float64":
                # Check if the column contains whole numbers only
                if df_clean[col].dropna().apply(lambda x: x.is_integer()).all():
                    df_clean[col] = df_clean[col].astype("Int64")  # nullable integer

        return df_clean
