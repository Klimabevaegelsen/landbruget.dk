"""
Fertiliser transformer for processing fertiliser data files in the drive pipeline.

This transformer handles the harmonization of Danish fertiliser data from multiple sources:
- Efterafgrøder (cover crops)
- GKEA markplan files
- Gødningsregnskaber (fertilizer accounts)
"""

import logging
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from ..models.schema import ColumnSchema, DataType, TableSchema
from .base import BaseTransformer, TransformResult

# Handle imports for both standalone and package usage
try:
    from unified_pipeline.util.gcs_access import get_duckdb_with_gcs
except ImportError:
    # Fallback for standalone usage
    def get_duckdb_with_gcs() -> duckdb.DuckDBPyConnection:
        conn = duckdb.connect()
        try:
            conn.execute("INSTALL spatial")
            conn.execute("LOAD spatial")
        except Exception:
            pass
        return conn


logger = logging.getLogger(__name__)


class FertiliserTransformer(BaseTransformer):
    """Transformer for fertiliser parquet files."""

    def __init__(self) -> None:
        """Initialize the fertiliser transformer."""
        super().__init__()
        self.conn = get_duckdb_with_gcs()
        self._setup_harmonization_schemas()

    def _setup_harmonization_schemas(self) -> None:
        """Setup the standardized schemas for harmonized fertiliser data."""
        self.harmonized_schema = {
            "data_source": DataType.STRING,
            "year": DataType.STRING,
            "cvr_number": DataType.STRING,
            "capnumber": DataType.STRING,
            "markbloknummer": DataType.STRING,
            "marknummer": DataType.STRING,
            "indberet_alternativ": DataType.STRING,
            "faktisk_areal_ha": DataType.FLOAT,
            "omregnet_areal_ha": DataType.FLOAT,
            "journal_nummer": DataType.STRING,
            "total_n_kvote": DataType.FLOAT,
            "fosfortal": DataType.FLOAT,
            "data_type": DataType.STRING,
            "data_source_file": DataType.STRING,
        }

    def can_handle(self, file_path: Path, metadata: dict[str, Any]) -> bool:
        """
        Check if this transformer can handle the given file.

        Args:
            file_path: Path to the file
            metadata: File metadata

        Returns:
            True if this transformer can handle the file
        """
        filename = file_path.name.lower()

        # Get the full path to check for In-depth directory structure
        full_path_str = str(file_path).lower()

        # Check for main fertiliser-related files
        fertiliser_patterns = [
            "efterafgrøder",
            "efterafgroeder",
            "gkea",
            "gødningsregnskaber",
            "goedningsregnskaber",
            "fertiliser",
            "fertilizer",
        ]

        # Check for In-depth fertilizer files (B_*, V_*, etc. in GR folders)
        indepth_patterns = [
            "b_aftrk",
            "b_aoggoed",
            "b_biomasr",
            "b_blandrk",
            "b_dyrerk",
            "b_forarbr",
            "b_goedrk",
            "b_modhumr",
            "b_modrk",
            "b_ovdrk",
            "v_",  # V_ files
            "erklrk",
            "lg_company",
        ]

        # Check if it's in an In-depth/GR folder structure
        is_in_gr_folder = "in-depth" in full_path_str and (
            "gr " in full_path_str or "gr/" in full_path_str
        )

        # Main patterns match
        main_match = any(pattern in filename for pattern in fertiliser_patterns)

        # In-depth patterns match (and in correct folder structure)
        indepth_match = is_in_gr_folder and any(pattern in filename for pattern in indepth_patterns)

        return main_match or indepth_match

    def transform(
        self,
        file_path: Path,
        metadata: Any,
        output_dir: Path,
    ) -> TransformResult:
        """
        Transform a fertiliser file.

        Args:
            file_path: Path to the input file
            metadata: File metadata
            output_dir: Output directory for transformed data

        Returns:
            TransformResult containing the transformation outcome
        """
        try:
            logger.info(f"Transforming fertiliser file: {file_path.name}")

            # Read the file based on its extension
            file_suffix = file_path.suffix.lower()

            if file_suffix == ".xlsx" or file_suffix == ".xls":
                # Read Excel file - try all sheets and combine if multiple
                try:
                    excel_file = pd.ExcelFile(file_path)
                    all_sheets_data = []

                    logger.info(
                        f"Found {len(excel_file.sheet_names)} sheets in {file_path.name}: "
                        f"{excel_file.sheet_names}"
                    )

                    for sheet_name in excel_file.sheet_names:
                        try:
                            sheet_df = pd.read_excel(
                                excel_file,
                                sheet_name=sheet_name,
                                dtype=str,  # Read as strings to avoid type inference issues
                                na_filter=False,  # Don't convert empty strings to NaN
                            )

                            if not sheet_df.empty:
                                # Add sheet name as a column for tracking
                                sheet_df["source_sheet"] = sheet_name
                                all_sheets_data.append(sheet_df)
                                logger.debug(f"Read sheet '{sheet_name}' with {len(sheet_df)} rows")
                            else:
                                logger.debug(f"Skipping empty sheet: {sheet_name}")

                        except Exception as sheet_e:
                            logger.warning(
                                f"Failed to read sheet '{sheet_name}' from {file_path.name}: "
                                f"{str(sheet_e)}"
                            )
                            continue

                    if not all_sheets_data:
                        return TransformResult(
                            success=False, error="No readable sheets found in Excel file"
                        )

                    # Combine all sheets into one DataFrame
                    if len(all_sheets_data) == 1:
                        df = all_sheets_data[0]
                    else:
                        # Try to combine sheets - find common columns
                        common_cols = set(all_sheets_data[0].columns)
                        for sheet_df in all_sheets_data[1:]:
                            common_cols &= set(sheet_df.columns)

                        if common_cols:
                            # Combine using common columns
                            combined_sheets = []
                            for sheet_df in all_sheets_data:
                                combined_sheets.append(sheet_df[list(common_cols)])
                            df = pd.concat(combined_sheets, ignore_index=True)
                            logger.info(
                                f"Combined {len(all_sheets_data)} sheets using "
                                f"{len(common_cols)} common columns"
                            )
                        else:
                            # No common columns, just use the first sheet
                            df = all_sheets_data[0]
                            logger.warning(
                                f"No common columns found, using only first sheet from "
                                f"{file_path.name}"
                            )

                except Exception as excel_e:
                    logger.error(f"Failed to read Excel file {file_path.name}: {str(excel_e)}")
                    return TransformResult(
                        success=False, error=f"Failed to read Excel file: {str(excel_e)}"
                    )

            elif file_suffix == ".parquet":
                # Read parquet file
                df = self.conn.execute(f"SELECT * FROM read_parquet('{file_path}')").df()
            else:
                return TransformResult(
                    success=False,
                    error=f"Unsupported file type for fertiliser transformer: {file_suffix}",
                )

            if df.empty:
                return TransformResult(success=False, error="Empty data file")

            # Determine the type of fertiliser file
            filename = file_path.name
            harmonized_df = self._harmonize_fertiliser_data(df, filename)

            if harmonized_df is None or harmonized_df.empty:
                return TransformResult(success=False, error="No valid data after harmonization")

            # Create output path
            output_path = output_dir / f"{file_path.stem}_harmonized.parquet"

            # Save harmonized data
            harmonized_df.to_parquet(output_path, index=False)

            logger.info(
                f"Successfully transformed fertiliser data to: {output_path} "
                f"({len(harmonized_df)} rows)"
            )

            return TransformResult(
                success=True,
                output_path=output_path,
                rows_processed=len(harmonized_df),
                metadata={
                    "original_filename": filename,
                    "harmonized_columns": list(harmonized_df.columns),
                    "data_source": self._get_data_source(filename),
                    "data_type": self._get_data_type(filename),
                },
            )

        except Exception as e:
            logger.error(f"Failed to transform fertiliser file {file_path}: {str(e)}")
            return TransformResult(success=False, error=str(e))

    def transform_from_content(
        self, content: bytes, filename: str, metadata: dict[str, Any]
    ) -> pd.DataFrame | None:
        """
        Transform fertiliser data from file content.

        Args:
            content: File content bytes
            filename: Original filename
            metadata: File metadata

        Returns:
            Harmonized DataFrame or None if transformation failed
        """
        try:
            import os
            import tempfile
            from pathlib import Path

            # Get the original file extension
            file_suffix = Path(filename).suffix.lower()

            # Create temporary file with correct extension
            with tempfile.NamedTemporaryFile(suffix=file_suffix, delete=False) as tmp:
                tmp.write(content)
                tmp.flush()

                try:
                    # Read the file based on its extension
                    if file_suffix == ".xlsx" or file_suffix == ".xls":
                        # Read Excel file - try all sheets and combine if multiple
                        try:
                            excel_file = pd.ExcelFile(tmp.name)
                            all_sheets_data = []

                            logger.info(
                                f"Found {len(excel_file.sheet_names)} sheets in {filename}: "
                                f"{excel_file.sheet_names}"
                            )

                            for sheet_name in excel_file.sheet_names:
                                try:
                                    sheet_df = pd.read_excel(
                                        excel_file,
                                        sheet_name=sheet_name,
                                        dtype=str,  # Read as strings to avoid type inference issues
                                        na_filter=False,  # Don't convert empty strings to NaN
                                    )

                                    if not sheet_df.empty:
                                        # Add sheet name as a column for tracking
                                        sheet_df["source_sheet"] = sheet_name
                                        all_sheets_data.append(sheet_df)
                                        logger.debug(
                                            f"Read sheet '{sheet_name}' with {len(sheet_df)} rows"
                                        )
                                    else:
                                        logger.debug(f"Skipping empty sheet: {sheet_name}")

                                except Exception as sheet_e:
                                    logger.warning(
                                        f"Failed to read sheet '{sheet_name}' from {filename}: "
                                        f"{str(sheet_e)}"
                                    )
                                    continue

                            if not all_sheets_data:
                                logger.warning(
                                    f"No readable sheets found in Excel file: {filename}"
                                )
                                return None

                            # Combine all sheets into one DataFrame
                            if len(all_sheets_data) == 1:
                                df = all_sheets_data[0]
                            else:
                                # Try to combine sheets - find common columns
                                common_cols = set(all_sheets_data[0].columns)
                                for sheet_df in all_sheets_data[1:]:
                                    common_cols &= set(sheet_df.columns)

                                if common_cols:
                                    # Combine using common columns
                                    combined_sheets = []
                                    for sheet_df in all_sheets_data:
                                        combined_sheets.append(sheet_df[list(common_cols)])
                                    df = pd.concat(combined_sheets, ignore_index=True)
                                    logger.info(
                                        f"Combined {len(all_sheets_data)} sheets using "
                                        f"{len(common_cols)} common columns"
                                    )
                                else:
                                    # No common columns, just use the first sheet
                                    df = all_sheets_data[0]
                                    logger.warning(
                                        f"No common columns found, using only first sheet from "
                                        f"{filename}"
                                    )

                        except Exception as excel_e:
                            logger.error(f"Failed to read Excel file {filename}: {str(excel_e)}")
                            return None

                    elif file_suffix == ".parquet":
                        # Read parquet file
                        df = self.conn.execute(f"SELECT * FROM read_parquet('{tmp.name}')").df()
                    else:
                        logger.error(
                            f"Unsupported file type for fertiliser transformer: {file_suffix}"
                        )
                        return None

                    if df.empty:
                        logger.warning(f"Empty data file: {filename}")
                        return None

                    # Harmonize the data
                    harmonized_df = self._harmonize_fertiliser_data(df, filename)

                    if harmonized_df is not None and not harmonized_df.empty:
                        logger.info(
                            f"Successfully harmonized fertiliser data from: {filename} "
                            f"({len(harmonized_df)} rows)"
                        )
                    else:
                        logger.warning(f"No data after harmonization for: {filename}")

                    return harmonized_df

                finally:
                    # Clean up temporary file
                    if os.path.exists(tmp.name):
                        os.unlink(tmp.name)

        except Exception as e:
            logger.error(f"Failed to transform fertiliser content from {filename}: {str(e)}")
            return None

    def _harmonize_fertiliser_data(self, df: pd.DataFrame, filename: str) -> pd.DataFrame | None:
        """
        Harmonize fertiliser data based on the file type.

        Args:
            df: Raw DataFrame
            filename: Source filename to determine processing type

        Returns:
            Harmonized DataFrame
        """
        try:
            filename_lower = filename.lower()

            if "efterafgrøder" in filename_lower or "efterafgroeder" in filename_lower:
                return self._process_efterafgroeder(df, filename)
            elif "gkea" in filename_lower:
                return self._process_gkea(df, filename)
            elif (
                "gødningsregnskaber" in filename_lower
                or "goedningsregnskaber" in filename_lower
                or any(
                    pattern in filename_lower for pattern in ["b_", "v_", "erklrk", "lg_company"]
                )
            ):
                return self._process_goedningsregnskaber(df, filename)
            else:
                logger.warning(f"Unknown fertiliser file type: {filename}")
                return self._process_generic_fertiliser(df, filename)

        except Exception as e:
            logger.error(f"Failed to harmonize fertiliser data from {filename}: {str(e)}")
            return None

    def _process_efterafgroeder(self, df: pd.DataFrame, filename: str) -> pd.DataFrame:
        """Process Efterafgrøder (cover crops) files."""
        logger.info(f"Processing Efterafgrøder file: {filename}")

        # If DataFrame is empty, return empty harmonized DataFrame
        if df.empty:
            return pd.DataFrame(columns=list(self.harmonized_schema.keys()))

        # Create standardized DataFrame using actual column mappings from the data
        harmonized_data = {
            "data_source": ["efterafgroeder"] * len(df),
            "year": df["PROD_AAR"].astype(str)
            if "PROD_AAR" in df.columns
            else pd.Series([pd.NA] * len(df)),
            "cvr_number": df["CVR"].astype(str)
            if "CVR" in df.columns
            else pd.Series([pd.NA] * len(df)),
            "capnumber": df["CapNumber"]
            if "CapNumber" in df.columns
            else pd.Series([pd.NA] * len(df)),
            "markbloknummer": df["MARKBLOKNUMMER"]
            if "MARKBLOKNUMMER" in df.columns
            else pd.Series([pd.NA] * len(df)),
            "marknummer": df["MARKNUMMER"]
            if "MARKNUMMER" in df.columns
            else pd.Series([pd.NA] * len(df)),
            "indberet_alternativ": pd.Series([pd.NA] * len(df)),
            "faktisk_areal_ha": pd.Series([pd.NA] * len(df)),
            "omregnet_areal_ha": pd.Series([pd.NA] * len(df)),
            "journal_nummer": pd.Series([pd.NA] * len(df)),
            "total_n_kvote": pd.Series([pd.NA] * len(df)),
            "fosfortal": pd.Series([pd.NA] * len(df)),
            "data_type": ["Efterafgrøder"] * len(df),
            "data_source_file": [filename] * len(df),
        }

        # Map year-specific columns for Efterafgrøder using actual column names
        if "A19_INDBERETEFTERAFGALTERNATIV" in df.columns:
            # 2023 format (based on inspection)
            harmonized_data["indberet_alternativ"] = df["A19_INDBERETEFTERAFGALTERNATIV"]
            harmonized_data["faktisk_areal_ha"] = (
                pd.to_numeric(
                    df["A20_FAKTISKHAUDLAGTEAALTERNATIV"].astype(str).str.replace(",", "."),
                    errors="coerce",
                )
                if "A20_FAKTISKHAUDLAGTEAALTERNATIV" in df.columns
                else pd.Series([pd.NA] * len(df))
            )
            harmonized_data["omregnet_areal_ha"] = (
                pd.to_numeric(
                    df["A23_OMREGNETHAMEDEA"].astype(str).str.replace(",", "."), errors="coerce"
                )
                if "A23_OMREGNETHAMEDEA" in df.columns
                else pd.Series([pd.NA] * len(df))
            )
        elif "A18_INDBERETEFTERAFGALTERNATIV" in df.columns:
            # 2020 format
            harmonized_data["indberet_alternativ"] = df["A18_INDBERETEFTERAFGALTERNATIV"]
            harmonized_data["faktisk_areal_ha"] = (
                pd.to_numeric(
                    df["A19_FAKTISKHAUDLAGTEAALTERNATIV"].astype(str).str.replace(",", "."),
                    errors="coerce",
                )
                if "A19_FAKTISKHAUDLAGTEAALTERNATIV" in df.columns
                else pd.Series([pd.NA] * len(df))
            )
            harmonized_data["omregnet_areal_ha"] = (
                pd.to_numeric(
                    df["A20_OMREGNETHAMEDEA"].astype(str).str.replace(",", "."), errors="coerce"
                )
                if "A20_OMREGNETHAMEDEA" in df.columns
                else pd.Series([pd.NA] * len(df))
            )
        elif "A20_INDBERETEFTERAFGALTERNATIV" in df.columns:
            # 2022 format
            harmonized_data["indberet_alternativ"] = df["A20_INDBERETEFTERAFGALTERNATIV"]
            harmonized_data["faktisk_areal_ha"] = (
                pd.to_numeric(
                    df["A21_FAKTISKHAUDLAGTEAALTERNATIV"].astype(str).str.replace(",", "."),
                    errors="coerce",
                )
                if "A21_FAKTISKHAUDLAGTEAALTERNATIV" in df.columns
                else pd.Series([pd.NA] * len(df))
            )
            harmonized_data["omregnet_areal_ha"] = (
                pd.to_numeric(
                    df["A24_OMREGNETHAMEDEA"].astype(str).str.replace(",", "."), errors="coerce"
                )
                if "A24_OMREGNETHAMEDEA" in df.columns
                else pd.Series([pd.NA] * len(df))
            )

        return pd.DataFrame(harmonized_data)

    def _process_gkea(self, df: pd.DataFrame, filename: str) -> pd.DataFrame:
        """Process GKEA markplan files."""
        logger.info(f"Processing GKEA file: {filename}")

        # If DataFrame is empty, return empty harmonized DataFrame
        if df.empty:
            return pd.DataFrame(columns=list(self.harmonized_schema.keys()))

        # GKEA files have different column structures by year
        # Extract year from filename
        year = None
        for yr in ["2021", "2022", "2023", "2024"]:
            if yr in filename:
                year = yr
                break

        # Map GKEA columns based on expected patterns
        # Skip header rows if they exist
        if len(df) > 2:
            df_data = df.iloc[2:].copy()  # Skip potential header rows
        else:
            df_data = df.copy()

        # If processed DataFrame is empty, return empty harmonized DataFrame
        if df_data.empty:
            return pd.DataFrame(columns=list(self.harmonized_schema.keys()))

        # Create base harmonized structure using DataFrame length as index
        num_rows = len(df_data)
        harmonized_data = {
            "data_source": ["gkea"] * num_rows,
            "year": [year] * num_rows,
            "cvr_number": pd.Series([pd.NA] * num_rows),
            "capnumber": pd.Series([pd.NA] * num_rows),
            "markbloknummer": pd.Series([pd.NA] * num_rows),
            "marknummer": pd.Series([pd.NA] * num_rows),
            "indberet_alternativ": pd.Series([pd.NA] * num_rows),
            "faktisk_areal_ha": pd.Series([pd.NA] * num_rows),
            "omregnet_areal_ha": pd.Series([pd.NA] * num_rows),
            "journal_nummer": pd.Series([pd.NA] * num_rows),
            "total_n_kvote": pd.Series([pd.NA] * num_rows),
            "fosfortal": pd.Series([pd.NA] * num_rows),
            "data_type": ["GKEA Markplan"] * num_rows,
            "data_source_file": [filename] * num_rows,
        }

        # Try to map columns based on common GKEA patterns
        cols = df_data.columns.tolist()

        if len(cols) > 1:
            harmonized_data["journal_nummer"] = df_data.iloc[:, 0]  # First column usually journal
            harmonized_data["cvr_number"] = df_data.iloc[:, 1]  # Second column usually CVR

        # Map other columns based on year-specific patterns
        if year == "2021" and len(cols) > 14:
            harmonized_data["marknummer"] = df_data.iloc[:, 5]
            harmonized_data["faktisk_areal_ha"] = pd.to_numeric(
                df_data.iloc[:, 6].astype(str).str.replace(",", "."), errors="coerce"
            )
            harmonized_data["omregnet_areal_ha"] = pd.to_numeric(
                df_data.iloc[:, 10].astype(str).str.replace(",", "."), errors="coerce"
            )
            harmonized_data["indberet_alternativ"] = df_data.iloc[:, 14]
            if len(cols) > 19:
                harmonized_data["fosfortal"] = pd.to_numeric(
                    df_data.iloc[:, 19].astype(str).str.replace(",", "."), errors="coerce"
                )
        elif year == "2022" and len(cols) > 12:
            harmonized_data["marknummer"] = df_data.iloc[:, 3]
            harmonized_data["faktisk_areal_ha"] = pd.to_numeric(
                df_data.iloc[:, 4].astype(str).str.replace(",", "."), errors="coerce"
            )
            harmonized_data["omregnet_areal_ha"] = pd.to_numeric(
                df_data.iloc[:, 8].astype(str).str.replace(",", "."), errors="coerce"
            )
            harmonized_data["indberet_alternativ"] = df_data.iloc[:, 12]
            if len(cols) > 17:
                harmonized_data["fosfortal"] = pd.to_numeric(
                    df_data.iloc[:, 17].astype(str).str.replace(",", "."), errors="coerce"
                )
        elif year in ["2023", "2024"] and len(cols) > 10:
            harmonized_data["marknummer"] = df_data.iloc[:, 3]
            harmonized_data["faktisk_areal_ha"] = pd.to_numeric(
                df_data.iloc[:, 4].astype(str).str.replace(",", "."), errors="coerce"
            )
            harmonized_data["omregnet_areal_ha"] = pd.to_numeric(
                df_data.iloc[:, 6].astype(str).str.replace(",", "."), errors="coerce"
            )
            harmonized_data["indberet_alternativ"] = df_data.iloc[:, 10]
            if len(cols) > 19:
                harmonized_data["total_n_kvote"] = pd.to_numeric(
                    df_data.iloc[:, 19].astype(str).str.replace(",", "."), errors="coerce"
                )

        return pd.DataFrame(harmonized_data)

    def _process_goedningsregnskaber(self, df: pd.DataFrame, filename: str) -> pd.DataFrame:
        """Process Gødningsregnskaber (fertilizer accounts) files and In-depth files."""
        logger.info(f"Processing Gødningsregnskaber/In-depth file: {filename}")

        # If DataFrame is empty, return empty harmonized DataFrame
        if df.empty:
            return pd.DataFrame(columns=list(self.harmonized_schema.keys()))

        # Extract year from filename or folder structure
        year = None
        for yr in ["2018", "2019", "2020", "2021", "2022", "2023", "2024"]:
            if yr in filename:
                year = yr
                break

        # Determine data source and type based on filename
        filename_lower = filename.lower()
        if "gødningsregnskaber" in filename_lower or "goedningsregnskaber" in filename_lower:
            data_source = "goedningsregnskaber"
            data_type = "Gødningsregnskaber"
        elif "b_" in filename_lower:
            data_source = "goedningsregnskaber_blok"
            data_type = "Gødningsregnskaber Blok"
        elif "v_" in filename_lower:
            data_source = "goedningsregnskaber_vurdering"
            data_type = "Gødningsregnskaber Vurdering"
        else:
            data_source = "goedningsregnskaber_other"
            data_type = "Gødningsregnskaber Other"

        # Create standardized DataFrame using actual column mappings from the data
        harmonized_data = {
            "data_source": [data_source] * len(df),
            "year": [year] * len(df),
            "cvr_number": df["CVR"].astype(str)
            if "CVR" in df.columns
            else pd.Series([pd.NA] * len(df)),
            "capnumber": pd.Series([pd.NA] * len(df)),
            "markbloknummer": pd.Series([pd.NA] * len(df)),
            "marknummer": pd.Series([pd.NA] * len(df)),
            "indberet_alternativ": pd.Series([pd.NA] * len(df)),
            "faktisk_areal_ha": pd.Series([pd.NA] * len(df)),
            "omregnet_areal_ha": pd.Series([pd.NA] * len(df)),
            "journal_nummer": df["NUMMER"]
            if "NUMMER" in df.columns
            else pd.Series([pd.NA] * len(df)),
            "total_n_kvote": pd.Series([pd.NA] * len(df)),
            "fosfortal": pd.Series([pd.NA] * len(df)),
            "data_type": [data_type] * len(df),
            "data_source_file": [filename] * len(df),
        }

        return pd.DataFrame(harmonized_data)

    def _process_generic_fertiliser(self, df: pd.DataFrame, filename: str) -> pd.DataFrame:
        """Process generic fertiliser files."""
        logger.info(f"Processing generic fertiliser file: {filename}")

        # If DataFrame is empty, return empty harmonized DataFrame
        if df.empty:
            return pd.DataFrame(columns=list(self.harmonized_schema.keys()))

        # Create minimal harmonized structure using DataFrame length as index
        num_rows = len(df)
        harmonized_data = {
            "data_source": ["generic"] * num_rows,
            "year": pd.Series([pd.NA] * num_rows),
            "cvr_number": df.get("cvr_number", pd.Series([pd.NA] * num_rows)).reset_index(
                drop=True
            ),
            "capnumber": pd.Series([pd.NA] * num_rows),
            "markbloknummer": pd.Series([pd.NA] * num_rows),
            "marknummer": pd.Series([pd.NA] * num_rows),
            "indberet_alternativ": pd.Series([pd.NA] * num_rows),
            "faktisk_areal_ha": pd.Series([pd.NA] * num_rows),
            "omregnet_areal_ha": pd.Series([pd.NA] * num_rows),
            "journal_nummer": pd.Series([pd.NA] * num_rows),
            "total_n_kvote": pd.Series([pd.NA] * num_rows),
            "fosfortal": pd.Series([pd.NA] * num_rows),
            "data_type": ["Generic Fertiliser"] * num_rows,
            "data_source_file": [filename] * num_rows,
        }

        return pd.DataFrame(harmonized_data)

    def _get_data_source(self, filename: str) -> str:
        """Determine data source from filename."""
        filename_lower = filename.lower()

        if "efterafgrøder" in filename_lower or "efterafgroeder" in filename_lower:
            return "efterafgroeder"
        elif "gkea" in filename_lower:
            return "gkea"
        elif "gødningsregnskaber" in filename_lower or "goedningsregnskaber" in filename_lower:
            return "goedningsregnskaber"
        elif "b_" in filename_lower:
            return "goedningsregnskaber_blok"
        elif "v_" in filename_lower:
            return "goedningsregnskaber_vurdering"
        else:
            return "generic"

    def _get_data_type(self, filename: str) -> str:
        """Determine data type from filename."""
        filename_lower = filename.lower()

        if "efterafgrøder" in filename_lower or "efterafgroeder" in filename_lower:
            return "Efterafgrøder"
        elif "gkea" in filename_lower:
            return "GKEA Markplan"
        elif "gødningsregnskaber" in filename_lower or "goedningsregnskaber" in filename_lower:
            return "Gødningsregnskaber"
        elif "b_" in filename_lower:
            return "Gødningsregnskaber Blok"
        elif "v_" in filename_lower:
            return "Gødningsregnskaber Vurdering"
        else:
            return "Generic Fertiliser"

    def get_expected_schema(self) -> TableSchema | None:
        """
        Get the expected schema for harmonized fertiliser data.

        Returns:
            TableSchema for the harmonized output
        """
        columns = []
        for col_name, col_type in self.harmonized_schema.items():
            columns.append(
                ColumnSchema(
                    name=col_name,
                    data_type=col_type,
                    nullable=True,  # Most columns are nullable in fertiliser data
                    description=f"Harmonized {col_name} column",
                )
            )

        return TableSchema(
            name="fertiliser_harmonized",
            columns=columns,
            description="Harmonized Danish fertiliser data from multiple sources",
        )
