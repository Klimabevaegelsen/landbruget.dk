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

logger = logging.getLogger(__name__)


class FertiliserTransformer(BaseTransformer):
    """Transformer for fertiliser parquet files."""

    def __init__(self) -> None:
        """Initialize the fertiliser transformer."""
        super().__init__()
        self.conn = duckdb.connect()
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

        # Check for fertiliser-related files
        fertiliser_patterns = [
            "efterafgrøder",
            "efterafgroeder",
            "gkea",
            "gødningsregnskaber",
            "goedningsregnskaber",
            "fertiliser",
            "fertilizer",
        ]

        return any(pattern in filename for pattern in fertiliser_patterns)

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

            # Read the parquet file
            df = self.conn.execute(f"SELECT * FROM read_parquet('{file_path}')").df()

            if df.empty:
                return TransformResult(success=False, error="Empty parquet file")

            # Determine the type of fertiliser file
            filename = file_path.name
            harmonized_df = self._harmonize_fertiliser_data(df, filename)

            if harmonized_df is None or harmonized_df.empty:
                return TransformResult(success=False, error="No valid data after harmonization")

            # Create output path
            output_path = output_dir / f"{file_path.stem}_harmonized.parquet"

            # Save harmonized data
            harmonized_df.to_parquet(output_path, index=False)

            logger.info(f"Successfully transformed fertiliser data to: {output_path}")

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

            # Create temporary file
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                tmp.write(content)
                tmp.flush()

                try:
                    # Read the parquet file
                    df = self.conn.execute(f"SELECT * FROM read_parquet('{tmp.name}')").df()

                    if df.empty:
                        logger.warning(f"Empty parquet file: {filename}")
                        return None

                    # Harmonize the data
                    harmonized_df = self._harmonize_fertiliser_data(df, filename)

                    logger.info(f"Successfully harmonized fertiliser data from: {filename}")
                    return harmonized_df

                finally:
                    # Clean up temporary file
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
            elif "gødningsregnskaber" in filename_lower or "goedningsregnskaber" in filename_lower:
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

        # Create standardized DataFrame
        harmonized_data = {
            "data_source": "efterafgroeder",
            "year": df.get("prod_aar", pd.NA),
            "cvr_number": df.get("cvr_number", pd.NA),
            "capnumber": df.get("capnumber", pd.NA),
            "markbloknummer": df.get("markbloknummer", pd.NA),
            "marknummer": df.get("marknummer", pd.NA),
            "journal_nummer": pd.NA,
            "total_n_kvote": pd.NA,
            "fosfortal": pd.NA,
            "data_type": "Efterafgrøder",
            "data_source_file": filename,
        }

        # Map year-specific columns for Efterafgrøder
        if "a18_indberetefterafgalternativ" in df.columns:
            # 2020 format
            harmonized_data["indberet_alternativ"] = df["a18_indberetefterafgalternativ"]
            harmonized_data["faktisk_areal_ha"] = pd.to_numeric(
                df["a19_faktiskhaudlagteaalternativ"].astype(str).str.replace(",", "."),
                errors="coerce",
            )
            harmonized_data["omregnet_areal_ha"] = pd.to_numeric(
                df["a20_omregnethamedea"].astype(str).str.replace(",", "."), errors="coerce"
            )
        elif "a19_indberetefterafgalternativ" in df.columns:
            # 2021 or 2023 format
            harmonized_data["indberet_alternativ"] = df["a19_indberetefterafgalternativ"]
            harmonized_data["faktisk_areal_ha"] = pd.to_numeric(
                df["a20_faktiskhaudlagteaalternativ"].astype(str).str.replace(",", "."),
                errors="coerce",
            )
            if "a21_omregnethamedea" in df.columns:
                # 2021 format
                harmonized_data["omregnet_areal_ha"] = pd.to_numeric(
                    df["a21_omregnethamedea"].astype(str).str.replace(",", "."), errors="coerce"
                )
            elif "a23_omregnethamedea" in df.columns:
                # 2023 format
                harmonized_data["omregnet_areal_ha"] = pd.to_numeric(
                    df["a23_omregnethamedea"].astype(str).str.replace(",", "."), errors="coerce"
                )
        elif "a20_indberetefterafgalternativ" in df.columns:
            # 2022 format
            harmonized_data["indberet_alternativ"] = df["a20_indberetefterafgalternativ"]
            harmonized_data["faktisk_areal_ha"] = pd.to_numeric(
                df["a21_faktiskhaudlagteaalternativ"].astype(str).str.replace(",", "."),
                errors="coerce",
            )
            harmonized_data["omregnet_areal_ha"] = pd.to_numeric(
                df["a24_omregnethamedea"].astype(str).str.replace(",", "."), errors="coerce"
            )
        else:
            # Generic mapping for unknown formats
            harmonized_data["indberet_alternativ"] = pd.NA
            harmonized_data["faktisk_areal_ha"] = pd.NA
            harmonized_data["omregnet_areal_ha"] = pd.NA

        return pd.DataFrame(harmonized_data)

    def _process_gkea(self, df: pd.DataFrame, filename: str) -> pd.DataFrame:
        """Process GKEA markplan files."""
        logger.info(f"Processing GKEA file: {filename}")

        # GKEA files have different column structures by year
        # Extract year from filename
        year = None
        for yr in ["2021", "2022", "2023", "2024"]:
            if yr in filename:
                year = yr
                break

        # Create base harmonized structure
        harmonized_data = {
            "data_source": "gkea",
            "year": year,
            "cvr_number": pd.NA,
            "capnumber": pd.NA,
            "markbloknummer": pd.NA,
            "marknummer": pd.NA,
            "indberet_alternativ": pd.NA,
            "faktisk_areal_ha": pd.NA,
            "omregnet_areal_ha": pd.NA,
            "journal_nummer": pd.NA,
            "total_n_kvote": pd.NA,
            "fosfortal": pd.NA,
            "data_type": "GKEA Markplan",
            "data_source_file": filename,
        }

        # Map GKEA columns based on expected patterns
        # Skip header rows if they exist
        if len(df) > 2:
            df_data = df.iloc[2:].copy()  # Skip potential header rows
        else:
            df_data = df.copy()

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
        """Process Gødningsregnskaber (fertilizer accounts) files."""
        logger.info(f"Processing Gødningsregnskaber file: {filename}")

        # Create standardized DataFrame
        harmonized_data = {
            "data_source": "goedningsregnskaber",
            "year": df.get("f_planaar", pd.NA).astype(str) if "f_planaar" in df.columns else pd.NA,
            "cvr_number": df.get("cvr_number", pd.NA),
            "capnumber": pd.NA,
            "markbloknummer": pd.NA,
            "marknummer": pd.NA,
            "indberet_alternativ": pd.NA,
            "faktisk_areal_ha": pd.NA,
            "omregnet_areal_ha": pd.NA,
            "journal_nummer": pd.NA,
            "total_n_kvote": pd.NA,
            "fosfortal": pd.NA,
            "data_type": "Gødningsregnskaber",
            "data_source_file": filename,
        }

        return pd.DataFrame(harmonized_data)

    def _process_generic_fertiliser(self, df: pd.DataFrame, filename: str) -> pd.DataFrame:
        """Process generic fertiliser files."""
        logger.info(f"Processing generic fertiliser file: {filename}")

        # Create minimal harmonized structure
        harmonized_data = {
            "data_source": "generic",
            "year": pd.NA,
            "cvr_number": df.get("cvr_number", pd.NA),
            "capnumber": pd.NA,
            "markbloknummer": pd.NA,
            "marknummer": pd.NA,
            "indberet_alternativ": pd.NA,
            "faktisk_areal_ha": pd.NA,
            "omregnet_areal_ha": pd.NA,
            "journal_nummer": pd.NA,
            "total_n_kvote": pd.NA,
            "fosfortal": pd.NA,
            "data_type": "Generic Fertiliser",
            "data_source_file": filename,
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
