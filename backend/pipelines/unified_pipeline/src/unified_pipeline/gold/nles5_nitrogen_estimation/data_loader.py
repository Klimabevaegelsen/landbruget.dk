"""
NLES5 Data Loading Module

This module handles all data loading operations for the NLES5 nitrogen estimation pipeline,
including agricultural fields data, fertilizer data, field plans, catch crops, and DMI climate data.

All methods maintain the exact same functionality and error handling as the original implementation.
"""

import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from unified_pipeline.util.timing import timed


class NLES5DataLoader:
    """
    Data loading operations for NLES5 nitrogen estimation.

    This class handles loading and preprocessing of all input datasets required
    for NLES5 calculations, including agricultural fields, fertilizer data,
    climate data, and supporting datasets.
    """

    def __init__(self, processor):
        """
        Initialize the data loader with reference to the main processor.

        Args:
            processor: The main NLES5NitrogenEstimationGold processor instance
        """
        self.processor = processor
        self.config = processor.config
        self.log = processor.log
        self.gcs_access = processor.gcs_access
        self.db = processor.conn

    def _get_available_fvm_marker_years(self) -> List[int]:
        """
        Get all available fvm_marker years from GCS storage.

        Returns:
            List of available years for fvm_marker datasets
        """
        years: Set[int] = set()

        # Primary: discover from GCS using the correct fvm_marker path pattern
        try:
            files = self.gcs_access.list_files(
                f"gs://{self.config.bucket}/silver/{self.config.agricultural_fields_dataset}_*/*/*"
            )
            for file_path in files:
                match = re.search(
                    r"silver/fvm_marker_(\d{4})/.*?/(?:fvm_marker_(\d{4})\.parquet|data\.parquet)",
                    file_path,
                )
                if match:
                    year1 = int(match.group(1))
                    year2 = match.group(2)
                    if year2:
                        year2_int = int(year2)
                        if year1 == year2_int:
                            years.add(year1)
                    else:
                        years.add(year1)
        except Exception as e:
            self.log.error(f"Error discovering FVM marker years from GCS: {e}")

        # Secondary: derive from hardcoded known years if GCS discovery failed
        if not years:
            self.log.info(
                "GCS discovery returned empty, using hardcoded fallback "
                "years based on GCS tree analysis"
            )

        # Tertiary: use hardcoded fallback if GCS discovery failed (network issues)
        if not years:
            self.log.warning("⚠️ GCS discovery failed - using hardcoded fallback years")
            # Based on GCS tree analysis, these years are available:
            fallback_years = [
                2005,
                2006,
                2007,
                2008,
                2009,
                2010,
                2011,
                2012,
                2013,
                2014,
                2015,
                2016,
                2017,
                2018,
                2019,
                2020,
                2021,
                2022,
                2023,
                2024,
                2025,
                2026,
            ]
            years.update(fallback_years)
            self.log.info(f"Using hardcoded fallback FVM years from GCS tree: {sorted(years)}")

        return sorted(list(years))

    def _read_fvm_marker_data_for_year(self, year: int) -> Optional[str]:
        """
        Read FVM marker data for a specific year using dynamic timestamped directory discovery.

        Args:
            year: The year to read data for

        Returns:
            GCS path to the FVM marker data file, or None if not found
        """
        try:
            # Use dynamic discovery to find the latest timestamped directory for this year
            base_path = f"gs://{self.config.bucket}/silver/{self.config.agricultural_fields_dataset}_{year}/"
            latest_dir = self._get_latest_timestamped_directory(base_path, f"FVM marker {year}")

            if not latest_dir:
                self.log.warning(f"No FVM marker data found for year {year}")
                return None

            # Try different file naming patterns in the latest directory
            possible_files = [
                f"{latest_dir}data.parquet",
                f"{latest_dir}{self.config.agricultural_fields_dataset}_{year}.parquet",
            ]

            for file_path in possible_files:
                if self.gcs_access.file_exists(file_path):
                    self.log.info(f"✅ Found FVM marker data for {year}: {file_path}")
                    return file_path

            # If specific files not found, try to find any parquet file in the directory
            pattern = f"{latest_dir}*.parquet"
            files = self.gcs_access.list_files(pattern)
            if files:
                file_path = files[0]
                self.log.info(f"✅ Found FVM marker data for {year} (any parquet): {file_path}")
                return file_path

            self.log.warning(
                f"No parquet files found in FVM marker directory for year {year}: {latest_dir}"
            )
            return None

        except Exception as e:
            self.log.error(f"Error finding FVM marker data for year {year}: {e}")
            return None

    def _get_latest_timestamped_directory(
        self, base_path: str, dataset_name: str = "dataset"
    ) -> Optional[str]:
        """
        Get the latest timestamped directory from a base GCS path.

        This method dynamically discovers timestamped directories (format: YYYYMMDD_HHMMSS)
        and returns the path to the most recent one.

        Args:
            base_path: Base GCS path (e.g., "gs://bucket/silver/dataset/")
            dataset_name: Dataset name for logging purposes

        Returns:
            Path to latest timestamped directory with trailing slash, or None if not found
        """
        import re

        try:
            # Ensure base path ends with /
            if not base_path.endswith("/"):
                base_path += "/"

            self.log.debug(f"🔍 Searching for timestamped directories in: {base_path}")

            # Try to find files in timestamped subdirectories
            # This will list all files in all subdirectories
            pattern = f"{base_path}*/*.parquet"
            files = self.gcs_access.list_files(pattern)

            if not files:
                self.log.warning(f"⚠️ No files found for {dataset_name} using pattern: {pattern}")
                # Try to list just directories to see what's there
                try:
                    dir_pattern = f"{base_path}*/"
                    dirs = self.gcs_access.list_files(dir_pattern)
                    if dirs:
                        self.log.warning(
                            f"   Found {len(dirs)} subdirectories but no parquet files:"
                        )
                        for d in dirs[:5]:
                            self.log.warning(f"   - {d}")
                    else:
                        self.log.warning(f"   No subdirectories found in {base_path}")
                except Exception as e:
                    self.log.debug(f"   Could not list directories: {e}")
                return None

            self.log.debug(f"   Found {len(files)} parquet files in subdirectories")

            # Extract unique timestamped directories from file paths
            timestamped_dirs = {}
            non_timestamped_files = []

            for file_path in files:
                # Extract timestamp directory from path like:
                # gs://.../silver/fertiliser/20250803_205033/file.parquet
                match = re.search(r"/(\d{8}_\d{6})/", file_path)
                if match:
                    timestamp = match.group(1)
                    # Extract directory path up to and including the timestamp
                    dir_path = file_path[: file_path.index(timestamp) + len(timestamp)] + "/"
                    timestamped_dirs[timestamp] = dir_path
                else:
                    non_timestamped_files.append(file_path)

            if not timestamped_dirs:
                self.log.warning(f"⚠️ No timestamped directories found for {dataset_name}")
                if non_timestamped_files:
                    self.log.warning(
                        f"   Found {len(non_timestamped_files)} files in "
                        f"non-timestamped directories:"
                    )
                    for f in non_timestamped_files[:3]:
                        self.log.warning(f"   - {f}")
                return None

            self.log.debug(f"   Found {len(timestamped_dirs)} unique timestamped directories")

            # Sort by timestamp (string comparison works for YYYYMMDD_HHMMSS format)
            # and get the latest
            latest_timestamp = sorted(timestamped_dirs.keys(), reverse=True)[0]
            latest_dir = timestamped_dirs[latest_timestamp]

            self.log.info(
                f"✅ Found latest {dataset_name} directory: {latest_dir} "
                f"(timestamp: {latest_timestamp})"
            )
            return latest_dir

        except Exception as e:
            self.log.error(
                f"❌ Failed to discover latest timestamped directory for {dataset_name}: {e}"
            )
            import traceback

            self.log.debug(f"   Traceback: {traceback.format_exc()}")
        return None

    def _get_fertilizer_data_path(self, target_year: int = None) -> str:
        """
        Get the path to fertilizer data from the fertiliser directory structure.

        Args:
            target_year: Optional target year to match

        Returns:
            GCS path to fertilizer data directory (contains GKEA and Efterafgrøder files)
        """
        try:
            base_path = f"gs://{self.config.bucket}/silver/{self.config.fertilizer_dataset}/"
            latest_dir = self._get_latest_timestamped_directory(base_path, "fertilizer")

            if latest_dir:
                return latest_dir

            # If discovery fails, raise an error instead of using hardcoded paths
            raise FileNotFoundError(
                f"No fertilizer data found in {base_path}. "
                f"Please ensure the silver layer has been processed."
            )

        except Exception as e:
            self.log.error(f"Fertilizer directory discovery failed: {e}")
            raise

    def _get_fertilizer_accounts_file_path(self, target_year: int = None) -> str:
        """
        Get the specific path to fertilizer accounts data file (Gødningsregnskab - GR).

        Looks for year-specific GR data in 'gr {year}/' directories first,
        with correct file patterns:
        - 2019: B_GOEDRK_6B.parquet
        - 2023+: V_4061GR_{YY}_ISKV1_B_GOEDRK_6B.parquet

        Falls back to 'fertiliser/' directory if year-specific not found.

        Args:
            target_year: Optional target year to match

        Returns:
            GCS path to specific fertilizer accounts parquet file, or None if not found
        """
        try:
            # Strategy 1: Try year-specific GR directory first (e.g., "gr 2019/", "gr 2023/")
            if target_year:
                gr_dir_path = f"gs://{self.config.bucket}/silver/gr {target_year}/"

                try:
                    # Get latest timestamped directory within the gr year directory
                    dirs = self.gcs_access.list_files(f"{gr_dir_path}*/")
                    if dirs:
                        latest_dir = sorted([d for d in dirs if d.endswith("/")])[-1]
                        self.log.info(f"🔍 Checking year-specific GR directory: {latest_dir}")

                        # Define year-specific file patterns for Gødningsregnskab (GR) data
                        # B_GOEDRK_6B = Bedrift Gødningsregnskab (company fertilizer accounts)
                        file_patterns = [
                            f"{latest_dir}B_GOEDRK_6B.parquet",  # 2019 pattern
                            # 2021+ pattern (e.g., V_4061GR_21_...)
                            f"{latest_dir}V_4061GR_{target_year % 100:02d}_ISKV1_B_GOEDRK_6B"
                            ".parquet",
                            # Alternative with full year
                            f"{latest_dir}V_4061GR_{target_year}_ISKV1_B_GOEDRK_6B.parquet",
                        ]

                        # Try each pattern
                        for pattern in file_patterns:
                            files = self.gcs_access.list_files(pattern)
                            if files:
                                selected_file = files[0]
                                self.log.info(
                                    f"✅ Found year-specific GR data for {target_year}: "
                                    f"{selected_file}"
                                )
                                return selected_file

                        # If exact patterns don't match, try wildcard search in the directory
                        all_files = self.gcs_access.list_files(f"{latest_dir}*.parquet")
                        gr_files = [f for f in all_files if "GOEDRK" in f or "B_GOEDRK" in f]
                        if gr_files:
                            selected_file = gr_files[0]
                            self.log.info(
                                f"✅ Found GR file (wildcard match) for {target_year}: "
                                f"{selected_file}"
                            )
                            return selected_file

                        self.log.warning(f"⚠️ No GR fertilizer data found in {latest_dir}")
                except Exception as e:
                    self.log.info(f"Year-specific gr {target_year}/ directory not available: {e}")

            # Strategy 2: Fall back to main fertiliser/ directory (legacy location)
            self.log.info("⏭️ Falling back to main fertiliser/ directory")
            fertilizer_dir = self._get_fertilizer_data_path(target_year)

            # List all files in the directory to find fertilizer accounts files
            pattern = f"{fertilizer_dir}*.parquet"
            files = self.gcs_access.list_files(pattern)

            # Look for files that match fertilizer accounts pattern (Gødningsregnskaber)
            fertilizer_accounts_files = []
            for file_path in files:
                if "Gødningsregnskaber" in file_path or "fertilizer" in file_path.lower():
                    # Extract year from filename (look for 4 digits that are not part of timestamp)
                    import re

                    # Get just the filename, not the full path to avoid timestamp confusion
                    filename = file_path.split("/")[-1]
                    year_match = re.search(r"(\d{4})", filename)
                    if year_match:
                        file_year = int(year_match.group(1))
                        fertilizer_accounts_files.append((file_year, file_path))
                    else:
                        # If no year found, use as fallback
                        fertilizer_accounts_files.append((0, file_path))

            if fertilizer_accounts_files:
                # If target_year specified, prefer closest year match
                if target_year:
                    # Sort by proximity to target year
                    fertilizer_accounts_files.sort(
                        key=lambda x: abs(x[0] - target_year) if x[0] > 0 else 9999
                    )
                    selected_year, selected_file = fertilizer_accounts_files[0]
                    if selected_year != target_year:
                        self.log.warning(
                            f"⚠️ Using {selected_year} fertilizer data as proxy for {target_year}"
                        )
                else:
                    # Sort by year (descending) and get the most recent
                    fertilizer_accounts_files.sort(reverse=True)
                    selected_year, selected_file = fertilizer_accounts_files[0]

                self.log.info(
                    f"Found fertilizer accounts file: {selected_file} (year: {selected_year})"
                )
                return selected_file
            else:
                self.log.warning(f"❌ No fertilizer accounts files found in {fertilizer_dir}")
                return None

        except Exception as e:
            self.log.error(f"Error finding fertilizer accounts file: {e}")
            return None

    def _get_field_plan_data_path(self, target_year: int = None) -> str:
        """
        Get the path to GKEA field plan data from the fertiliser directory.

        Args:
            target_year: Optional target year to match

        Returns:
            GCS path to GKEA field plan data
        """
        try:
            # Get the fertiliser base directory (will raise exception if not found)
            fertiliser_dir = self._get_fertilizer_data_path(target_year)
            self.log.info(f"🔍 Looking for GKEA field plan files in: {fertiliser_dir}")

            # Look for GKEA field plan files with correct patterns
            # Always look for the most recent GKEA file since field plans are typically multi-year
            patterns = [
                f"{fertiliser_dir}GKEA*_Markplan_med_Gødningsoplysninger*.parquet",
                f"{fertiliser_dir}GKEA*_Markplan*.parquet",
            ]

            # If target_year is specified, prefer files from that year
            if target_year:
                priority_patterns = [
                    f"{fertiliser_dir}GKEA{target_year}_Markplan_med_Gødningsoplysninger*.parquet",
                    f"{fertiliser_dir}GKEA{target_year}_Markplan*.parquet",
                ]
                patterns = priority_patterns + patterns  # Try target year first, then any year

            for pattern in patterns:
                try:
                    self.log.debug(f"Trying pattern: {pattern}")
                    files = self.gcs_access.list_files(pattern)
                    if files:
                        # Get the most recent file (by name)
                        latest_file = sorted(files, reverse=True)[0]
                        self.log.info(f"✅ Found GKEA field plan data: {latest_file}")
                        return latest_file
                except Exception as e:
                    self.log.debug(f"Pattern {pattern} failed: {e}")
                    continue

            # If specific year not found, try to find any GKEA file
            try:
                all_gkea_files = self.gcs_access.list_files(
                    f"{fertiliser_dir}GKEA*_Markplan*.parquet"
                )
                if all_gkea_files:
                    # Get the most recent GKEA file
                    latest_file = sorted(all_gkea_files, reverse=True)[0]
                    self.log.info(f"✅ Found alternative GKEA field plan data: {latest_file}")
                    return latest_file
            except Exception as e:
                self.log.debug(f"Alternative GKEA search failed: {e}")

            # List all files in the directory to help debug
            try:
                all_files = self.gcs_access.list_files(f"{fertiliser_dir}*.parquet")
                self.log.warning(f"No GKEA files found. Available files in {fertiliser_dir}:")
                for f in all_files[:10]:  # Show first 10 files
                    self.log.warning(f"  - {f.split('/')[-1]}")
                if len(all_files) > 10:
                    self.log.warning(f"  ... and {len(all_files) - 10} more files")
            except Exception as e:
                self.log.error(f"Could not list files in directory: {e}")

            raise FileNotFoundError(
                f"No GKEA field plan data found in {fertiliser_dir}. "
                f"Expected files matching pattern GKEA*_Markplan*.parquet"
            )

        except FileNotFoundError:
            # Re-raise FileNotFoundError as-is
            raise
        except Exception as e:
            self.log.error(f"Failed to get field plan data path: {e}")
            raise

    def _get_catch_crops_data_path(self, target_year: int = None) -> str:
        """
        Get the path to catch crops (Efterafgrøder) data from the fertiliser directory.

        Args:
            target_year: Optional target year to match

        Returns:
            GCS path to catch crops data
        """
        try:
            # Get the fertiliser base directory
            fertiliser_dir = self._get_fertilizer_data_path(target_year)

            # Look for Efterafgrøder (catch crops) files
            if target_year:
                patterns = [
                    f"{fertiliser_dir}Efterafgrøder {target_year}.parquet",
                    f"{fertiliser_dir}Efterafgrøder_{target_year}.parquet",
                ]
            else:
                patterns = [
                    f"{fertiliser_dir}Efterafgrøder *.parquet",
                    f"{fertiliser_dir}Efterafgrøder_*.parquet",
                ]

            for pattern in patterns:
                try:
                    files = self.gcs_access.list_files(pattern)
                    if files:
                        # Get the most recent file (by name)
                        latest_file = sorted(files, reverse=True)[0]
                        self.log.info(f"Found catch crops (Efterafgrøder) data: {latest_file}")
                        return latest_file
                except Exception as e:
                    self.log.debug(f"Pattern {pattern} failed: {e}")
                    continue

            # If specific year not found, try to find any Efterafgrøder file
            try:
                all_catch_files = self.gcs_access.list_files(
                    f"{fertiliser_dir}Efterafgrøder *.parquet"
                )
                if all_catch_files:
                    # Get the most recent catch crops file
                    latest_file = sorted(all_catch_files, reverse=True)[0]
                    self.log.info(f"Found alternative catch crops data: {latest_file}")
                    return latest_file
            except Exception as e:
                self.log.debug(f"Alternative catch crops search failed: {e}")

            # If nothing found, return a fallback path
            fallback_path = f"{fertiliser_dir}Efterafgrøder 2023.parquet"
            self.log.warning(f"No catch crops data found, using fallback: {fallback_path}")
            return fallback_path

        except Exception as e:
            self.log.error(f"Failed to get catch crops data path: {e}")
            return f"gs://{self.config.bucket}/silver/fertiliser/Efterafgrøder.parquet"

    def _read_silver_data_from_path(
        self, dataset_name: str, file_path: str, target_table: str
    ) -> bool:
        """
        Read silver data from a specific GCS path into a DuckDB table.

        Args:
            dataset_name: Name of the dataset for logging
            file_path: GCS path to the data file
            target_table: Name of the target DuckDB table

        Returns:
            True if successful, False otherwise
        """
        try:
            self.log.info(f"📥 Loading {dataset_name} from: {file_path}")

            # Check if file exists
            if not self.gcs_access.file_exists(file_path):
                self.log.error(f"File not found: {file_path}")
                return False

            # Special handling for GKEA field plan data
            if dataset_name == self.config.field_plan_dataset and "GKEA" in file_path:
                return self._process_gkea_field_plan_data(file_path, target_table)

            # Use the standard GCSDataAccess method to create table from GCS
            self.gcs_access.create_table_from_gcs(target_table, file_path)

            # Verify the table was created and has data
            row_count = self.db.execute(f"SELECT COUNT(*) FROM {target_table}").fetchone()[0]
            self.log.info(f"✅ Successfully loaded {dataset_name}: {row_count:,} rows")

            return True

        except Exception as e:
            self.log.error(f"❌ Failed to read {dataset_name} from {file_path}: {e}")
            return False

    def _process_gkea_field_plan_data(self, file_path: str, target_table: str) -> bool:
        """
        Process GKEA field plan data with proper column mapping and header handling.

        GKEA files can have two formats:
        1. Parquet with proper column names (modern format)
        2. CSV-like with headers in row 2 (legacy format)

        Args:
            file_path: GCS path to the GKEA field plan file
            target_table: Target table name (should be 'field_plan')

        Returns:
            True if successful, False otherwise
        """
        try:
            # Extract year from the filename (e.g., GKEA2021_Markplan_... -> 2021)
            import re

            year_match = re.search(r"GKEA(\d{4})_", file_path)
            gkea_year = int(year_match.group(1)) if year_match else 2024

            # First, load the data to check its structure
            self.db.execute(f"""
                CREATE OR REPLACE TABLE field_plan_temp AS
                SELECT * FROM '{file_path}'
            """)

            # Check the actual column names in the parquet file
            columns_info = self.db.execute("PRAGMA table_info(field_plan_temp)").fetchall()
            column_names = [col[1] for col in columns_info]  # col[1] is the column name

            self.log.info(f"🔧 Processing GKEA field plan for year {gkea_year}")
            self.log.info(f"📋 Parquet columns: {column_names[:10]}")

            # Check if the parquet already has proper column names (case-insensitive)
            # Look for common GKEA column patterns
            column_names_lower = [c.lower() for c in column_names]
            has_proper_columns = (
                any("cvr" in c for c in column_names_lower)
                and any("marknummer" in c for c in column_names_lower)
                and any("areal" in c for c in column_names_lower)
            )

            if has_proper_columns:
                # Modern format: parquet already has proper column names
                self.log.info("✅ Parquet file has proper column names (modern format)")
                return self._process_gkea_with_column_names(
                    file_path, target_table, gkea_year, column_names
                )
            else:
                # Legacy format: need to read headers from row 2
                self.log.info("🔄 Parquet file uses legacy format (headers in row 2)")
                return self._process_gkea_legacy_format(
                    file_path, target_table, gkea_year, column_names
                )

        except Exception as e:
            self.log.error(f"❌ Failed to process GKEA field plan data: {e}")
            return False

    def _process_gkea_with_column_names(
        self, file_path: str, target_table: str, gkea_year: int, column_names: List[str]
    ) -> bool:
        """
        Process GKEA data that already has proper Danish column names.

        Args:
            file_path: GCS path to the GKEA field plan file
            target_table: Target table name
            gkea_year: Year extracted from filename
            column_names: List of column names from the parquet file

        Returns:
            True if successful, False otherwise
        """
        try:
            # Map Danish column names to the columns we need (case-insensitive search)
            column_names_lower = {c.lower(): c for c in column_names}

            # Find CVR column (cvr_number, CVR, cvr, etc.)
            cvr_col = None
            for pattern in ["cvr_number", "cvr"]:
                if pattern in column_names_lower:
                    cvr_col = column_names_lower[pattern]
                    break

            # Find Marknummer column (marknummer, Marknummer, etc.)
            marknummer_col = next(
                (v for k, v in column_names_lower.items() if "marknummer" in k), None
            )

            # Find Areal column (faktisk_areal_ha, omregnet_areal_ha, Areal, areal, etc.)
            # Prefer faktisk_areal_ha over omregnet_areal_ha
            areal_col = None
            for pattern in ["faktisk_areal_ha", "omregnet_areal_ha", "areal"]:
                if pattern in column_names_lower:
                    areal_col = column_names_lower[pattern]
                    break

            # Find Journal nummer column (journal_nummer, etc.)
            journal_col = next((v for k, v in column_names_lower.items() if "journal" in k), None)

            if not all([cvr_col, marknummer_col, areal_col]):
                missing = []
                if not cvr_col:
                    missing.append("cvr (tried: cvr_number, cvr)")
                if not marknummer_col:
                    missing.append("marknummer")
                if not areal_col:
                    missing.append("areal (tried: faktisk_areal_ha, omregnet_areal_ha, areal)")
                self.log.error(f"Available columns: {column_names}")
                raise ValueError(f"Missing essential columns: {missing}")

            self.log.info(
                f"🗂️ Column mapping - CVR: {cvr_col}, Marknummer: {marknummer_col}, "
                f"Areal: {areal_col}, Journal: {journal_col}"
            )

            # Create the target table with standardized schema
            self.db.execute(f"""
                CREATE OR REPLACE TABLE {target_table} AS
                SELECT
                    CONCAT("{cvr_col}", '_', "{marknummer_col}") as field_id,
                    {gkea_year} as year,
                    {f'"{journal_col}"' if journal_col else "NULL"} as journal_nummer,
                    "{cvr_col}" as cvr_number,
                    "{marknummer_col}" as marknummer,
                    NULL as modtaget_dato,
                    TRY_CAST("{areal_col}" AS DOUBLE) as areal,
                    NULL as harmoni_areal_indikator,
                    NULL as harmoni_areal,
                    NULL as jordbundstype,
                    NULL as crop_code,
                    NULL as total_n_kg_ha,
                    NULL as total_n_kg_mark,
                    NULL as mineral_n_spring,
                    NULL as organic_n_total
                FROM field_plan_temp
                WHERE "{marknummer_col}" IS NOT NULL 
                  AND "{marknummer_col}" != ''
                  AND TRY_CAST("{areal_col}" AS DOUBLE) > 0
            """)

            # Clean up
            self.db.execute("DROP TABLE IF EXISTS field_plan_temp")

            count = self.db.execute(f"SELECT COUNT(*) FROM {target_table}").fetchone()[0]
            self.log.info(f"✅ Successfully processed GKEA field plan data: {count:,} records")
            return True

        except Exception as e:
            self.log.error(f"❌ Failed to process GKEA with column names: {e}")
            return False

    def _process_gkea_legacy_format(
        self, file_path: str, target_table: str, gkea_year: int, column_names: List[str]
    ) -> bool:
        """
        Process GKEA data with legacy format (headers in row 2).

        Args:
            file_path: GCS path to the GKEA field plan file
            target_table: Target table name
            gkea_year: Year extracted from filename
            column_names: List of column names from the parquet file

        Returns:
            True if successful, False otherwise
        """
        try:
            # Add row numbers to find header row
            self.db.execute("""
                CREATE OR REPLACE TABLE field_plan_all AS
                SELECT 
                    ROW_NUMBER() OVER () as row_num,
                    *
                FROM field_plan_temp
            """)

            # Get the header row (row 2) to map column names
            headers = self.db.execute("""
                SELECT * FROM field_plan_all 
                WHERE row_num = 2
                LIMIT 1
            """).fetchone()

            if not headers:
                raise ValueError("Could not find header row in field plan data")

            # Create raw data table (skip first 2 rows)
            self.db.execute("""
                CREATE OR REPLACE TABLE field_plan_raw AS
                SELECT * FROM field_plan_all 
                WHERE row_num >= 3
            """)

            self.log.info(f"🗺️ Mapping field plan columns from Danish headers: {headers[:8]}...")

            # Get column info for mapping
            columns_info = self.db.execute("PRAGMA table_info(field_plan_raw)").fetchall()
            column_names = [col[1] for col in columns_info]

            # Find the journal nummer column
            journal_column = next(
                (c for c in column_names if "markplan" in c.lower() and "goedning" in c.lower()),
                column_names[0] if column_names else "column_0",
            )

            # Create header-to-column mapping
            header_to_column = {}
            for i, header_value in enumerate(headers[1:], 1):  # Skip row_num column
                if header_value and str(header_value).strip():
                    clean_header = str(header_value).strip().replace("\n", " ")
                    if i == 1:
                        header_to_column[clean_header] = journal_column
                    else:
                        header_to_column[clean_header] = f"column_{i - 1}"

            self.log.info(f"🗺️ Header mapping: {list(header_to_column.keys())[:6]}")

            # Dynamically find key columns based on headers
            cvr_column = None
            marknummer_column = None
            areal_column = None
            modtaget_dato_column = None

            for header, col in header_to_column.items():
                if header == "CVR":  # Exact match to avoid matching "Kundetype (CVR/CPR)"
                    cvr_column = col
                elif "Marknummer" in header:
                    marknummer_column = col
                elif header == "Areal":  # Exact match to avoid matching "Fradrags Arealer" etc
                    areal_column = col
                elif "Modtaget Dato" in header:
                    modtaget_dato_column = col

            self.log.info(
                f"🗂️ Column mapping - Journal: {journal_column}, CVR: {cvr_column}, "
                f"Marknummer: {marknummer_column}, Areal: {areal_column}"
            )

            # Ensure we have the essential columns
            if not all([cvr_column, marknummer_column, areal_column]):
                missing = []
                if not cvr_column:
                    missing.append("CVR")
                if not marknummer_column:
                    missing.append("Marknummer")
                if not areal_column:
                    missing.append("Areal")
                raise ValueError(f"Missing essential columns in GKEA data: {missing}")

            self.db.execute(f"""
                CREATE OR REPLACE TABLE {target_table} AS
                SELECT
                    -- COMPOSITE KEY: Create field_id from CVR + marknummer for FVM matching
                    -- Dynamic CVR_Marknummer composite key
                    CONCAT({cvr_column}, '_', {marknummer_column}) as field_id,
                    -- Year from filename (e.g., 2021 from GKEA2021_...)
                    {gkea_year} as year,
                    -- 'Journal Nummer' (dynamically determined)
                    {journal_column} as journal_nummer,
                    {cvr_column} as cvr_number,      -- 'CVR' (dynamic)
                    {marknummer_column} as marknummer,      -- 'Marknummer' (dynamic)
                    -- 'Modtaget Dato' (dynamic, may be NULL)
                    {modtaget_dato_column if modtaget_dato_column else "NULL"} as modtaget_dato,
                    -- Handle mixed data types in area column - try to cast, use NULL if it fails
                    TRY_CAST({areal_column} as DOUBLE) as areal,  -- 'Areal' (dynamic)
                    -- Additional columns with fallbacks for different year structures
                    -- Only in 2021
                    CASE WHEN '{gkea_year}' = '2021' THEN column_7 
                        ELSE NULL END as harmoni_areal_indikator,
                    -- Only in 2021
                    CASE WHEN '{gkea_year}' = '2021' THEN TRY_CAST(column_8 as DOUBLE) 
                        ELSE NULL END as harmoni_areal,
                    -- Only in 2021
                    CASE WHEN '{gkea_year}' = '2021' THEN column_9 ELSE NULL END as jordbundstype,
                    -- Crop codes and nitrogen data - structure varies significantly by year
                    -- use NULL for now
                    NULL as crop_code,      -- Structure varies too much between years
                    NULL as total_n_kg_ha,  -- Structure varies too much between years
                    NULL as total_n_kg_mark,    -- Structure varies too much between years
                    NULL as mineral_n_spring,   -- Structure varies too much between years
                    NULL as organic_n_total     -- Structure varies too much between years
                FROM field_plan_raw
                WHERE {marknummer_column} IS NOT NULL 
                  AND {marknummer_column} != ''
                  AND {marknummer_column} != 'Marknummer'  -- Skip any remaining header rows
                  AND {journal_column} IS NOT NULL     -- Must have journal number for composite key
                  -- Must have positive area (dynamic column!)
                  AND TRY_CAST({areal_column} as DOUBLE) > 0
            """)

            # Clean up temporary tables
            self.db.execute("DROP TABLE IF EXISTS field_plan_all")
            self.db.execute("DROP TABLE IF EXISTS field_plan_raw")
            self.db.execute("DROP TABLE IF EXISTS field_plan_temp")

            # Validate the processed data
            count = self.db.execute(f"SELECT COUNT(*) FROM {target_table}").fetchone()[0]
            if count == 0:
                raise ValueError("No valid field plan records found after processing")

            self.log.info(
                f"✅ Successfully processed GKEA field plan data (legacy format): {count:,} records"
            )

            # Log sample data for verification
            sample = self.db.execute(f"""
                SELECT field_id, journal_nummer, cvr_number, areal, crop_code
                FROM {target_table} 
                LIMIT 5
            """).fetchall()

            self.log.info("📋 Sample processed GKEA data:")
            for i, row in enumerate(sample, 1):
                self.log.info(
                    f"   {i}. field_id={row[0]}, journal={row[1]}, cvr={row[2]}, "
                    f"area={row[3]}, crop={row[4]}"
                )

            return True

        except Exception as e:
            self.log.error(f"❌ Failed to process GKEA legacy format: {e}")
            return False

    async def _apply_agricultural_pattern_matching(self, gkea_table: str) -> None:
        """Apply agricultural pattern matching to enhance GKEA-FVM field matching."""
        try:
            self.log.info("🌾 Applying agricultural pattern matching enhancement...")

            # Check if FVM marker data exists
            marker_exists = False
            try:
                marker_count = self.db.execute("SELECT COUNT(*) FROM marker").fetchone()[0]
                marker_exists = marker_count > 0
            except Exception:
                marker_exists = False

            if not marker_exists:
                self.log.warning(
                    "   FVM marker data not available - skipping agricultural pattern matching"
                )
                return

            # Import and run agricultural pattern matcher
            from unified_pipeline.gold.agricultural_pattern_matcher import (
                AgriculturalPatternMatcherConfig,
                run_agricultural_pattern_matching,
            )

            # Configure for high-quality matches
            config = AgriculturalPatternMatcherConfig(
                min_pattern_score=0.8, min_field_score=0.7, max_operations_to_process=2000
            )

            # Run the pattern matching with our database connection
            results = await run_agricultural_pattern_matching(config, self.db)

            # Log results
            if results.get("matches_found", 0) > 0:
                matches_found = results["matches_found"]
                self.log.info(
                    f"✅ Agricultural pattern matching found {matches_found:,} "
                    f"additional field matches"
                )

                # Create enhanced field mappings table for NLES5 use
                try:
                    self.db.execute(f"""
                        CREATE OR REPLACE TABLE gkea_fvm_enhanced_mappings AS
                        SELECT 
                            g.field_id as gkea_field_id,
                            f.field_id as fvm_field_id,
                            'direct_composite_key' as match_method,
                            1.0 as confidence_score
                        FROM {gkea_table} g
                        JOIN marker f ON g.field_id = f.field_id
                        
                        UNION ALL
                        
                        SELECT 
                            gkea_field_id,
                            fvm_field_id,
                            'agricultural_pattern' as match_method,
                            field_similarity_score as confidence_score
                        FROM enhanced_gkea_fvm_matches
                        WHERE field_similarity_score >= 0.7
                    """)

                    total_matches = self.db.execute(
                        "SELECT COUNT(*) FROM gkea_fvm_enhanced_mappings"
                    ).fetchone()[0]
                    original_matches = self.db.execute("""
                        SELECT COUNT(*) FROM gkea_fvm_enhanced_mappings 
                        WHERE match_method = 'direct_composite_key'
                    """).fetchone()[0]

                    improvement = total_matches - original_matches
                    self.log.info(
                        f"   Total GKEA-FVM matches: {total_matches:,} "
                        f"(original: {original_matches:,}, +{improvement:,} from patterns)"
                    )

                except Exception as mapping_error:
                    self.log.warning(
                        f"   Could not create enhanced mappings table: {mapping_error}"
                    )

            else:
                self.log.info("   No additional matches found via agricultural pattern matching")

        except Exception as e:
            self.log.warning(f"⚠️ Agricultural pattern matching failed: {str(e)}")
            self.log.warning("   Continuing with standard GKEA field processing...")

    def _apply_agricultural_pattern_matching_sync(self, gkea_table: str) -> None:
        """Synchronous wrapper for agricultural pattern matching."""
        import asyncio

        try:
            # Run the async method in a new event loop
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(self._apply_agricultural_pattern_matching(gkea_table))
            finally:
                loop.close()
        except Exception as e:
            self.log.warning(f"⚠️ Agricultural pattern matching sync wrapper failed: {str(e)}")
            self.log.warning("   Continuing with standard GKEA field processing...")

    @timed(name="Loading required silver datasets")
    def _load_required_silver_datasets(
        self, silver_data: Optional[Dict[str, Any]]
    ) -> Dict[str, str]:
        """
        Load all required silver datasets for NLES5 processing.

        Args:
            silver_data: Optional in-memory silver data

        Returns:
            Dictionary mapping dataset names to table names
        """
        loaded_tables: Dict[str, str] = {}
        required_datasets = [
            (self.config.soil_types_dataset, "soil_types"),
            ("dmi_climate", "dmi_data"),  # Special handling for DMI data
            (self.config.fertilizer_dataset, "fertilizer_accounts"),
            (self.config.field_plan_dataset, "field_plan_data"),  # Match backup naming
            (self.config.catch_crops_dataset, "catch_crops_data"),  # Match backup naming
        ]

        self.log.info("📂 Loading required silver datasets for NLES5...")

        for dataset_name, table_name in required_datasets:
            self.log.info(f"🔍 Processing dataset: {dataset_name} -> {table_name}")
            try:
                if silver_data and dataset_name in silver_data:
                    # Use data passed from a previous pipeline if available
                    self.log.info(f"Using in-memory data for {dataset_name}")
                    loaded_tables[dataset_name] = silver_data[dataset_name]
                else:
                    # Special handling for DMI climate data
                    if dataset_name == "dmi_climate":
                        try:
                            success = self._load_and_combine_dmi_data()
                            if success:
                                loaded_tables[dataset_name] = table_name
                                self.log.info("✅ Successfully loaded DMI climate data")
                                continue
                            else:
                                self.log.error("❌ Failed to load DMI climate data")
                                continue
                        except Exception as e:
                            self.log.error(
                                f"❌ CRITICAL: Failed to load required DMI climate data: {e}"
                            )
                            continue

                    # Special handling for field plan data
                    # Always try to load from fertiliser directory
                    elif dataset_name == self.config.field_plan_dataset:
                        try:
                            # Use the first target year as the reference for field plan data
                            target_year = (
                                self.config.target_years[0]
                                if getattr(self.config, "target_years", None)
                                and len(self.config.target_years) > 0
                                else None
                            )
                            field_plan_path = self._get_field_plan_data_path(target_year)
                            self.log.info(
                                f"Using field plan file from fertiliser directory for year "
                                f"{target_year}: {field_plan_path}"
                            )
                            success = self._read_silver_data_from_path(
                                dataset_name, field_plan_path, table_name
                            )
                            if success:
                                loaded_tables[dataset_name] = table_name
                                self.log.info(
                                    f"✅ Successfully loaded field plan data: {dataset_name}"
                                )
                                continue
                            else:
                                self.log.error(f"❌ Failed to load field plan data {dataset_name}")
                                continue
                        except Exception as e:
                            self.log.error(
                                f"❌ CRITICAL: Failed to load required field plan data: {e}"
                            )
                            continue

                    # Load from GCS using modern pattern
                    else:
                        # Special handling for fertilizer and catch crops datasets
                        # (they don't follow standard patterns)
                        if dataset_name in [
                            self.config.fertilizer_dataset,
                            self.config.catch_crops_dataset,
                        ]:
                            # For fertilizer/catch crops, check if we can find
                            # the directory structure
                            try:
                                if dataset_name == self.config.fertilizer_dataset:
                                    test_path = self._get_fertilizer_accounts_file_path()
                                    files = [test_path] if test_path else []
                                elif dataset_name == self.config.catch_crops_dataset:
                                    test_path = self._get_catch_crops_data_path()
                                    files = [test_path] if test_path else []
                                else:
                                    files = []
                            except Exception as e:
                                self.log.debug(f"Special handling failed for {dataset_name}: {e}")
                                files = []
                        else:
                            # Use dynamic discovery to find the latest timestamped directory
                            base_path = f"gs://{self.config.bucket}/silver/{dataset_name}/"
                            latest_dir = self._get_latest_timestamped_directory(
                                base_path, dataset_name
                            )

                            if not latest_dir:
                                self.log.error(
                                    f"❌ Dataset not found in silver layer: {dataset_name}"
                                )
                                continue

                            # Look for data.parquet in the latest directory
                            data_file = f"{latest_dir}data.parquet"
                            files = [data_file] if self.gcs_access.file_exists(data_file) else []

                        if files:
                            self.log.info(f"Found {dataset_name} in silver layer.")

                            # Special handling for fertilizer data to get the latest 2024 data
                            if dataset_name == self.config.fertilizer_dataset:
                                try:
                                    # Use the first target year as the reference for fertilizer data
                                    target_year = (
                                        self.config.target_years[0]
                                        if getattr(self.config, "target_years", None)
                                        and len(self.config.target_years) > 0
                                        else None
                                    )
                                    fertilizer_file_path = self._get_fertilizer_accounts_file_path(
                                        target_year
                                    )
                                    if fertilizer_file_path:
                                        self.log.info(
                                            f"Using fertilizer accounts file for year "
                                            f"{target_year}: {fertilizer_file_path}"
                                        )
                                        success = self._read_silver_data_from_path(
                                            dataset_name, fertilizer_file_path, table_name
                                        )
                                        if success:
                                            # Add year column to fertilizer data
                                            self._add_year_to_fertilizer_data(
                                                fertilizer_file_path, table_name
                                            )
                                            # Transform raw fertilizer data to expected schema
                                            self._transform_raw_fertilizer_data(table_name)
                                            loaded_tables[dataset_name] = table_name
                                            self.log.info(
                                                f"✅ Successfully loaded fertilizer data: "
                                                f"{dataset_name}"
                                            )
                                            continue
                                        else:
                                            self.log.error(
                                                f"❌ Failed to load fertilizer data {dataset_name}"
                                            )
                                            continue
                                    else:
                                        self.log.error(
                                            f"❌ No fertilizer accounts file found for year "
                                            f"{target_year}"
                                        )
                                        continue
                                except Exception as e:
                                    self.log.error(
                                        f"❌ CRITICAL: Failed to load required fertilizer data: {e}"
                                    )
                                    continue

                            # Special handling for catch crops data (optional)
                            elif dataset_name == self.config.catch_crops_dataset:
                                try:
                                    target_year = (
                                        self.config.target_years[0]
                                        if getattr(self.config, "target_years", None)
                                        and len(self.config.target_years) > 0
                                        else None
                                    )
                                    catch_crops_path = self._get_catch_crops_data_path(target_year)
                                    self.log.info(
                                        f"Using catch crops file for year {target_year}: "
                                        f"{catch_crops_path}"
                                    )
                                    success = self._read_silver_data_from_path(
                                        dataset_name, catch_crops_path, table_name
                                    )
                                    if success:
                                        loaded_tables[dataset_name] = table_name
                                        self.log.info(
                                            f"✅ Successfully loaded catch crops data: "
                                            f"{dataset_name}"
                                        )
                                        continue
                                    else:
                                        self.log.warning(
                                            f"⚠️ Catch crops data not available (optional): "
                                            f"{dataset_name}"
                                        )
                                        continue
                                except Exception as e:
                                    self.log.warning(
                                        f"⚠️ Catch crops data not available (optional): {e}"
                                    )
                                    continue

                            # Standard loading for other datasets
                            else:
                                try:
                                    # Get the most recent file
                                    latest_file = sorted(files, reverse=True)[0]
                                    self.log.info(f"📥 Loading {dataset_name} from: {latest_file}")

                                    # Use the standard GCSDataAccess method
                                    self.gcs_access.create_table_from_gcs(table_name, latest_file)

                                    # Verify and log
                                    row_count = self.db.execute(
                                        f"SELECT COUNT(*) FROM {table_name}"
                                    ).fetchone()[0]
                                    loaded_tables[dataset_name] = table_name
                                    self.log.info(
                                        f"✅ Successfully loaded {dataset_name}: {row_count:,} rows"
                                    )
                                except Exception as e:
                                    self.log.error(f"❌ Failed to load {dataset_name}: {e}")
                                    continue
                        else:
                            self.log.error(f"❌ Dataset not found in silver layer: {dataset_name}")
                            continue

            except Exception as e:
                self.log.error(f"❌ Error processing dataset {dataset_name}: {e}")
                continue

        self.log.info(
            f"📊 Successfully loaded {len(loaded_tables)}/{len(required_datasets)} "
            f"required datasets"
        )

        # Log which datasets failed to load
        failed_datasets = [name for name, _ in required_datasets if name not in loaded_tables]
        if failed_datasets:
            self.log.warning(f"⚠️ Failed to load datasets: {failed_datasets}")

        return loaded_tables

    def _load_and_combine_dmi_data(self) -> bool:
        """
        Load and combine DMI climate data from precipitation and evaporation datasets.

        Returns:
            True if successful, False otherwise
        """
        try:
            self.log.info("🌤️ Loading DMI climate data from multiple sources...")

            # Load precipitation data
            precip_success = self._load_dmi_precipitation_data()

            # Load evaporation data
            evap_success = self._load_dmi_evaporation_data()

            if precip_success and evap_success:
                # Combine the two datasets
                self._combine_dmi_datasets()
                return True
            elif precip_success:
                self.log.warning("⚠️ Only precipitation data available, using that")
                self.db.execute(
                    "CREATE OR REPLACE TABLE dmi_data AS SELECT * FROM dmi_precipitation"
                )
                return True
            elif evap_success:
                self.log.warning("⚠️ Only evaporation data available, using that")
                self.db.execute("CREATE OR REPLACE TABLE dmi_data AS SELECT * FROM dmi_evaporation")
                return True
            else:
                self.log.error("❌ No DMI climate data could be loaded")
                return False

        except Exception as e:
            self.log.error(f"❌ Failed to load DMI data: {e}")
            return False

    def _load_dmi_precipitation_data(self) -> bool:
        """Load DMI precipitation data from the latest timestamped directory."""
        try:
            base_path = f"gs://{self.config.bucket}/silver/{self.config.dmi_precipitation_dataset}/"
            latest_dir = self._get_latest_timestamped_directory(base_path, "DMI precipitation")

            if not latest_dir:
                self.log.warning("⚠️ DMI precipitation data not found - no timestamped directories")
                return False

            # Look for data.parquet in the latest directory
            data_file = f"{latest_dir}data.parquet"

            if not self.gcs_access.file_exists(data_file):
                self.log.warning(f"⚠️ data.parquet not found in {latest_dir}")
                return False

            self.log.info(f"📥 Loading DMI precipitation data from: {data_file}")
            # Drop existing table if it exists to avoid collision
            self.db.execute("DROP TABLE IF EXISTS dmi_precipitation")
            self.gcs_access.create_table_from_gcs("dmi_precipitation", data_file)

            row_count = self.db.execute("SELECT COUNT(*) FROM dmi_precipitation").fetchone()[0]
            self.log.info(f"✅ DMI precipitation data loaded: {row_count:,} rows")
            return True

        except Exception as e:
            self.log.error(f"❌ Failed to load DMI precipitation data: {e}")
            return False

    def _load_dmi_evaporation_data(self) -> bool:
        """Load DMI evaporation data from the latest timestamped directory."""
        try:
            base_path = f"gs://{self.config.bucket}/silver/{self.config.dmi_evaporation_dataset}/"
            latest_dir = self._get_latest_timestamped_directory(base_path, "DMI evaporation")

            if not latest_dir:
                self.log.warning("⚠️ DMI evaporation data not found - no timestamped directories")
                return False

            # Look for data.parquet in the latest directory
            data_file = f"{latest_dir}data.parquet"

            if not self.gcs_access.file_exists(data_file):
                self.log.warning(f"⚠️ data.parquet not found in {latest_dir}")
                return False

            self.log.info(f"📥 Loading DMI evaporation data from: {data_file}")
            # Drop existing table if it exists to avoid collision
            self.db.execute("DROP TABLE IF EXISTS dmi_evaporation")
            self.gcs_access.create_table_from_gcs("dmi_evaporation", data_file)

            row_count = self.db.execute("SELECT COUNT(*) FROM dmi_evaporation").fetchone()[0]
            self.log.info(f"✅ DMI evaporation data loaded: {row_count:,} rows")
            return True

        except Exception as e:
            self.log.error(f"❌ Failed to load DMI evaporation data: {e}")
            return False

    def _combine_dmi_datasets(self):
        """Combine precipitation and evaporation datasets into a single DMI table."""
        try:
            self.log.info("🔗 Combining DMI precipitation and evaporation data...")

            # Check what columns are available in each dataset
            precip_columns = self.db.execute("DESCRIBE dmi_precipitation").fetchall()
            evap_columns = self.db.execute("DESCRIBE dmi_evaporation").fetchall()

            self.log.info(f"Precipitation columns: {[col[0] for col in precip_columns[:5]]}")
            self.log.info(f"Evaporation columns: {[col[0] for col in evap_columns[:5]]}")

            # DIAGNOSTIC: Check variation in raw silver data BEFORE combining
            precip_variation = self.db.execute("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT avg_value) as unique_values,
                    MIN(avg_value) as min_value,
                    MAX(avg_value) as max_value,
                    AVG(avg_value) as avg_value
                FROM dmi_precipitation 
                WHERE avg_value IS NOT NULL
            """).fetchone()

            self.log.info("🔍 RAW SILVER PRECIPITATION DATA VARIATION:")
            self.log.info(
                f"   Records: {precip_variation[0]:,}, Unique values: {precip_variation[1]:,}"
            )
            self.log.info(
                f"   Range: {precip_variation[2]:.3f} to {precip_variation[3]:.3f}, "
                f"Avg: {precip_variation[4]:.3f}"
            )

            if precip_variation[1] <= 1:
                self.log.error(
                    f"🚨 SILVER LAYER PROBLEM: Only {precip_variation[1]} unique "
                    f"precipitation value(s) in silver data!"
                )
                self.log.error("   This explains uniform percolation values in gold layer")
                self.log.error(
                    "   🔍 INVESTIGATE: Check DMI silver layer pipeline processing "
                    "and bronze layer source data"
                )

            # Also check evaporation data variation
            try:
                evap_variation = self.db.execute("""
                    SELECT 
                        COUNT(*) as total_records,
                        COUNT(DISTINCT avg_value) as unique_values,
                        MIN(avg_value) as min_value,
                        MAX(avg_value) as max_value,
                        AVG(avg_value) as avg_value
                    FROM dmi_evaporation 
                    WHERE avg_value IS NOT NULL
                """).fetchone()

                self.log.info("🔍 RAW SILVER EVAPORATION DATA VARIATION:")
                self.log.info(
                    f"   Records: {evap_variation[0]:,}, Unique values: {evap_variation[1]:,}"
                )
                self.log.info(
                    f"   Range: {evap_variation[2]:.3f} to {evap_variation[3]:.3f}, "
                    f"Avg: {evap_variation[4]:.3f}"
                )

                if evap_variation[1] <= 1:
                    self.log.error(
                        f"🚨 EVAPORATION DATA ALSO UNIFORM: Only {evap_variation[1]} "
                        f"unique value(s)!"
                    )

            except Exception as e:
                self.log.warning(f"Could not check evaporation variation: {e}")

            # Create combined table with both datasets
            # Since both tables have the same structure, just use precipitation data for now
            # TODO: Implement proper spatial joining once we understand the spatial column structure
            self.db.execute("""
                CREATE OR REPLACE TABLE dmi_data AS
                SELECT 
                    *,
                    'precipitation' as data_type
                FROM dmi_precipitation
            """)

            combined_count = self.db.execute("SELECT COUNT(*) FROM dmi_data").fetchone()[0]
            self.log.info(f"✅ Combined DMI data: {combined_count:,} rows")

        except Exception as e:
            self.log.warning(f"⚠️ Failed to combine DMI datasets, using precipitation only: {e}")
            self.db.execute("CREATE OR REPLACE TABLE dmi_data AS SELECT * FROM dmi_precipitation")

    def _load_climate_data_for_years(self, years: List[int]) -> str:
        """
        Load climate data for specific years.

        Args:
            years: List of years to load climate data for

        Returns:
            Name of the table containing the loaded climate data
        """
        try:
            self.log.info(f"🌤️ Loading climate data for years: {years}")

            # Create a combined climate table
            combined_table = "climate_data_years"
            self.db.execute(f"DROP TABLE IF EXISTS {combined_table}")

            # Load DMI data if not already loaded
            if not self._load_and_combine_dmi_data():
                raise ValueError("Failed to load DMI climate data")

            # Filter for the specified years
            year_filter = ",".join(map(str, years))
            # Use the already-processed climate_percolation table instead of raw DMI data
            filter_sql = f"""
            CREATE TABLE {combined_table} AS
            SELECT *
            FROM climate_percolation
            WHERE year IN ({year_filter})
            """

            self.db.execute(filter_sql)

            # Verify the filtered data
            row_count = self.db.execute(f"SELECT COUNT(*) FROM {combined_table}").fetchone()[0]
            self.log.info(f"✅ Climate data loaded for {len(years)} years: {row_count:,} rows")

            return combined_table

        except Exception as e:
            self.log.error(f"❌ Failed to load climate data for years {years}: {e}")
            raise

    @timed(name="Loading farm data")
    def _load_farm_data(self, years: List[int]) -> Optional[str]:
        """
        Load farm-level gødningsregnskab data for enhanced NLES5 calculations.

        Loads animal production data (C_2016, C_2006) and fertilizer application data
        (F_901, F_902, F_512, F_703_1, F_706_1, F_308_1) to replace estimated values
        with actual farm-specific data.

        Args:
            years: List of years to load farm data for

        Returns:
            Table name containing farm data or None if disabled/unavailable
        """
        if not self.config.enable_farm_data_integration:
            self.log.info("Farm data integration disabled in configuration")
            return None

        if not self.config.farm_data_years:
            self.log.info("No farm data years configured")
            return None

        try:
            # Filter to available years
            available_years = [y for y in years if y in self.config.farm_data_years]
            if not available_years:
                self.log.warning(f"No farm data available for requested years {years}")
                return None

            self.log.info(f"🚜 Loading farm data for years: {available_years}")

            # Create final table to hold all farm data
            table_name = self.config.farm_data_cache_table
            self.db.execute(f"DROP TABLE IF EXISTS {table_name}")

            year_tables = []
            for year in available_years:
                year_table = self._load_farm_data_for_year(year)
                if year_table is not None:
                    year_tables.append((year, year_table))

            if not year_tables:
                self.log.warning("No farm data loaded for any year")
                return None

            # Combine all year tables using DuckDB
            if len(year_tables) == 1:
                year, single_table = year_tables[0]
                self.db.execute(
                    f"CREATE TABLE {table_name} AS SELECT *, {year} as data_year "
                    f"FROM {single_table}"
                )
            else:
                # Union all year tables
                union_queries = []
                for year, year_table in year_tables:
                    union_queries.append(f"SELECT *, {year} as data_year FROM {year_table}")

                combined_query = " UNION ALL ".join(union_queries)
                self.db.execute(f"CREATE TABLE {table_name} AS {combined_query}")

            # Log summary
            row_count = self.db.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            unique_cvr = self.db.execute(
                f"SELECT COUNT(DISTINCT cvr) FROM {table_name}"
            ).fetchone()[0]
            self.log.info(
                f"✅ Farm data loaded: {row_count:,} records from {unique_cvr:,} farms "
                f"across {len(available_years)} years"
            )

            return table_name

        except Exception as e:
            self.log.error(f"❌ Failed to load farm data: {e}")
            return None

    def _load_farm_data_for_year(self, year: int) -> Optional[str]:
        """
        Load farm data for a specific year from GCS bucket into DuckDB.

        Args:
            year: Year to load data for

        Returns:
            Table name with farm data or None if not available
        """
        try:
            # Load gødningsregnskab from GCS bucket
            # Dynamically find the latest timestamped directory for this year
            base_path = f"gs://{self.config.bucket}/silver/gr {year}/"
            latest_dir = self._get_latest_timestamped_directory(base_path, f"farm data {year}")

            if not latest_dir:
                self.log.warning(f"No farm data found for year {year} in {base_path}")
                return None

            self.log.info(f"Loading farm data from: {latest_dir}")

            # List available parquet files in the latest directory
            pattern = f"{latest_dir}*.parquet"
            files = self.gcs_access.list_files(pattern)

            if not files:
                self.log.warning(f"No farm data files found for year {year} in GCS: {pattern}")
                return None

            # Create temporary table name for this year
            main_table = f"farm_main_{year}"
            animal_table = f"farm_animal_{year}"
            final_table = f"farm_data_{year}"

            # Find main data files (typically 4061GR files or Del1/Del2 files)
            main_files = [
                f
                for f in files
                if any(pattern in f for pattern in ["4061GR", "_Del1", "_Del2", "FELTDEFINITION"])
            ]
            animal_files = [
                f for f in files if any(pattern in f for pattern in ["DYRERK", "B_DYRERK"])
            ]

            if not main_files:
                self.log.warning(f"No main farm data files found for year {year}")
                return None

            # Load main files
            self.log.info(f"Loading main farm data files: {len(main_files)} files")
            if len(main_files) == 1:
                self.gcs_access.create_table_from_gcs(main_table, main_files[0])
            else:
                # Combine multiple main files
                union_parts = []
                for i, file_path in enumerate(main_files):
                    temp_table = f"farm_temp_main_{year}_{i}"
                    self.gcs_access.create_table_from_gcs(temp_table, file_path)
                    union_parts.append(f"SELECT * FROM {temp_table}")

                # Create combined main table
                union_sql = f"CREATE TABLE {main_table} AS {' UNION ALL '.join(union_parts)}"
                self.db.execute(union_sql)

                # Clean up temp tables
                for i, _ in enumerate(main_files):
                    self.db.execute(f"DROP TABLE IF EXISTS farm_temp_main_{year}_{i}")

            # Load animal data if available
            if animal_files:
                self.log.info(f"Loading animal farm data files: {len(animal_files)} files")
                if len(animal_files) == 1:
                    self.gcs_access.create_table_from_gcs(animal_table, animal_files[0])
                else:
                    # Combine multiple animal files
                    union_parts = []
                    for i, file_path in enumerate(animal_files):
                        temp_table = f"farm_temp_animal_{year}_{i}"
                        self.gcs_access.create_table_from_gcs(temp_table, file_path)
                        union_parts.append(f"SELECT * FROM {temp_table}")

                    # Create combined animal table
                    union_sql = f"CREATE TABLE {animal_table} AS {' UNION ALL '.join(union_parts)}"
                    self.db.execute(union_sql)

                    # Clean up temp tables
                    for i, _ in enumerate(animal_files):
                        self.db.execute(f"DROP TABLE IF EXISTS farm_temp_animal_{year}_{i}")

                # Merge main data with animal data (try to find CVR column in different formats)
                try:
                    self.db.execute(f"""
                        CREATE TABLE {final_table} AS
                        SELECT m.*, 
                               COALESCE(a.organic_n_production, 0) as organic_n_production,
                               COALESCE(a.animal_count, 0) as animal_count,
                               COALESCE(a.animal_units, 0) as animal_units
                        FROM {main_table} m
                        LEFT JOIN {animal_table} a ON m.CVR = a.CVR
                    """)
                except Exception as e:
                    self.log.warning(f"Failed to join with animal data: {e}. Using main data only.")
                    self.db.execute(f"CREATE TABLE {final_table} AS SELECT * FROM {main_table}")

                # Drop temporary tables
                self.db.execute(f"DROP TABLE IF EXISTS {main_table}")
                self.db.execute(f"DROP TABLE IF EXISTS {animal_table}")
            else:
                # No animal data available, just use main data
                self.db.execute(f"CREATE TABLE {final_table} AS SELECT * FROM {main_table}")
                self.db.execute(f"DROP TABLE IF EXISTS {main_table}")

            # Check if data was loaded successfully
            try:
                row_count = self.db.execute(f"SELECT COUNT(*) FROM {final_table}").fetchone()[0]
                if row_count == 0:
                    self.log.warning(f"Farm data table for year {year} is empty")
                    return None

                self.log.info(f"Successfully loaded {row_count:,} farm records for year {year}")
                return final_table
            except Exception as e:
                self.log.error(f"Failed to verify farm data for year {year}: {e}")
                return None

        except Exception as e:
            self.log.error(f"Failed to load farm data for year {year}: {e}")
            return None

    def _find_main_farm_file(self, base_path: Path) -> Optional[Path]:
        """
        Legacy method for finding main farm data file - no longer used with GCS implementation.

        This method is kept for backward compatibility but should not be called
        in the GCS-based farm data loading.
        """
        self.log.warning("_find_main_farm_file called but farm data is now loaded from GCS")
        return None

    def _load_file_to_duckdb(self, file_path: Path, table_name: str) -> bool:
        """
        Legacy method for loading CSV/Excel files to DuckDB
        - no longer used with GCS implementation.

        This method is kept for backward compatibility but should not be called
        in the GCS-based farm data loading where parquet files are used directly.
        """
        self.log.warning(
            "_load_file_to_duckdb called but farm data is now loaded from GCS parquet files"
        )
        return False

    def _load_and_combine_animal_data(self, animal_files: List[Path], animal_table: str) -> None:
        """
        Legacy method for loading and combining animal data files
        - no longer used with GCS implementation.

        This method is kept for backward compatibility but should not be called
        in the GCS-based farm data loading where animal data is handled directly
        in _load_farm_data_for_year.
        """
        self.log.warning(
            "_load_and_combine_animal_data called but farm data is now loaded from GCS"
        )
        return

    def _get_key_farm_data_columns(self) -> List[str]:
        """
        Get the list of key farm data columns needed for NLES5 integration.

        Returns:
            List of column names to extract from farm data
        """
        return [
            "cvr",  # Farm identifier
            # Quota and compliance data
            "f_901",  # Total nitrogen consumption
            "f_902",  # Quota minus consumption
            "f_512",  # Corrected nitrogen quota
            # Actual fertilizer applications
            "f_703_1",  # Spring mineral fertilizer
            "f_706_1",  # Autumn mineral fertilizer
            "f_308_1",  # Actual manure consumption
            "f_318_1",  # Grazing applications
            # Farm infrastructure
            "f_101_1",  # Total cultivated area (calculated)
            "f_101_2",  # Total cultivated area (manual)
            "f_106_1",  # Harmony area (calculated)
            "f_106_2",  # Harmony area (manual)
            # Animal production (from aggregated data)
            "organic_n_production",  # C_2016 aggregated
            "animal_count",  # C_2006 aggregated
            "animal_units",  # C_2017 aggregated
        ]

    @timed(name="Loading agricultural fields data")
    def _load_agricultural_fields_data(self, silver_data: Optional[Dict[str, Any]]) -> str:
        """
        Load agricultural fields data from multiple yearly datasets.

        Args:
            silver_data: Optional in-memory silver data

        Returns:
            Table name containing combined agricultural fields data
        """
        # OPTIMIZATION: Determine target calculation years and required supporting years for NLES5
        if self.config.target_years:
            target_calculation_years = self.config.target_years
            self.log.info(f"Target NLES5 calculation years: {target_calculation_years}")
        else:
            all_available_years = self._get_available_fvm_marker_years()
            # Apply year limit for memory management
            if self.config.max_years_to_process:
                # Take the most recent years up to the limit as targets
                target_calculation_years = sorted(all_available_years)[
                    -self.config.max_years_to_process :
                ]
                self.log.info(
                    f"Auto-discovered {len(all_available_years)} available, "
                    f"targeting most recent {len(target_calculation_years)}: "
                    f"{target_calculation_years}"
                )
            else:
                target_calculation_years = all_available_years
                self.log.info(
                    f"Auto-discovered target years (no limit): {target_calculation_years}"
                )

        if not target_calculation_years:
            self.log.error("No FVM marker years found to process")
            raise ValueError("No FVM marker data available")

        # CRITICAL OPTIMIZATION: Calculate minimum years needed for NLES5 (3-year windows)
        years_to_load = self.processor._calculate_required_data_years(
            target_calculation_years,
            all_available_years
            if "all_available_years" in locals()
            else self._get_available_fvm_marker_years(),
        )

        self.log.info("🎯 NLES5 Memory Optimization:")
        self.log.info(
            f"   Target calculation years: {len(target_calculation_years)} years → "
            f"{target_calculation_years}"
        )
        self.log.info(f"   Required data years: {len(years_to_load)} years → {years_to_load}")
        all_years_len = len(
            all_available_years
            if "all_available_years" in locals()
            else self._get_available_fvm_marker_years()
        )
        self.log.info(f"   Memory reduction: {all_years_len - len(years_to_load)} years eliminated")

        years_to_process = years_to_load

        # Process each year and collect table names
        yearly_tables = []
        for i, year in enumerate(years_to_process):
            try:
                # Clean up temp files more frequently to manage disk space
                if i > 0 and i % 2 == 0:  # Every 2 years instead of 3
                    self.log.info(f"Cleaning up temporary files after processing {i} years...")
                    self.processor._cleanup_temp_files()

                # Check if data is available in silver_data dict
                year_dataset = (
                    f"{self.config.agricultural_fields_dataset}_{year}"  # Use fvm_marker_YYYY
                )
                table_name = f"agricultural_fields_{year}"

                if silver_data and year_dataset in silver_data:
                    self.log.info(f"Using in-memory data for {year_dataset}")
                    yearly_tables.append((year, silver_data[year_dataset]))
                    continue

                # Read from GCS
                fvm_path = self._read_fvm_marker_data_for_year(year)
                if not fvm_path:
                    self.log.warning(f"⚠️ No FVM marker data found for year {year}, skipping")
                    continue

                # Load the data
                success = self._read_silver_data_from_path(year_dataset, fvm_path, table_name)
                if success:
                    yearly_tables.append((year, table_name))
                    self.log.info(f"✅ Loaded agricultural fields for {year}")
                else:
                    self.log.error(f"❌ Failed to load agricultural fields for {year}")

            except Exception as e:
                self.log.error(f"❌ Error processing year {year}: {e}")
                continue

        if not yearly_tables:
            raise ValueError("No agricultural fields data could be loaded")

        # Combine all yearly tables
        combined_table = self._combine_yearly_fvm_data(
            {year: table for year, table in yearly_tables}
        )

        self.log.info(
            f"✅ Agricultural fields data loading completed: {len(yearly_tables)} years processed"
        )
        return combined_table

    def _load_agricultural_fields_for_years(self, years: List[int], table_name: str):
        """
        Load agricultural fields data for specific years into a named table.

        Args:
            years: List of years to load
            table_name: Name of the target table
        """
        try:
            self.log.info(f"📊 Loading agricultural fields for years: {years}")

            yearly_tables = {}
            for year in years:
                year_table = f"agricultural_fields_{year}"
                fvm_path = self._read_fvm_marker_data_for_year(year)

                if fvm_path:
                    success = self._read_silver_data_from_path(
                        f"{self.config.agricultural_fields_dataset}_{year}", fvm_path, year_table
                    )
                    if success:
                        yearly_tables[year] = year_table
                        self.log.info(f"✅ Loaded agricultural fields for {year}")
                    else:
                        self.log.warning(f"⚠️ Failed to load agricultural fields for {year}")
                else:
                    self.log.warning(f"⚠️ No FVM marker data found for year {year}")

            if yearly_tables:
                # Combine the tables
                combined_table = self._combine_yearly_fvm_data(yearly_tables)

                # Rename to the target table name
                self.db.execute(f"DROP TABLE IF EXISTS {table_name}")
                self.db.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {combined_table}")
                self.db.execute(f"DROP TABLE IF EXISTS {combined_table}")

                row_count = self.db.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                self.log.info(
                    f"✅ Agricultural fields loaded for {len(yearly_tables)} years: "
                    f"{row_count:,} rows"
                )
            else:
                raise ValueError(f"No agricultural fields data could be loaded for years: {years}")

        except Exception as e:
            self.log.error(f"❌ Failed to load agricultural fields for years {years}: {e}")
            raise

    def _load_required_silver_datasets_for_batch(
        self, silver_data: Optional[Dict[str, Any]], batch_years: List[int]
    ) -> Dict[str, str]:
        """
        Load required silver datasets for a specific batch of years.

        Args:
            silver_data: Optional in-memory silver data
            batch_years: List of years in the current batch

        Returns:
            Dictionary mapping dataset names to table names
        """
        self.log.info(f"📂 Loading silver datasets for batch years: {batch_years}")

        # Use the same logic as the main loader but with batch-specific optimizations
        loaded_tables = self._load_required_silver_datasets(silver_data)

        # Log batch-specific information
        self.log.info(f"✅ Loaded {len(loaded_tables)} datasets for batch processing")

        return loaded_tables

    def _load_agricultural_fields_data_for_batch(
        self,
        silver_data: Optional[Dict[str, Any]],
        batch_years: List[int],
        loaded_tables: Dict[str, str],
    ) -> str:
        """
        Load agricultural fields data for a specific batch of years.

        Args:
            silver_data: Optional in-memory silver data
            batch_years: List of years in the current batch
            loaded_tables: Dictionary of already loaded tables

        Returns:
            Table name containing combined agricultural fields data for the batch
        """
        self.log.info(f"📊 Loading agricultural fields for batch years: {batch_years}")

        # Calculate required data years for this batch (including 3-year windows)
        all_available_years = self._get_available_fvm_marker_years()
        required_years = self.processor._calculate_required_data_years(
            batch_years, all_available_years
        )

        self.log.info(f"Batch {batch_years} requires data years: {required_years}")

        # Load the agricultural fields for the required years
        # Use the standard table name expected by nles5_calculator
        self._load_agricultural_fields_for_years(required_years, "agricultural_fields")

        return "agricultural_fields"

    def _combine_yearly_fvm_data(self, yearly_tables: Dict[int, str]) -> str:
        """
        Combine FVM data from multiple years into a single table.

        Args:
            yearly_tables: Dictionary mapping years to table names

        Returns:
            Name of the combined table (temporary name to avoid conflicts)
        """
        if not yearly_tables:
            raise ValueError("No yearly tables to combine")

        # Use a unique temporary table name to avoid naming conflicts
        # The calling method will rename this to the final desired name
        combined_table_name = "temp_agricultural_fields_combined"

        # Clean up any existing table
        self.processor.conn.execute(f"DROP TABLE IF EXISTS {combined_table_name}")

        # Create the combined table by unioning all yearly tables
        union_parts = []
        for year, table_name in yearly_tables.items():
            # Verify the source table exists before adding to union
            try:
                table_count = self.processor.conn.execute(
                    f"SELECT COUNT(*) FROM {table_name}"
                ).fetchone()[0]
                if table_count > 0:
                    union_parts.append(f"SELECT *, {year} AS data_year FROM {table_name}")
                    self.log.info(
                        f"   Including {table_name}: {table_count:,} rows for year {year}"
                    )
                else:
                    self.log.warning(f"   Skipping empty table {table_name} for year {year}")
            except Exception as e:
                self.log.warning(f"   Skipping missing table {table_name} for year {year}: {e}")

        if not union_parts:
            raise ValueError("No valid yearly tables found to combine")

        # Create the union table
        union_sql = f"""
        CREATE TABLE {combined_table_name} AS
        {" UNION ALL ".join(union_parts)}
        """

        self.log.info(f"Creating combined table from {len(union_parts)} yearly tables...")
        self.processor.conn.execute(union_sql)

        # Verify the combined table was created successfully
        row_count = self.processor.conn.execute(
            f"SELECT COUNT(*) FROM {combined_table_name}"
        ).fetchone()[0]
        if row_count == 0:
            raise ValueError(f"Combined table {combined_table_name} is empty after union")

        year_count = len(yearly_tables)
        self.log.info(f"✅ Combined {year_count} years of FVM data: {row_count:,} total rows")

        return combined_table_name

    def _add_year_to_fertilizer_data(self, file_path: str, table_name: str) -> None:
        """
        Add year column to fertilizer data by extracting it from the filename.

        Args:
            file_path: GCS path to the fertilizer file (e.g., "Gødningsregnskaber 2023.parquet")
            table_name: Name of the table to update (e.g., "fertilizer_accounts")
        """
        import re

        # Extract 4-digit year from filename
        filename = file_path.split("/")[-1]  # Get just the filename
        year_match = re.search(r"(\d{4})", filename)

        if not year_match:
            self.log.warning(f"⚠️ Could not extract year from filename: {filename}")
            return

        year = int(year_match.group(1))
        self.log.info(f"🗓️ Adding year {year} to fertilizer data from filename: {filename}")

        # Check if table exists before trying to modify it
        try:
            tables_list = [t[0] for t in self.processor.conn.execute("SHOW TABLES").fetchall()]
            self.log.info(f"🔍 Available tables before year addition: {tables_list}")

            # Verify the table exists
            self.processor.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            self.log.info(f"✅ Table {table_name} found successfully")

            # Add year column to the existing table as INTEGER (not VARCHAR)
            self.processor.conn.execute(f"""
                ALTER TABLE {table_name} 
                ADD COLUMN IF NOT EXISTS year INTEGER DEFAULT {year}
            """)

            # Update all rows with the extracted year, ensuring it's stored as INTEGER
            self.processor.conn.execute(f"""
                UPDATE {table_name} 
                SET year = CAST({year} AS INTEGER)
                WHERE year IS NULL OR year = {year}
            """)

            self.log.info(f"✅ Successfully added year column to {table_name}: {year}")

        except Exception as e:
            self.log.error(f"❌ Failed to add year to fertilizer data: {e}")
            # List available tables for debugging
            try:
                tables = self.processor.conn.execute("SHOW TABLES").fetchall()
                self.log.error(f"Available tables: {[t[0] for t in tables]}")
            except Exception:
                self.log.error("Could not list available tables")

    def _transform_raw_fertilizer_data(self, table_name: str) -> None:
        """
        Transform raw fertilizer data to expected schema.

        The Gødningsregnskaber files can have different structures depending on the source:
        1. Farm accounting data with form codes (f_901, f_902, etc.)
        2. GKEA-style field data (cvr_number, marknummer, areal, etc.)

        Args:
            table_name: Name of the fertilizer table to transform
        """
        try:
            self.log.info("🔧 Analyzing fertilizer data structure...")

            # Check current columns
            columns = self.processor.conn.execute(f"DESCRIBE {table_name}").fetchall()
            column_names = [col[0] for col in columns]
            column_names_lower = [c.lower() for c in column_names]

            self.log.info(
                f"📋 Available fertilizer columns ({len(column_names)}): {column_names[:15]}"
            )

            # Detect file type based on columns
            has_form_codes = any(c.startswith("f_") for c in column_names_lower)
            has_field_data = (
                "marknummer" in column_names_lower and "faktisk_areal_ha" in column_names_lower
            )

            if has_form_codes:
                self.log.info("✅ Detected farm accounting data (form codes: f_901, f_902, etc.)")
                self._transform_fertilizer_accounting_data(table_name, column_names)
            elif has_field_data:
                self.log.info("✅ Detected GKEA-style field data (marknummer, areal, etc.)")
                self._transform_fertilizer_field_data(table_name, column_names)
            else:
                self.log.warning("⚠️ Unknown fertilizer file structure. Creating minimal schema.")
                self._create_minimal_fertilizer_schema(table_name)

        except Exception as e:
            self.log.error(f"❌ Failed to transform fertilizer data: {e}")
            import traceback

            self.log.debug(f"Traceback: {traceback.format_exc()}")
            self._create_minimal_fertilizer_schema(table_name)

    def _transform_fertilizer_accounting_data(
        self, table_name: str, column_names: List[str]
    ) -> None:
        """Transform fertilizer accounting data with form codes (f_901, etc.)."""
        try:
            # Map form codes to expected columns
            self.processor.conn.execute(f"""
                CREATE OR REPLACE TABLE {table_name}_transformed AS
                SELECT
                    cvr_number,
                    year,
                    COALESCE(TRY_CAST(f_901 AS DOUBLE), 0.0) as tn_t_ha,
                    COALESCE(TRY_CAST(f_185_2 AS DOUBLE), 0.0) as mineral_n_foraar,
                    COALESCE(TRY_CAST(f_185_3 AS DOUBLE), 0.0) as mineral_n_eft,
                    COALESCE(TRY_CAST(f_188_2 AS DOUBLE), 0.0) as mineral_n_udb,
                    COALESCE(TRY_CAST(f_601_2 AS DOUBLE), 0.0) as organic_n_hus,
                    'Standard' as niveau
                FROM {table_name}
                WHERE cvr_number IS NOT NULL
            """)

            self.processor.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.processor.conn.execute(
                f"ALTER TABLE {table_name}_transformed RENAME TO {table_name}"
            )

            row_count = self.processor.conn.execute(
                f"SELECT COUNT(*) FROM {table_name}"
            ).fetchone()[0]
            self.log.info(f"✅ Transformed accounting data: {row_count:,} rows")

        except Exception as e:
            self.log.error(f"Failed to transform accounting data: {e}")
            raise

    def _transform_fertilizer_field_data(self, table_name: str, column_names: List[str]) -> None:
        """Transform GKEA-style field data to fertilizer schema."""
        try:
            # This is actually field plan data, not fertilizer accounts
            # For now, just keep the original structure since the NLES pipeline
            # may not actually use this fertilizer data if field plans are available
            self.log.info("ℹ️  File appears to be field plan data, not fertilizer accounts")
            self.log.info("   The NLES pipeline will use the separate field_plan dataset")

            # Create a minimal compatible schema just in case
            self._create_minimal_fertilizer_schema(table_name)

        except Exception as e:
            self.log.error(f"Failed to handle field data: {e}")
            raise

    def _create_minimal_fertilizer_schema(self, table_name: str) -> None:
        """Create minimal fertilizer schema with defaults."""
        try:
            # Check if cvr_number exists
            columns = self.processor.conn.execute(f"DESCRIBE {table_name}").fetchall()
            column_names = [col[0] for col in columns]

            has_cvr = "cvr_number" in column_names or "CVR" in column_names
            has_year = "year" in column_names

            cvr_col = (
                "cvr_number"
                if "cvr_number" in column_names
                else "CVR"
                if "CVR" in column_names
                else None
            )

            if has_cvr and has_year:
                self.processor.conn.execute(f"""
                    CREATE OR REPLACE TABLE {table_name}_minimal AS
                    SELECT
                        {cvr_col} as cvr_number,
                        year,
                        0.0 as tn_t_ha,
                        0.0 as mineral_n_foraar,
                        0.0 as mineral_n_eft,
                        0.0 as mineral_n_udb,
                        0.0 as organic_n_hus,
                        'Default' as niveau
                    FROM {table_name}
                    WHERE {cvr_col} IS NOT NULL
                """)

                self.processor.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                self.processor.conn.execute(
                    f"ALTER TABLE {table_name}_minimal RENAME TO {table_name}"
                )

                row_count = self.processor.conn.execute(
                    f"SELECT COUNT(*) FROM {table_name}"
                ).fetchone()[0]
                self.log.info(
                    f"✅ Created minimal fertilizer schema: {row_count:,} rows "
                    f"(defaults for nitrogen values)"
                )
            else:
                self.log.warning(
                    "⚠️ Cannot create minimal schema - missing cvr_number or year columns"
                )
                self.log.warning(f"   Available columns: {column_names}")

        except Exception as e:
            self.log.error(f"Failed to create minimal schema: {e}")
