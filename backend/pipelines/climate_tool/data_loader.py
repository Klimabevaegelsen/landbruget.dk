"""
GCS Data Loader for Climate Tool

This module loads data from GCS silver/gold layers for climate calculations.
Uses the GCSDataAccess pattern from unified_pipeline for optimal performance.

Data Sources (ACTUAL GCS structure from exploration):
- Livestock data: silver/gr {year}/ (Green Accounts with space in path!)
  Fields: cvr_number, c_2001 (species), c_2006 (animal count), c_2016 (total N)
  Years: 2018-2023, 64,999 farm records per year
- Field data: silver/fvm_marker_{year}/ (FVM WFS pipeline)
  Fields: cvr, bfe_nummer, afgroede, areal_ha
  Years: 2008-2025
- Fertilizer data: silver/fertiliser/ (GKEA gødningsregnskab)
  Fields: cvr_number, total_n_kvote, faktisk_areal_ha, year
  Years: 2021-2024, 585,988 field records (2024)

Usage:
    loader = ClimateDataLoader()
    livestock_df = loader.load_livestock(cvr="12345678", year=2024)
    fields_df = loader.load_fields(cvr="12345678", year=2024)
"""

import os
import sys
from typing import Optional, List, Dict, Any
from pathlib import Path
import pandas as pd

# Add unified_pipeline to path for GCSDataAccess import
unified_pipeline_path = Path(__file__).parent.parent / "unified_pipeline" / "src"
if str(unified_pipeline_path) not in sys.path:
    sys.path.insert(0, str(unified_pipeline_path))

from unified_pipeline.util.gcs_access import GCSDataAccess
from unified_pipeline.util.log_util import Logger

logger = Logger.get_logger()


class ClimateDataLoader:
    """
    Loads agricultural data from GCS silver/gold layers for climate calculations.

    This loader provides convenient access to:
    - CHR livestock data (herds, animal counts, movements)
    - FVM field data (agricultural fields, crop types, areas)
    - Fertilizer application data (gødningsregnskab)
    - Climate/weather data (DMI, optional)
    """

    def __init__(self, bucket: str = None):
        """
        Initialize the climate data loader.

        Args:
            bucket: GCS bucket name. Defaults to GCS_BUCKET env var or 'landbrugsdata-raw-data'
        """
        self.bucket = bucket or os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")
        self.gcs = GCSDataAccess()
        logger.info(f"✅ ClimateDataLoader initialized with bucket: {self.bucket}")

    def load_livestock(self, cvr: str, year: Optional[int] = None) -> pd.DataFrame:
        """
        Load livestock data from Green Accounts (gr) silver layer for a specific CVR number.

        This queries the Green Accounts data for livestock associated with a CVR, including:
        - Animal counts by species and type (c_2001: species, c_2006: count)
        - Housing systems (c_2005: type, c_2030: code)
        - Nitrogen production norms (c_2016: total N kg)
        - Manure types and characteristics

        Args:
            cvr: Company CVR number (8 digits)
            year: Year (2018-2023). If None, uses 2023 (latest)

        Returns:
            DataFrame with livestock data, or empty DataFrame if no data found

        Example:
            >>> loader = ClimateDataLoader()
            >>> livestock = loader.load_livestock(cvr="31373077", year=2023)
            >>> print(livestock[['cvr_number', 'c_2001', 'c_2006', 'c_2016']])
        """
        try:
            # Validate CVR format
            cvr_str = str(cvr).zfill(8)
            if len(cvr_str) != 8 or not cvr_str.isdigit():
                logger.error(f"Invalid CVR format: {cvr}. Must be 8 digits.")
                return pd.DataFrame()

            # Default to 2023 (latest available year)
            if year is None:
                year = 2023
                logger.info(f"No year specified, using latest: {year}")

            # Green Accounts path - NOTE: has space in folder name!
            # DYRERK = Animal records (Dyre Regnskab)
            pattern = f"gs://{self.bucket}/silver/gr {year}/*/V_4061GR_*_DYRERK_*_pii_handled.parquet"
            files = self.gcs.list_files(pattern)

            if not files:
                logger.warning(f"No Green Accounts livestock data found in {pattern}")
                return pd.DataFrame()

            # Use latest file (sorted by timestamp in path)
            latest_file = sorted(files)[-1]
            logger.info(f"Loading livestock data from: {latest_file}")

            # Create table in DuckDB
            table_name = "gr_livestock_temp"
            self.gcs.create_table_from_gcs(table_name, latest_file)

            # Query for specific CVR - use cvr_number column (not cvr)
            query = f"""
                SELECT *
                FROM {table_name}
                WHERE cvr_number = '{cvr_str}'
            """

            result_df = self.gcs.duckdb_conn.execute(query).df()

            # Cleanup
            self.gcs.duckdb_conn.execute(f"DROP TABLE IF EXISTS {table_name}")

            logger.info(f"✅ Loaded {len(result_df)} livestock records for CVR {cvr_str}, year {year}")
            return result_df

        except Exception as e:
            logger.error(f"Error loading livestock data for CVR {cvr}: {e}")
            return pd.DataFrame()

    def load_fields(self, cvr: str, year: int) -> pd.DataFrame:
        """
        Load field data from FVM silver layer for a specific CVR and year.

        This queries the agricultural field data including:
        - Field boundaries (BFE numbers)
        - Crop types and areas
        - Field management practices

        Args:
            cvr: Company CVR number (8 digits)
            year: Agricultural year (YYYY)

        Returns:
            DataFrame with field data, or empty DataFrame if no data found

        Example:
            >>> loader = ClimateDataLoader()
            >>> fields = loader.load_fields(cvr="31373077", year=2024)
            >>> print(fields[['bfe_nummer', 'afgroede', 'areal_ha']])
        """
        try:
            # Validate CVR format
            cvr_str = str(cvr).zfill(8)
            if len(cvr_str) != 8 or not cvr_str.isdigit():
                logger.error(f"Invalid CVR format: {cvr}. Must be 8 digits.")
                return pd.DataFrame()

            # Find FVM data for specific year
            pattern = f"gs://{self.bucket}/silver/fvm_marker_{year}/*/data.parquet"
            files = self.gcs.list_files(pattern)

            if not files:
                logger.warning(f"No field data found for year {year} in {pattern}")
                return pd.DataFrame()

            # Use latest file for the year
            latest_file = sorted(files)[-1]
            logger.info(f"Loading field data from: {latest_file}")

            # Create table in DuckDB
            table_name = "fvm_fields_temp"
            self.gcs.create_table_from_gcs(table_name, latest_file)

            # Query for specific CVR - FVM data uses cvr_number column
            query = f"""
                SELECT *
                FROM {table_name}
                WHERE cvr_number = '{cvr_str}'
            """

            result_df = self.gcs.duckdb_conn.execute(query).df()

            # Cleanup
            self.gcs.duckdb_conn.execute(f"DROP TABLE IF EXISTS {table_name}")

            logger.info(f"✅ Loaded {len(result_df)} field records for CVR {cvr_str}, year {year}")
            return result_df

        except Exception as e:
            logger.error(f"Error loading field data for CVR {cvr}, year {year}: {e}")
            return pd.DataFrame()

    def load_fertilizer(self, cvr: str, year: int) -> pd.DataFrame:
        """
        Load fertilizer application data from GKEA (Gødningskvoteberegning) for a specific CVR and year.

        This queries the GKEA fertilizer accounting data including:
        - total_n_kvote: Total N quota in kg (primary field for N2O calculation)
        - faktisk_areal_ha: Actual field area in hectares
        - marknummer: Field number
        - fosfortal: Phosphorus number

        Args:
            cvr: Company CVR number (8 digits)
            year: Agricultural year (2021-2024)

        Returns:
            DataFrame with fertilizer data, or empty DataFrame if no data found

        Example:
            >>> loader = ClimateDataLoader()
            >>> fert = loader.load_fertilizer(cvr="31373077", year=2024)
            >>> print(fert[['cvr_number', 'total_n_kvote', 'faktisk_areal_ha']])
            >>> # Calculate N2O: total_n_kvote * 0.01 * (44/28) * 298 = kg CO2e
        """
        try:
            # Validate CVR format
            cvr_str = str(cvr).zfill(8)
            if len(cvr_str) != 8 or not cvr_str.isdigit():
                logger.error(f"Invalid CVR format: {cvr}. Must be 8 digits.")
                return pd.DataFrame()

            # GKEA fertilizer data - files named like GKEA2024_Markplan_med_Gødningsoplysninger.parquet
            # Located in: silver/fertiliser/
            pattern = f"gs://{self.bucket}/silver/fertiliser/GKEA{year}_*.parquet"
            files = self.gcs.list_files(pattern)

            if not files:
                logger.warning(f"No GKEA fertilizer data found for year {year} in pattern: {pattern}")
                # Try without year filter
                pattern_fallback = f"gs://{self.bucket}/silver/fertiliser/GKEA*.parquet"
                files = self.gcs.list_files(pattern_fallback)
                if files:
                    logger.info(f"Found {len(files)} GKEA files without year filter")

            if not files:
                logger.warning(f"No GKEA fertilizer data found at all")
                return pd.DataFrame()

            # Use file matching the year (or latest)
            matching_files = [f for f in files if f"GKEA{year}" in f]
            if matching_files:
                target_file = sorted(matching_files)[-1]
            else:
                target_file = sorted(files)[-1]
                logger.warning(f"No exact year match, using latest: {target_file}")

            logger.info(f"Loading fertilizer data from: {target_file}")

            # Create table in DuckDB
            table_name = "fertilizer_temp"
            self.gcs.create_table_from_gcs(table_name, target_file)

            # Query for specific CVR - GKEA uses cvr_number column
            query = f"""
                SELECT *
                FROM {table_name}
                WHERE cvr_number = '{cvr_str}'
            """

            # GKEA has 'year' column
            columns = self.gcs.duckdb_conn.execute(f"SELECT * FROM {table_name} LIMIT 0").df().columns.tolist()

            if "year" in columns:
                query += f" AND year = {year}"
            elif "aar" in columns:
                query += f" AND aar = {year}"

            result_df = self.gcs.duckdb_conn.execute(query).df()

            # Cleanup
            self.gcs.duckdb_conn.execute(f"DROP TABLE IF EXISTS {table_name}")

            logger.info(f"✅ Loaded {len(result_df)} fertilizer records for CVR {cvr_str}, year {year}")
            return result_df

        except Exception as e:
            logger.error(f"Error loading fertilizer data for CVR {cvr}, year {year}: {e}")
            return pd.DataFrame()

    def load_climate_data(self, cvr: str, year: int) -> pd.DataFrame:
        """
        Load climate/weather data from DMI for a specific location and year.

        This is optional and may not be available for all locations.

        Args:
            cvr: Company CVR number (8 digits) - used to find location
            year: Year (YYYY)

        Returns:
            DataFrame with climate data, or empty DataFrame if not available
        """
        try:
            # DMI climate data might be structured by location/year
            pattern = f"gs://{self.bucket}/silver/dmi/*/data.parquet"
            files = self.gcs.list_files(pattern)

            if not files:
                logger.info("No DMI climate data found (optional)")
                return pd.DataFrame()

            # Use latest file
            latest_file = sorted(files)[-1]
            logger.info(f"Loading climate data from: {latest_file}")

            # Create table and query (implementation depends on DMI data structure)
            table_name = "dmi_climate_temp"
            self.gcs.create_table_from_gcs(table_name, latest_file)

            query = f"SELECT * FROM {table_name} WHERE year = {year}"
            result_df = self.gcs.duckdb_conn.execute(query).df()

            # Cleanup
            self.gcs.duckdb_conn.execute(f"DROP TABLE IF EXISTS {table_name}")

            logger.info(f"✅ Loaded {len(result_df)} climate records for year {year}")
            return result_df

        except Exception as e:
            logger.warning(f"Climate data not available: {e}")
            return pd.DataFrame()

    def list_available_years(self, dataset: str) -> List[int]:
        """
        List available years for a specific dataset.

        Args:
            dataset: Dataset name ('fvm_marker', 'chr', etc.)

        Returns:
            List of available years (sorted)
        """
        try:
            if dataset == "fvm_marker":
                pattern = f"gs://{self.bucket}/silver/fvm_marker_*/*/*.parquet"
                files = self.gcs.list_files(pattern)
                years = set()

                import re

                for file_path in files:
                    match = re.search(r"fvm_marker_(\d{4})/", file_path)
                    if match:
                        years.add(int(match.group(1)))

                return sorted(list(years))

            elif dataset == "chr":
                # CHR data is timestamped, not year-specific in path
                # Return years based on data content if possible
                logger.info("CHR data is timestamped. Use load_livestock() with year parameter.")
                return []

            else:
                logger.warning(f"Unknown dataset: {dataset}")
                return []

        except Exception as e:
            logger.error(f"Error listing available years for {dataset}: {e}")
            return []

    def get_latest_data_timestamp(self, dataset: str) -> Optional[str]:
        """
        Get the timestamp of the latest data file for a dataset.

        Args:
            dataset: Dataset name ('chr', 'fvm_marker_YYYY', etc.)

        Returns:
            Timestamp string (YYYYMMDD_HHMMSS) or None if not found
        """
        try:
            if dataset.startswith("fvm_marker_"):
                pattern = f"gs://{self.bucket}/silver/{dataset}/*/data.parquet"
            else:
                pattern = f"gs://{self.bucket}/silver/{dataset}/*/*.parquet"

            files = self.gcs.list_files(pattern)

            if not files:
                return None

            # Extract timestamp from path
            import re

            latest_file = sorted(files)[-1]
            match = re.search(r"/(\d{8}_\d{6})/", latest_file)

            if match:
                return match.group(1)

            return None

        except Exception as e:
            logger.error(f"Error getting latest timestamp for {dataset}: {e}")
            return None

    def __del__(self):
        """Cleanup DuckDB connection on deletion."""
        try:
            if hasattr(self, "gcs") and self.gcs:
                # GCSDataAccess handles its own cleanup
                pass
        except Exception:
            pass
