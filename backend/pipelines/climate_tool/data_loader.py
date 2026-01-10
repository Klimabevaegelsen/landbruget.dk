"""
GCS Data Loader for Climate Tool

This module loads data from GCS silver/gold layers for climate calculations.
Uses the GCSDataAccess pattern from unified_pipeline for optimal performance.

Data Sources:
- Livestock data: silver/chr (CHR pipeline)
- Field data: silver/fvm_marker_YYYY (FVM WFS pipeline)
- Fertilizer data: silver/fertiliser (gødningsregnskab from drive_data)
- Climate data: silver/dmi (optional, if needed)

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
        Load livestock data from CHR silver layer for a specific CVR number.

        This queries the CHR data for herds associated with a CVR, including:
        - Animal counts by species and type
        - Herd locations (CHR numbers)
        - Animal movements and changes

        Args:
            cvr: Company CVR number (8 digits)
            year: Optional year filter (if None, loads latest available data)

        Returns:
            DataFrame with livestock data, or empty DataFrame if no data found

        Example:
            >>> loader = ClimateDataLoader()
            >>> livestock = loader.load_livestock(cvr="31373077", year=2024)
            >>> print(livestock[['chr_nummer', 'species_code', 'animal_count']])
        """
        try:
            # Validate CVR format
            cvr_str = str(cvr).zfill(8)
            if len(cvr_str) != 8 or not cvr_str.isdigit():
                logger.error(f"Invalid CVR format: {cvr}. Must be 8 digits.")
                return pd.DataFrame()

            # Find latest CHR data file
            pattern = f"gs://{self.bucket}/silver/chr/*/herds*.parquet"
            files = self.gcs.list_files(pattern)

            if not files:
                logger.warning(f"No CHR livestock data found in {pattern}")
                return pd.DataFrame()

            # Use latest file (sorted by timestamp in path)
            latest_file = sorted(files)[-1]
            logger.info(f"Loading livestock data from: {latest_file}")

            # Create table in DuckDB
            table_name = "chr_livestock_temp"
            self.gcs.create_table_from_gcs(table_name, latest_file)

            # Query for specific CVR
            query = f"""
                SELECT *
                FROM {table_name}
                WHERE cvr = '{cvr_str}'
            """

            if year:
                query += f" AND EXTRACT(YEAR FROM dato) = {year}"

            result_df = self.gcs.duckdb_conn.execute(query).df()

            # Cleanup
            self.gcs.duckdb_conn.execute(f"DROP TABLE IF EXISTS {table_name}")

            logger.info(f"✅ Loaded {len(result_df)} livestock records for CVR {cvr_str}")
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

            # Query for specific CVR
            query = f"""
                SELECT *
                FROM {table_name}
                WHERE cvr = '{cvr_str}'
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
        Load fertilizer application data for a specific CVR and year.

        This queries the gødningsregnskab (fertilizer accounting) data including:
        - Fertilizer types and amounts (N, P, K)
        - Application dates and methods
        - Field-level allocations

        Args:
            cvr: Company CVR number (8 digits)
            year: Agricultural year (YYYY)

        Returns:
            DataFrame with fertilizer data, or empty DataFrame if no data found

        Example:
            >>> loader = ClimateDataLoader()
            >>> fert = loader.load_fertilizer(cvr="31373077", year=2024)
            >>> print(fert[['goedningstype', 'n_kg_ha', 'areal_ha']])
        """
        try:
            # Validate CVR format
            cvr_str = str(cvr).zfill(8)
            if len(cvr_str) != 8 or not cvr_str.isdigit():
                logger.error(f"Invalid CVR format: {cvr}. Must be 8 digits.")
                return pd.DataFrame()

            # Try to find fertilizer data from drive_data_pipeline silver layer
            # Format: silver/fertiliser_* or silver/gødningsregnskab_*
            patterns = [
                f"gs://{self.bucket}/silver/fertiliser/*/data.parquet",
                f"gs://{self.bucket}/silver/goedningsregnskab/*/data.parquet",
            ]

            files = []
            for pattern in patterns:
                found = self.gcs.list_files(pattern)
                if found:
                    files.extend(found)

            if not files:
                logger.warning(f"No fertilizer data found. Tried patterns: {patterns}")
                return pd.DataFrame()

            # Use latest file
            latest_file = sorted(files)[-1]
            logger.info(f"Loading fertilizer data from: {latest_file}")

            # Create table in DuckDB
            table_name = "fertilizer_temp"
            self.gcs.create_table_from_gcs(table_name, latest_file)

            # Query for specific CVR and year
            query = f"""
                SELECT *
                FROM {table_name}
                WHERE cvr = '{cvr_str}'
            """

            # Try to filter by year if column exists
            columns = self.gcs.duckdb_conn.execute(f"SELECT * FROM {table_name} LIMIT 0").df().columns.tolist()

            if "aar" in columns:
                query += f" AND aar = {year}"
            elif "year" in columns:
                query += f" AND year = {year}"

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
