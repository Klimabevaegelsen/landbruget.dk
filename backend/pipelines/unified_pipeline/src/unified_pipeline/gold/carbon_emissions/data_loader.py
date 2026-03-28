"""
GCS Data Loader for Climate Tool

This module loads data from GCS silver/gold layers for climate calculations.
Uses the StorageAccess pattern from unified_pipeline for optimal performance.

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
    livestock_rel = loader.load_livestock(cvr="12345678", year=2024)
    fields_rel = loader.load_fields(cvr="12345678", year=2024)
"""

import os

import duckdb
from common.storage import StorageAccess

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

    All methods return DuckDB relations (duckdb.DuckDBPyRelation) for efficient
    lazy evaluation and chaining of operations.
    """

    def __init__(self, storage: StorageAccess | None = None, bucket: str | None = None):
        """
        Initialize the climate data loader.

        Args:
            storage: Optional StorageAccess instance to reuse from parent pipeline.
                     If None, creates a new StorageAccess().
            bucket: GCS bucket name. Defaults to GCS_BUCKET env var or 'landbruget-data'
        """
        self.bucket = (
            bucket
            or os.getenv("STORAGE_BUCKET")
            or os.getenv("R2_BUCKET")
            or os.getenv("GCS_BUCKET", "landbruget-data")
        )
        self.gcs = storage if storage is not None else StorageAccess()
        logger.info(f"ClimateDataLoader initialized with bucket: {self.bucket}")

    def _empty_relation(self) -> duckdb.DuckDBPyRelation:
        """Return an empty DuckDB relation."""
        return self.gcs.duckdb_conn.sql("SELECT 1 WHERE FALSE")

    def load_livestock(self, cvr: str, year: int | None = None) -> duckdb.DuckDBPyRelation | None:
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
            DuckDB relation with livestock data, or None if no data found

        Example:
            >>> loader = ClimateDataLoader()
            >>> livestock = loader.load_livestock(cvr="31373077", year=2023)
            >>> print(livestock.select('cvr_number, c_2001, c_2006, c_2016').df())
        """
        try:
            # Validate CVR format
            cvr_str = str(cvr).zfill(8)
            if len(cvr_str) != 8 or not cvr_str.isdigit():
                logger.error(f"Invalid CVR format: {cvr}. Must be 8 digits.")
                return None

            # Default to 2023 (latest available year)
            if year is None:
                year = 2023
                logger.info(f"No year specified, using latest: {year}")

            # Green Accounts path - NOTE: has space in folder name!
            # DYRERK = Animal records (Dyre Regnskab)
            pattern = f"{self.bucket}/silver/gr {year}/*/V_4061GR_*_DYRERK_*_pii_handled.parquet"
            files = self.gcs.list_files(pattern)

            if not files:
                logger.warning(f"No Green Accounts livestock data found in {pattern}")
                return None

            # Use latest file (sorted by timestamp in path)
            latest_file = sorted(files)[-1]
            logger.info(f"Loading livestock data from: {latest_file}")

            # Create table in DuckDB
            table_name = "gr_livestock_temp"
            self.gcs.create_table_from_storage(table_name, latest_file)

            # Query for specific CVR - use cvr_number column (not cvr)
            query = f"""
                SELECT *
                FROM {table_name}
                WHERE cvr_number = '{cvr_str}'
            """

            result_rel = self.gcs.duckdb_conn.sql(query)
            row_count = result_rel.count("*").fetchone()[0]

            # Cleanup
            self.gcs.duckdb_conn.execute(f"DROP TABLE IF EXISTS {table_name}")

            logger.info(f"Loaded {row_count} livestock records for CVR {cvr_str}, year {year}")
            return result_rel

        except Exception as e:
            logger.error(f"Error loading livestock data for CVR {cvr}: {e}")
            return None

    def load_fields(self, cvr: str, year: int) -> duckdb.DuckDBPyRelation | None:
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
            DuckDB relation with field data, or None if no data found

        Example:
            >>> loader = ClimateDataLoader()
            >>> fields = loader.load_fields(cvr="31373077", year=2024)
            >>> print(fields.select('bfe_nummer, afgroede, areal_ha').df())
        """
        try:
            # Validate CVR format
            cvr_str = str(cvr).zfill(8)
            if len(cvr_str) != 8 or not cvr_str.isdigit():
                logger.error(f"Invalid CVR format: {cvr}. Must be 8 digits.")
                return None

            # Find FVM data for specific year
            pattern = f"{self.bucket}/silver/fvm_marker_{year}/*/data.parquet"
            files = self.gcs.list_files(pattern)

            if not files:
                logger.warning(f"No field data found for year {year} in {pattern}")
                return None

            # Use latest file for the year
            latest_file = sorted(files)[-1]
            logger.info(f"Loading field data from: {latest_file}")

            # Create table in DuckDB
            table_name = "fvm_fields_temp"
            self.gcs.create_table_from_storage(table_name, latest_file)

            # Query for specific CVR - FVM data uses cvr_number column
            query = f"""
                SELECT *
                FROM {table_name}
                WHERE cvr_number = '{cvr_str}'
            """

            result_rel = self.gcs.duckdb_conn.sql(query)
            row_count = result_rel.count("*").fetchone()[0]

            # Cleanup
            self.gcs.duckdb_conn.execute(f"DROP TABLE IF EXISTS {table_name}")

            logger.info(f"Loaded {row_count} field records for CVR {cvr_str}, year {year}")
            return result_rel

        except Exception as e:
            logger.error(f"Error loading field data for CVR {cvr}, year {year}: {e}")
            return None

    def load_fertilizer(self, cvr: str, year: int) -> duckdb.DuckDBPyRelation | None:
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
            DuckDB relation with fertilizer data, or None if no data found

        Example:
            >>> loader = ClimateDataLoader()
            >>> fert = loader.load_fertilizer(cvr="31373077", year=2024)
            >>> print(fert.select('cvr_number, total_n_kvote, faktisk_areal_ha').df())
            >>> # Calculate N2O: total_n_kvote * 0.01 * (44/28) * 298 = kg CO2e
        """
        try:
            # Validate CVR format
            cvr_str = str(cvr).zfill(8)
            if len(cvr_str) != 8 or not cvr_str.isdigit():
                logger.error(f"Invalid CVR format: {cvr}. Must be 8 digits.")
                return None

            # GKEA fertilizer data - files named like GKEA2024_Markplan_med_Gødningsoplysninger.parquet
            # Located in: silver/fertiliser/{timestamp}/GKEA{year}_*.parquet
            pattern = f"{self.bucket}/silver/fertiliser/*/GKEA{year}_*.parquet"
            files = self.gcs.list_files(pattern)

            if not files:
                logger.warning(
                    f"No GKEA fertilizer data found for year {year} in pattern: {pattern}"
                )
                # Try without year filter
                pattern_fallback = f"{self.bucket}/silver/fertiliser/*/GKEA*.parquet"
                files = self.gcs.list_files(pattern_fallback)
                if files:
                    logger.info(f"Found {len(files)} GKEA files without year filter")

            if not files:
                logger.warning("No GKEA fertilizer data found at all")
                return None

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
            self.gcs.create_table_from_storage(table_name, target_file)

            # Get column names to check for year column
            columns_rel = self.gcs.duckdb_conn.sql(f"SELECT * FROM {table_name} LIMIT 0")
            columns = columns_rel.columns

            # Build query for specific CVR - GKEA uses cvr_number column
            query = f"""
                SELECT *
                FROM {table_name}
                WHERE cvr_number = '{cvr_str}'
            """

            if "year" in columns:
                query += f" AND year = {year}"
            elif "aar" in columns:
                query += f" AND aar = {year}"

            result_rel = self.gcs.duckdb_conn.sql(query)
            row_count = result_rel.count("*").fetchone()[0]

            # Cleanup
            self.gcs.duckdb_conn.execute(f"DROP TABLE IF EXISTS {table_name}")

            logger.info(f"Loaded {row_count} fertilizer records for CVR {cvr_str}, year {year}")
            return result_rel

        except Exception as e:
            logger.error(f"Error loading fertilizer data for CVR {cvr}, year {year}: {e}")
            return None

    def load_soil_types(self) -> duckdb.DuckDBPyRelation | None:
        """
        Load soil types data from silver layer.

        Returns soil polygons with soil_code, soil_description, and geometry.
        This is used to identify organic soils (Humusjord) for emissions calculations.

        Returns:
            DuckDB relation with soil type polygons and geometries, or None if not found
        """
        try:
            pattern = f"{self.bucket}/silver/soil_types/*/data.parquet"
            files = self.gcs.list_files(pattern)

            if not files:
                logger.warning("No soil types data found")
                return None

            # Use latest file
            latest_file = sorted(files)[-1]
            logger.info(f"Loading soil types from: {latest_file}")

            table_name = "soil_types_temp"
            self.gcs.create_table_from_storage(table_name, latest_file)

            # Select all columns
            query = f"""
                SELECT
                    soil_code,
                    soil_description,
                    geometry,
                    processed_at,
                    data_quality
                FROM {table_name}
            """
            result_rel = self.gcs.duckdb_conn.sql(query)
            row_count = result_rel.count("*").fetchone()[0]

            # Cleanup
            self.gcs.duckdb_conn.execute(f"DROP TABLE IF EXISTS {table_name}")

            logger.info(f"Loaded {row_count} soil type polygons")
            return result_rel

        except Exception as e:
            logger.error(f"Error loading soil types data: {e}")
            return None

    def load_lavbundsjord(self, cvr: str, year: int) -> duckdb.DuckDBPyRelation | None:
        """
        Load lavbundsjord (wetland/peat soil) data for fields belonging to a CVR.

        Uses the field_analysis_field_wetland_intersections gold layer which contains
        pre-computed spatial joins between fields and wetland data with carbon percentages.

        This data is used for organic soil emission calculations (formulas/marker/organogene_jorde.py).

        Args:
            cvr: Company CVR number
            year: Year for field data

        Returns:
            DuckDB relation with columns: field_id, wetland_id, toerv_pct, field_wetland_area_m2,
                                   field_area_m2, coverage_fraction, carbon_pct
            where carbon_pct is "6-12%" or ">12%" for use in emission formulas
            Returns None if no data found
        """
        try:
            # Validate CVR format
            cvr_padded = str(cvr).zfill(8)

            # Use pre-computed field-wetland intersections from gold layer
            pattern = f"{self.bucket}/gold/field_analysis_field_wetland_intersections_{year}/*/data.parquet"
            files = self.gcs.list_files(pattern)

            if not files:
                logger.warning(f"No field-wetland intersection data found for year {year}")
                return None

            # Use latest file
            latest_file = sorted(files)[-1]
            logger.info(f"Loading field-wetland intersections from: {latest_file}")

            table_name = "field_wetland_temp"
            self.gcs.create_table_from_storage(table_name, latest_file)

            # Query for this CVR with carbon percentage mapping
            # First check what columns are available
            columns_rel = self.gcs.duckdb_conn.sql(f"SELECT * FROM {table_name} LIMIT 0")
            columns = columns_rel.columns
            logger.debug(f"Available columns in field_wetland_intersections: {columns}")

            # Build query based on available columns
            # Note: Some versions may have different column names
            has_toerv = "toerv_pct" in columns or "toerv" in columns
            has_year = "year" in columns

            if not has_toerv:
                logger.warning(f"No toerv_pct column found in wetland data. Available: {columns}")
                return None

            toerv_col = "toerv_pct" if "toerv_pct" in columns else "toerv"
            cvr_col = "cvr_number" if "cvr_number" in columns else "cvr"

            # Query for this CVR
            where_clauses = [f"{cvr_col} = '{cvr_padded}'"]
            if has_year:
                where_clauses.append(f"year = {year}")
            where_clauses.append(f"{toerv_col} IS NOT NULL")

            query = f"""
                SELECT
                    field_id,
                    field_uuid,
                    wetland_id,
                    {toerv_col} as toerv_pct,
                    CASE
                        WHEN {toerv_col} = '6-12' THEN '6-12%'
                        WHEN {toerv_col} = '>12' THEN '>12%'
                        ELSE {toerv_col}
                    END as carbon_pct,
                    ST_Area(ST_Transform(field_wetland_geometry, 'EPSG:4326', 'EPSG:25832')) / 10000 as area_ha
                FROM {table_name}
                WHERE {" AND ".join(where_clauses)}
                ORDER BY field_id
            """
            result_rel = self.gcs.duckdb_conn.sql(query)
            row_count = result_rel.count("*").fetchone()[0]

            # Cleanup
            self.gcs.duckdb_conn.execute(f"DROP TABLE IF EXISTS {table_name}")

            logger.info(f"Loaded {row_count} lavbundsjord records for CVR {cvr_padded}")

            # Summary statistics using DuckDB aggregation
            if row_count > 0:
                # Get summary stats via DuckDB
                stats_query = f"""
                    WITH data AS ({query})
                    SELECT
                        SUM(area_ha) as total_area,
                        SUM(CASE WHEN carbon_pct = '6-12%' THEN area_ha ELSE 0 END) as area_6_12,
                        SUM(CASE WHEN carbon_pct = '>12%' THEN area_ha ELSE 0 END) as area_gt_12,
                        COUNT(CASE WHEN carbon_pct = '6-12%' THEN 1 END) as count_6_12,
                        COUNT(CASE WHEN carbon_pct = '>12%' THEN 1 END) as count_gt_12
                    FROM data
                """
                # Re-create table for stats query since we dropped it
                self.gcs.create_table_from_storage(table_name, latest_file)
                stats = self.gcs.duckdb_conn.sql(stats_query).fetchone()
                self.gcs.duckdb_conn.execute(f"DROP TABLE IF EXISTS {table_name}")

                total_wetland_area_ha = stats[0] or 0
                area_6_12 = stats[1] or 0
                area_gt_12 = stats[2] or 0
                count_6_12 = stats[3] or 0
                count_gt_12 = stats[4] or 0

                logger.info(f"   Total lavbundsjord area: {total_wetland_area_ha:.1f} ha")
                if count_6_12 > 0:
                    logger.info(f"   6-12% carbon: {count_6_12} intersections ({area_6_12:.1f} ha)")
                if count_gt_12 > 0:
                    logger.info(
                        f"   >12% carbon: {count_gt_12} intersections ({area_gt_12:.1f} ha)"
                    )

            return result_rel

        except Exception as e:
            logger.error(f"Error loading lavbundsjord data: {e}")
            return None

    def load_organic_certification(self, cvr: str, year: int) -> dict[str, bool | float]:
        """
        Check if a farm has organic certification for its fields or livestock production.

        This method checks the FVM organic areas dataset to determine if the farm
        has any organic certified fields. If a significant portion of the farm's
        area is organic (>50%), we assume the livestock production is also organic.

        Args:
            cvr: Company CVR number
            year: Year to check (2012-2024 available)

        Returns:
            Dictionary with organic status:
            {
                'has_organic_fields': bool,
                'organic_area_ha': float,
                'total_area_ha': float,
                'organic_percentage': float,
                'is_organic_livestock': bool  # True if >50% organic area
            }
        """
        try:
            # Validate CVR format
            cvr_padded = str(cvr).zfill(8)

            # First, get the farm's total field area from FVM
            fields_rel = self.load_fields(cvr, year)

            if fields_rel is None:
                logger.info(
                    f"No fields found for CVR {cvr_padded}, cannot determine organic status"
                )
                return {
                    "has_organic_fields": False,
                    "organic_area_ha": 0.0,
                    "total_area_ha": 0.0,
                    "organic_percentage": 0.0,
                    "is_organic_livestock": False,
                }

            # Get column names and calculate total field area
            columns = fields_rel.columns
            area_col = "areal_ha" if "areal_ha" in columns else "area_ha"

            # Calculate total area using DuckDB
            total_area_result = fields_rel.aggregate(f"SUM({area_col}) as total").fetchone()
            total_area_ha = total_area_result[0] if total_area_result[0] else 0.0

            # Get journal numbers from fields (needed to match organic data)
            journal_col = None
            if "journal_number" in columns:
                journal_col = "journal_number"
            elif "fs_journal_number" in columns:
                journal_col = "fs_journal_number"

            if not journal_col:
                logger.warning(f"No journal numbers found in field data for CVR {cvr_padded}")
                return {
                    "has_organic_fields": False,
                    "organic_area_ha": 0.0,
                    "total_area_ha": total_area_ha,
                    "organic_percentage": 0.0,
                    "is_organic_livestock": False,
                }

            # Get distinct journal numbers
            journal_result = (
                fields_rel.filter(f"{journal_col} IS NOT NULL")
                .distinct()
                .select(journal_col)
                .fetchall()
            )
            journal_numbers = [row[0] for row in journal_result if row[0]]

            if not journal_numbers:
                logger.warning(f"No journal numbers found in field data for CVR {cvr_padded}")
                return {
                    "has_organic_fields": False,
                    "organic_area_ha": 0.0,
                    "total_area_ha": total_area_ha,
                    "organic_percentage": 0.0,
                    "is_organic_livestock": False,
                }

            # Load organic areas for the year
            pattern = f"{self.bucket}/silver/fvm_organic_areas_{year}/*/data.parquet"
            files = self.gcs.list_files(pattern)

            if not files:
                logger.info(f"No organic areas data found for year {year}")
                return {
                    "has_organic_fields": False,
                    "organic_area_ha": 0.0,
                    "total_area_ha": total_area_ha,
                    "organic_percentage": 0.0,
                    "is_organic_livestock": False,
                }

            # Use latest file
            latest_file = sorted(files)[-1]
            logger.info(f"Loading organic areas from: {latest_file}")

            # Create table
            table_name = "organic_areas_temp"
            self.gcs.create_table_from_storage(table_name, latest_file)

            # Query for fields with organic certification
            # Check if conversion_status = 1 (fully converted to organic)
            journal_list = "', '".join(str(j) for j in journal_numbers)
            query = f"""
                SELECT
                    SUM(ST_Area(ST_Transform(geometry, 'EPSG:4326', 'EPSG:25832')) / 10000) as organic_area_ha
                FROM {table_name}
                WHERE fs_journal_number IN ('{journal_list}')
                    AND conversion_status = 1
                    AND (deregistration_date IS NULL OR deregistration_date > '{year}-12-31')
            """

            organic_result = self.gcs.duckdb_conn.sql(query).fetchone()

            # Cleanup
            self.gcs.duckdb_conn.execute(f"DROP TABLE IF EXISTS {table_name}")

            organic_area_ha = organic_result[0] if organic_result[0] else 0.0

            if organic_area_ha == 0.0:
                logger.info(f"No organic certification found for CVR {cvr_padded}")
                return {
                    "has_organic_fields": False,
                    "organic_area_ha": 0.0,
                    "total_area_ha": total_area_ha,
                    "organic_percentage": 0.0,
                    "is_organic_livestock": False,
                }

            # Calculate organic percentage
            organic_percentage = (
                (organic_area_ha / total_area_ha * 100) if total_area_ha > 0 else 0.0
            )

            # Assume organic livestock if >50% of farm area is organic
            is_organic_livestock = organic_percentage > 50.0

            logger.info(
                f"Organic certification for CVR {cvr_padded}: "
                f"{organic_area_ha:.1f} ha ({organic_percentage:.1f}%) organic, "
                f"livestock: {'organic' if is_organic_livestock else 'conventional'}"
            )

            return {
                "has_organic_fields": True,
                "organic_area_ha": organic_area_ha,
                "total_area_ha": total_area_ha,
                "organic_percentage": organic_percentage,
                "is_organic_livestock": is_organic_livestock,
            }

        except Exception as e:
            logger.error(f"Error checking organic certification for CVR {cvr}: {e}")
            return {
                "has_organic_fields": False,
                "organic_area_ha": 0.0,
                "total_area_ha": 0.0,
                "organic_percentage": 0.0,
                "is_organic_livestock": False,
            }

    def load_nles5_nitrogen(self, cvr: str, year: int) -> duckdb.DuckDBPyRelation | None:
        """
        Load NLES5 nitrogen estimation data for fields belonging to a CVR.

        This provides actual calculated nitrogen washout (nitrate leaching) values
        from the NLES5 model, which is more accurate than using typetal lookups.

        Args:
            cvr: Company CVR number
            year: Year for field data

        Returns:
            DuckDB relation with columns: field_id, nitrogen_washout_kg_ha, crop_type, area_ha,
                                   soil_description, percolation_mm, uncertainty_pct
            Returns None if no data found
        """
        try:
            # Validate CVR format
            cvr_padded = str(cvr).zfill(8)

            # Use NLES5 nitrogen estimation from gold layer
            pattern = (
                f"{self.bucket}/gold/nles5_nitrogen_estimation_nitrogen_estimates/*/data.parquet"
            )
            files = self.gcs.list_files(pattern)

            if not files:
                logger.warning("No NLES5 nitrogen data found")
                return None

            # Use latest file
            latest_file = sorted(files)[-1]
            logger.info(f"Loading NLES5 nitrogen data from: {latest_file}")

            table_name = "nles5_nitrogen_temp"
            self.gcs.create_table_from_storage(table_name, latest_file)

            # Query for this CVR
            query = f"""
                SELECT
                    field_id,
                    block_id,
                    field_uuid,
                    crop_type,
                    area_ha,
                    soil_description,
                    clay_content,
                    nitrogen_washout_kg_ha,
                    percolation_mm,
                    uncertainty_pct,
                    data_quality_score
                FROM {table_name}
                WHERE cvr_number = '{cvr_padded}'
                    AND year = {year}
                ORDER BY field_id
            """
            result_rel = self.gcs.duckdb_conn.sql(query)
            row_count = result_rel.count("*").fetchone()[0]

            # Cleanup
            self.gcs.duckdb_conn.execute(f"DROP TABLE IF EXISTS {table_name}")

            logger.info(f"Loaded {row_count} NLES5 nitrogen records for CVR {cvr_padded}")

            if row_count > 0:
                # Calculate average using DuckDB - need to recreate table
                self.gcs.create_table_from_storage(table_name, latest_file)
                avg_query = f"""
                    SELECT AVG(nitrogen_washout_kg_ha) as avg_n
                    FROM {table_name}
                    WHERE cvr_number = '{cvr_padded}' AND year = {year}
                """
                avg_result = self.gcs.duckdb_conn.sql(avg_query).fetchone()
                self.gcs.duckdb_conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                avg_n_washout = avg_result[0] if avg_result[0] else 0.0
                logger.info(f"   Average nitrogen washout: {avg_n_washout:.1f} kg N/ha")

            return result_rel

        except Exception as e:
            logger.error(f"Error loading NLES5 nitrogen data: {e}")
            return None

    def load_climate_data(self, cvr: str, year: int) -> duckdb.DuckDBPyRelation | None:
        """
        Load climate/weather data from DMI for a specific location and year.

        This is optional and may not be available for all locations.

        Args:
            cvr: Company CVR number (8 digits) - used to find location
            year: Year (YYYY)

        Returns:
            DuckDB relation with climate data, or None if not available
        """
        try:
            # DMI climate data might be structured by location/year
            pattern = f"{self.bucket}/silver/dmi/*/data.parquet"
            files = self.gcs.list_files(pattern)

            if not files:
                logger.info("No DMI climate data found (optional)")
                return None

            # Use latest file
            latest_file = sorted(files)[-1]
            logger.info(f"Loading climate data from: {latest_file}")

            # Create table and query (implementation depends on DMI data structure)
            table_name = "dmi_climate_temp"
            self.gcs.create_table_from_storage(table_name, latest_file)

            query = f"SELECT * FROM {table_name} WHERE year = {year}"
            result_rel = self.gcs.duckdb_conn.sql(query)
            row_count = result_rel.count("*").fetchone()[0]

            # Cleanup
            self.gcs.duckdb_conn.execute(f"DROP TABLE IF EXISTS {table_name}")

            logger.info(f"Loaded {row_count} climate records for year {year}")
            return result_rel

        except Exception as e:
            logger.warning(f"Climate data not available: {e}")
            return None

    def load_cattle_breeds(self, cvr: str, year: int) -> duckdb.DuckDBPyRelation | None:
        """
        Load cattle breed information from CHR animal movements data.

        This joins:
        1. CVR -> herd_number (from chr_herd_owners)
        2. herd_number -> breed counts (from chr_dyr_animal_movements)

        Args:
            cvr: Company CVR number (8 digits)
            year: Year to query (for filtering active animals)

        Returns:
            DuckDB relation with columns: breed, animal_count
            None if no CHR data available

        Example:
            >>> loader = ClimateDataLoader()
            >>> breeds = loader.load_cattle_breeds(cvr="12345678", year=2023)
            >>> print(breeds.df())
               breed  animal_count
            0  Holstein  100
            1  Jersey    25
        """
        try:
            cvr_str = str(cvr).zfill(8)

            # Load herd owners mapping (CVR -> herd_number)
            herd_owners_pattern = f"{self.bucket}/silver/chr_herd_owners*.parquet"
            herd_owners_files = self.gcs.list_files(herd_owners_pattern)

            if not herd_owners_files:
                logger.debug("No CHR herd_owners data found (breed detection unavailable)")
                return None

            # Load animal movements (herd_number -> breed)
            animal_movements_pattern = f"{self.bucket}/silver/chr_dyr_animal_movements*.parquet"
            animal_movements_files = self.gcs.list_files(animal_movements_pattern)

            if not animal_movements_files:
                logger.debug("No CHR animal movements data found (breed detection unavailable)")
                return None

            # Create tables
            self.gcs.create_table_from_storage(
                "chr_herd_owners_temp", sorted(herd_owners_files)[-1]
            )
            self.gcs.create_table_from_storage(
                "chr_animal_movements_temp", sorted(animal_movements_files)[-1]
            )

            # Query to join CVR -> herds -> breeds
            query = f"""
            SELECT
                am.breed,
                COUNT(DISTINCT am.ckr_number) as animal_count
            FROM chr_herd_owners_temp ho
            INNER JOIN chr_animal_movements_temp am
                ON ho.herd_number = am.reporting_herd_number
            WHERE ho.owner_cvr = '{cvr_str}'
                AND am.breed IS NOT NULL
                AND am.breed != ''
                AND YEAR(am.period_til) >= {year}  -- Active in year
            GROUP BY am.breed
            ORDER BY animal_count DESC
            """

            result_rel = self.gcs.duckdb_conn.sql(query)
            row_count = result_rel.count("*").fetchone()[0]

            # Cleanup
            self.gcs.duckdb_conn.execute("DROP TABLE IF EXISTS chr_herd_owners_temp")
            self.gcs.duckdb_conn.execute("DROP TABLE IF EXISTS chr_animal_movements_temp")

            if row_count > 0:
                logger.info(f"Loaded breed data for CVR {cvr}: {row_count} breeds found")
            else:
                logger.debug(f"No breed data found for CVR {cvr}")

            return result_rel if row_count > 0 else None

        except Exception as e:
            logger.debug(f"Breed data not available for CVR {cvr}: {e}")
            return None

    def list_available_years(self, dataset: str) -> list[int]:
        """
        List available years for a specific dataset.

        Args:
            dataset: Dataset name ('fvm_marker', 'chr', etc.)

        Returns:
            List of available years (sorted)
        """
        try:
            if dataset == "fvm_marker":
                pattern = f"{self.bucket}/silver/fvm_marker_*/*/*.parquet"
                files = self.gcs.list_files(pattern)
                years = set()

                import re

                for file_path in files:
                    match = re.search(r"fvm_marker_(\d{4})/", file_path)
                    if match:
                        years.add(int(match.group(1)))

                return sorted(years)

            if dataset == "chr":
                # CHR data is timestamped, not year-specific in path
                # Return years based on data content if possible
                logger.info("CHR data is timestamped. Use load_livestock() with year parameter.")
                return []

            logger.warning(f"Unknown dataset: {dataset}")
            return []

        except Exception as e:
            logger.error(f"Error listing available years for {dataset}: {e}")
            return []

    def get_latest_data_timestamp(self, dataset: str) -> str | None:
        """
        Get the timestamp of the latest data file for a dataset.

        Args:
            dataset: Dataset name ('chr', 'fvm_marker_YYYY', etc.)

        Returns:
            Timestamp string (YYYYMMDD_HHMMSS) or None if not found
        """
        try:
            if dataset.startswith("fvm_marker_"):
                pattern = f"{self.bucket}/silver/{dataset}/*/data.parquet"
            else:
                pattern = f"{self.bucket}/silver/{dataset}/*/*.parquet"

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
                # StorageAccess handles its own cleanup
                pass
        except Exception:
            pass
