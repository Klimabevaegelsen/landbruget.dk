"""
Data Loading for H3 PFAS Exposure Analysis

=== WHAT THIS FILE DOES (FOR NON-TECHNICAL READERS) ===

This file is responsible for loading and preparing all the different types of data
needed for the PFAS analysis. Think of it as the "librarian" of the system - it
knows where to find all the different datasets and how to prepare them for analysis.

The main types of data it handles:
1. BMD Pesticide Data - Official registry of all pesticides and their ingredients
2. Field Data - Geographic boundaries and details of farm fields
3. Pesticide Application Data - Records of actual pesticide usage on farms
4. Municipality Data - Administrative boundaries for regional analysis

=== KEY CONCEPTS ===

- GCS (Google Cloud Storage): Where all our data files are stored in the cloud
- Parquet Files: Efficient file format for storing large datasets
- Data Layers: Bronze (raw), Silver (processed), Gold (analysis-ready)
- Spatial Data: Geographic information with coordinates and boundaries
- Temporal Data: Information that changes over time (different years)

=== TECHNICAL DETAILS ===

Data loading utilities for H3 PFAS exposure analysis.
"""

import gc

import duckdb
from loguru import logger

from ..config import H3SpatialConfig


class H3DataLoader:
    """
    Handles loading and preparing data from various sources for PFAS analysis.

    This class is like a specialized librarian that:
    - Knows where to find different types of data in cloud storage
    - Can download and prepare data for analysis
    - Handles different file formats and data structures
    - Manages memory efficiently when working with large datasets
    - Ensures data quality and consistency
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection, config: H3SpatialConfig, gcs_access=None):
        """
        Initialize the data loader.

        Args:
            conn: Database connection for loading data into tables
            config: Configuration settings (where to find data, processing parameters)
            gcs_access: Tool for accessing Google Cloud Storage
        """
        self.conn = conn
        self.config = config
        self.gcs_access = gcs_access
        self.log = logger.bind(component="H3DataLoader")

    def _load_table_from_gcs(self, gcs_path: str, table_name: str):
        """
        Load a data file from Google Cloud Storage into a database table.

        This is like downloading a file from the cloud and opening it in a spreadsheet,
        but optimized for large datasets. It:
        1. Downloads the file temporarily
        2. Loads it into the database
        3. Cleans up the temporary file
        4. Manages memory usage

        Args:
            gcs_path: Location of the file in Google Cloud Storage
            table_name: Name to give the table in our database
        """
        try:
            # Use the optimized download approach with our DuckDB connection
            with self.gcs_access._temp_download(gcs_path) as temp_file:
                self.conn.execute(f"""
                    CREATE OR REPLACE TABLE {table_name} AS
                    SELECT * FROM read_parquet('{temp_file}')
                """)
            self.log.debug(f"✅ Loaded {table_name} from {gcs_path}")

            # Force garbage collection after loading large files
            # This helps prevent memory issues when working with big datasets
            gc.collect()

        except Exception as e:
            self.log.error(f"❌ Failed to load {table_name} from {gcs_path}: {e}")
            raise

    def _get_latest_silver_path(self, dataset: str) -> str:
        """
        Find the most recent processed version of a dataset.

        Data in our system goes through different processing stages:
        - Bronze: Raw data as received
        - Silver: Cleaned and standardized data
        - Gold: Analysis-ready data

        This function finds the latest Silver version of a dataset, which is
        the best balance of being processed but still general-purpose.

        Args:
            dataset: Name of the dataset to find (e.g., "bmd", "fvm_marker")

        Returns:
            str: Path to the latest version of the dataset
        """
        # Try new standardized format first
        pattern = f"gs://{self.config.bucket}/silver/{dataset}/*/data.parquet"
        files = self.gcs_access.list_files(pattern)

        if not files:
            # Fallback to legacy format for backward compatibility
            self.log.warning(f"No new format files found for {dataset}, trying legacy format")
            legacy_pattern = f"gs://{self.config.bucket}/silver/{dataset}/*.parquet"
            files = self.gcs_access.list_files(legacy_pattern)

            if files:
                self.log.info(f"Found legacy format files for {dataset}: {len(files)} files")
                return sorted(files)[-1]  # Latest by filename

        # Special handling for BMD data with different file naming patterns
        if not files and dataset == "bmd":
            self.log.warning(
                f"No standard format files found for {dataset}, trying BMD-specific patterns"
            )
            # Try BMD-specific patterns
            bmd_patterns = [
                f"gs://{self.config.bucket}/silver/{dataset}/*/pesticide_products.parquet",
                f"gs://{self.config.bucket}/silver/{dataset}/*/bmd_data_*.parquet",
            ]

            for pattern in bmd_patterns:
                files = self.gcs_access.list_files(pattern)
                if files:
                    self.log.info(f"Found BMD files with pattern {pattern}: {len(files)} files")
                    return sorted(files)[-1]  # Latest by timestamp

        if not files:
            raise FileNotFoundError(f"No silver data found for {dataset}")

        return sorted(files)[-1]  # Latest by timestamp

    def _get_latest_gold_path(self, dataset: str, year: int) -> str:
        """
        Find the most recent analysis-ready version of a dataset for a specific year.

        Gold layer data is the final, analysis-ready version that has been processed
        through all our data pipelines. This is typically year-specific data like
        pesticide application records or field usage data.

        Args:
            dataset: Name of the dataset (e.g., "pesticide_disaggregation")
            year: Year of data to find (e.g., 2022)

        Returns:
            str: Path to the latest version of the dataset for that year
        """
        # Try new standardized format first
        pattern = f"gs://{self.config.bucket}/gold/{dataset}/{year}/*/data.parquet"
        files = self.gcs_access.list_files(pattern)

        if not files:
            # Try the actual unified pipeline format: dataset_year/timestamp/dataset_year.parquet
            self.log.warning(
                f"No new format files found for {dataset} {year}, trying unified pipeline format"
            )
            unified_pattern = (
                f"gs://{self.config.bucket}/gold/{dataset}_{year}/*/{dataset}_{year}.parquet"
            )
            files = self.gcs_access.list_files(unified_pattern)

            if files:
                self.log.info(
                    f"Found unified pipeline format files for {dataset} {year}: {len(files)} files"
                )
                return sorted(files)[-1]  # Latest by timestamp

        if not files:
            # Fallback to legacy format for backward compatibility
            self.log.warning(
                f"No unified format files found for {dataset} {year}, trying legacy format"
            )
            legacy_pattern = f"gs://{self.config.bucket}/gold/{dataset}/*{year}*.parquet"
            files = self.gcs_access.list_files(legacy_pattern)

            if files:
                self.log.info(f"Found legacy format files for {dataset} {year}: {len(files)} files")
                return sorted(files)[-1]  # Latest by filename

        if not files:
            self.log.warning(f"No gold data found for {dataset} {year}")
            return None

        return sorted(files)[-1]  # Latest by timestamp

    def _check_gcs_path_exists(self, path: str) -> bool:
        """
        Check if a file or directory exists in Google Cloud Storage.

        This is like checking if a file exists on your computer, but for cloud storage.

        Args:
            path: Path to check in cloud storage

        Returns:
            bool: True if the path exists and has data, False otherwise
        """
        try:
            return self.gcs_access.file_exists(path)
        except Exception as e:
            self.log.debug(f"Error checking GCS path {path}: {e}")
            return False

    def _check_year_data_availability(self, year: int) -> bool:
        """
        Check if we have all the required data for a specific year.

        For PFAS analysis, we need:
        - Pesticide application data for year Y (what pesticides were used)
        - Field boundary data for year Y+1 (where the fields are located)

        The Y+1 pattern exists because field data is typically reported in the year
        after the growing season.

        Args:
            year: Year to check data availability for

        Returns:
            bool: True if we have all required data for this year
        """
        # Check pesticide disaggregation data for year Y
        pesticide_path = f"gs://{self.config.bucket}/gold/pesticide_disaggregation_{year}/"
        pesticide_available = self._check_gcs_path_exists(pesticide_path)

        # Check FVM marker data for year Y+1 (Y+1 pattern)
        field_year = year + 1
        field_path = f"gs://{self.config.bucket}/silver/fvm_marker_{field_year}/"
        field_available = self._check_gcs_path_exists(field_path)

        self.log.info(f"📊 Year {year} data availability (Y+1 pattern):")
        self.log.info(
            f"   Pesticide disaggregation ({year}): {'✅' if pesticide_available else '❌'}"
        )
        self.log.info(f"   FVM marker ({field_year}): {'✅' if field_available else '❌'}")

        return pesticide_available and field_available

    def get_available_years(self) -> list[int]:
        """
        Get a list of all years for which we have complete data.

        This scans our data storage to find which years have both pesticide
        application data and field boundary data available. Only years with
        complete data can be analyzed.

        Returns:
            list[int]: List of years that can be analyzed (e.g., [2020, 2021, 2022, 2023])
        """
        self.log.info("🔍 Checking data availability for all years")

        available_years = []
        for year in self.config.available_years:
            if self._check_year_data_availability(year):
                available_years.append(year)

        if available_years:
            self.log.info(f"✅ Found data for {len(available_years)} years: {available_years}")
        else:
            self.log.warning("❌ No years with complete data available")

        return available_years

    def load_bmd_data_from_gcs(self) -> str:
        """
        Load the BMD pesticide registration database.

        BMD (Bekæmpelsesmidler Database) is the official Danish registry of all
        approved pesticides. This database contains:
        - Product names and registration numbers
        - Active ingredients and their concentrations
        - Environmental impact ratings
        - PFAS content indicators (our key interest)

        This data is cached because it doesn't change often and is expensive to process.

        Returns:
            str: Name of the database table containing BMD data
        """
        self.log.info("🧪 Loading BMD pesticide data from GCS")

        # Get the latest BMD data from silver layer
        bmd_path = self._get_latest_silver_path("bmd")
        if not bmd_path:
            raise Exception("BMD data not found in silver layer")

        self.log.info(f"📄 Loading BMD data from: {bmd_path}")

        # Load BMD data directly from GCS
        temp_bmd_table = "temp_bmd_raw"
        self._load_table_from_gcs(bmd_path, temp_bmd_table)

        # Process BMD data - use existing PFAS, diquat, and glyphosate detection from BMD pipeline
        # This creates a clean lookup table with standardized column names
        self.conn.execute("""
            CREATE OR REPLACE TABLE bmd_pfas_lookup AS
            SELECT DISTINCT
                registrerings_nr as registration_number,
                produktnavn as pesticide_name,
                aktivstofnavn_e as active_ingredients,
                COALESCE(contains_pfas, false) as contains_pfas_compounds,
                COALESCE(contains_diquat, false) as contains_diquat,
                COALESCE(contains_glyphosate, false) as contains_glyphosate,
                koncentration_er as active_ingredient_content_pct,
                formulering as formulation_type,
                produktstatus as approval_status,
                godkendelsesdato as approval_date,
                udløbsdato as expiry_date,
                -- Include additional BMD-specific columns for environmental impact
                samlet_belastning as total_load_per_unit,
                belastning_miljøeffekt as environmental_effect_per_unit,
                belastning_miljøadfærd as environmental_behavior_per_unit,
                belastning_sundhed as health_effect_per_unit,
                -- Convert concentration to numeric for calculations
                TRY_CAST(REPLACE(REPLACE(koncentration_er, ',', '.'), ' ', '') AS DOUBLE) as concentration_numeric,
                enhed_er
            FROM temp_bmd_raw
            WHERE registrerings_nr IS NOT NULL
            AND produktnavn IS NOT NULL
        """)

        # Get statistics about what we loaded
        stats = self.conn.execute("""
            SELECT 
                COUNT(*) as total_products,
                COUNT(CASE WHEN contains_pfas_compounds = true THEN 1 END) as pfas_products,
                COUNT(CASE WHEN contains_diquat = true THEN 1 END) as diquat_products,
                COUNT(CASE WHEN contains_glyphosate = true THEN 1 END) as glyphosate_products
            FROM bmd_pfas_lookup
        """).fetchone()

        total, pfas, diquat, glyphosate = stats
        self.log.info(f"✅ BMD data processed: {total:,} products total")
        self.log.info(f"   🧪 PFAS-containing products: {pfas:,}")
        self.log.info(f"   🧪 Diquat-containing products: {diquat:,}")
        self.log.info(f"   🧪 Glyphosate-containing products: {glyphosate:,}")

        # Clean up temporary table
        self.conn.execute(f"DROP TABLE IF EXISTS {temp_bmd_table}")

        return "bmd_pfas_lookup"

    def load_and_prepare_fields_from_gcs(self, field_year: int, pesticide_year: int) -> str:
        """
        Load and prepare farm field boundary data.

        This loads the FVM (Field and Vegetation Map) data, which contains:
        - Geographic boundaries of all agricultural fields in Denmark
        - Field identification numbers
        - Crop types and areas
        - Company registration numbers (CVR)

        The data is filtered to only include fields that:
        1. Have valid geometric boundaries
        2. Are associated with actual pesticide applications
        3. Have complete identification information

        Args:
            field_year: Year of field data to load (typically pesticide_year + 1)
            pesticide_year: Year of pesticide data (for filtering relevant fields)

        Returns:
            str: Name of the database table containing prepared field data
        """
        self.log.info(
            f"📍 Loading and preparing field data for year {field_year} (pesticide year {pesticide_year})"
        )

        # Get the latest field data from silver layer
        field_dataset = f"fvm_marker_{field_year}"
        field_path = self._get_latest_silver_path(field_dataset)

        if not field_path:
            raise Exception(f"Field data not found for year {field_year}")

        self.log.info(f"📄 Loading field data from: {field_path}")

        # Load raw field data
        temp_field_table = f"temp_fvm_{field_year}"
        self._load_table_from_gcs(field_path, temp_field_table)

        # First, get the pesticide application data to know which fields we need
        # This is important for performance - we only process fields that actually have pesticide data
        pesticide_path = self._get_latest_gold_path("pesticide_disaggregation", pesticide_year)
        if not pesticide_path:
            raise Exception(f"Pesticide data not found for year {pesticide_year}")

        temp_pesticide_table = f"temp_pesticide_lookup_{pesticide_year}"
        self._load_table_from_gcs(pesticide_path, temp_pesticide_table)

        # Create a lookup table of fields that have pesticide applications
        # This helps us focus only on relevant fields
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE pesticide_field_lookup AS
            SELECT DISTINCT
                CompanyRegistrationNumber as cvr,
                REGEXP_EXTRACT(MatchedFieldID, 'marker_(.+)', 1) as field_id,
                REGEXP_EXTRACT(MatchedBlockID, 'block_(.+)', 1) as block_id
            FROM {temp_pesticide_table}
            WHERE MatchedFieldID IS NOT NULL
            AND MatchedBlockID IS NOT NULL
            AND CompanyRegistrationNumber IS NOT NULL
        """)

        lookup_count = self.conn.execute("SELECT COUNT(*) FROM pesticide_field_lookup").fetchone()[
            0
        ]
        self.log.info(f"📊 Found {lookup_count:,} field-pesticide combinations")

        # Now prepare the field data, filtering to only relevant fields
        prepared_table = f"prepared_fields_{field_year}"
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE {prepared_table} AS
            SELECT
                f.field_id,
                f.block_id,
                f.cvr_number,
                CAST(f.area_ha AS DOUBLE) as area_ha,
                f.crop_code,
                f.crop_name,
                f.geometry_wkt,
                -- Add field identification for easier joining
                CONCAT(f.cvr_number, '_', f.block_id, '_', f.field_id) as field_key
            FROM {temp_field_table} f
            INNER JOIN pesticide_field_lookup p ON (
                f.cvr_number = p.cvr
                AND f.field_id = p.field_id
                AND f.block_id = p.block_id
            )
            WHERE f.geometry_wkt IS NOT NULL
            AND ST_IsValid(ST_GeomFromText(f.geometry_wkt))
            AND CAST(f.area_ha AS DOUBLE) > 0
            AND f.cvr_number IS NOT NULL
            AND f.block_id IS NOT NULL
            AND f.field_id IS NOT NULL
        """)

        # Get statistics about the prepared data
        stats = self.conn.execute(f"""
            SELECT 
                COUNT(*) as total_fields,
                COUNT(DISTINCT cvr_number) as unique_companies,
                COUNT(DISTINCT crop_code) as unique_crops,
                SUM(area_ha) as total_area_ha,
                AVG(area_ha) as avg_field_size_ha
            FROM {prepared_table}
        """).fetchone()

        total_fields, companies, crops, total_area, avg_size = stats
        self.log.info(f"✅ Field data prepared: {total_fields:,} fields")
        self.log.info(f"   🏢 Companies: {companies:,}")
        self.log.info(f"   🌾 Crop types: {crops:,}")
        self.log.info(f"   📐 Total area: {total_area:,.0f} hectares")
        self.log.info(f"   📏 Average field size: {avg_size:.1f} hectares")

        # Clean up temporary tables
        self.conn.execute(f"DROP TABLE IF EXISTS {temp_field_table}")
        self.conn.execute(f"DROP TABLE IF EXISTS {temp_pesticide_table}")
        self.conn.execute("DROP TABLE IF EXISTS pesticide_field_lookup")

        return prepared_table

    def load_pesticide_disaggregation_from_gcs(self, year: int) -> str:
        """
        Load pesticide application records for a specific year.

        This loads the "pesticide disaggregation" data, which contains detailed
        records of actual pesticide applications on Danish farms:
        - Which pesticide was used (product name and registration number)
        - How much was applied (dosage and units)
        - Where it was applied (field and company identification)
        - When it was applied (year and sometimes season)

        This is the core data that tells us about actual PFAS exposure.

        Args:
            year: Year of pesticide application data to load

        Returns:
            str: Name of the database table containing pesticide application data
        """
        self.log.info(f"🧪 Loading pesticide disaggregation data for year {year}")

        # Get the latest pesticide data from gold layer
        pesticide_path = self._get_latest_gold_path("pesticide_disaggregation", year)
        if not pesticide_path:
            raise Exception(f"Pesticide disaggregation data not found for year {year}")

        self.log.info(f"📄 Loading pesticide data from: {pesticide_path}")

        # Load raw pesticide data
        temp_pesticide_table = "temp_pesticides_raw"
        self._load_table_from_gcs(pesticide_path, temp_pesticide_table)

        # Process and clean the pesticide data
        processed_table = f"pesticides_{year}"
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE {processed_table} AS
            SELECT
                DisaggregatedID,
                MatchedFieldID,
                MatchedBlockID,
                CompanyRegistrationNumber as cvr,
                PesticideName,
                PesticideRegistrationNumber,
                DosageQuantity,
                DosageUnit,
                AllocatedArea,
                AllocationMethod,
                MatchConfidence,
                -- Extract clean field and block IDs for easier joining
                REGEXP_EXTRACT(MatchedFieldID, 'marker_(.+)', 1) as extracted_field_id,
                REGEXP_EXTRACT(MatchedBlockID, 'block_(.+)', 1) as extracted_block_id,
                -- Create composite key for joining with field data
                CONCAT(CompanyRegistrationNumber, '_', 
                       REGEXP_EXTRACT(MatchedBlockID, 'block_(.+)', 1), '_',
                       REGEXP_EXTRACT(MatchedFieldID, 'marker_(.+)', 1)) as field_key
            FROM {temp_pesticide_table}
            WHERE MatchedFieldID IS NOT NULL
            AND MatchedBlockID IS NOT NULL
            AND CompanyRegistrationNumber IS NOT NULL
            AND PesticideRegistrationNumber IS NOT NULL
            AND DosageQuantity > 0
        """)

        # Get statistics about the loaded data
        stats = self.conn.execute(f"""
            SELECT 
                COUNT(*) as total_applications,
                COUNT(DISTINCT PesticideRegistrationNumber) as unique_pesticides,
                COUNT(DISTINCT cvr) as unique_companies,
                COUNT(DISTINCT field_key) as unique_fields,
                SUM(DosageQuantity) as total_dosage
            FROM {processed_table}
        """).fetchone()

        applications, pesticides, companies, fields, total_dosage = stats
        self.log.info(f"✅ Pesticide data loaded: {applications:,} applications")
        self.log.info(f"   🧪 Unique pesticides: {pesticides:,}")
        self.log.info(f"   🏢 Companies: {companies:,}")
        self.log.info(f"   📍 Fields: {fields:,}")
        self.log.info(f"   📊 Total dosage: {total_dosage:,.0f} units")

        # Clean up temporary table
        self.conn.execute(f"DROP TABLE IF EXISTS {temp_pesticide_table}")

        return processed_table

    def join_pesticide_with_bmd_pfas(self, pesticide_table: str, bmd_table: str, year: int) -> str:
        """
        Join pesticide application data with PFAS detection information.

        This is where the magic happens - we combine:
        1. Actual pesticide usage records (what was applied and where)
        2. Official pesticide registry data (which products contain PFAS)

        The result tells us exactly where PFAS-containing pesticides were applied
        and in what quantities.

        Args:
            pesticide_table: Table with pesticide application records
            bmd_table: Table with BMD registry data (including PFAS indicators)
            year: Year being processed (for table naming)

        Returns:
            str: Name of the database table containing joined data with PFAS indicators
        """
        self.log.info(f"🔗 Joining pesticide applications with PFAS detection data for year {year}")

        result_table = f"pesticide_pfas_{year}"

        # Perform the join and calculate PFAS exposure amounts
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE {result_table} AS
            SELECT
                p.*,
                b.active_ingredients,
                b.total_load_per_unit,
                b.environmental_effect_per_unit,
                b.environmental_behavior_per_unit,
                b.health_effect_per_unit,
                
                -- PFAS detection flags
                COALESCE(b.contains_pfas_compounds, false) as contains_pfas,
                COALESCE(b.contains_diquat, false) as contains_diquat,
                COALESCE(b.contains_glyphosate, false) as contains_glyphosate,

                -- Calculate actual PFAS-containing active ingredient amounts
                -- This converts dosage quantities to grams of PFAS-containing active ingredients
                CASE
                    WHEN b.contains_pfas_compounds = true AND b.concentration_numeric IS NOT NULL THEN
                        CASE
                            -- Liquid pesticides (unit 4) with concentration in g/l
                            WHEN p.DosageUnit = 4 AND b.enhed_er LIKE '%g/l%' THEN
                                p.DosageQuantity * b.concentration_numeric / 1000.0
                            -- Solid pesticides (unit 2) with concentration in g/kg  
                            WHEN p.DosageUnit = 2 AND b.enhed_er LIKE '%g/kg%' THEN
                                p.DosageQuantity * b.concentration_numeric / 1000.0
                            ELSE 0
                        END
                    ELSE 0
                END as pfas_containing_active_ingredient_grams,

                -- Calculate diquat-containing active ingredient amounts
                CASE
                    WHEN b.contains_diquat = true AND b.concentration_numeric IS NOT NULL THEN
                        CASE
                            WHEN p.DosageUnit = 4 AND b.enhed_er LIKE '%g/l%' THEN
                                p.DosageQuantity * b.concentration_numeric / 1000.0
                            WHEN p.DosageUnit = 2 AND b.enhed_er LIKE '%g/kg%' THEN
                                p.DosageQuantity * b.concentration_numeric / 1000.0
                            ELSE 0
                        END
                    ELSE 0
                END as diquat_containing_active_ingredient_grams,

                -- Calculate glyphosate-containing active ingredient amounts
                CASE
                    WHEN b.contains_glyphosate = true AND b.concentration_numeric IS NOT NULL THEN
                        CASE
                            WHEN p.DosageUnit = 4 AND b.enhed_er LIKE '%g/l%' THEN
                                p.DosageQuantity * b.concentration_numeric / 1000.0
                            WHEN p.DosageUnit = 2 AND b.enhed_er LIKE '%g/kg%' THEN
                                p.DosageQuantity * b.concentration_numeric / 1000.0
                            ELSE 0
                        END
                    ELSE 0
                END as glyphosate_containing_active_ingredient_grams,

                -- Calculate environmental load (pesticide impact metrics)
                CASE
                    WHEN b.total_load_per_unit IS NOT NULL THEN
                        p.DosageQuantity * b.total_load_per_unit
                    ELSE 0
                END as pesticide_belastning_applied,

                -- Calculate PFAS-specific environmental load
                CASE
                    WHEN b.contains_pfas_compounds = true AND b.total_load_per_unit IS NOT NULL THEN
                        p.DosageQuantity * b.total_load_per_unit
                    ELSE 0
                END as pfas_containing_pesticide_belastning_applied,

                -- Calculate diquat-specific environmental load
                CASE
                    WHEN b.contains_diquat = true AND b.total_load_per_unit IS NOT NULL THEN
                        p.DosageQuantity * b.total_load_per_unit
                    ELSE 0
                END as diquat_containing_pesticide_belastning_applied,

                -- Calculate glyphosate-specific environmental load
                CASE
                    WHEN b.contains_glyphosate = true AND b.total_load_per_unit IS NOT NULL THEN
                        p.DosageQuantity * b.total_load_per_unit
                    ELSE 0
                END as glyphosate_containing_pesticide_belastning_applied
                
            FROM {pesticide_table} p
            LEFT JOIN {bmd_table} b ON p.PesticideRegistrationNumber = b.registration_number
        """)

        # Get statistics about the joined data
        stats = self.conn.execute(f"""
            SELECT 
                COUNT(*) as total_applications,
                COUNT(CASE WHEN contains_pfas = true THEN 1 END) as pfas_applications,
                COUNT(CASE WHEN contains_diquat = true THEN 1 END) as diquat_applications,
                COUNT(CASE WHEN contains_glyphosate = true THEN 1 END) as glyphosate_applications,
                SUM(pfas_containing_active_ingredient_grams) as total_pfas_grams,
                SUM(diquat_containing_active_ingredient_grams) as total_diquat_grams,
                SUM(glyphosate_containing_active_ingredient_grams) as total_glyphosate_grams
            FROM {result_table}
        """).fetchone()

        (
            total,
            pfas_apps,
            diquat_apps,
            glyphosate_apps,
            pfas_grams,
            diquat_grams,
            glyphosate_grams,
        ) = stats

        self.log.info(f"✅ Pesticide-PFAS join completed: {total:,} total applications")
        self.log.info(f"   🧪 PFAS applications: {pfas_apps:,} ({pfas_apps / total * 100:.1f}%)")
        self.log.info(
            f"   🧪 Diquat applications: {diquat_apps:,} ({diquat_apps / total * 100:.1f}%)"
        )
        self.log.info(
            f"   🧪 Glyphosate applications: {glyphosate_apps:,} ({glyphosate_apps / total * 100:.1f}%)"
        )
        self.log.info(f"   ⚗️ Total PFAS active ingredients: {pfas_grams:.1f} grams")
        self.log.info(f"   ⚗️ Total diquat active ingredients: {diquat_grams:.1f} grams")
        self.log.info(f"   ⚗️ Total glyphosate active ingredients: {glyphosate_grams:.1f} grams")

        return result_table
