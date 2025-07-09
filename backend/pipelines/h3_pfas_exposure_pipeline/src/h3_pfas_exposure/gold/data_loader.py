"""
Data loading utilities for H3 PFAS exposure analysis.
"""

import gc

import duckdb
from loguru import logger

from ..config import H3SpatialConfig


class H3DataLoader:
    """Handles data loading from GCS and local sources."""

    def __init__(self, conn: duckdb.DuckDBPyConnection, config: H3SpatialConfig, gcs_access=None):
        self.conn = conn
        self.config = config
        self.gcs_access = gcs_access
        self.log = logger.bind(component="H3DataLoader")

    def _load_table_from_gcs(self, gcs_path: str, table_name: str):
        """Load data from GCS into a DuckDB table using optimized GCS access - WITH CLEANUP."""
        try:
            # Use the optimized download approach with our DuckDB connection
            with self.gcs_access._temp_download(gcs_path) as temp_file:
                self.conn.execute(f"""
                    CREATE OR REPLACE TABLE {table_name} AS
                    SELECT * FROM read_parquet('{temp_file}')
                """)
            self.log.debug(f"✅ Loaded {table_name} from {gcs_path}")

            # Force garbage collection after loading large files
            gc.collect()

        except Exception as e:
            self.log.error(f"❌ Failed to load {table_name} from {gcs_path}: {e}")
            raise

    def _get_latest_silver_path(self, dataset: str) -> str:
        """Get path to latest silver data file."""
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
        """Get path to latest gold data file for a specific year."""
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
        """Check if a GCS path exists and has data."""
        try:
            return self.gcs_access.file_exists(path)
        except Exception as e:
            self.log.debug(f"Error checking GCS path {path}: {e}")
            return False

    def _check_year_data_availability(self, year: int) -> bool:
        """Check if required data is available for a given year."""
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
        """Get list of years for which both pesticide and field data are available."""
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
        """Load BMD data from GCS - CACHED to avoid reloading."""
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
                -- Include additional BMD-specific columns
                samlet_belastning as total_load_per_unit,
                belastning_miljøeffekt as environmental_effect_per_unit,
                belastning_miljøadfærd as environmental_behavior_per_unit,
                belastning_sundhed as health_effect_per_unit,
                TRY_CAST(REPLACE(REPLACE(koncentration_er, ',', '.'), ' ', '') AS DOUBLE) as concentration_numeric,
                enhed_er
            FROM temp_bmd_raw
            WHERE registrerings_nr IS NOT NULL
            AND produktnavn IS NOT NULL
            AND aktivstofnavn_e IS NOT NULL
        """)

        # Clean up temporary table
        self.conn.execute(f"DROP TABLE IF EXISTS {temp_bmd_table}")

        # Get statistics for logging
        total_count = self.conn.execute("SELECT COUNT(*) FROM bmd_pfas_lookup").fetchone()[0]
        pfas_count = self.conn.execute(
            "SELECT COUNT(*) FROM bmd_pfas_lookup WHERE contains_pfas_compounds = true"
        ).fetchone()[0]
        diquat_count = self.conn.execute(
            "SELECT COUNT(*) FROM bmd_pfas_lookup WHERE contains_diquat = true"
        ).fetchone()[0]
        glyphosate_count = self.conn.execute(
            "SELECT COUNT(*) FROM bmd_pfas_lookup WHERE contains_glyphosate = true"
        ).fetchone()[0]

        self.log.info(f"✅ BMD data processed: {total_count:,} products")
        self.log.info(
            f"   - PFAS compounds: {pfas_count:,} ({pfas_count / total_count * 100:.1f}%)"
        )
        self.log.info(
            f"   - Diquat compounds: {diquat_count:,} ({diquat_count / total_count * 100:.1f}%)"
        )
        self.log.info(
            f"   - Glyphosate compounds: {glyphosate_count:,} ({glyphosate_count / total_count * 100:.1f}%)"
        )

        return "bmd_pfas_lookup"

    def load_and_prepare_fields_from_gcs(self, field_year: int, pesticide_year: int) -> str:
        """Load FVM field data from GCS and prepare for spatial intersection."""
        self.log.info(f"📄 Loading FVM field data for year {field_year} from GCS")

        # Get FVM data path
        silver_path = self._get_latest_silver_path(f"fvm_marker_{field_year}")
        if not silver_path:
            raise FileNotFoundError(f"No FVM marker data found for year {field_year}")

        # Load into temporary table
        temp_table = f"temp_fvm_{field_year}"
        self._load_table_from_gcs(silver_path, temp_table)

        # Get pesticide field lookup for filtering
        pesticide_path = self._get_latest_gold_path("pesticide_disaggregation", pesticide_year)
        if pesticide_path:
            self.log.info(
                f"🔗 Filtering FVM fields to only those with pesticide data from {pesticide_year}"
            )
            temp_pesticide_table = f"temp_pesticide_lookup_{pesticide_year}"
            self._load_table_from_gcs(pesticide_path, temp_pesticide_table)

            # Check what CVR column exists in pesticide data
            pest_lookup_columns = self.conn.execute(
                f"PRAGMA table_info({temp_pesticide_table})"
            ).fetchall()
            pest_lookup_column_names = [col[1] for col in pest_lookup_columns]

            # Handle CVR column mapping for lookup
            if "CompanyRegistrationNumber" in pest_lookup_column_names:
                pest_cvr_col = "CompanyRegistrationNumber"
            elif "cvr_number" in pest_lookup_column_names:
                pest_cvr_col = "cvr_number"
            elif "company_registration_number" in pest_lookup_column_names:
                pest_cvr_col = "company_registration_number"
            else:
                self.log.warning(
                    f"No CVR column found in pesticide data. Available columns: {pest_lookup_column_names}"
                )
                pest_cvr_col = "NULL"

            # Create lookup table
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE pesticide_field_lookup AS
                SELECT DISTINCT
                    {pest_cvr_col} as cvr,
                    REGEXP_EXTRACT(MatchedFieldID, 'marker_(.+)', 1) as field_id,
                    REGEXP_EXTRACT(MatchedBlockID, 'block_(.+)', 1) as block_id
                FROM {temp_pesticide_table}
                WHERE MatchedFieldID IS NOT NULL
                AND MatchedBlockID IS NOT NULL
                AND {pest_cvr_col} IS NOT NULL
            """)
            self.conn.execute(f"DROP TABLE IF EXISTS {temp_pesticide_table}")
        else:
            # Create empty lookup
            self.conn.execute("""
                CREATE OR REPLACE TABLE pesticide_field_lookup AS
                SELECT 'dummy' as cvr, 'dummy' as field_id, 'dummy' as block_id
                WHERE FALSE
            """)

        # Check available columns (handle older years and new standardized names)
        columns = self.conn.execute(f"PRAGMA table_info({temp_table})").fetchall()
        column_names = [col[1] for col in columns]

        # Handle CVR column - prioritize new standardized name
        if "cvr_number" in column_names:
            cvr_select = "cvr_number"
        elif "company_registration_number" in column_names:
            cvr_select = "company_registration_number as cvr_number"
        else:
            cvr_select = "NULL as cvr_number"

        # Handle block ID column - prioritize new standardized name
        if "block_id" in column_names:
            block_select = "block_id"
        elif "block_number" in column_names:
            block_select = "block_number as block_id"
        else:
            block_select = "NULL as block_id"

        # Handle area column - prioritize new standardized name with proper casting
        if "area_ha" in column_names:
            area_select = "CAST(area_ha AS DOUBLE) as area_ha"
        elif "field_area_ha" in column_names:
            area_select = "CAST(field_area_ha AS DOUBLE) as area_ha"
        else:
            area_select = "NULL as area_ha"

        self.log.info(f"🔍 Available columns: {column_names}")
        self.log.info(f"🔍 CVR column handling: {cvr_select}")
        self.log.info(f"🔍 Block ID column handling: {block_select}")
        self.log.info(f"🔍 Area column handling: {area_select}")

        # Build the SELECT clause with proper column references
        if "area_ha" in column_names:
            area_field_select = "CAST(f.area_ha AS DOUBLE) as area_ha"
        elif "field_area_ha" in column_names:
            area_field_select = "CAST(f.field_area_ha AS DOUBLE) as area_ha"
        else:
            area_field_select = "NULL as area_ha"

        if "cvr_number" in column_names:
            cvr_field_select = "f.cvr_number"
        elif "company_registration_number" in column_names:
            cvr_field_select = "f.company_registration_number as cvr_number"
        else:
            cvr_field_select = "NULL as cvr_number"

        if "block_id" in column_names:
            block_field_select = "f.block_id"
        elif "block_number" in column_names:
            block_field_select = "f.block_number as block_id"
        else:
            block_field_select = "NULL as block_id"

        # Handle geometry column name dynamically
        if "geometry_wkt" in column_names:
            geometry_field_select = "f.geometry_wkt"
            geometry_column = "f.geometry_wkt"
        elif "geometry" in column_names:
            geometry_field_select = "f.geometry as geometry_wkt"
            geometry_column = "f.geometry"
        else:
            geometry_field_select = "NULL as geometry_wkt"
            geometry_column = "NULL"

        self.log.info(f"🔍 Geometry column handling: {geometry_column}")

        # Process fields with geometry preparation
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE prepared_fields AS
            SELECT
                f.field_id,
                {area_field_select},
                {cvr_field_select},
                {block_field_select},
                f.crop_code,
                f.crop_name,
                {geometry_field_select}
            FROM {temp_table} f
            INNER JOIN pesticide_field_lookup p ON (
                {"f.cvr_number" if "cvr_number" in column_names else "f.company_registration_number"} = p.cvr
                AND f.field_id = p.field_id
                AND {"f.block_id" if "block_id" in column_names else "f.block_number"} = p.block_id
            )
            WHERE {geometry_column} IS NOT NULL
            AND ST_IsValid(ST_GeomFromText({geometry_column}))
            AND {"CAST(f.area_ha AS DOUBLE)" if "area_ha" in column_names else "CAST(f.field_area_ha AS DOUBLE)"} > 0
            AND {"f.cvr_number" if "cvr_number" in column_names else "f.company_registration_number"} IS NOT NULL
            AND {"f.block_id" if "block_id" in column_names else "f.block_number"} IS NOT NULL
        """)

        # Clean up temporary table
        self.conn.execute(f"DROP TABLE IF EXISTS {temp_table}")

        count = self.conn.execute("SELECT COUNT(*) FROM prepared_fields").fetchone()[0]
        self.log.info(f"✅ Field data processed: {count:,} fields with geometries")

        return "prepared_fields"

    def load_pesticide_disaggregation_from_gcs(self, year: int) -> str:
        """Load pesticide disaggregation data from GCS for a specific year."""
        table_name = f"pesticides_{year}"

        self.log.info(f"🧪 Loading pesticide disaggregation for year {year} from GCS")

        # Get the latest pesticide disaggregation file for the year
        full_path = self._get_latest_gold_path("pesticide_disaggregation", year)

        if not full_path:
            raise Exception(f"No pesticide disaggregation data found for year {year}")

        self.log.info(f"📄 Loading pesticides from: {full_path}")

        # Load pesticide data directly from GCS
        temp_pesticides_table = "temp_pesticides_raw"
        self._load_table_from_gcs(full_path, temp_pesticides_table)

        # Check available columns and handle both old and new standardized names
        pest_columns = self.conn.execute("PRAGMA table_info(temp_pesticides_raw)").fetchall()
        pest_column_names = [col[1] for col in pest_columns]

        # Handle CVR column mapping - check both old and new names
        cvr_column_candidates = [
            "CompanyRegistrationNumber",  # Original unified pipeline format
            "cvr_number",  # New standardized format
            "company_registration_number",  # Alternative standardized format
        ]
        cvr_column = None
        for candidate in cvr_column_candidates:
            if candidate in pest_column_names:
                cvr_column = candidate
                break

        if not cvr_column:
            self.log.error(
                f"No CVR column found in pesticide disaggregation data. Available columns: {pest_column_names}"
            )
            raise Exception("No CVR column found in pesticide disaggregation data")

        # Handle pesticide name column - check both old and new names
        pesticide_name_candidates = ["PesticideName", "pesticide_name"]
        pesticide_name_column = None
        for candidate in pesticide_name_candidates:
            if candidate in pest_column_names:
                pesticide_name_column = candidate
                break
        if not pesticide_name_column:
            pesticide_name_column = "PesticideName"  # fallback

        # Handle pesticide registration number column - check both old and new names
        pesticide_reg_candidates = ["PesticideRegistrationNumber", "pesticide_registration_number"]
        pesticide_reg_column = None
        for candidate in pesticide_reg_candidates:
            if candidate in pest_column_names:
                pesticide_reg_column = candidate
                break
        if not pesticide_reg_column:
            pesticide_reg_column = "PesticideRegistrationNumber"  # fallback

        # Handle dosage quantity column - check both old and new names
        dosage_quantity_candidates = ["DosageQuantity", "dosage_quantity"]
        dosage_quantity_column = None
        for candidate in dosage_quantity_candidates:
            if candidate in pest_column_names:
                dosage_quantity_column = candidate
                break
        if not dosage_quantity_column:
            dosage_quantity_column = "DosageQuantity"  # fallback

        # Handle dosage unit column - check both old and new names
        dosage_unit_candidates = ["DosageUnit", "dosage_unit"]
        dosage_unit_column = None
        for candidate in dosage_unit_candidates:
            if candidate in pest_column_names:
                dosage_unit_column = candidate
                break
        if not dosage_unit_column:
            dosage_unit_column = "DosageUnit"  # fallback

        self.log.info("🔍 Pesticide column mappings:")
        self.log.info(f"   CVR: {cvr_column}")
        self.log.info(f"   Pesticide name: {pesticide_name_column}")
        self.log.info(f"   Registration number: {pesticide_reg_column}")
        self.log.info(f"   Dosage quantity: {dosage_quantity_column}")
        self.log.info(f"   Dosage unit: {dosage_unit_column}")

        # Process pesticide data with correct field names and CVR + block_id + field_id extraction
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT
                DisaggregatedID,
                MatchedFieldID,
                MatchedBlockID,
                {cvr_column} as cvr,
                {pesticide_name_column} as PesticideName,
                {pesticide_reg_column} as PesticideRegistrationNumber,
                {dosage_quantity_column} as DosageQuantity,
                {dosage_unit_column} as DosageUnit,
                AllocatedArea,
                AllocationMethod,
                MatchConfidence,
                -- Extract field_id and block_id for matching
                REGEXP_EXTRACT(MatchedFieldID, 'marker_(.+)', 1) as extracted_field_id,
                REGEXP_EXTRACT(MatchedBlockID, 'block_(.+)', 1) as extracted_block_id
            FROM temp_pesticides_raw
            WHERE MatchedFieldID IS NOT NULL
            AND MatchedBlockID IS NOT NULL
            AND {cvr_column} IS NOT NULL
            AND {pesticide_reg_column} IS NOT NULL
        """)

        # Clean up temporary table
        self.conn.execute(f"DROP TABLE IF EXISTS {temp_pesticides_table}")

        # Get count for logging
        count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        self.log.info(f"✅ Loaded {count:,} pesticide disaggregation records for year {year}")

        return table_name

    def join_pesticide_with_bmd_pfas(self, pesticide_table: str, bmd_table: str, year: int) -> str:
        """Join pesticide disaggregation with BMD data for PFAS detection."""
        pesticide_pfas_table = f"pesticide_pfas_{year}"

        self.log.info(f"🧪 Joining pesticide data with BMD PFAS indicators for year {year}")

        self.conn.execute(f"""
            CREATE OR REPLACE TABLE {pesticide_pfas_table} AS
            SELECT
                p.DisaggregatedID,
                p.MatchedFieldID,
                p.MatchedBlockID,
                p.cvr,
                p.extracted_field_id,
                p.extracted_block_id,
                p.PesticideName,
                p.PesticideRegistrationNumber,
                p.DosageQuantity,
                p.DosageUnit,
                p.AllocatedArea,
                p.AllocationMethod,
                p.MatchConfidence,

                -- BMD substance detection data
                b.active_ingredients,
                b.total_load_per_unit,
                COALESCE(b.contains_pfas_compounds, false) as contains_pfas,
                COALESCE(b.contains_diquat, false) as contains_diquat,
                COALESCE(b.contains_glyphosate, false) as contains_glyphosate,

                -- Calculate actual PFAS-containing active ingredient amount applied (grams) with proper unit conversion
                CASE
                    WHEN b.contains_pfas_compounds = true AND b.concentration_numeric IS NOT NULL THEN
                        CASE
                            -- Liter dosage (Unit 4) with g/l concentration: L × g/l = g
                            WHEN p.DosageUnit = 4 AND b.enhed_er LIKE '%g/l%' THEN
                                p.DosageQuantity * b.concentration_numeric / 1000.0

                            -- Kg dosage (Unit 2) with g/kg concentration: kg × g/kg = g
                            WHEN p.DosageUnit = 2 AND b.enhed_er LIKE '%g/kg%' THEN
                                p.DosageQuantity * b.concentration_numeric / 1000.0

                            ELSE 0
                        END
                    ELSE 0
                END as pfas_containing_active_ingredient_grams,

                -- Calculate diquat-containing active ingredient amount applied (grams)
                CASE
                    WHEN b.contains_diquat = true AND b.concentration_numeric IS NOT NULL THEN
                        CASE
                            -- Liter dosage (Unit 4) with g/l concentration: L × g/l = g
                            WHEN p.DosageUnit = 4 AND b.enhed_er LIKE '%g/l%' THEN
                                p.DosageQuantity * b.concentration_numeric / 1000.0

                            -- Kg dosage (Unit 2) with g/kg concentration: kg × g/kg = g
                            WHEN p.DosageUnit = 2 AND b.enhed_er LIKE '%g/kg%' THEN
                                p.DosageQuantity * b.concentration_numeric / 1000.0

                            ELSE 0
                        END
                    ELSE 0
                END as diquat_containing_active_ingredient_grams,

                -- Calculate glyphosate-containing active ingredient amount applied (grams)
                CASE
                    WHEN b.contains_glyphosate = true AND b.concentration_numeric IS NOT NULL THEN
                        CASE
                            -- Liter dosage (Unit 4) with g/l concentration: L × g/l = g
                            WHEN p.DosageUnit = 4 AND b.enhed_er LIKE '%g/l%' THEN
                                p.DosageQuantity * b.concentration_numeric / 1000.0

                            -- Kg dosage (Unit 2) with g/kg concentration: kg × g/kg = g
                            WHEN p.DosageUnit = 2 AND b.enhed_er LIKE '%g/kg%' THEN
                                p.DosageQuantity * b.concentration_numeric / 1000.0

                            ELSE 0
                        END
                    ELSE 0
                END as glyphosate_containing_active_ingredient_grams,

                -- Pesticide load applied
                CASE
                    WHEN b.total_load_per_unit IS NOT NULL THEN
                        p.DosageQuantity * b.total_load_per_unit
                    ELSE 0
                END as pesticide_belastning_applied,

                -- PFAS-containing pesticide load
                CASE
                    WHEN b.contains_pfas_compounds = true AND b.total_load_per_unit IS NOT NULL THEN
                        p.DosageQuantity * b.total_load_per_unit
                    ELSE 0
                END as pfas_containing_pesticide_belastning_applied,

                -- Diquat-containing pesticide load
                CASE
                    WHEN b.contains_diquat = true AND b.total_load_per_unit IS NOT NULL THEN
                        p.DosageQuantity * b.total_load_per_unit
                    ELSE 0
                END as diquat_containing_pesticide_belastning_applied,

                -- Glyphosate-containing pesticide load
                CASE
                    WHEN b.contains_glyphosate = true AND b.total_load_per_unit IS NOT NULL THEN
                        p.DosageQuantity * b.total_load_per_unit
                    ELSE 0
                END as glyphosate_containing_pesticide_belastning_applied

            FROM {pesticide_table} p
            LEFT JOIN {bmd_table} b ON (
                p.PesticideRegistrationNumber = b.registration_number
                OR LOWER(p.PesticideName) = LOWER(b.pesticide_name)
            )
        """)

        # Get statistics for logging
        total_count = self.conn.execute(f"SELECT COUNT(*) FROM {pesticide_pfas_table}").fetchone()[
            0
        ]
        pfas_count = self.conn.execute(
            f"SELECT COUNT(*) FROM {pesticide_pfas_table} WHERE contains_pfas = true"
        ).fetchone()[0]
        diquat_count = self.conn.execute(
            f"SELECT COUNT(*) FROM {pesticide_pfas_table} WHERE contains_diquat = true"
        ).fetchone()[0]
        glyphosate_count = self.conn.execute(
            f"SELECT COUNT(*) FROM {pesticide_pfas_table} WHERE contains_glyphosate = true"
        ).fetchone()[0]

        self.log.info(f"✅ Pesticide-BMD join completed: {total_count:,} records")
        self.log.info(
            f"   - PFAS-containing applications: {pfas_count:,} ({pfas_count / total_count * 100:.1f}%)"
        )
        self.log.info(
            f"   - Diquat-containing applications: {diquat_count:,} ({diquat_count / total_count * 100:.1f}%)"
        )
        self.log.info(
            f"   - Glyphosate-containing applications: {glyphosate_count:,} ({glyphosate_count / total_count * 100:.1f}%)"
        )

        return pesticide_pfas_table
