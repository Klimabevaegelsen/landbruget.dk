"""
NLES5 Nitrogen Estimation Gold Layer

This module implements the gold layer processor for NLES5 nitrogen washout estimation.
It combines agricultural fields data with real climate data (DMI), soil types, and
fertilizer data to create comprehensive nitrogen washout estimates using the full NLES5 model.

ENHANCED IMPLEMENTATION (Updated):
The processor now includes full fertilizer data integration following the complete NLES5 model:

The NLES5 model calculates nitrogen washout based on:
- Field geometry and crop type with accurate crop parameters
- Real percolation data from DMI (precipitation - evaporation) in 3 seasonal periods
- Soil type parameters and drainage characteristics (sand vs clay)
- Complete fertilizer application data:
  * Total nitrogen quota (tn_t_ha)
  * Mineral nitrogen spring application (mineral_n_foraar)
  * Mineral nitrogen autumn application (mineral_n_eft)
  * Mineral nitrogen applied during growing season (mineral_n_udb)
  * Organic nitrogen from livestock manure (organic_n_hus)
  * Harmoni area level calculations (niveau)
  * Nitrogen fixation from legumes (nfix_ha) - to be enhanced
- All 8 NLES5 nitrogen coefficients (Bt, Bcs, Bca, Budb, Bm1, Bf0, Bf1, Bg0)
- Trend effect calculation with reference year 2017

Final nitrogen washout formula: Y5 = trend_effect + V^1.5 * perco_soil_effect
Where V = 23.51 + crop_effect + nitrogen_effect

DATASETS INTEGRATED:
- Required: agricultural_fields (fvm_marker_YYYY), dmi_data, soil_types,
  fertilizer_accounts, field_plan
- Optional: catch_crops
- STRICT: The pipeline will FAIL immediately if any required dataset is missing.

OUTPUT:
- Detailed nitrogen washout estimates per field with quality indicators
- Summary statistics by soil type, crop type, and overall
- Full audit trail of all model components and data sources
"""

import contextlib
import glob
import json
import os
import re
import tempfile
from typing import Any

from unified_pipeline.common.base import BaseSource, GoldJobInterface
from common.gcs import GCSDataAccess
from unified_pipeline.util.log_util import Logger
from unified_pipeline.util.timing import timed

from .climate_processor import NLES5ClimateProcessor

# Import configuration and modules from the new modular structure
from .config import NLES5NitrogenEstimationGoldConfig
from .data_loader import NLES5DataLoader
from .field_id_validator import FieldIDValidator
from .memory_utils import NLES5MemoryUtils
from .nles5_calculator import NLES5Calculator
from .pipeline_orchestrator import NLES5PipelineOrchestrator
from .prejoin_validations import NLES5PrejoinValidator
from .spatial_operations import NLES5SpatialOperations
from .validator import NLES5Validator


class NLES5NitrogenEstimationGold(BaseSource[NLES5NitrogenEstimationGoldConfig], GoldJobInterface):
    """
    Gold layer processor for NLES5 nitrogen washout estimation using real climate data.

    This processor implements the full NLES5 model with:
    - Real DMI climate data (precipitation and evaporation)
    - Spatial joins between fields and climate grids
    - Seasonal percolation aggregations
    - Complete nitrogen effect calculations
    - Soil and drainage effect modeling

    The processor handles yearly FVM marker datasets (fvm_marker_YYYY) and automatically
    discovers available years or processes specified target years.
    """

    def __init__(self, config: NLES5NitrogenEstimationGoldConfig):
        super().__init__(config)

        # Configure file logging in the pipeline directory
        pipeline_dir = os.path.dirname(os.path.abspath(__file__))
        log_dir = os.path.join(pipeline_dir, "logs")
        os.makedirs(log_dir, exist_ok=True)

        # Set log directory for this session
        os.environ["LOG_DIR"] = log_dir

        self.log = Logger.get_logger()

        # Add AI-parsable JSON log file
        json_log_path = os.path.join(log_dir, "log_ai_{time}.json")
        self.log.add(json_log_path, serialize=True, level="INFO")

        self.log.info(f"📝 Pipeline logs will be saved to: {log_dir}")
        self.log.info(f"💾 Log files pattern: {log_dir}/log_*.log")
        self.log.info(f"🤖 AI-parsable JSON log: {json_log_path}")
        self.log.info(
            f"🔧 Pipeline configuration: {config.batch_size:,} batch size, "
            f"{config.max_memory_usage_gb}GB memory limit"
        )

        self.phase_times: dict[str, float] = {}
        self.gcs_access = GCSDataAccess()
        self.conn = self.gcs_access.duckdb_conn
        self._configure_duckdb()

        # Initialize specialized processors

        self.data_loader = NLES5DataLoader(self)
        self.climate_processor = NLES5ClimateProcessor(self)
        self.spatial_operations = NLES5SpatialOperations(self)
        self.nles5_calculator = NLES5Calculator(self)
        self.validator = NLES5Validator(self)
        self.prejoin_validator = NLES5PrejoinValidator(self)
        self.memory_utils = NLES5MemoryUtils(self)
        self.pipeline_orchestrator = NLES5PipelineOrchestrator(self)
        self.field_id_validator = FieldIDValidator(self)

    def _configure_duckdb(self):
        """Configure DuckDB for optimal production spatial operations."""
        # Clean up any existing temp files first
        self._cleanup_temp_files()

        # Always use local workspace temp directory
        self.temp_dir = os.path.abspath("data_cache/duckdb_temp/nles5")
        os.makedirs(self.temp_dir, exist_ok=True)
        self.log.info(f"💾 Using temp directory: {self.temp_dir}")

        # Memory settings optimized for large dataset processing
        self.conn.execute(f"SET memory_limit = '{self.config.max_memory_usage_gb}GB'")
        self.conn.execute(f"SET threads = {self.config.threads}")  # Use configured thread count
        self.conn.execute(f"SET temp_directory = '{self.temp_dir}'")
        self.conn.execute(
            f"SET preserve_insertion_order = {str(self.config.preserve_insertion_order).lower()}"
        )

        # Set increased temp directory size for large datasets
        self.conn.execute(
            f"SET max_temp_directory_size = '{self.config.max_temp_directory_size_gb}GB'"
        )
        self.log.info(f"💽 Max temp directory size: {self.config.max_temp_directory_size_gb}GB")

        # Additional memory optimizations for uncertainty analysis
        # Remove invalid DuckDB parameters that don't exist in current version

        # Additional memory optimizations for complex temporal analysis
        self.conn.execute("SET enable_progress_bar = false")  # Reduce overhead
        # Checkpoint management is handled automatically by DuckDB

        # Additional memory optimizations for large spatial operations
        # Note: DuckDB handles external sorting automatically when memory limits are reached
        # Only use verified DuckDB configuration parameters

        # Memory management optimizations
        self.conn.execute("SET http_timeout = 120000")  # Increase timeout for large GCS files

        # Spatial query optimization settings
        self.conn.execute("SET default_null_order = 'nulls_last'")

        # Spatial extensions already loaded by GCSDataAccess
        # Verify SPATIAL_JOIN operator availability
        try:
            version_result = self.conn.execute(
                "SELECT extension_name, extension_version FROM duckdb_extensions() "
                "WHERE extension_name = 'spatial'"
            ).fetchone()
            if version_result:
                self.log.info(f"DuckDB Spatial version: {version_result[1]}")
                if version_result[1] >= "1.2.2":
                    self.log.info("✅ SPATIAL_JOIN operator available")
                else:
                    self.log.warning(
                        f"⚠️  SPATIAL_JOIN operator may not be available in "
                        f"version {version_result[1]}"
                    )
        except Exception as e:
            self.log.warning(f"Could not verify spatial extension version: {e}")

        # Initial configuration complete

    def _cleanup_temp_files(self):
        """Delegate to memory utils if available, otherwise provide basic cleanup."""
        if hasattr(self, "memory_utils") and self.memory_utils:
            return self.memory_utils._cleanup_temp_files()
        # Basic cleanup implementation
        try:
            import glob
            import os
            import shutil

            # Force DuckDB to flush any pending writes
            with contextlib.suppress(Exception):
                self.conn.execute("CHECKPOINT")

            # Clean workspace temp directories
            temp_patterns = ["data_cache/duckdb_temp/*", "data_cache/temp/*"]

            # Add the specific temp directory for this job
            if hasattr(self, "temp_dir") and os.path.exists(self.temp_dir):
                temp_patterns.append(os.path.join(self.temp_dir, "*"))

            cleaned_files = 0
            freed_bytes = 0

            for pattern in temp_patterns:
                for file_path in glob.glob(pattern):
                    try:
                        if os.path.isfile(file_path):
                            # 🔍 DIAGNOSTIC: Skip DuckDB temp files to prevent corruption
                            filename = os.path.basename(file_path)
                            if filename.startswith("duckdb_temp_storage_") and filename.endswith(
                                ".tmp"
                            ):
                                if hasattr(self, "log"):
                                    self.log.debug(
                                        f"🔍 CLEANUP: Skipping active DuckDB temp file: {filename}"
                                    )
                                continue

                            file_size = os.path.getsize(file_path)
                            os.remove(file_path)
                            freed_bytes += file_size
                            cleaned_files += 1
                        elif os.path.isdir(file_path):
                            # Calculate directory size before removal
                            dir_size = sum(
                                os.path.getsize(os.path.join(dirpath, filename))
                                for dirpath, dirnames, filenames in os.walk(file_path)
                                for filename in filenames
                            )
                            shutil.rmtree(file_path)
                            freed_bytes += dir_size
                            cleaned_files += 1
                    except Exception as e:
                        if hasattr(self, "log"):
                            self.log.debug(f"Could not remove temp file {file_path}: {e}")

            if hasattr(self, "log") and cleaned_files > 0:
                self.log.info(
                    f"🧹 Cleaned {cleaned_files} temp files, "
                    f"freed {freed_bytes / 1024 / 1024:.1f} MB"
                )

        except Exception as e:
            if hasattr(self, "log"):
                self.log.debug(f"Temp file cleanup error: {e}")
            pass

    def _get_available_fvm_marker_years(self) -> list[int]:
        """Delegate to data loader."""
        return self.data_loader._get_available_fvm_marker_years()

    def _read_fvm_marker_data_for_year(self, year: int) -> str | None:
        """Delegate to data loader."""
        return self.data_loader._read_fvm_marker_data_for_year(year)

    def _prepare_crop_sequences(
        self, agricultural_fields_table: str, loaded_tables: dict[str, str]
    ) -> str:
        """Delegate to NLES5 calculator."""
        return self.nles5_calculator._prepare_crop_sequences(
            agricultural_fields_table, loaded_tables
        )

    def _create_simplified_crop_classification(self, agricultural_fields_table: str) -> str:
        """
        Create a simplified crop classification table using agricultural fields data.
        Fallback method when field_plan data is not available.
        """
        self.log.info(
            "Creating simplified crop classification table using agricultural fields data "
            "(field_plan not available)..."
        )

        # Use the existing agricultural fields data to create basic crop classifications
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE fields_with_crop_classifications AS
            SELECT
                a.field_id,
                a.year,
                COALESCE(a.crop_name, 'Unknown crop') as current_crop_name,
                'Unknown crop' as prev_crop_name,

                -- Simple M code classification based on available crop data
                CASE
                    WHEN a.crop_name ILIKE '%hvede%' THEN 'M1'  -- Wheat variants
                    WHEN a.crop_name ILIKE '%byg%' THEN 'M2'   -- Barley variants
                    WHEN a.crop_name ILIKE '%græs%' THEN 'M4'  -- Grass variants
                    WHEN a.crop_name ILIKE '%raps%' THEN 'M9'  -- Rape variants
                    WHEN a.crop_name ILIKE '%majs%' THEN 'M8'  -- Maize
                    WHEN a.crop_name ILIKE '%brak%' THEN 'M6'  -- Set-aside
                    ELSE 'M2' -- Default to spring cereal
                END as m_code,

                -- Simple W code - default to moderate winter cover
                'W5' as w_code,

                -- Simple MP code - default to other crops
                'MP2' as mp_code,

                -- Simple WP code - default to bare soil
                'WP2' as wp_code,

                -- Simple WC code - default to low N uptake
                'WC2' as wc_code,

                -- Validation flags
                true as has_current_crop,
                false as has_previous_crop,
                false as has_two_year_history

            FROM {agricultural_fields_table} a
        """)

        simplified_count = self.conn.execute(
            "SELECT COUNT(*) FROM fields_with_crop_classifications"
        ).fetchone()[0]
        self.log.info(
            f"✅ Created {simplified_count:,} simplified crop classifications "
            f"using agricultural fields data."
        )
        return "fields_with_crop_classifications"

    @timed(name="Loading agricultural fields data")
    def _load_agricultural_fields_data(self, silver_data: dict[str, Any] | None) -> str:
        """Delegate to data loader."""
        return self.data_loader._load_agricultural_fields_data(silver_data)

    @timed(name="Validating field IDs before processing")
    def _validate_field_ids_before_processing(self) -> None:
        """Collect and validate field IDs before processing begins."""
        return self.field_id_validator.collect_field_ids_before_processing()

    @timed(name="Validating field IDs after processing")
    def _validate_field_ids_after_processing(self) -> None:
        """Collect and validate field IDs after processing completes."""
        return self.field_id_validator.collect_field_ids_after_processing()

    @timed(name="Running comprehensive field ID validation")
    def _run_field_id_validation(self) -> dict[str, Any]:
        """Run comprehensive field ID validation for the entire pipeline."""
        return self.field_id_validator.run_comprehensive_validation()

    def _get_fertilizer_data_path(self, target_year: int | None = None) -> str:
        """
        Get path to fertilizer data for the specified year, prioritizing GKEA files
        over Gødningsregnskaber.
        """
        try:
            # Look for files in the latest fertilizer directory
            dirs = self.gcs_access.list_files(f"gs://{self.config.bucket}/silver/fertiliser/*/")

            if not dirs:
                raise FileNotFoundError("No fertiliser directories found")

            # Get the most recent directory
            latest_dir = sorted(dirs, reverse=True)[0]
            if not latest_dir.endswith("/"):
                latest_dir += "/"

            # List all files in the directory
            pattern = f"{latest_dir}*.parquet"
            files = self.gcs_access.list_files(pattern)

            if not files:
                raise FileNotFoundError(f"No parquet files found in {latest_dir}")

            # Extract just the filenames for easier filtering
            filenames = [f.split("/")[-1] for f in files]
            self.log.info(f"Available fertilizer files: {filenames}")

            # Priority 1: Try to find GKEA files for the target year
            if target_year:
                gkea_files = [
                    f for f in files if f"GKEA{target_year}" in f and "Gødningsoplysninger" in f
                ]
                if gkea_files:
                    selected_file = sorted(gkea_files)[-1]
                    self.log.info(f"🎯 Selected {target_year} fertilizer data: {selected_file}")
                    return selected_file

            # If no target year or target year not found, try recent years in order
            for year in [2024, 2023, 2022, 2021]:
                gkea_files = [f for f in files if f"GKEA{year}" in f and "Gødningsoplysninger" in f]
                if gkea_files:
                    selected_file = sorted(gkea_files)[-1]
                    self.log.info(f"🎯 Selected {year} fertilizer data (fallback): {selected_file}")
                    return selected_file

            # Final fallback to any fertilizer files
            fertilizer_files = [f for f in files if "Gødnings" in f or "fertiliser" in f]
            if fertilizer_files:
                selected_file = sorted(fertilizer_files)[-1]
                self.log.info(f"🎯 Selected fallback fertilizer data: {selected_file}")
                return selected_file

            raise FileNotFoundError("No suitable fertilizer files found")

        except Exception as e:
            self.log.error(f"Error selecting fertilizer data: {e}")
            # Fall back to default method
            return self._get_latest_silver_path(self.config.fertilizer_dataset)

    def _get_field_plan_data_path(self, target_year: int | None = None) -> str:
        """
        Get the specific path for field plan data (Markplan_med_Gødningsoplysninger)
        from fertiliser directory.

        Priority order:
        1. GKEA[target_year]_Markplan_med_Gødningsoplysninger.parquet (target year field plan)
        2. Most recent Markplan_med_Gødningsoplysninger [year].parquet file
        3. None found - raise exception
        """
        try:
            # If target year is specified, prioritize that year
            if target_year is not None:
                # Look for GKEA files for the target year
                # 2023 has _Aktindsigt suffix, other years don't
                if target_year == 2023:
                    gkea_pattern = f"gs://{self.config.bucket}/silver/fertiliser/*/GKEA{target_year}_Markplan_med_Gødningsoplysninger_Aktindsigt.parquet"
                else:
                    gkea_pattern = f"gs://{self.config.bucket}/silver/fertiliser/*/GKEA{target_year}_Markplan_med_Gødningsoplysninger.parquet"

                self.log.info(
                    f"🔍 Searching for GKEA {target_year} field plan data with pattern: "
                    f"{gkea_pattern}"
                )
                gkea_files = self.gcs_access.list_files(gkea_pattern)

                if gkea_files:
                    selected_file = sorted(gkea_files)[-1]  # Get most recent timestamp
                    self.log.info(f"📋 Selected {target_year} field plan data: {selected_file}")
                    return selected_file
                self.log.warning(f"⚠️ No GKEA {target_year} field plan files found.")

            # Priority 2: Historical Markplan files (try recent years in order)
            for year in [2024, 2023, 2022, 2021]:
                # 2023 has _Aktindsigt suffix, other years don't
                if year == 2023:
                    historical_pattern = f"gs://{self.config.bucket}/silver/fertiliser/*/GKEA{year}_Markplan_med_Gødningsoplysninger_Aktindsigt.parquet"
                else:
                    historical_pattern = f"gs://{self.config.bucket}/silver/fertiliser/*/GKEA{year}_Markplan_med_Gødningsoplysninger.parquet"

                self.log.info(
                    f"🔍 Searching for {year} field plan data with pattern: {historical_pattern}"
                )
                historical_files = self.gcs_access.list_files(historical_pattern)

                if historical_files:
                    selected_file = sorted(historical_files)[-1]  # Get most recent
                    self.log.info(f"📅 Selected {year} field plan data: {selected_file}")
                    return selected_file

            # Final fallback to any field plan files
            fallback_pattern = f"gs://{self.config.bucket}/silver/fertiliser/*/GKEA*_Markplan_med_Gødningsoplysninger*.parquet"
            fallback_files = self.gcs_access.list_files(fallback_pattern)

            if fallback_files:
                selected_file = sorted(fallback_files)[-1]  # Get most recent
                self.log.info(f"📅 Selected fallback field plan data: {selected_file}")
                return selected_file

            raise ValueError(
                "No field plan (Markplan_med_Gødningsoplysninger) files found in "
                "fertiliser directory"
            )

        except Exception as e:
            self.log.error(f"Error selecting field plan data: {e}")
            raise ValueError(f"Cannot find field plan data: {e}") from e

    def _get_catch_crops_data_path(self, target_year: int | None = None) -> str:
        """
        Get the specific path for catch crops data (Efterafgrøder) from fertiliser directory.

        Priority order:
        1. Efterafgrøder [target_year].parquet (target year catch crops)
        2. Most recent Efterafgrøder [year].parquet file
        3. None found - raise exception
        """
        try:
            # If target year is specified, prioritize that year
            if target_year is not None:
                pattern_target = (
                    f"gs://{self.config.bucket}/silver/fertiliser/*/"
                    f"Efterafgrøder {target_year}.parquet"
                )
                files_target = self.gcs_access.list_files(pattern_target)

                if files_target:
                    selected_file = sorted(files_target)[-1]
                    self.log.info(f"🌱 Selected {target_year} catch crops data: {selected_file}")
                    return selected_file

            # Priority 2: Historical Efterafgrøder files (try recent years in order)
            for year in [2024, 2023, 2022, 2021]:
                pattern_year = (
                    f"gs://{self.config.bucket}/silver/fertiliser/*/Efterafgrøder {year}.parquet"
                )
                files_year = self.gcs_access.list_files(pattern_year)

                if files_year:
                    selected_file = sorted(files_year)[-1]
                    self.log.info(f"🌱 Selected {year} catch crops data: {selected_file}")
                    return selected_file

            # Final fallback to any catch crops files
            fallback_pattern = (
                f"gs://{self.config.bucket}/silver/fertiliser/*/Efterafgrøder*.parquet"
            )
            fallback_files = self.gcs_access.list_files(fallback_pattern)

            if fallback_files:
                selected_file = sorted(fallback_files)[-1]
                self.log.info(f"🌱 Selected fallback catch crops data: {selected_file}")
                return selected_file

            raise ValueError("No catch crops (Efterafgrøder) files found in fertiliser directory")

        except Exception as e:
            self.log.error(f"Error selecting catch crops data: {e}")
            raise ValueError(f"Cannot find catch crops data: {e}") from e

    def _read_silver_data_from_path(
        self, dataset_name: str, file_path: str, target_table: str
    ) -> bool:
        """Delegate to data loader."""
        return self.data_loader._read_silver_data_from_path(dataset_name, file_path, target_table)

    def _load_required_silver_datasets(self, silver_data: dict[str, Any] | None) -> dict[str, str]:
        """Delegate to data loader."""
        return self.data_loader._load_required_silver_datasets(silver_data)

    def _load_and_combine_dmi_data(self) -> bool:
        """Delegate to data loader."""
        return self.data_loader._load_and_combine_dmi_data()

    def _process_climate_data(self) -> str:
        """Delegate to climate processor."""
        return self.climate_processor._process_climate_data()

    def _spatial_join_fields_climate(self) -> str:
        """Delegate to spatial operations."""
        return self.spatial_operations._spatial_join_fields_climate()

    def _join_with_soil_data(self) -> str:
        """Delegate to spatial operations."""
        return self.spatial_operations._join_with_soil_data()

    def _join_fields_with_soil(self, input_table: str) -> str:
        """Delegate to spatial operations."""
        return self.spatial_operations._join_fields_with_soil(input_table)

    def _join_fields_with_crops(self, input_table: str) -> str:
        """Delegate to spatial operations."""
        return self.spatial_operations._join_fields_with_crops(input_table)

    def _join_fields_with_nitrogen(self, input_table: str) -> str:
        """Delegate to spatial operations."""
        return self.spatial_operations._join_fields_with_nitrogen(input_table)

    def _log_spatial_join_summary(self, final_table: str):
        """Delegate to spatial operations."""
        return self.spatial_operations._log_spatial_join_summary(final_table)

    @timed(name="Implementing detailed percolation effects")
    def _calculate_detailed_percolation_effects(self) -> str:
        """Delegate to NLES5 calculator."""
        return self.nles5_calculator._calculate_detailed_percolation_effects()

    @timed(name="Calculating NLES5 nitrogen estimates")
    def _calculate_nles5_estimates(self) -> str:
        """Delegate to NLES5 calculator."""
        return self.nles5_calculator._calculate_nles5_estimates()

    @timed(name="Validating NLES5 estimates")
    def _validate_nles5_estimates(self) -> bool:
        """Delegate to validator."""
        return self.validator._validate_nles5_estimates()

    @timed(name="Testing reference implementation compliance")
    def _test_reference_compliance(self) -> bool:
        """Delegate to validator."""
        return self.validator._test_reference_compliance()

    @timed(name="Analyzing estimates distribution")
    def _analyze_estimates_distribution(self) -> None:
        """Delegate to validator."""
        return self.validator._analyze_estimates_distribution()

    @timed(name="Calculating uncertainty estimates")
    def _calculate_uncertainty_estimates(self) -> str:
        """Delegate to validator."""
        return self.validator._calculate_uncertainty_estimates()

    @timed(name="Analyzing uncertainty patterns")
    def _analyze_uncertainty_patterns(self) -> str:
        """Delegate to validator."""
        return self.validator._analyze_uncertainty_patterns()

    @timed(name="Creating unified NLES5 results table")
    def _create_unified_results_table(self) -> None:
        """Create a single unified table combining all NLES5 analysis results."""
        try:
            self.log.info("🔄 Creating unified NLES5 results table with all analysis data...")

            # Create unified table combining main estimates with analysis data
            self.conn.execute("""
                CREATE OR REPLACE TABLE nles5_unified_results AS
                SELECT
                    -- Main estimate fields
                    main.field_id,
                    main.field_uuid,  -- UUID populated via join with agricultural fields data
                    main.block_id,
                    main.cvr_number,
                    main.year,
                    main.area_ha,
                    main.crop_type,
                    main.soil_code,
                    main.soil_description,
                    main.clay_content,
                    main.nitrogen_washout_kg_ha,
                    main.percolation_mm,
                    main.uncertainty_pct,
                    main.data_quality_score,
                    main.geometry_wkt,
                    main.created_at,

                    -- Enhanced uncertainty information from detailed analysis
                    COALESCE(
                        unc.total_uncertainty_kg_ha,
                        main.nitrogen_washout_kg_ha * main.uncertainty_pct / 100.0
                    ) as total_uncertainty_kg_ha,
                    COALESCE(
                        unc.total_uncertainty_pct, main.uncertainty_pct
                    ) as enhanced_uncertainty_pct,

                    -- Soil type analysis
                    soil_analysis.avg_nitrogen_by_soil,
                    soil_analysis.soil_type_percentile,

                    -- Crop type analysis
                    crop_analysis.avg_nitrogen_by_crop,
                    crop_analysis.crop_type_percentile,

                    -- Uncertainty components
                    unc.bt_uncertainty,
                    unc.bcs_uncertainty,
                    unc.bca_uncertainty,
                    unc.budb_uncertainty,
                    unc.bm1_uncertainty,
                    unc.bf0_uncertainty,
                    unc.bf1_uncertainty,
                    unc.bg0_uncertainty

                FROM nles5_nitrogen_estimates_gold main

                -- Join with uncertainty estimates
                LEFT JOIN nles5_uncertainty_estimates unc ON
                    main.field_id = unc.field_id AND main.year = unc.year

                -- Join with soil type analysis
                LEFT JOIN (
                    SELECT
                        soil_code,
                        AVG(nitrogen_washout_kg_ha) as avg_nitrogen_by_soil,
                        PERCENT_RANK() OVER (
                            ORDER BY AVG(nitrogen_washout_kg_ha)
                        ) * 100 as soil_type_percentile
                    FROM nles5_estimates_by_soil_type
                    GROUP BY soil_code
                ) soil_analysis ON main.soil_code = soil_analysis.soil_code

                -- Join with crop type analysis
                LEFT JOIN (
                    SELECT
                        crop_type,
                        AVG(nitrogen_washout_kg_ha) as avg_nitrogen_by_crop,
                        PERCENT_RANK() OVER (
                            ORDER BY AVG(nitrogen_washout_kg_ha)
                        ) * 100 as crop_type_percentile
                    FROM nles5_estimates_by_crop_type
                    GROUP BY crop_type
                ) crop_analysis ON main.crop_type = crop_analysis.crop_type
            """)

            # Get statistics on the unified table
            unified_stats = self.conn.execute("""
                SELECT
                    COUNT(*) as total_records,
                    AVG(nitrogen_washout_kg_ha) as avg_nitrogen,
                    AVG(enhanced_uncertainty_pct) as avg_uncertainty,
                    AVG(data_quality_score) as avg_quality,
                    COUNT(
                        CASE WHEN enhanced_uncertainty_pct BETWEEN 8 AND 15 THEN 1 END
                    ) as records_in_target_uncertainty
                FROM nles5_unified_results
            """).fetchone()

            if unified_stats:
                total, avg_n, avg_unc, avg_qual, target_unc_count = unified_stats
                target_unc_pct = (target_unc_count / total * 100) if total > 0 else 0

                self.log.info(f"✅ Created unified results table with {total:,} records")
                self.log.info(f"📊 Average nitrogen washout: {avg_n:.2f} kg N/ha")
                self.log.info(f"📊 Average uncertainty: {avg_unc:.1f}% (target: 8-15%)")
                self.log.info(f"📊 Average data quality: {avg_qual:.2f} (target: 0.8+)")
                self.log.info(
                    f"📊 Records in target uncertainty range: {target_unc_count:,} "
                    f"({target_unc_pct:.1f}%)"
                )

        except Exception as e:
            self.log.error(f"Error creating unified results table: {e}")
            raise

    @timed(name="Saving NLES5 results to gold layer")
    def _save_results_to_gold(self) -> None:
        """Save NLES5 results to the gold layer using shared GCS interface."""
        try:
            self.log.info("Saving NLES5 results to gold layer using shared GCS interface")

            # Create unified results table first
            self._create_unified_results_table()

            failed_uploads = 0

            # Define output tables with subdataset names for standard pattern
            # Include the new unified table as the primary output
            failed_uploads = 0

            # Define output tables with subdataset names for standard pattern
            tables_to_save = [
                ("nles5_unified_results", "unified_results"),  # NEW: Primary comprehensive table
                ("nles5_nitrogen_estimates_gold", "nitrogen_estimates"),
                ("nles5_estimates_analysis", "estimates_analysis"),
                ("nles5_estimates_by_soil_type", "estimates_by_soil_type"),
                ("nles5_estimates_by_crop_type", "estimates_by_crop_type"),
                ("nles5_uncertainty_estimates", "uncertainty_estimates"),
                ("nles5_uncertainty_analysis", "uncertainty_analysis"),
                ("nles5_uncertainty_patterns", "uncertainty_patterns"),
            ]

            for table_name, subdataset in tables_to_save:
                try:
                    count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                    if count > 0:
                        # Create standard timestamped path structure
                        timestamp = self.date_pattern
                        dataset_name = f"{self.config.dataset}_{subdataset}"
                        gcs_path = f"gold/{dataset_name}/{timestamp}/data.parquet"
                        full_gcs_path = f"gs://{self.config.bucket}/{gcs_path}"

                        # Export to local file first to avoid DuckDB temp file issues
                        # with GCS writes
                        with tempfile.NamedTemporaryFile(
                            suffix=".parquet", delete=False
                        ) as tmp_file:
                            local_parquet = tmp_file.name

                        try:
                            # Export to local parquet file
                            try:
                                self.conn.execute(f"""
                                    COPY {table_name}
                                    TO '{local_parquet}'
                                    (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
                                """)
                            except Exception as copy_error:
                                # DuckDB sometimes fails to cleanup temp files but the COPY succeeds
                                if "Could not remove file" in str(copy_error) and os.path.exists(
                                    local_parquet
                                ):
                                    self.log.warning(
                                        f"⚠️ DuckDB temp file cleanup warning (ignorable): "
                                        f"{copy_error}"
                                    )
                                else:
                                    raise

                            # Upload local file to GCS using streaming
                            import shutil

                            with (
                                open(local_parquet, "rb") as src,
                                self.gcs_access.fs.open(full_gcs_path, "wb") as dst,
                            ):
                                shutil.copyfileobj(src, dst)

                            self.log.info(
                                f"✅ Saved {table_name} ({count:,} rows) to {full_gcs_path}"
                            )
                        finally:
                            # Clean up local temp file
                            if os.path.exists(local_parquet):
                                os.remove(local_parquet)
                                self.log.info("🧹 Cleaned up local temp file")
                    else:
                        self.log.warning(f"Table {table_name} is empty, skipping")
                except Exception as e:
                    self.log.error(f"Failed to save {table_name}: {e}")
                    failed_uploads += 1

            if failed_uploads > 0:
                raise RuntimeError(f"{failed_uploads} GCS uploads failed. Check logs for details.")

            # Log the standard path structure being used
            timestamp = self.date_pattern
            base_path = f"gs://{self.config.bucket}/gold/{self.config.dataset}_*/{timestamp}/"
            unified_path = f"gs://{self.config.bucket}/gold/{self.config.dataset}_unified_results/{timestamp}/data.parquet"

            self.log.info("✅ NLES5 results saved using shared GCS interface")
            self.log.info(f"🎯 PRIMARY TABLE: {unified_path}")
            self.log.info(f"📁 Base path structure: {base_path}")
            self.log.info(
                f"📊 Saved {len(tables_to_save) - failed_uploads}/"
                f"{len(tables_to_save)} tables successfully"
            )
            self.log.info(
                "💡 Use 'nles5_unified_results' table for comprehensive analysis "
                "with uncertainty data"
            )

        except Exception as e:
            self.log.error(f"Error saving results: {e}")
            raise

    async def run(self, silver_data: dict[str, Any] | None = None) -> None:
        """Delegate to pipeline orchestrator."""
        return await self.pipeline_orchestrator.run(silver_data)

    async def _run_pipeline_batched(self, silver_data: dict[str, Any] | None = None) -> None:
        """Delegate to pipeline orchestrator."""
        return await self.pipeline_orchestrator._run_pipeline_batched(silver_data)

    async def _run_pipeline_single(self, silver_data: dict[str, Any] | None = None) -> None:
        """Delegate to pipeline orchestrator."""
        return await self.pipeline_orchestrator._run_pipeline_single(silver_data)

    def _process_fields_in_chunks(self, table_name: str, operation_name: str) -> int:
        """Delegate to spatial operations."""
        return self.spatial_operations._process_fields_in_chunks(table_name, operation_name)

    def _process_tessellation_in_chunks(self) -> str:
        """Delegate to spatial operations."""
        return self.spatial_operations._process_tessellation_in_chunks()

    def _spatial_join_fields_climate_batched(self) -> str:
        """Delegate to spatial operations."""
        return self.spatial_operations._spatial_join_fields_climate_batched()

    def _calculate_nles5_estimates_batched(self) -> str:
        """Delegate to NLES5 calculator."""
        return self.nles5_calculator._calculate_nles5_estimates_batched()

    def _optimize_table_for_production(self, table_name: str) -> None:
        """Delegate to spatial operations."""
        return self.spatial_operations._optimize_table_for_production(table_name)

    def _verify_spatial_join_optimization(self) -> None:
        """Delegate to spatial operations."""
        return self.spatial_operations._verify_spatial_join_optimization()

    def _optimize_spatial_table_for_joins(self, table_name: str) -> None:
        """Delegate to spatial operations."""
        return self.spatial_operations._optimize_spatial_table_for_joins(table_name)

    def _log_nles5_results_preview(self):
        """Log a preview of the generated NLES5 estimates in a structured format."""
        try:
            self.log.info("📊 PREVIEW OF GENERATED NLES5 DATA")

            # Check if the main results table exists
            tables_to_check = [
                "nles5_nitrogen_estimates",
                "nles5_estimates_final_batched",
                "estimates_target_2021",
                "estimates_target_2022",
                "estimates_target_2023",
                "estimates_target_2024",
            ]

            for table_name in tables_to_check:
                try:
                    count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                    if count > 0:
                        self.log.info(f"✅ Found {count:,} records in table: {table_name}")

                        # Check what columns exist in this table
                        columns = self.conn.execute(f"""
                            SELECT column_name
                            FROM information_schema.columns
                            WHERE table_name = '{table_name}'
                        """).fetchall()
                        col_set = {col[0].lower() for col in columns}

                        # Build column selection based on what exists
                        soil_col = (
                            "soil_code"
                            if "soil_code" in col_set
                            else "soil_type"
                            if "soil_type" in col_set
                            else "soil_type_category"
                            if "soil_type_category" in col_set
                            else "'unknown'"
                        )
                        crop_col = (
                            "crop_type"
                            if "crop_type" in col_set
                            else "m_code"
                            if "m_code" in col_set
                            else "'unknown'"
                        )
                        weather_col = "w_code" if "w_code" in col_set else "'n/a'"
                        nitrogen_col = (
                            "nitrogen_washout_kg_ha"
                            if "nitrogen_washout_kg_ha" in col_set
                            else "nitrogen_leaching_nles5"
                            if "nitrogen_leaching_nles5" in col_set
                            else "nitrogen_washout_kg_n_ha"
                            if "nitrogen_washout_kg_n_ha" in col_set
                            else "0"
                        )

                        # Get sample data from this table with dynamic column selection
                        preview_data = self.conn.execute(f"""
                            SELECT
                                field_id,
                                year,
                                {soil_col} as soil_type,
                                {crop_col} as m_code,
                                {weather_col} as w_code,
                                {nitrogen_col} as nitrogen_washout_kg_ha
                            FROM {table_name}
                            WHERE {nitrogen_col} > 0
                            ORDER BY random()
                            LIMIT 5
                        """).fetchall()

                        if preview_data:
                            # Human-readable table format
                            self.log.info(f"📋 Sample data from {table_name}:")
                            self.log.info(
                                "   Field ID  | Year | Soil Type | Crop Code | "
                                "Weather | N-Washout(kg/ha)"
                            )
                            self.log.info("   " + "-" * 70)
                            for row in preview_data:
                                field_id, year, soil_type, m_code, w_code, nitrogen = row
                                self.log.info(
                                    f"   {field_id[:8]:<9} | {year} | {soil_type[:9]:<9} | "
                                    f"{m_code[:9]:<9} | {w_code[:7]:<7} | {nitrogen:12.2f}"
                                )

                            # Also log JSON for programmatic access
                            for row in preview_data:
                                preview_log = {
                                    "table": table_name,
                                    "field_id": row[0],
                                    "year": row[1],
                                    "soil_type": row[2],
                                    "m_code": row[3],
                                    "w_code": row[4],
                                    "nitrogen_washout_kg_ha": f"{row[5]:.2f}",
                                }
                                self.log.info(f"NLES5_PREVIEW: {json.dumps(preview_log)}")
                        else:
                            self.log.warning(
                                f"Table {table_name} has {count:,} records but no positive "
                                f"nitrogen washout values"
                            )

                        # Show statistics for this table using the detected nitrogen column
                        stats = self.conn.execute(f"""
                            SELECT
                                COUNT(*) as total_records,
                                COUNT(CASE WHEN {nitrogen_col} > 0 THEN 1 END)
                                    as positive_estimates,
                                AVG({nitrogen_col}) as avg_nitrogen,
                                MIN({nitrogen_col}) as min_nitrogen,
                                MAX({nitrogen_col}) as max_nitrogen
                            FROM {table_name}
                        """).fetchone()

                        stats_log = {
                            "table": table_name,
                            "total_records": stats[0],
                            "positive_estimates": stats[1],
                            "avg_nitrogen": f"{stats[2]:.2f}" if stats[2] else "0.00",
                            "min_nitrogen": f"{stats[3]:.2f}" if stats[3] else "0.00",
                            "max_nitrogen": f"{stats[4]:.2f}" if stats[4] else "0.00",
                        }
                        self.log.info(f"NLES5_STATS: {json.dumps(stats_log)}")

                        # Only log preview for the first table with data
                        break

                except Exception:
                    # Table doesn't exist or can't be queried - continue to next
                    continue
            else:
                self.log.warning("No NLES5 results tables found with data")

        except Exception as e:
            self.log.error(f"Failed to generate NLES5 data preview: {e}")

    def _get_memory_usage(self) -> float:
        """Delegate to memory utils."""
        return self.memory_utils._get_memory_usage()

    def _monitor_memory_usage(self, operation_name: str) -> None:
        """Delegate to memory utils."""
        return self.memory_utils._monitor_memory_usage(operation_name)

    def _aggressive_memory_cleanup(self) -> None:
        """Delegate to memory utils."""
        return self.memory_utils._aggressive_memory_cleanup()

    def _log_production_performance_summary(self, total_time: float, result_count: int) -> None:
        """Log comprehensive production performance summary."""
        self.log.info("\n" + "=" * 80)
        self.log.info("🚀 NLES5 PRODUCTION PERFORMANCE SUMMARY")
        self.log.info("=" * 80)
        self.log.info(
            f"⏱️  Total execution time: {total_time:.1f} seconds ({total_time / 60:.1f} minutes)"
        )
        self.log.info(f"📊 Fields processed: {result_count:,}")
        self.log.info(
            f"🔧 Configuration: {self.config.batch_size:,} batch size, "
            f"{self.config.max_memory_usage_gb}GB memory"
        )

        if result_count > 0:
            self.log.info(f"⚡ Processing rate: {result_count / total_time:.0f} fields/second")
            self.log.info(f"📈 Throughput: {(result_count / total_time) * 3600:.0f} fields/hour")

        # Log DuckDB version and spatial capabilities
        try:
            version_result = self.conn.execute(
                "SELECT extension_name, extension_version FROM duckdb_extensions() "
                "WHERE extension_name = 'spatial'"
            ).fetchone()
            if version_result:
                self.log.info(f"🦆 DuckDB Spatial version: {version_result[1]}")
        except Exception:
            pass

        self.log.info("🌍 PRODUCTION-READY DENMARK-WIDE NLES5 PROCESSING COMPLETE")
        self.log.info("=" * 80)

    @timed(name="Creating NLES5 parameter lookup tables")
    def _create_nles5_parameter_tables(self) -> None:
        """Delegate to NLES5 calculator."""
        return self.nles5_calculator._create_nles5_parameter_tables()

    def _create_spatial_tables(self) -> None:
        """Delegate to spatial operations."""
        return self.spatial_operations._create_spatial_tables()

    def _verify_spatial_join_readiness(self) -> None:
        """Delegate to spatial operations."""
        return self.spatial_operations._verify_spatial_join_readiness()

    @timed(name="Preparing nitrogen input tables")
    def _prepare_nitrogen_inputs_tables(self) -> None:
        """Delegate to NLES5 calculator."""
        return self.nles5_calculator._prepare_nitrogen_inputs_tables()

    def _comprehensive_data_validation(self) -> dict[str, Any]:
        """Delegate to validator."""
        return self.validator._comprehensive_data_validation()

    def _validate_table_quality(self, table_name: str) -> dict[str, Any]:
        """Validate data quality for a specific table."""
        stats = {
            "table_name": table_name,
            "total_records": 0,
            "null_geometries": 0,
            "invalid_geometries": 0,
            "quality_score": 0.0,
            "issues": [],
            "exists": False,
        }

        try:
            # Check if table exists
            exists_result = self.conn.execute(f"""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_name = '{table_name}'
            """).fetchone()

            if not exists_result or exists_result[0] == 0:
                stats["issues"].append(f"Table {table_name} does not exist")
                return stats

            stats["exists"] = True

            # Get basic statistics
            basic_stats = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            stats["total_records"] = basic_stats[0] if basic_stats else 0

            if stats["total_records"] == 0:
                stats["issues"].append(f"Table {table_name} is empty")
                return stats

            # Geometry-specific validation for spatial tables
            if "spatial" in table_name or table_name in [
                "climate_percolation",
                "soil_types_prepared",
            ]:
                geom_column = "geom" if table_name != "climate_percolation" else "geometry"

                if table_name == "soil_types_prepared":
                    geom_column = "geom"  # soil_types uses 'geom' after ST_Dump

                try:
                    geom_stats = self.conn.execute(f"""
                         SELECT
                             COUNT(*) as total,
                             COUNT(CASE WHEN {geom_column} IS NULL THEN 1 END) as null_geom,
                             COUNT(CASE WHEN {geom_column} IS NOT NULL AND
                                             NOT ST_IsValid({geom_column})
                                   THEN 1 END) as invalid_geom,
                             COUNT(CASE WHEN {geom_column} IS NOT NULL AND
                                             ST_IsValid({geom_column})
                                   THEN 1 END) as valid_geom
                         FROM {table_name}
                     """).fetchone()

                    if geom_stats:
                        stats["null_geometries"] = geom_stats[1]
                        stats["invalid_geometries"] = geom_stats[2]
                        valid_geom_count = geom_stats[3]

                        # Calculate geometry quality score
                        if stats["total_records"] > 0:
                            geom_quality = valid_geom_count / stats["total_records"]
                            stats["quality_score"] = geom_quality * 100

                            if geom_quality < 0.95:
                                stats["issues"].append(
                                    f"Only {geom_quality:.1%} valid geometries in {table_name}"
                                )
                            if stats["null_geometries"] > 0:
                                stats["issues"].append(
                                    f"{stats['null_geometries']:,} null geometries in {table_name}"
                                )
                            if stats["invalid_geometries"] > 0:
                                stats["issues"].append(
                                    f"{stats['invalid_geometries']:,} invalid geometries in "
                                    f"{table_name}"
                                )

                except Exception as e:
                    stats["issues"].append(f"Could not validate geometries in {table_name}: {e}")

            # Table-specific validations
            if table_name == "dmi_data":
                self._validate_climate_data_quality(stats)
            elif table_name == "agricultural_fields_spatial":
                self._validate_field_data_quality(stats)
            elif table_name == "soil_types_prepared":
                self._validate_soil_data_quality(stats)

        except Exception as e:
            stats["issues"].append(f"Validation failed for {table_name}: {e}")

        return stats

    def _validate_climate_data_quality(self, stats: dict[str, Any]) -> None:
        """Validate climate data specific quality metrics."""
        try:
            # Check for required climate parameters
            climate_stats = self.conn.execute("""
                SELECT
                    COUNT(DISTINCT parameter_id) as param_count,
                    COUNT(DISTINCT EXTRACT(YEAR FROM CAST(valid_time AS TIMESTAMP))) as year_count,
                    MIN(valid_time) as min_date,
                    MAX(valid_time) as max_date
                FROM dmi_data
                WHERE valid_time IS NOT NULL
            """).fetchone()

            if climate_stats:
                param_count, year_count, min_date, max_date = climate_stats

                # Determine climate data quality score based on parameter and year coverage
                param_ok = param_count >= 2
                year_ok = year_count >= 3

                if not param_ok:
                    stats["issues"].append(
                        f"CRITICAL: Only {param_count} climate parameters found - "
                        f"NLES5 requires both precipitation and evaporation data"
                    )
                if not year_ok:
                    stats["issues"].append(
                        f"CRITICAL: Only {year_count} years of climate data - "
                        f"NLES5 requires minimum 3 years of real climate data"
                    )

                # Assign a simple quality score: 100% if both criteria met, else 0%
                stats["quality_score"] = 100.0 if (param_ok and year_ok) else 0.0

                stats["climate_years"] = year_count
                stats["climate_parameters"] = param_count
                stats["date_range"] = f"{min_date} to {max_date}"

        except Exception as e:
            stats["issues"].append(f"Climate data validation failed: {e}")

    def _validate_field_data_quality(self, stats: dict[str, Any]) -> None:
        """Validate agricultural fields data quality."""
        try:
            # Check for required field attributes
            field_stats = self.conn.execute("""
                SELECT
                    COUNT(DISTINCT year) as year_count,
                    COUNT(CASE WHEN area_ha IS NULL OR area_ha <= 0 THEN 1 END) as invalid_areas,
                    COUNT(CASE WHEN crop_code IS NULL THEN 1 END) as missing_crops,
                    AVG(area_ha) as avg_area_ha
                FROM agricultural_fields_spatial
            """).fetchone()

            if field_stats:
                year_count, invalid_areas, missing_crops, avg_area = field_stats

                if invalid_areas > 0:
                    stats["issues"].append(
                        f"{invalid_areas:,} fields with invalid/missing area data"
                    )

                if missing_crops > 0:
                    stats["issues"].append(
                        f"{missing_crops:,} fields with missing crop information"
                    )

                stats["field_years"] = year_count
                stats["avg_field_size_ha"] = round(avg_area, 2) if avg_area else 0

        except Exception as e:
            stats["issues"].append(f"Field data validation failed: {e}")

    def _validate_soil_data_quality(self, stats: dict[str, Any]) -> None:
        """Validate soil types data quality."""
        try:
            soil_stats = self.conn.execute("""
                SELECT
                    COUNT(DISTINCT soil_type) as soil_type_count,
                    COUNT(CASE WHEN clay_content IS NULL THEN 1 END) as missing_clay,
                    AVG(clay_content) as avg_clay_content
                FROM soil_types_prepared
            """).fetchone()

            if soil_stats:
                soil_types, missing_clay, avg_clay = soil_stats

                if soil_types < 5:
                    stats["issues"].append(
                        f"Only {soil_types} soil types found (may indicate incomplete data)"
                    )

                if missing_clay > 0:
                    stats["issues"].append(f"{missing_clay:,} soil records missing clay content")

                stats["soil_type_count"] = soil_types
                stats["avg_clay_content"] = round(avg_clay, 1) if avg_clay else 0

        except Exception as e:
            stats["issues"].append(f"Soil data validation failed: {e}")

    def _generate_validation_recommendations(self, validation_results: dict[str, Any]) -> None:
        """Generate actionable recommendations based on validation results - NO FALLBACK DATA."""
        score = validation_results["data_quality_score"]

        if score < 50:
            validation_results["recommendations"].append(
                "CRITICAL: Data quality severely compromised - pipeline CANNOT proceed "
                "without complete real data"
            )
            validation_results["passed"] = False
        elif score < 75:
            validation_results["recommendations"].append(
                "ERROR: Data quality insufficient - real data must be improved "
                "before pipeline execution"
            )
            validation_results["passed"] = False
        elif score < 90:
            validation_results["recommendations"].append(
                "WARNING: Data quality issues detected - verify real data completeness"
            )
        else:
            validation_results["recommendations"].append(
                "GOOD: Real data quality is acceptable for pipeline execution"
            )

        # Specific recommendations based on table stats - NO FALLBACK SUGGESTIONS
        for table_name, table_stats in validation_results["table_stats"].items():
            if not table_stats["exists"]:
                validation_results["recommendations"].append(
                    f"REQUIRED: Load real {table_name} data from silver layer"
                )
                validation_results["errors"].append(f"Missing table: {table_name}")
                validation_results["passed"] = False
            elif table_stats["total_records"] == 0:
                validation_results["recommendations"].append(
                    f"REQUIRED: Ensure {table_name} contains real data records"
                )
                validation_results["errors"].append(f"Empty table: {table_name}")
                validation_results["passed"] = False
            elif table_stats["quality_score"] < 80:
                validation_results["recommendations"].append(
                    f"REQUIRED: Fix geometry/data quality issues in {table_name} using real data"
                )
                if table_stats["quality_score"] < 50:
                    validation_results["errors"].append(
                        f"Poor data quality in {table_name}: {table_stats['quality_score']:.1f}%"
                    )
                    validation_results["passed"] = False

    def _log_validation_summary(self, validation_results: dict[str, Any]) -> None:
        """Log comprehensive validation summary."""
        self.log.info("📊 DATA VALIDATION SUMMARY:")
        self.log.info(f"   Overall Quality Score: {validation_results['data_quality_score']:.1f}%")
        self.log.info(
            f"   Validation Status: {'✅ PASSED' if validation_results['passed'] else '❌ FAILED'}"
        )

        if validation_results["errors"]:
            self.log.error(f"   Errors ({len(validation_results['errors'])}):")
            for error in validation_results["errors"][:5]:  # Show first 5 errors
                self.log.error(f"     - {error}")

        if validation_results["warnings"]:
            self.log.warning(f"   Warnings ({len(validation_results['warnings'])}):")
            for warning in validation_results["warnings"][:3]:  # Show first 3 warnings
                self.log.warning(f"     - {warning}")

        if validation_results["recommendations"]:
            self.log.info("   Recommendations:")
            for rec in validation_results["recommendations"][:3]:  # Show first 3 recommendations
                self.log.info(f"     - {rec}")

    @timed(name="Validating data availability")
    def _validate_data_availability(self) -> None:
        """Delegate to validator."""
        return self.validator._validate_data_availability()

    def _create_climate_tessellation(self) -> str:
        """Delegate to climate processor."""
        return self.climate_processor._create_climate_tessellation()

    def _spatial_join_fields_climate_tessellation(self) -> str:
        """Delegate to climate processor."""
        return self.climate_processor._spatial_join_fields_climate_tessellation()

    def _join_climate_fields_by_year(self) -> str:
        """Delegate to climate processor."""
        return self.climate_processor._join_climate_fields_by_year()

    def _load_climate_data_for_years(self, years: list[int]) -> str:
        """Delegate to climate processor."""
        return self.climate_processor._load_climate_data_for_years(years)

    def _spatial_join_year_climate(self, year: int, climate_table: str) -> str:
        """Delegate to climate processor."""
        return self.climate_processor._spatial_join_year_climate(year, climate_table)

    def _create_year_tessellation(self, climate_table: str, year: int) -> str:
        """Delegate to climate processor."""
        return self.climate_processor._create_year_tessellation(climate_table, year)

    def _calculate_required_data_years(
        self, target_calculation_years: list[int], available_years: list[int]
    ) -> list[int]:
        """
        Calculate minimum years needed for NLES5 calculations based on 3-year temporal requirements.

        NLES5 Requirements (from Danish documentation):
        - Crop sequence: 3 years (current + previous + year before previous)
        - Fertilizer data: 3 years (current + 2-year averages)
        - Percolation: 2 years (current + previous for drainage effects)

        Args:
            target_calculation_years: Years we want to calculate NLES5 for
            available_years: All years available in the dataset

        Returns:
            Minimum set of years that need to be loaded
        """
        try:
            required_years = set()
            available_years_set = set(available_years)

            self.log.info("🔍 Calculating required data years for NLES5 3-year windows...")

            for target_year in target_calculation_years:
                # Add the target year itself
                if target_year in available_years_set:
                    required_years.add(target_year)
                    self.log.info(f"   Year {target_year}: ✅ target year (current)")
                else:
                    self.log.warning(f"   Year {target_year}: ❌ target year not available")
                    continue

                # Add previous year (needed for percolation effects)
                prev_year = target_year - 1
                if prev_year in available_years_set:
                    required_years.add(prev_year)
                    self.log.info(f"   Year {prev_year}: ✅ previous year (percolation)")
                else:
                    self.log.warning(
                        f"   Year {prev_year}: ❌ previous year not available "
                        f"(percolation effects will be limited)"
                    )

                # Add year before previous (needed for 2-year averages)
                prev_prev_year = target_year - 2
                if prev_prev_year in available_years_set:
                    required_years.add(prev_prev_year)
                    self.log.info(
                        f"   Year {prev_prev_year}: ✅ year before previous (2-year averages)"
                    )
                else:
                    self.log.warning(
                        f"   Year {prev_prev_year}: ❌ year before previous not available "
                        f"(2-year averages will be limited)"
                    )

            # Convert to sorted list
            final_years = sorted(required_years)

            # Calculate memory savings
            total_available = len(available_years)
            total_required = len(final_years)
            years_eliminated = total_available - total_required
            percent_reduction = (
                (years_eliminated / total_available) * 100 if total_available > 0 else 0
            )

            self.log.info("📊 NLES5 Year Optimization Results:")
            self.log.info(
                f"   Available years: {total_available} "
                f"({min(available_years)}-{max(available_years)})"
            )
            self.log.info(f"   Required years: {total_required} → {final_years}")
            self.log.info(
                f"   Years eliminated: {years_eliminated} ({percent_reduction:.1f}% reduction)"
            )
            self.log.info(f"   Memory impact: Loading {total_required}/{total_available} years")

            if total_required == 0:
                raise ValueError("No required years could be satisfied from available data")

            return final_years

        except Exception as e:
            self.log.error(f"Error calculating required data years: {e}")
            raise

    @timed(name="Target-year-by-target-year NLES5 processing")
    def _process_nles5_target_year_by_target_year(self, loaded_tables: dict[str, Any]) -> str:
        """Delegate to NLES5 calculator."""
        return self.nles5_calculator._process_nles5_target_year_by_target_year(loaded_tables)

    def _process_single_target_year(
        self, target_year: int, required_years: list[int], loaded_tables: dict[str, Any]
    ) -> str:
        """Delegate to pipeline orchestrator."""
        return self.pipeline_orchestrator._process_single_target_year(
            target_year, required_years, loaded_tables
        )

    def _aggressive_cleanup_target_year(self):
        """Delegate to memory utils."""
        return self.memory_utils._aggressive_cleanup_target_year()

    def _load_agricultural_fields_for_years(self, years: list[int], table_name: str):
        """Load agricultural fields data for specific years only."""
        try:
            # Prefer already-prepared spatial table to ensure CRS/validity alignment
            try:
                exists_result = self.conn.execute("""
                    SELECT COUNT(*) FROM information_schema.tables
                    WHERE table_name = 'agricultural_fields_spatial'
                """).fetchone()
                if exists_result and exists_result[0] > 0:
                    years_filter = ", ".join(map(str, years))
                    self.conn.execute(f"""
                        CREATE OR REPLACE TEMPORARY TABLE {table_name} AS
                        SELECT
                            field_id, block_id, cvr_number, year,
                            crop_code, area_ha,
                            geom
                        FROM agricultural_fields_spatial
                        WHERE year IN ({years_filter})
                    """)
                    count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                    self.log.info(
                        f"   ✅ Loaded {count:,} agricultural fields from "
                        f"agricultural_fields_spatial for years {years}"
                    )
                    return
            except Exception:
                # If check fails, fall through to GCS load
                pass

            # Fallback to direct GCS load per year (validated, no synthetic defaults)
            union_parts = []
            for year in years:
                dataset_name = f"fvm_marker_{year}"
                try:
                    gcs_path = self._get_latest_silver_path(dataset_name)
                    if gcs_path:
                        union_parts.append(f"""
                            SELECT
                                field_id, block_id, cvr_number, {year} as year,
                                crop_code, area_ha,
                                geometry as geom
                            FROM read_parquet('{gcs_path}')
                            WHERE geometry IS NOT NULL
                        """)
                except Exception as e:
                    self.log.warning(f"   Year {year} data not available: {e}")
                    continue

            if not union_parts:
                raise ValueError(f"No agricultural fields data available for years {years}")

            union_query = " UNION ALL ".join(union_parts)
            self.conn.execute(f"""
                CREATE OR REPLACE TEMPORARY TABLE {table_name} AS
                {union_query}
            """)

            count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            self.log.info(f"   ✅ Loaded {count:,} agricultural fields for years {years}")

        except Exception as e:
            self.log.error(f"Error loading agricultural fields for years {years}: {e}")
            raise

    def _join_climate_fields_for_target_year(self, target_year: int, climate_table: str) -> str:
        """Delegate to climate processor."""
        return self.climate_processor._join_climate_fields_for_target_year(
            target_year, climate_table
        )

    def _join_with_soil_data_target_year(self, fields_climate_table: str) -> str:
        """Join soil data for target year processing."""
        try:
            if fields_climate_table is None:
                self.log.error("❌ Climate joining failed - fields_climate_table is None")
                self.log.error("🔍 This indicates climate data processing or spatial join failed")
                self.log.error(
                    "💡 Check: 1) Climate data availability, 2) Spatial join performance, "
                    "3) Memory limits"
                )
                raise ValueError(
                    "fields_climate_table cannot be None - climate joining must have failed"
                )

            result_table = "fields_complete_target"

            # 🔍 DIAGNOSTIC LOG: Check DuckDB version and EXCEPT support
            try:
                version_result = self.conn.execute("SELECT version()").fetchone()
                self.log.info(f"🔍 DuckDB version: {version_result[0]}")

                # Test EXCEPT syntax support
                test_except = self.conn.execute("SELECT 1 as col1, 2 as col2, 3 as col3").fetchone()
                self.log.info(f"🔍 DuckDB basic SELECT test: {test_except}")
            except Exception as e:
                self.log.warning(f"🔍 Could not check DuckDB version: {e}")

            # 🔍 DIAGNOSTIC LOG: Check if soil_types_prepared table exists and has expected schema
            try:
                soil_table_check = self.conn.execute("""
                    SELECT COUNT(*) as count,
                           COUNT(CASE WHEN soil_code IS NOT NULL THEN 1 END) as has_soil_code,
                           COUNT(CASE WHEN geom IS NOT NULL THEN 1 END) as has_geom
                    FROM soil_types_prepared
                """).fetchone()
                self.log.info(
                    f"🔍 soil_types_prepared table: {soil_table_check[0]:,} rows, "
                    f"{soil_table_check[1]:,} with soil_code, {soil_table_check[2]:,} with geom"
                )
            except Exception as e:
                self.log.error(f"🔍 soil_types_prepared table check failed: {e}")
                raise ValueError(f"soil_types_prepared table is not accessible: {e}") from e

            # 🔍 DIAGNOSTIC LOG: Check actual schema of fields_climate_table
            try:
                schema_info = self.conn.execute(f"""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name = '{fields_climate_table}'
                    ORDER BY ordinal_position
                """).fetchall()
                self.log.info(f"🔍 Schema of {fields_climate_table}:")
                for col_name, col_type in schema_info:
                    self.log.info(f"   - {col_name}: {col_type}")
            except Exception as e:
                self.log.warning(f"🔍 Could not get schema for {fields_climate_table}: {e}")

            # 🔍 DIAGNOSTIC LOG: Count fields before soil join
            fields_before = self.conn.execute(f"""
                SELECT COUNT(*) as total, COUNT(DISTINCT field_uuid) as unique_uuids
                FROM {fields_climate_table}
            """).fetchone()
            self.log.info(
                f"🔍 BEFORE soil join: {fields_before[0]:,} rows, "
                f"{fields_before[1]:,} unique field_uuids"
            )

            # FIXED: Use window function to select closest soil polygon
            # when field intersects multiple polygons
            # This eliminates duplicates by keeping only the nearest soil match per field_uuid
            # FIXED: Replace SELECT * EXCEPT with explicit column selection
            # for DuckDB compatibility
            # FIXED: Use correct schema from fields_climate_target table
            # (no block_id, crop_code columns)

            # 🔧 MEMORY: Apply temporary DuckDB mitigations to reduce peak memory
            # during heavy spatial join
            # Log memory before join
            with contextlib.suppress(Exception):
                self._monitor_memory_usage("Before target-year soil join")

            # 🔍 DIAGNOSTIC: Log soil table complexity and geometry statistics
            try:
                soil_stats = self.conn.execute("""
                    SELECT
                        COUNT(*) as total_rows,
                        COUNT(DISTINCT soil_code) as unique_soil_codes,
                        COUNT(CASE WHEN geom IS NOT NULL THEN 1 END) as geoms_present,
                        AVG(ST_Area(geom)) as avg_area
                    FROM soil_types_prepared
                """).fetchone()

                self.log.info("🔍 SOIL TABLE STATS:")
                self.log.info(f"   Total soil polygons: {soil_stats[0]:,}")
                self.log.info(f"   Unique soil codes: {soil_stats[1]:,}")
                self.log.info(f"   Geometries present: {soil_stats[2]:,}")
                self.log.info(f"   Avg polygon area: {soil_stats[3]:.2e} sq degrees")

                # Log memory footprint estimate based on row count
                estimated_mb_per_row = 0.2  # Conservative estimate for original geometries
                estimated_mb = soil_stats[0] * estimated_mb_per_row
                self.log.info(f"   Estimated geometry memory: ~{estimated_mb:.1f} MB")
            except Exception as e:
                self.log.warning(f"🔍 Could not gather soil table stats: {e}")

            original_threads = None
            original_pio = None
            original_object_cache = None
            try:
                try:
                    original_threads = self.conn.execute(
                        "SELECT current_setting('threads')"
                    ).fetchone()[0]
                except Exception:
                    original_threads = None
                try:
                    original_pio = self.conn.execute(
                        "SELECT current_setting('preserve_insertion_order')"
                    ).fetchone()[0]
                except Exception:
                    original_pio = None
                try:
                    original_object_cache = self.conn.execute(
                        "SELECT current_setting('enable_object_cache')"
                    ).fetchone()[0]
                except Exception:
                    original_object_cache = None

                # Lower threads and disable insertion order to reduce memory fragmentation
                self.conn.execute("SET threads = 1")
                self.conn.execute("SET preserve_insertion_order = false")

                # PHASE 5: Additional memory optimizations
                # Disable object cache to prevent intermediate results from being cached
                self.conn.execute("SET enable_object_cache = false")

                # Note: temp_directory is already set in __init__, cannot be changed after first use

                self.log.info(
                    "🔧 MEMORY: Configured DuckDB for minimal memory usage during soil join"
                )

            except Exception as e:
                # If settings cannot be changed, continue with best effort
                self.log.warning(f"Could not apply all memory settings: {e}")

            # Force aggressive cleanup BEFORE starting the join
            # (outside try block to ensure it runs)
            try:
                self._aggressive_cleanup_target_year()
                self.log.info("🧹 Pre-join cleanup completed")
            except Exception as e:
                self.log.warning(f"Pre-join cleanup failed: {e}")

            # 🔍 DIAGNOSTIC: Check field and soil geometry characteristics
            try:
                field_geom_stats = self.conn.execute(f"""
                    SELECT
                        COUNT(*) as total_fields,
                        COUNT(CASE WHEN geom IS NOT NULL THEN 1 END) as has_geometry,
                        COUNT(CASE WHEN ST_IsValid(geom) THEN 1 END) as valid_geometry,
                        AVG(ST_Area(geom)) as avg_area,
                        MIN(ST_XMin(geom)) as min_x,
                        MAX(ST_XMax(geom)) as max_x,
                        MIN(ST_YMin(geom)) as min_y,
                        MAX(ST_YMax(geom)) as max_y
                    FROM {fields_climate_table}
                    WHERE geom IS NOT NULL
                """).fetchone()

                self.log.info("🔍 FIELD GEOMETRY DIAGNOSTICS:")
                self.log.info(f"   Total fields: {field_geom_stats[0]:,}")
                has_geom_pct = field_geom_stats[1] / field_geom_stats[0] * 100
                self.log.info(f"   Has geometry: {field_geom_stats[1]:,} ({has_geom_pct:.1f}%)")
                valid_geom_pct = field_geom_stats[2] / field_geom_stats[0] * 100
                self.log.info(f"   Valid geometry: {field_geom_stats[2]:,} ({valid_geom_pct:.1f}%)")
                self.log.info(f"   Avg field area: {field_geom_stats[3]:.2e} sq degrees")
                self.log.info(
                    f"   Bounds: X=[{field_geom_stats[4]:.2f}, {field_geom_stats[5]:.2f}], "
                    f"Y=[{field_geom_stats[6]:.2f}, {field_geom_stats[7]:.2f}]"
                )

                soil_geom_stats = self.conn.execute("""
                    SELECT
                        COUNT(*) as total_polygons,
                        MIN(ST_XMin(geom)) as min_x,
                        MAX(ST_XMax(geom)) as max_x,
                        MIN(ST_YMin(geom)) as min_y,
                        MAX(ST_YMax(geom)) as max_y
                    FROM soil_types_prepared
                    WHERE geom IS NOT NULL
                """).fetchone()

                self.log.info("🔍 SOIL GEOMETRY DIAGNOSTICS:")
                self.log.info(f"   Total soil polygons: {soil_geom_stats[0]:,}")
                self.log.info(
                    f"   Bounds: X=[{soil_geom_stats[1]:.2f}, {soil_geom_stats[2]:.2f}], "
                    f"Y=[{soil_geom_stats[3]:.2f}, {soil_geom_stats[4]:.2f}]"
                )

                # Check if bounds overlap
                x_overlap = (
                    field_geom_stats[4] <= soil_geom_stats[2]
                    and field_geom_stats[5] >= soil_geom_stats[1]
                )
                y_overlap = (
                    field_geom_stats[6] <= soil_geom_stats[4]
                    and field_geom_stats[7] >= soil_geom_stats[3]
                )

                if x_overlap and y_overlap:
                    self.log.info("   ✅ Spatial bounds overlap - geometries should be matchable")
                else:
                    self.log.warning("   ⚠️  Spatial bounds DO NOT overlap - possible CRS mismatch!")
                    self.log.warning(f"      X overlap: {x_overlap}, Y overlap: {y_overlap}")

            except Exception as e:
                self.log.warning(f"Could not gather geometry diagnostics: {e}")

            # 🔧 ANALYSIS: Previous code used ST_Centroid which caused ~4% match failures
            # Switching to full geometry intersection for better coverage
            self.log.info(
                "🔧 Using full geometry intersection for soil matching (previously used centroid)"
            )

            # PHASE 3: OPTIMIZED TWO-PHASE SOIL JOIN
            # Instead of materializing all matches with ROW_NUMBER(),
            # use a multi-phase approach:
            # 1. Find intersecting soil polygons and calculate overlap areas
            #    (only for matching pairs)
            # 2. Select largest overlap per field using QUALIFY
            #    (more memory-efficient than window functions)
            # 3. Join back to get final result with all field columns

            self.log.info("🔍 Phase 3.1: Finding intersecting soil polygons...")

            # Phase 1: Find all field-soil intersections and calculate overlap areas
            # BATCHED APPROACH: Process fields in chunks to avoid memory exhaustion
            self.conn.execute("DROP TABLE IF EXISTS field_soil_matches_temp")

            # Get total field count
            total_fields = self.conn.execute(
                f"SELECT COUNT(*) FROM {fields_climate_table}"
            ).fetchone()[0]
            batch_size = 2000  # Process 2K fields at a time - reduced from 5K due to memory constraints in GitHub Actions (8GB limit)
            num_batches = (total_fields + batch_size - 1) // batch_size

            self.log.info(
                f"🔍 Processing {total_fields:,} fields in {num_batches} batches of {batch_size:,}"
            )

            # Create empty result table
            self.conn.execute("""
                CREATE TEMPORARY TABLE field_soil_matches_temp (
                    field_uuid VARCHAR,
                    soil_code VARCHAR,
                    soil_description VARCHAR,
                    clay_content DOUBLE,
                    overlap_area DOUBLE
                )
            """)

            # Process in batches
            for batch_idx in range(num_batches):
                offset = batch_idx * batch_size

                # Log progress every batch (they're small now)
                self.log.info(
                    f"   Batch {batch_idx + 1}/{num_batches}: Processing fields "
                    f"{offset:,} to {min(offset + batch_size, total_fields):,}"
                )

                self.conn.execute(f"""
                    INSERT INTO field_soil_matches_temp
                    SELECT
                        f.field_uuid,
                        s.soil_code,
                        s.soil_description,
                        s.clay_content,
                        COALESCE(ST_Area(ST_Intersection(f.geom, s.geom)), 0) as overlap_area
                    FROM (
                        SELECT * FROM {fields_climate_table}
                        ORDER BY field_uuid
                        LIMIT {batch_size} OFFSET {offset}
                    ) f
                    LEFT JOIN soil_types_prepared s ON ST_Intersects(f.geom, s.geom)
                """)

                # Force checkpoint after each batch to prevent memory accumulation
                with contextlib.suppress(Exception):
                    self.conn.execute("CHECKPOINT")

                # Progress update every 5 batches (reduced from 10 for tighter memory control)
                if (batch_idx + 1) % 5 == 0:
                    try:
                        rows_so_far = self.conn.execute(
                            "SELECT COUNT(*) FROM field_soil_matches_temp"
                        ).fetchone()[0]
                        self.log.info(
                            f"   ✓ Completed {batch_idx + 1}/{num_batches} batches, "
                            f"{rows_so_far:,} matches so far"
                        )
                    except Exception:
                        pass

            # Log intermediate table size for diagnostics
            try:
                match_stats = self.conn.execute("""
                    SELECT
                        COUNT(*) as total_matches,
                        COUNT(DISTINCT field_uuid) as unique_fields,
                        COUNT(*) FILTER (WHERE soil_code IS NOT NULL) as fields_with_soil,
                        AVG(CASE WHEN soil_code IS NOT NULL THEN 1 ELSE 0 END) *
                            COUNT(DISTINCT field_uuid) as matched_fields
                    FROM field_soil_matches_temp
                """).fetchone()

                self.log.info(
                    f"🔍 Intermediate matches: {match_stats[0]:,} total, "
                    f"{match_stats[1]:,} unique fields"
                )
                soil_pct = int(match_stats[3]) / match_stats[1] * 100
                self.log.info(
                    f"   Fields with soil data: {int(match_stats[3]):,} ({soil_pct:.1f}%)"
                )
            except Exception as e:
                self.log.warning(f"Could not log intermediate stats: {e}")

            # Force cleanup after Phase 3.1 to free memory
            try:
                self._aggressive_cleanup_target_year()
                self._monitor_memory_usage("After Phase 3.1")
            except Exception:
                pass

            self.log.info("🔍 Phase 3.2: Selecting largest overlap soil match per field...")

            # Phase 2: Select largest overlap per field
            # (for fields that intersect multiple soil polygons)
            # DuckDB OPTIMIZATION: Use explicit subquery with QUALIFY instead of DISTINCT ON
            # QUALIFY is more memory-efficient for window functions in DuckDB
            self.conn.execute("""
                DROP TABLE IF EXISTS closest_soil_temp
            """)

            self.conn.execute("""
                CREATE TEMPORARY TABLE closest_soil_temp AS
                SELECT
                    field_uuid,
                    soil_code,
                    soil_description,
                    clay_content,
                    CASE WHEN soil_code IS NOT NULL THEN true ELSE false END as has_soil_data
                FROM field_soil_matches_temp
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY field_uuid
                    ORDER BY
                        CASE WHEN soil_code IS NULL THEN 1 ELSE 0 END,
                        overlap_area DESC NULLS LAST
                ) = 1
            """)

            # Clean up intermediate table immediately
            self.conn.execute("DROP TABLE IF EXISTS field_soil_matches_temp")

            # DuckDB OPTIMIZATION: Force garbage collection to free memory
            # This ensures intermediate results are fully cleaned up before next phase
            try:
                self.conn.execute("CHECKPOINT")
                self.log.info("🔧 Memory checkpoint completed between phases")
            except Exception:
                pass  # CHECKPOINT may not be available in all DuckDB versions

            # Force cleanup after Phase 3.2 to free memory
            try:
                self._aggressive_cleanup_target_year()
                self._monitor_memory_usage("After Phase 3.2")
            except Exception:
                pass

            # ENHANCEMENT: Phase 3.2.5 - Nearest Neighbor Fallback for unmatched fields
            # This reduces the default rate by finding the nearest soil polygon for fields
            # that didn't intersect any polygon (typically small fields or edge cases)
            self.log.info("🔍 Phase 3.2.5: Nearest neighbor fallback for unmatched fields...")

            # Initialize for tracking in diagnostics
            unmatched_count = 0

            try:
                # Count fields without soil data
                unmatched_count = self.conn.execute("""
                    SELECT COUNT(*) FROM closest_soil_temp WHERE soil_code IS NULL
                """).fetchone()[0]

                if unmatched_count > 0:
                    self.log.info(
                        f"   Found {unmatched_count:,} fields without soil match, "
                        f"searching for nearest neighbors..."
                    )

                    # For unmatched fields, find the nearest soil polygon within 100m
                    # This helps small fields and edge cases near polygon boundaries
                    self.conn.execute(f"""
                        CREATE TEMPORARY TABLE nearest_soil_fallback AS
                        SELECT
                            field_uuid,
                            soil_code,
                            soil_description,
                            clay_content,
                            distance
                        FROM (
                            SELECT
                                u.field_uuid,
                                s.soil_code,
                                s.soil_description,
                                s.clay_content,
                                ST_Distance(
                                    ST_Centroid(f.geom),
                                    ST_Centroid(s.geom)
                                ) as distance,
                                ROW_NUMBER() OVER (
                                    PARTITION BY u.field_uuid
                                    ORDER BY ST_Distance(
                                        ST_Centroid(f.geom),
                                        ST_Centroid(s.geom)
                                    )
                                ) as rn
                            FROM closest_soil_temp u
                            JOIN {fields_climate_table} f ON u.field_uuid = f.field_uuid
                            CROSS JOIN soil_types_prepared s
                            WHERE u.soil_code IS NULL
                        ) subquery
                        WHERE rn = 1
                          AND distance <= 0.001  -- ~100m in degrees at Denmark's latitude
                    """)

                    # Count how many were recovered
                    recovered_count = self.conn.execute("""
                        SELECT COUNT(*) FROM nearest_soil_fallback
                    """).fetchone()[0]

                    if recovered_count > 0:
                        self.log.info(
                            f"   ✅ Recovered {recovered_count:,} fields using nearest neighbor "
                            f"(within 100m)"
                        )
                        reduction_pct = recovered_count / unmatched_count * 100
                        self.log.info(f"   → Reducing potential defaults by {reduction_pct:.1f}%")

                        # Update closest_soil_temp with nearest neighbor matches
                        self.conn.execute("""
                            UPDATE closest_soil_temp
                            SET
                                soil_code = n.soil_code,
                                soil_description = n.soil_description,
                                clay_content = n.clay_content,
                                has_soil_data = true
                            FROM nearest_soil_fallback n
                            WHERE closest_soil_temp.field_uuid = n.field_uuid
                        """)

                        self.log.info(
                            f"   ✅ Updated {recovered_count:,} fields with nearest soil data"
                        )
                    else:
                        self.log.info(
                            "   ⚠️  No nearby soil polygons found (>100m from all unmatched fields)"
                        )

                    # Clean up temporary table
                    self.conn.execute("DROP TABLE IF EXISTS nearest_soil_fallback")

                    # Log final unmatched count
                    final_unmatched = self.conn.execute("""
                        SELECT COUNT(*) FROM closest_soil_temp WHERE soil_code IS NULL
                    """).fetchone()[0]

                    if final_unmatched > 0:
                        self.log.info(
                            f"   ℹ️  Remaining unmatched fields: {final_unmatched:,} "
                            f"(will use default '5')"
                        )
                    else:
                        self.log.info("   ✅ All fields matched to soil data!")
                else:
                    self.log.info(
                        "   ✅ All fields already matched (no nearest neighbor fallback needed)"
                    )

            except Exception as e:
                self.log.warning(f"   ⚠️  Nearest neighbor fallback failed: {e}")
                self.log.warning("   → Continuing with original matches")

            self.log.info("🔍 Phase 3.3: Joining soil data back to fields...")

            # Phase 3: Join back to original fields table to get final result
            # FIXED: Use closest_soil_temp instead of re-joining with soil_types_prepared
            # This preserves all the work done in Phases 3.1-3.2.5 (batched processing,
            # largest overlap selection, nearest neighbor fallback)
            self.conn.execute(f"""
                CREATE OR REPLACE TEMPORARY TABLE {result_table} AS
                SELECT
                    f.*,
                    COALESCE(s.soil_code, '5') as soil_code,
                    COALESCE(s.soil_description, 'Medium clay soil') as soil_description,
                    COALESCE(s.clay_content, 15.0) as clay_content,
                    CASE WHEN s.soil_code IS NOT NULL THEN true ELSE false END as has_soil_data
                FROM {fields_climate_table} f
                LEFT JOIN closest_soil_temp s ON f.field_uuid = s.field_uuid
            """)

            # Clean up temporary table
            self.conn.execute("DROP TABLE IF EXISTS closest_soil_temp")

            # 🔧 MEMORY: Restore DuckDB settings and log memory after join
            try:
                if original_threads is not None:
                    self.conn.execute(f"SET threads = {original_threads}")
                else:
                    # Restore to configured threads if original couldn't be read
                    self.conn.execute(f"SET threads = {self.config.threads}")
                if original_pio is not None:
                    # original_pio is 'true'/'false'
                    self.conn.execute(f"SET preserve_insertion_order = {original_pio}")
                else:
                    setting_value = str(self.config.preserve_insertion_order).lower()
                    self.conn.execute(f"SET preserve_insertion_order = {setting_value}")
                if original_object_cache is not None:
                    self.conn.execute(f"SET enable_object_cache = {original_object_cache}")
                else:
                    # Default to true if we couldn't read original
                    self.conn.execute("SET enable_object_cache = true")

                self._monitor_memory_usage("After target-year soil join")
            except Exception:
                pass

            # 🔍 DIAGNOSTIC LOG: Verify deduplication worked
            fields_after = self.conn.execute(f"""
                SELECT
                    COUNT(*) as total,
                    COUNT(DISTINCT field_uuid) as unique_uuids,
                    COUNT(*) - COUNT(DISTINCT field_uuid) as duplicate_rows
                FROM {result_table}
            """).fetchone()

            if fields_after[2] == 0:
                self.log.info(
                    f"✅ AFTER soil join (deduplicated): {fields_after[0]:,} rows, "
                    f"{fields_after[1]:,} unique field_uuids, 0 duplicates"
                )
            else:
                self.log.warning(
                    f"⚠️ AFTER soil join: {fields_after[0]:,} rows, "
                    f"{fields_after[1]:,} unique field_uuids, "
                    f"{fields_after[2]:,} DUPLICATE ROWS STILL PRESENT"
                )

            # 🔍 ENHANCED DIAGNOSTIC: Track soil code assignment statistics
            try:
                soil_stats = self.conn.execute(f"""
                    SELECT
                        COUNT(*) as total_fields,
                        COUNT(CASE WHEN soil_code != '5' THEN 1 END) as real_soil_codes,
                        COUNT(CASE WHEN soil_code = '5' THEN 1 END) as default_codes,
                        AVG(CASE WHEN soil_code = '5' THEN area_ha END) as avg_area_defaults,
                        AVG(CASE WHEN soil_code != '5' THEN area_ha END) as avg_area_matched
                    FROM {result_table}
                """).fetchone()

                total, real, defaults, avg_default_area, avg_matched_area = soil_stats
                default_pct = (defaults / total * 100) if total > 0 else 0
                real_pct = (real / total * 100) if total > 0 else 0

                self.log.info("\n📊 SOIL CODE ASSIGNMENT SUMMARY:")
                self.log.info(
                    f"   Real soil codes:  {real:,} ({real_pct:.2f}%) - "
                    f"avg area: {avg_matched_area:.2f} ha"
                )
                self.log.info(
                    f"   Default code '5': {defaults:,} ({default_pct:.2f}%) - "
                    f"avg area: {avg_default_area:.2f} ha"
                )

                # Log improvement from nearest neighbor fallback
                if defaults < unmatched_count:
                    recovered = unmatched_count - defaults
                    improvement_pct = (
                        (recovered / unmatched_count * 100) if unmatched_count > 0 else 0
                    )
                    self.log.info(
                        f"   ✅ Nearest neighbor fallback recovered {recovered:,} fields "
                        f"({improvement_pct:.1f}%)"
                    )

                # Log quality thresholds
                if default_pct < 2.0:
                    self.log.info("   ✅ EXCELLENT: Default rate <2% indicates high soil coverage")
                elif default_pct < 5.0:
                    self.log.info("   ✅ GOOD: Default rate <5% is acceptable for production")
                elif default_pct < 10.0:
                    self.log.info(
                        "   ⚠️  FAIR: Default rate <10% - consider investigating coverage gaps"
                    )
                else:
                    self.log.warning(
                        f"   ⚠️  HIGH: Default rate >{default_pct:.1f}% - "
                        f"investigate soil coverage issues"
                    )

            except Exception as e:
                self.log.warning(f"Could not generate soil code statistics: {e}")

            return result_table

        except Exception as e:
            self.log.error(f"Error joining soil data for target year: {e}")
            raise

    def _add_default_soil_data_target_year(self, fields_climate_table: str) -> str:
        """Add default soil data when soil dataset not available."""
        try:
            result_table = "fields_complete_target"

            self.conn.execute(f"""
                CREATE OR REPLACE TEMPORARY TABLE {result_table} AS
                SELECT
                    f.*,
                    '5' as soil_code,
                    'Medium clay soil' as soil_description,
                    15.0 as clay_content,
                    false as has_soil_data
                FROM {fields_climate_table} f
            """)

            return result_table

        except Exception as e:
            self.log.error(f"Error adding default soil data for target year: {e}")
            raise

    def _calculate_percolation_effects_target_year(self, fields_complete_table: str) -> str:
        """Delegate to NLES5 calculator."""
        return self.nles5_calculator._calculate_percolation_effects_target_year(
            fields_complete_table
        )

    def _calculate_nles5_estimates_target_year(
        self, percolation_table: str, target_year: int
    ) -> str:
        """Delegate to NLES5 calculator."""
        return self.nles5_calculator._calculate_nles5_estimates_target_year(
            percolation_table, target_year
        )

    def _determine_all_target_years(self) -> list[int]:
        """Delegate to pipeline orchestrator."""
        return self.pipeline_orchestrator._determine_all_target_years()

    def _create_target_year_batches(self, target_years: list[int]) -> list[list[int]]:
        """Delegate to pipeline orchestrator."""
        return self.pipeline_orchestrator._create_target_year_batches(target_years)

    async def _run_pipeline_for_batch(
        self, batch_years: list[int], silver_data: dict[str, Any] | None = None
    ) -> int:
        """Delegate to pipeline orchestrator."""
        return await self.pipeline_orchestrator._run_pipeline_for_batch(batch_years, silver_data)

    def _diagnose_missing_data(self, batch_years: list[int], original_error: Exception) -> str:
        """
        Diagnose exactly what real data is missing to provide clear error messages.
        NO FALLBACK DATA IS CREATED - this method only identifies missing data.

        Returns:
            str: Detailed description of what real data is missing and needs to be provided
        """
        missing_data_issues = []

        try:
            # Check agricultural fields data availability
            try:
                field_count = self.conn.execute(f"""
                    SELECT COUNT(*) FROM agricultural_fields_spatial
                    WHERE year IN ({",".join(map(str, batch_years))})
                        AND geom IS NOT NULL
                        AND area_ha > 0
                """).fetchone()[0]

                if field_count == 0:
                    missing_data_issues.append(
                        f"No valid agricultural fields data found for years {batch_years}"
                    )
                else:
                    self.log.info(f"✓ Found {field_count:,} agricultural fields for {batch_years}")
            except Exception as e:
                missing_data_issues.append(f"Cannot access agricultural fields data: {e}")

            # Check climate data availability
            try:
                year_range = ",".join(map(str, range(min(batch_years) - 2, max(batch_years) + 1)))
                climate_years = self.conn.execute(f"""
                    SELECT DISTINCT EXTRACT(YEAR FROM CAST(valid_time AS TIMESTAMP)) as year
                    FROM dmi_data
                    WHERE valid_time IS NOT NULL
                        AND EXTRACT(YEAR FROM CAST(valid_time AS TIMESTAMP)) IN ({year_range})
                    ORDER BY year
                """).fetchall()

                available_climate_years = [row[0] for row in climate_years if row[0] is not None]
                required_climate_years = list(
                    range(min(batch_years) - 2, max(batch_years) + 1)
                )  # NLES5 needs 3-year windows

                missing_climate_years = [
                    year for year in required_climate_years if year not in available_climate_years
                ]
                if missing_climate_years:
                    missing_data_issues.append(
                        f"Missing climate data for years {missing_climate_years} "
                        f"(required for NLES5 3-year windows)"
                    )

            except Exception as e:
                missing_data_issues.append(f"Cannot access climate data: {e}")

            # Check soil types data availability
            try:
                soil_count = self.conn.execute(
                    "SELECT COUNT(*) FROM soil_types_prepared WHERE geom IS NOT NULL"
                ).fetchone()[0]
                if soil_count == 0:
                    missing_data_issues.append(
                        "No soil types data available - real soil data is required for NLES5"
                    )
            except Exception as e:
                missing_data_issues.append(f"Cannot access soil types data: {e}")

            # Check climate data availability
            try:
                climate_count = self.conn.execute(
                    "SELECT COUNT(*) FROM climate_percolation"
                ).fetchone()[0]
                if climate_count == 0:
                    missing_data_issues.append(
                        "No climate percolation data available - real climate grid data is required"
                    )
            except Exception as e:
                missing_data_issues.append(f"Cannot access climate percolation: {e}")

            # Add the original error context
            missing_data_issues.append(f"Original error: {original_error!s}")

        except Exception as e:
            missing_data_issues.append(f"Data diagnosis failed: {e}")

        if not missing_data_issues:
            return (
                "Unknown data availability issue - all tables appear accessible "
                "but processing failed"
            )

        return "; ".join(missing_data_issues)

    def _ensure_final_batched_table_exists(self):
        """Ensure the final batched results table exists."""
        try:
            # Check if table exists
            self.conn.execute("SELECT COUNT(*) FROM nles5_estimates_final_batched")
        except Exception:
            # Table doesn't exist, create it with proper schema
            self.log.info("Creating nles5_estimates_final_batched table...")
            self.conn.execute("""
                CREATE TABLE nles5_estimates_final_batched (
                    field_id VARCHAR,
                    field_uuid VARCHAR,  -- Will be populated via join with agricultural fields data
                    block_id VARCHAR,
                    cvr_number VARCHAR,
                    year INTEGER,
                    area_ha DECIMAL(10,4),  -- FIXED: Support up to 999,999.9999 hectares
                                        -- (was causing DECIMAL(2,1) error)
                    crop_type VARCHAR,
                    soil_code VARCHAR,
                    soil_description VARCHAR,
                    clay_content DOUBLE,
                    nitrogen_washout_kg_ha DOUBLE,
                    percolation_mm DOUBLE,
                    percolation_april_august DOUBLE,
                    percolation_sept_march DOUBLE,
                    percolation_april_august_prev DOUBLE,
                    percolation_sept_march_prev DOUBLE,
                    uncertainty_pct DOUBLE,
                    data_quality_score DOUBLE,
                    geometry_wkt VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

    def _aggressive_pipeline_cleanup(self) -> None:
        """Delegate to memory utils."""
        return self.memory_utils._aggressive_pipeline_cleanup()

    def _save_batched_results_to_gold(self) -> None:
        """Save final batched results to gold layer using shared GCS interface."""
        try:
            self.log.info("💾 Saving batched results to gold layer using shared GCS interface...")

            # Final validation
            final_count = self.conn.execute(
                "SELECT COUNT(*) FROM nles5_estimates_final_batched"
            ).fetchone()[0]

            if final_count == 0:
                self.log.error("❌ No batched results to save")
                return

            # Create gold table from batched results with explicit CREATE AS SELECT
            # This ensures proper materialization for subsequent COPY operations

            # 🔍 DIAGNOSTIC: Log temp file state before table operations
            temp_files_before = glob.glob(os.path.join(self.temp_dir, "duckdb_temp_storage_*.tmp"))
            self.log.info(f"🔍 BEFORE table creation: {len(temp_files_before)} temp files exist")
            for tf in temp_files_before[:5]:
                try:
                    size = os.path.getsize(tf) / (1024**2)
                    self.log.info(f"   - {os.path.basename(tf)}: {size:.2f} MB")
                except Exception:
                    self.log.warning(f"   - {os.path.basename(tf)}: FILE INACCESSIBLE")

            # 🔍 DIAGNOSTIC: Check if DuckDB connection is still valid
            try:
                test_result = self.conn.execute("SELECT 1").fetchone()
                self.log.info(f"🔍 DuckDB connection test: OK (result={test_result})")
            except Exception as e:
                self.log.error(f"🔍 DuckDB connection test: FAILED - {e}")

            self.conn.execute("DROP TABLE IF EXISTS nles5_nitrogen_estimates_gold")

            # 🔍 DIAGNOSTIC: Log temp file state after DROP
            temp_files_after_drop = glob.glob(
                os.path.join(self.temp_dir, "duckdb_temp_storage_*.tmp")
            )
            self.log.info(f"🔍 AFTER DROP: {len(temp_files_after_drop)} temp files exist")

            try:
                self.log.info(
                    f"🔍 BEFORE CREATE TABLE: Starting materialization of "
                    f"{final_count:,} records..."
                )
                self.conn.execute("""
                    CREATE TABLE nles5_nitrogen_estimates_gold AS
                    SELECT * FROM nles5_estimates_final_batched
                """)
                self.log.info("🔍 AFTER CREATE TABLE: Materialization successful")
            except Exception as create_error:
                # 🔍 DIAGNOSTIC: Capture detailed error information
                temp_files_on_error = glob.glob(
                    os.path.join(self.temp_dir, "duckdb_temp_storage_*.tmp")
                )
                self.log.error(
                    f"🔍 CREATE TABLE FAILED with {len(temp_files_on_error)} temp files present"
                )
                self.log.error(f"🔍 Error details: {create_error}")

                # Check if temp files mentioned in error exist
                error_str = str(create_error)
                if "Could not remove file" in error_str:
                    # Extract filename from error message
                    match = re.search(r'Could not remove file "([^"]+)"', error_str)
                    if match:
                        missing_file = match.group(1)
                        self.log.error(f"🔍 Missing file: {missing_file}")
                        self.log.error(f"🔍 File exists: {os.path.exists(missing_file)}")

                        # Check if file was in our list before
                        missing_basename = os.path.basename(missing_file)
                        was_present_before = any(missing_basename in tf for tf in temp_files_before)
                        self.log.error(
                            f"🔍 File was present before operation: {was_present_before}"
                        )

                raise

            # Clean up source table
            self.conn.execute("DROP TABLE IF EXISTS nles5_estimates_final_batched")

            # 🔍 DIAGNOSTIC: Log temp file state after all operations
            temp_files_final = glob.glob(os.path.join(self.temp_dir, "duckdb_temp_storage_*.tmp"))
            self.log.info(
                f"🔍 AFTER all table operations: {len(temp_files_final)} temp files exist"
            )

            # Log final statistics
            final_years = self.conn.execute("""
                SELECT DISTINCT year FROM nles5_nitrogen_estimates_gold ORDER BY year
            """).fetchall()

            self.log.info(f"✅ Created gold table with {final_count:,} NLES5 estimates")
            self.log.info(f"📅 Years processed: {[row[0] for row in final_years]}")

            # Save to GCS using shared GCS interface - main results table
            timestamp = self.date_pattern
            dataset_name = f"{self.config.dataset}_nitrogen_estimates"
            gcs_path = f"gold/{dataset_name}/{timestamp}/data.parquet"
            full_gcs_path = f"gs://{self.config.bucket}/{gcs_path}"

            # Export to local file first to avoid DuckDB temp file issues with GCS writes
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
                local_parquet = tmp_file.name

            try:
                # 🔍 DIAGNOSTIC: Check DuckDB temp settings before export
                temp_info = self.conn.execute(
                    "SELECT current_setting('temp_directory') as temp_dir"
                ).fetchone()
                self.log.info(f"🔍 DuckDB temp_directory: {temp_info[0]}")

                memory_limit = self.conn.execute(
                    "SELECT current_setting('memory_limit') as mem"
                ).fetchone()
                self.log.info(f"🔍 DuckDB memory_limit: {memory_limit[0]}")

                threads = self.conn.execute(
                    "SELECT current_setting('threads') as threads"
                ).fetchone()
                self.log.info(f"🔍 DuckDB threads: {threads[0]}")

                # 🔍 DIAGNOSTIC: Check for existing temp files
                temp_dir = temp_info[0]
                if temp_dir and os.path.exists(temp_dir):
                    temp_files = glob.glob(os.path.join(temp_dir, "duckdb_temp_storage_*.tmp"))
                    self.log.info(f"🔍 Existing temp files in {temp_dir}: {len(temp_files)}")
                    if temp_files:
                        self.log.warning(f"⚠️ Found {len(temp_files)} temp files before export")
                        for tf in temp_files[:5]:  # Show first 5
                            self.log.warning(f"   - {os.path.basename(tf)}")

                # 🔍 DIAGNOSTIC: Try to force checkpoint and disable temp file usage
                self.log.info("🔧 Attempting to checkpoint and optimize for export...")
                try:
                    # Force checkpoint to flush any pending writes
                    self.conn.execute("CHECKPOINT")
                    self.log.info("✓ Checkpoint completed")
                except Exception as e:
                    self.log.warning(f"Could not checkpoint: {e}")

                try:
                    # Force DuckDB to not use temp files during this operation
                    self.conn.execute("SET preserve_insertion_order=false")
                    self.log.info("✓ Disabled insertion order preservation")
                except Exception as e:
                    self.log.warning(f"Could not disable insertion order: {e}")

                # Export to local parquet file
                self.log.info(f"📝 Exporting {final_count:,} records to local parquet file...")
                self.log.info(f"📝 Target file: {local_parquet}")

                try:
                    self.conn.execute(f"""
                        COPY nles5_nitrogen_estimates_gold
                        TO '{local_parquet}'
                        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
                    """)
                except Exception as copy_error:
                    # DuckDB sometimes fails to cleanup temp files but the COPY succeeds
                    # Check if the output file was created despite the error
                    if "Could not remove file" in str(copy_error) and os.path.exists(local_parquet):
                        self.log.warning(
                            f"⚠️ DuckDB temp file cleanup warning (ignorable): {copy_error}"
                        )
                        self.log.info("✓ Export completed successfully despite cleanup warning")
                    else:
                        # Real error - re-raise
                        raise

                # 🔍 DIAGNOSTIC: Check if file was created
                if os.path.exists(local_parquet):
                    file_size = os.path.getsize(local_parquet) / (1024**2)
                    self.log.info(f"✓ Export file size: {file_size:.2f} MB")
                else:
                    self.log.error("❌ Export failed: file not created")
                    raise RuntimeError("COPY command did not create output file")

                # Upload local file to GCS using streaming
                self.log.info(f"☁️ Uploading to GCS: {full_gcs_path}")
                import shutil

                with (
                    open(local_parquet, "rb") as src,
                    self.gcs_access.fs.open(full_gcs_path, "wb") as dst,
                ):
                    shutil.copyfileobj(src, dst)

                self.log.info(f"✅ Saved batched results to {full_gcs_path}")
            finally:
                # Clean up local temp file
                if os.path.exists(local_parquet):
                    os.remove(local_parquet)
                    self.log.info("🧹 Cleaned up local temp file")

            # Add UUIDs via join before validation
            self._add_field_uuids_to_gold_table("nles5_nitrogen_estimates_gold")

            # Re-collect field IDs/UUIDs after UUID join to update validation
            self.log.info("🔍 Re-collecting field IDs after UUID join...")
            self.field_id_validator.collect_field_ids_after_processing()

            # Perform final validation
            self._validate_nles5_estimates()

        except Exception as e:
            self.log.error(f"❌ Failed to save batched results: {e}")
            raise

    def _add_field_uuids_to_gold_table(self, table_name: str) -> None:
        """
        Verify field UUIDs are present in the final results table.

        field_uuid is now preserved throughout the pipeline from the source
        agricultural fields data, eliminating the need for an expensive JOIN
        operation at the end.

        Args:
            table_name: Name of the table to verify UUIDs in
        """
        try:
            self.log.info(
                f"✅ Verifying field UUIDs in {table_name} "
                f"(no JOIN needed - UUIDs preserved throughout pipeline)..."
            )

            # Simple verification that UUIDs are present
            total_rows = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            uuid_rows = self.conn.execute(
                f"SELECT COUNT(*) FROM {table_name} WHERE field_uuid IS NOT NULL"
            ).fetchone()[0]
            coverage_pct = (uuid_rows / total_rows * 100) if total_rows > 0 else 0

            self.log.info(
                f"📊 UUID Coverage: {uuid_rows:,}/{total_rows:,} rows ({coverage_pct:.1f}%)"
            )

            if coverage_pct < 95:
                self.log.warning(
                    f"⚠️ Low UUID coverage ({coverage_pct:.1f}%) - "
                    f"some fields may not have UUIDs from source data"
                )
            else:
                self.log.info(f"✅ UUID coverage is good ({coverage_pct:.1f}%)")

        except Exception as e:
            self.log.error(f"❌ Failed to verify field UUIDs in {table_name}: {e}")
            raise

    def _add_field_uuids_to_final_results(self) -> None:
        """Verify field UUIDs are present in final NLES5 estimates table.

        UUIDs are preserved throughout pipeline.
        """
        try:
            # Check if we have the main estimates table
            if (
                self.conn.execute(
                    "SELECT COUNT(*) FROM information_schema.tables "
                    "WHERE table_name = 'nles5_nitrogen_estimates_gold'"
                ).fetchone()[0]
                > 0
            ):
                self._add_field_uuids_to_gold_table("nles5_nitrogen_estimates_gold")

                # Re-collect field IDs/UUIDs after verification
                self.log.info("🔍 Re-collecting field IDs after UUID verification...")
                self.field_id_validator.collect_field_ids_after_processing()
            else:
                self.log.warning("⚠️ No nles5_nitrogen_estimates_gold table found for UUID addition")
        except Exception as e:
            self.log.error(f"❌ Failed to add field UUIDs to final results: {e}")
            raise

    def _load_required_silver_datasets_for_batch(
        self, silver_data: dict[str, Any] | None, batch_years: list[int]
    ) -> dict[str, str]:
        """Delegate to data loader."""
        return self.data_loader._load_required_silver_datasets_for_batch(silver_data, batch_years)

    def _load_agricultural_fields_data_for_batch(
        self,
        silver_data: dict[str, Any] | None,
        batch_years: list[int],
        loaded_tables: dict[str, str],
    ) -> str:
        """Load agricultural fields data for specific batch years."""
        try:
            # Delegate to the data loader which has the correct implementation
            return self.data_loader._load_agricultural_fields_data_for_batch(
                silver_data, batch_years, loaded_tables
            )
        except Exception as e:
            self.log.error(f"❌ Failed to load agricultural fields for batch {batch_years}: {e}")
            raise

    def _process_nles5_target_year_by_target_year_for_batch(
        self, loaded_tables: dict[str, Any], batch_years: list[int]
    ) -> str:
        """Delegate to pipeline orchestrator."""
        return self.pipeline_orchestrator._process_nles5_target_year_by_target_year_for_batch(
            loaded_tables, batch_years
        )

    def _combine_yearly_fvm_data(self, yearly_tables: dict[int, str]) -> str:
        """Delegate to data loader."""
        return self.data_loader._combine_yearly_fvm_data(yearly_tables)

    def _load_farm_data(self) -> str | None:
        """Load farm-level gødningsregnskab data for enhanced NLES5 calculations."""
        # Determine years needed for farm data
        if self.config.target_years:
            target_years = self.config.target_years
        else:
            # Auto-discover from available data
            available_years = self.data_loader._get_available_fvm_marker_years()
            if self.config.max_years_to_process:
                target_years = sorted(available_years)[-self.config.max_years_to_process :]
            else:
                target_years = available_years

        # Delegate to data loader with required years
        return self.data_loader._load_farm_data(target_years)

    def _load_required_silver_datasets(self, silver_data: dict[str, Any] | None) -> dict[str, str]:
        """Delegate to data loader."""
        return self.data_loader._load_required_silver_datasets(silver_data)

    def _load_agricultural_fields_data(self, silver_data: dict[str, Any] | None) -> str:
        """Delegate to data loader."""
        return self.data_loader._load_agricultural_fields_data(silver_data)
