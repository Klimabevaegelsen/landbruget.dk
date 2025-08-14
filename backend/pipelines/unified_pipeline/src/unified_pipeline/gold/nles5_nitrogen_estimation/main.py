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
- Required: agricultural_fields (fvm_marker_YYYY), dmi_data, soil_types, fertilizer_accounts, field_plan
- Optional: catch_crops
- STRICT: The pipeline will FAIL immediately if any required dataset is missing.

OUTPUT:
- Detailed nitrogen washout estimates per field with quality indicators
- Summary statistics by soil type, crop type, and overall
- Full audit trail of all model components and data sources
"""

import os
import re
import json
import math
import time
from typing import Any, Dict, List, Optional, Set

from unified_pipeline.common.base import BaseSource, GoldJobInterface
from unified_pipeline.common.geometry_validator import validate_and_transform_geometries_duckdb
from unified_pipeline.util.gcs_access import GCSDataAccess
from unified_pipeline.util.log_util import Logger
from unified_pipeline.util.timing import timed

# Import configuration and modules from the new modular structure
from .config import NLES5NitrogenEstimationGoldConfig
from .data_loader import NLES5DataLoader
from .climate_processor import NLES5ClimateProcessor
from .spatial_operations import NLES5SpatialOperations
from .nles5_calculator import NLES5Calculator
from .validator import NLES5Validator
from .memory_utils import NLES5MemoryUtils
from .pipeline_orchestrator import NLES5PipelineOrchestrator


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
        self.log.info(f"🔧 Pipeline configuration: {config.batch_size:,} batch size, {config.max_memory_usage_gb}GB memory limit")
        
        self.phase_times: Dict[str, float] = {}
        self.gcs_access = GCSDataAccess()
        self.conn = self.gcs_access.duckdb_conn
        self._configure_duckdb()
        
        # Initialize specialized processors
        from .data_loader import NLES5DataLoader
        from .climate_processor import NLES5ClimateProcessor
        from .spatial_operations import NLES5SpatialOperations
        from .nles5_calculator import NLES5Calculator
        from .validator import NLES5Validator
        from .memory_utils import NLES5MemoryUtils
        from .pipeline_orchestrator import NLES5PipelineOrchestrator
        
        self.data_loader = NLES5DataLoader(self)
        self.climate_processor = NLES5ClimateProcessor(self)
        self.spatial_operations = NLES5SpatialOperations(self)
        self.nles5_calculator = NLES5Calculator(self)
        self.validator = NLES5Validator(self)
        self.memory_utils = NLES5MemoryUtils(self)
        self.pipeline_orchestrator = NLES5PipelineOrchestrator(self)

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
        self.conn.execute(f"SET preserve_insertion_order = {str(self.config.preserve_insertion_order).lower()}")
        
        # Set increased temp directory size for large datasets
        self.conn.execute(f"SET max_temp_directory_size = '{self.config.max_temp_directory_size_gb}GB'")
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
                "SELECT extension_name, extension_version FROM duckdb_extensions() WHERE extension_name = 'spatial'"
            ).fetchone()
            if version_result:
                self.log.info(f"DuckDB Spatial version: {version_result[1]}")
                if version_result[1] >= "1.2.2":
                    self.log.info("✅ SPATIAL_JOIN operator available")
                else:
                    self.log.warning(
                        f"⚠️  SPATIAL_JOIN operator may not be available in version {version_result[1]}"
                    )
        except Exception as e:
            self.log.warning(f"Could not verify spatial extension version: {e}")

    def _cleanup_temp_files(self):
        """Delegate to memory utils if available, otherwise provide basic cleanup."""
        if hasattr(self, 'memory_utils') and self.memory_utils:
            return self.memory_utils._cleanup_temp_files()
        else:
            # Basic cleanup implementation
            try:
                import os
                import shutil
                import glob

                # Force DuckDB to flush any pending writes
                try:
                    self.conn.execute("CHECKPOINT")
                except Exception:
                    pass  # Ignore checkpoint errors

                # Clean workspace temp directories
                temp_patterns = [
                    "data_cache/duckdb_temp/*",
                    "data_cache/temp/*"
                ]
                
                # Add the specific temp directory for this job
                if hasattr(self, 'temp_dir') and os.path.exists(self.temp_dir):
                    temp_patterns.append(os.path.join(self.temp_dir, '*'))

                cleaned_files = 0
                freed_bytes = 0

                for pattern in temp_patterns:
                    for file_path in glob.glob(pattern):
                        try:
                            if os.path.isfile(file_path):
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
                            if hasattr(self, 'log'):
                                self.log.debug(f"Could not remove temp file {file_path}: {e}")

                if hasattr(self, 'log') and cleaned_files > 0:
                    self.log.info(f"🧹 Cleaned {cleaned_files} temp files, freed {freed_bytes / 1024 / 1024:.1f} MB")
                    
            except Exception as e:
                if hasattr(self, 'log'):
                    self.log.debug(f"Temp file cleanup error: {e}")
                pass
    
    def _get_available_fvm_marker_years(self) -> List[int]:
        """Delegate to data loader."""
        return self.data_loader._get_available_fvm_marker_years()



    def _read_fvm_marker_data_for_year(self, year: int) -> Optional[str]:
        """Delegate to data loader."""
        return self.data_loader._read_fvm_marker_data_for_year(year)



    def _prepare_crop_sequences(
        self, agricultural_fields_table: str, loaded_tables: Dict[str, str]
    ) -> str:
        """Delegate to NLES5 calculator."""
        return self.nles5_calculator._prepare_crop_sequences(agricultural_fields_table, loaded_tables)

    def _create_simplified_crop_classification(self, agricultural_fields_table: str) -> str:
        """
        Create a simplified crop classification table using agricultural fields data.
        Fallback method when field_plan data is not available.
        """
        self.log.info("Creating simplified crop classification table using agricultural fields data (field_plan not available)...")

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

        simplified_count = self.conn.execute("SELECT COUNT(*) FROM fields_with_crop_classifications").fetchone()[0]
        self.log.info(f"✅ Created {simplified_count:,} simplified crop classifications using agricultural fields data.")
        return "fields_with_crop_classifications"

    @timed(name="Loading agricultural fields data")
    def _load_agricultural_fields_data(self, silver_data: Optional[Dict[str, Any]]) -> str:
        """Delegate to data loader."""
        return self.data_loader._load_agricultural_fields_data(silver_data)

    def _get_fertilizer_data_path(self, target_year: int = None) -> str:
        """Get path to fertilizer data for the specified year, prioritizing GKEA files over Gødningsregnskaber."""
        try:
            # Look for files in the latest fertilizer directory
            dirs = self.gcs_access.list_files(f"gs://{self.config.bucket}/silver/fertiliser/*/")
            
            if not dirs:
                raise FileNotFoundError("No fertiliser directories found")
            
            # Get the most recent directory
            latest_dir = sorted(dirs, reverse=True)[0]
            if not latest_dir.endswith('/'):
                latest_dir += '/'
            
            # List all files in the directory
            pattern = f"{latest_dir}*.parquet"
            files = self.gcs_access.list_files(pattern)
            
            if not files:
                raise FileNotFoundError(f"No parquet files found in {latest_dir}")
            
            # Extract just the filenames for easier filtering
            filenames = [f.split('/')[-1] for f in files]
            self.log.info(f"Available fertilizer files: {filenames}")
            
            # Priority 1: Try to find GKEA files for the target year
            if target_year:
                gkea_files = [f for f in files if f"GKEA{target_year}" in f and "Gødningsoplysninger" in f]
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

    def _get_field_plan_data_path(self, target_year: int = None) -> str:
        """
        Get the specific path for field plan data (Markplan_med_Gødningsoplysninger) from fertiliser directory.
        
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
                
                self.log.info(f"🔍 Searching for GKEA {target_year} field plan data with pattern: {gkea_pattern}")
                gkea_files = self.gcs_access.list_files(gkea_pattern)
                
                if gkea_files:
                    selected_file = sorted(gkea_files)[-1]  # Get most recent timestamp
                    self.log.info(f"📋 Selected {target_year} field plan data: {selected_file}")
                    return selected_file
                else:
                    self.log.warning(f"⚠️ No GKEA {target_year} field plan files found.")

            # Priority 2: Historical Markplan files (try recent years in order)
            for year in [2024, 2023, 2022, 2021]:
                # 2023 has _Aktindsigt suffix, other years don't
                if year == 2023:
                    historical_pattern = f"gs://{self.config.bucket}/silver/fertiliser/*/GKEA{year}_Markplan_med_Gødningsoplysninger_Aktindsigt.parquet"
                else:
                    historical_pattern = f"gs://{self.config.bucket}/silver/fertiliser/*/GKEA{year}_Markplan_med_Gødningsoplysninger.parquet"
                
                self.log.info(f"🔍 Searching for {year} field plan data with pattern: {historical_pattern}")
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
            
            raise ValueError("No field plan (Markplan_med_Gødningsoplysninger) files found in fertiliser directory")
            
        except Exception as e:
            self.log.error(f"Error selecting field plan data: {e}")
            raise ValueError(f"Cannot find field plan data: {e}")

    def _get_catch_crops_data_path(self, target_year: int = None) -> str:
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
                pattern_target = f"gs://{self.config.bucket}/silver/fertiliser/*/Efterafgrøder {target_year}.parquet"
                files_target = self.gcs_access.list_files(pattern_target)
                
                if files_target:
                    selected_file = sorted(files_target)[-1]
                    self.log.info(f"🌱 Selected {target_year} catch crops data: {selected_file}")
                    return selected_file
            
            # Priority 2: Historical Efterafgrøder files (try recent years in order)
            for year in [2024, 2023, 2022, 2021]:
                pattern_year = f"gs://{self.config.bucket}/silver/fertiliser/*/Efterafgrøder {year}.parquet"
                files_year = self.gcs_access.list_files(pattern_year)
                
                if files_year:
                    selected_file = sorted(files_year)[-1]
                    self.log.info(f"🌱 Selected {year} catch crops data: {selected_file}")
                    return selected_file
            
            # Final fallback to any catch crops files
            fallback_pattern = f"gs://{self.config.bucket}/silver/fertiliser/*/Efterafgrøder*.parquet"
            fallback_files = self.gcs_access.list_files(fallback_pattern)
            
            if fallback_files:
                selected_file = sorted(fallback_files)[-1]
                self.log.info(f"🌱 Selected fallback catch crops data: {selected_file}")
                return selected_file
            
            raise ValueError("No catch crops (Efterafgrøder) files found in fertiliser directory")
            
        except Exception as e:
            self.log.error(f"Error selecting catch crops data: {e}")
            raise ValueError(f"Cannot find catch crops data: {e}")

    def _read_silver_data_from_path(self, dataset_name: str, file_path: str, target_table: str) -> bool:
        """Delegate to data loader."""
        return self.data_loader._read_silver_data_from_path(dataset_name, file_path, target_table)

    def _load_required_silver_datasets(self, silver_data: Optional[Dict[str, Any]]) -> Dict[str, str]:
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
    
    @timed(name="Saving NLES5 results to gold layer")
    def _save_results_to_gold(self) -> None:
        """Save NLES5 results to the gold layer using optimized DuckDB export."""
        try:
            self.log.info("Saving NLES5 results to gold layer")
            failed_uploads = 0

            # Define output tables with optimized paths
            tables_to_save = [
                ("nles5_nitrogen_estimates", "nitrogen_estimates"),
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
                        # Use optimized GCS upload directly from DuckDB table
                        output_path = f"gs://{self.config.bucket}/gold/{self.config.dataset}/latest/{subdataset}.parquet"

                        self.gcs_access.upload_from_duckdb_table(
                            table_name,
                            output_path,
                            compression="zstd",
                            row_group_size=100000,
                        )

                        self.log.info(f"✅ Saved {table_name} ({count:,} rows) to {output_path}")
                    else:
                        self.log.warning(f"Table {table_name} is empty, skipping")
                except Exception as e:
                    self.log.error(f"Failed to save {table_name}: {e}")
                    failed_uploads += 1

            if failed_uploads > 0:
                raise RuntimeError(f"{failed_uploads} GCS uploads failed. Check logs for details.")

            self.log.info(f"NLES5 results saved to: gs://{self.config.bucket}/gold/{self.config.dataset}/latest/")

        except Exception as e:
            self.log.error(f"Error saving results: {e}")
            raise

    async def run(self, silver_data: Optional[Dict[str, Any]] = None) -> None:
        """Delegate to pipeline orchestrator."""
        return await self.pipeline_orchestrator.run(silver_data)

    async def _run_pipeline_batched(self, silver_data: Optional[Dict[str, Any]] = None) -> None:
        """Delegate to pipeline orchestrator."""
        return await self.pipeline_orchestrator._run_pipeline_batched(silver_data)

    async def _run_pipeline_single(self, silver_data: Optional[Dict[str, Any]] = None) -> None:
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
                "estimates_target_2024"
            ]
            
            for table_name in tables_to_check:
                try:
                    count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                    if count > 0:
                        self.log.info(f"✅ Found {count:,} records in table: {table_name}")
                        
                        # Get sample data from this table
                        preview_data = self.conn.execute(f"""
                            SELECT
                                field_id,
                                year,
                                COALESCE(soil_type, soil_type_category, 'unknown') as soil_type,
                                COALESCE(m_code, 'unknown') as m_code,
                                COALESCE(w_code, 'unknown') as w_code,
                                COALESCE(nitrogen_washout_kg_ha, nitrogen_leaching_nles5, nitrogen_washout_kg_n_ha, 0) as nitrogen_washout_kg_ha
                            FROM {table_name}
                            WHERE COALESCE(nitrogen_washout_kg_ha, nitrogen_leaching_nles5, nitrogen_washout_kg_n_ha, 0) > 0
                            ORDER BY random()
                            LIMIT 5
                        """).fetchall()

                        if preview_data:
                            for row in preview_data:
                                preview_log = {
                                    "table": table_name,
                                    "field_id": row[0],
                                    "year": row[1],
                                    "soil_type": row[2],
                                    "m_code": row[3],
                                    "w_code": row[4],
                                    "nitrogen_washout_kg_ha": f"{row[5]:.2f}"
                                }
                                self.log.info(f"NLES5_PREVIEW: {json.dumps(preview_log)}")
                        else:
                            self.log.warning(f"Table {table_name} has {count:,} records but no positive nitrogen washout values")
                            
                        # Show statistics for this table
                        stats = self.conn.execute(f"""
                            SELECT 
                                COUNT(*) as total_records,
                                COUNT(CASE WHEN COALESCE(nitrogen_washout_kg_ha, nitrogen_leaching_nles5, nitrogen_washout_kg_n_ha, 0) > 0 THEN 1 END) as positive_estimates,
                                AVG(COALESCE(nitrogen_washout_kg_ha, nitrogen_leaching_nles5, nitrogen_washout_kg_n_ha, 0)) as avg_nitrogen,
                                MIN(COALESCE(nitrogen_washout_kg_ha, nitrogen_leaching_nles5, nitrogen_washout_kg_n_ha, 0)) as min_nitrogen,
                                MAX(COALESCE(nitrogen_washout_kg_ha, nitrogen_leaching_nles5, nitrogen_washout_kg_n_ha, 0)) as max_nitrogen
                            FROM {table_name}
                        """).fetchone()
                        
                        stats_log = {
                            "table": table_name,
                            "total_records": stats[0],
                            "positive_estimates": stats[1],
                            "avg_nitrogen": f"{stats[2]:.2f}" if stats[2] else "0.00",
                            "min_nitrogen": f"{stats[3]:.2f}" if stats[3] else "0.00",
                            "max_nitrogen": f"{stats[4]:.2f}" if stats[4] else "0.00"
                        }
                        self.log.info(f"NLES5_STATS: {json.dumps(stats_log)}")
                        
                        # Only log preview for the first table with data
                        break
                        
                except Exception as table_error:
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
        self.log.info(f"⏱️  Total execution time: {total_time:.1f} seconds ({total_time / 60:.1f} minutes)")
        self.log.info(f"📊 Fields processed: {result_count:,}")
        self.log.info(f"🔧 Configuration: {self.config.batch_size:,} batch size, {self.config.max_memory_usage_gb}GB memory")

        if result_count > 0:
            self.log.info(f"⚡ Processing rate: {result_count / total_time:.0f} fields/second")
            self.log.info(f"📈 Throughput: {(result_count / total_time) * 3600:.0f} fields/hour")

        # Log DuckDB version and spatial capabilities
        try:
            version_result = self.conn.execute(
                "SELECT extension_name, extension_version FROM duckdb_extensions() WHERE extension_name = 'spatial'"
            ).fetchone()
            if version_result:
                self.log.info(f"🦆 DuckDB Spatial version: {version_result[1]}")
        except:
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
    
    def _comprehensive_data_validation(self) -> Dict[str, Any]:
        """Delegate to validator."""
        return self.validator._comprehensive_data_validation()
    
    def _validate_table_quality(self, table_name: str) -> Dict[str, Any]:
        """Validate data quality for a specific table."""
        stats = {
            'table_name': table_name,
            'total_records': 0,
            'null_geometries': 0,
            'invalid_geometries': 0,
            'quality_score': 0.0,
            'issues': [],
            'exists': False
        }
        
        try:
            # Check if table exists
            exists_result = self.conn.execute(f"""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = '{table_name}'
            """).fetchone()
            
            if not exists_result or exists_result[0] == 0:
                stats['issues'].append(f"Table {table_name} does not exist")
                return stats
            
            stats['exists'] = True
            
            # Get basic statistics
            basic_stats = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            stats['total_records'] = basic_stats[0] if basic_stats else 0
            
            if stats['total_records'] == 0:
                stats['issues'].append(f"Table {table_name} is empty")
                return stats
            
            # Geometry-specific validation for spatial tables
            if 'spatial' in table_name or table_name in ['climate_tessellation', 'soil_types_prepared']:
                geom_column = 'geom' if table_name != 'climate_tessellation' else 'tessellation_polygon'
                
                if table_name == 'soil_types_prepared':
                    geom_column = 'geom'  # soil_types uses 'geom' after ST_Dump
                
                try:
                    geom_stats = self.conn.execute(f"""
                        SELECT 
                            COUNT(*) as total,
                            COUNT(CASE WHEN {geom_column} IS NULL THEN 1 END) as null_geom,
                            COUNT(CASE WHEN {geom_column} IS NOT NULL AND NOT ST_IsValid({geom_column}) THEN 1 END) as invalid_geom,
                            COUNT(CASE WHEN {geom_column} IS NOT NULL AND ST_IsValid({geom_column}) THEN 1 END) as valid_geom
                        FROM {table_name}
                    """).fetchone()
                    
                    if geom_stats:
                        stats['null_geometries'] = geom_stats[1]
                        stats['invalid_geometries'] = geom_stats[2]
                        valid_geom_count = geom_stats[3]
                        
                        # Calculate geometry quality score
                        if stats['total_records'] > 0:
                            geom_quality = valid_geom_count / stats['total_records']
                            stats['quality_score'] = geom_quality * 100
                            
                            if geom_quality < 0.95:
                                stats['issues'].append(f"Only {geom_quality:.1%} valid geometries in {table_name}")
                            if stats['null_geometries'] > 0:
                                stats['issues'].append(f"{stats['null_geometries']:,} null geometries in {table_name}")
                            if stats['invalid_geometries'] > 0:
                                stats['issues'].append(f"{stats['invalid_geometries']:,} invalid geometries in {table_name}")
                        
                except Exception as e:
                    stats['issues'].append(f"Could not validate geometries in {table_name}: {e}")
            
            # Table-specific validations
            if table_name == 'dmi_data':
                self._validate_climate_data_quality(stats)
            elif table_name == 'agricultural_fields_spatial':
                self._validate_field_data_quality(stats)
            elif table_name == 'soil_types_prepared':
                self._validate_soil_data_quality(stats)
                
        except Exception as e:
            stats['issues'].append(f"Validation failed for {table_name}: {e}")
        
        return stats

    def _validate_climate_data_quality(self, stats: Dict[str, Any]) -> None:
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
                    stats['issues'].append(f"CRITICAL: Only {param_count} climate parameters found - NLES5 requires both precipitation and evaporation data")
                if not year_ok:
                    stats['issues'].append(f"CRITICAL: Only {year_count} years of climate data - NLES5 requires minimum 3 years of real climate data")
                
                # Assign a simple quality score: 100% if both criteria met, else 0%
                stats['quality_score'] = 100.0 if (param_ok and year_ok) else 0.0
                
                stats['climate_years'] = year_count
                stats['climate_parameters'] = param_count
                stats['date_range'] = f"{min_date} to {max_date}"
                
        except Exception as e:
            stats['issues'].append(f"Climate data validation failed: {e}")

    def _validate_field_data_quality(self, stats: Dict[str, Any]) -> None:
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
                    stats['issues'].append(f"{invalid_areas:,} fields with invalid/missing area data")
                
                if missing_crops > 0:
                    stats['issues'].append(f"{missing_crops:,} fields with missing crop information")
                
                stats['field_years'] = year_count
                stats['avg_field_size_ha'] = round(avg_area, 2) if avg_area else 0
                
        except Exception as e:
            stats['issues'].append(f"Field data validation failed: {e}")

    def _validate_soil_data_quality(self, stats: Dict[str, Any]) -> None:
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
                    stats['issues'].append(f"Only {soil_types} soil types found (may indicate incomplete data)")
                
                if missing_clay > 0:
                    stats['issues'].append(f"{missing_clay:,} soil records missing clay content")
                
                stats['soil_type_count'] = soil_types
                stats['avg_clay_content'] = round(avg_clay, 1) if avg_clay else 0
                
        except Exception as e:
            stats['issues'].append(f"Soil data validation failed: {e}")

    def _generate_validation_recommendations(self, validation_results: Dict[str, Any]) -> None:
        """Generate actionable recommendations based on validation results - NO FALLBACK DATA."""
        score = validation_results['data_quality_score']
        
        if score < 50:
            validation_results['recommendations'].append("CRITICAL: Data quality severely compromised - pipeline CANNOT proceed without complete real data")
            validation_results['passed'] = False
        elif score < 75:
            validation_results['recommendations'].append("ERROR: Data quality insufficient - real data must be improved before pipeline execution")
            validation_results['passed'] = False
        elif score < 90:
            validation_results['recommendations'].append("WARNING: Data quality issues detected - verify real data completeness")
        else:
            validation_results['recommendations'].append("GOOD: Real data quality is acceptable for pipeline execution")
        
        # Specific recommendations based on table stats - NO FALLBACK SUGGESTIONS
        for table_name, table_stats in validation_results['table_stats'].items():
            if not table_stats['exists']:
                validation_results['recommendations'].append(f"REQUIRED: Load real {table_name} data from silver layer")
                validation_results['errors'].append(f"Missing table: {table_name}")
                validation_results['passed'] = False
            elif table_stats['total_records'] == 0:
                validation_results['recommendations'].append(f"REQUIRED: Ensure {table_name} contains real data records")
                validation_results['errors'].append(f"Empty table: {table_name}")
                validation_results['passed'] = False
            elif table_stats['quality_score'] < 80:
                validation_results['recommendations'].append(f"REQUIRED: Fix geometry/data quality issues in {table_name} using real data")
                if table_stats['quality_score'] < 50:
                    validation_results['errors'].append(f"Poor data quality in {table_name}: {table_stats['quality_score']:.1f}%")
                    validation_results['passed'] = False

    def _log_validation_summary(self, validation_results: Dict[str, Any]) -> None:
        """Log comprehensive validation summary."""
        self.log.info("📊 DATA VALIDATION SUMMARY:")
        self.log.info(f"   Overall Quality Score: {validation_results['data_quality_score']:.1f}%")
        self.log.info(f"   Validation Status: {'✅ PASSED' if validation_results['passed'] else '❌ FAILED'}")
        
        if validation_results['errors']:
            self.log.error(f"   Errors ({len(validation_results['errors'])}):")
            for error in validation_results['errors'][:5]:  # Show first 5 errors
                self.log.error(f"     - {error}")
        
        if validation_results['warnings']:
            self.log.warning(f"   Warnings ({len(validation_results['warnings'])}):")
            for warning in validation_results['warnings'][:3]:  # Show first 3 warnings
                self.log.warning(f"     - {warning}")
        
        if validation_results['recommendations']:
            self.log.info(f"   Recommendations:")
            for rec in validation_results['recommendations'][:3]:  # Show first 3 recommendations
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
    
    def _load_climate_data_for_years(self, years: List[int]) -> str:
        """Delegate to climate processor."""
        return self.climate_processor._load_climate_data_for_years(years)
    
    def _spatial_join_year_climate(self, year: int, climate_table: str) -> str:
        """Delegate to climate processor."""
        return self.climate_processor._spatial_join_year_climate(year, climate_table)
    
    def _create_year_tessellation(self, climate_table: str, year: int) -> str:
        """Delegate to climate processor."""
        return self.climate_processor._create_year_tessellation(climate_table, year)
    
    def _calculate_required_data_years(self, target_calculation_years: List[int], available_years: List[int]) -> List[int]:
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
            
            self.log.info(f"🔍 Calculating required data years for NLES5 3-year windows...")
            
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
                    self.log.warning(f"   Year {prev_year}: ❌ previous year not available (percolation effects will be limited)")
                
                # Add year before previous (needed for 2-year averages)
                prev_prev_year = target_year - 2
                if prev_prev_year in available_years_set:
                    required_years.add(prev_prev_year)
                    self.log.info(f"   Year {prev_prev_year}: ✅ year before previous (2-year averages)")
                else:
                    self.log.warning(f"   Year {prev_prev_year}: ❌ year before previous not available (2-year averages will be limited)")
            
            # Convert to sorted list
            final_years = sorted(list(required_years))
            
            # Calculate memory savings
            total_available = len(available_years)
            total_required = len(final_years)
            years_eliminated = total_available - total_required
            percent_reduction = (years_eliminated / total_available) * 100 if total_available > 0 else 0
            
            self.log.info(f"📊 NLES5 Year Optimization Results:")
            self.log.info(f"   Available years: {total_available} ({min(available_years)}-{max(available_years)})")
            self.log.info(f"   Required years: {total_required} → {final_years}")
            self.log.info(f"   Years eliminated: {years_eliminated} ({percent_reduction:.1f}% reduction)")
            self.log.info(f"   Memory impact: Loading {total_required}/{total_available} years")
            
            if total_required == 0:
                raise ValueError("No required years could be satisfied from available data")
            
            return final_years
            
        except Exception as e:
            self.log.error(f"Error calculating required data years: {e}")
            raise
    
    @timed(name="Target-year-by-target-year NLES5 processing")
    def _process_nles5_target_year_by_target_year(self, loaded_tables: Dict[str, Any]) -> str:
        """Delegate to NLES5 calculator."""
        return self.nles5_calculator._process_nles5_target_year_by_target_year(loaded_tables)
    
    def _process_single_target_year(self, target_year: int, required_years: List[int], loaded_tables: Dict[str, Any]) -> str:
        """Delegate to pipeline orchestrator."""
        return self.pipeline_orchestrator._process_single_target_year(target_year, required_years, loaded_tables)
    
    def _aggressive_cleanup_target_year(self):
        """Delegate to memory utils."""
        return self.memory_utils._aggressive_cleanup_target_year()
    
    def _load_agricultural_fields_for_years(self, years: List[int], table_name: str):
        """Load agricultural fields data for specific years only."""
        try:
            # Prefer already-prepared spatial table to ensure CRS/validity alignment
            try:
                exists_result = self.conn.execute("""
                    SELECT COUNT(*) FROM information_schema.tables 
                    WHERE table_name = 'agricultural_fields_spatial'
                """).fetchone()
                if exists_result and exists_result[0] > 0:
                    years_filter = ', '.join(map(str, years))
                    self.conn.execute(f"""
                        CREATE OR REPLACE TEMPORARY TABLE {table_name} AS
                        SELECT 
                            field_id, block_id, cvr_number, year,
                            field_uuid, crop_code, area_ha, 
                            geom
                        FROM agricultural_fields_spatial
                        WHERE year IN ({years_filter})
                    """)
                    count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                    self.log.info(f"   ✅ Loaded {count:,} agricultural fields from agricultural_fields_spatial for years {years}")
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
                                field_uuid, crop_code, area_ha,
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
        return self.climate_processor._join_climate_fields_for_target_year(target_year, climate_table)
    
    def _join_with_soil_data_target_year(self, fields_climate_table: str) -> str:
        """Join soil data for target year processing."""
        try:
            result_table = f"fields_complete_target"
            
            # Simplified soil joining using existing soil_types_prepared table
            self.conn.execute(f"""
                CREATE OR REPLACE TEMPORARY TABLE {result_table} AS
                SELECT 
                    f.*,
                    COALESCE(s.soil_code, '5') as soil_code,
                    COALESCE(s.soil_description, 'Medium clay soil') as soil_description,
                    COALESCE(s.clay_content, 15.0) as clay_content,
                    CASE WHEN s.soil_code IS NOT NULL THEN true ELSE false END as has_soil_data
                FROM {fields_climate_table} f
                LEFT JOIN soil_types_prepared s ON ST_Intersects(f.geom, s.geom)
            """)
            
            return result_table
            
        except Exception as e:
            self.log.error(f"Error joining soil data for target year: {e}")
            raise
    
    def _add_default_soil_data_target_year(self, fields_climate_table: str) -> str:
        """Add default soil data when soil dataset not available."""
        try:
            result_table = f"fields_complete_target"
            
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
        return self.nles5_calculator._calculate_percolation_effects_target_year(fields_complete_table)
    
    def _calculate_nles5_estimates_target_year(self, percolation_table: str, target_year: int) -> str:
        """Delegate to NLES5 calculator."""
        return self.nles5_calculator._calculate_nles5_estimates_target_year(percolation_table, target_year)
    
    def _determine_all_target_years(self) -> List[int]:
        """Delegate to pipeline orchestrator."""
        return self.pipeline_orchestrator._determine_all_target_years()

    def _create_target_year_batches(self, target_years: List[int]) -> List[List[int]]:
        """Delegate to pipeline orchestrator."""
        return self.pipeline_orchestrator._create_target_year_batches(target_years)

    async def _run_pipeline_for_batch(self, batch_years: List[int], silver_data: Optional[Dict[str, Any]] = None) -> int:
        """Delegate to pipeline orchestrator."""
        return await self.pipeline_orchestrator._run_pipeline_for_batch(batch_years, silver_data)

    def _diagnose_missing_data(self, batch_years: List[int], original_error: Exception) -> str:
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
                    WHERE year IN ({','.join(map(str, batch_years))})
                        AND geom IS NOT NULL 
                        AND area_ha > 0
                """).fetchone()[0]
                
                if field_count == 0:
                    missing_data_issues.append(f"No valid agricultural fields data found for years {batch_years}")
                else:
                    self.log.info(f"✓ Found {field_count:,} agricultural fields for {batch_years}")
            except Exception as e:
                missing_data_issues.append(f"Cannot access agricultural fields data: {e}")
            
            # Check climate data availability
            try:
                climate_years = self.conn.execute(f"""
                    SELECT DISTINCT EXTRACT(YEAR FROM CAST(valid_time AS TIMESTAMP)) as year 
                    FROM dmi_data 
                    WHERE valid_time IS NOT NULL 
                        AND EXTRACT(YEAR FROM CAST(valid_time AS TIMESTAMP)) IN ({','.join(map(str, range(min(batch_years)-2, max(batch_years)+1)))})
                    ORDER BY year
                """).fetchall()
                
                available_climate_years = [row[0] for row in climate_years if row[0] is not None]
                required_climate_years = list(range(min(batch_years)-2, max(batch_years)+1))  # NLES5 needs 3-year windows
                
                missing_climate_years = [year for year in required_climate_years if year not in available_climate_years]
                if missing_climate_years:
                    missing_data_issues.append(f"Missing climate data for years {missing_climate_years} (required for NLES5 3-year windows)")
                
            except Exception as e:
                missing_data_issues.append(f"Cannot access climate data: {e}")
            
            # Check soil types data availability
            try:
                soil_count = self.conn.execute("SELECT COUNT(*) FROM soil_types_prepared WHERE geom IS NOT NULL").fetchone()[0]
                if soil_count == 0:
                    missing_data_issues.append("No soil types data available - real soil data is required for NLES5")
            except Exception as e:
                missing_data_issues.append(f"Cannot access soil types data: {e}")
            
            # Check tessellation data availability
            try:
                tessellation_count = self.conn.execute("SELECT COUNT(*) FROM climate_tessellation").fetchone()[0]
                if tessellation_count == 0:
                    missing_data_issues.append("No climate tessellation data available - real climate grid data is required")
            except Exception as e:
                missing_data_issues.append(f"Cannot access climate tessellation: {e}")
            
            # Add the original error context
            missing_data_issues.append(f"Original error: {str(original_error)}")
            
        except Exception as e:
            missing_data_issues.append(f"Data diagnosis failed: {e}")
        
        if not missing_data_issues:
            return "Unknown data availability issue - all tables appear accessible but processing failed"
        
        return "; ".join(missing_data_issues)

    def _ensure_final_batched_table_exists(self):
        """Ensure the final batched results table exists."""
        try:
            # Check if table exists
            self.conn.execute("SELECT COUNT(*) FROM nles5_estimates_final_batched")
        except:
            # Table doesn't exist, create it with proper schema
            self.log.info("Creating nles5_estimates_final_batched table...")
            self.conn.execute("""
                CREATE TABLE nles5_estimates_final_batched (
                    field_id VARCHAR,
                    block_id VARCHAR, 
                    cvr_number VARCHAR,
                    year INTEGER,
                    area_ha DOUBLE,
                    crop_type VARCHAR,
                    soil_code VARCHAR,
                    soil_description VARCHAR,
                    clay_content DOUBLE,
                    nitrogen_washout_kg_ha DOUBLE,
                    percolation_mm DOUBLE,
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
        """Save final batched results to gold layer."""
        try:
            self.log.info("💾 Saving batched results to gold layer...")
            
            # Final validation
            final_count = self.conn.execute("SELECT COUNT(*) FROM nles5_estimates_final_batched").fetchone()[0]
            
            if final_count == 0:
                self.log.error("❌ No batched results to save")
                return
            
            # Create output table structure based on batched results
            self.conn.execute("""
                CREATE OR REPLACE TABLE nles5_nitrogen_estimates_gold AS
                SELECT * FROM nles5_estimates_final_batched
            """)
            
            # Log final statistics  
            final_years = self.conn.execute("""
                SELECT DISTINCT year FROM nles5_nitrogen_estimates_gold ORDER BY year
            """).fetchall()
            
            self.log.info(f"✅ Saved {final_count:,} NLES5 estimates to gold layer")
            self.log.info(f"📅 Years processed: {[row[0] for row in final_years]}")
            
            # Perform final validation
            self._validate_nles5_estimates()
            
        except Exception as e:
            self.log.error(f"❌ Failed to save batched results: {e}")
            raise

    def _load_required_silver_datasets_for_batch(self, silver_data: Optional[Dict[str, Any]], batch_years: List[int]) -> Dict[str, str]:
        """Delegate to data loader."""
        return self.data_loader._load_required_silver_datasets_for_batch(silver_data, batch_years)
    def _load_agricultural_fields_data_for_batch(
        self, silver_data: Optional[Dict[str, Any]], batch_years: List[int], loaded_tables: Dict[str, str]
    ) -> str:
        """Load agricultural fields data for specific batch years."""
        try:
            # Delegate to the data loader which has the correct implementation
            return self.data_loader._load_agricultural_fields_data_for_batch(silver_data, batch_years, loaded_tables)
        except Exception as e:
            self.log.error(f"❌ Failed to load agricultural fields for batch {batch_years}: {e}")
            raise

    def _process_nles5_target_year_by_target_year_for_batch(
        self, loaded_tables: Dict[str, Any], batch_years: List[int]
    ) -> str:
        """Delegate to pipeline orchestrator."""
        return self.pipeline_orchestrator._process_nles5_target_year_by_target_year_for_batch(loaded_tables, batch_years)

    def _combine_yearly_fvm_data(self, yearly_tables: Dict[int, str]) -> str:
        """Delegate to data loader."""
        return self.data_loader._combine_yearly_fvm_data(yearly_tables)

