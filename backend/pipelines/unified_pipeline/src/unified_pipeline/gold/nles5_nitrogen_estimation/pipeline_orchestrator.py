"""
NLES5 Pipeline Orchestrator Module

This module handles the high-level pipeline orchestration logic for the NLES5 nitrogen estimation system.
It manages async execution, batching strategies, target year processing, and overall pipeline coordination.

Key responsibilities:
- Main pipeline entry point (run)
- Batched vs single pipeline execution strategies
- Target year batch management
- Individual batch processing
- Single target year processing
- Memory management between batches
"""

import os
import time
from typing import Any, Dict, List, Optional

from unified_pipeline.util.timing import timed


class NLES5PipelineOrchestrator:
    """Pipeline orchestrator for NLES5 nitrogen estimation processing."""
    
    def __init__(self, processor):
        """Initialize with reference to main processor for access to all dependencies."""
        self.processor = processor
        self.config = processor.config
        self.log = processor.log
        self.conn = processor.conn
        self.gcs_access = processor.gcs_access

    async def run(self, silver_data: Optional[Dict[str, Any]] = None) -> None:
        """Run production-optimized NLES5 nitrogen estimation with real climate data."""
        import time
        start_time = time.time()
        self.processor._cleanup_temp_files()

        try:
            self.log.info("🚀 Starting NLES5 nitrogen estimation with test configuration")
            self.log.info(f"🔧 Configuration: {self.config.batch_size:,} batch size, {self.config.max_memory_usage_gb}GB memory limit")

            # Log test configuration
            if self.config.test_bounds:
                self.log.info(f"🌍 Geographic test area: {self.config.test_bounds} (Small Aarhus area)")
            else:
                self.log.info("🌍 Processing entire Denmark")

            if self.config.max_years_to_process:
                self.log.info(f"📅 Years limit: {self.config.max_years_to_process} years (for disk space management)")
            else:
                self.log.info("📅 Processing all available years")

            # Check if pipeline-level batching is enabled
            if self.config.enable_pipeline_batching:
                self.log.info(f"🔄 Pipeline batching enabled: {self.config.target_year_batch_size} years per batch")
                await self._run_pipeline_batched(silver_data)
            else:
                self.log.info("🔄 Running single pipeline execution for all target years")
                await self._run_pipeline_single(silver_data)

        except Exception as e:
            self.log.error(f"NLES5 pipeline failed: {e}")
            raise
        finally:
            # Final cleanup
            self.processor._cleanup_temp_files()
            total_time = time.time() - start_time
            self.log.info(f"🏁 NLES5 pipeline completed in {total_time:.1f} seconds")

    async def _run_pipeline_batched(self, silver_data: Optional[Dict[str, Any]] = None) -> None:
        """Run pipeline with batching around target years for maximum memory efficiency."""
        import time
        
        # Determine all target years
        all_target_years = self._determine_all_target_years()
        
        # Split into batches
        target_year_batches = self._create_target_year_batches(all_target_years)
        
        self.log.info(f"🎯 PIPELINE BATCHING STRATEGY:")
        self.log.info(f"   Total target years: {len(all_target_years)} → {all_target_years}")
        self.log.info(f"   Batch size: {self.config.target_year_batch_size} years per batch")
        self.log.info(f"   Number of batches: {len(target_year_batches)}")
        self.log.info(f"   Batches: {target_year_batches}")
        self.log.info(f"   💾 Maximum memory footprint: {self.config.target_year_batch_size + 2} years (batch + 2 previous)")
        
        # NOTE: Comprehensive data validation will run after silver data loading in each batch
        
        # Initialize final results table with proper schema
        self.conn.execute("""
            CREATE OR REPLACE TABLE nles5_estimates_final_batched (
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
        
        total_fields_processed = 0
        
        # Process each batch independently
        for batch_num, batch_years in enumerate(target_year_batches, 1):
            batch_start_time = time.time()
            
            self.log.info(f"")
            self.log.info(f"🔄 ========== PIPELINE BATCH {batch_num}/{len(target_year_batches)} ==========")
            self.log.info(f"📅 Processing target years: {batch_years}")
            self.log.info(f"💾 Memory before batch: {self.processor._get_memory_usage():.1f}GB")
            
            # NOTE: Connection re-initialization disabled to maintain data consistency
            # Re-initializing connections between batches breaks table visibility
            # If memory issues occur, use chunked processing within single connection instead
            
            # Run complete pipeline for this batch
            batch_results = await self._run_pipeline_for_batch(batch_years, silver_data)
            
            # Append batch results to final table
            if batch_results > 0:
                total_fields_processed += batch_results
                self.log.info(f"✅ Batch {batch_num} completed: {batch_results:,} fields processed")
            else:
                self.log.warning(f"⚠️ Batch {batch_num} produced no results")
            
            # Aggressive cleanup between batches
            self.log.info(f"🧹 Cleaning up batch {batch_num}...")
            self.processor._aggressive_pipeline_cleanup()
            
            batch_time = time.time() - batch_start_time
            memory_after = self.processor._get_memory_usage()
            self.log.info(f"✅ Batch {batch_num} completed in {batch_time:.1f}s (Memory: {memory_after:.1f}GB)")
            
        # Final validation
        try:
            final_count = self.conn.execute("SELECT COUNT(*) FROM nles5_estimates_final_batched").fetchone()[0]
        except Exception:
            self.log.warning("⚠️ Final table missing - creating empty table")
            self.processor._ensure_final_batched_table_exists()
            final_count = 0
            
        self.log.info(f"")
        self.log.info(f"🎯 PIPELINE BATCHING SUMMARY:")
        self.log.info(f"   Batches processed: {len(target_year_batches)}")
        self.log.info(f"   Total fields: {final_count:,}")
        self.log.info(f"   Average per batch: {final_count // len(target_year_batches):,} fields")
        
        if final_count == 0:
            self.log.error("❌ No NLES5 estimates generated across all batches")
            return
            
        # Log preview of final batched results
        self.processor._log_nles5_results_preview()
            
        # Save final batched results
        self.processor._save_batched_results_to_gold()

    async def _run_pipeline_single(self, silver_data: Optional[Dict[str, Any]] = None) -> None:
        """Run single pipeline execution for all target years (original approach)."""
        import time
        
        # Monitor memory usage
        self.processor._monitor_memory_usage("startup")

        # Phase 1: Load required silver datasets
        self.log.info("📥 Phase 1: Loading silver datasets...")
        phase_start = time.time()
        loaded_tables = self.processor._load_required_silver_datasets(silver_data)
        phase_time = time.time() - phase_start
        self.log.info(f"✅ Phase 1 completed in {phase_time:.1f} seconds")

        if len(loaded_tables) < 2:  # At least some datasets
            self.log.error("Insufficient data loaded - need at least climate data and other datasets")
            return

        # Phase 1.5: Load agricultural fields data (FVM marker data)
        self.log.info("📊 Loading agricultural fields data...")
        phase_start = time.time()
        agricultural_fields_table = self.processor._load_agricultural_fields_data(silver_data)
        phase_time = time.time() - phase_start
        self.log.info(f"✅ Agricultural fields loaded: {agricultural_fields_table}")
        self.log.info(f"✅ Phase 1.5 completed in {phase_time:.1f} seconds")

        # Phase 2: Process climate data to calculate percolation (MUST come before spatial tables)
        self.log.info("🌧️  Phase 2: Processing climate data for percolation...")
        phase_start = time.time()
        climate_table = self.processor._process_climate_data()
        phase_time = time.time() - phase_start
        self.log.info(f"✅ Phase 2 completed in {phase_time:.1f} seconds")

        # Phase 3: Create spatial tables and parameter lookup tables
        self.log.info("⚡ Phase 3: Creating spatial tables and parameters...")
        phase_start = time.time()
        self.processor._create_spatial_tables()
        self.processor._create_nles5_parameter_tables()
        phase_time = time.time() - phase_start
        self.log.info(f"✅ Phase 3 completed in {phase_time:.1f} seconds")
        self.processor._monitor_memory_usage("spatial_tables")

        # Phase 4: Prepare nitrogen input tables (fertilizer history)
        self.log.info("🧪 Phase 4: Preparing nitrogen input tables...")
        phase_start = time.time()
        self.processor._prepare_nitrogen_inputs_tables()
        phase_time = time.time() - phase_start
        self.log.info(f"✅ Phase 4 completed in {phase_time:.1f} seconds")
        self.processor._monitor_memory_usage("nitrogen_inputs")

        # Phase 5: Process NLES5 by target year
        self.log.info("🎯 Phase 5: NLES5 target-year-by-target-year processing...")
        phase_start = time.time()
        estimates_table = self.processor._process_nles5_target_year_by_target_year(loaded_tables)
        phase_time = time.time() - phase_start
        self.log.info(f"✅ Phase 5 completed in {phase_time:.1f} seconds")
        self.processor._monitor_memory_usage("nles5_estimates")

        # Phase 6: Validate final NLES5 estimates
        self.log.info("🔍 Phase 6: Validating NLES5 estimates...")
        phase_start = time.time()
        validation_passed = self.processor._validate_nles5_estimates()
        phase_time = time.time() - phase_start
        self.log.info(f"✅ Phase 6 validation completed in {phase_time:.1f} seconds")

        if not validation_passed:
            self.log.error("❌ NLES5 estimates validation failed")
            return

        # Phase 7: Calculate uncertainty estimates
        self.log.info("📊 Phase 7: Calculating uncertainty estimates...")
        phase_start = time.time()
        uncertainty_table = self.processor._calculate_uncertainty_estimates()
        phase_time = time.time() - phase_start
        self.log.info(f"✅ Phase 7 completed in {phase_time:.1f} seconds")

        # Phase 8: Final analysis and export
        self.log.info("📋 Phase 8: Final analysis and results export...")
        phase_start = time.time()
        
        # Log results preview
        self.processor._log_nles5_results_preview()
        
        # Analyze estimates distribution
        self.processor._analyze_estimates_distribution()
        
        # Save results to gold layer
        self.processor._save_results_to_gold()
        
        phase_time = time.time() - phase_start
        self.log.info(f"✅ Phase 8 completed in {phase_time:.1f} seconds")

        # Phase 9: Final validation (now that all processing is complete)
        self.log.info("🔍 Phase 9: Final validation of completed pipeline...")
        phase_start = time.time()
        try:
            validation_results = self.processor._comprehensive_data_validation()
            
            # Log validation warnings but don't fail the pipeline
            warnings = validation_results.get('warnings', [])
            if warnings:
                for warning in warnings:
                    self.log.warning(f"⚠️ {warning}")
            
            # Log data quality score if available
            quality_score = validation_results.get('data_quality_score')
            if quality_score is not None:
                self.log.info(f"📊 Final data quality score: {quality_score:.1f}%")
            
            # Store validation results
            self.processor._validation_results = validation_results
            
        except Exception as e:
            self.log.warning(f"⚠️ Final validation encountered issues: {e}")
            self.processor._validation_results = {}
        
        phase_time = time.time() - phase_start
        self.log.info(f"✅ Phase 9 validation completed in {phase_time:.1f} seconds")

        # Final memory cleanup
        self.processor._aggressive_memory_cleanup()
        final_memory = self.processor._get_memory_usage()
        self.log.info(f"💾 Final memory usage: {final_memory:.1f}GB")

    async def _run_pipeline_for_batch(self, batch_years: List[int], silver_data: Optional[Dict[str, Any]] = None) -> int:
        """Run complete pipeline for a single batch of target years."""
        import time
        
        try:
            batch_start = time.time()
            
            # Phase 1: Load silver datasets for this batch only
            self.log.info(f"📥 Batch Phase 1: Loading silver datasets for years {batch_years}...")
            phase_start = time.time()
            loaded_tables = self.processor._load_required_silver_datasets_for_batch(silver_data, batch_years)
            
            # Also load agricultural fields data for the batch
            self.log.info(f"📊 Loading agricultural fields data for batch years {batch_years}...")
            agricultural_fields_table = self.processor._load_agricultural_fields_data_for_batch(silver_data, batch_years, loaded_tables)
            if agricultural_fields_table:
                loaded_tables['agricultural_fields'] = agricultural_fields_table
                self.log.info(f"✅ Agricultural fields loaded: {agricultural_fields_table}")
            else:
                self.log.error(f"❌ Failed to load agricultural fields for batch {batch_years}")
            
            phase_time = time.time() - phase_start
            self.log.info(f"✅ Batch Phase 1 completed in {phase_time:.1f} seconds")

            if len(loaded_tables) < 2:
                self.log.error(f"Insufficient data loaded for batch {batch_years}")
                return 0

            # Phase 2: Process climate data for this batch
            self.log.info(f"🌧️  Batch Phase 2: Processing climate data...")
            phase_start = time.time()
            climate_table = self.processor._process_climate_data()
            phase_time = time.time() - phase_start
            self.log.info(f"✅ Batch Phase 2 completed in {phase_time:.1f} seconds")

            # Phase 3: Create spatial tables and parameters for this batch
            self.log.info(f"⚡ Batch Phase 3: Creating spatial tables...")
            phase_start = time.time()
            self.processor._create_spatial_tables()
            self.processor._create_nles5_parameter_tables()
            self.processor._prepare_nitrogen_inputs_tables()  # CRITICAL: Create fertilizer_history table
            phase_time = time.time() - phase_start
            self.log.info(f"✅ Batch Phase 3 completed in {phase_time:.1f} seconds")

            # Phase 3.5: Comprehensive data validation (now that tables exist)
            self.log.info(f"🔍 Batch Phase 3.5: Validating data quality for batch {batch_years}...")
            phase_start = time.time()
            validation_results = self.processor._comprehensive_data_validation()
            
            if not validation_results['passed']:
                error_msg = f"Batch {batch_years} validation failed - required real data is missing or invalid"
                self.log.error(f"❌ {error_msg}")
                for error in validation_results['errors']:
                    self.log.error(f"   - {error}")
                self.log.error("🚫 NO FALLBACK DATA WILL BE CREATED - batch requires complete real data")
                return 0  # Fail this batch but allow others to continue
            
            phase_time = time.time() - phase_start
            self.log.info(f"✅ Batch Phase 3.5 validation completed in {phase_time:.1f} seconds")
            self.log.info(f"📊 Data quality score: {validation_results['data_quality_score']:.1f}%")

            # Phase 4: Process NLES5 calculations for this batch
            self.log.info(f"🎯 Batch Phase 4: NLES5 calculations for years {batch_years}...")
            phase_start = time.time()
            estimates_table = self._process_nles5_target_year_by_target_year_for_batch(loaded_tables, batch_years)
            phase_time = time.time() - phase_start
            self.log.info(f"✅ Batch Phase 4 completed in {phase_time:.1f} seconds")

            # Get result count and append to final table
            if estimates_table:
                result_count = self.conn.execute(f"SELECT COUNT(*) FROM {estimates_table}").fetchone()[0]
                if result_count > 0:
                    # Ensure final batched table exists
                    self.processor._ensure_final_batched_table_exists()
                    
                    # Append batch results to final table
                    self.conn.execute(f"""
                        INSERT INTO nles5_estimates_final_batched
                        SELECT 
                            field_id,
                            block_id,
                            cvr_number,
                            year,
                            area_ha,
                            crop_type,
                            soil_code,
                            soil_description,
                            clay_content,
                            nitrogen_washout_kg_ha,
                            percolation_mm,
                            uncertainty_pct,
                            data_quality_score,
                            geometry_wkt,
                            CURRENT_TIMESTAMP as created_at
                        FROM {estimates_table}
                        WHERE nitrogen_washout_kg_ha IS NOT NULL
                    """)
                    
                    batch_time = time.time() - batch_start
                    self.log.info(f"✅ Batch {batch_years} results appended: {result_count:,} estimates in {batch_time:.1f}s")
                    return result_count
                else:
                    self.log.warning(f"⚠️ Batch {batch_years} produced empty results table")
                    return 0
            else:
                self.log.warning(f"⚠️ Batch {batch_years} failed to create estimates table")
                return 0
                
        except Exception as e:
            self.log.error(f"❌ Batch {batch_years} failed: {e}")
            
            # Attempt to diagnose the error
            try:
                diagnostic_msg = self.processor._diagnose_missing_data(batch_years, e)
                self.log.error(f"💡 Diagnosis: {diagnostic_msg}")
            except Exception as diag_e:
                self.log.error(f"Could not diagnose error: {diag_e}")
            
            return 0

    def _process_single_target_year(self, target_year: int, required_years: List[int], loaded_tables: Dict[str, Any]) -> str:
        """
        Process complete NLES5 calculations for a single target year using only its 3-year data window.
        
        This method loads only the minimal data needed for one target year and processes it completely
        before cleanup. This ensures memory usage never exceeds the footprint of 3 years of data.
        
        Args:
            target_year: The year to calculate NLES5 estimates for
            required_years: The 3-year window needed (current + 2 previous)
            loaded_tables: Reference datasets (soil, etc.)
            
        Returns:
            Table name containing NLES5 estimates for the target year
        """
        try:
            self.log.info(f"   🔄 Loading agricultural fields for 3-year window: {sorted(required_years)}")
            
            # Step 1: Load ONLY the agricultural fields data for required years
            table_name = f"target_year_{target_year}_estimates"
            self.processor._load_agricultural_fields_for_years(required_years, f"fields_target_{target_year}")
            
            # Step 2: Load climate data for required years
            self.log.info(f"   🌧️ Loading climate data for {len(required_years)} years...")
            climate_table = self.processor._load_climate_data_for_years(required_years)
            
            # Step 3: Process climate joining for target year (tessellation-based SPATIAL_JOIN)
            self.log.info(f"   🗺️ Climate-field joining for target year {target_year}...")
            fields_climate_table = self.processor._spatial_join_year_climate(target_year, climate_table)
            # Log join stats
            try:
                result_count = self.conn.execute(f"SELECT COUNT(*) FROM {fields_climate_table}").fetchone()[0]
                climate_matched = self.conn.execute(f"SELECT COUNT(*) FROM {fields_climate_table} WHERE total_percolation IS NOT NULL").fetchone()[0]
                self.log.info(f"   Year {target_year} spatial join: {result_count:,} fields, {climate_matched:,} with climate data")
            except Exception:
                pass
            
            # Step 4: Join with soil data  
            self.log.info(f"   🌱 Soil data joining for target year {target_year}...")
            if self.config.soil_types_dataset in loaded_tables:
                fields_complete_table = self.processor._join_with_soil_data_target_year(fields_climate_table)
            else:
                fields_complete_table = self.processor._add_default_soil_data_target_year(fields_climate_table)
            
            # Step 5: Calculate percolation effects
            self.log.info(f"   💧 Percolation effects for target year {target_year}...")
            percolation_table = self.processor._calculate_percolation_effects_target_year(fields_complete_table)
            
            # Step 6: Calculate final NLES5 estimates
            self.log.info(f"   🧪 NLES5 calculations for target year {target_year}...")
            estimates_table = self.processor._calculate_nles5_estimates_target_year(percolation_table, target_year)
            
            # Step 7: Validate results for this target year
            target_count = self.conn.execute(f"SELECT COUNT(*) FROM {estimates_table}").fetchone()[0]
            if target_count == 0:
                self.log.warning(f"   ⚠️ No NLES5 estimates produced for target year {target_year}")
                return None
            
            self.log.info(f"   ✅ NLES5 calculations completed for target year {target_year}: {target_count:,} estimates")
            return estimates_table
            
        except Exception as e:
            self.log.error(f"Error processing single target year {target_year}: {e}")
            raise

    def _determine_all_target_years(self) -> List[int]:
        """Determine all target years to be processed (without loading data)."""
        if self.config.target_years:
            target_years = self.config.target_years
            self.log.info(f"🎯 Target years specified in config: {target_years}")
        else:
            all_available_years = self.processor._get_available_fvm_marker_years()
            if self.config.max_years_to_process:
                target_years = sorted(all_available_years)[-self.config.max_years_to_process:]
                self.log.info(f"🎯 Auto-selected {len(target_years)} most recent target years: {target_years}")
            else:
                target_years = all_available_years
                self.log.info(f"🎯 Processing all available target years: {target_years}")
        
        if not target_years:
            raise ValueError("No target years available for processing")
            
        return sorted(target_years)

    def _create_target_year_batches(self, target_years: List[int]) -> List[List[int]]:
        """Split target years into batches for pipeline-level processing."""
        batch_size = self.config.target_year_batch_size
        batches = []
        
        for i in range(0, len(target_years), batch_size):
            batch = target_years[i:i + batch_size]
            batches.append(batch)
            
        return batches

    def _process_nles5_target_year_by_target_year_for_batch(
        self, loaded_tables: Dict[str, Any], batch_years: List[int]
    ) -> str:
        """Process NLES5 target year by target year for a specific batch."""
        try:
            self.log.info(f"🎯 Processing NLES5 for batch target years: {batch_years}")
            
            # Initialize final results table for this batch
            batch_table_name = f"nles5_estimates_batch_{batch_years[0]}_{batch_years[-1]}"
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE {batch_table_name} AS
                SELECT * FROM (VALUES 
                    ('dummy', 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
                ) AS t(field_id, year, nitrogen_washout, trend_effect, crop_effect, soil_effect, 
                       climate_effect, percolation_effect, uncertainty_estimate, confidence_level)
                WHERE false
            """)
            
            # Process each target year in the batch
            total_fields_processed = 0
            for target_num, target_year in enumerate(batch_years, 1):
                target_start_time = time.time()
                
                self.log.info(f"🎯 Processing target year {target_num}/{len(batch_years)}: {target_year}")
                
                # Calculate 3-year window for this target year
                required_years = [target_year]
                all_available = self.processor._get_available_fvm_marker_years() 
                if target_year - 1 in all_available:
                    required_years.append(target_year - 1)
                if target_year - 2 in all_available:
                    required_years.append(target_year - 2)
                
                self.log.info(f"   📅 Using 3-year window: {sorted(required_years)}")
                
                # Process this target year
                target_estimates = self._process_single_target_year(target_year, required_years, loaded_tables)
                
                # Append results to batch table
                if target_estimates:
                    target_results = self.conn.execute(f"SELECT COUNT(*) FROM {target_estimates}").fetchone()[0]
                    if target_results > 0:
                        self.conn.execute(f"""
                            INSERT INTO {batch_table_name}
                            SELECT * FROM {target_estimates}
                        """)
                        total_fields_processed += target_results
                        self.log.info(f"   ✅ Target year {target_year}: {target_results:,} fields processed")
                    else:
                        self.log.warning(f"   ⚠️ Target year {target_year}: No results produced")
                
                target_time = time.time() - target_start_time
                self.log.info(f"   ✅ Target year {target_year} completed in {target_time:.1f}s")
            
            # Validate batch results
            batch_count = self.conn.execute(f"SELECT COUNT(*) FROM {batch_table_name}").fetchone()[0]
            self.log.info(f"🎯 Batch {batch_years} completed: {batch_count:,} total estimates")
            
            if batch_count == 0:
                self.log.error(f"❌ No estimates generated for batch {batch_years}")
                return None
                
            return batch_table_name
            
        except Exception as e:
            self.log.error(f"❌ Failed to process NLES5 for batch {batch_years}: {e}")
            raise
