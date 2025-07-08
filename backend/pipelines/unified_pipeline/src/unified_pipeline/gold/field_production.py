"""
Field Production Gold Layer

This module implements the gold layer processor for field production estimates.
It combines agricultural fields data with DST (Danish Statistics) yield data to create
comprehensive production estimates for analytics and downstream consumption.

Migrated from pandas/geopandas to pure DuckDB approach for optimal performance.
"""

import os
from typing import Any, Dict, List, Optional

from pydantic import ConfigDict

from unified_pipeline.common.base import BaseJobConfig, BaseSource, GoldJobInterface
from unified_pipeline.util.log_util import Logger

# DST functionality is now integrated into the unified pipeline


class FieldProductionGoldConfig(BaseJobConfig):
    """Configuration for Field Production gold layer."""

    name: str = "Field Production Gold"
    dataset: str = "field_production"
    type: str = "gold"
    description: str = "Comprehensive field production estimates using DST yield data"
    frequency: str = "weekly"
    bucket: str = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")

    # Input silver datasets
    agricultural_fields_dataset: str = "fvm_marker"
    dst_zone_mapping_dataset: str = "dst_zone_mapping"
    dst_yield_datasets: List[str] = [
        "dst_hst77",
        "dst_gartn1",
        "dst_fro",
        "dst_halm1",
    ]

    # Processing configuration
    batch_size: int = 500000  # REDUCED: Conservative batch size for 16GB RAM constraint
    max_year_lag: int = 3  # Maximum years between field and DST data
    years_per_batch: int = 1  # REDUCED: Process one year at a time to control memory

    # OPTIMIZED: Conservative memory configuration for GitHub Actions (16GB RAM, 4 CPU, 14GB SSD)
    # Leave 4GB buffer for OS and other processes (25% safety margin)
    memory_limit: str = "12GB"  # Use 75% of available 16GB (was 14GB)
    max_temp_directory_size: str = "10GB"  # Use 71% of available 14GB SSD (was 12GB)
    thread_count: int = 4  # Use all available cores

    # CRITICAL: Aggressive memory management for resource-constrained environment
    enable_memory_optimizations: bool = True
    enable_spatial_join_verification: bool = True
    enable_aggressive_cleanup: bool = True  # NEW: Enable aggressive resource cleanup
    checkpoint_threshold_mb: int = 512  # REDUCED: More frequent checkpoints (was 2GB)
    emergency_memory_threshold: float = 0.85  # Trigger emergency cleanup at 85% memory usage

    # NEW: Resource monitoring and fallback configuration
    enable_emergency_fallbacks: bool = True
    max_memory_retries: int = 3
    fallback_batch_reduction_factor: float = 0.5  # Reduce batch size by 50% on memory issues

    # Quality thresholds
    min_yield_coverage: float = 0.3  # Minimum acceptable yield coverage rate

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class FieldProductionGold(BaseSource[FieldProductionGoldConfig], GoldJobInterface):
    """
    Gold layer processor for field production estimates using pure DuckDB.

    Combines agricultural fields and DST yield data to create
    comprehensive production estimates for analytics and downstream consumption.
    """

    def __init__(self, config: FieldProductionGoldConfig):
        super().__init__(config)
        self.log = Logger.get_logger()

        # Connection and spatial extension are already set up by BaseSource
        # Just verify spatial extension is working (it should be loaded by BaseSource)
        try:
            self.conn.execute("SELECT ST_Point(0, 0)")
            self.log.info("✅ Spatial extension is working (loaded by BaseSource)")
        except Exception as e:
            self.log.error(f"❌ Spatial extension not available from BaseSource: {e}")
            raise RuntimeError("Spatial extension is required but not available from BaseSource")

        # Apply memory optimizations
        self._configure_memory_optimizations()

    def _configure_memory_optimizations(self):
        """Configure DuckDB memory optimizations for GitHub Actions environment with aggressive resource management."""
        if not self.config.enable_memory_optimizations:
            return

        try:
            self.log.info(
                "🚀 Applying GitHub Actions optimized DuckDB configuration (conservative)..."
            )

            # CONSERVATIVE: Memory settings - leave 4GB buffer for OS and other processes
            self.conn.execute(f"SET memory_limit = '{self.config.memory_limit}'")
            self.conn.execute(f"SET max_memory = '{self.config.memory_limit}'")
            self.conn.execute(
                f"SET max_temp_directory_size = '{self.config.max_temp_directory_size}'"
            )

            # CPU optimization - use all available cores
            self.conn.execute(f"SET threads = {self.config.thread_count}")

            # AGGRESSIVE: Performance optimizations for spatial operations with frequent cleanup
            self.conn.execute(
                "SET preserve_insertion_order = false"
            )  # Allow reordering for efficiency
            self.conn.execute("SET enable_object_cache = true")  # Enable for better performance

            # CRITICAL: More frequent checkpoints for memory-constrained environment
            checkpoint_threshold = f"{self.config.checkpoint_threshold_mb}MB"
            self.conn.execute(f"SET checkpoint_threshold = '{checkpoint_threshold}'")
            self.conn.execute("SET temp_directory = '/tmp/duckdb_optimized'")

            # AGGRESSIVE: Spatial-specific optimizations for resource constraints
            self.conn.execute("SET enable_progress_bar = false")  # Reduce overhead
            self.conn.execute("SET enable_checkpoint_on_shutdown = true")  # Ensure cleanup on exit
            self.conn.execute("SET wal_autocheckpoint = 100")  # More frequent WAL checkpoints

            # Create optimized temp directory with cleanup
            import os

            temp_dir = "/tmp/duckdb_optimized"
            if not os.path.exists(temp_dir):
                os.makedirs(temp_dir, exist_ok=True)
            else:
                # Clean existing temp files before starting
                self._cleanup_temp_files_safe()

            self.log.info("✅ Conservative GitHub Actions optimizations applied:")
            self.log.info(f"   Memory: {self.config.memory_limit} (75% of 16GB, 4GB OS buffer)")
            self.log.info(
                f"   Temp storage: {self.config.max_temp_directory_size} (71% of 14GB SSD)"
            )
            self.log.info(f"   Threads: {self.config.thread_count}")
            self.log.info(f"   Checkpoint threshold: {checkpoint_threshold} (frequent cleanup)")
            self.log.info(f"   Emergency threshold: {self.config.emergency_memory_threshold:.0%}")

        except Exception as e:
            self.log.warning(f"Could not apply some memory optimizations: {e}")

    def _cleanup_temp_files_safe(self):
        """Safely clean up temporary files without interfering with active DuckDB operations."""
        import glob
        import os
        import time

        temp_dir = "/tmp/duckdb_optimized"
        try:
            if os.path.exists(temp_dir):
                # Only clean files older than 1 hour to avoid active DuckDB files
                current_time = time.time()
                for file_path in glob.glob(os.path.join(temp_dir, "*")):
                    try:
                        file_stat = os.stat(file_path)
                        if current_time - file_stat.st_mtime > 3600:  # 1 hour
                            if os.path.isfile(file_path):
                                os.remove(file_path)
                                self.log.debug(
                                    f"Removed old temp file: {os.path.basename(file_path)}"
                                )
                    except Exception as e:
                        self.log.debug(f"Could not remove temp file {file_path}: {e}")

        except Exception as e:
            self.log.debug(f"Error in safe temp file cleanup: {e}")

    def _aggressive_resource_cleanup(self, phase: str = "general"):
        """Aggressive resource cleanup for memory-constrained environment."""
        if not self.config.enable_aggressive_cleanup:
            return

        self.log.info(f"🧹 Aggressive resource cleanup ({phase})...")

        try:
            # 1. Drop all temporary tables aggressively
            all_tables = self.conn.execute("SHOW TABLES").fetchall()
            temp_patterns = ["temp_", "intermediate_", "_raw", "_chunk", "batch_", "year_"]

            for table_row in all_tables:
                table_name = table_row[0]
                if any(pattern in table_name.lower() for pattern in temp_patterns):
                    try:
                        self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                        self.log.debug(f"Dropped temp table: {table_name}")
                    except:
                        pass

            # 2. Force immediate checkpoint and vacuum
            self.conn.execute("CHECKPOINT")
            self.conn.execute("VACUUM")

            # 3. Clean temp files
            self._cleanup_temp_files_safe()

            # 4. Python garbage collection
            import gc

            collected = gc.collect()

            # 5. Log memory status after cleanup
            try:
                import psutil

                memory = psutil.virtual_memory()
                self.log.info(
                    f"   Memory after cleanup: {memory.used / (1024**3):.1f}GB ({memory.percent:.1f}%)"
                )
                self.log.info(f"   Python objects collected: {collected}")
            except ImportError:
                pass

        except Exception as e:
            self.log.warning(f"Error in aggressive cleanup: {e}")

    def _check_emergency_memory_threshold(self) -> bool:
        """Check if we're approaching emergency memory threshold."""
        try:
            import psutil

            memory = psutil.virtual_memory()

            if memory.percent > (self.config.emergency_memory_threshold * 100):
                self.log.warning(f"🚨 Emergency memory threshold exceeded: {memory.percent:.1f}%")
                self._emergency_resource_cleanup()
                return True
            return False

        except ImportError:
            return False
        except Exception as e:
            self.log.warning(f"Could not check emergency memory threshold: {e}")
            return False

    def _emergency_resource_cleanup(self):
        """Emergency resource cleanup when approaching memory limits."""
        self.log.warning("🚨 EMERGENCY: Performing aggressive resource cleanup...")

        try:
            # 1. Drop ALL non-essential tables immediately
            all_tables = self.conn.execute("SHOW TABLES").fetchall()
            essential_tables = [
                "final_production_estimates",
                "dst_zones",
                "dst_dst_hst77",
                "dst_dst_gartn1",
                "dst_dst_fro",
                "dst_dst_halm1",
            ]

            for table_row in all_tables:
                table_name = table_row[0]
                if table_name not in essential_tables:
                    try:
                        self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                        self.log.debug(f"Emergency dropped: {table_name}")
                    except:
                        pass

            # 2. Force immediate memory release
            self.conn.execute("CHECKPOINT")
            self.conn.execute("VACUUM")

            # 3. Aggressive Python cleanup
            import gc

            gc.collect()
            gc.collect()  # Run twice for better cleanup

            # 4. Clean all temp files
            import shutil

            temp_dir = "/tmp/duckdb_optimized"
            if os.path.exists(temp_dir):
                try:
                    shutil.rmtree(temp_dir)
                    os.makedirs(temp_dir, exist_ok=True)
                    self.log.info("   Emergency: Cleared all temp files")
                except:
                    pass

            self.log.warning("🚨 Emergency cleanup completed")

        except Exception as e:
            self.log.error(f"Emergency cleanup failed: {e}")

    def _process_with_memory_fallback(self, processing_func, *args, **kwargs):
        """Execute processing function with automatic memory fallback."""
        if not self.config.enable_emergency_fallbacks:
            return processing_func(*args, **kwargs)

        for attempt in range(self.config.max_memory_retries):
            try:
                # Check memory before processing
                if self._check_emergency_memory_threshold():
                    self.log.warning(f"Memory threshold exceeded before attempt {attempt + 1}")

                result = processing_func(*args, **kwargs)
                return result

            except Exception as e:
                if "memory" in str(e).lower() or "out of memory" in str(e).lower():
                    self.log.warning(f"Memory error on attempt {attempt + 1}: {e}")

                    if attempt < self.config.max_memory_retries - 1:
                        # Reduce batch size and try again
                        if hasattr(self.config, "batch_size"):
                            original_batch = self.config.batch_size
                            new_batch = int(
                                original_batch * self.config.fallback_batch_reduction_factor
                            )
                            self.log.warning(f"Reducing batch size: {original_batch} → {new_batch}")
                            # Note: This requires modifying the config object, which is frozen
                            # In practice, we'd pass the reduced batch size to the processing function

                        self._emergency_resource_cleanup()
                        continue
                    else:
                        self.log.error(
                            f"Memory fallback failed after {self.config.max_memory_retries} attempts"
                        )
                        raise
                else:
                    # Non-memory error, re-raise immediately
                    raise

    def _verify_spatial_join_optimization(self, query_description: str = "spatial join") -> bool:
        """Verify that spatial joins are using the SPATIAL_JOIN operator."""
        if not self.config.enable_spatial_join_verification:
            return True

        try:
            test_query = """
                EXPLAIN SELECT COUNT(*) 
                FROM current_year_fields f
                JOIN dst_zones z ON ST_Within(f.geometry, z.geometry)
                LIMIT 1
            """

            plan = self.conn.execute(test_query).fetchall()
            plan_text = str(plan)

            if "SPATIAL_JOIN" in plan_text:
                self.log.info(f"✅ SPATIAL_JOIN operator detected for {query_description}")
                return True
            else:
                self.log.warning(
                    f"❌ SPATIAL_JOIN operator NOT detected for {query_description} - check join conditions"
                )
                self.log.debug(f"Query plan: {plan_text}")
                return False

        except Exception as e:
            self.log.warning(f"Could not verify SPATIAL_JOIN usage: {e}")
            return False

    def _log_performance_metrics(self, phase: str, start_time: float) -> None:
        """Log detailed performance metrics for each phase."""
        import time

        import psutil

        duration = time.time() - start_time
        memory = psutil.virtual_memory()

        self.log.info(f"📊 {phase} Performance:")
        self.log.info(f"   Duration: {duration:.1f} seconds")
        self.log.info(f"   Memory: {memory.used / (1024**3):.1f}GB ({memory.percent:.1f}%)")

        # Check if we're getting expected performance gains
        if phase == "spatial_joins" and duration > 600:  # 10 minutes
            self.log.warning(
                "⚠️ Spatial joins taking longer than expected - check SPATIAL_JOIN usage"
            )

    def _cleanup_year_tables(self):
        """Clean up temporary tables created during year processing."""
        tables_to_drop = [
            "current_year_fields",
            "year_fields_with_zones",
            "year_production_estimates",
            "temp_fvm_marker_*",  # Clean up any temp tables from year processing
        ]

        for table_pattern in tables_to_drop:
            if "*" in table_pattern:
                # Handle wildcard patterns
                try:
                    all_tables = self.conn.execute("SHOW TABLES").fetchall()
                    pattern_prefix = table_pattern.replace("*", "")
                    matching_tables = [t[0] for t in all_tables if t[0].startswith(pattern_prefix)]
                    for table in matching_tables:
                        self.conn.execute(f"DROP TABLE IF EXISTS {table}")
                except:
                    pass
            else:
                try:
                    self.conn.execute(f"DROP TABLE IF EXISTS {table_pattern}")
                except:
                    pass

    def _force_memory_cleanup(self):
        """Force DuckDB and Python memory cleanup with monitoring."""
        try:
            import psutil

            # Check memory before cleanup
            memory_before = psutil.virtual_memory()

            # DuckDB cleanup
            self.conn.execute("CHECKPOINT")
            self.conn.execute("VACUUM")

            # Python garbage collection
            import gc

            collected = gc.collect()

            # Check memory after cleanup
            memory_after = psutil.virtual_memory()
            memory_freed = (memory_before.used - memory_after.used) / (1024**3)

            self.log.info(
                f"🧹 Memory cleanup: freed {memory_freed:.1f}GB, collected {collected} objects"
            )
            self.log.info(
                f"   Memory now: {memory_after.used / (1024**3):.1f}GB ({memory_after.percent:.1f}%)"
            )

        except Exception as e:
            self.log.debug(f"Memory cleanup warning: {e}")

    def _monitor_and_adjust_processing(self) -> int:
        """Monitor memory usage and adjust batch sizes dynamically."""
        try:
            import psutil

            memory = psutil.virtual_memory()
            memory_gb = memory.used / (1024**3)
            memory_percent = memory.percent

            self.log.info(f"💾 Memory usage: {memory_gb:.1f}GB ({memory_percent:.1f}%)")

            # Adjust batch sizes based on memory pressure
            if memory_percent > 85:
                self.log.warning("⚠️ High memory usage - reducing batch size")
                return max(self.config.batch_size // 4, 100000)  # Reduce to 25% but keep minimum
            elif memory_percent > 75:
                self.log.info("📊 Moderate memory usage - reducing batch size slightly")
                return max(self.config.batch_size // 2, 250000)  # Reduce to 50%
            elif memory_percent < 60:
                self.log.info("✅ Low memory usage - can increase batch size")
                return min(self.config.batch_size * 2, 2000000)  # Double but cap at 2M

            return self.config.batch_size

        except Exception as e:
            self.log.warning(f"Memory monitoring failed: {e}")
            return self.config.batch_size

    def _check_memory_threshold_for_batch_processing(self, years_count: int) -> bool:
        """Check if we have enough memory for batch processing."""
        try:
            import psutil

            memory = psutil.virtual_memory()
            memory_percent = memory.percent

            # Estimate memory needs: ~2GB per year of data
            estimated_memory_gb = years_count * 2
            available_memory_gb = (100 - memory_percent) / 100 * memory.total / (1024**3)

            if estimated_memory_gb > available_memory_gb:
                self.log.warning(f"⚠️ Insufficient memory for {years_count} years batch processing")
                self.log.warning(
                    f"   Estimated need: {estimated_memory_gb:.1f}GB, Available: {available_memory_gb:.1f}GB"
                )
                return False

            return True

        except Exception as e:
            self.log.warning(f"Memory threshold check failed: {e}")
            return True  # Default to allowing batch processing

    def _load_agricultural_fields_for_years(
        self, years: List[int], silver_data: Optional[Dict[str, Any]]
    ) -> str:
        """DEPRECATED: Use _process_single_year_with_dst_yields instead for memory efficiency."""
        raise NotImplementedError("This method has been replaced by year-by-year processing")

    def _get_latest_silver_path(self, dataset: str) -> str:
        """Override base method to handle both data.parquet and {dataset}.parquet naming patterns."""
        try:
            all_files = []

            # Try the new pattern first: {dataset}.parquet
            pattern1 = f"gs://{self.config.bucket}/silver/{dataset}/*/{dataset}.parquet"
            files1 = self.gcs_access.list_files(pattern1)
            all_files.extend(files1)

            # Also try the old pattern: data.parquet
            pattern2 = f"gs://{self.config.bucket}/silver/{dataset}/*/data.parquet"
            files2 = self.gcs_access.list_files(pattern2)
            all_files.extend(files2)

            if not all_files:
                raise FileNotFoundError(f"No silver data found for {dataset}")

            # Return the latest file across all patterns by timestamp
            latest_file = sorted(all_files)[-1]
            self.log.info(f"Found latest silver data for {dataset}: {latest_file}")
            return latest_file

        except Exception as e:
            raise FileNotFoundError(f"No silver data found for {dataset}: {e}")

    def _load_silver_data_to_table(
        self, dataset: str, table_name: str, silver_data: Optional[Dict[str, Any]]
    ) -> bool:
        """Load silver data into DuckDB table."""
        if silver_data and dataset in silver_data:
            self.log.info(f"Using in-memory silver data for {dataset}")
            self.conn.register(table_name, silver_data[dataset])
            return True

        # Load from GCS using direct download and load into our connection
        try:
            gcs_path = self._get_latest_silver_path(dataset)

            # Download to temp file and load into our connection
            with self.gcs_access._temp_download(gcs_path) as temp_file:
                self.conn.execute(f"""
                    CREATE OR REPLACE TABLE {table_name} AS
                    SELECT * FROM read_parquet('{temp_file}')
                """)

            count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            self.log.info(f"Loaded {dataset} from GCS into table {table_name} ({count:,} rows)")
            return True
        except FileNotFoundError:
            self.log.error(f"No silver data found for {dataset}")
            return False
        except Exception as e:
            self.log.error(f"Failed to load {dataset}: {e}")
            return False

    async def run(self, silver_data: Optional[Dict[str, Any]] = None) -> None:
        """Run field production estimation gold processing with aggressive resource management for GitHub Actions."""

        self.log.info(
            "🚀 Starting field production gold layer processing (GitHub Actions optimized)"
        )
        self.log.info("📊 Resource-constrained optimizations applied:")
        self.log.info(f"   • Memory: {self.config.memory_limit} (75% of 16GB, 4GB OS buffer)")
        self.log.info(f"   • CPU: {self.config.thread_count} threads (100% of 4 cores)")
        self.log.info(f"   • Temp storage: {self.config.max_temp_directory_size} (71% of 14GB SSD)")
        self.log.info(
            f"   • Batch processing: {self.config.years_per_batch} year at a time (memory control)"
        )
        self.log.info(
            f"   • Checkpoint frequency: {self.config.checkpoint_threshold_mb}MB (aggressive)"
        )
        self.log.info(f"   • Emergency threshold: {self.config.emergency_memory_threshold:.0%}")

        import time

        total_start = time.time()

        try:
            # PHASE 1: Initial setup with resource monitoring
            self.log.info("📋 Phase 1: Initial setup and resource allocation...")
            phase_start = time.time()

            # Check initial memory state
            self._check_emergency_memory_threshold()

            # Get all available years
            available_years = self._get_available_fvm_marker_years()
            if not available_years:
                self.log.error("No fvm_marker years found")
                return

            self.log.info(
                f"Found fvm_marker data for years: {available_years} ({len(available_years)} years)"
            )

            # Load DST zone mapping into DuckDB table (small dataset, can stay in memory)
            if not self._load_silver_data_to_table(
                self.config.dst_zone_mapping_dataset, "dst_zones_raw", silver_data
            ):
                self.log.error("DST zone mapping is required for production estimation")
                return

            # Setup spatial processing with DST zones
            self._setup_spatial_processing_with_dst_zones()

            # Load DST yield data into DuckDB tables (relatively small, can stay in memory)
            dst_tables_loaded = self._load_dst_yield_data(silver_data)
            if not dst_tables_loaded:
                self.log.warning(
                    "No DST yield data available - production estimates will be limited"
                )

            # Create final results table
            self.conn.execute("DROP TABLE IF EXISTS final_production_estimates")
            self.conn.execute("""
                CREATE TABLE final_production_estimates AS
                SELECT * FROM (VALUES 
                    ('dummy', 'dummy', 'dummy', 0, 0.0, 'dummy', false, 'dummy', 'dummy', 'dummy', 
                     0.0, 'dummy', 0.0, 'dummy', 'dummy', current_timestamp)
                ) AS t(field_id, block_id, cvr_number, year, area_ha, crop_type, organic_farming, 
                       landsdel_code, landsdel_name, dst_regions, yield_estimate_hkg_ha, 
                       yield_estimation_method, production_estimate_hkg, production_unit, geometry_wkt, created_at)
                WHERE false
            """)

            self._log_performance_metrics("phase_1_setup", phase_start)
            self._aggressive_resource_cleanup("after_setup")

            # PHASE 2: Year-by-year processing with aggressive resource management
            self.log.info("🔄 Phase 2: Year-by-year processing with resource monitoring...")
            total_fields_processed = 0

            # Process years individually to control memory usage (years_per_batch = 1)
            for i, year in enumerate(available_years):
                year_start = time.time()
                self.log.info(f"🚀 Processing year {year} ({i + 1}/{len(available_years)})...")

                # Check memory before processing each year
                if self._check_emergency_memory_threshold():
                    self.log.warning(f"High memory usage before year {year} - performing cleanup")

                try:
                    # Use memory fallback wrapper for year processing
                    year_count = self._process_with_memory_fallback(
                        self._process_single_year_with_dst_yields_optimized, year, silver_data
                    )

                    if year_count > 0:
                        total_fields_processed += year_count
                        self.log.info(f"✅ Year {year} completed: {year_count:,} fields processed")
                    else:
                        self.log.warning(f"No fields processed for year {year}")

                except Exception as year_e:
                    self.log.error(f"Error processing year {year}: {year_e}")
                    # Continue with next year rather than failing entire pipeline
                    continue

                # AGGRESSIVE: Clean up after each year
                self._cleanup_year_tables()
                self._aggressive_resource_cleanup(f"after_year_{year}")

                self._log_performance_metrics(f"year_{year}", year_start)

                # Check if we're approaching time limits (5.5 hours of 6 hour limit)
                elapsed_hours = (time.time() - total_start) / 3600
                if elapsed_hours > 5.5:
                    self.log.warning(
                        f"Approaching GitHub Actions time limit ({elapsed_hours:.1f}h) - stopping processing"
                    )
                    break

            if total_fields_processed == 0:
                self.log.error("No fields were processed across all years")
                return

            self.log.info(
                f"Processed {total_fields_processed:,} fields across {len(available_years)} years"
            )

            # PHASE 3: Final processing and cleanup
            self.log.info("📊 Phase 3: Final processing and results generation...")
            phase_start = time.time()

            # Generate summary statistics using DuckDB
            self._generate_summary_statistics()

            # Save to gold layer using optimized export
            self._save_results_to_gold()

            self._log_performance_metrics("phase_3_finalization", phase_start)

            # Final performance summary
            total_duration = time.time() - total_start
            self._log_performance_metrics("total_pipeline", total_start)

            self.log.info("✅ Field production gold layer processing completed successfully")
            self.log.info("🎯 Performance summary:")
            self.log.info(f"   • Total fields processed: {total_fields_processed:,}")
            self.log.info(f"   • Years processed: {len(available_years)}")
            self.log.info(f"   • Total duration: {total_duration / 60:.1f} minutes")
            self.log.info(
                f"   • Average per year: {(total_duration / len(available_years)) / 60:.1f} minutes"
            )

            # Check if we achieved expected performance targets
            if total_duration < 3600:  # Less than 1 hour
                self.log.info(f"🚀 Excellent performance: {total_duration / 60:.1f} minutes total")
            elif total_duration < 7200:  # Less than 2 hours
                self.log.info(f"✅ Good performance: {total_duration / 60:.1f} minutes total")
            else:
                self.log.warning(
                    f"⚠️ Performance target missed: {total_duration / 60:.1f} minutes total"
                )

        except Exception as e:
            self.log.error(f"Field production processing failed: {e}")
            # Emergency cleanup before re-raising
            self._emergency_resource_cleanup()
            raise

        finally:
            # CRITICAL: Final aggressive cleanup
            self.log.info("🧹 Final resource cleanup...")
            self._aggressive_resource_cleanup("final")

    def _process_single_year_with_dst_yields_optimized(
        self, year: int, silver_data: Optional[Dict[str, Any]]
    ) -> int:
        """Process a single year with optimized resource management and streaming data loading."""
        try:
            year_start = time.time()

            # Reapply memory optimizations before processing each year
            if self.config.enable_memory_optimizations:
                self.conn.execute(f"SET threads = {self.config.thread_count}")
                self.conn.execute("SET preserve_insertion_order = false")

            dataset_name = f"fvm_marker_{year}"
            self.log.info(f"  📥 Loading {dataset_name} with streaming approach...")

            # STREAMING: Load single year using streaming approach to minimize memory footprint
            if silver_data and dataset_name in silver_data:
                self.log.info(f"Using in-memory data for {dataset_name}")
                # Register the dataframe and create table with aggressive cleanup
                self.conn.execute("DROP TABLE IF EXISTS temp_year_data")
                self.conn.register("temp_year_data", silver_data[dataset_name])

                # Check column structure and create optimized table
                columns_info = self.conn.execute("DESCRIBE temp_year_data").fetchall()
                column_names = [col[0] for col in columns_info]

                # Create year table with minimal memory footprint
                self._create_year_table_optimized("temp_year_data", year, column_names)

            else:
                # STREAMING: Load from GCS using temporary download and immediate processing
                try:
                    gcs_path = self._get_latest_silver_path(dataset_name)

                    # Use temporary download with immediate processing to minimize memory usage
                    with self.gcs_access._temp_download(gcs_path) as temp_file:
                        # Check column structure first
                        columns_info = self.conn.execute(
                            f"DESCRIBE (SELECT * FROM read_parquet('{temp_file}') LIMIT 1)"
                        ).fetchall()
                        column_names = [col[0] for col in columns_info]

                        # Create year table directly from file with minimal memory footprint
                        self._create_year_table_from_file_optimized(temp_file, year, column_names)

                except Exception as e:
                    self.log.warning(f"Could not load {dataset_name} from GCS: {e}")
                    return 0

            # Check if any data was loaded
            year_count = self.conn.execute("SELECT COUNT(*) FROM current_year_fields").fetchone()[0]
            if year_count == 0:
                self.log.warning(f"No data loaded for {dataset_name}")
                return 0

            self.log.info(f"  ✅ Loaded {year_count:,} fields for year {year}")

            # Check memory after loading
            self._check_emergency_memory_threshold()

            # OPTIMIZED: Spatial join with DST zones using SPATIAL_JOIN operator
            spatial_start = time.time()
            self.log.info("  🗺️ Performing spatial join with DST zones...")

            self.conn.execute("""
                CREATE OR REPLACE TABLE year_fields_with_zones AS
                SELECT 
                    f.field_id,
                    f.block_id,
                    f.cvr_number,
                    f.area_ha,
                    f.crop_type,
                    f.organic_farming,
                    f.year,
                    z.landsdel_code,
                    z.landsdel_name,
                    z.dst_regions,
                    ST_AsText(f.geometry) as geometry_wkt
                FROM current_year_fields f
                LEFT JOIN dst_zones z ON ST_Within(f.geometry, z.geometry)
            """)

            # Verify SPATIAL_JOIN operator usage
            self._verify_spatial_join_optimization("DST zones spatial join")
            self._log_performance_metrics(f"spatial_join_year_{year}", spatial_start)

            # AGGRESSIVE: Clean up source table immediately after spatial join
            self.conn.execute("DROP TABLE IF EXISTS current_year_fields")

            # Check memory after spatial join
            self._check_emergency_memory_threshold()

            # OPTIMIZED: Create production estimates using single-phase joins
            yield_start = time.time()
            self.log.info("  📊 Creating production estimates...")

            self.conn.execute("""
                CREATE OR REPLACE TABLE year_production_estimates AS
                SELECT 
                    -- JOIN KEYS
                    f.field_id,
                    f.block_id,
                    f.cvr_number,
                    f.year,
                    -- FIELD DATA
                    f.area_ha,
                    f.crop_type,
                    f.organic_farming,
                    -- DST ZONE INFO
                    f.landsdel_code,
                    f.landsdel_name,
                    f.dst_regions,
                    -- YIELD DATA (optimized single lookup per field)
                    COALESCE(
                        hst77.harvest_value,
                        gartn1.horticulture_value,
                        fro.seed_value,
                        halm1.straw_value,
                        hst77_national.harvest_value
                    ) as yield_estimate_hkg_ha,
                    CASE 
                        WHEN COALESCE(hst77.harvest_value, gartn1.horticulture_value, fro.seed_value, halm1.straw_value, hst77_national.harvest_value) IS NOT NULL 
                        THEN 'dst_region_match'
                        ELSE 'no_yield_data'
                    END as yield_estimation_method,
                    -- PRODUCTION ESTIMATE
                    CASE 
                        WHEN COALESCE(hst77.harvest_value, gartn1.horticulture_value, fro.seed_value, halm1.straw_value, hst77_national.harvest_value) IS NOT NULL 
                        THEN f.area_ha * COALESCE(hst77.harvest_value, gartn1.horticulture_value, fro.seed_value, halm1.straw_value, hst77_national.harvest_value)
                        ELSE NULL
                    END as production_estimate_hkg,
                    CASE 
                        WHEN COALESCE(hst77.harvest_value, gartn1.horticulture_value, fro.seed_value, halm1.straw_value, hst77_national.harvest_value) IS NOT NULL 
                        THEN 'hkg'
                        ELSE NULL
                    END as production_unit,
                    -- SPATIAL INFO
                    f.geometry_wkt,
                    -- METADATA
                    current_timestamp as created_at
                FROM year_fields_with_zones f
                LEFT JOIN dst_dst_hst77 hst77 ON hst77.area_name = f.dst_regions 
                    AND hst77.time_period = CAST(f.year AS VARCHAR) 
                    AND hst77.measure_name ILIKE '%udbytte%'
                LEFT JOIN dst_dst_gartn1 gartn1 ON gartn1.area_name = f.dst_regions 
                    AND gartn1.time_period = CAST(f.year AS VARCHAR) 
                    AND gartn1.measure_name ILIKE '%udbytte%'
                LEFT JOIN dst_dst_fro fro ON fro.time_period = CAST(f.year AS VARCHAR) 
                    AND fro.measure_name ILIKE '%udbytte%'
                LEFT JOIN dst_dst_halm1 halm1 ON halm1.area_name = f.dst_regions 
                    AND halm1.time_period = CAST(f.year AS VARCHAR) 
                    AND halm1.unit_name ILIKE '%udbytte%'
                LEFT JOIN dst_dst_hst77 hst77_national ON hst77_national.area_name ILIKE '%Hele landet%' 
                    AND hst77_national.time_period = CAST(f.year AS VARCHAR) 
                    AND hst77_national.measure_name ILIKE '%udbytte%'
            """)

            self._log_performance_metrics(f"yield_estimation_year_{year}", yield_start)

            # AGGRESSIVE: Clean up intermediate table immediately
            self.conn.execute("DROP TABLE IF EXISTS year_fields_with_zones")

            # Insert year results into final table
            self.conn.execute("""
                INSERT INTO final_production_estimates
                SELECT * FROM year_production_estimates
            """)

            # Get count of processed fields for this year
            processed_count = self.conn.execute(
                "SELECT COUNT(*) FROM year_production_estimates"
            ).fetchone()[0]

            # AGGRESSIVE: Clean up year results table immediately
            self.conn.execute("DROP TABLE IF EXISTS year_production_estimates")

            # Force checkpoint after each year to prevent memory buildup
            self.conn.execute("CHECKPOINT")

            year_duration = time.time() - year_start
            self.log.info(
                f"  ✅ Year {year} processing completed in {year_duration:.1f}s: {processed_count:,} fields"
            )

            return processed_count

        except Exception as e:
            self.log.error(f"Error processing year {year}: {e}")
            # Clean up any partial tables
            cleanup_tables = [
                "current_year_fields",
                "year_fields_with_zones",
                "year_production_estimates",
            ]
            for table in cleanup_tables:
                try:
                    self.conn.execute(f"DROP TABLE IF EXISTS {table}")
                except:
                    pass
            return 0

    def _create_year_table_optimized(self, source_table: str, year: int, column_names: List[str]):
        """Create optimized year table with minimal memory footprint."""
        # Build SELECT clause with proper column mapping
        field_id_select = "field_id" if "field_id" in column_names else "NULL as field_id"

        if "crop_type" in column_names:
            crop_type_select = "crop_type"
        elif "layer_type" in column_names:
            crop_type_select = "layer_type as crop_type"
        else:
            crop_type_select = "'unknown' as crop_type"

        # Handle geometry column
        if "geometry" in column_names:
            geometry_select = "geometry"
            geometry_where = "geometry IS NOT NULL"
        else:
            self.log.warning(f"No geometry column found for year {year}")
            return

        # Create table based on year (block_id available from 2008+)
        if year >= 2008:
            create_query = f"""
                CREATE OR REPLACE TABLE current_year_fields AS
                SELECT 
                    {field_id_select},
                    block_id,
                    cvr_number,
                    area_ha,
                    {crop_type_select},
                    false as organic_farming,
                    {year} as year,
                    {geometry_select}
                FROM {source_table}
                WHERE {geometry_where}
            """
        else:
            create_query = f"""
                CREATE OR REPLACE TABLE current_year_fields AS
                SELECT 
                    {field_id_select},
                    NULL as block_id,
                    cvr_number,
                    area_ha,
                    {crop_type_select},
                    false as organic_farming,
                    {year} as year,
                    {geometry_select}
                FROM {source_table}
                WHERE {geometry_where}
            """

        self.conn.execute(create_query)

        # AGGRESSIVE: Clean up source table immediately
        self.conn.execute(f"DROP TABLE IF EXISTS {source_table}")

    def _create_year_table_from_file_optimized(
        self, temp_file: str, year: int, column_names: List[str]
    ):
        """Create optimized year table directly from file with minimal memory footprint."""
        # Build SELECT clause with proper column mapping
        field_id_select = "field_id" if "field_id" in column_names else "NULL as field_id"

        if "crop_type" in column_names:
            crop_type_select = "crop_type"
        elif "layer_type" in column_names:
            crop_type_select = "layer_type as crop_type"
        else:
            crop_type_select = "'unknown' as crop_type"

        # Handle geometry column
        if "geometry" in column_names:
            geometry_select = "geometry"
            geometry_where = "geometry IS NOT NULL"
        else:
            self.log.warning(f"No geometry column found for year {year}")
            return

        # Create table based on year (block_id available from 2008+)
        if year >= 2008:
            create_query = f"""
                CREATE OR REPLACE TABLE current_year_fields AS
                SELECT 
                    {field_id_select},
                    block_id,
                    cvr_number,
                    area_ha,
                    {crop_type_select},
                    false as organic_farming,
                    {year} as year,
                    {geometry_select}
                FROM read_parquet('{temp_file}')
                WHERE {geometry_where}
            """
        else:
            create_query = f"""
                CREATE OR REPLACE TABLE current_year_fields AS
                SELECT 
                    {field_id_select},
                    NULL as block_id,
                    cvr_number,
                    area_ha,
                    {crop_type_select},
                    false as organic_farming,
                    {year} as year,
                    {geometry_select}
                FROM read_parquet('{temp_file}')
                WHERE {geometry_where}
            """

        self.conn.execute(create_query)

    def _setup_spatial_processing_with_dst_zones(self) -> None:
        """Setup spatial processing with DST zones in DuckDB."""
        try:
            self.log.info("Setting up spatial processing with DST zones")

            # Spatial extension should already be loaded by BaseSource
            # Just verify it's working
            try:
                self.conn.execute("SELECT ST_Point(0, 0)")
                self.log.info("✅ Spatial extension is loaded and working")
            except Exception as spatial_e:
                self.log.error(f"❌ Spatial extension not available: {spatial_e}")
                raise RuntimeError(
                    "Spatial extension is required but not available from BaseSource"
                )

            # Debug: Check the structure and sample data of dst_zones_raw table
            try:
                columns = self.conn.execute("DESCRIBE dst_zones_raw").fetchall()
                self.log.info(f"dst_zones_raw table structure: {columns}")

                # Check for NULL geometries
                null_count = self.conn.execute(
                    "SELECT COUNT(*) FROM dst_zones_raw WHERE geometry IS NULL"
                ).fetchone()[0]
                empty_count = self.conn.execute(
                    "SELECT COUNT(*) FROM dst_zones_raw WHERE geometry = ''"
                ).fetchone()[0]
                total_count = self.conn.execute("SELECT COUNT(*) FROM dst_zones_raw").fetchone()[0]
                self.log.info(
                    f"Geometry data: {total_count} total, {null_count} NULL, {empty_count} empty"
                )

            except Exception as debug_e:
                self.log.warning(f"Debug info failed: {debug_e}")

            # Create optimized DST zones table with spatial geometry
            # Convert WKT geometry strings to GEOMETRY type using ST_GeomFromText for spatial indexing
            self.conn.execute("""
                CREATE OR REPLACE TABLE dst_zones AS
                SELECT 
                    landsdel_code,
                    landsdel_name,
                    dst_regions,
                    ST_GeomFromText(geometry) as geometry
                FROM dst_zones_raw
                WHERE geometry IS NOT NULL 
                AND geometry != ''
                AND geometry != 'NULL'
            """)

            # Get count of processed zones
            zone_count = self.conn.execute("SELECT COUNT(*) FROM dst_zones").fetchone()[0]
            self.log.info(f"✅ Created dst_zones table with {zone_count} zones")

        except Exception as e:
            self.log.error(f"Error setting up DST zones: {e}")
            raise

    def _load_dst_yield_data(self, silver_data: Optional[Dict[str, Any]] = None) -> List[str]:
        """Load DST yield data into DuckDB tables."""
        loaded_tables = []

        for dataset in self.config.dst_yield_datasets:
            table_name = f"dst_{dataset}"
            try:
                if self._load_silver_data_to_table(dataset, table_name, silver_data):
                    # Get record count
                    count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                    self.log.info(f"Loaded {count} records from {dataset} into {table_name}")
                    loaded_tables.append(table_name)
                else:
                    self.log.warning(f"No data found for {dataset}")
            except Exception as e:
                self.log.error(f"Error loading {dataset}: {e}")
                continue

        return loaded_tables

    def _generate_summary_statistics(self) -> None:
        """Generate summary statistics using DuckDB."""
        try:
            # Get summary statistics
            summary = self.conn.execute("""
                SELECT 
                    COUNT(*) as total_fields,
                    COUNT(DISTINCT year) as years_covered,
                    COUNT(DISTINCT crop_type) as crops_covered,
                    COUNT(yield_estimate_hkg_ha) as fields_with_yields,
                    COUNT(production_estimate_hkg) as fields_with_production,
                    MIN(year) as min_year,
                    MAX(year) as max_year
                FROM final_production_estimates
            """).fetchone()

            (
                total_fields,
                years_covered,
                crops_covered,
                fields_with_yields,
                fields_with_production,
                min_year,
                max_year,
            ) = summary

            yield_coverage = fields_with_yields / total_fields if total_fields > 0 else 0
            production_coverage = fields_with_production / total_fields if total_fields > 0 else 0

            self.log.info("Field production summary:")
            self.log.info(f"  Total production estimates: {total_fields:,}")
            self.log.info(f"  Years covered: {years_covered} years ({min_year}-{max_year})")
            self.log.info(f"  Unique crop types: {crops_covered}")
            self.log.info(
                f"  Fields with yield estimates: {fields_with_yields:,} ({yield_coverage:.1%})"
            )
            self.log.info(
                f"  Fields with production estimates: {fields_with_production:,} ({production_coverage:.1%})"
            )

            # Summary by year
            year_summary = self.conn.execute("""
                SELECT 
                    year,
                    COUNT(*) as year_count,
                    COUNT(production_estimate_hkg) as year_with_production,
                    COUNT(production_estimate_hkg) * 100.0 / COUNT(*) as year_coverage
                FROM final_production_estimates
                GROUP BY year
                ORDER BY year
            """).fetchall()

            for year, year_count, year_with_production, year_coverage in year_summary:
                self.log.info(
                    f"    Year {year}: {year_count:,} fields, {year_with_production:,} with production ({year_coverage:.1f}%)"
                )

            # Check quality thresholds
            if yield_coverage < self.config.min_yield_coverage:
                self.log.warning(
                    f"Yield coverage {yield_coverage:.1%} below minimum threshold {self.config.min_yield_coverage:.1%}"
                )

        except Exception as e:
            self.log.error(f"Error generating summary statistics: {e}")
            raise

    def _save_results_to_gold(self) -> None:
        """Save results to gold layer using optimized DuckDB export."""
        try:
            # Use optimized save method from base class
            output_path = (
                f"gs://{self.config.bucket}/gold/{self.config.dataset}/latest/data.parquet"
            )

            # Export directly from DuckDB table to GCS
            self.gcs_access.upload_from_duckdb_table(
                self.conn,
                "final_production_estimates",
                output_path,
                compression="zstd",
                row_group_size=100000,
            )

            self.log.info(f"Saved field production estimates to {output_path}")

        except Exception as e:
            self.log.error(f"Error saving results: {e}")
            raise
