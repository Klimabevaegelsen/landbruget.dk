"""
NLES5 Memory Management Utilities Module

This module contains all memory management and cleanup utilities for NLES5 nitrogen washout estimation.
It includes:
- Memory usage monitoring and reporting
- Aggressive memory cleanup for large datasets
- Target year specific cleanup operations
- Pipeline-wide memory management
- Temporary file cleanup utilities

All methods maintain the exact same functionality and cleanup strategies from the original implementation.
"""

import gc
import os
import psutil
from typing import Optional

from unified_pipeline.util.timing import timed


class NLES5MemoryUtils:
    """
    NLES5 Memory Management Utilities containing all memory monitoring and cleanup methods.
    
    This class handles:
    - Memory usage monitoring and reporting for performance optimization
    - Aggressive memory cleanup operations for large dataset processing
    - Target year specific cleanup to manage memory during batch processing
    - Pipeline-wide memory management and temporary file cleanup
    - Production-ready memory optimization strategies
    """
    
    def __init__(self, processor):
        """Initialize memory utils with reference to main processor."""
        self.processor = processor
        self.config = processor.config
        self.log = processor.log
        self.conn = processor.conn
    
    def _get_memory_usage(self) -> float:
        """Get current memory usage in GB."""
        try:
            process = psutil.Process()
            memory_info = process.memory_info()
            memory_gb = memory_info.rss / (1024 ** 3)  # Convert bytes to GB
            return memory_gb
        except Exception as e:
            self.log.debug(f"Could not get memory usage: {e}")
            return 0.0

    def _monitor_memory_usage(self, operation_name: str) -> None:
        """Monitor and log memory usage for a specific operation."""
        try:
            current_memory = self._get_memory_usage()
            
            # Log if memory usage is significant
            if current_memory > 1.0:  # More than 1GB
                self.log.info(f"💾 Memory usage after {operation_name}: {current_memory:.2f} GB")
            
            # Warn if approaching memory limit
            if current_memory > self.config.max_memory_usage_gb * 0.8:
                self.log.warning(f"⚠️  High memory usage after {operation_name}: {current_memory:.2f} GB (limit: {self.config.max_memory_usage_gb} GB)")
                
            # Perform cleanup if memory usage is too high
            if current_memory > self.config.max_memory_usage_gb * 0.9:
                self.log.warning(f"🧹 Triggering aggressive cleanup due to high memory usage: {current_memory:.2f} GB")
                self._aggressive_memory_cleanup()
                
        except Exception as e:
            self.log.debug(f"Could not monitor memory usage for {operation_name}: {e}")

    def _aggressive_memory_cleanup(self) -> None:
        """Perform aggressive memory cleanup to free up resources."""
        try:
            initial_memory = self._get_memory_usage()
            
            # Close any temporary database connections
            try:
                # Force garbage collection
                gc.collect()
            except Exception:
                pass
            
            # Clean up temporary files
            self.processor._cleanup_temp_files()
            
            # Drop temporary tables if they exist
            temp_tables_to_drop = [
                'temp_climate_data',
                'temp_spatial_join',
                'temp_batch_processing',
                'temp_tessellation',
                'nles5_batch',
                'nles5_estimates_batch',
                'fields_batch',
                'climate_batch'
            ]
            
            for table in temp_tables_to_drop:
                try:
                    self.conn.execute(f"DROP TABLE IF EXISTS {table}")
                except Exception:
                    pass  # Table might not exist
            
            # Force another garbage collection
            gc.collect()
            
            final_memory = self._get_memory_usage()
            memory_freed = initial_memory - final_memory
            
            if memory_freed > 0.1:  # Only log if significant memory was freed
                self.log.info(f"🧹 Aggressive cleanup completed: freed {memory_freed:.2f} GB (from {initial_memory:.2f} to {final_memory:.2f} GB)")
            
        except Exception as e:
            self.log.debug(f"Error during aggressive memory cleanup: {e}")

    def _aggressive_cleanup_target_year(self) -> None:
        """Perform aggressive cleanup specific to target year processing."""
        try:
            self.log.debug("🧹 Performing aggressive target year cleanup")
            
            # Drop target-year specific temporary tables
            target_year_tables = [
                'climate_target_year',
                'fields_climate_target_year',
                'fields_complete_target_year',
                'percolation_target',
                'estimates_target_*',  # Pattern for target year estimate tables
                'fertilizer_target_year',
                'field_plan_target_year',
                # Agricultural pattern matching tables
                'gkea_fvm_enhanced_mappings_*',  # Year-specific enhanced mappings
                'unmatched_gkea_fields_*',  # Pattern matcher temporary tables
                'gkea_agricultural_signatures_*',
                'fvm_agricultural_signatures_*',
                'agricultural_pattern_matches_*',
                'agricultural_field_mappings_*',
                'enhanced_gkea_fvm_matches_*'
            ]
            
            # Get list of all tables to find target year tables
            try:
                all_tables = self.conn.execute("SHOW TABLES").fetchall()
                table_names = [table[0] for table in all_tables]
                
                # Drop tables matching target year patterns
                for table_name in table_names:
                    if any(pattern.replace('*', '') in table_name for pattern in target_year_tables if '*' in pattern):
                        try:
                            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                            self.log.debug(f"Dropped target year table: {table_name}")
                        except Exception:
                            pass
                
                # Drop exact matches
                for table in target_year_tables:
                    if '*' not in table:
                        try:
                            self.conn.execute(f"DROP TABLE IF EXISTS {table}")
                        except Exception:
                            pass
                            
            except Exception as e:
                self.log.debug(f"Could not list tables for cleanup: {e}")
            
            # Force garbage collection
            gc.collect()
            
            # Clean up temp files
            self.processor._cleanup_temp_files()
            
            # Clean up agricultural pattern matching tables specifically
            self._cleanup_agricultural_pattern_tables()
            
            self.log.debug("✅ Target year cleanup completed")
            
        except Exception as e:
            self.log.debug(f"Error during target year cleanup: {e}")
    
    def _cleanup_agricultural_pattern_tables(self) -> None:
        """Clean up agricultural pattern matching tables after processing."""
        try:
            self.log.debug("🧹 Cleaning up agricultural pattern matching tables")
            
            # Get all tables to find pattern matching tables
            all_tables = self.conn.execute("SHOW TABLES").fetchall()
            table_names = [table[0] for table in all_tables]
            
            # Pattern matching table patterns
            pattern_table_patterns = [
                'unmatched_gkea_fields',
                'gkea_agricultural_signatures',
                'gkea_crop_profiles', 
                'fvm_agricultural_signatures',
                'fvm_crop_profiles',
                'agricultural_pattern_matches',
                'agricultural_field_mappings',
                'enhanced_gkea_fvm_matches',
                'agricultural_matching_summary',
                'fertilizer_enhanced_field_mappings',
                'company_field_bridge'
            ]
            
            cleaned_count = 0
            for table_name in table_names:
                # Check if table matches any pattern
                should_clean = False
                for pattern in pattern_table_patterns:
                    if pattern in table_name:
                        # Keep persistent tables with year suffixes, clean temporary ones
                        if ('enhanced_gkea_fvm_matches' in table_name and '_' in table_name.split('enhanced_gkea_fvm_matches')[-1]) or \
                           ('gkea_fvm_enhanced_mappings' in table_name and '_' in table_name.split('gkea_fvm_enhanced_mappings')[-1]):
                            # Keep year-specific enhanced mappings
                            continue
                        elif any(temp_marker in table_name for temp_marker in ['_temp', '_tmp', 'unmatched_', 'signatures_', 'mappings_']):
                            # Clean temporary tables
                            should_clean = True
                            break
                
                if should_clean:
                    try:
                        self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                        cleaned_count += 1
                        self.log.debug(f"Dropped agricultural pattern table: {table_name}")
                    except Exception:
                        pass
            
            if cleaned_count > 0:
                self.log.debug(f"Cleaned up {cleaned_count} agricultural pattern matching tables")
                
        except Exception as e:
            self.log.debug(f"Error cleaning up agricultural pattern tables: {e}")

    def _aggressive_pipeline_cleanup(self) -> None:
        """Perform comprehensive pipeline cleanup to free all possible memory."""
        try:
            self.log.info("🧹 Performing aggressive pipeline cleanup")
            
            initial_memory = self._get_memory_usage()
            
            # Drop all non-essential tables
            tables_to_preserve = [
                'nles5_nitrogen_estimates',
                'nles5_uncertainty_estimates',
                'agricultural_fields',
                'fertilizer_history',
                'field_plan',
                'catch_crops'
            ]
            
            try:
                # Get all current tables
                all_tables = self.conn.execute("SHOW TABLES").fetchall()
                table_names = [table[0] for table in all_tables]
                
                # Drop tables not in preserve list
                for table_name in table_names:
                    if table_name not in tables_to_preserve:
                        try:
                            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                            self.log.debug(f"Dropped table: {table_name}")
                        except Exception:
                            pass
                            
            except Exception as e:
                self.log.debug(f"Could not perform comprehensive table cleanup: {e}")
            
            # Clean up all temporary files aggressively
            self.processor._cleanup_temp_files()
            
            # Force multiple garbage collections
            for _ in range(3):
                gc.collect()
            
            # Additional memory cleanup for DuckDB
            try:
                # Force DuckDB to release memory
                self.conn.execute("CHECKPOINT")
                self.conn.execute("VACUUM")
            except Exception:
                pass
            
            final_memory = self._get_memory_usage()
            memory_freed = initial_memory - final_memory
            
            self.log.info(f"🧹 Aggressive pipeline cleanup completed")
            if memory_freed > 0.1:
                self.log.info(f"💾 Memory freed: {memory_freed:.2f} GB (from {initial_memory:.2f} to {final_memory:.2f} GB)")
            else:
                self.log.info(f"💾 Current memory usage: {final_memory:.2f} GB")
                
        except Exception as e:
            self.log.error(f"Error during aggressive pipeline cleanup: {e}")

    def _cleanup_temp_files(self) -> None:
        """Clean up temporary files and directories."""
        try:
            temp_dir = getattr(self.processor, 'temp_dir', None)
            if not temp_dir or not os.path.exists(temp_dir):
                return
            
            files_removed = 0
            space_freed = 0
            
            # Remove temporary files
            for root, dirs, files in os.walk(temp_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    try:
                        file_size = os.path.getsize(file_path)
                        os.remove(file_path)
                        files_removed += 1
                        space_freed += file_size
                    except Exception:
                        pass  # File might be in use or already removed
            
            # Log cleanup results if significant
            if files_removed > 0:
                space_freed_mb = space_freed / (1024 * 1024)
                self.log.debug(f"🧹 Cleaned {files_removed} temp files, freed {space_freed_mb:.1f} MB")
                
        except Exception as e:
            self.log.debug(f"Error cleaning temp files: {e}")

    def _check_memory_threshold(self, operation_name: str) -> bool:
        """
        Check if current memory usage is below threshold for safe operation.
        
        Args:
            operation_name: Name of the operation being checked
            
        Returns:
            True if memory usage is safe, False if cleanup is needed
        """
        try:
            current_memory = self._get_memory_usage()
            threshold = self.config.max_memory_usage_gb * 0.75  # 75% threshold
            
            if current_memory > threshold:
                self.log.warning(f"⚠️  Memory threshold exceeded before {operation_name}: {current_memory:.2f} GB > {threshold:.2f} GB")
                return False
            
            return True
            
        except Exception as e:
            self.log.debug(f"Could not check memory threshold: {e}")
            return True  # Assume safe if can't check

    def _optimize_memory_for_operation(self, operation_name: str) -> None:
        """
        Optimize memory usage before a memory-intensive operation.
        
        Args:
            operation_name: Name of the upcoming operation
        """
        try:
            if not self._check_memory_threshold(operation_name):
                self.log.info(f"🧹 Pre-optimizing memory for {operation_name}")
                self._aggressive_memory_cleanup()
                
                # Check again after cleanup
                if not self._check_memory_threshold(operation_name):
                    self.log.warning(f"⚠️  Memory still high after cleanup for {operation_name}")
                    
        except Exception as e:
            self.log.debug(f"Error optimizing memory for {operation_name}: {e}")

    def _log_memory_summary(self) -> None:
        """Log a summary of current memory usage and system resources."""
        try:
            current_memory = self._get_memory_usage()
            memory_limit = self.config.max_memory_usage_gb
            memory_percentage = (current_memory / memory_limit) * 100 if memory_limit > 0 else 0
            
            # Get system memory info
            system_memory = psutil.virtual_memory()
            system_memory_gb = system_memory.total / (1024 ** 3)
            system_usage_pct = system_memory.percent
            
            self.log.info("💾 MEMORY USAGE SUMMARY:")
            self.log.info(f"   Process memory: {current_memory:.2f} GB ({memory_percentage:.1f}% of {memory_limit} GB limit)")
            self.log.info(f"   System memory: {system_memory_gb:.1f} GB total, {system_usage_pct:.1f}% used")
            
            # Warn if usage is high
            if memory_percentage > 80:
                self.log.warning(f"⚠️  High process memory usage: {memory_percentage:.1f}%")
            if system_usage_pct > 85:
                self.log.warning(f"⚠️  High system memory usage: {system_usage_pct:.1f}%")
                
        except Exception as e:
            self.log.debug(f"Could not generate memory summary: {e}")

    def _ensure_memory_available(self, required_gb: float, operation_name: str) -> None:
        """
        Ensure sufficient memory is available for an operation.
        
        Args:
            required_gb: Estimated memory requirement in GB
            operation_name: Name of the operation requiring memory
            
        Raises:
            MemoryError: If insufficient memory cannot be freed
        """
        try:
            current_memory = self._get_memory_usage()
            available_memory = self.config.max_memory_usage_gb - current_memory
            
            if available_memory < required_gb:
                self.log.warning(f"⚠️  Insufficient memory for {operation_name}: need {required_gb:.1f} GB, have {available_memory:.1f} GB available")
                
                # Try aggressive cleanup
                self._aggressive_memory_cleanup()
                
                # Check again
                current_memory = self._get_memory_usage()
                available_memory = self.config.max_memory_usage_gb - current_memory
                
                if available_memory < required_gb:
                    raise MemoryError(f"Insufficient memory for {operation_name}: need {required_gb:.1f} GB, have {available_memory:.1f} GB available after cleanup")
                else:
                    self.log.info(f"✅ Memory available for {operation_name}: {available_memory:.1f} GB after cleanup")
            else:
                self.log.debug(f"✅ Sufficient memory for {operation_name}: {available_memory:.1f} GB available")
                
        except MemoryError:
            raise
        except Exception as e:
            self.log.debug(f"Could not ensure memory availability: {e}")
            # Continue without guarantee if we can't check
