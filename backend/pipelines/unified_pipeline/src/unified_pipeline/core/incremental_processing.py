#!/usr/bin/env python3
"""
Incremental Processing Utilities

This module provides utilities for incremental data processing as specified in
CHR_INCREMENTAL_PROCESSING_PLAN.md, including:

1. Parquet file merging and deduplication
2. Processing state management
3. Data freshness tracking
4. Incremental vs full processing decision logic

Author: CHR Incremental Processing Implementation
Date: 2025-09-14
"""

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from .gcs_data_access import GCSDataAccess

logger = logging.getLogger(__name__)


@dataclass
class ProcessingRun:
    """Represents a single processing run."""

    pipeline_name: str
    bronze_timestamp: str
    processing_mode: str  # 'full', 'incremental', 'backfill'
    data_period_start: Optional[date] = None
    data_period_end: Optional[date] = None
    months_processed: Optional[int] = None


@dataclass
class DataFreshness:
    """Represents data freshness information."""

    bronze_timestamp: str
    has_main_data: bool = False
    has_movement_data: bool = False
    has_vetstat_data: bool = False
    data_coverage_start: Optional[date] = None
    data_coverage_end: Optional[date] = None
    months_covered: int = 0


class IncrementalProcessor:
    """
    Main class for handling incremental processing operations.

    This class provides methods for:
    - Merging parquet files from multiple sources
    - Managing processing state
    - Making incremental vs full processing decisions
    - Tracking data freshness and completeness
    """

    def __init__(self, gcs_access: Optional[GCSDataAccess] = None, supabase_client=None):
        """Initialize the incremental processor."""
        self.gcs_access = gcs_access or GCSDataAccess()
        self.supabase = supabase_client
        self.duckdb_conn = self.gcs_access.duckdb_conn

    def merge_parquet_files(
        self,
        source_paths: List[str],
        output_path: str,
        deduplication_columns: Optional[List[str]] = None,
        partition_columns: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Merge multiple parquet files into a single output file with deduplication.

        Args:
            source_paths: List of GCS paths to parquet files to merge
            output_path: GCS path for the merged output file
            deduplication_columns: Columns to use for deduplication (if None, no deduplication)
            partition_columns: Columns to partition by (optional)

        Returns:
            Dictionary with merge statistics and metadata
        """
        logger.info(f"🔄 Merging {len(source_paths)} parquet files to {output_path}")

        if not source_paths:
            raise ValueError("No source paths provided for merging")

        merge_stats = {
            "source_files": len(source_paths),
            "total_input_records": 0,
            "total_output_records": 0,
            "duplicates_removed": 0,
            "processing_time_seconds": 0,
            "output_size_mb": 0,
        }

        start_time = datetime.now()

        try:
            # Read and union all source files
            union_queries = []
            for i, source_path in enumerate(source_paths):
                # Count input records
                count_query = f"SELECT COUNT(*) as count FROM read_parquet('{source_path}')"
                count_result = self.duckdb_conn.execute(count_query).fetchone()
                if count_result:
                    merge_stats["total_input_records"] += count_result[0]
                    logger.debug(f"Source {i+1}: {count_result[0]:,} records from {source_path}")

                union_queries.append(f"SELECT * FROM read_parquet('{source_path}')")

            # Create union query
            union_query = " UNION ALL ".join(union_queries)

            # Apply deduplication if specified
            if deduplication_columns:
                logger.info(f"🔧 Applying deduplication on columns: {deduplication_columns}")

                # Create the deduplication query using ROW_NUMBER() window function
                dedup_columns_str = ", ".join(deduplication_columns)
                final_query = f"""
                WITH merged_data AS ({union_query}),
                deduplicated AS (
                    SELECT *,
                           ROW_NUMBER() OVER (PARTITION BY {dedup_columns_str} ORDER BY 1) as rn
                    FROM merged_data
                )
                SELECT * EXCLUDE (rn) FROM deduplicated WHERE rn = 1
                """
            else:
                final_query = union_query

            # Write the merged and deduplicated data
            if partition_columns:
                partition_str = ", ".join([f"'{col}'" for col in partition_columns])
                write_query = f"""
                COPY ({final_query})
                TO '{output_path}'
                (FORMAT PARQUET, PARTITION_BY [{partition_str}])
                """
            else:
                write_query = f"""
                COPY ({final_query})
                TO '{output_path}'
                (FORMAT PARQUET)
                """

            logger.debug(f"Executing merge query: {write_query[:200]}...")
            self.duckdb_conn.execute(write_query)

            # Count output records to calculate deduplication stats
            count_output_query = f"SELECT COUNT(*) as count FROM read_parquet('{output_path}')"
            output_count_result = self.duckdb_conn.execute(count_output_query).fetchone()
            if output_count_result:
                merge_stats["total_output_records"] = output_count_result[0]
                merge_stats["duplicates_removed"] = (
                    merge_stats["total_input_records"] - merge_stats["total_output_records"]
                )

            # Calculate processing time
            end_time = datetime.now()
            merge_stats["processing_time_seconds"] = (end_time - start_time).total_seconds()

            # Try to get output file size (approximate)
            try:
                size_query = f"""
                SELECT SUM(file_size) as total_size
                FROM glob('{output_path}/**/*.parquet')
                """
                size_result = self.duckdb_conn.execute(size_query).fetchone()
                if size_result and size_result[0]:
                    merge_stats["output_size_mb"] = size_result[0] / (1024 * 1024)
            except Exception as e:
                logger.debug(f"Could not calculate output size: {e}")

            logger.info(
                f"✅ Merge completed: {merge_stats['total_input_records']:,} → "
                f"{merge_stats['total_output_records']:,} records "
                f"({merge_stats['duplicates_removed']:,} duplicates removed) "
                f"in {merge_stats['processing_time_seconds']:.1f}s"
            )

            return merge_stats

        except Exception as e:
            logger.error(f"❌ Error merging parquet files: {e}")
            raise

    def get_processing_recommendation(
        self, pipeline_name: str, bronze_timestamp: str
    ) -> Dict[str, Any]:
        """
        Simple processing recommendation - defaults to incremental (3 months).

        Args:
            pipeline_name: Name of the pipeline ('chr', 'svineflytning', etc.)
            bronze_timestamp: Current bronze timestamp

        Returns:
            Dictionary with processing recommendation (always incremental)
        """
        logger.info(f"📊 Using default incremental processing for {pipeline_name}")

        return {
            "recommended_mode": "incremental",
            "reason": "Default 3-month incremental processing",
            "recommended_timestamp": bronze_timestamp,
            "estimated_processing_time_minutes": 30,
        }

    def track_processing_run(
        self,
        processing_run: ProcessingRun,
        status: str = "running",
        error_message: Optional[str] = None,
        performance_metrics: Optional[Dict] = None,
    ) -> str:
        """
        Track a processing run using simple upsert.

        Args:
            processing_run: ProcessingRun dataclass with run information
            status: Current status ('running', 'completed', 'failed', 'cancelled')
            error_message: Error message if status is 'failed'
            performance_metrics: Optional performance metrics dictionary

        Returns:
            UUID of the created/updated processing run record
        """
        if not self.supabase:
            logger.warning("Supabase client not available, skipping tracking")
            return "no-supabase"

        try:
            # Simple upsert with basic data
            record_data = {
                "pipeline_name": processing_run.pipeline_name,
                "processing_mode": processing_run.processing_mode,
                "bronze_timestamp": processing_run.bronze_timestamp,
                "status": status,
            }

            if error_message:
                record_data["error_message"] = error_message

            # Simple upsert - let Supabase handle the rest
            result = (
                self.supabase.table("pipeline_processing_history").upsert(record_data).execute()
            )

            if result.data:
                run_id = result.data[0]["id"]
                logger.info(f"📊 Tracked processing run: {run_id}")
                return run_id
            else:
                logger.warning("No data returned from processing run tracking")
                return "unknown"

        except Exception as e:
            logger.error(f"❌ Error tracking processing run: {e}")
            return "error"

    def update_data_freshness(self, bronze_timestamp: str, freshness_info: DataFreshness) -> bool:
        """
        Update CHR data freshness information in the tracking table.

        Args:
            bronze_timestamp: Bronze timestamp directory
            freshness_info: DataFreshness dataclass with freshness information

        Returns:
            True if successful, False otherwise
        """
        if not self.supabase:
            logger.warning("Supabase client not available, skipping freshness tracking")
            return False

        try:
            record_data = {
                "bronze_timestamp": bronze_timestamp,
                "has_main_chr_data": freshness_info.has_main_data,
                "has_movement_data": freshness_info.has_movement_data,
                "has_vetstat_json": freshness_info.has_vetstat_data,
                "months_covered": freshness_info.months_covered,
            }

            if freshness_info.data_coverage_start:
                record_data["data_coverage_start"] = freshness_info.data_coverage_start.isoformat()
            if freshness_info.data_coverage_end:
                record_data["data_coverage_end"] = freshness_info.data_coverage_end.isoformat()

            result = (
                self.supabase.table("chr_data_freshness")
                .upsert(record_data, on_conflict="bronze_timestamp")
                .execute()
            )

            if result.data:
                logger.info(f"📊 Updated data freshness for {bronze_timestamp}")
                return True
            else:
                logger.warning("No data returned from freshness update")
                return False

        except Exception as e:
            logger.error(f"❌ Error updating data freshness: {e}")
            return False

    def _get_pipeline_state(self, pipeline_name: str) -> Dict[str, Any]:
        """Get current pipeline state from Supabase."""
        if not self.supabase:
            return {}

        try:
            result = (
                self.supabase.table("incremental_processing_state")
                .select("*")
                .eq("pipeline_name", pipeline_name)
                .execute()
            )

            if result.data:
                return result.data[0]
            else:
                return {}
        except Exception as e:
            logger.warning(f"Could not get pipeline state: {e}")
            return {}

    def _analyze_data_freshness(self, pipeline_name: str, bronze_timestamp: str) -> DataFreshness:
        """Analyze data freshness for a given bronze timestamp."""
        freshness = DataFreshness(bronze_timestamp=bronze_timestamp)

        try:
            # Check what data is available in the bronze timestamp
            bronze_path = f"bronze/{pipeline_name}/{bronze_timestamp}/"
            files = self.gcs_access.list_files(bronze_path)

            # Analyze file availability
            for file_path in files:
                filename = file_path.split("/")[-1]
                if "ejendom_" in filename and filename.endswith(".json"):
                    freshness.has_main_data = True
                elif "flytning_" in filename and filename.endswith(".json"):
                    freshness.has_movement_data = True
                elif "vetstat_antibiotics.json" in filename:
                    freshness.has_vetstat_data = True

            # For CHR pipeline, try to determine date coverage
            if pipeline_name == "chr" and "_" in bronze_timestamp:
                parts = bronze_timestamp.split("_")
                if len(parts) >= 3:  # Monthly format: YYYYMMDD_HHMMSS_YYYY-MM
                    month_part = parts[2]
                    if "-" in month_part:
                        year, month = month_part.split("-")
                        freshness.data_coverage_start = date(int(year), int(month), 1)
                        # Calculate end of month
                        if int(month) == 12:
                            next_month = date(int(year) + 1, 1, 1)
                        else:
                            next_month = date(int(year), int(month) + 1, 1)
                        freshness.data_coverage_end = next_month - timedelta(days=1)
                        freshness.months_covered = 1

        except Exception as e:
            logger.warning(f"Could not analyze data freshness: {e}")

        return freshness

    def _make_processing_decision(
        self,
        pipeline_name: str,
        current_state: Dict[str, Any],
        freshness_info: DataFreshness,
        available_timestamps: List[str],
    ) -> Dict[str, Any]:
        """Make processing mode decision based on current state and data freshness."""

        # Default configuration
        max_incremental_months = current_state.get("max_incremental_months", 3)
        backfill_threshold_months = current_state.get("backfill_threshold_months", 6)

        latest_timestamp = available_timestamps[-1]
        last_full_timestamp = current_state.get("last_full_processing_timestamp")
        last_incremental_timestamp = current_state.get("last_incremental_processing_timestamp")

        # If no previous processing, recommend backfill with best available data
        if not last_full_timestamp and not last_incremental_timestamp:
            # Find best monthly run if available (for CHR)
            best_monthly_timestamp = self._find_best_monthly_timestamp(available_timestamps)
            if best_monthly_timestamp:
                return {
                    "recommended_mode": "backfill",
                    "reason": "No previous processing - using best monthly coverage for backfill",
                    "recommended_timestamp": best_monthly_timestamp,
                    "estimated_processing_time_minutes": 180,  # 3 hours for backfill
                }
            else:
                return {
                    "recommended_mode": "full",
                    "reason": "No previous processing - full processing required",
                    "recommended_timestamp": latest_timestamp,
                    "estimated_processing_time_minutes": 240,  # 4 hours for full
                }

        # Calculate time since last processing
        last_processing_timestamp = last_incremental_timestamp or last_full_timestamp

        try:
            last_processing_date = datetime.strptime(
                last_processing_timestamp.split("_")[0], "%Y%m%d"
            )
            latest_date = datetime.strptime(latest_timestamp.split("_")[0], "%Y%m%d")
            days_since_last = (latest_date - last_processing_date).days
            months_since_last = days_since_last / 30.44  # Average days per month

            # Decision logic
            if months_since_last <= max_incremental_months:
                return {
                    "recommended_mode": "incremental",
                    "reason": f"Fresh data ({months_since_last:.1f} months) - incremental",
                    "recommended_timestamp": latest_timestamp,
                    "estimated_processing_time_minutes": 30,  # 30 minutes for incremental
                }
            elif months_since_last <= backfill_threshold_months:
                return {
                    "recommended_mode": "backfill",
                    "reason": f"Moderate gap ({months_since_last:.1f} months) - backfill needed",
                    "recommended_timestamp": latest_timestamp,
                    "estimated_processing_time_minutes": 120,  # 2 hours for backfill
                }
            else:
                return {
                    "recommended_mode": "full",
                    "reason": f"Large gap ({months_since_last:.1f} months) - full needed",
                    "recommended_timestamp": latest_timestamp,
                    "estimated_processing_time_minutes": 240,  # 4 hours for full
                }

        except Exception as e:
            logger.warning(f"Could not calculate time gaps: {e}")
            return {
                "recommended_mode": "incremental",
                "reason": "Could not determine gap - defaulting to incremental",
                "recommended_timestamp": latest_timestamp,
                "estimated_processing_time_minutes": 30,
            }

    def _find_best_monthly_timestamp(self, timestamps: List[str]) -> Optional[str]:
        """Find the timestamp with the best monthly coverage."""
        monthly_timestamps = [t for t in timestamps if t.count("_") == 2]
        if not monthly_timestamps:
            return None

        # Find the timestamp with the most months covered
        best_timestamp = None
        max_months = 0

        for timestamp in monthly_timestamps:
            # Count how many months this timestamp covers by looking for related timestamps
            base_timestamp = "_".join(timestamp.split("_")[:2])
            related_timestamps = [t for t in monthly_timestamps if t.startswith(base_timestamp)]
            months_count = len(related_timestamps)

            if months_count > max_months:
                max_months = months_count
                best_timestamp = timestamp

        return best_timestamp


# Utility functions for common operations


def get_incremental_processor(supabase_client=None) -> IncrementalProcessor:
    """Get a configured incremental processor instance."""
    gcs_access = GCSDataAccess()
    return IncrementalProcessor(gcs_access=gcs_access, supabase_client=supabase_client)


def merge_chr_parquet_files(
    source_paths: List[str], output_path: str, remove_duplicates: bool = True
) -> Dict[str, Any]:
    """
    Convenience function to merge CHR parquet files with standard deduplication.

    Args:
        source_paths: List of GCS paths to CHR parquet files
        output_path: GCS path for merged output
        remove_duplicates: Whether to deduplicate based on CHR number and date

    Returns:
        Merge statistics dictionary
    """
    processor = get_incremental_processor()

    dedup_columns = ["chr_nummer", "dato"] if remove_duplicates else None

    return processor.merge_parquet_files(
        source_paths=source_paths, output_path=output_path, deduplication_columns=dedup_columns
    )


def merge_vetstat_parquet_files(
    source_paths: List[str], output_path: str, remove_duplicates: bool = True
) -> Dict[str, Any]:
    """
    Convenience function to merge VetStat parquet files with standard deduplication.

    Args:
        source_paths: List of GCS paths to VetStat parquet files
        output_path: GCS path for merged output
        remove_duplicates: Whether to deduplicate based on CHR number and product

    Returns:
        Merge statistics dictionary
    """
    processor = get_incremental_processor()

    dedup_columns = ["CHRNummer", "ProduktNavn", "AnvendelsesDato"] if remove_duplicates else None

    return processor.merge_parquet_files(
        source_paths=source_paths, output_path=output_path, deduplication_columns=dedup_columns
    )
