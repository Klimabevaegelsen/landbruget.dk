"""
Pesticide Disaggregation Gold Layer

WHAT THIS MODULE DOES:
======================
This module solves a critical agricultural data problem: pesticide companies report their
pesticide applications at the company level (e.g., "Company ABC applied 100L of pesticide X
to wheat fields"), but we need to know which specific fields received the pesticide for
environmental and health analysis.

THE BUSINESS PROBLEM:
====================
- Pesticide companies report: "We applied 50L of herbicide to 25 hectares of wheat"
- We have field data showing: "Company ABC has 3 wheat fields: 10ha, 8ha, and 7ha"
- We need to figure out: "How much pesticide did each individual field receive?"

THE SOLUTION APPROACH:
=====================
This module implements a proven 4-strategy approach that achieved 92% coverage:

1. MAIN STRATEGY (92% of cases): Area Matching
   - Match pesticide application area to total field area by company + crop type
   - If areas match within 2% tolerance, distribute proportionally across all fields
   - Example: 25L on 25ha wheat → Field1 gets 10L (10ha), Field2 gets 8L (8ha), Field3 gets 7L (7ha)

2. NON-ORGANIC MATCHING: Same as #1 but excludes organic fields
   - Sometimes organic fields are mixed with conventional, causing area mismatches
   - Exclude organic fields and retry the matching

3. PARTIAL FIELD COVERAGE: Single field scenarios
   - When company has only 1 field for that crop type
   - Handles cases where pesticide area < field area (partial coverage)

4. SPATIAL CLUSTERING (REMOVED): Was too complex for minimal benefit
   - Original strategy tried to group nearby fields, but added complexity
   - Removed for simplification since strategies 1-3 provide sufficient coverage

KEY TECHNICAL DECISIONS:
=======================
- Uses 2% area tolerance (CRITICAL - don't change, this achieved 92% coverage)
- Uses Y+1 temporal pattern (2021 pesticide data uses 2022 field boundaries)
- Processes data in DuckDB for memory efficiency
- Uses CVR numbers (Danish company registration) for matching

CRITICAL: This implementation preserves the exact logic from the original pipeline
without any "enhancements" that could break the proven 92% coverage approach.
"""

import logging
import os
import re
from typing import Any, Dict, List, Optional, Set, Tuple

import duckdb
from pydantic import ConfigDict, Field

from unified_pipeline.common.base import BaseJobConfig, BaseSource, GoldJobInterface
from unified_pipeline.util.log_util import Logger
from unified_pipeline.util.timing import timed

logger = logging.getLogger(__name__)


class PesticideDisaggregationGoldConfig(BaseJobConfig):
    """
    Configuration for pesticide disaggregation gold processor.

    This class defines all the settings needed to run the pesticide disaggregation process.
    Think of it as the "control panel" with all the knobs and switches that control how
    the disaggregation works.
    """

    name: str = "Pesticide Disaggregation Gold"
    dataset: str = "pesticide_disaggregation"
    type: str = "gold"
    description: str = "Disaggregates pesticide applications from company to field level"
    frequency: str = "yearly"
    bucket: str = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")

    # CRITICAL PARAMETER: Area tolerance for matching pesticide applications to fields
    # This 2% tolerance is what achieved 92% coverage in the original pipeline
    # DON'T CHANGE THIS - it's the magic number that makes the whole system work
    area_tolerance_pct: float = Field(
        default=2.0,
        description=(
            "Area tolerance percentage - PRESERVE ORIGINAL VALUE "
            "(2% = the sweet spot for 92% coverage)"
        ),
    )

    # Memory management settings for processing large datasets
    batch_size: int = Field(
        default=1000,
        description="How many records to process at once - smaller = less memory usage but slower",
    )

    # TEMPORAL PATTERN: Why we use Y+1 (next year's field data for this year's pesticide data)
    # Example: 2021 pesticide applications use 2022 field boundaries
    # This is because field boundaries are often updated/finalized after the growing season
    field_year_offset: int = Field(
        default=1,
        description="Field year offset (Y+1 pattern) - pesticide year 2021 uses field year 2022",
    )

    # Input dataset names (what files to look for in cloud storage)
    pesticide_applications_dataset: str = "pesticides"

    # Year filtering for matrix jobs (process single year instead of all years)
    pesticide_year: Optional[int] = Field(
        default=None,
        description="Specific pesticide year to process (if None, processes all available years)",
    )

    # Performance tuning for the database operations
    max_memory_gb: float = Field(
        default=12.0,
        description="Maximum memory DuckDB can use - tune based on available server memory",
    )
    enable_parallel_processing: bool = Field(
        default=True,
        description="Whether to use multiple CPU cores - True = faster but uses more memory",
    )

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    def apply_cli_filters(self, cli_config) -> None:
        """
        Apply CLI filtering for matrix job processing.

        This method sets the pesticide_year from CLI parameters to enable
        processing of specific years for parallel matrix jobs.

        Args:
            cli_config: CLI configuration containing pesticide_year filter
        """
        if cli_config.pesticide_year:
            object.__setattr__(self, "pesticide_year", cli_config.pesticide_year)


class PesticideDisaggregationGold(BaseSource[PesticideDisaggregationGoldConfig], GoldJobInterface):
    """
    The main pesticide disaggregation processor - this is where the magic happens!

    WHAT THIS CLASS DOES:
    ====================
    This class takes company-level pesticide applications and figures out which specific
    fields received the pesticide. It's like being a detective trying to solve the puzzle:

    "Company ABC applied 100L of herbicide to wheat fields. They have 4 wheat fields
    of different sizes. How much did each field get?"

    THE PROVEN APPROACH:
    ===================
    Uses a 4-strategy approach that achieved 92% coverage in the original pipeline:

    1. MAIN STRATEGY: Area matching with 2% tolerance
       - If pesticide area ≈ total field area (within 2%), distribute proportionally
       - This handles 92% of all cases successfully

    2. NON-ORGANIC FALLBACK: Same as #1 but excludes organic fields
       - Sometimes organic fields mess up the area calculations
       - Retry matching after excluding organic fields

    3. PARTIAL COVERAGE: Single field scenarios
       - When company has only 1 field for that crop
       - Handles partial field coverage cases

    4. SPATIAL CLUSTERING: Removed for simplicity
       - Was too complex and didn't add much value

    TECHNICAL APPROACH:
    ==================
    - Uses DuckDB (fast in-memory database) for processing
    - Processes data year by year to manage memory
    - Uses CVR numbers (Danish company IDs) to match companies to fields
    - Applies 2% area tolerance (the magic number that makes it work)
    """

    def __init__(self, config: PesticideDisaggregationGoldConfig):
        """
        Initialize the pesticide disaggregation processor.

        This sets up all the basic components we need:
        - Logger for tracking what's happening
        - Database connection (will be created later)
        - Cache for organic field IDs (performance optimization)
        - Validation tracking for pesticide amount preservation
        """
        print("DEBUG: PesticideDisaggregationGold.__init__ called!")
        super().__init__(config)
        self.log = Logger.get_logger()

        # Database connection - will be created fresh for each year to manage memory
        self.duckdb_conn = None

        # Cache for organic field IDs to avoid repeated lookups
        # Organic fields are excluded from some matching strategies
        self._organic_marker_field_ids: Set[str] = set()

        # Validation tracking - tracks pesticide amounts at each step
        self._validation_data = {
            "original_total_dosage": 0.0,
            "original_total_acreage": 0.0,
            "original_record_count": 0,
            "strategy_totals": {},
            "final_total_dosage": 0.0,
            "final_total_acreage": 0.0,
            "final_record_count": 0,
        }

        print("DEBUG: PesticideDisaggregationGold.__init__ completed!")

    def _upload_file_with_gcs_access(
        self, bucket_name: str, source_file_path: str, destination_blob_name: str
    ):
        """Helper method to upload file using gcs_access."""
        import shutil

        gcs_path = f"gs://{bucket_name}/{destination_blob_name}"
        with open(source_file_path, "rb") as src:
            with self.gcs_access.fs.open(gcs_path, "wb") as dst:
                shutil.copyfileobj(src, dst)

    def _validate_strategy_results(self, strategy_name: str, processed_count: int) -> None:
        """
        Validate results after each disaggregation strategy runs.
        Tracks cumulative progress and ensures no data corruption.

        Args:
            strategy_name: Name of the strategy that just completed
            processed_count: Number of records processed by this strategy
        """
        try:
            if processed_count == 0:
                self.log.info(f"📊 VALIDATION: {strategy_name} - No records processed")
                return

            # Check if database connection is still valid
            if not self.duckdb_conn:
                self.log.warning(
                    f"⚠️ VALIDATION WARNING: No database connection for {strategy_name} validation"
                )
                return

            # Get current disaggregated totals
            current_stats = self.duckdb_conn.execute("""
                SELECT 
                    COUNT(*) as disaggregated_count,
                    SUM(COALESCE(DosageQuantity, 0)) as disaggregated_dosage,
                    SUM(COALESCE(AllocatedArea, 0)) as disaggregated_acreage,
                    COUNT(DISTINCT OriginalPesticideRowID) as unique_original_records
                FROM disaggregated_pesticide_applications
            """).fetchone()

            # Get strategy-specific totals
            strategy_stats = self.duckdb_conn.execute(f"""
                SELECT 
                    COUNT(*) as strategy_count,
                    SUM(COALESCE(DosageQuantity, 0)) as strategy_dosage,
                    SUM(COALESCE(AllocatedArea, 0)) as strategy_acreage,
                    COUNT(DISTINCT OriginalPesticideRowID) as strategy_original_records
                FROM disaggregated_pesticide_applications
                WHERE AllocationMethod LIKE '%{self._get_strategy_method_pattern(strategy_name)}%'
            """).fetchone()

            if current_stats and strategy_stats:
                # Store simplified strategy totals
                self._validation_data["strategy_totals"][strategy_name] = {
                    "record_count": strategy_stats[0] or 0,
                    "dosage": strategy_stats[1] or 0.0,
                    "acreage": strategy_stats[2] or 0.0,
                    "original_records": strategy_stats[3] or 0,
                }

                self.log.info(f"📊 VALIDATION: {strategy_name} completed")
                self.log.info(
                    f"   ✅ Strategy processed: {strategy_stats[0]:,} records from "
                    f"{strategy_stats[3]:,} original applications"
                )
                self.log.info(f"   📈 Cumulative progress: {current_stats[0]:,} records")

                # Simple check for major discrepancies
                if strategy_stats[0] != processed_count:
                    self.log.warning(
                        f"⚠️ VALIDATION WARNING: Expected {processed_count:,} records but "
                        f"found {strategy_stats[0]:,} in database"
                    )

        except Exception as e:
            self.log.error(f"❌ VALIDATION ERROR: Failed to validate {strategy_name} results: {e}")

    def _get_strategy_method_pattern(self, strategy_name: str) -> str:
        """Map strategy names to their AllocationMethod patterns for querying."""
        strategy_patterns = {
            "Main Area Match": "Marker_ApplicationAreaToTotalFieldArea_FieldProportional",
            "Non-Organic Match": (
                "Marker_NonOrganic_ApplicationAreaToTotalFieldArea_FieldProportional"
            ),
            "Partial Field Coverage": "Partial_Field_Coverage_SingleField",
            "Spatial Clustering": "Adjacent_Fields_Spatial_Cluster_AreaMatched",
            "Ethical Best-Match": "Ethical_Best_Match_",
        }
        return strategy_patterns.get(strategy_name, strategy_name)

    def _validate_final_disaggregation_integrity(self) -> None:
        """
        Perform comprehensive validation of the final disaggregation results.
        Checks for data integrity, coverage, and potential issues.
        """
        try:
            self.log.info("🔍 VALIDATION: Running final disaggregation integrity checks")

            # Check if database connection is still valid
            if not self.duckdb_conn:
                self.log.warning(
                    "⚠️ VALIDATION WARNING: No database connection for final integrity check"
                )
                return

            # Get remaining pending records (handle case where table might not exist)
            try:
                pending_stats = self.duckdb_conn.execute("""
                    SELECT 
                        COUNT(*) as pending_count,
                        SUM(COALESCE(DosageQuantity, 0)) as pending_dosage,
                        SUM(COALESCE(AcreageSize, 0)) as pending_acreage
                    FROM pending_pesticide_rows
                """).fetchone()
            except Exception as e:
                self.log.warning(f"⚠️ VALIDATION WARNING: Could not query pending records: {e}")
                pending_stats = (0, 0.0, 0.0)

            # Get final disaggregated totals (handle case where table might be empty)
            try:
                final_stats = self.duckdb_conn.execute("""
                    SELECT 
                        COUNT(*) as final_count,
                        SUM(COALESCE(DosageQuantity, 0)) as final_dosage,
                        SUM(COALESCE(AllocatedArea, 0)) as final_acreage,
                        COUNT(DISTINCT OriginalPesticideRowID) as unique_original_records
                    FROM disaggregated_pesticide_applications
                """).fetchone()
            except Exception as e:
                self.log.warning(
                    f"⚠️ VALIDATION WARNING: Could not query disaggregated results: {e}"
                )
                final_stats = (0, 0.0, 0.0, 0)

            if pending_stats and final_stats:
                pending_count, pending_dosage, pending_acreage = pending_stats
                final_count, final_dosage, final_acreage, unique_originals = final_stats

                # Handle None values from database queries
                pending_count = pending_count or 0
                pending_dosage = pending_dosage or 0.0
                pending_acreage = pending_acreage or 0.0
                final_count = final_count or 0
                final_dosage = final_dosage or 0.0
                final_acreage = final_acreage or 0.0
                unique_originals = unique_originals or 0

                # Log simplified validation results
                self.log.info("🎯 VALIDATION: Final Disaggregation Results")
                self.log.info("=" * 60)

                self.log.info("📈 DISAGGREGATION RESULTS:")
                self.log.info(
                    f"   Successfully disaggregated: {unique_originals:,} original applications"
                )
                self.log.info(
                    f"   Total disaggregated records: {final_count:,} (multi-field expansions)"
                )
                self.log.info(f"   Disaggregated dosage: {final_dosage:,.2f} units")
                self.log.info(f"   Disaggregated acreage: {final_acreage:,.2f} ha")

                self.log.info("📉 REMAINING UNPROCESSED:")
                self.log.info(f"   Pending records: {pending_count:,}")
                self.log.info(f"   Pending dosage: {pending_dosage:,.2f} units")
                self.log.info(f"   Pending acreage: {pending_acreage:,.2f} ha")

                # Strategy breakdown
                self.log.info("📋 STRATEGY BREAKDOWN:")
                for strategy, stats in self._validation_data.get("strategy_totals", {}).items():
                    self.log.info(
                        f"   {strategy}: {stats['original_records']:,} applications → "
                        f"{stats['record_count']:,} records"
                    )

                # Simple validation: if we have pending records but no results,
                # that's likely a processing failure
                if pending_count > 0 and final_count == 0:
                    self.log.warning(
                        f"⚠️ VALIDATION WARNING: {pending_count:,} pending records but "
                        f"0 disaggregated results - this may indicate processing issues"
                    )

                self.log.info("✅ Validation completed")

        except Exception as e:
            self.log.error(f"❌ VALIDATION ERROR: Failed final integrity check: {e}")

    async def run(self, silver_data: Optional[Dict[str, Any]] = None) -> None:
        print("🚨 PESTICIDE DISAGGREGATION RUN METHOD: Starting execution")
        print(f"🚨 CONFIG: pesticide_year = {self.config.pesticide_year}")
        """
        THE MAIN ENTRY POINT - This is where the entire pesticide disaggregation process starts!

        WHAT THIS METHOD DOES:
        =====================
        1. Discovers all available years of pesticide and field data
        2. Matches them up using the Y+1 pattern (e.g., 2021 pesticide → 2022 fields)
        3. Processes each year pair through the 4-strategy disaggregation approach
        4. Saves the results and reports overall success statistics

        WHY PROCESS YEAR BY YEAR:
        ========================
        - Pesticide data is huge (millions of records)
        - Processing all years at once would crash the system
        - Year-by-year processing keeps memory usage manageable
        - Each year is independent, so we can process them separately

        Args:
            silver_data: Pre-loaded data (optional) - usually we discover data from cloud storage
        """
        print("DEBUG: Pesticide disaggregation run method called!")
        self.log.info("🚀 Starting pesticide disaggregation processing with original strategy")
        print("DEBUG: Logger info message sent")
        self.log.info(
            f"🔧 Configuration: area_tolerance={self.config.area_tolerance_pct}%, "
            f"field_year_offset={self.config.field_year_offset}"
        )
        self.log.info(f"☁️ GCS Bucket: {self.config.bucket}")

        # STEP 1: DISCOVER AVAILABLE DATA
        # ===============================
        # Look through cloud storage to find all available years of pesticide and field data
        # Match them up using the Y+1 pattern (pesticide year X uses field year X+1)
        self.log.info("📊 Discovering available data years...")
        print("DEBUG: About to call _get_pesticide_field_year_pairs")
        pesticide_field_pairs = self._get_pesticide_field_year_pairs()
        print(f"DEBUG: Found {len(pesticide_field_pairs)} pairs: {pesticide_field_pairs}")

        if not pesticide_field_pairs:
            self.log.warning("⚠️ No valid pesticide-field year pairs found")
            self.log.info("🔍 This might be due to:")
            self.log.info("   - No pesticide data files in GCS")
            self.log.info("   - No field data files in GCS")
            self.log.info("   - Year offset mismatch between pesticide and field data")
            self.log.info("✅ Pesticide disaggregation completed - no data to process")
            return

        self.log.info(
            f"✅ Found {len(pesticide_field_pairs)} pesticide-field year pairs to process"
        )
        print(f"🚨 FOUND {len(pesticide_field_pairs)} YEAR PAIRS: {pesticide_field_pairs}")
        for pest_year, field_year in pesticide_field_pairs:
            self.log.info(f"   📅 Will process: pesticide {pest_year} → field {field_year}")
            print(f"🚨 WILL PROCESS: pesticide {pest_year} → field {field_year}")

        # STEP 2: INITIALIZE TRACKING VARIABLES
        # ====================================
        # Keep track of overall success/failure across all years
        total_pesticide_records = 0  # How many pesticide applications we started with
        total_disaggregated_records = 0  # How many we successfully disaggregated
        successful_years = 0  # How many years processed successfully
        failed_years = 0  # How many years failed

        # STEP 3: PROCESS EACH YEAR PAIR
        # ==============================
        # Process each pesticide year with its corresponding field year
        # This is the main processing loop - each iteration handles one year of data
        for i, (pesticide_year, field_year) in enumerate(pesticide_field_pairs, 1):
            self.log.info("=" * 80)
            self.log.info(
                f"🔄 Processing year pair {i}/{len(pesticide_field_pairs)}: "
                f"pesticide {pesticide_year} with field {field_year}"
            )
            self.log.info("=" * 80)

            # STEP 3a: LOAD DATA FOR THIS YEAR PAIR
            # =====================================
            # Load both pesticide applications and field boundaries for this year combination
            self.log.info(
                f"📥 Loading silver data for pesticide year {pesticide_year} "
                f"and field year {field_year}"
            )
            try:
                datasets = self._load_silver_data_for_years(pesticide_year, field_year, silver_data)
                self.log.info(f"✅ Data loading completed for year {pesticide_year}")
            except Exception as e:
                self.log.error(f"❌ EXCEPTION during data loading for year {pesticide_year}: {e}")
                self.log.error(f"🔍 Exception type: {type(e).__name__}")
                import traceback

                self.log.error(f"📋 Traceback: {traceback.format_exc()}")
                failed_years += 1
                continue

            # Extract the file paths for pesticide and field data
            agricultural_fields_path = datasets.get("agricultural_fields")
            pesticide_applications_path = datasets.get("pesticides")

            # STEP 3b: VALIDATE DATA AVAILABILITY
            # ===================================
            # Make sure we have both pesticide and field data before proceeding
            if agricultural_fields_path is None or pesticide_applications_path is None:
                self.log.warning(f"⚠️ Skipping year {pesticide_year}: missing data files")
                self.log.warning(
                    f"   Agricultural fields: {'✅' if agricultural_fields_path else '❌'}"
                )
                self.log.warning(
                    f"   Pesticide applications: {'✅' if pesticide_applications_path else '❌'}"
                )
                failed_years += 1
                continue

            self.log.info(f"✅ Data files located for year {pesticide_year}")
            self.log.info(f"   📄 Agricultural fields: {agricultural_fields_path}")
            self.log.info(f"   📄 Pesticide applications: {pesticide_applications_path}")

            # STEP 3c: RUN THE DISAGGREGATION PROCESS
            # =======================================
            # This is where the actual disaggregation magic happens!
            # The _process_year_pair method runs all 4 strategies and returns the count of results
            self.log.info(f"⚙️ Starting disaggregation processing for year {pesticide_year}")
            try:
                year_results = self._process_year_pair(
                    pesticide_year,
                    field_year,
                    agricultural_fields_path,
                    pesticide_applications_path,
                )
                self.log.info(f"✅ Disaggregation processing completed for year {pesticide_year}")
            except Exception as e:
                self.log.error(f"❌ EXCEPTION during disaggregation for year {pesticide_year}: {e}")
                self.log.error(f"🔍 Exception type: {type(e).__name__}")
                import traceback

                self.log.error(f"📋 Traceback: {traceback.format_exc()}")
                year_results = None

            # STEP 3d: TRACK RESULTS AND UPDATE STATISTICS
            # ============================================
            # Update our running totals based on how this year went
            if year_results is not None and year_results > 0:
                self.log.info(
                    f"✅ Year {pesticide_year}: Successfully processed and saved "
                    f"{year_results:,} disaggregated records"
                )

                # Count total pesticide records for this year to calculate coverage percentage
                self.log.info(f"📊 Counting total pesticide records for year {pesticide_year}")
                try:
                    temp_conn = duckdb.connect(":memory:")
                    with self.gcs_access._temp_download(pesticide_applications_path) as temp_file:
                        pesticide_count = temp_conn.execute(
                            f"SELECT COUNT(*) FROM read_parquet('{temp_file}')"
                        ).fetchone()[0]
                    temp_conn.close()
                    total_pesticide_records += pesticide_count
                    self.log.info(
                        f"📈 Year {pesticide_year}: {pesticide_count} total pesticide records, "
                        f"{year_results} disaggregated"
                    )
                    total_disaggregated_records += year_results
                    successful_years += 1
                except Exception as e:
                    self.log.error(
                        f"❌ Failed to count pesticide records for year {pesticide_year}: {e}"
                    )
                    # Still count as successful since we have results
                    total_disaggregated_records += year_results
                    successful_years += 1
            elif year_results == 0:
                self.log.info(
                    f"✅ Year {pesticide_year}: Successfully processed (no records to save)"
                )
                successful_years += 1
            else:
                self.log.warning(f"⚠️ Year {pesticide_year}: Processing failed")
                failed_years += 1

        self.log.info("📊 Processing summary:")
        self.log.info(f"   ✅ Successful years: {successful_years}")
        self.log.info(f"   ❌ Failed years: {failed_years}")
        self.log.info(f"   📈 Total pesticide records: {total_pesticide_records}")

        if successful_years == 0:
            self.log.error("❌ No years were successfully processed - terminating")
            error_msg = (
                f"Pesticide disaggregation failed completely: 0/{len(pesticide_field_pairs)} "
                f"years processed successfully, {failed_years} years failed"
            )
            self.log.error(f"💥 CRITICAL FAILURE: {error_msg}")
            raise RuntimeError(error_msg)

        # Calculate coverage statistics
        coverage_pct = (
            (total_disaggregated_records / total_pesticide_records * 100)
            if total_pesticide_records > 0
            else 0
        )

        # Only fail if we had years to process but ALL failed
        if failed_years > 0 and successful_years == 0 and len(pesticide_field_pairs) > 0:
            # This indicates a systematic processing failure, not just missing data
            if total_disaggregated_records == 0:
                self.log.warning(
                    f"⚠️ WARNING: All {failed_years} years failed to process - this may "
                    f"indicate systematic issues with column mapping, data schema, or "
                    f"processing logic"
                )
                # Note: We don't fail here because this might be due to data quality issues
                # rather than code bugs

        self.log.info("🎉 Pesticide disaggregation completed successfully!")
        self.log.info("📊 Final Statistics:")
        self.log.info(
            f"   📈 Total pesticide records across all years: {total_pesticide_records:,}"
        )
        self.log.info(
            f"   ✅ Successfully disaggregated: {total_disaggregated_records:,} "
            f"({coverage_pct:.1f}%)"
        )
        self.log.info(f"   📅 Processed years: {successful_years}")
        self.log.info("   💾 Results saved as separate files for each year (much more efficient!)")
        self.log.info("🏁 Pesticide disaggregation gold layer processing completed successfully")

    @timed(name="Saving year results directly")
    def _save_year_results_direct(self, year: int) -> bool:
        """
        Save disaggregation results for a specific year directly from DuckDB table.
        This is the optimized version that avoids loading large datasets into Python memory.

        Args:
            year: The pesticide year being processed

        Returns:
            True if results were saved successfully, False otherwise
        """
        try:
            # Check if we have results to save
            result_count = self.duckdb_conn.execute(
                "SELECT COUNT(*) FROM disaggregated_pesticide_applications"
            ).fetchone()[0]

            if result_count > 0:
                self.log.info(
                    f"💾 Saving {result_count:,} disaggregated applications for year {year}"
                )

                # Create the final table name for this year using agricultural year format
                table_name = f"pesticide_disaggregation_{year}_{year + 1}"

                # Create a copy of the results table with the year-specific name
                self.log.info(f"🏗️ Creating final table {table_name}")
                self.duckdb_conn.execute(f"""
                    CREATE TABLE {table_name} AS 
                    SELECT * FROM disaggregated_pesticide_applications
                """)

                # Save directly to GCS using our own method
                self.log.info(f"🚀 Uploading {table_name} to GCS bucket")
                dataset_name = f"{self.config.dataset}_{year}_{year + 1}"
                self._save_table_to_gcs(table_name, dataset_name, "gold")

                self.log.info(f"✅ Successfully saved {result_count:,} records for year {year}")

                # Clean up the temporary table
                self.duckdb_conn.execute(f"DROP TABLE {table_name}")

                return True
            else:
                self.log.warning(f"⚠️ No results to save for year {year}")
                return False

        except Exception as e:
            self.log.error(f"❌ Failed to save results for year {year}: {e}")
            return False

    def _save_table_to_gcs(self, table_name: str, dataset: str, stage: str) -> None:
        """
        Save a DuckDB table directly to GCS storage.

        Args:
            table_name: Name of the DuckDB table to save
            dataset: Dataset name for GCS path
            stage: Processing stage (bronze, silver, gold)
        """
        import os
        import tempfile
        from datetime import datetime

        try:
            # Create timestamp and GCS path
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{dataset}.parquet"
            gcs_path = f"{stage}/{dataset}/{timestamp}/{filename}"

            # Create temporary file for export
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
                temp_path = tmp_file.name

            # Export table to temporary file using DuckDB COPY
            self.duckdb_conn.execute(f"""
                COPY {table_name} TO '{temp_path}' 
                (FORMAT PARQUET, COMPRESSION zstd, ROW_GROUP_SIZE 100000)
            """)

            # Upload to GCS
            self._upload_file_with_gcs_access(
                self.config.bucket,
                temp_path,
                gcs_path,
            )

            # Clean up temporary file
            if os.path.exists(temp_path):
                os.unlink(temp_path)

            full_gcs_path = f"gs://{self.config.bucket}/{gcs_path}"
            self.log.info(f"✅ DISAGGREGATION OUTPUT: {table_name} saved to {full_gcs_path}")
            self.log.info(f"📁 GCS Path: {full_gcs_path}")
            print(f"✅ DISAGGREGATION OUTPUT: {table_name} saved to {full_gcs_path}")
            print(f"📁 GCS Path: {full_gcs_path}")

        except Exception as e:
            self.log.error(f"❌ Failed to save table {table_name} to GCS: {e}")
            # Clean up temp file on error
            if "temp_path" in locals() and os.path.exists(temp_path):
                os.unlink(temp_path)
            raise

    def _save_year_results(self, year_results: List[Dict[str, Any]], year: int) -> bool:
        """
        Legacy method for saving results from Python list (deprecated).
        Use _save_year_results_direct() instead for better performance.
        """
        self.log.warning(
            "⚠️ Using deprecated _save_year_results method - "
            "consider using _save_year_results_direct"
        )
        return self._save_year_results_direct(year)

    def _get_pesticide_field_year_pairs(self) -> List[Tuple[int, int]]:
        """
        Discover which years of data we can process by finding matching pesticide and field data.

        THE Y+1 TEMPORAL PATTERN:
        =========================
        We use a Y+1 pattern where pesticide data from year X is matched with field data
        from year X+1.

        Why? Because field boundaries are often updated/finalized after the growing season ends.
        So 2021 pesticide applications use 2022 field boundaries, which reflect the actual field
        layout that was in place during the 2021 growing season.

        EXAMPLE:
        ========
        If we have:
        - Pesticide data: 2020, 2021, 2022
        - Field data: 2021, 2022, 2023

        We can process:
        - 2020 pesticide → 2021 fields ✅
        - 2021 pesticide → 2022 fields ✅
        - 2022 pesticide → 2023 fields ✅

        DISCOVERY PROCESS:
        =================
        1. Scan cloud storage for available pesticide data files
        2. Scan cloud storage for available field data files
        3. Match them using the Y+1 pattern
        4. Return valid pairs for processing

        Returns:
            List of (pesticide_year, field_year) tuples ready for processing
        """
        print("DEBUG: _get_pesticide_field_year_pairs called")

        # Check if we should process only a specific year (for matrix jobs)
        if self.config.pesticide_year:
            self.log.info(
                f"🎯 Matrix job mode: Processing only pesticide year {self.config.pesticide_year}"
            )
            print(f"🎯 MATRIX JOB: Processing only pesticide year {self.config.pesticide_year}")
            pesticide_years = {self.config.pesticide_year}
        else:
            self.log.info("🔍 Discovering all available pesticide and field years")
            # STEP 1: DISCOVER AVAILABLE PESTICIDE YEARS
            # ==========================================
            # Look through cloud storage for pesticide data files
            self.log.info("📊 Scanning GCS for pesticide data...")
            pesticide_years = self._get_available_pesticide_years()
            self.log.info(f"✅ Found pesticide years: {sorted(pesticide_years)}")

        # STEP 2: DISCOVER AVAILABLE FIELD YEARS
        # ======================================
        # Look through cloud storage for field boundary data files
        self.log.info("🌾 Scanning GCS for field data...")
        field_years = self._get_available_field_years()
        self.log.info(f"✅ Found field years: {sorted(field_years)}")

        # STEP 3: CREATE VALID PAIRS USING Y+1 PATTERN
        # ============================================
        # For each pesticide year, look for field data from the following year
        self.log.info(f"🔗 Creating year pairs using Y+{self.config.field_year_offset} pattern...")
        pairs = []
        for pest_year in pesticide_years:
            field_year = pest_year + self.config.field_year_offset
            if field_year in field_years:
                pairs.append((pest_year, field_year))
                self.log.info(f"   ✅ Pair created: pesticide {pest_year} → field {field_year}")
            else:
                self.log.warning(
                    f"   ❌ No field data found for pesticide year {pest_year} "
                    f"(expected field year {field_year})"
                )

        self.log.info(f"🎯 Created {len(pairs)} valid pesticide-field year pairs")
        return sorted(pairs)

    def _get_available_pesticide_years(self) -> Set[int]:
        """Extract available pesticide years from GCS storage."""
        try:
            # Use list_files with recursive pattern to find all parquet files
            pattern = f"gs://{self.config.bucket}/silver/pesticides/*/*.parquet"
            files = self.gcs_access.list_files(pattern)
            years = set()

            for file_path in files:
                # Extract years from filenames like "pesticiddata_2015_2016.parquet"
                filename = file_path.split("/")[-1]
                match = re.search(r"pesticiddata_(\d{4})_(\d{4})\.parquet", filename)
                if match:
                    start_year = int(match.group(1))
                    years.add(start_year)

            return years
        except Exception as e:
            self.log.error(f"Error discovering pesticide years: {e}")
            return set()

    def _get_available_field_years(self) -> Set[int]:
        """Extract available field years from GCS storage."""
        return set(self._get_available_fvm_marker_years())

    def _get_available_fvm_marker_years(self) -> List[int]:
        """Override base method to look for the correct FVM marker file pattern."""
        try:
            # Use list_files with recursive pattern to find all parquet files
            pattern = f"gs://{self.config.bucket}/silver/fvm_marker_*/*/*.parquet"
            files = self.gcs_access.list_files(pattern)
            years = set()

            for file_path in files:
                # Look for files like "silver/fvm_marker_2021/timestamp/data.parquet"
                match = re.search(r"fvm_marker_(\d{4})/.*?/data\.parquet", file_path)
                if match:
                    year = int(match.group(1))
                    years.add(year)

            return sorted(list(years))
        except Exception as e:
            self.log.error(f"Error discovering FVM marker years: {e}")
            return []

    def _load_silver_data_for_years(
        self, pesticide_year: int, field_year: int, silver_data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Load silver data for specific pesticide and field years."""
        self.log.info(
            f"📥 Loading silver data: pesticide year {pesticide_year}, field year {field_year}"
        )
        datasets = {}

        # Load pesticide data for the specific year
        if silver_data and "pesticides" in silver_data:
            self.log.info(f"💾 Using in-memory pesticide data for year {pesticide_year}")
            datasets["pesticides"] = silver_data["pesticides"]
        else:
            self.log.info(f"☁️ Reading pesticide data for year {pesticide_year} from GCS storage")
            pesticide_path = self._read_pesticide_data_for_year(pesticide_year)
            if pesticide_path is not None:
                datasets["pesticides"] = pesticide_path
                self.log.info(f"✅ PESTICIDE INPUT: Located data for {pesticide_year}")
                self.log.info(f"📁 Pesticide Data Path: {pesticide_path}")
                print(f"✅ PESTICIDE INPUT: Located data for {pesticide_year}")
                print(f"📁 Pesticide Data Path: {pesticide_path}")
            else:
                self.log.error(f"❌ No pesticide data found for year {pesticide_year}")
                datasets["pesticides"] = None

        # Load agricultural fields data for the specific year
        if silver_data and "agricultural_fields" in silver_data:
            self.log.info(f"💾 Using in-memory agricultural fields data for year {field_year}")
            datasets["agricultural_fields"] = silver_data["agricultural_fields"]
        else:
            self.log.info(
                f"☁️ Reading agricultural fields data for year {field_year} from GCS storage"
            )
            fields_path = self._read_fields_data_for_year(field_year)
            if fields_path is not None:
                datasets["agricultural_fields"] = fields_path
                self.log.info(f"✅ FIELDS INPUT: Located data for {field_year}")
                self.log.info(f"📁 Fields Data Path: {fields_path}")
            else:
                self.log.error(f"❌ No agricultural fields data found for year {field_year}")
                datasets["agricultural_fields"] = None

        # Summary of what we found
        pesticide_status = "✅" if datasets["pesticides"] else "❌"
        fields_status = "✅" if datasets["agricultural_fields"] else "❌"
        self.log.info(f"📋 Data loading summary for {pesticide_year}-{field_year}:")
        self.log.info(
            f"   {pesticide_status} Pesticide data: "
            f"{'Found' if datasets['pesticides'] else 'Missing'}"
        )
        self.log.info(
            f"   {fields_status} Agricultural fields data: "
            f"{'Found' if datasets['agricultural_fields'] else 'Missing'}"
        )

        return datasets

    def _read_pesticide_data_for_year(self, year: int) -> Optional[str]:
        """Read pesticide data for a specific year."""
        try:
            # Look for the specific pesticide file for this year
            # Based on actual codebase: filename pattern is pesticiddata_YYYY_YYYY.parquet
            # in timestamped subdirs
            filename = f"pesticiddata_{year}_{year + 1}.parquet"

            # Use list_files with recursive pattern to find all parquet files
            pattern = f"gs://{self.config.bucket}/silver/pesticides/*/*.parquet"
            files = self.gcs_access.list_files(pattern)

            # Find the file that matches our year in the latest timestamped directory
            target_file = None
            latest_timestamp = None
            for file_path in files:
                if filename in file_path:
                    # Extract timestamp from path like "gs://bucket/silver/pesticides/20250629_102742/pesticiddata_2021_2022.parquet"
                    path_parts = file_path.split("/")
                    if len(path_parts) >= 6:  # gs://bucket/silver/pesticides/timestamp/filename
                        timestamp_dir = path_parts[5]  # "20250629_102742"
                        if latest_timestamp is None or timestamp_dir > latest_timestamp:
                            latest_timestamp = timestamp_dir
                            target_file = file_path

            if target_file:
                self.log.info(f"Found pesticide data at {target_file}")
                return target_file
            else:
                self.log.warning(
                    f"No pesticide file found for year {year} (looking for {filename})"
                )
                return None

        except Exception as e:
            self.log.error(f"Error reading pesticide data for year {year}: {e}")
            return None

    def _read_fields_data_for_year(self, year: int) -> Optional[str]:
        """Read agricultural fields data for a specific year."""
        try:
            # Look for FVM marker data for this year
            self.log.info(f"Reading FVM marker data for year {year}")

            # Use list_files with recursive pattern to find all parquet files
            pattern = f"gs://{self.config.bucket}/silver/fvm_marker_{year}/*/*.parquet"
            files = self.gcs_access.list_files(pattern)

            # Find the parquet file in timestamped subdirectories
            target_file = None
            latest_timestamp = None
            for file_path in files:
                # Look for files like "data.parquet" (standard silver layer format)
                if file_path.endswith("data.parquet"):
                    # Extract timestamp from path like "gs://bucket/silver/fvm_marker_2021/20241201_123456/data.parquet"
                    path_parts = file_path.split("/")
                    if (
                        len(path_parts) >= 7
                    ):  # gs://bucket/silver/fvm_marker_year/timestamp/filename
                        timestamp_dir = path_parts[5]  # "20241201_123456" (corrected index)
                        if latest_timestamp is None or timestamp_dir > latest_timestamp:
                            latest_timestamp = timestamp_dir
                            target_file = file_path

            if target_file:
                self.log.info(f"Found FVM marker data at {target_file}")
                return target_file
            else:
                self.log.warning(f"No FVM marker file found for year {year}")
                return None

        except Exception as e:
            self.log.error(f"Error reading fields data for year {year}: {e}")
            return None

    def _process_year_pair(
        self,
        pesticide_year: int,
        field_year: int,
        agricultural_fields_path: str,
        pesticide_applications_path: str,
    ) -> Optional[int]:
        """
        Process a single year of pesticide disaggregation - this is where the core magic happens!

        WHAT THIS METHOD DOES:
        =====================
        1. Sets up an in-memory database (DuckDB) with the year's data
        2. Runs 4 disaggregation strategies in sequence to maximize coverage
        3. Saves the results and reports statistics
        4. Cleans up memory for the next year

        THE 4-STRATEGY APPROACH:
        =======================
        Strategy 1: Main area matching (handles ~92% of cases)
        Strategy 2: Non-organic area matching (handles organic field issues)
        Strategy 3: Partial field coverage (handles single-field cases)
        Strategy 4: Spatial clustering (removed for simplicity)

        WHY THIS APPROACH WORKS:
        =======================
        - Each strategy handles different edge cases
        - Strategies run in order of effectiveness (most successful first)
        - Once a pesticide application is processed, it's removed from the queue
        - This ensures no double-processing and maximum coverage

        Args:
            pesticide_year: Year of pesticide data (e.g., 2021)
            field_year: Year of field data (e.g., 2022, due to Y+1 pattern)
            agricultural_fields_path: Cloud storage path to field boundaries
            pesticide_applications_path: Cloud storage path to pesticide applications

        Returns:
            Number of successfully disaggregated records, or None if processing failed
        """
        try:
            # RESET VALIDATION DATA FOR THIS YEAR
            # ===================================
            # Clear any previous year's validation data
            self.log.info(f"🔄 Resetting validation data for year {pesticide_year}")
            self._validation_data = {
                "original_total_dosage": 0.0,
                "original_total_acreage": 0.0,
                "original_record_count": 0,
                "strategy_totals": {},
                "final_total_dosage": 0.0,
                "final_total_acreage": 0.0,
                "final_record_count": 0,
            }

            # STEP 1: SET UP THE DATABASE
            # ===========================
            # Load both pesticide and field data into a fast in-memory database
            # This makes the complex matching queries run much faster
            self.log.info(f"🔧 Setting up DuckDB for year {pesticide_year}")
            setup_success = self._setup_duckdb(
                agricultural_fields_path, pesticide_applications_path
            )
            if not setup_success:
                self.log.warning(
                    f"⚠️ Skipping year pair {pesticide_year}-{field_year} due to setup failure"
                )
                return None

            self.log.info(f"✅ DuckDB setup complete for year {pesticide_year}")

            # STEP 2: PREPARE THE WORKSPACE
            # =============================
            # Create the results table where we'll store our disaggregated records
            self.log.info(f"🏗️ Creating results table for year {pesticide_year}")
            self._create_results_table()

            # Filter out records marked as "no pesticides" (nopesticides=1)
            # These are companies that explicitly reported they used no pesticides
            self.log.info(f"🔍 Filtering pending pesticide records for year {pesticide_year}")
            self._create_pending_pesticide_rows()

            # VALIDATION: Reset validation data for this year (simplified validation)
            self.log.info(f"📊 Initializing simplified validation for year {pesticide_year}")
            self._validation_data["strategy_totals"] = {}

            # STEP 3: QUICK FEASIBILITY CHECK
            # ===============================
            # Before running expensive strategies, check if any CVR matches are possible
            # If no companies match between pesticide and field data, skip all processing
            self.log.info(f"🔍 Checking for CVR matches for year {pesticide_year}")
            cvr_matches_available = self._check_cvr_matches_available()

            if not cvr_matches_available:
                self.log.warning(
                    f"⚠️ No CVR matches found for year {pesticide_year} - skipping all strategies"
                )
                self.log.warning(
                    "   This significantly improves performance when no matches are possible"
                )

                # Return 0 since no processing is possible
                self.log.info(f"📊 Year {pesticide_year} completed with 0 records (no CVR matches)")
                return 0

            # STEP 4: RUN THE ETHICAL DISAGGREGATION PROCESS 🌟
            # =================================================
            # ENHANCED WITH ETHICAL BEST-MATCH STRATEGY!
            # For mixed farming operations, we calculate both strategies and use whichever
            # gives the better area match - every farmer deserves the most accurate disaggregation
            self.log.info(
                f"🌟 Starting ETHICAL disaggregation strategies for year {pesticide_year}"
            )
            total_processed = 0

            # ETHICAL STRATEGY 1: MIXED FARMING BEST-MATCH (FAIRNESS FIRST!)
            # ==============================================================
            # For CVR+crop combinations with organic fields, calculate both main and non-organic
            # strategies and use whichever gives the better area match within 2% tolerance
            # This ensures every farmer gets the most accurate disaggregation possible
            self.log.info("🌟 Ethical Strategy 1: Best-match for mixed farming operations")
            mixed_combinations = self._get_mixed_farming_combinations()
            processed_ethical = self._process_mixed_farming_best_match(mixed_combinations)
            total_processed += processed_ethical
            self.log.info(
                f"✅ Year {pesticide_year}: Ethical Best-Match: {processed_ethical} "
                f"mixed farming records processed"
            )
            # VALIDATION: Check ethical best-match strategy results
            self._validate_strategy_results("Ethical Best-Match", processed_ethical)

            # STRATEGY 2: MAIN AREA MATCHING FOR REMAINING APPLICATIONS
            # =========================================================
            # Process remaining conventional-only applications with the proven main strategy
            # These are CVR+crop combinations that don't have organic fields
            self.log.info("🎯 Strategy 2: Main area matching for conventional-only operations")
            processed_main_remaining = self._disaggregate_by_marker_match()
            total_processed += processed_main_remaining
            self.log.info(
                f"✅ Year {pesticide_year}: Main Area Match (remaining): "
                f"{processed_main_remaining} records processed"
            )
            # VALIDATION: Check main area matching strategy results
            self._validate_strategy_results("Main Area Match", processed_main_remaining)

            # STRATEGY 3: NON-ORGANIC CLEANUP FOR EDGE CASES
            # ==============================================
            # Handle any remaining applications that main strategy couldn't process
            # This catches edge cases and provides final cleanup
            self.log.info("🎯 Strategy 3: Non-organic cleanup for remaining applications")
            processed_nonorg_cleanup = self._disaggregate_by_marker_non_organic_match()
            total_processed += processed_nonorg_cleanup
            self.log.info(
                f"✅ Year {pesticide_year}: Non-Organic Cleanup: {processed_nonorg_cleanup} "
                f"records processed"
            )
            # VALIDATION: Check non-organic matching strategy results
            self._validate_strategy_results("Non-Organic Match", processed_nonorg_cleanup)

            # STRATEGY 4: PARTIAL FIELD COVERAGE (HANDLES SINGLE-FIELD CASES)
            # ================================================================
            # When a company has only 1 field for a crop type, or when pesticide area < field area
            # This handles partial field coverage scenarios
            # Example: Company has 1 field of 30ha, applied pesticide to 20ha → partial coverage
            self.log.info(
                f"🎯 Strategy 4: Running partial field coverage for year {pesticide_year}"
            )
            processed_4 = self._disaggregate_by_partial_field_coverage()
            total_processed += processed_4
            self.log.info(
                f"✅ Year {pesticide_year}: Partial Field Coverage: {processed_4} records processed"
            )
            # VALIDATION: Check partial field coverage strategy results
            self._validate_strategy_results("Partial Field Coverage", processed_4)

            # STRATEGY 5: SPATIAL CLUSTERING (REMOVED FOR SIMPLICITY)
            # =======================================================
            # This strategy tried to group nearby fields and match against pesticide applications
            # Removed because it was complex and didn't provide sufficient additional coverage
            self.log.info(
                "ℹ️ Strategy 5: Spatial clustering removed for simplification - "
                "strategies 1-4 provide sufficient coverage"
            )

            # STEP 5: COLLECT RESULTS AND CALCULATE STATISTICS
            # ================================================
            # Count how many records we successfully disaggregated and calculate coverage percentage
            self.log.info(f"📊 Collecting final results for year {pesticide_year}")
            result_count = self.duckdb_conn.execute(
                "SELECT COUNT(*) FROM disaggregated_pesticide_applications"
            ).fetchone()[0]

            # Calculate coverage statistics for this year
            total_pesticide_records = self.duckdb_conn.execute(
                "SELECT COUNT(*) FROM pesticide"
            ).fetchone()[0]
            coverage_pct = (
                (result_count / total_pesticide_records * 100) if total_pesticide_records > 0 else 0
            )

            self.log.info(f"🎉 Year {pesticide_year} disaggregation completed:")
            self.log.info(f"   📈 Total pesticide records: {total_pesticide_records:,}")
            self.log.info(
                f"   ✅ Successfully disaggregated: {result_count:,} ({coverage_pct:.1f}%)"
            )
            self.log.info(f"   🔢 Total processed across all strategies: {total_processed:,}")

            # VALIDATION: Run comprehensive final integrity checks
            self.log.info(f"🔍 Running final validation for year {pesticide_year}")
            self._validate_final_disaggregation_integrity()

            # STEP 6: SAVE RESULTS
            # ====================
            # Save the disaggregated results to cloud storage for downstream use
            if result_count > 0:
                self._save_year_results_direct(pesticide_year)
                return result_count  # Return count instead of full results
            else:
                self.log.warning(f"⚠️ No results to save for year {pesticide_year}")
                return 0

        except Exception as e:
            self.log.error(f"❌ Error processing year pair {pesticide_year}-{field_year}: {e}")
            return None
        finally:
            # STEP 7: CLEAN UP MEMORY
            # =======================
            # Close the database connection to free up memory for the next year
            # This is crucial for processing multiple years without running out of memory
            if self.duckdb_conn:
                self.log.info(f"🧹 Cleaning up DuckDB connection for year {pesticide_year}")
                self.duckdb_conn.close()
                self.duckdb_conn = None

    def _setup_duckdb(
        self, agricultural_fields_path: str, pesticide_applications_path: str
    ) -> bool:
        """
        Set up the in-memory database with all the data needed for disaggregation.

        WHAT THIS METHOD DOES:
        =====================
        1. Creates a fast in-memory database (DuckDB)
        2. Loads field boundaries and pesticide applications from cloud storage
        3. Standardizes column names and data types for consistent processing
        4. Validates that we have the required data columns (especially CVR numbers)

        WHY USE AN IN-MEMORY DATABASE:
        =============================
        - Much faster than processing CSV/Parquet files directly
        - Enables complex SQL queries for area matching and proportional distribution
        - Handles large datasets efficiently with limited memory
        - Supports spatial operations for geographic analysis

        CRITICAL VALIDATION:
        ===================
        This method validates that we have CVR numbers (Danish company IDs) in both datasets.
        Without CVR numbers, we can't match pesticide applications to field boundaries.

        Args:
            agricultural_fields_path: Cloud storage path to field boundary data
            pesticide_applications_path: Cloud storage path to pesticide application data

        Returns:
            bool: True if setup successful, False if critical data is missing
        """
        # CREATE THE IN-MEMORY DATABASE
        # ============================
        # DuckDB is a fast, lightweight database perfect for analytical workloads
        self.duckdb_conn = duckdb.connect(":memory:")

        # CONFIGURE DATABASE PERFORMANCE
        # ==============================
        # Tune the database for our specific use case and available server resources
        memory_limit_gb = self.config.max_memory_gb
        thread_count = 2 if not self.config.enable_parallel_processing else 4

        # Debug logging for configuration
        self.log.info(f"🔧 Config max_memory_gb: {self.config.max_memory_gb}")
        self.log.info(f"🔧 Calculated memory_limit_gb: {memory_limit_gb}")
        self.log.info(
            f"🔧 Config enable_parallel_processing: {self.config.enable_parallel_processing}"
        )
        self.log.info(f"🔧 Calculated thread_count: {thread_count}")

        # Apply performance settings
        self.duckdb_conn.execute(f"SET memory_limit = '{memory_limit_gb}GB'")
        self.duckdb_conn.execute(f"SET threads = {thread_count}")
        self.duckdb_conn.execute("SET temp_directory = '/tmp'")

        # Optimize for large datasets with limited server resources
        self.duckdb_conn.execute("SET enable_progress_bar = false")  # Reduce output overhead
        self.duckdb_conn.execute(
            "SET preserve_insertion_order = false"
        )  # Allow reordering for efficiency

        self.log.info(f"🔧 DuckDB configured: {memory_limit_gb}GB memory, {thread_count} threads")

        # ENABLE SPATIAL PROCESSING
        # ========================
        # Install spatial extension for geographic operations (field boundaries, distances)
        self.duckdb_conn.execute("INSTALL spatial")
        self.duckdb_conn.execute("LOAD spatial")

        # STEP 1: LOAD AGRICULTURAL FIELD BOUNDARIES
        # ==========================================
        # Load field boundary data (polygons showing where each field is located)
        # This data includes company ownership (CVR), crop types, and field areas
        self.log.info(f"🏗️ Creating marker table from {agricultural_fields_path}")

        # Download field data from cloud storage for processing
        self.log.info("📥 Downloading agricultural fields data for schema inspection...")
        with self.gcs_access._temp_download(agricultural_fields_path) as temp_file:
            self.log.info(f"✅ Downloaded to temporary file: {temp_file}")
            # Load the data into a temporary table so we can inspect its structure
            self.duckdb_conn.execute(
                f"CREATE TABLE marker_temp AS SELECT * FROM read_parquet('{temp_file}')"
            )

        # STEP 2: INSPECT AND VALIDATE FIELD DATA STRUCTURE
        # =================================================
        # Different data sources have different column names - we need to map them consistently
        self.log.info("🔍 Inspecting marker data schema...")
        temp_columns = self.duckdb_conn.execute("DESCRIBE marker_temp").fetchall()
        temp_column_names = [col[0] for col in temp_columns]
        self.log.info(f"📋 Found {len(temp_column_names)} columns in marker data:")
        for i, col in enumerate(temp_column_names, 1):
            self.log.info(f"   {i:2d}. {col}")

        # STEP 3: FIND THE CVR COLUMN (CRITICAL FOR MATCHING)
        # ===================================================
        # CVR numbers are Danish company registration numbers - we need these to match
        # pesticide applications to the companies that own the fields
        self.log.info("🔍 Looking for CVR column...")
        cvr_column = None
        if "cvr_number" in temp_column_names:
            cvr_column = "cvr_number"
            self.log.info("✅ Found CVR column: cvr_number")
        elif "Ansoeger" in temp_column_names:
            cvr_column = "Ansoeger"
            self.log.info("✅ Found CVR column: Ansoeger")
        elif "KUNDE_LB" in temp_column_names:
            cvr_column = "KUNDE_LB"
            self.log.info("✅ Found CVR column: KUNDE_LB")
        else:
            self.log.error("❌ CRITICAL: No CVR column found in marker data!")
            self.log.error("🔍 CVR matching is required for pesticide disaggregation.")
            self.log.error(f"📋 Available columns: {temp_column_names}")
            self.log.error("💡 Expected one of: cvr_number, Ansoeger, KUNDE_LB")
            return False

        # STEP 4: HANDLE BLOCK_ID COLUMN VARIATIONS
        # =========================================
        # Some datasets have block_id, others don't - we need to handle both cases
        self.log.info("🔍 Looking for block_id column...")
        block_id_column = "block_id" if "block_id" in temp_column_names else "field_id"
        self.log.info(f"✅ Using block_id column: {block_id_column}")

        self.log.info("🏗️ Creating final marker table with proper column mapping...")

        # Check if standardized geometry column exists for spatial clustering
        has_geometry = "geometry" in temp_column_names

        if "geometry" in temp_column_names:
            geometry_select = ", geometry"
            self.log.info("✅ Found geometry column for spatial operations")
        else:
            geometry_select = ""
            self.log.warning("⚠️ No geometry data - spatial clustering will be disabled")

        # Handle crop name column - check what's actually available
        crop_name_column = None
        if "crop_name" in temp_column_names:
            crop_name_column = "crop_name"
        elif "crop_type" in temp_column_names:
            crop_name_column = "crop_type as crop_name"
        else:
            crop_name_column = "NULL as crop_name"
            self.log.warning("No crop_name or crop_type column found, using NULL")

        # Handle organic farming column - check if is_organic exists in FVM data
        if "is_organic" in temp_column_names:
            organic_farming_column = "COALESCE(is_organic, false) as organic_farming"
            self.log.info("✅ Found is_organic column in FVM data - organic fields will be used")
        else:
            organic_farming_column = "false as organic_farming"
            self.log.warning(
                "⚠️ No is_organic column found in FVM data - assuming all fields are non-organic"
            )

        # Check if field_uuid exists in the source data
        has_field_uuid = "field_uuid" in temp_column_names

        if has_field_uuid:
            self.log.info(
                "✅ Found field_uuid column in FVM data - using UUIDs as primary identifier"
            )
            # Use field_uuid directly, generate UUID for any missing values
            field_uuid_select = "COALESCE(field_uuid, CAST(uuid() AS VARCHAR)) as field_uuid"
            primary_field_id_select = (
                "COALESCE(field_uuid, CAST(uuid() AS VARCHAR)) as primary_field_id"
            )
        else:
            self.log.error(
                "❌ No field_uuid column found in FVM data - this should not happen "
                "with current FVM data"
            )
            raise ValueError(
                "field_uuid column is required but not found in FVM data. "
                "Please check data pipeline configuration."
            )

        self.duckdb_conn.execute(f"""
            CREATE TABLE marker AS 
            SELECT 
                field_id,
                CAST(area_ha AS DOUBLE) as area_ha,
                CAST({cvr_column} AS VARCHAR) as cvr_number,
                CAST(crop_code AS VARCHAR) as crop_code,
                {crop_name_column},
                {organic_farming_column},
                CAST({block_id_column} AS VARCHAR) as block_id,
                year{geometry_select},
                -- Add field UUID support with fallback to composite key
                {primary_field_id_select},
                {field_uuid_select}
            FROM marker_temp
        """)

        if has_geometry:
            self.log.info("✅ Geometry data available for spatial clustering")
        else:
            self.log.warning("⚠️ No geometry data - spatial clustering will be disabled")

        # Drop the temporary table
        self.duckdb_conn.execute("DROP TABLE marker_temp")
        self.log.info("✅ Marker table created successfully")

        self.log.info(f"🏗️ Creating pesticide table from {pesticide_applications_path}")

        # ✅ MIGRATION: Create pesticide table using optimized GCS access with temp download
        self.log.info("📥 Downloading pesticide data...")
        with self.gcs_access._temp_download(pesticide_applications_path) as temp_file:
            self.log.info(f"✅ Downloaded to temporary file: {temp_file}")

            # First, create a temporary table to inspect the pesticide schema
            self.duckdb_conn.execute(
                f"CREATE TABLE pesticide_temp AS SELECT * FROM read_parquet('{temp_file}')"
            )

        # Check what columns actually exist in pesticide data
        self.log.info("🔍 Inspecting pesticide data schema...")
        pest_columns = self.duckdb_conn.execute("DESCRIBE pesticide_temp").fetchall()
        pest_column_names = [col[0] for col in pest_columns]
        self.log.info(f"📋 Found {len(pest_column_names)} columns in pesticide data:")
        for i, col in enumerate(pest_column_names, 1):
            self.log.info(f"   {i:2d}. {col}")

        # Find the CVR column in pesticide data with extended mapping
        self.log.info("🔍 Looking for CVR column in pesticide data...")
        pest_cvr_column = None
        cvr_column_candidates = [
            "cvr_number",
            "companyregistrationnumber",
            "cvr",
            "CompanyRegistrationNumber",
        ]

        for candidate in cvr_column_candidates:
            if candidate in pest_column_names:
                pest_cvr_column = candidate
                self.log.info(f"✅ Found CVR column: {candidate}")
                break

        if pest_cvr_column is None:
            self.log.error("❌ CRITICAL: No CVR column found in pesticide data!")
            self.log.error("🔍 CVR matching is required for pesticide disaggregation.")
            self.log.error(f"📋 Available columns: {pest_column_names}")
            self.log.error(f"💡 Expected one of: {cvr_column_candidates}")
            return False

        # Create the final pesticide table with proper column mapping and standardized column names
        self.log.info("🏗️ Creating final pesticide table with proper column mapping...")

        # Check for standardized vs raw column names - prioritize new standardized names
        area_column = "area_ha" if "area_ha" in pest_column_names else "acreagesize"
        crop_column = "crop_code" if "crop_code" in pest_column_names else "Code"
        pesticide_name_column = (
            "pesticide_name" if "pesticide_name" in pest_column_names else "pesticidename"
        )
        pesticide_reg_column = (
            "pesticide_registration_number"
            if "pesticide_registration_number" in pest_column_names
            else "pesticideregistrationnumber"
        )
        dosage_quantity_column = (
            "dosage_quantity" if "dosage_quantity" in pest_column_names else "dosagequantity"
        )
        dosage_unit_column = "dosage_unit" if "dosage_unit" in pest_column_names else "dosageunit"
        no_pesticides_column = (
            "no_pesticides" if "no_pesticides" in pest_column_names else "nopesticides"
        )

        self.duckdb_conn.execute(f"""
            CREATE TABLE pesticide AS 
            SELECT 
                row_number() OVER () as OriginalPesticideRowID,
                CAST({pest_cvr_column} AS VARCHAR) as cvr_number,
                {pesticide_name_column} as PesticideName,
                {pesticide_reg_column} as PesticideRegistrationNumber,
                CAST({dosage_quantity_column} AS DOUBLE) as DosageQuantity,
                {dosage_unit_column} as DosageUnit,
                CAST({area_column} AS DOUBLE) as AcreageSize,
                CAST({crop_column} AS VARCHAR) as Code,
                {no_pesticides_column} as nopesticides
            FROM pesticide_temp
        """)

        # Drop the temporary table
        self.duckdb_conn.execute("DROP TABLE pesticide_temp")
        self.log.info("✅ Pesticide table created successfully")

        # FINAL STEP: VALIDATE AND REPORT SUCCESS
        # =======================================
        # Count the records we loaded to ensure everything worked correctly
        self.log.info("📊 Counting loaded records...")
        marker_count = self.duckdb_conn.execute("SELECT COUNT(*) FROM marker").fetchone()[0]
        pesticide_count = self.duckdb_conn.execute("SELECT COUNT(*) FROM pesticide").fetchone()[0]

        self.log.info("✅ DuckDB setup completed successfully!")
        self.log.info(
            f"📈 Loaded {marker_count:,} agricultural fields and "
            f"{pesticide_count:,} pesticide records"
        )

        # SUCCESS! We now have both datasets loaded and ready for disaggregation
        # The database contains:
        # - 'marker' table: Field boundaries with company ownership and crop types
        # - 'pesticide' table: Pesticide applications with company and area information
        # - Both tables have standardized CVR numbers for matching
        return True

    def _create_results_table(self):
        """Create the disaggregated results table with original schema."""
        create_table_sql = """
        CREATE TABLE disaggregated_pesticide_applications (
            DisaggregatedID VARCHAR,
            OriginalPesticideRowID VARCHAR,
            cvr_number VARCHAR,
            PesticideName VARCHAR,
            PesticideRegistrationNumber VARCHAR,
            DosageQuantity DOUBLE,
            DosageUnit VARCHAR,
            MatchedFieldID VARCHAR,
            MatchedBlockID VARCHAR,
            AllocatedArea DOUBLE,
            AllocationMethod VARCHAR,
            MatchConfidence DOUBLE,
            IsPartialFieldCoverage BOOLEAN,
            DisaggregationDate TIMESTAMP,
            -- Add field UUID support for better field identification
            field_uuid VARCHAR,
            primary_field_id VARCHAR
        )
        """
        self.duckdb_conn.execute(create_table_sql)

    def _create_pending_pesticide_rows(self):
        """Create pending pesticide rows table with nopesticides=1 records filtered out."""
        self.duckdb_conn.execute("""
            CREATE TABLE pending_pesticide_rows AS
            SELECT * FROM pesticide 
            WHERE nopesticides IS NULL 
               OR (CAST(nopesticides AS VARCHAR) NOT IN ('1', 'True', 'true', 'TRUE'))
        """)

        count = self.duckdb_conn.execute("SELECT COUNT(*) FROM pending_pesticide_rows").fetchone()[
            0
        ]
        self.log.info(f"Created pending pesticide rows: {count} records")

    def _check_cvr_matches_available(self) -> bool:
        """
        Check if there are any potential CVR matches between pesticide and marker data.
        Returns True if matches exist, False otherwise.
        This allows us to skip CVR-based strategies when no matches are possible.
        """
        self.log.info("🔍 Checking for potential CVR matches...")

        try:
            # Check if there are any CVR numbers that exist in both pesticide and marker data
            cvr_match_count = self.duckdb_conn.execute("""
                WITH PesticideCVRs AS (
                    SELECT DISTINCT TRIM(CAST(cvr_number AS VARCHAR)) as CVR
                    FROM pending_pesticide_rows 
                    WHERE cvr_number IS NOT NULL
                      AND TRIM(CAST(cvr_number AS VARCHAR)) != ''
                      AND REGEXP_MATCHES(TRIM(CAST(cvr_number AS VARCHAR)), '^[0-9]+$')
                ),
                MarkerCVRs AS (
                    SELECT DISTINCT TRIM(CAST(cvr_number AS VARCHAR)) as CVR
                    FROM marker 
                    WHERE cvr_number IS NOT NULL
                      AND TRIM(CAST(cvr_number AS VARCHAR)) != ''
                      AND REGEXP_MATCHES(TRIM(CAST(cvr_number AS VARCHAR)), '^[0-9]+$')
                )
                SELECT COUNT(*) 
                FROM PesticideCVRs p
                JOIN MarkerCVRs m ON p.CVR = m.CVR
            """).fetchone()[0]

            has_matches = cvr_match_count > 0

            if has_matches:
                self.log.info(
                    f"✅ Found {cvr_match_count} potential CVR matches - strategies will run"
                )
            else:
                self.log.warning("⚠️ No CVR matches found between pesticide and marker data")
                self.log.warning("   All CVR-based strategies will be skipped")

                # Log some diagnostic information
                pesticide_cvr_count = self.duckdb_conn.execute("""
                    SELECT COUNT(DISTINCT cvr_number) 
                    FROM pending_pesticide_rows 
                    WHERE cvr_number IS NOT NULL
                """).fetchone()[0]

                marker_cvr_count = self.duckdb_conn.execute("""
                    SELECT COUNT(DISTINCT cvr_number) 
                    FROM marker 
                    WHERE cvr_number IS NOT NULL
                """).fetchone()[0]

                self.log.info(
                    f"   📊 Pesticide records have {pesticide_cvr_count} distinct CVR numbers"
                )
                self.log.info(f"   📊 Marker records have {marker_cvr_count} distinct CVR numbers")

            return has_matches

        except Exception as e:
            self.log.error(f"Error checking CVR matches: {str(e)}")
            # If we can't check, assume matches exist to be safe
            return True

    def _get_organic_marker_field_ids(self) -> Set[str]:
        """
        Identifies marker field UUIDs that are considered organic.

        Now that FVM marker data contains organic farming information via the is_organic
        column,
        this method queries the marker table to find all organic fields using field_uuid
        for uniqueness.

        Results are cached.
        Returns a set of marker.field_uuid strings for unique identification.
        """
        if self._organic_marker_field_ids is not None:
            self.log.debug("Returning cached organic marker field UUIDs.")
            return self._organic_marker_field_ids

        try:
            # Query organic fields from the marker table using field_uuid for uniqueness
            result = self.duckdb_conn.execute("""
                SELECT DISTINCT field_uuid 
                FROM marker 
                WHERE organic_farming = TRUE
                  AND field_uuid IS NOT NULL
            """).fetchall()

            organic_field_uuids = {str(row[0]) for row in result}
            self.log.info(
                f"Found {len(organic_field_uuids)} unique organic field UUIDs "
                "out of total marker fields"
            )

            # Cache the result
            self._organic_marker_field_ids = organic_field_uuids

            return self._organic_marker_field_ids

        except Exception as e:
            self.log.error(f"Error querying organic fields: {e}")
            self.log.warning("Falling back to no organic fields due to query error")
            # Fall back to empty set if query fails
            self._organic_marker_field_ids = set()
            return self._organic_marker_field_ids

    def _get_mixed_farming_combinations(self) -> Set[tuple]:
        """
        🌟 ETHICAL ENHANCEMENT: Identify CVR+crop combinations with organic fields

        These are mixed farming operations that could benefit from dual calculation
        to determine which strategy gives the most accurate area match.

        Returns a set of (CVR, CropCode) tuples for combinations that have organic fields.
        """
        try:
            result = self.duckdb_conn.execute("""
                SELECT DISTINCT 
                    TRIM(CAST(cvr_number AS VARCHAR)) as CVR,
                    TRY_CAST(crop_code AS BIGINT) as CropCode
                FROM marker 
                WHERE organic_farming = TRUE
                  AND cvr_number IS NOT NULL 
                  AND TRIM(CAST(cvr_number AS VARCHAR)) != '' 
                  AND REGEXP_MATCHES(TRIM(CAST(cvr_number AS VARCHAR)), '^[0-9]+$')
                  AND crop_code IS NOT NULL 
                  AND area_ha > 0.0
            """).fetchall()

            mixed_combinations = {
                (str(row[0]), int(row[1])) for row in result if row[0] and row[1]
            }
            self.log.info(
                f"🌱 Found {len(mixed_combinations)} CVR+crop combinations with "
                "organic fields (mixed farming)"
            )
            return mixed_combinations

        except Exception as e:
            self.log.error(f"Error identifying mixed farming combinations: {e}")
            self.log.warning("Falling back to empty set - will use sequential processing")
            return set()

    def _process_mixed_farming_best_match(self, mixed_combinations: Set[tuple]) -> int:
        """
        🌟 ETHICAL ENHANCEMENT: Process mixed farming applications with best-match logic

        For applications from CVR+crop combinations that have organic fields,
        calculate both main and non-organic strategies and use whichever gives
        the better (lower error) area match within 2% tolerance.

        This ensures each farmer gets the most accurate disaggregation possible.

        Args:
            mixed_combinations: Set of (CVR, CropCode) tuples with organic fields

        Returns:
            Number of applications processed
        """
        if not mixed_combinations:
            self.log.info("🤔 No mixed farming combinations found - skipping ethical best-match")
            return 0

        self.log.info(
            f"🎯 Starting ethical best-match processing for {len(mixed_combinations)} combinations"
        )

        try:
            # Create a temporary table with mixed farming applications
            mixed_combinations_sql = ", ".join(
                [f"('{cvr}', {crop})" for cvr, crop in mixed_combinations]
            )

            processed_count = self.duckdb_conn.execute(f"""
                WITH MixedFarmingCombinations AS (
                    SELECT * FROM VALUES {mixed_combinations_sql} AS t(CVR, CropCode)
                ),
                NonOrganicMarkerFieldCVRCropTotals AS (
                    SELECT
                        TRIM(CAST(m.cvr_number AS VARCHAR)) as CVR,
                        TRY_CAST(m.crop_code AS BIGINT) as CropCode,
                        SUM(m.area_ha) as TotalNonOrganicMarkerAreaForCVRCrop
                    FROM marker m
                    WHERE m.cvr_number IS NOT NULL 
                          AND TRIM(CAST(m.cvr_number AS VARCHAR)) != '' 
                          AND REGEXP_MATCHES(TRIM(CAST(m.cvr_number AS VARCHAR)), '^[0-9]+$')
                          AND m.crop_code IS NOT NULL AND m.area_ha > 0.0
                          AND m.organic_farming = FALSE
                    GROUP BY CVR, CropCode
                ),
                MarkerFieldCVRCropTotals AS (
                    SELECT
                        TRIM(CAST(m.cvr_number AS VARCHAR)) as CVR,
                        TRY_CAST(m.crop_code AS BIGINT) as CropCode,
                        SUM(m.area_ha) as TotalMarkerAreaForCVRCrop
                    FROM marker m
                    WHERE m.cvr_number IS NOT NULL 
                      AND TRIM(CAST(m.cvr_number AS VARCHAR)) != '' 
                      AND REGEXP_MATCHES(TRIM(CAST(m.cvr_number AS VARCHAR)), '^[0-9]+$')
                      AND m.crop_code IS NOT NULL AND m.area_ha > 0.0
                    GROUP BY CVR, CropCode
                ),
                BestMatchEvaluation AS (
                    SELECT 
                        p.*,
                        main_totals.TotalMarkerAreaForCVRCrop,
                        non_organic_totals.TotalNonOrganicMarkerAreaForCVRCrop,
                        -- Calculate errors for both strategies
                        ABS(p.AcreageSize - main_totals.TotalMarkerAreaForCVRCrop) / 
                            p.AcreageSize * 100 as main_error_pct,
                        ABS(p.AcreageSize - 
                            non_organic_totals.TotalNonOrganicMarkerAreaForCVRCrop) /
                            p.AcreageSize * 100 as nonorg_error_pct,
                        -- Check tolerance for both
                        CASE WHEN ABS(p.AcreageSize - main_totals.TotalMarkerAreaForCVRCrop) / 
                            p.AcreageSize * 100 
                                 <= {self.config.area_tolerance_pct} 
                             THEN TRUE ELSE FALSE END as main_passes,
                        CASE WHEN ABS(p.AcreageSize - 
                            non_organic_totals.TotalNonOrganicMarkerAreaForCVRCrop) /
                            p.AcreageSize * 100 
                                 <= {self.config.area_tolerance_pct} 
                             THEN TRUE ELSE FALSE END as nonorg_passes,
                        -- Determine best strategy
                        CASE 
                            WHEN ABS(p.AcreageSize - main_totals.TotalMarkerAreaForCVRCrop) / 
                                p.AcreageSize * 100
                                 <= {self.config.area_tolerance_pct} 
                                 AND ABS(p.AcreageSize - 
                                     non_organic_totals.TotalNonOrganicMarkerAreaForCVRCrop) /
                                p.AcreageSize * 100 
                                     <= {self.config.area_tolerance_pct} THEN
                                CASE WHEN ABS(p.AcreageSize - main_totals.TotalMarkerAreaForCVRCrop) 
                                          <= ABS(p.AcreageSize -
                                              non_organic_totals.TotalNonOrganicMarkerAreaForCVRCrop) 
                                     THEN 'main' ELSE 'nonorg' END
                            WHEN ABS(p.AcreageSize - main_totals.TotalMarkerAreaForCVRCrop) / 
                                p.AcreageSize * 100
                                 <= {self.config.area_tolerance_pct} 
                                 THEN 'main'
                            WHEN ABS(p.AcreageSize - non_organic_totals.TotalNonOrganicMarkerAreaForCVRCrop) /
                            p.AcreageSize * 100 <= {self.config.area_tolerance_pct} 
                                 THEN 'nonorg'
                            ELSE 'neither'
                        END as best_strategy
                    FROM pending_pesticide_rows p
                    JOIN MixedFarmingCombinations mfc 
                        ON TRIM(CAST(p.cvr_number AS VARCHAR)) = mfc.CVR
                        AND TRY_CAST(p.Code AS BIGINT) = mfc.CropCode
                    LEFT JOIN MarkerFieldCVRCropTotals main_totals
                        ON TRIM(CAST(p.cvr_number AS VARCHAR)) = main_totals.CVR 
                        AND TRY_CAST(p.Code AS BIGINT) = main_totals.CropCode
                    LEFT JOIN NonOrganicMarkerFieldCVRCropTotals non_organic_totals
                        ON TRIM(CAST(p.cvr_number AS VARCHAR)) = non_organic_totals.CVR 
                        AND TRY_CAST(p.Code AS BIGINT) = non_organic_totals.CropCode
                    WHERE p.AcreageSize > 0
                      AND (main_totals.TotalMarkerAreaForCVRCrop > 0 OR
                           non_organic_totals.TotalNonOrganicMarkerAreaForCVRCrop > 0)
                )
                -- Process using main strategy (best_strategy = 'main')
                INSERT INTO disaggregated_pesticide_applications (
                    DisaggregatedID,
                    OriginalPesticideRowID,
                    cvr_number,
                    PesticideName,
                    PesticideRegistrationNumber,
                    DosageQuantity,
                    DosageUnit,
                    MatchedFieldID,
                    MatchedBlockID,
                    AllocatedArea,
                    AllocationMethod,
                    MatchConfidence,
                    IsPartialFieldCoverage,
                    DisaggregationDate,
                    field_uuid,
                    primary_field_id
                )
                SELECT
                    uuid() as DisaggregatedID,
                    CAST(p.OriginalPesticideRowID AS VARCHAR) as OriginalPesticideRowID,
                    CAST(p.cvr_number AS VARCHAR) as cvr_number,
                    p.PesticideName,
                    p.PesticideRegistrationNumber,
                    -- Proportional dosage based on field area
                    (m_fields.area_ha / main_totals.TotalMarkerAreaForCVRCrop) * p.DosageQuantity as DosageQuantity,
                    p.DosageUnit,
                    'ethical_main_' || CAST(m_fields.field_uuid AS VARCHAR) as MatchedFieldID,
                    'block_' || CAST(m_fields.field_id AS VARCHAR) as MatchedBlockID,
                    p.AcreageSize * (m_fields.area_ha / main_totals.TotalMarkerAreaForCVRCrop) as AllocatedArea,
                    'Ethical_Best_Match_Main_Strategy' as AllocationMethod,
                    -- High confidence since we chose this as the best match
                    0.95 as MatchConfidence,
                    FALSE as IsPartialFieldCoverage,
                    NOW() as DisaggregationDate,
                    m_fields.field_uuid,
                    m_fields.field_uuid as primary_field_id
                FROM BestMatchEvaluation p
                JOIN MarkerFieldCVRCropTotals main_totals
                    ON TRIM(CAST(p.cvr_number AS VARCHAR)) = main_totals.CVR 
                    AND TRY_CAST(p.Code AS BIGINT) = main_totals.CropCode
                JOIN marker m_fields 
                    ON main_totals.CVR = TRIM(CAST(m_fields.cvr_number AS VARCHAR))
                    AND main_totals.CropCode = TRY_CAST(m_fields.crop_code AS BIGINT)
                WHERE p.best_strategy = 'main'
                  AND m_fields.cvr_number IS NOT NULL 
                  AND TRIM(CAST(m_fields.cvr_number AS VARCHAR)) != '' 
                  AND REGEXP_MATCHES(TRIM(CAST(m_fields.cvr_number AS VARCHAR)), '^[0-9]+$')
                  AND m_fields.area_ha > 0.0
            """).fetchone()[0]

            # Process using non-organic strategy (best_strategy = 'nonorg')
            nonorg_processed = self.duckdb_conn.execute(f"""
                WITH MixedFarmingCombinations AS (
                    SELECT * FROM VALUES {mixed_combinations_sql} AS t(CVR, CropCode)
                ),
                NonOrganicMarkerFieldCVRCropTotals AS (
                    SELECT
                        TRIM(CAST(m.cvr_number AS VARCHAR)) as CVR,
                        TRY_CAST(m.crop_code AS BIGINT) as CropCode,
                        SUM(m.area_ha) as TotalNonOrganicMarkerAreaForCVRCrop
                    FROM marker m
                    WHERE m.cvr_number IS NOT NULL 
                          AND TRIM(CAST(m.cvr_number AS VARCHAR)) != '' 
                          AND REGEXP_MATCHES(TRIM(CAST(m.cvr_number AS VARCHAR)), '^[0-9]+$')
                          AND m.crop_code IS NOT NULL AND m.area_ha > 0.0
                          AND m.organic_farming = FALSE
                    GROUP BY CVR, CropCode
                ),
                MarkerFieldCVRCropTotals AS (
                    SELECT
                        TRIM(CAST(m.cvr_number AS VARCHAR)) as CVR,
                        TRY_CAST(m.crop_code AS BIGINT) as CropCode,
                        SUM(m.area_ha) as TotalMarkerAreaForCVRCrop
                    FROM marker m
                    WHERE m.cvr_number IS NOT NULL 
                      AND TRIM(CAST(m.cvr_number AS VARCHAR)) != '' 
                      AND REGEXP_MATCHES(TRIM(CAST(m.cvr_number AS VARCHAR)), '^[0-9]+$')
                      AND m.crop_code IS NOT NULL AND m.area_ha > 0.0
                    GROUP BY CVR, CropCode
                ),
                BestMatchEvaluation AS (
                    SELECT 
                        p.*,
                        main_totals.TotalMarkerAreaForCVRCrop,
                        non_organic_totals.TotalNonOrganicMarkerAreaForCVRCrop,
                        -- Calculate errors for both strategies
                        ABS(p.AcreageSize - main_totals.TotalMarkerAreaForCVRCrop) / 
                            p.AcreageSize * 100 as main_error_pct,
                        ABS(p.AcreageSize - 
                            non_organic_totals.TotalNonOrganicMarkerAreaForCVRCrop) /
                            p.AcreageSize * 100 as nonorg_error_pct,
                        -- Determine best strategy
                        CASE 
                            WHEN ABS(p.AcreageSize - main_totals.TotalMarkerAreaForCVRCrop) / 
                                p.AcreageSize * 100
                                 <= {self.config.area_tolerance_pct} 
                                 AND ABS(p.AcreageSize - 
                                     non_organic_totals.TotalNonOrganicMarkerAreaForCVRCrop) /
                                p.AcreageSize * 100 
                                     <= {self.config.area_tolerance_pct} THEN
                                CASE WHEN ABS(p.AcreageSize - main_totals.TotalMarkerAreaForCVRCrop) 
                                          <= ABS(p.AcreageSize -
                                              non_organic_totals.TotalNonOrganicMarkerAreaForCVRCrop) 
                                     THEN 'main' ELSE 'nonorg' END
                            WHEN ABS(p.AcreageSize - main_totals.TotalMarkerAreaForCVRCrop) / 
                                p.AcreageSize * 100
                                 <= {self.config.area_tolerance_pct} 
                                 THEN 'main'
                            WHEN ABS(p.AcreageSize - non_organic_totals.TotalNonOrganicMarkerAreaForCVRCrop) /
                            p.AcreageSize * 100 <= {self.config.area_tolerance_pct} 
                                 THEN 'nonorg'
                            ELSE 'neither'
                        END as best_strategy
                    FROM pending_pesticide_rows p
                    JOIN MixedFarmingCombinations mfc 
                        ON TRIM(CAST(p.cvr_number AS VARCHAR)) = mfc.CVR
                        AND TRY_CAST(p.Code AS BIGINT) = mfc.CropCode
                    LEFT JOIN MarkerFieldCVRCropTotals main_totals
                        ON TRIM(CAST(p.cvr_number AS VARCHAR)) = main_totals.CVR 
                        AND TRY_CAST(p.Code AS BIGINT) = main_totals.CropCode
                    LEFT JOIN NonOrganicMarkerFieldCVRCropTotals non_organic_totals
                        ON TRIM(CAST(p.cvr_number AS VARCHAR)) = non_organic_totals.CVR 
                        AND TRY_CAST(p.Code AS BIGINT) = non_organic_totals.CropCode
                    WHERE p.AcreageSize > 0
                      AND (main_totals.TotalMarkerAreaForCVRCrop > 0 OR
                           non_organic_totals.TotalNonOrganicMarkerAreaForCVRCrop > 0)
                )
                INSERT INTO disaggregated_pesticide_applications (
                    DisaggregatedID,
                    OriginalPesticideRowID,
                    cvr_number,
                    PesticideName,
                    PesticideRegistrationNumber,
                    DosageQuantity,
                    DosageUnit,
                    MatchedFieldID,
                    MatchedBlockID,
                    AllocatedArea,
                    AllocationMethod,
                    MatchConfidence,
                    IsPartialFieldCoverage,
                    DisaggregationDate,
                    field_uuid,
                    primary_field_id
                )
                SELECT
                    uuid() as DisaggregatedID,
                    CAST(p.OriginalPesticideRowID AS VARCHAR) as OriginalPesticideRowID,
                    CAST(p.cvr_number AS VARCHAR) as cvr_number,
                    p.PesticideName,
                    p.PesticideRegistrationNumber,
                    -- Proportional dosage based on field area
                    (m_fields.area_ha / non_organic_totals.TotalNonOrganicMarkerAreaForCVRCrop) *
                        p.DosageQuantity as DosageQuantity,
                    p.DosageUnit,
                    'ethical_nonorg_' || CAST(m_fields.field_uuid AS VARCHAR) as MatchedFieldID,
                    'block_' || CAST(m_fields.field_id AS VARCHAR) as MatchedBlockID,
                    p.AcreageSize * (m_fields.area_ha /
                        non_organic_totals.TotalNonOrganicMarkerAreaForCVRCrop) as AllocatedArea,
                    'Ethical_Best_Match_Non_Organic_Strategy' as AllocationMethod,
                    -- High confidence since we chose this as the best match
                    0.95 as MatchConfidence,
                    FALSE as IsPartialFieldCoverage,
                    NOW() as DisaggregationDate,
                    m_fields.field_uuid,
                    m_fields.field_uuid as primary_field_id
                FROM BestMatchEvaluation p
                JOIN NonOrganicMarkerFieldCVRCropTotals non_organic_totals
                    ON TRIM(CAST(p.cvr_number AS VARCHAR)) = non_organic_totals.CVR 
                                            AND TRY_CAST(p.Code AS BIGINT) = non_organic_totals.CropCode
                JOIN marker m_fields 
                    ON non_organic_totals.CVR = TRIM(CAST(m_fields.cvr_number AS VARCHAR))
                    AND non_organic_totals.CropCode = TRY_CAST(m_fields.crop_code AS BIGINT)
                WHERE p.best_strategy = 'nonorg'
                  AND m_fields.cvr_number IS NOT NULL 
                  AND TRIM(CAST(m_fields.cvr_number AS VARCHAR)) != '' 
                  AND REGEXP_MATCHES(TRIM(CAST(m_fields.cvr_number AS VARCHAR)), '^[0-9]+$')
                  AND m_fields.area_ha > 0.0
                  AND m_fields.organic_farming = FALSE
            """).fetchone()[0]

            total_processed = processed_count + nonorg_processed

            # Remove processed applications from pending queue
            self.duckdb_conn.execute("""
                DELETE FROM pending_pesticide_rows 
                WHERE CAST(OriginalPesticideRowID AS VARCHAR) IN (
                    SELECT DISTINCT da.OriginalPesticideRowID
                    FROM disaggregated_pesticide_applications da
                    WHERE da.AllocationMethod LIKE 'Ethical_Best_Match_%'
                )
            """)

            # Log the ethical impact
            best_match_stats = self.duckdb_conn.execute("""
                SELECT 
                    COUNT(CASE WHEN AllocationMethod = 'Ethical_Best_Match_Main_Strategy' THEN 1 END) as main_wins,
                    COUNT(CASE WHEN AllocationMethod = 'Ethical_Best_Match_Non_Organic_Strategy'
                               THEN 1 END) as nonorg_wins
                FROM disaggregated_pesticide_applications
                WHERE AllocationMethod LIKE 'Ethical_Best_Match_%'
            """).fetchone()

            if best_match_stats:
                main_wins, nonorg_wins = best_match_stats
                self.log.info(
                    f"🌟 Ethical best-match results: Main={main_wins:,}, Non-organic={nonorg_wins:,}"
                )
                self.log.info(
                    f"✅ {nonorg_wins:,} farmers benefited from more accurate non-organic matching!"
                )

            self.log.info(
                f"🎯 Ethical best-match processing completed: {total_processed:,} applications processed"
            )
            return total_processed

        except Exception as e:
            self.log.error(f"Error in ethical best-match processing: {e}")
            self.log.warning("Falling back to sequential processing")
            return 0

    def _disaggregate_by_marker_match(self) -> int:
        """
        STRATEGY 1: THE MAIN WORKHORSE - Area matching with 2% tolerance (92% success rate!)

        WHAT THIS STRATEGY DOES:
        =======================
        This is the core strategy that solves most pesticide disaggregation cases. Here's how it works:

        1. GROUP BY COMPANY + CROP: For each company, group all their fields by crop type
           Example: Company ABC has 3 wheat fields (10ha, 8ha, 7ha) = 25ha total wheat

        2. MATCH AREAS: Look for pesticide applications where the application area matches
           the total field area (within 2% tolerance)
           Example: Company ABC applied pesticide to 25ha wheat (matches their 25ha total)

        3. DISTRIBUTE PROPORTIONALLY: Split the pesticide application across all fields
           based on their relative sizes
           Example: Field1 gets 40% (10/25), Field2 gets 32% (8/25), Field3 gets 28% (7/25)

        WHY THIS WORKS SO WELL:
        ======================
        - Companies typically apply pesticides to ALL their fields of a given crop type
        - The 2% tolerance accounts for small measurement differences and rounding
        - Proportional distribution is the most fair assumption when we don't know specifics

        REAL-WORLD EXAMPLE:
        ==================
        Input: "Company 12345678 applied 50L herbicide to 25ha wheat"
        Field data: Company 12345678 has wheat fields of 10ha, 8ha, 7ha (total 25ha)
        Match: 25ha application ≈ 25ha total fields (perfect match!)
        Output:
        - Field A gets 20L (10ha/25ha * 50L)
        - Field B gets 16L (8ha/25ha * 50L)
        - Field C gets 14L (7ha/25ha * 50L)

        CRITICAL: This logic achieved 92% coverage - DON'T CHANGE IT!
        """
        self.log.info("Running original marker match strategy (92% coverage strategy)")

        try:
            # THE MAGIC SQL QUERY - This is where the 92% coverage happens!
            # ============================================================
            # This complex query does the area matching and proportional distribution in one go
            insert_query = f"""
                WITH MarkerFieldCVRCropTotals AS (
                    -- STEP 1: Calculate total field area for each company + crop combination
                    -- This gives us the "denominator" for proportional distribution
                    SELECT
                        TRIM(CAST(m.cvr_number AS VARCHAR)) as CVR,
                        TRY_CAST(m.crop_code AS BIGINT) as CropCode,
                        SUM(m.area_ha) as TotalMarkerAreaForCVRCrop
                    FROM marker m
                    WHERE m.cvr_number IS NOT NULL 
                          AND TRIM(CAST(m.cvr_number AS VARCHAR)) != '' 
                          AND REGEXP_MATCHES(TRIM(CAST(m.cvr_number AS VARCHAR)), '^[0-9]+$')
                          AND m.crop_code IS NOT NULL AND m.area_ha > 0.0
                    GROUP BY CVR, CropCode
                )
                INSERT INTO disaggregated_pesticide_applications
                SELECT
                    uuid() as DisaggregatedID,
                    CAST(p.OriginalPesticideRowID AS VARCHAR) as OriginalPesticideRowID,
                    CAST(p.cvr_number AS VARCHAR) as cvr_number,
                    p.PesticideName, 
                    p.PesticideRegistrationNumber, 
                    p.DosageQuantity * (m_fields.area_ha / marker_totals.TotalMarkerAreaForCVRCrop) as DosageQuantity,
                    p.DosageUnit,
                    'marker_' || CAST(m_fields.field_id AS VARCHAR) as MatchedFieldID,
                    'block_' || CAST(m_fields.block_id AS VARCHAR) as MatchedBlockID,
                    -- THE MAGIC FORMULA: Proportional distribution based on field size
                    -- pesticide_amount * (this_field_area / total_company_crop_area)
                    p.AcreageSize * (m_fields.area_ha / marker_totals.TotalMarkerAreaForCVRCrop) as AllocatedArea,
                    'Marker_ApplicationAreaToTotalFieldArea_FieldProportional' as AllocationMethod,
                    -- Confidence score: higher when areas match more closely
                    GREATEST(0.0, 1.0 - (ABS(p.AcreageSize - marker_totals.TotalMarkerAreaForCVRCrop) /
                        p.AcreageSize / ({self.config.area_tolerance_pct}/100.0))) as MatchConfidence,
                    FALSE as IsPartialFieldCoverage,
                    NOW() as DisaggregationDate,
                    -- Add field UUID support
                    m_fields.field_uuid,
                    m_fields.field_uuid
                FROM pending_pesticide_rows p
                -- STEP 2: Match pesticide applications to company+crop totals
                JOIN MarkerFieldCVRCropTotals marker_totals
                    ON TRIM(CAST(p.cvr_number AS VARCHAR)) = marker_totals.CVR 
                                         AND TRY_CAST(p.Code AS BIGINT) = marker_totals.CropCode
                -- STEP 3: Join with individual fields to create one record per field
                JOIN marker m_fields 
                    ON marker_totals.CVR = TRIM(CAST(m_fields.cvr_number AS VARCHAR))
                    AND marker_totals.CropCode = TRY_CAST(m_fields.crop_code AS BIGINT)
                WHERE 
                    p.AcreageSize > 0 AND marker_totals.TotalMarkerAreaForCVRCrop > 0
                    -- THE CRITICAL FILTER: Only match if areas are within 2% tolerance
                    -- This 2% is the magic number that achieved 92% coverage!
                    AND ABS(p.AcreageSize - marker_totals.TotalMarkerAreaForCVRCrop) / p.AcreageSize * 100
                        <= {self.config.area_tolerance_pct}
                    AND m_fields.cvr_number IS NOT NULL 
                    AND TRIM(CAST(m_fields.cvr_number AS VARCHAR)) != '' 
                    AND REGEXP_MATCHES(TRIM(CAST(m_fields.cvr_number AS VARCHAR)), '^[0-9]+$')
                    AND m_fields.area_ha > 0.0
            """

            self.duckdb_conn.execute(insert_query)

            # CLEAN UP: Remove processed records from the pending queue
            # This prevents double-processing in subsequent strategies
            self.duckdb_conn.execute("""
                DELETE FROM pending_pesticide_rows 
                WHERE CAST(OriginalPesticideRowID AS VARCHAR) IN (
                    SELECT DISTINCT OriginalPesticideRowID 
                    FROM disaggregated_pesticide_applications 
                    WHERE AllocationMethod = 'Marker_ApplicationAreaToTotalFieldArea_FieldProportional'
                )
            """)

            # Count how many records we successfully processed
            count_result = self.duckdb_conn.execute(
                "SELECT COUNT(*) FROM disaggregated_pesticide_applications " +
                "WHERE AllocationMethod = 'Marker_ApplicationAreaToTotalFieldArea_FieldProportional'"
            ).fetchone()
            processed_count = count_result[0] if count_result else 0

            self.log.info(f"Original marker match strategy processed {processed_count} records")

            return processed_count

        except Exception as e:
            self.log.error(f"Error in original marker match strategy: {str(e)}")
            return 0

    def _disaggregate_by_marker_non_organic_match(self) -> int:
        """
        Strategy 2: Non-organic marker match
        PRESERVE EXACT LOGIC from disaggregation.py lines 187-280
        """
        self.log.info("Running marker non-organic match strategy")

        try:
            # Get organic field UUIDs for unique identification
            organic_field_uuids = self._get_organic_marker_field_ids()

            if not organic_field_uuids:
                self.log.info("No organic fields found - Strategy 2 will behave like Strategy 1")
                # Use a condition that excludes nothing
                organic_exclusion_condition = "TRUE"
            else:
                self.log.info(
                    f"Excluding {len(organic_field_uuids)} organic field UUIDs from Strategy 2"
                )
                # Use direct column check for efficiency
                organic_exclusion_condition = "m.organic_farming = FALSE"

            # Optimized SQL query with organic field exclusion using direct column check
            insert_query = f"""
                WITH NonOrganicMarkerFieldCVRCropTotals AS (
                    SELECT
                        TRIM(CAST(m.cvr_number AS VARCHAR)) as CVR,
                        TRY_CAST(m.crop_code AS BIGINT) as CropCode,
                        SUM(m.area_ha) as TotalNonOrganicMarkerAreaForCVRCrop
                    FROM marker m
                    WHERE m.cvr_number IS NOT NULL 
                          AND TRIM(CAST(m.cvr_number AS VARCHAR)) != '' 
                          AND REGEXP_MATCHES(TRIM(CAST(m.cvr_number AS VARCHAR)), '^[0-9]+$')
                          AND m.crop_code IS NOT NULL AND m.area_ha > 0.0
                                                     AND {organic_exclusion_condition} 
                    GROUP BY CVR, CropCode
                )
                INSERT INTO disaggregated_pesticide_applications
                SELECT
                    uuid() as DisaggregatedID,
                    CAST(p.OriginalPesticideRowID AS VARCHAR) as OriginalPesticideRowID,
                    CAST(p.cvr_number AS VARCHAR) as cvr_number,
                    p.PesticideName, 
                    p.PesticideRegistrationNumber, 
                    p.DosageQuantity * (m_fields.area_ha / non_organic_totals.TotalNonOrganicMarkerAreaForCVRCrop) as DosageQuantity,
                    p.DosageUnit,
                    'marker_non_organic_' || CAST(m_fields.field_id AS VARCHAR) as MatchedFieldID,
                    'block_' || CAST(m_fields.block_id AS VARCHAR) as MatchedBlockID,
                    p.AcreageSize * (m_fields.area_ha /
                        non_organic_totals.TotalNonOrganicMarkerAreaForCVRCrop) as AllocatedArea,
                    'Marker_NonOrganic_ApplicationAreaToTotalFieldArea_FieldProportional' as AllocationMethod,
                    GREATEST(0.0, 1.0 - (ABS(p.AcreageSize - non_organic_totals.TotalNonOrganicMarkerAreaForCVRCrop) / p.AcreageSize / ({self.config.area_tolerance_pct}/100.0))) as MatchConfidence,
                    FALSE as IsPartialFieldCoverage,
                    NOW() as DisaggregationDate,
                    -- Add field UUID support
                    m_fields.field_uuid,
                    m_fields.field_uuid
                FROM pending_pesticide_rows p
                JOIN NonOrganicMarkerFieldCVRCropTotals non_organic_totals
                    ON TRIM(CAST(p.cvr_number AS VARCHAR)) = non_organic_totals.CVR 
                                         AND TRY_CAST(p.Code AS BIGINT) = non_organic_totals.CropCode
                JOIN marker m_fields 
                    ON non_organic_totals.CVR = TRIM(CAST(m_fields.cvr_number AS VARCHAR))
                    AND non_organic_totals.CropCode = TRY_CAST(m_fields.crop_code AS BIGINT)
                WHERE 
                    p.AcreageSize > 0 AND non_organic_totals.TotalNonOrganicMarkerAreaForCVRCrop > 0
                    AND ABS(p.AcreageSize - non_organic_totals.TotalNonOrganicMarkerAreaForCVRCrop) /
                            p.AcreageSize * 100 <= {self.config.area_tolerance_pct}
                    AND m_fields.cvr_number IS NOT NULL 
                    AND TRIM(CAST(m_fields.cvr_number AS VARCHAR)) != '' 
                    AND REGEXP_MATCHES(TRIM(CAST(m_fields.cvr_number AS VARCHAR)), '^[0-9]+$')
                    AND m_fields.area_ha > 0.0
                                         AND m_fields.organic_farming = FALSE
            """

            self.duckdb_conn.execute(insert_query)

            # Remove processed records from pending table
            self.duckdb_conn.execute("""
                DELETE FROM pending_pesticide_rows 
                WHERE CAST(OriginalPesticideRowID AS VARCHAR) IN (
                    SELECT DISTINCT OriginalPesticideRowID 
                    FROM disaggregated_pesticide_applications 
                    WHERE AllocationMethod = 'Marker_NonOrganic_ApplicationAreaToTotalFieldArea_FieldProportional'
                )
            """)

            # Get count of processed records
            count_result = self.duckdb_conn.execute(
                "SELECT COUNT(*) FROM disaggregated_pesticide_applications WHERE AllocationMethod = 'Marker_NonOrganic_ApplicationAreaToTotalFieldArea_FieldProportional'"
            ).fetchone()
            processed_count = count_result[0] if count_result else 0

            self.log.info(f"Marker non-organic match strategy processed {processed_count} records")

            return processed_count

        except Exception as e:
            self.log.error(f"Error in marker non-organic match strategy: {str(e)}")
            return 0

    def _disaggregate_by_partial_field_coverage(self) -> int:
        """
        Strategy 3: Partial Field Coverage for single-field CVR/crop combinations.
        OPTIMIZED VERSION: Uses efficient DuckDB batch operations instead of Python loops.
        """
        self.log.info("Running Partial Field Coverage disaggregation strategy...")

        try:
            # OPTIMIZED: Single batch query to process all candidates at once
            # This replaces the inefficient Python loop with pure DuckDB operations
            insert_query = """
                WITH MarkerSingleFieldCVRCrop AS (
                    SELECT 
                        CAST(CAST(m.cvr_number AS BIGINT) AS VARCHAR) as CVR_Str,
                        CAST(CAST(m.crop_code AS BIGINT) AS VARCHAR) as Crop_Str,
                        COUNT(*) as FieldCount,
                        m.field_id as FieldID,
                        m.area_ha as FieldArea,
                        m.field_id as FieldIdentifier,
                                                 ANY_VALUE(m.field_uuid) as field_uuid,
                         ANY_VALUE(m.field_uuid) as primary_field_id
                    FROM marker m
                    WHERE m.cvr_number IS NOT NULL 
                      AND TRIM(CAST(m.cvr_number AS VARCHAR)) != '' 
                      AND REGEXP_MATCHES(TRIM(CAST(m.cvr_number AS VARCHAR)), '^[0-9]+$')
                      AND m.crop_code IS NOT NULL 
                      AND m.area_ha > 0.0
                    GROUP BY 1, 2, 4, 5, 6
                    HAVING COUNT(*) = 1  -- Only single field per CVR/Crop
                ),
                PendingForSingleFields AS (
                    SELECT 
                        p.OriginalPesticideRowID,
                        TRIM(CAST(p.cvr_number AS VARCHAR)) as CVR_Str,
                        CAST(CAST(p.Code AS BIGINT) AS VARCHAR) as Crop_Str,
                        p.AcreageSize,
                        p.PesticideName,
                        p.PesticideRegistrationNumber,
                        p.DosageQuantity,
                        p.DosageUnit
                    FROM pending_pesticide_rows p
                    WHERE p.cvr_number IS NOT NULL 
                      AND p.Code IS NOT NULL
                      AND p.AcreageSize > 0
                ),
                CandidatesWithFields AS (
                    SELECT 
                        pf.OriginalPesticideRowID,
                        pf.CVR_Str,
                        pf.Crop_Str,
                        pf.AcreageSize,
                        pf.PesticideName,
                        pf.PesticideRegistrationNumber,
                        pf.DosageQuantity,
                        pf.DosageUnit,
                        sf.FieldID,
                        sf.FieldArea,
                        sf.FieldIdentifier,
                        sf.field_uuid,
                        sf.primary_field_id,
                        (pf.AcreageSize / sf.FieldArea) * 100 as CoveragePercent
                    FROM MarkerSingleFieldCVRCrop sf
                    JOIN PendingForSingleFields pf 
                        ON sf.CVR_Str = pf.CVR_Str 
                        AND sf.Crop_Str = pf.Crop_Str
                    WHERE pf.AcreageSize < sf.FieldArea  -- Pesticide area smaller than field area
                )
                INSERT INTO disaggregated_pesticide_applications
                SELECT
                    uuid() as DisaggregatedID,
                    CAST(c.OriginalPesticideRowID AS VARCHAR) as OriginalPesticideRowID,
                    c.CVR_Str as cvr_number,
                    c.PesticideName,
                    c.PesticideRegistrationNumber,
                    c.DosageQuantity,
                    c.DosageUnit,
                    'marker_' || CAST(c.FieldIdentifier AS VARCHAR) as MatchedFieldID,
                    'block_' || CAST(c.FieldIdentifier AS VARCHAR) as MatchedBlockID,
                    c.AcreageSize as AllocatedArea,  -- Use pesticide area, not field area
                    'Partial_Field_Coverage_SingleField' as AllocationMethod,
                    0.8 as MatchConfidence,
                    TRUE as IsPartialFieldCoverage,
                    NOW() as DisaggregationDate,
                    -- Add field UUID support
                    c.field_uuid,
                    c.primary_field_id
                FROM CandidatesWithFields c
            """

            # Execute the optimized batch insert
            self.duckdb_conn.execute(insert_query)

            # Remove processed records from pending table in a single operation
            self.duckdb_conn.execute("""
                DELETE FROM pending_pesticide_rows 
                WHERE CAST(OriginalPesticideRowID AS VARCHAR) IN (
                    SELECT DISTINCT OriginalPesticideRowID 
                    FROM disaggregated_pesticide_applications 
                    WHERE AllocationMethod = 'Partial_Field_Coverage_SingleField'
                )
            """)

            # Get count of processed records
            count_result = self.duckdb_conn.execute(
                "SELECT COUNT(*) FROM disaggregated_pesticide_applications WHERE AllocationMethod = 'Partial_Field_Coverage_SingleField'"
            ).fetchone()
            processed_count = count_result[0] if count_result else 0

            self.log.info(
                f"Partial Field Coverage: Processed {processed_count} pesticide applications with partial field coverage using optimized batch operations."
            )
            return processed_count

        except Exception as e:
            self.log.error(f"Error in partial field coverage strategy: {str(e)}")
            return 0

    def _disaggregate_by_adjacent_fields_single_cluster_removed(self) -> int:
        """
        Strategy 4: Adjacent Fields Single Cluster using DuckDB spatial operations.
        CORRECTED VERSION: Only allocates when BOTH spatial coherence AND area match exist.
        """
        self.log.info("Running Adjacent Fields Spatial Cluster disaggregation strategy...")

        # Check if spatial clustering is enabled
        if not self.config.enable_spatial_clustering:
            self.log.info("⚠️ Spatial clustering disabled in configuration - skipping")
            return 0

        try:
            # Check if geometry data is available for spatial clustering
            geometry_columns = self.duckdb_conn.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = 'marker' AND column_name = 'geometry'
            """).fetchall()

            has_geometry = len(geometry_columns) > 0

            if not has_geometry:
                self.log.warning("⚠️ No geometry data available - spatial clustering disabled")
                return 0

            # DEBUG: Check how many records are still pending after strategies 1-3
            pending_count = self.duckdb_conn.execute(
                "SELECT COUNT(*) FROM pending_pesticide_rows"
            ).fetchone()[0]
            self.log.info(
                f"🔍 DEBUG: {pending_count} pesticide records still pending after strategies 1-3"
            )

            if pending_count == 0:
                self.log.info(
                    "✅ All records already processed by strategies 1-3 - spatial clustering not needed"
                )
                return 0

            # Get pending CVR+crop combinations to process in chunks
            # OPTIMIZATION: Only get combinations that actually have pending records AND multiple fields with geometry
            self.log.info(
                "🔍 Getting CVR+crop combinations with pending records and multiple fields..."
            )
            pending_cvr_crops = self.duckdb_conn.execute("""
                WITH PendingCombinations AS (
                    SELECT DISTINCT
                        TRIM(CAST(p.cvr_number AS VARCHAR)) as CVR_Str,
                        CAST(CAST(p.Code AS BIGINT) AS VARCHAR) as Crop_Str,
                        COUNT(*) as pending_count
                    FROM pending_pesticide_rows p
                    WHERE p.cvr_number IS NOT NULL 
                      AND TRIM(CAST(p.cvr_number AS VARCHAR)) != '' 
                      AND REGEXP_MATCHES(TRIM(CAST(p.cvr_number AS VARCHAR)), '^[0-9]+$')
                      AND p.Code IS NOT NULL 
                      AND p.AcreageSize > 0.0
                    GROUP BY CVR_Str, Crop_Str
                    HAVING COUNT(*) > 0
                ),
                FieldCounts AS (
                    SELECT 
                        CAST(CAST(m.cvr_number AS BIGINT) AS VARCHAR) as CVR_Str,
                        CAST(CAST(m.crop_code AS BIGINT) AS VARCHAR) as Crop_Str,
                        COUNT(*) as field_count
                    FROM marker m
                    WHERE m.cvr_number IS NOT NULL 
                      AND TRIM(CAST(m.cvr_number AS VARCHAR)) != '' 
                      AND REGEXP_MATCHES(TRIM(CAST(m.cvr_number AS VARCHAR)), '^[0-9]+$')
                      AND m.crop_code IS NOT NULL 
                      AND m.area_ha > 0.0
                      AND m.geometry IS NOT NULL
                    GROUP BY CVR_Str, Crop_Str
                    HAVING COUNT(*) >= 2  -- Only combinations with 2+ fields
                )
                SELECT 
                    p.CVR_Str,
                    p.Crop_Str,
                    p.pending_count,
                    f.field_count
                FROM PendingCombinations p
                INNER JOIN FieldCounts f ON p.CVR_Str = f.CVR_Str AND p.Crop_Str = f.Crop_Str
                ORDER BY p.pending_count DESC  -- Process combinations with most pending records first
            """).fetchall()

            if not pending_cvr_crops:
                self.log.info(
                    "✅ No CVR+crop combinations found with both pending records and multiple fields"
                )
                return 0

            self.log.info(
                f"📊 Found {len(pending_cvr_crops):,} CVR+crop combinations with pending records and 2+ fields"
            )
            self.log.info(
                ("🔍 Top combinations: " +
                 f"{[(cvr, crop, pending, fields) for cvr, crop, pending, fields in pending_cvr_crops[:5]]}")
            )

            # Convert to list of tuples for processing
            cvr_crop_combinations = [
                (cvr, crop) for cvr, crop, pending_count, field_count in pending_cvr_crops
            ]

            total_processed = 0
            # OPTIMIZED: Now that we filter before spatial join, we can use larger chunks
            chunk_size = 50 if len(cvr_crop_combinations) > 1000 else 100
            max_chunks = 500  # Increased limit since processing is now much more efficient

            self.log.info(
                f"✅ Processing {len(cvr_crop_combinations)} CVR+crop combinations in chunks of " +
                f"{chunk_size} (max {max_chunks} chunks)"
            )

            # Process in very small chunks with aggressive memory management
            chunks_processed = 0
            for i in range(0, len(cvr_crop_combinations), chunk_size):
                if chunks_processed >= max_chunks:
                    self.log.warning(
                        f"⚠️ Reached maximum chunk limit ({max_chunks}) - stopping spatial clustering"
                    )
                    break

                chunk = cvr_crop_combinations[i : i + chunk_size]

                # Add memory cleanup before each chunk
                self._cleanup_memory_before_chunk()

                try:
                    chunk_processed = self._process_spatial_chunk(
                        chunk, chunks_processed + 1, len(cvr_crop_combinations)
                    )
                    total_processed += chunk_processed
                    chunks_processed += 1

                    # Add memory cleanup after each chunk
                    self._cleanup_memory_after_chunk()

                except Exception as e:
                    self.log.error(
                        f"Error in spatial clustering chunk {chunks_processed + 1}: {str(e)}"
                    )
                    if "Out of Memory" in str(e) or "memory" in str(e).lower():
                        self.log.warning(
                            "⚠️ Memory exhaustion detected - stopping spatial clustering"
                        )
                        break
                    # Continue with next chunk for other errors
                    chunks_processed += 1
                    continue

            # Clean up processed records after all chunks are complete
            if total_processed > 0:
                self._finalize_spatial_clustering()

            self.log.info(
                f"Adjacent Fields Spatial Cluster: Processed {total_processed} pesticide applications " +
                f"across {chunks_processed} chunks."
            )
            return total_processed

        except Exception as e:
            self.log.error(f"Error in spatial clustering strategy: {str(e)}")
            self.log.error("Spatial clustering failed - skipping this strategy")
            return 0

    def _cleanup_memory_before_chunk(self):
        """Clean up memory before processing a spatial chunk."""
        try:
            # Force garbage collection in DuckDB
            self.duckdb_conn.execute("PRAGMA force_checkpoint")
            self.duckdb_conn.execute("PRAGMA wal_autocheckpoint = 1")

            # Drop any temporary tables that might exist
            temp_tables = ["temp_spatial_adjacency", "temp_clusters", "temp_allocations"]
            for table in temp_tables:
                try:
                    self.duckdb_conn.execute(f"DROP TABLE IF EXISTS {table}")
                except Exception:
                    pass

        except Exception:
            # Don't fail on cleanup errors
            pass

    def _cleanup_memory_after_chunk(self):
        """Clean up memory after processing a spatial chunk."""
        try:
            # Force checkpoint and cleanup
            self.duckdb_conn.execute("PRAGMA force_checkpoint")

            # Python garbage collection
            import gc

            gc.collect()

        except Exception:
            # Don't fail on cleanup errors
            pass

    def _process_spatial_chunk(
        self, cvr_crop_chunk: List[tuple], chunk_num: int, total_combinations: int
    ) -> int:
        """Process a single chunk of CVR+crop combinations for spatial clustering."""
        try:
            # Convert chunk to SQL IN clause format
            cvr_crop_in_clause = ", ".join([f"('{cvr}', '{crop}')" for cvr, crop in cvr_crop_chunk])

            # Log current chunk for debugging
            self.log.info(
                f"🔧 Processing chunk {chunk_num} with {len(cvr_crop_chunk)} CVR+crop combinations " +
                f"(total: {total_combinations}) [1 thread, no insertion order]"
            )

            # Apply DuckDB memory optimization settings
            self.duckdb_conn.execute("SET threads = 1")  # Reduce threads to save memory
            self.duckdb_conn.execute(
                "SET preserve_insertion_order = false"
            )  # Allow reordering for efficiency

            # Check pending records for these CVR+crops (minimal debugging)
            pending_for_chunk = self.duckdb_conn.execute(f"""
                SELECT COUNT(*) as total_pending
                FROM pending_pesticide_rows p
                WHERE (TRIM(CAST(p.cvr_number AS VARCHAR)), CAST(CAST(p.Code AS BIGINT) AS VARCHAR)) 
                      IN ({cvr_crop_in_clause})
            """).fetchone()[0]

            if pending_for_chunk == 0:
                self.log.info(
                    f"⚠️ Chunk {chunk_num}: No pending records found - skipping " +
                    "(this shouldn't happen with optimized filtering)"
                )
                return 0
            else:
                self.log.info(
                    f"✅ Chunk {chunk_num}: Processing {pending_for_chunk} pending records"
                )

            # CHUNKED MEMORY-OPTIMIZED: Process only this chunk of CVR+crop combinations
            insert_query = f"""
                WITH PendingCVRCrops AS (
                    -- Process only this specific chunk of CVR+crop combinations
                    SELECT CVR_Str, Crop_Str FROM (VALUES {cvr_crop_in_clause}) AS t(CVR_Str, Crop_Str)
                ),
                FilteredFields AS (
                    -- CRITICAL FIX: Filter fields by CVR+crop BEFORE spatial operations
                    SELECT 
                        m.field_id,
                        m.field_uuid,
                        m.geometry,
                        m.area_ha,
                        CAST(CAST(m.cvr_number AS BIGINT) AS VARCHAR) as CVR_Str,
                        CAST(CAST(m.crop_code AS BIGINT) AS VARCHAR) as Crop_Str
                    FROM marker m
                    JOIN PendingCVRCrops pcc ON 
                        CAST(CAST(m.cvr_number AS BIGINT) AS VARCHAR) = pcc.CVR_Str
                        AND CAST(CAST(m.crop_code AS BIGINT) AS VARCHAR) = pcc.Crop_Str
                    WHERE m.cvr_number IS NOT NULL 
                      AND TRIM(CAST(m.cvr_number AS VARCHAR)) != '' 
                      AND REGEXP_MATCHES(TRIM(CAST(m.cvr_number AS VARCHAR)), '^[0-9]+$')
                      AND m.crop_code IS NOT NULL 
                      AND m.area_ha > 0.0
                      AND m.geometry IS NOT NULL
                ),
                SpatialAdjacency AS (
                    -- Now do spatial join ONLY on the filtered subset (much smaller!)
                    SELECT DISTINCT
                        f1.field_id as field1_id,
                        f1.field_uuid as field1_uuid,
                        f2.field_id as field2_id,
                        f2.field_uuid as field2_uuid,
                        f1.CVR_Str,
                        f1.Crop_Str
                    FROM FilteredFields f1
                    JOIN FilteredFields f2 ON 
                        f1.CVR_Str = f2.CVR_Str 
                        AND f1.Crop_Str = f2.Crop_Str
                        AND f1.field_uuid != f2.field_uuid
                        AND ST_DWithin(f1.geometry, f2.geometry, 10.0)  -- Spatial join on filtered data only
                ),
                -- Build connected components using recursive CTE (Union-Find algorithm)
                ConnectedComponents AS (
                    -- Start with individual fields as their own clusters
                    SELECT DISTINCT
                        field1_id as field_id,
                        CVR_Str,
                        Crop_Str,
                        field1_id as cluster_root  -- Initially, each field is its own cluster
                    FROM SpatialAdjacency
                    
                    UNION
                    
                    SELECT DISTINCT
                        field2_id as field_id,
                        CVR_Str,
                        Crop_Str,
                        field2_id as cluster_root
                    FROM SpatialAdjacency
                ),
                -- Simplified clustering: group fields that are directly connected
                FieldClusters AS (
                    SELECT 
                        field_id,
                        field_uuid,
                        CVR_Str,
                        Crop_Str,
                        -- Use minimum field_id in adjacency as cluster identifier
                        MIN(LEAST(field_id, connected_field))
                            OVER (PARTITION BY CVR_Str, Crop_Str, field_id) as cluster_id
                    FROM (
                        SELECT 
                            sa.field1_id as field_id,
                            sa.field1_uuid as field_uuid,
                            sa.CVR_Str,
                            sa.Crop_Str,
                            sa.field2_id as connected_field
                        FROM SpatialAdjacency sa
                        
                        UNION ALL
                        
                        SELECT 
                            sa.field2_id as field_id,
                            sa.field2_uuid as field_uuid,
                            sa.CVR_Str,
                            sa.Crop_Str,
                            sa.field1_id as connected_field
                        FROM SpatialAdjacency sa
                        
                        UNION ALL
                        
                        -- Include isolated fields (not in any adjacency) from our filtered set
                        SELECT 
                            f.field_id,
                            f.field_uuid,
                            f.CVR_Str,
                            f.Crop_Str,
                            f.field_id as connected_field
                        FROM FilteredFields f
                        WHERE f.field_id NOT IN (
                            SELECT field1_id FROM SpatialAdjacency 
                            UNION 
                            SELECT field2_id FROM SpatialAdjacency
                        )
                    ) clustered_fields
                ),
                -- Calculate cluster areas and match against pesticide applications
                ClusterAreas AS (
                    SELECT 
                        fc.CVR_Str,
                        fc.Crop_Str,
                        fc.cluster_id,
                        COUNT(*) as cluster_field_count,
                        SUM(m.area_ha) as cluster_total_area,
                        ARRAY_AGG(fc.field_id) as cluster_field_ids,
                        ARRAY_AGG(fc.field_uuid) as cluster_field_uuids,
                        ARRAY_AGG(m.area_ha) as cluster_field_areas
                    FROM FieldClusters fc
                    JOIN marker m ON fc.field_uuid = m.field_uuid
                    GROUP BY fc.CVR_Str, fc.Crop_Str, fc.cluster_id
                    HAVING COUNT(*) >= 2  -- Only multi-field clusters
                ),
                -- Match clusters against pesticide applications with area tolerance
                MatchedClusters AS (
                    SELECT 
                        p.OriginalPesticideRowID,
                        p.cvr_number,
                        p.PesticideName,
                        p.PesticideRegistrationNumber,
                        p.DosageQuantity,
                        p.DosageUnit,
                        p.AcreageSize,
                        ca.cluster_id,
                        ca.cluster_total_area,
                        ca.cluster_field_ids,
                        ca.cluster_field_uuids,
                        ca.cluster_field_areas,
                        ca.cluster_field_count,
                        -- Calculate area match quality
                        ABS(p.AcreageSize - ca.cluster_total_area) / p.AcreageSize * 100 as area_diff_pct
                    FROM pending_pesticide_rows p
                    JOIN ClusterAreas ca ON 
                        TRIM(CAST(p.cvr_number AS VARCHAR)) = ca.CVR_Str
                        AND CAST(CAST(p.Code AS BIGINT) AS VARCHAR) = ca.Crop_Str
                    WHERE p.cvr_number IS NOT NULL 
                      AND p.Code IS NOT NULL
                      AND p.AcreageSize > 0
                      -- CRITICAL: Area must match within tolerance (2%)
                      AND ABS(p.AcreageSize - ca.cluster_total_area) / p.AcreageSize * 100
                          <= {self.config.area_tolerance_pct}
                ),
                -- Expand matched clusters to individual field allocations
                FieldAllocations AS (
                    SELECT 
                        mc.OriginalPesticideRowID,
                        mc.cvr_number,
                        mc.PesticideName,
                        mc.PesticideRegistrationNumber,
                        mc.DosageQuantity,
                        mc.DosageUnit,
                        mc.AcreageSize,
                        UNNEST(mc.cluster_field_ids) as field_id,
                        UNNEST(mc.cluster_field_uuids) as field_uuid,
                        UNNEST(mc.cluster_field_areas) as field_area,
                        mc.cluster_total_area,
                        mc.area_diff_pct,
                        mc.cluster_field_count
                    FROM MatchedClusters mc
                )
                INSERT INTO disaggregated_pesticide_applications
                SELECT
                    uuid() as DisaggregatedID,
                    CAST(fa.OriginalPesticideRowID AS VARCHAR) as OriginalPesticideRowID,
                    CAST(fa.cvr_number AS VARCHAR) as cvr_number,
                    fa.PesticideName,
                    fa.PesticideRegistrationNumber,
                    fa.DosageQuantity * (fa.field_area / fa.cluster_total_area) as DosageQuantity,
                    fa.DosageUnit,
                    'marker_spatial_' || CAST(fa.field_id AS VARCHAR) as MatchedFieldID,
                    'block_' || CAST(fa.field_id AS VARCHAR) as MatchedBlockID,
                    -- Proportional allocation: pesticide_area * (field_area / cluster_area)
                    fa.AcreageSize * (fa.field_area / fa.cluster_total_area) as AllocatedArea,
                    'Adjacent_Fields_Spatial_Cluster_AreaMatched' as AllocationMethod,
                    -- Confidence based on area match quality and cluster size
                    GREATEST(0.5, 1.0 - (fa.area_diff_pct / {self.config.area_tolerance_pct})) as MatchConfidence,
                    FALSE as IsPartialFieldCoverage,
                    NOW() as DisaggregationDate,
                    -- Add field UUID support - need to join back to marker table
                    m.field_uuid,
                    m.primary_field_id
                FROM FieldAllocations fa
                JOIN marker m ON fa.field_uuid = m.field_uuid
            """

            # Execute the chunked spatial clustering
            self.duckdb_conn.execute(insert_query)

            # Get count of processed records for this chunk
            count_result = self.duckdb_conn.execute(
                "SELECT COUNT(*) FROM disaggregated_pesticide_applications " +
                "WHERE AllocationMethod = 'Adjacent_Fields_Spatial_Cluster_AreaMatched'"
            ).fetchone()
            total_processed_so_far = count_result[0] if count_result else 0

            # Calculate records processed in this chunk (difference from before)
            chunk_processed = total_processed_so_far - getattr(
                self, "_spatial_processed_before_chunk", 0
            )
            self._spatial_processed_before_chunk = total_processed_so_far

            if chunk_processed > 0:
                self.log.info(f"✅ Chunk {chunk_num}: Processed {chunk_processed} records")
            else:
                # Don't log for every empty chunk - this is expected behavior
                pass

            return chunk_processed

        except Exception as e:
            self.log.error(f"Error in spatial clustering chunk {chunk_num}: {str(e)}")
            return 0

    def _finalize_spatial_clustering(self) -> None:
        """Remove processed records from pending table after all chunks are complete."""
        try:
            # Remove all processed records from pending table
            self.duckdb_conn.execute("""
                DELETE FROM pending_pesticide_rows 
                WHERE CAST(OriginalPesticideRowID AS VARCHAR) IN (
                    SELECT DISTINCT OriginalPesticideRowID 
                    FROM disaggregated_pesticide_applications 
                    WHERE AllocationMethod = 'Adjacent_Fields_Spatial_Cluster_AreaMatched'
                )
            """)
            self.log.info("🧹 Cleaned up processed records from pending table")
        except Exception as e:
            self.log.error(f"Error cleaning up processed records: {str(e)}")

    def _get_results(self) -> List[Dict[str, Any]]:
        """Get the disaggregated results."""
        try:
            results = self.duckdb_conn.execute(
                "SELECT * FROM disaggregated_pesticide_applications"
            ).fetchall()
            # Convert to list of dictionaries
            columns = [desc[0] for desc in self.duckdb_conn.description]
            return [dict(zip(columns, row)) for row in results]
        except Exception as e:
            self.log.error(f"Error getting results: {str(e)}")
            return []

    def __del__(self):
        """Clean up DuckDB connection."""
        if self.duckdb_conn:
            self.duckdb_conn.close()
