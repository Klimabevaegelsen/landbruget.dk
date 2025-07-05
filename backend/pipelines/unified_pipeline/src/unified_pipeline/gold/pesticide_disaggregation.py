"""
Pesticide Disaggregation Gold Layer

This module implements the gold layer processor for pesticide disaggregation.
It preserves the EXACT original strategy that achieved 92% coverage:
- Simple area matching between pesticide applications and total field areas by CVR+crop
- 2% area tolerance (PRESERVE ORIGINAL)
- Direct proportional allocation to fields

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

print("DEBUG: pesticide_disaggregation.py module loaded!")
logger = logging.getLogger(__name__)
print(f"DEBUG: Logger created: {logger}")


class PesticideDisaggregationGoldConfig(BaseJobConfig):
    """Configuration for pesticide disaggregation gold processor."""

    name: str = "Pesticide Disaggregation Gold"
    dataset: str = "pesticide_disaggregation"
    type: str = "gold"
    description: str = "Disaggregates pesticide applications from company to field level"
    frequency: str = "yearly"
    bucket: str = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")

    # Core parameters from original config.py - PRESERVE ORIGINAL VALUES
    area_tolerance_pct: float = Field(
        default=2.0, description="Area tolerance percentage - PRESERVE ORIGINAL VALUE"
    )
    batch_size: int = Field(
        default=1000,
        description="Batch size for processing - optimized for GitHub runners with limited memory",
    )

    # Temporal configuration (Y+1 pattern from original)
    field_year_offset: int = Field(default=1, description="Field year offset (Y+1 pattern)")

    # Input datasets
    pesticide_applications_dataset: str = "pesticides"

    # Performance optimization settings
    max_memory_gb: float = Field(
        default=12.0, description="Maximum memory usage in GB for DuckDB operations"
    )
    enable_parallel_processing: bool = Field(
        default=True, description="Enable parallel processing for large datasets"
    )

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class PesticideDisaggregationGold(BaseSource[PesticideDisaggregationGoldConfig], GoldJobInterface):
    """
    Gold layer processor for pesticide disaggregation.

    Implements the ORIGINAL strategy that achieved 92% coverage:
    - Simple area matching between pesticide applications and total field areas by CVR+crop
    - 2% area tolerance (PRESERVE ORIGINAL)
    - Direct proportional allocation to fields
    """

    def __init__(self, config: PesticideDisaggregationGoldConfig):
        print("DEBUG: PesticideDisaggregationGold.__init__ called!")
        super().__init__(config)
        self.log = Logger.get_logger()
        self.duckdb_conn = None
        self._organic_marker_field_ids: Set[str] = set()
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

    async def run(self, silver_data: Optional[Dict[str, Any]] = None) -> None:
        """
        Process pesticide disaggregation for all available years using the original proven strategy.

        Args:
            silver_data: Optional dictionary containing silver data
        """
        print("DEBUG: Pesticide disaggregation run method called!")
        self.log.info("🚀 Starting pesticide disaggregation processing with original strategy")
        print("DEBUG: Logger info message sent")
        self.log.info(
            f"🔧 Configuration: area_tolerance={self.config.area_tolerance_pct}%, field_year_offset={self.config.field_year_offset}"
        )
        self.log.info(f"☁️ GCS Bucket: {self.config.bucket}")

        # Get all available pesticide years and their corresponding field years
        self.log.info("📊 Discovering available data years...")
        print("DEBUG: About to call _get_pesticide_field_year_pairs")
        pesticide_field_pairs = self._get_pesticide_field_year_pairs()
        print(f"DEBUG: Found {len(pesticide_field_pairs)} pairs: {pesticide_field_pairs}")

        if not pesticide_field_pairs:
            self.log.error("❌ No valid pesticide-field year pairs found")
            self.log.error("🔍 This might be due to:")
            self.log.error("   - No pesticide data files in GCS")
            self.log.error("   - No field data files in GCS")
            self.log.error("   - Year offset mismatch between pesticide and field data")
            return

        self.log.info(
            f"✅ Found {len(pesticide_field_pairs)} pesticide-field year pairs to process"
        )
        for pest_year, field_year in pesticide_field_pairs:
            self.log.info(f"   📅 Will process: pesticide {pest_year} → field {field_year}")

        total_pesticide_records = 0
        total_disaggregated_records = 0
        successful_years = 0
        failed_years = 0

        # Process each pesticide year with its corresponding field year
        for i, (pesticide_year, field_year) in enumerate(pesticide_field_pairs, 1):
            self.log.info("=" * 80)
            self.log.info(
                f"🔄 Processing year pair {i}/{len(pesticide_field_pairs)}: pesticide {pesticide_year} with field {field_year}"
            )
            self.log.info("=" * 80)

            # Load data for this year pair
            self.log.info(
                f"📥 Loading silver data for pesticide year {pesticide_year} and field year {field_year}"
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
            agricultural_fields_path = datasets.get("agricultural_fields")
            pesticide_applications_path = datasets.get("pesticides")

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

            # Process this year pair
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

            if year_results is not None and year_results > 0:
                self.log.info(
                    f"✅ Year {pesticide_year}: Successfully processed and saved {year_results:,} disaggregated records"
                )

                # Count pesticide records for this year using a separate connection
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
                        f"📈 Year {pesticide_year}: {pesticide_count} total pesticide records, {year_results} disaggregated"
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
            return

        # Calculate coverage statistics
        coverage_pct = (
            (total_disaggregated_records / total_pesticide_records * 100)
            if total_pesticide_records > 0
            else 0
        )

        self.log.info("🎉 Pesticide disaggregation completed successfully!")
        self.log.info("📊 Final Statistics:")
        self.log.info(
            f"   📈 Total pesticide records across all years: {total_pesticide_records:,}"
        )
        self.log.info(
            f"   ✅ Successfully disaggregated: {total_disaggregated_records:,} ({coverage_pct:.1f}%)"
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

                # Create the final table name for this year
                table_name = f"pesticide_disaggregation_{year}"

                # Create a copy of the results table with the year-specific name
                self.log.info(f"🏗️ Creating final table {table_name}")
                self.duckdb_conn.execute(f"""
                    CREATE TABLE {table_name} AS 
                    SELECT * FROM disaggregated_pesticide_applications
                """)

                # Save directly to GCS using our own method
                self.log.info(f"🚀 Uploading {table_name} to GCS bucket")
                dataset_name = f"{self.config.dataset}_{year}"
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

            self.log.info(f"✅ Saved table {table_name} to gs://{self.config.bucket}/{gcs_path}")

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
            "⚠️ Using deprecated _save_year_results method - consider using _save_year_results_direct"
        )
        return self._save_year_results_direct(year)

    def _get_pesticide_field_year_pairs(self) -> List[Tuple[int, int]]:
        """
        Get all available pesticide years and their corresponding field years using Y+1 pattern.

        Returns:
            List of (pesticide_year, field_year) tuples
        """
        print("DEBUG: _get_pesticide_field_year_pairs called")
        self.log.info("🔍 Discovering available pesticide and field years")

        # Get available pesticide years from GCS
        self.log.info("📊 Scanning GCS for pesticide data...")
        pesticide_years = self._get_available_pesticide_years()
        self.log.info(f"✅ Found pesticide years: {sorted(pesticide_years)}")

        # Get available field years from GCS
        self.log.info("🌾 Scanning GCS for field data...")
        field_years = self._get_available_field_years()
        self.log.info(f"✅ Found field years: {sorted(field_years)}")

        # Create pairs using Y+1 pattern (pesticide year Y matches with field year Y+1)
        self.log.info(f"🔗 Creating year pairs using Y+{self.config.field_year_offset} pattern...")
        pairs = []
        for pest_year in pesticide_years:
            field_year = pest_year + self.config.field_year_offset
            if field_year in field_years:
                pairs.append((pest_year, field_year))
                self.log.info(f"   ✅ Pair created: pesticide {pest_year} → field {field_year}")
            else:
                self.log.warning(
                    f"   ❌ No field data found for pesticide year {pest_year} (expected field year {field_year})"
                )

        self.log.info(f"🎯 Created {len(pairs)} valid pesticide-field year pairs")
        return sorted(pairs)

    def _get_available_pesticide_years(self) -> Set[int]:
        """Extract available pesticide years from GCS storage."""
        try:
            # List all files in the pesticides silver directory
            files = self.gcs_access.list_files_with_metadata(
                self.config.bucket, "silver/pesticides/"
            )
            years = set()

            for file_blob in files:
                # Extract years from filenames like "pesticiddata_2015_2016.parquet"
                match = re.search(r"pesticiddata_(\d{4})_(\d{4})\.parquet", file_blob.name)
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
            # List all files in silver layer to find fvm_marker directories with actual data
            files = self.gcs_access.list_files_with_metadata(
                self.config.bucket, "silver/fvm_marker_"
            )
            years = set()

            for file_blob in files:
                # Look for files like "silver/fvm_marker_2021/timestamp/fvm_marker_2021.parquet"
                match = re.search(
                    r"silver/fvm_marker_(\d{4})/.*?/fvm_marker_(\d{4})\.parquet", file_blob.name
                )
                if match:
                    year1 = int(match.group(1))
                    year2 = int(match.group(2))
                    # Ensure both years match (sanity check)
                    if year1 == year2:
                        years.add(year1)

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
                self.log.info(
                    f"✅ Successfully located pesticide data for {pesticide_year}: {pesticide_path}"
                )
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
                self.log.info(
                    f"✅ Successfully located agricultural fields data for {field_year}: {fields_path}"
                )
            else:
                self.log.error(f"❌ No agricultural fields data found for year {field_year}")
                datasets["agricultural_fields"] = None

        # Summary of what we found
        pesticide_status = "✅" if datasets["pesticides"] else "❌"
        fields_status = "✅" if datasets["agricultural_fields"] else "❌"
        self.log.info(f"📋 Data loading summary for {pesticide_year}-{field_year}:")
        self.log.info(
            f"   {pesticide_status} Pesticide data: {'Found' if datasets['pesticides'] else 'Missing'}"
        )
        self.log.info(
            f"   {fields_status} Agricultural fields data: {'Found' if datasets['agricultural_fields'] else 'Missing'}"
        )

        return datasets

    def _read_pesticide_data_for_year(self, year: int) -> Optional[str]:
        """Read pesticide data for a specific year."""
        try:
            # Look for the specific pesticide file for this year
            # Based on actual codebase: filename pattern is pesticiddata_YYYY_YYYY.parquet in timestamped subdirs
            filename = f"pesticiddata_{year}_{year + 1}.parquet"

            # Look for the file in timestamped subdirectories
            files = self.gcs_access.list_files_with_metadata(
                self.config.bucket, "silver/pesticides/"
            )

            # Find the file that matches our year in the latest timestamped directory
            target_file = None
            latest_timestamp = None
            for file_blob in files:
                if filename in file_blob.name:
                    # Extract timestamp from path like "silver/pesticides/20250629_102742/pesticiddata_2021_2022.parquet"
                    path_parts = file_blob.name.split("/")
                    if len(path_parts) >= 3:
                        timestamp_dir = path_parts[2]  # "20250629_102742"
                        if latest_timestamp is None or timestamp_dir > latest_timestamp:
                            latest_timestamp = timestamp_dir
                            target_file = file_blob.name

            if target_file:
                # ✅ MIGRATION: Return GCS path directly instead of downloading
                gcs_path = f"gs://{self.config.bucket}/{target_file}"
                self.log.info(f"Found pesticide data at {gcs_path}")
                return gcs_path
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

            # Based on actual codebase: find latest timestamped directory in fvm_marker_YYYY/
            files = self.gcs_access.list_files_with_metadata(
                self.config.bucket, f"silver/fvm_marker_{year}/"
            )

            # Find the parquet file in timestamped subdirectories
            target_file = None
            latest_timestamp = None
            for file_blob in files:
                # Look for files like "fvm_marker_2021.parquet" instead of "data.parquet"
                if file_blob.name.endswith(f"fvm_marker_{year}.parquet"):
                    # Extract timestamp from path like "silver/fvm_marker_2021/20241201_123456/fvm_marker_2021.parquet"
                    path_parts = file_blob.name.split("/")
                    if len(path_parts) >= 3:
                        timestamp_dir = path_parts[2]  # "20241201_123456"
                        if latest_timestamp is None or timestamp_dir > latest_timestamp:
                            latest_timestamp = timestamp_dir
                            target_file = file_blob.name

            if target_file:
                # ✅ MIGRATION: Return GCS path directly instead of downloading
                gcs_path = f"gs://{self.config.bucket}/{target_file}"
                self.log.info(f"Found FVM marker data at {gcs_path}")
                return gcs_path
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
        """Process a single pesticide-field year pair."""
        try:
            self.log.info(f"🔧 Setting up DuckDB for year {pesticide_year}")
            # Setup DuckDB with spatial extensions
            setup_success = self._setup_duckdb(
                agricultural_fields_path, pesticide_applications_path
            )
            if not setup_success:
                self.log.warning(
                    f"⚠️ Skipping year pair {pesticide_year}-{field_year} due to setup failure"
                )
                return None

            self.log.info(f"✅ DuckDB setup complete for year {pesticide_year}")

            # Create results table
            self.log.info(f"🏗️ Creating results table for year {pesticide_year}")
            self._create_results_table()

            # Filter out nopesticides=1 records (from original main.py lines 50-60)
            self.log.info(f"🔍 Filtering pending pesticide records for year {pesticide_year}")
            self._create_pending_pesticide_rows()

            # Check if CVR matches are available before running strategies
            self.log.info(f"🔍 Checking for CVR matches for year {pesticide_year}")
            cvr_matches_available = self._check_cvr_matches_available()

            if not cvr_matches_available:
                self.log.warning(
                    f"⚠️ No CVR matches found for year {pesticide_year} - skipping all strategies"
                )
                self.log.warning(
                    "   This significantly improves performance when no matches are possible"
                )

                # Return empty results since no processing is possible
                self.log.info(f"📊 Year {pesticide_year} completed with 0 records (no CVR matches)")
                return []

            # Run the original strategies in exact order (from original main.py lines 89-180)
            self.log.info(f"🎯 Starting disaggregation strategies for year {pesticide_year}")
            total_processed = 0

            # Strategy 1: Marker CVR-Area Match (THE MAIN 92% STRATEGY)
            self.log.info(f"🎯 Strategy 1: Running marker CVR-area match for year {pesticide_year}")
            processed_1 = self._disaggregate_by_marker_match()
            total_processed += processed_1
            self.log.info(
                f"✅ Year {pesticide_year}: Marker CVR-Area Match: {processed_1} records processed"
            )

            # Strategy 2: Marker Non-Organic CVR-Area Match
            self.log.info(f"🎯 Strategy 2: Running non-organic match for year {pesticide_year}")
            processed_2 = self._disaggregate_by_marker_non_organic_match()
            total_processed += processed_2
            self.log.info(
                f"✅ Year {pesticide_year}: Marker Non-Organic Match: {processed_2} records processed"
            )

            # Strategy 3: Partial Field Coverage
            self.log.info(
                f"🎯 Strategy 3: Running partial field coverage for year {pesticide_year}"
            )
            processed_3 = self._disaggregate_by_partial_field_coverage()
            total_processed += processed_3
            self.log.info(
                f"✅ Year {pesticide_year}: Partial Field Coverage: {processed_3} records processed"
            )

            # Strategy 4: Spatial clustering removed - was not providing additional value
            # while adding significant complexity and processing overhead
            processed_4 = 0
            self.log.info(
                "ℹ️ Strategy 4: Spatial clustering removed for simplification - strategies 1-3 provide sufficient coverage"
            )

            # Get result count and save directly from DuckDB table
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

            # Save directly from DuckDB table (no conversion to Python list)
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
            # Clean up DuckDB connection for this year
            if self.duckdb_conn:
                self.log.info(f"🧹 Cleaning up DuckDB connection for year {pesticide_year}")
                self.duckdb_conn.close()
                self.duckdb_conn = None

    def _setup_duckdb(
        self, agricultural_fields_path: str, pesticide_applications_path: str
    ) -> bool:
        """Setup DuckDB connection with spatial extensions and register data from GCS paths.

        Returns:
            bool: True if setup was successful, False if it failed (e.g., missing CVR column)
        """
        self.duckdb_conn = duckdb.connect(":memory:")

        # Configure DuckDB for optimal performance with limited memory (GitHub runners)
        memory_limit_gb = self.config.max_memory_gb
        thread_count = 2 if not self.config.enable_parallel_processing else 4

        # Debug logging for configuration
        self.log.info(f"🔧 Config max_memory_gb: {self.config.max_memory_gb}")
        self.log.info(f"🔧 Calculated memory_limit_gb: {memory_limit_gb}")
        self.log.info(
            f"🔧 Config enable_parallel_processing: {self.config.enable_parallel_processing}"
        )
        self.log.info(f"🔧 Calculated thread_count: {thread_count}")

        self.duckdb_conn.execute(f"SET memory_limit = '{memory_limit_gb}GB'")
        self.duckdb_conn.execute(f"SET threads = {thread_count}")
        self.duckdb_conn.execute("SET temp_directory = '/tmp'")

        # Optimize for large datasets with GitHub runner resources
        self.duckdb_conn.execute("SET enable_progress_bar = false")  # Reduce output overhead
        self.duckdb_conn.execute(
            "SET preserve_insertion_order = false"
        )  # Allow reordering for efficiency

        self.log.info(f"🔧 DuckDB configured: {memory_limit_gb}GB memory, {thread_count} threads")

        # Install and load spatial extension
        self.duckdb_conn.execute("INSTALL spatial")
        self.duckdb_conn.execute("LOAD spatial")

        # ✅ MIGRATION: Use optimized GCS access with temp download (since direct GCS doesn't work reliably)
        self.log.info(f"🏗️ Creating marker table from {agricultural_fields_path}")

        # Download and create marker table
        self.log.info("📥 Downloading agricultural fields data for schema inspection...")
        with self.gcs_access._temp_download(agricultural_fields_path) as temp_file:
            self.log.info(f"✅ Downloaded to temporary file: {temp_file}")
            # First, create a temporary table to inspect the schema
            self.duckdb_conn.execute(
                f"CREATE TABLE marker_temp AS SELECT * FROM read_parquet('{temp_file}')"
            )

        # Check what columns actually exist
        self.log.info("🔍 Inspecting marker data schema...")
        temp_columns = self.duckdb_conn.execute("DESCRIBE marker_temp").fetchall()
        temp_column_names = [col[0] for col in temp_columns]
        self.log.info(f"📋 Found {len(temp_column_names)} columns in marker data:")
        for i, col in enumerate(temp_column_names, 1):
            self.log.info(f"   {i:2d}. {col}")

        # Create the final marker table with proper column mapping
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

        # Check if block_id exists, if not use field_id
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

        self.duckdb_conn.execute(f"""
            CREATE TABLE marker AS 
            SELECT 
                field_id,
                CAST(area_ha AS DOUBLE) as area_ha,
                CAST({cvr_column} AS VARCHAR) as cvr_number,
                CAST(crop_code AS VARCHAR) as crop_code,
                crop_name,
                organic_farming,
                CAST({block_id_column} AS VARCHAR) as block_id,
                year{geometry_select}
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
        crop_column = "crop_code" if "crop_code" in pest_column_names else "code"
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

        # Get record counts for logging
        self.log.info("📊 Counting loaded records...")
        marker_count = self.duckdb_conn.execute("SELECT COUNT(*) FROM marker").fetchone()[0]
        pesticide_count = self.duckdb_conn.execute("SELECT COUNT(*) FROM pesticide").fetchone()[0]

        self.log.info("✅ DuckDB setup completed successfully!")
        self.log.info(
            f"📈 Loaded {marker_count:,} agricultural fields and {pesticide_count:,} pesticide records"
        )

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
            DisaggregationDate TIMESTAMP
        )
        """
        self.duckdb_conn.execute(create_table_sql)

    def _create_pending_pesticide_rows(self):
        """Create pending pesticide rows table with nopesticides=1 records filtered out."""
        self.duckdb_conn.execute("""
            CREATE TABLE pending_pesticide_rows AS
            SELECT * FROM pesticide 
            WHERE nopesticides IS NULL OR nopesticides != 1
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
        Identifies marker field IDs that are considered organic using the direct organic_farming column.
        Results are cached.
        Returns a set of marker.field_id strings.
        """
        if self._organic_marker_field_ids:
            self.log.debug("Returning cached organic marker field IDs.")
            return self._organic_marker_field_ids

        self.log.info("Identifying organic marker fields using direct organic_farming column...")
        query = """
        SELECT DISTINCT m.field_id 
        FROM marker m
        WHERE m.organic_farming IS NOT NULL AND UPPER(TRIM(m.organic_farming)) IN ('JA', 'YES', 'TRUE', '1');
        """
        try:
            result_tuples = self.duckdb_conn.execute(query).fetchall()
            self._organic_marker_field_ids = {row[0] for row in result_tuples}
            self.log.info(
                f"Identified {len(self._organic_marker_field_ids)} organic marker field IDs using organic_farming column."
            )
        except Exception as e:
            self.log.error(
                f"Error identifying organic marker fields: {e}. Proceeding without organic field exclusion for this run."
            )
            self._organic_marker_field_ids = set()
        return self._organic_marker_field_ids

    def _disaggregate_by_marker_match(self) -> int:
        """
        Original main strategy: Match pesticide application area to total field area by CVR+crop.
        This is the strategy that achieved 92% coverage in the original pipeline.

        PRESERVE EXACT LOGIC from disaggregation.py lines 97-170
        """
        self.log.info("Running original marker match strategy (92% coverage strategy)")

        try:
            # EXACT original SQL query - DO NOT MODIFY
            insert_query = f"""
                WITH MarkerFieldCVRCropTotals AS (
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
                    p.DosageQuantity, 
                    p.DosageUnit,
                    'marker_' || CAST(m_fields.field_id AS VARCHAR) as MatchedFieldID,
                    'block_' || CAST(m_fields.block_id AS VARCHAR) as MatchedBlockID,
                    p.AcreageSize * (m_fields.area_ha / marker_totals.TotalMarkerAreaForCVRCrop) as AllocatedArea,
                    'Marker_ApplicationAreaToTotalFieldArea_FieldProportional' as AllocationMethod,
                    GREATEST(0.0, 1.0 - (ABS(p.AcreageSize - marker_totals.TotalMarkerAreaForCVRCrop) / p.AcreageSize / ({self.config.area_tolerance_pct}/100.0))) as MatchConfidence,
                    FALSE as IsPartialFieldCoverage,
                    NOW() as DisaggregationDate
                FROM pending_pesticide_rows p
                JOIN MarkerFieldCVRCropTotals marker_totals
                    ON TRIM(CAST(p.cvr_number AS VARCHAR)) = marker_totals.CVR 
                    AND TRY_CAST(p.Code AS BIGINT) = marker_totals.CropCode
                JOIN marker m_fields 
                    ON marker_totals.CVR = TRIM(CAST(m_fields.cvr_number AS VARCHAR))
                    AND marker_totals.CropCode = TRY_CAST(m_fields.crop_code AS BIGINT)
                WHERE 
                    p.AcreageSize > 0 AND marker_totals.TotalMarkerAreaForCVRCrop > 0
                    AND ABS(p.AcreageSize - marker_totals.TotalMarkerAreaForCVRCrop) / p.AcreageSize * 100 <= {self.config.area_tolerance_pct}
                    AND m_fields.cvr_number IS NOT NULL 
                    AND TRIM(CAST(m_fields.cvr_number AS VARCHAR)) != '' 
                    AND REGEXP_MATCHES(TRIM(CAST(m_fields.cvr_number AS VARCHAR)), '^[0-9]+$')
                    AND m_fields.area_ha > 0.0
            """

            self.duckdb_conn.execute(insert_query)

            # Remove processed records from pending table (original logic)
            self.duckdb_conn.execute("""
                DELETE FROM pending_pesticide_rows 
                WHERE CAST(OriginalPesticideRowID AS VARCHAR) IN (
                    SELECT DISTINCT OriginalPesticideRowID 
                    FROM disaggregated_pesticide_applications 
                    WHERE AllocationMethod = 'Marker_ApplicationAreaToTotalFieldArea_FieldProportional'
                )
            """)

            # Get count of processed records
            count_result = self.duckdb_conn.execute(
                "SELECT COUNT(*) FROM disaggregated_pesticide_applications WHERE AllocationMethod = 'Marker_ApplicationAreaToTotalFieldArea_FieldProportional'"
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
            # Get organic field IDs
            organic_field_ids = self._get_organic_marker_field_ids()

            if not organic_field_ids:
                # If no organic fields found, create empty tuple for SQL
                organic_ids_sql_tuple = "('')"
            else:
                # Convert to SQL tuple format
                organic_ids_list = [f"'{field_id}'" for field_id in organic_field_ids]
                organic_ids_sql_tuple = f"({', '.join(organic_ids_list)})"

            # EXACT original SQL query with organic field exclusion
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
                          AND m.field_id NOT IN {organic_ids_sql_tuple} 
                    GROUP BY CVR, CropCode
                )
                INSERT INTO disaggregated_pesticide_applications
                SELECT
                    uuid() as DisaggregatedID,
                    CAST(p.OriginalPesticideRowID AS VARCHAR) as OriginalPesticideRowID,
                    CAST(p.cvr_number AS VARCHAR) as cvr_number,
                    p.PesticideName, 
                    p.PesticideRegistrationNumber, 
                    p.DosageQuantity, 
                    p.DosageUnit,
                    'marker_non_organic_' || CAST(m_fields.field_id AS VARCHAR) as MatchedFieldID,
                    'block_' || CAST(m_fields.block_id AS VARCHAR) as MatchedBlockID,
                    p.AcreageSize * (m_fields.area_ha / non_organic_totals.TotalNonOrganicMarkerAreaForCVRCrop) as AllocatedArea,
                    'Marker_NonOrganic_ApplicationAreaToTotalFieldArea_FieldProportional' as AllocationMethod,
                    GREATEST(0.0, 1.0 - (ABS(p.AcreageSize - non_organic_totals.TotalNonOrganicMarkerAreaForCVRCrop) / p.AcreageSize / ({self.config.area_tolerance_pct}/100.0))) as MatchConfidence,
                    FALSE as IsPartialFieldCoverage,
                    NOW() as DisaggregationDate
                FROM pending_pesticide_rows p
                JOIN NonOrganicMarkerFieldCVRCropTotals non_organic_totals
                    ON TRIM(CAST(p.cvr_number AS VARCHAR)) = non_organic_totals.CVR 
                    AND TRY_CAST(p.Code AS BIGINT) = non_organic_totals.CropCode
                JOIN marker m_fields 
                    ON non_organic_totals.CVR = TRIM(CAST(m_fields.cvr_number AS VARCHAR))
                    AND non_organic_totals.CropCode = TRY_CAST(m_fields.crop_code AS BIGINT)
                WHERE 
                    p.AcreageSize > 0 AND non_organic_totals.TotalNonOrganicMarkerAreaForCVRCrop > 0
                    AND ABS(p.AcreageSize - non_organic_totals.TotalNonOrganicMarkerAreaForCVRCrop) / p.AcreageSize * 100 <= {self.config.area_tolerance_pct}
                    AND m_fields.cvr_number IS NOT NULL 
                    AND TRIM(CAST(m_fields.cvr_number AS VARCHAR)) != '' 
                    AND REGEXP_MATCHES(TRIM(CAST(m_fields.cvr_number AS VARCHAR)), '^[0-9]+$')
                    AND m_fields.area_ha > 0.0
                    AND m_fields.field_id NOT IN {organic_ids_sql_tuple}
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
                        m.field_id as FieldIdentifier
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
                    NOW() as DisaggregationDate
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

    def _disaggregate_by_adjacent_fields_single_cluster_REMOVED(self) -> int:
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
                f"🔍 Top combinations: {[(cvr, crop, pending, fields) for cvr, crop, pending, fields in pending_cvr_crops[:5]]}"
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
                f"✅ Processing {len(cvr_crop_combinations)} CVR+crop combinations in chunks of {chunk_size} (max {max_chunks} chunks)"
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
                f"Adjacent Fields Spatial Cluster: Processed {total_processed} pesticide applications across {chunks_processed} chunks."
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
                except:
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
                f"🔧 Processing chunk {chunk_num} with {len(cvr_crop_chunk)} CVR+crop combinations (total: {total_combinations}) [1 thread, no insertion order]"
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
                    f"⚠️ Chunk {chunk_num}: No pending records found - skipping (this shouldn't happen with optimized filtering)"
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
                        f2.field_id as field2_id,
                        f1.CVR_Str,
                        f1.Crop_Str
                    FROM FilteredFields f1
                    JOIN FilteredFields f2 ON 
                        f1.CVR_Str = f2.CVR_Str 
                        AND f1.Crop_Str = f2.Crop_Str
                        AND f1.field_id != f2.field_id
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
                        CVR_Str,
                        Crop_Str,
                        -- Use minimum field_id in adjacency as cluster identifier
                        MIN(LEAST(field_id, connected_field)) OVER (PARTITION BY CVR_Str, Crop_Str, field_id) as cluster_id
                    FROM (
                        SELECT 
                            sa.field1_id as field_id,
                            sa.CVR_Str,
                            sa.Crop_Str,
                            sa.field2_id as connected_field
                        FROM SpatialAdjacency sa
                        
                        UNION ALL
                        
                        SELECT 
                            sa.field2_id as field_id,
                            sa.CVR_Str,
                            sa.Crop_Str,
                            sa.field1_id as connected_field
                        FROM SpatialAdjacency sa
                        
                        UNION ALL
                        
                        -- Include isolated fields (not in any adjacency) from our filtered set
                        SELECT 
                            f.field_id,
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
                        ARRAY_AGG(m.area_ha) as cluster_field_areas
                    FROM FieldClusters fc
                    JOIN marker m ON fc.field_id = m.field_id
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
                      AND ABS(p.AcreageSize - ca.cluster_total_area) / p.AcreageSize * 100 <= {self.config.area_tolerance_pct}
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
                    fa.DosageQuantity,
                    fa.DosageUnit,
                    'marker_spatial_' || CAST(fa.field_id AS VARCHAR) as MatchedFieldID,
                    'block_' || CAST(fa.field_id AS VARCHAR) as MatchedBlockID,
                    -- Proportional allocation: pesticide_area * (field_area / cluster_area)
                    fa.AcreageSize * (fa.field_area / fa.cluster_total_area) as AllocatedArea,
                    'Adjacent_Fields_Spatial_Cluster_AreaMatched' as AllocationMethod,
                    -- Confidence based on area match quality and cluster size
                    GREATEST(0.5, 1.0 - (fa.area_diff_pct / {self.config.area_tolerance_pct})) as MatchConfidence,
                    FALSE as IsPartialFieldCoverage,
                    NOW() as DisaggregationDate
                FROM FieldAllocations fa
            """

            # Execute the chunked spatial clustering
            self.duckdb_conn.execute(insert_query)

            # Get count of processed records for this chunk
            count_result = self.duckdb_conn.execute(
                "SELECT COUNT(*) FROM disaggregated_pesticide_applications WHERE AllocationMethod = 'Adjacent_Fields_Spatial_Cluster_AreaMatched'"
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
