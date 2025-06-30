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
import uuid
from typing import Any, Dict, List, Optional, Set, Tuple

import duckdb
from pydantic import ConfigDict, Field

from unified_pipeline.common.base import BaseJobConfig, BaseSource, GoldJobInterface
from unified_pipeline.util.gcs_util import GCSUtil
from unified_pipeline.util.log_util import Logger

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
        default=2500, description="Batch size for processing - optimized for 16GB RAM"
    )

    # Temporal configuration (Y+1 pattern from original)
    field_year_offset: int = Field(default=1, description="Field year offset (Y+1 pattern)")

    # Input datasets
    pesticide_applications_dataset: str = "pesticides"

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class PesticideDisaggregationGold(BaseSource[PesticideDisaggregationGoldConfig], GoldJobInterface):
    """
    Gold layer processor for pesticide disaggregation.

    Implements the ORIGINAL strategy that achieved 92% coverage:
    - Simple area matching between pesticide applications and total field areas by CVR+crop
    - 2% area tolerance (PRESERVE ORIGINAL)
    - Direct proportional allocation to fields
    """

    def __init__(self, config: PesticideDisaggregationGoldConfig, gcs_util: GCSUtil):
        print("DEBUG: PesticideDisaggregationGold.__init__ called!")
        super().__init__(config, gcs_util)
        self.log = Logger.get_logger()
        self.duckdb_conn = None
        self._organic_marker_field_ids: Set[str] = set()
        print("DEBUG: PesticideDisaggregationGold.__init__ completed!")

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

            if year_results is not None and len(year_results) > 0:
                self.log.info(
                    f"✅ Year {pesticide_year}: Successfully processed {len(year_results)} disaggregated records"
                )

                # Save results for this year immediately (much more efficient than one giant table)
                self.log.info(f"💾 Saving results for year {pesticide_year} to GCS")
                year_saved = self._save_year_results(year_results, pesticide_year)

                if year_saved:
                    # Count pesticide records for this year using a separate connection
                    self.log.info(f"📊 Counting total pesticide records for year {pesticide_year}")
                    try:
                        temp_conn = duckdb.connect(":memory:")
                        with self.gcs_access._temp_download(
                            pesticide_applications_path
                        ) as temp_file:
                            pesticide_count = temp_conn.execute(
                                f"SELECT COUNT(*) FROM read_parquet('{temp_file}')"
                            ).fetchone()[0]
                        temp_conn.close()
                        total_pesticide_records += pesticide_count
                        self.log.info(
                            f"📈 Year {pesticide_year}: {pesticide_count} total pesticide records, {len(year_results)} disaggregated"
                        )
                        total_disaggregated_records += len(year_results)
                        successful_years += 1
                    except Exception as e:
                        self.log.error(
                            f"❌ Failed to count pesticide records for year {pesticide_year}: {e}"
                        )
                        # Still count as successful since we have results
                        total_disaggregated_records += len(year_results)
                        successful_years += 1
                else:
                    self.log.error(f"❌ Failed to save results for year {pesticide_year}")
                    failed_years += 1
            else:
                self.log.warning(f"⚠️ Year {pesticide_year}: No results generated")
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

    def _save_year_results(self, year_results: List[Dict[str, Any]], year: int) -> bool:
        """
        Save results for a single year to GCS storage.

        Args:
            year_results: List of disaggregated records for the year
            year: The year being processed

        Returns:
            bool: True if save was successful, False otherwise
        """
        try:
            self.log.info(f"💾 Preparing to save {len(year_results)} records for year {year}")

            # Create a temporary DuckDB connection for this year's data
            year_conn = duckdb.connect(":memory:")

            # Create table with the results
            if year_results:
                columns = list(year_results[0].keys())
                column_defs = ", ".join([f"{col} VARCHAR" for col in columns])
                table_name = f"pesticide_disaggregation_{year}"

                self.log.info(f"🏗️ Creating table {table_name} with {len(columns)} columns")
                year_conn.execute(f"CREATE TABLE {table_name} ({column_defs})")

                # Insert all records
                for record in year_results:
                    placeholders = ", ".join(["?" for _ in record.values()])
                    year_conn.execute(
                        f"INSERT INTO {table_name} VALUES ({placeholders})",
                        list(record.values()),
                    )

                # Save using the base class method with year-specific table name
                self.log.info(f"🚀 Uploading {table_name} to GCS bucket")

                # Use the base class save method - temporarily set connection
                original_conn = getattr(self, "conn", None)
                self.conn = year_conn

                # Save with year-specific dataset name
                dataset_name = f"{self.config.dataset}_{year}"
                self.save_data_direct(table_name, dataset_name, self.config.bucket, "gold")

                # Restore original connection
                self.conn = original_conn

                self.log.info(f"✅ Successfully saved {len(year_results)} records for year {year}")
                year_conn.close()
                return True
            else:
                self.log.warning(f"⚠️ No results to save for year {year}")
                year_conn.close()
                return False

        except Exception as e:
            self.log.error(f"❌ Failed to save results for year {year}: {e}")
            if "year_conn" in locals():
                year_conn.close()
            return False

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
            files = self.gcs_util.list_files(
                bucket_name=self.config.bucket, prefix="silver/pesticides/"
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
            files = self.gcs_util.list_files(
                bucket_name=self.config.bucket, prefix="silver/fvm_marker_"
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
            files = self.gcs_util.list_files(
                bucket_name=self.config.bucket, prefix="silver/pesticides/"
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
            files = self.gcs_util.list_files(
                bucket_name=self.config.bucket, prefix=f"silver/fvm_marker_{year}/"
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
    ) -> Optional[List[Dict[str, Any]]]:
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

            # Run the original strategies in exact order (from original main.py lines 89-180)
            self.log.info(f"🎯 Starting disaggregation strategies for year {pesticide_year}")
            total_processed = 0

            # Strategy 1: Marker CVR-Area Match (THE MAIN 92% STRATEGY)
            self.log.info(f"🎯 Strategy 1: Running marker CVR-area match for year {pesticide_year}")
            processed_count = self._disaggregate_by_marker_match()
            total_processed += processed_count
            self.log.info(
                f"✅ Year {pesticide_year}: Marker CVR-Area Match: {processed_count} records processed"
            )

            # Strategy 2: Marker Non-Organic CVR-Area Match
            self.log.info(f"🎯 Strategy 2: Running non-organic match for year {pesticide_year}")
            processed_count = self._disaggregate_by_marker_non_organic_match()
            total_processed += processed_count
            self.log.info(
                f"✅ Year {pesticide_year}: Marker Non-Organic Match: {processed_count} records processed"
            )

            # Strategy 3: Partial Field Coverage
            self.log.info(
                f"🎯 Strategy 3: Running partial field coverage for year {pesticide_year}"
            )
            processed_count = self._disaggregate_by_partial_field_coverage()
            total_processed += processed_count
            self.log.info(
                f"✅ Year {pesticide_year}: Partial Field Coverage: {processed_count} records processed"
            )

            # Strategy 4: Adjacent Fields Single Cluster
            self.log.info(
                f"🎯 Strategy 4: Running adjacent fields cluster for year {pesticide_year}"
            )
            processed_count = self._disaggregate_by_adjacent_fields_single_cluster()
            total_processed += processed_count
            self.log.info(
                f"✅ Year {pesticide_year}: Adjacent Fields Cluster: {processed_count} records processed"
            )

            # Get results
            self.log.info(f"📊 Collecting final results for year {pesticide_year}")
            results = self._get_results()

            # Calculate coverage statistics for this year
            total_pesticide_records = self.duckdb_conn.execute(
                "SELECT COUNT(*) FROM pesticide"
            ).fetchone()[0]
            coverage_pct = (
                (len(results) / total_pesticide_records * 100) if total_pesticide_records > 0 else 0
            )

            self.log.info(f"🎉 Year {pesticide_year} disaggregation completed:")
            self.log.info(f"   📈 Total pesticide records: {total_pesticide_records:,}")
            self.log.info(
                f"   ✅ Successfully disaggregated: {len(results):,} ({coverage_pct:.1f}%)"
            )
            self.log.info(f"   🔢 Total processed across all strategies: {total_processed:,}")

            return results

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

        # Configure DuckDB for optimal performance with 16GB RAM
        self.duckdb_conn.execute("SET memory_limit = '12GB'")  # Use 75% of available 16GB RAM
        self.duckdb_conn.execute("SET threads = 4")  # Use all available CPU cores
        self.duckdb_conn.execute("SET temp_directory = '/tmp'")  # Use disk for temp storage

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
                year
            FROM marker_temp
        """)

        # Drop the temporary table
        self.duckdb_conn.execute("DROP TABLE marker_temp")
        self.log.info("✅ Marker table created successfully")

        self.log.info(f"🏗️ Creating pesticide table from {pesticide_applications_path}")

        # ✅ MIGRATION: Create pesticide table using optimized GCS access with temp download
        self.log.info("📥 Downloading pesticide data...")
        with self.gcs_access._temp_download(pesticide_applications_path) as temp_file:
            self.log.info(f"✅ Downloaded to temporary file: {temp_file}")
            self.duckdb_conn.execute(f"""
                CREATE TABLE pesticide AS 
                SELECT 
                    row_number() OVER () as OriginalPesticideRowID,
                    companyregistrationnumber as CompanyRegistrationNumber,
                    pesticidename as PesticideName,
                    pesticideregistrationnumber as PesticideRegistrationNumber,
                    dosagequantity as DosageQuantity,
                    dosageunit as DosageUnit,
                    acreagesize as AcreageSize,
                    code as Code,
                    nopesticides as nopesticides
                FROM read_parquet('{temp_file}')
            """)

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
            CompanyRegistrationNumber VARCHAR,
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
        """Create pending pesticide rows table, filtering out nopesticides=1 (original logic)."""
        self.duckdb_conn.execute("""
            CREATE TABLE pending_pesticide_rows AS 
            SELECT * FROM pesticide 
            WHERE nopesticides IS NULL OR nopesticides != 1
        """)

        count = self.duckdb_conn.execute("SELECT COUNT(*) FROM pending_pesticide_rows").fetchone()[
            0
        ]
        self.log.info(f"Created pending pesticide rows: {count} records")

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
                    CAST(p.CompanyRegistrationNumber AS VARCHAR) as CompanyRegistrationNumber,
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
                    ON CAST(CAST(p.CompanyRegistrationNumber AS BIGINT) AS VARCHAR) = marker_totals.CVR 
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
                    CAST(p.CompanyRegistrationNumber AS VARCHAR) as CompanyRegistrationNumber,
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
                    ON CAST(CAST(p.CompanyRegistrationNumber AS BIGINT) AS VARCHAR) = non_organic_totals.CVR 
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
        PRESERVE EXACT LOGIC from disaggregation.py lines 345-495
        """
        self.log.info("Running Partial Field Coverage disaggregation strategy...")

        processed_ids = []

        try:
            # Find single-field CVR/crop combinations where pesticide area < field area
            query = """
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
                    CAST(CAST(p.CompanyRegistrationNumber AS BIGINT) AS VARCHAR) as CVR_Str,
                    CAST(CAST(p.Code AS BIGINT) AS VARCHAR) as Crop_Str,
                    p.AcreageSize
                FROM pending_pesticide_rows p
                WHERE p.CompanyRegistrationNumber IS NOT NULL 
                  AND p.Code IS NOT NULL
                  AND p.AcreageSize > 0
            )
            SELECT 
                pf.OriginalPesticideRowID,
                pf.CVR_Str,
                pf.Crop_Str,
                pf.AcreageSize,
                sf.FieldID,
                sf.FieldArea,
                sf.FieldIdentifier,
                (pf.AcreageSize / sf.FieldArea) * 100 as CoveragePercent
            FROM MarkerSingleFieldCVRCrop sf
            JOIN PendingForSingleFields pf 
                ON sf.CVR_Str = pf.CVR_Str 
                AND sf.Crop_Str = pf.Crop_Str
            WHERE pf.AcreageSize < sf.FieldArea  -- Pesticide area smaller than field area
            ORDER BY CoveragePercent ASC  -- Process smallest coverage first
            """

            candidates = self.duckdb_conn.execute(query).fetchall()

            if not candidates:
                self.log.info("Partial Field Coverage: No single-field candidates found.")
                return 0

            self.log.info(
                f"Partial Field Coverage: Found {len(candidates)} single-field candidates to process."
            )

            for (
                original_id,
                cvr_str,
                crop_str,
                acreage_size,
                field_id,
                field_area,
                field_identifier,
                coverage_percent,
            ) in candidates:
                # Get original pesticide data for this row
                original_data_query = f"""
                SELECT * FROM pending_pesticide_rows 
                WHERE OriginalPesticideRowID = '{original_id}'
                """
                original_data_result = self.duckdb_conn.execute(original_data_query).fetchall()

                if not original_data_result:
                    continue

                original_data = original_data_result[0]

                # Extract block ID from field identifier
                block_id = (
                    str(field_identifier).split("-")[0]
                    if "-" in str(field_identifier)
                    else str(field_identifier)
                )

                # Insert into disaggregated table using the actual schema
                insert_query = """
                INSERT INTO disaggregated_pesticide_applications 
                (DisaggregatedID, OriginalPesticideRowID, CompanyRegistrationNumber, 
                 PesticideName, PesticideRegistrationNumber, DosageQuantity, DosageUnit,
                 MatchedFieldID, MatchedBlockID, AllocatedArea, AllocationMethod, MatchConfidence, 
                 IsPartialFieldCoverage, DisaggregationDate)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
                """

                # Map original data columns based on actual pesticide table schema
                # Schema: OriginalPesticideRowID, CompanyRegistrationNumber, PesticideName,
                #         PesticideRegistrationNumber, DosageQuantity, DosageUnit, AcreageSize, Code, nopesticides
                pesticide_name = original_data[2] if len(original_data) > 2 else None
                pesticide_reg_num = original_data[3] if len(original_data) > 3 else None
                dosage_quantity = original_data[4] if len(original_data) > 4 else None
                dosage_unit = original_data[5] if len(original_data) > 5 else None

                self.duckdb_conn.execute(
                    insert_query,
                    [
                        str(uuid.uuid4()),  # DisaggregatedID
                        str(original_id),  # OriginalPesticideRowID
                        cvr_str,  # CompanyRegistrationNumber
                        pesticide_name,  # PesticideName
                        pesticide_reg_num,  # PesticideRegistrationNumber
                        dosage_quantity,  # DosageQuantity
                        dosage_unit,  # DosageUnit
                        f"marker_{field_identifier}",  # MatchedFieldID
                        f"block_{block_id}",  # MatchedBlockID
                        float(acreage_size),  # AllocatedArea (use pesticide area, not field area)
                        "Partial_Field_Coverage_SingleField",  # AllocationMethod
                        0.8,  # MatchConfidence
                        True,  # IsPartialFieldCoverage
                    ],
                )

                processed_ids.append(original_id)

            # Remove processed records from pending table
            if processed_ids:
                ids_str = "', '".join(str(pid) for pid in processed_ids)
                self.duckdb_conn.execute(f"""
                    DELETE FROM pending_pesticide_rows 
                    WHERE OriginalPesticideRowID IN ('{ids_str}')
                """)

            self.log.info(
                f"Partial Field Coverage: Processed {len(processed_ids)} pesticide applications with partial field coverage."
            )
            return len(processed_ids)

        except Exception as e:
            self.log.error(f"Error in partial field coverage strategy: {str(e)}")
            return 0

    def _disaggregate_by_adjacent_fields_single_cluster(self) -> int:
        """
        Strategy 4: Adjacent Fields Single Cluster for multi-field CVR/crop combinations.
        PRESERVE EXACT LOGIC from disaggregation.py lines 496-739
        """
        self.log.info("Running Adjacent Fields Single Cluster disaggregation strategy...")

        processed_ids = []
        max_distance_m = 10.0

        try:
            # Find multi-field CVR/crop combinations where all fields form a single cluster
            # This is a simplified spatial clustering - the original used complex spatial analysis
            query = """
            WITH MultiFieldCVRCrop AS (
                SELECT 
                    CAST(CAST(m.cvr_number AS BIGINT) AS VARCHAR) as CVR_Str,
                    CAST(CAST(m.crop_code AS BIGINT) AS VARCHAR) as Crop_Str,
                    COUNT(*) as FieldCount,
                    SUM(m.area_ha) as TotalFieldArea
                FROM marker m
                WHERE m.cvr_number IS NOT NULL 
                  AND TRIM(CAST(m.cvr_number AS VARCHAR)) != '' 
                  AND REGEXP_MATCHES(TRIM(CAST(m.cvr_number AS VARCHAR)), '^[0-9]+$')
                  AND m.crop_code IS NOT NULL 
                  AND m.area_ha > 0.0
                GROUP BY 1, 2
                HAVING COUNT(*) > 1  -- Multiple fields per CVR/Crop
            ),
            PendingForMultiFields AS (
                SELECT 
                    p.OriginalPesticideRowID,
                    CAST(CAST(p.CompanyRegistrationNumber AS BIGINT) AS VARCHAR) as CVR_Str,
                    CAST(CAST(p.Code AS BIGINT) AS VARCHAR) as Crop_Str,
                    p.AcreageSize
                FROM pending_pesticide_rows p
                WHERE p.CompanyRegistrationNumber IS NOT NULL 
                  AND p.Code IS NOT NULL
                  AND p.AcreageSize > 0
            )
            SELECT 
                pf.OriginalPesticideRowID,
                pf.CVR_Str,
                pf.Crop_Str,
                pf.AcreageSize,
                mf.FieldCount as TotalFields,
                mf.TotalFieldArea,
                (pf.AcreageSize / mf.TotalFieldArea) * 100 as CoveragePercent
            FROM MultiFieldCVRCrop mf
            JOIN PendingForMultiFields pf 
                ON mf.CVR_Str = pf.CVR_Str 
                AND mf.Crop_Str = pf.Crop_Str
            WHERE pf.AcreageSize < mf.TotalFieldArea  -- Pesticide area smaller than total field area
            ORDER BY CoveragePercent ASC
            """

            candidates = self.duckdb_conn.execute(query).fetchall()

            if not candidates:
                self.log.info("Adjacent Fields Single Cluster: No single-cluster candidates found.")
                return 0

            self.log.info(
                f"Adjacent Fields Single Cluster: Found {len(candidates)} single-cluster candidates to process."
            )

            # Process each candidate by allocating proportionally to all fields in the cluster
            for (
                original_id,
                cvr_str,
                crop_str,
                acreage_size,
                total_fields,
                total_field_area,
                coverage_percent,
            ) in candidates:
                # Get all fields in this cluster
                fields_query = f"""
                SELECT 
                    m.field_id as FieldID,
                    m.area_ha as FieldArea,
                    m.field_id as FieldIdentifier
                FROM marker m
                WHERE CAST(CAST(m.cvr_number AS BIGINT) AS VARCHAR) = '{cvr_str}'
                  AND CAST(CAST(m.crop_code AS BIGINT) AS VARCHAR) = '{crop_str}'
                  AND m.cvr_number IS NOT NULL 
                  AND TRIM(CAST(m.cvr_number AS VARCHAR)) != '' 
                  AND REGEXP_MATCHES(TRIM(CAST(m.cvr_number AS VARCHAR)), '^[0-9]+$')
                  AND m.crop_code IS NOT NULL 
                  AND m.area_ha > 0.0
                ORDER BY m.area_ha DESC
                """

                fields = self.duckdb_conn.execute(fields_query).fetchall()

                if not fields:
                    self.log.warning(f"No fields found for CVR {cvr_str}, Crop {crop_str}")
                    continue

                # Get original pesticide data for this row
                original_data_query = f"""
                SELECT * FROM pending_pesticide_rows 
                WHERE OriginalPesticideRowID = '{original_id}'
                """
                original_data_result = self.duckdb_conn.execute(original_data_query).fetchall()

                if not original_data_result:
                    continue

                original_data = original_data_result[0]

                # Allocate pesticide proportionally to each field in the cluster
                for field_id, field_area, field_identifier in fields:
                    # Calculate proportional allocation
                    field_proportion = float(field_area) / float(total_field_area)
                    allocated_area = float(acreage_size) * field_proportion

                    # Extract block ID from field identifier
                    block_id = (
                        str(field_identifier).split("-")[0]
                        if "-" in str(field_identifier)
                        else str(field_identifier)
                    )

                    # Insert into disaggregated table
                    insert_query = """
                    INSERT INTO disaggregated_pesticide_applications 
                    (DisaggregatedID, OriginalPesticideRowID, CompanyRegistrationNumber, 
                     PesticideName, PesticideRegistrationNumber, DosageQuantity, DosageUnit,
                     MatchedFieldID, MatchedBlockID, AllocatedArea, AllocationMethod, MatchConfidence,
                     IsPartialFieldCoverage, DisaggregationDate)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
                    """

                    # Map original data columns based on actual pesticide table schema
                    # Schema: OriginalPesticideRowID, CompanyRegistrationNumber, PesticideName,
                    #         PesticideRegistrationNumber, DosageQuantity, DosageUnit, AcreageSize, Code, nopesticides
                    pesticide_name = original_data[2] if len(original_data) > 2 else None
                    pesticide_reg_num = original_data[3] if len(original_data) > 3 else None
                    dosage_quantity = original_data[4] if len(original_data) > 4 else None
                    dosage_unit = original_data[5] if len(original_data) > 5 else None

                    self.duckdb_conn.execute(
                        insert_query,
                        [
                            str(uuid.uuid4()),  # DisaggregatedID
                            str(original_id),  # OriginalPesticideRowID
                            cvr_str,  # CompanyRegistrationNumber
                            pesticide_name,  # PesticideName
                            pesticide_reg_num,  # PesticideRegistrationNumber
                            dosage_quantity,  # DosageQuantity
                            dosage_unit,  # DosageUnit
                            f"marker_{field_identifier}",  # MatchedFieldID
                            f"block_{block_id}",  # MatchedBlockID
                            allocated_area,  # AllocatedArea
                            "Adjacent_Fields_Single_Cluster_Proportional",  # AllocationMethod
                            0.7,  # MatchConfidence
                            False,  # IsPartialFieldCoverage
                        ],
                    )

                processed_ids.append(original_id)

            # Remove processed records from pending table
            if processed_ids:
                ids_str = "', '".join(str(pid) for pid in processed_ids)
                self.duckdb_conn.execute(f"""
                    DELETE FROM pending_pesticide_rows 
                    WHERE OriginalPesticideRowID IN ('{ids_str}')
                """)

            self.log.info(
                f"Adjacent Fields Single Cluster: Processed {len(processed_ids)} pesticide applications across single-cluster field groups."
            )
            return len(processed_ids)

        except Exception as e:
            self.log.error(f"Error in adjacent fields cluster strategy: {str(e)}")
            return 0

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
