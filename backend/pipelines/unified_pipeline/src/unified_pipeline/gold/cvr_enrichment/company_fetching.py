"""
Company Raw Data Fetching Step - Step 2 of CVR Enrichment Pipeline

This step fetches raw company data from the CVR register for the collected CVR numbers,
saving raw JSON responses immediately to prevent memory issues. Processing and enrichment
are handled in separate downstream steps following the Bronze→Silver→Gold pattern.

Memory Efficiency:
- Fetches raw JSON data in batches
- Saves immediately to GCS with minimal processing
- Keeps memory usage constant (~50MB) instead of accumulating
- Defers all parsing/enrichment to separate steps
"""

import contextlib
import json
import os
from datetime import datetime
from typing import Any, ClassVar

from pydantic import Field

from unified_pipeline.common.base import BaseJobConfig, BaseSource, GoldJobInterface
from unified_pipeline.util.cvr_api_client import CVRAPIClient
from unified_pipeline.util.timing import timed

from .shared.config import CVREnrichmentSharedConfig, CVREnrichmentStep, get_step_input_paths


class CompanyFetchingConfig(BaseJobConfig):
    """Configuration for company fetching step."""

    name: str = "Company Raw Data Fetching"
    dataset: str = "cvr_raw_companies"
    type: str = "cvr_api"
    description: str = "Fetch raw company JSON data from CVR register with minimal processing"
    frequency: str = "monthly"
    bucket: str = "landbruget-data"

    # Shared configuration
    shared_config: CVREnrichmentSharedConfig = Field(
        default_factory=CVREnrichmentSharedConfig,
        description="Shared configuration for CVR enrichment pipeline",
    )

    # Company fetching specific configuration with memory-aware batching
    memory_safe_batch_size: int = Field(
        default=250,
        description="Number of CVR numbers to process per memory-safe batch "
        "(ultra-conservative for GitHub Actions to prevent OOM errors)",
    )

    fetch_all_fields: bool = Field(
        default=True, description="Whether to fetch all available fields from CVR API"
    )

    # Note: Address geocoding is handled in separate step - disabled here to prevent
    # duplicate DAWA calls
    enable_address_geocoding: bool = Field(
        default=False,
        description="Whether to enrich addresses with geometry "
        "(should be False - handled in Address Geocoding step)",
    )

    model_config: ClassVar[dict[str, bool]] = {"frozen": True}

    def apply_cli_filters(self, cli_config):
        """Apply CLI configuration filters to this config."""
        updates = {}

        if cli_config.test_limit is not None:
            updates["test_limit"] = cli_config.test_limit

        if hasattr(cli_config, "batch_number") and cli_config.batch_number is not None:
            updates["batch_number"] = cli_config.batch_number

        if hasattr(cli_config, "total_batches") and cli_config.total_batches is not None:
            updates["total_batches"] = cli_config.total_batches

        if updates:
            object.__setattr__(
                self,
                "shared_config",
                self.shared_config.model_copy(update=updates),
            )


class CompanyFetching(BaseSource[CompanyFetchingConfig], GoldJobInterface):
    """
    Company fetching step implementation.

    This step:
    1. Loads CVR numbers from collection step
    2. Fetches company data from CVR register API
    3. Processes and structures company data
    4. Saves company data for subsequent steps
    """

    def __init__(self, config: CompanyFetchingConfig):
        """
        Initialize company fetching step.

        Args:
            config: Configuration for company fetching
        """
        super().__init__(config)

        # Apply memory optimizations for GitHub Actions environment
        self._apply_memory_optimizations()

        # Initialize CVR API client
        cvr_username = os.getenv("CVR_USERNAME", "Martin_Collignon_CVR_I_SKYEN")
        cvr_password = os.getenv("CVR_PASSWORD", "3a37d029-9588-4c00-8a09-3d2901452d45")

        self.cvr_api_client = CVRAPIClient(
            username=cvr_username,
            password=cvr_password,
            enable_geocoding=self.config.enable_address_geocoding,
            geocode_current_only=self.config.shared_config.geocoding_current_addresses_only,
        )

        self.log.info("Company fetching step initialized")
        self.log.info("📋 Configuration:")
        self.log.info(
            f"   • Processing mode: Memory-safe batching "
            f"({self.config.memory_safe_batch_size} CVRs per batch)"
        )
        self.log.info(f"   • Fetch all fields: {self.config.fetch_all_fields}")
        geocoding_status = (
            "INCORRECTLY ENABLED - should be disabled!"
            if self.config.enable_address_geocoding
            else "disabled (handled in Address Geocoding step)"
        )
        self.log.info(f"   • Address geocoding: {geocoding_status}")

    def _apply_memory_optimizations(self):
        """
        Apply DuckDB memory optimizations specifically for CVR company fetching.

        The company fetching step processes large JSON structures from the CVR API,
        which can be memory-intensive. This method configures DuckDB for GitHub Actions
        environment with conservative memory settings.
        """
        try:
            self.log.info(
                "🔧 Applying CVR company fetching memory optimizations for GitHub Actions..."
            )

            # CRITICAL: Reduce memory limit for GitHub Actions (16GB total RAM)
            # Leave 10GB buffer for OS, Python, and API processing to prevent OOM
            self.conn.execute("SET memory_limit = '6GB'")  # Ultra-conservative limit
            self.conn.execute("SET max_memory = '6GB'")

            # CRITICAL: Reduce threads to minimize memory pressure
            # CVR API processing is I/O bound, fewer threads = less memory per thread
            self.conn.execute("SET threads = 2")  # Reduced from default 4

            # CRITICAL: Enable memory-efficient settings
            self.conn.execute("SET preserve_insertion_order = false")  # Allow reordering
            self.conn.execute("SET enable_progress_bar = false")  # Reduce overhead

            # CRITICAL: More aggressive temporary directory management
            self.conn.execute("SET temp_directory = '/tmp/duckdb_cvr_company'")
            self.conn.execute("SET max_temp_directory_size = '3GB'")  # Conservative limit

            # NOTE: Checkpoint settings don't apply to in-memory databases
            # DuckDB handles memory management automatically for in-memory mode

            # ADDITIONAL: Disable object cache to reduce memory usage
            self.conn.execute("SET enable_object_cache = false")  # Prioritize memory over speed

            self.log.info("✅ CVR company fetching memory optimizations applied")
            self.log.info("   • Memory limit: 6GB (ultra-conservative for OOM prevention)")
            self.log.info("   • Threads: 2 (reduced from 4)")
            self.log.info("   • Temp directory size: 3GB")
            self.log.info("   • Database: In-memory (no checkpointing needed)")

        except Exception as e:
            self.log.warning(f"Failed to apply memory optimizations: {e}")

    def _cleanup_batch_memory(self):
        """
        Aggressive memory cleanup after processing each batch.

        This is critical for preventing memory accumulation across batches.
        Each batch can contain large JSON structures from CVR API responses,
        and without proper cleanup, memory usage grows until OOM errors occur.

        IMPORTANT: This method is called AFTER batch data has been safely
        inserted into DuckDB tables, so no data is lost during cleanup.
        """
        try:
            self.log.debug("🧹 Starting batch memory cleanup (data already persisted)...")

            # NOTE: CHECKPOINT is not supported in in-memory databases
            # Data is already safely stored in DuckDB in-memory tables
            # We rely on DuckDB's internal memory management instead

            # NOTE: PRAGMA optimize doesn't exist in DuckDB
            # For in-memory databases, we rely on DuckDB's internal memory management
            # and Python garbage collection instead
            self.log.debug("   ✓ DuckDB memory management (internal)")

            # CRITICAL: Clear query plan cache to free memory
            try:
                self.conn.execute("PRAGMA cache_size = 0")  # Clear cache
                self.conn.execute("PRAGMA cache_size = -1000")  # Reset to small cache
                self.log.debug("   ✓ Query plan cache cleared")
            except Exception as pragma_e:
                self.log.debug(f"   ⚠ Cache cleanup warning: {pragma_e}")

            # CRITICAL: Force Python garbage collection to free API response objects
            import gc

            collected = gc.collect()
            self.log.debug(f"   ✓ Python garbage collection: freed {collected} objects")

            # ADDITIONAL: Clear any temporary tables or views (preserving main data tables)
            try:
                temp_objects = self.conn.execute("""
                    SELECT table_name FROM information_schema.tables
                    WHERE table_name LIKE 'temp_%' OR table_name LIKE 'tmp_%'
                """).fetchall()

                dropped_count = 0
                for (temp_table,) in temp_objects:
                    try:
                        self.conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
                        dropped_count += 1
                    except Exception:
                        pass  # Ignore errors dropping temp tables

                if dropped_count > 0:
                    self.log.debug(f"   ✓ Dropped {dropped_count} temporary tables")

            except Exception:
                pass  # Ignore errors in temp cleanup

            self.log.debug("✅ Batch memory cleanup completed - ready for next batch")

        except Exception as e:
            self.log.warning(f"Memory cleanup warning (non-critical): {e}")
            # Continue processing even if cleanup fails

    @timed(name="Company fetching processing")
    async def run(self, silver_data: dict[str, Any] | None = None) -> str:
        """
        Run the company fetching process with memory-safe batching.

        Args:
            silver_data: Optional silver data (not used in this step)

        Returns:
            Table name containing company data
        """
        self.log.info("Starting company fetching step with memory-safe batching")

        try:
            # Step 1: Load all CVR numbers from collection step
            self.log.info("📋 Step 1/4: Loading all CVR numbers from collection")
            all_cvr_numbers = self._load_all_cvr_numbers()

            # Step 2: Create memory-safe batches
            self.log.info("📦 Step 2/4: Creating memory-safe processing batches")
            cvr_batches = self._create_memory_safe_batches(all_cvr_numbers)

            # Step 3: Fetch all CVR batches with raw data streaming to GCS
            self.log.info("🌐 Step 3/4: Fetching CVR batches with raw data streaming")
            total_stats = await self._process_all_batches(cvr_batches)

            # Step 4: Consolidate raw batch files into single raw data file
            self.log.info("🔄 Step 4/4: Consolidating raw batch files")
            final_table_name = self._consolidate_batch_files(len(cvr_batches), total_stats)

            # Success summary
            self.log.info("=" * 60)
            self.log.info("✅ COMPANY RAW DATA FETCHING COMPLETED SUCCESSFULLY")
            self.log.info("=" * 60)
            self.log.info("📊 Final Statistics:")
            self.log.info(f"   • Total companies fetched: {total_stats['total_successful']:,}")
            self.log.info(f"   • Total failures: {total_stats['total_failed']:,}")
            self.log.info(f"   • Total API calls: {total_stats['total_api_calls']:,}")
            self.log.info(f"   • Raw batch files processed: {len(cvr_batches)}")
            self.log.info(f"   • Consolidated raw data table: {final_table_name}")
            self.log.info("   • Memory usage: ~50MB max (raw JSON only)")
            self.log.info("   • Next step: Create separate parsing/enrichment jobs")
            self.log.info("=" * 60)

            return final_table_name

        except Exception as e:
            self.log.error("=" * 60)
            self.log.error("❌ COMPANY FETCHING FAILED")
            self.log.error("=" * 60)
            self.log.error(f"💥 Error: {e}")
            self.log.error("🔍 Check the logs above for detailed error information")
            self.log.error(
                f"📋 Processing mode: Memory-safe batching "
                f"({self.config.memory_safe_batch_size} CVRs per batch)"
            )
            self.log.error("=" * 60)
            raise

    @timed(name="Loading all CVR numbers")
    def _load_all_cvr_numbers(self) -> list[str]:
        """
        Load all CVR numbers from collection step output.

        Returns:
            List of all CVR numbers to process
        """
        self.log.info("Loading all CVR numbers from collection step")

        # Get input paths from collection step (with independent execution support)
        input_paths = get_step_input_paths(
            CVREnrichmentStep.COMPANY_FETCHING,
            self.date_pattern,
            bucket=self.config.bucket,
            enable_independent_execution=self.config.shared_config.enable_independent_execution,
            max_days_back=self.config.shared_config.max_days_back_for_inputs,
        )

        if not input_paths:
            raise ValueError("No collection data found for company fetching step")

        collection_path = input_paths[0]  # Should be collection.parquet

        # Load collection data
        self.log.info(f"Loading collection data from: {collection_path}")

        try:
            # Check if running in GitHub Actions and local artifact file exists
            import os

            local_artifact_path = "/tmp/cvr_collection_data.parquet"

            if os.getenv("GITHUB_ACTIONS") == "true" and os.path.exists(local_artifact_path):
                self.log.info("GitHub Actions detected - using local artifact data")
                result = self.conn.execute(f"""
                    SELECT cvr_number, collection_metadata
                    FROM read_parquet('{local_artifact_path}')
                    ORDER BY cvr_number
                """).fetchall()
            else:
                # Load CVR numbers directly from R2 using DuckDB (fallback)
                duckdb_collection_path = "r2://" + collection_path
                result = self.conn.execute(f"""
                    SELECT cvr_number, collection_metadata
                    FROM read_parquet('{duckdb_collection_path}')
                    ORDER BY cvr_number
                """).fetchall()

            all_cvrs = [row[0] for row in result]

            # Apply batch filtering if batch_number and total_batches are specified (for GitHub Actions job splitting)
            if (
                self.config.shared_config.batch_number is not None
                and self.config.shared_config.total_batches is not None
            ):
                batch_num = self.config.shared_config.batch_number
                total_batches = self.config.shared_config.total_batches

                self.log.info(
                    f"📦 Batch filtering enabled: Processing batch {batch_num} of {total_batches}"
                )

                # Split CVRs into equal chunks for parallel processing
                import math

                chunk_size = math.ceil(len(all_cvrs) / total_batches)
                start_idx = (batch_num - 1) * chunk_size
                end_idx = min(batch_num * chunk_size, len(all_cvrs))

                all_cvrs = all_cvrs[start_idx:end_idx]

                self.log.info(
                    f"📦 Batch {batch_num}/{total_batches}: Processing {len(all_cvrs):,} CVR numbers (indices {start_idx:,}-{end_idx:,})"
                )
            else:
                self.log.info(
                    f"Loaded {len(all_cvrs):,} CVR numbers for memory-safe batch processing"
                )

            return all_cvrs

        except Exception as e:
            self.log.error(f"Failed to load all CVR numbers: {e}")
            raise

    @timed(name="Creating memory-safe batches")
    def _create_memory_safe_batches(self, all_cvr_numbers: list[str]) -> list[list[str]]:
        """
        Split CVR numbers into memory-safe batches.

        Args:
            all_cvr_numbers: List of all CVR numbers to process

        Returns:
            List of CVR batches, each containing up to memory_safe_batch_size CVRs
        """
        import math

        total_cvrs = len(all_cvr_numbers)
        batch_size = self.config.memory_safe_batch_size
        num_batches = math.ceil(total_cvrs / batch_size)

        batches = []
        for i in range(num_batches):
            start_idx = i * batch_size
            end_idx = min(start_idx + batch_size, total_cvrs)
            batch = all_cvr_numbers[start_idx:end_idx]
            batches.append(batch)

        self.log.info(f"Created {num_batches} memory-safe batches:")
        self.log.info(f"   • Total CVRs: {total_cvrs:,}")
        self.log.info(f"   • Batch size: {batch_size:,}")
        self.log.info(f"   • Number of batches: {num_batches}")
        self.log.info(f"   • Last batch size: {len(batches[-1]) if batches else 0}")

        return batches

    @timed(name="Initializing output tables")
    def _initialize_output_tables(self) -> str:
        """
        Initialize empty output tables with proper schema.

        Returns:
            Table name for the main companies table
        """
        table_name = "cvr_companies"

        # Drop existing tables
        for table in [table_name, "cvr_persons", "cvr_employment"]:
            self.conn.execute(f"DROP TABLE IF EXISTS {table}")

        # Create main companies table with proper schema
        self.conn.execute(f"""
            CREATE TABLE {table_name} (
                cvr_number INTEGER,
                company_uuid VARCHAR,
                company_name VARCHAR,
                company_type_description VARCHAR,
                status VARCHAR,
                founded_date VARCHAR,
                dissolution_date VARCHAR,
                advertisement_protection BOOLEAN,
                pnumber_count INTEGER,
                current_full_address VARCHAR,
                current_street_name VARCHAR,
                current_house_number VARCHAR,
                current_floor VARCHAR,
                current_door VARCHAR,
                current_postal_code INTEGER,
                current_city VARCHAR,
                current_municipality_code INTEGER,
                current_municipality_name VARCHAR,
                current_address_type VARCHAR,
                latitude DOUBLE,
                longitude DOUBLE,
                coordinate_quality VARCHAR,
                coordinate_source VARCHAR,
                dawa_enriched BOOLEAN,
                primary_industry_code VARCHAR,
                primary_industry_description VARCHAR,
                is_agricultural_company BOOLEAN,
                company_data_json VARCHAR,
                processing_timestamp VARCHAR
            )
        """)

        # Create persons table with proper schema
        self.conn.execute("""
            CREATE TABLE cvr_persons (
                person_uuid VARCHAR,
                unit_number BIGINT,
                person_type VARCHAR,
                current_name VARCHAR,
                current_city VARCHAR,
                current_postal_code INTEGER,
                current_municipality VARCHAR,
                company_uuid VARCHAR,
                cvr_number INTEGER,
                role VARCHAR,
                role_formatted VARCHAR,
                role_start_date VARCHAR,
                role_end_date VARCHAR,
                is_current_role BOOLEAN,
                is_leadership BOOLEAN,
                is_owner BOOLEAN,
                processing_timestamp VARCHAR
            )
        """)

        # Create employment table with proper schema
        self.conn.execute("""
            CREATE TABLE cvr_employment (
                employment_uuid VARCHAR,
                company_uuid VARCHAR,
                cvr_number INTEGER,
                year INTEGER,
                quarter INTEGER,
                month INTEGER,
                total_employees INTEGER,
                full_time_equivalent DOUBLE,
                employees_including_owners INTEGER,
                fte_interval_code VARCHAR,
                employees_interval_code VARCHAR,
                owners_interval_code VARCHAR,
                last_updated VARCHAR,
                employment_type VARCHAR,
                processing_timestamp VARCHAR
            )
        """)

        # Setup UUID functions for DuckDB
        from unified_pipeline.common.uuid_utils import LandbrugsdataUUID

        LandbrugsdataUUID.setup_duckdb_functions(self.conn)

        self.log.info(f"Initialized empty output tables: {table_name}, cvr_persons, cvr_employment")
        self.log.info("Setup UUID functions for DuckDB")

        return table_name

    @timed(name="Processing all batches")
    async def _process_all_batches(self, cvr_batches: list[list[str]]) -> dict[str, Any]:
        """
        Process all CVR batches sequentially with memory management.

        Args:
            cvr_batches: List of CVR batches to process
            table_name: Name of the main output table

        Returns:
            Dictionary containing processing statistics
        """
        total_stats = {
            "total_requested": sum(len(batch) for batch in cvr_batches),
            "total_batches": len(cvr_batches),
            "total_successful": 0,
            "total_failed": 0,
            "total_api_calls": 0,
            "batches_processed": 0,
            "batches_failed": 0,
        }

        for batch_idx, cvr_batch in enumerate(cvr_batches, 1):
            self.log.info(
                f"🔄 Processing batch {batch_idx}/{len(cvr_batches)} ({len(cvr_batch)} CVRs)"
            )

            try:
                # Process this batch
                batch_stats = await self._process_single_batch(
                    cvr_batch, batch_idx, len(cvr_batches)
                )

                # Update total stats
                total_stats["total_successful"] += batch_stats["successful"]
                total_stats["total_failed"] += batch_stats["failed"]
                total_stats["total_api_calls"] += batch_stats["api_calls"]
                total_stats["batches_processed"] += 1

                # CRITICAL: Memory cleanup after each batch (data already persisted)
                self._cleanup_batch_memory()

                # Progress update
                progress = (batch_idx / len(cvr_batches)) * 100
                self.log.info(f"✅ Batch {batch_idx} completed. Progress: {progress:.1f}%")

            except Exception as e:
                self.log.error(f"❌ Batch {batch_idx} failed: {e}")
                total_stats["batches_failed"] += 1
                total_stats["total_failed"] += len(cvr_batch)

                # Continue processing other batches
                continue

        return total_stats

    @timed(name="Processing single batch")
    async def _process_single_batch(
        self, cvr_batch: list[str], batch_idx: int, total_batches: int
    ) -> dict[str, Any]:
        """
        Process a single batch of CVR numbers and immediately save to GCS.

        Args:
            cvr_batch: List of CVR numbers in this batch
            batch_idx: Index of this batch (for logging)
            total_batches: Total number of batches

        Returns:
            Dictionary containing batch processing statistics
        """
        # Fetch company data for this batch
        company_data = await self._fetch_company_data(cvr_batch)

        # Save raw data immediately to storage (minimal processing, maximum memory efficiency)
        self._save_raw_batch_to_storage(company_data, batch_idx, total_batches)

        # Return batch stats
        return {
            "successful": company_data.get("summary", {}).get("successful", 0),
            "failed": company_data.get("summary", {}).get("failed", 0),
            "api_calls": company_data.get("summary", {}).get("api_calls", 0),
        }

    def _append_batch_to_tables(self, processed_data: dict[str, Any], table_name: str) -> None:
        """
        Append batch data to existing tables.

        Args:
            processed_data: Processed company data for this batch
            table_name: Name of the main companies table
        """
        companies_data = processed_data["companies"]

        if not companies_data:
            self.log.debug("No companies data to append")
            return

        # Convert to JSON strings for DuckDB
        json_strings = [json.dumps(company) for company in companies_data]

        # Insert into main companies table
        self.conn.execute(
            f"""
            INSERT INTO {table_name}
            SELECT
                json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                company_uuid(json_extract(json_data, '$.cvr_number')::INTEGER) as company_uuid,
                json_extract_string(json_data, '$.company_name') as company_name,
                json_extract_string(json_data, '$.company_type_description')
                    as company_type_description,
                json_extract_string(json_data, '$.status') as status,
                json_extract_string(json_data, '$.founded_date') as founded_date,
                json_extract_string(json_data, '$.dissolution_date') as dissolution_date,
                json_extract(json_data, '$.advertisement_protection')::BOOLEAN
                    as advertisement_protection,
                json_extract(json_data, '$.pnumber_count')::INTEGER as pnumber_count,
                -- Note: Structured address fields will be populated in Address Geocoding step
                -- These fields will be NULL initially and filled by primary address selection
                NULL::VARCHAR as current_full_address,
                NULL::VARCHAR as current_street_name,
                NULL::VARCHAR as current_house_number,
                NULL::VARCHAR as current_floor,
                NULL::VARCHAR as current_door,
                NULL::INTEGER as current_postal_code,
                NULL::VARCHAR as current_city,
                NULL::INTEGER as current_municipality_code,
                NULL::VARCHAR as current_municipality_name,
                NULL::VARCHAR as current_address_type,
                NULL::DOUBLE as latitude,
                NULL::DOUBLE as longitude,
                NULL::VARCHAR as coordinate_quality,
                NULL::VARCHAR as coordinate_source,
                NULL::BOOLEAN as dawa_enriched,
                -- Extract industry information from the JSON data
                CASE
                    WHEN json_array_length(json_extract(json_data, '$.industries')) > 0 THEN
                        json_extract_string(
                            json_extract(json_data, '$.industries[0]'),
                            '$.industry_code'
                        )
                    ELSE NULL
                END as primary_industry_code,
                CASE
                    WHEN json_array_length(json_extract(json_data, '$.industries')) > 0 THEN
                        json_extract_string(
                            json_extract(json_data, '$.industries[0]'),
                            '$.industry_description'
                        )
                    ELSE NULL
                END as primary_industry_description,
                CASE
                    WHEN json_array_length(json_extract(json_data, '$.industries')) > 0 THEN
                        CASE
                            -- Get the primary industry code
                            WHEN json_extract_string(
                                json_extract(json_data, '$.industries[0]'),
                                '$.industry_code'
                            ) IS NOT NULL
                            AND json_extract(
                                json_extract(json_data, '$.industries[0]'),
                                '$.is_current'
                            )::BOOLEAN = true
                            THEN
                                CASE
                                    -- Primary Agriculture, Forestry and Fishing (codes 01-03)
                                    WHEN json_extract_string(
                                        json_extract(json_data, '$.industries[0]'),
                                        '$.industry_code'
                                    ) LIKE '01%'
                                         OR json_extract_string(
                                             json_extract(json_data, '$.industries[0]'),
                                             '$.industry_code'
                                         ) LIKE '02%'
                                         OR json_extract_string(
                                             json_extract(json_data, '$.industries[0]'),
                                             '$.industry_code'
                                         ) LIKE '03%' THEN true
                                    -- Fish farming and aquaculture
                                    WHEN json_extract_string(
                                        json_extract(json_data, '$.industries[0]'),
                                        '$.industry_code'
                                    ) IN ('050200') THEN true
                                    -- Real estate (agricultural properties)
                                    WHEN json_extract_string(
                                        json_extract(json_data, '$.industries[0]'),
                                        '$.industry_code'
                                    ) IN ('702040', '682040') THEN true
                                    -- Veterinary services
                                    WHEN json_extract_string(
                                        json_extract(json_data, '$.industries[0]'),
                                        '$.industry_code'
                                    ) IN ('852000', '750000') THEN true
                                    -- Agricultural support services and consulting
                                    WHEN json_extract_string(
                                        json_extract(json_data, '$.industries[0]'),
                                        '$.industry_code'
                                    ) IN ('749010', '741410') THEN true
                                    -- Agricultural machinery and equipment
                                    WHEN json_extract_string(
                                        json_extract(json_data, '$.industries[0]'),
                                        '$.industry_code'
                                    ) IN (
                                        '516600', '773100', '713100', '466100',
                                        '518800', '283000', '293220'
                                    ) THEN true
                                    -- Agricultural trade (livestock, feed, plants)
                                    WHEN json_extract_string(
                                        json_extract(json_data, '$.industries[0]'),
                                        '$.industry_code'
                                    ) IN (
                                        '512300', '462100', '462300', '462200',
                                        '512100', '512200', '461100', '511100'
                                    ) THEN true
                                    -- Agricultural processing and food production
                                    WHEN json_extract_string(
                                        json_extract(json_data, '$.industries[0]'),
                                        '$.industry_code'
                                    ) IN (
                                        '151110', '109100', '101110', '110200',
                                        '101190', '101300', '105100'
                                    ) THEN true
                                    -- Agricultural retail (flowers, pets)
                                    WHEN json_extract_string(
                                        json_extract(json_data, '$.industries[0]'),
                                        '$.industry_code'
                                    ) IN ('524875', '477630', '524885', '477610') THEN true
                                    -- Agricultural education and storage
                                    WHEN json_extract_string(
                                        json_extract(json_data, '$.industries[0]'),
                                        '$.industry_code'
                                    ) IN ('802240', '631200', '521000') THEN true
                                    -- Default to false for all other sectors
                                    ELSE false
                                END
                            ELSE false
                        END
                    ELSE false
                END as is_agricultural_company,
                json_data as company_data_json,
                json_extract_string(json_data, '$.processing_timestamp')
                    as processing_timestamp
            FROM unnest($1) as t(json_data)
        """,
            [json_strings],
        )

        # Append to persons table
        self._append_persons_batch(json_strings)

        # Append to employment table
        self._append_employment_batch(json_strings)

        self.log.debug(f"Appended {len(companies_data)} companies to output tables")

    @timed(name="Saving raw batch to storage")
    def _save_raw_batch_to_storage(
        self, company_data: dict[str, Any], batch_idx: int, total_batches: int
    ) -> None:
        """
        Save raw API response data immediately to storage with minimal processing.

        This approach saves raw JSON responses directly, keeping memory usage minimal
        and deferring all processing to separate parsing/enrichment steps.

        Args:
            company_data: Raw company data from CVR API
            batch_idx: Current batch index (1-based)
            total_batches: Total number of batches
        """
        raw_results = company_data.get("results", {})

        if not raw_results:
            self.log.debug(f"No raw data to save for batch {batch_idx}")
            return

        # Create minimal table with raw JSON only
        batch_table_name = f"raw_batch_{batch_idx}"

        try:
            # Drop existing batch table
            self.conn.execute(f"DROP TABLE IF EXISTS {batch_table_name}")

            # Create simple table with raw JSON data
            raw_json_list = []
            for cvr_number, company_result in raw_results.items():
                # company_result is the company data itself, not wrapped in {"data": ...}
                if company_result:
                    raw_entry = {
                        "cvr_number": int(cvr_number),
                        "raw_json": json.dumps(company_result),
                        "fetch_timestamp": company_data.get("fetch_timestamp"),
                        "batch_number": batch_idx,
                    }
                    raw_json_list.append(json.dumps(raw_entry))

            if raw_json_list:
                # Create table with minimal schema
                self.conn.execute(
                    f"""
                    CREATE TABLE {batch_table_name} AS
                    SELECT
                        json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                        json_extract_string(json_data, '$.raw_json') as raw_json,
                        json_extract_string(json_data, '$.fetch_timestamp') as fetch_timestamp,
                        json_extract(json_data, '$.batch_number')::INTEGER as batch_number
                    FROM unnest($1) as t(json_data)
                """,
                    [raw_json_list],
                )

                # Save to GCS immediately
                # Use subdirectory for GitHub Actions batch part to avoid file collisions
                timestamp = self.date_pattern
                ga_batch = self.config.shared_config.batch_number
                batch_path_suffix = f"raw_batch_{batch_idx:03d}_of_{total_batches:03d}"

                if ga_batch is not None:
                    # Running as part of split GitHub Actions job - use part subdirectory
                    raw_storage_path = f"{self.config.bucket}/bronze/cvr_raw_companies/{timestamp}/part{ga_batch}/{batch_path_suffix}.parquet"
                else:
                    # Running as single job - use flat structure
                    raw_storage_path = f"{self.config.bucket}/bronze/cvr_raw_companies/{timestamp}/{batch_path_suffix}.parquet"

                self.storage.upload_from_duckdb_table(
                    batch_table_name,
                    raw_storage_path,
                    compression="zstd",
                    row_group_size=100000,
                )

                self.log.info(
                    f"✅ Saved raw batch {batch_idx}/{total_batches} to GCS ({len(raw_json_list)} companies, ~{len(str(raw_json_list)) / 1024 / 1024:.1f}MB)"
                )

        finally:
            # Clean up batch table immediately
            with contextlib.suppress(Exception):
                self.conn.execute(f"DROP TABLE IF EXISTS {batch_table_name}")

    def _create_batch_companies_table(self, json_strings: list[str], table_name: str) -> None:
        """Create companies table for a single batch."""
        self.conn.execute(
            f"""
            CREATE TABLE {table_name} AS
            SELECT
                json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                company_uuid(json_extract(json_data, '$.cvr_number')::INTEGER) as company_uuid,
                json_extract_string(json_data, '$.company_name') as company_name,
                json_extract_string(json_data, '$.company_type_description') as company_type_description,
                json_extract_string(json_data, '$.status') as status,
                json_extract_string(json_data, '$.founded_date') as founded_date,
                json_extract_string(json_data, '$.dissolution_date') as dissolution_date,
                json_extract(json_data, '$.advertisement_protection')::BOOLEAN as advertisement_protection,
                json_extract(json_data, '$.pnumber_count')::INTEGER as pnumber_count,
                NULL::VARCHAR as current_full_address,
                NULL::VARCHAR as current_street_name,
                NULL::VARCHAR as current_house_number,
                NULL::VARCHAR as current_floor,
                NULL::VARCHAR as current_door,
                NULL::INTEGER as current_postal_code,
                NULL::VARCHAR as current_city,
                NULL::INTEGER as current_municipality_code,
                NULL::VARCHAR as current_municipality_name,
                NULL::VARCHAR as current_address_type,
                NULL::DOUBLE as latitude,
                NULL::DOUBLE as longitude,
                NULL::VARCHAR as coordinate_quality,
                NULL::VARCHAR as coordinate_source,
                NULL::BOOLEAN as dawa_enriched,
                CASE
                    WHEN json_array_length(json_extract(json_data, '$.industries')) > 0 THEN
                        json_extract_string(json_extract(json_data, '$.industries[0]'), '$.industry_code')
                    ELSE NULL
                END as primary_industry_code,
                CASE
                    WHEN json_array_length(json_extract(json_data, '$.industries')) > 0 THEN
                        json_extract_string(json_extract(json_data, '$.industries[0]'), '$.industry_description')
                    ELSE NULL
                END as primary_industry_description,
                CASE
                    WHEN json_array_length(json_extract(json_data, '$.industries')) > 0 THEN
                        CASE
                            WHEN json_extract_string(json_extract(json_data, '$.industries[0]'), '$.industry_code') IS NOT NULL
                            AND json_extract(json_extract(json_data, '$.industries[0]'), '$.is_current')::BOOLEAN = true
                            THEN
                                CASE
                                    WHEN json_extract_string(json_extract(json_data, '$.industries[0]'), '$.industry_code') LIKE '01%'
                                         OR json_extract_string(json_extract(json_data, '$.industries[0]'), '$.industry_code') LIKE '02%'
                                         OR json_extract_string(json_extract(json_data, '$.industries[0]'), '$.industry_code') LIKE '03%'
                                    THEN true
                                    ELSE false
                                END
                            ELSE false
                        END
                    ELSE false
                END as is_agricultural_company,
                json_data as company_data_json,
                json_extract_string(json_data, '$.processing_timestamp') as processing_timestamp
            FROM unnest($1) as t(json_data)
        """,
            [json_strings],
        )

    def _create_batch_persons_table(self, json_strings: list[str], table_name: str) -> None:
        """Create persons table for a single batch."""
        # Note: Filter for leadership_count > 0 BEFORE generate_series to avoid
        # UINT64 overflow when computing (0 - 1) on empty arrays
        self.conn.execute(
            f"""
            CREATE TABLE {table_name} AS
            WITH raw_with_leadership_count AS (
                SELECT
                    json_data,
                    json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                    json_array_length(json_extract(json_data, '$.leadership')) as leadership_count
                FROM unnest($1) as t(json_data)
            ),
            filtered_with_leadership AS (
                -- Filter BEFORE generate_series to avoid UINT64 overflow on (0-1)
                SELECT *
                FROM raw_with_leadership_count
                WHERE leadership_count > 0
            ),
            leadership_flattened AS (
                SELECT
                    cvr_number,
                    json_data,
                    idx as leadership_idx
                FROM filtered_with_leadership
                CROSS JOIN unnest(generate_series(0, (leadership_count - 1)::BIGINT)) as t2(idx)
            )
            SELECT
                uuid() as person_uuid,
                cvr_number,
                company_uuid(cvr_number) as company_uuid,
                leadership_idx,
                'leadership' as relation_type,
                json_extract_string(json_extract(json_data, '$.leadership[' || leadership_idx || ']'), '$.relation_type') as person_relation_type,
                json_data as person_data_json,
                json_extract_string(json_data, '$.processing_timestamp') as processing_timestamp
            FROM leadership_flattened
        """,
            [json_strings],
        )

    def _create_batch_employment_table(self, json_strings: list[str], table_name: str) -> None:
        """Create employment table for a single batch."""
        # Note: Filter for employment_count > 0 BEFORE generate_series to avoid
        # UINT64 overflow when computing (0 - 1) on empty arrays
        self.conn.execute(
            f"""
            CREATE TABLE {table_name} AS
            WITH raw_with_employment_count AS (
                SELECT
                    json_data,
                    json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                    json_array_length(json_extract(json_data, '$.employment')) as employment_count
                FROM unnest($1) as t(json_data)
            ),
            filtered_with_employment AS (
                -- Filter BEFORE generate_series to avoid UINT64 overflow on (0-1)
                SELECT *
                FROM raw_with_employment_count
                WHERE employment_count > 0
            ),
            employment_flattened AS (
                SELECT
                    cvr_number,
                    json_data,
                    idx as employment_idx
                FROM filtered_with_employment
                CROSS JOIN unnest(generate_series(0, (employment_count - 1)::BIGINT)) as t2(idx)
            )
            SELECT
                uuid() as employment_uuid,
                cvr_number,
                company_uuid(cvr_number) as company_uuid,
                employment_idx,
                json_extract(json_extract(json_data, '$.employment[' || employment_idx || ']'), '$.employee_count')::INTEGER as employee_count,
                json_extract_string(json_extract(json_data, '$.employment[' || employment_idx || ']'), '$.period_start') as period_start,
                json_extract_string(json_extract(json_data, '$.employment[' || employment_idx || ']'), '$.period_end') as period_end,
                json_extract(json_extract(json_data, '$.employment[' || employment_idx || ']'), '$.is_current')::BOOLEAN as is_current,
                json_data as employment_data_json,
                json_extract_string(json_data, '$.processing_timestamp') as processing_timestamp
            FROM employment_flattened
        """,
            [json_strings],
        )

    @timed(name="Consolidating raw batch files")
    def _consolidate_batch_files(self, total_batches: int, total_stats: dict[str, Any]) -> str:
        """
        Consolidate all raw batch files into a single raw data file.

        This creates a consolidated raw JSON file that can be processed by
        separate parsing/enrichment steps, maintaining the Bronze→Silver→Gold pattern.

        When running as part of a split GitHub Actions job (with batch_number set),
        this only consolidates the current part's batches into a part-specific file.
        A separate consolidation step will merge all parts together.

        Args:
            total_batches: Total number of batch files created (for this part)
            total_stats: Statistics from batch processing

        Returns:
            Name of the consolidated raw data table
        """
        timestamp = self.date_pattern
        ga_batch = self.config.shared_config.batch_number
        ga_total_batches = self.config.shared_config.total_batches
        consolidated_table = "cvr_raw_data_consolidated"

        if ga_batch is not None:
            self.log.info(
                f"Consolidating {total_batches} raw batch files for part {ga_batch}/{ga_total_batches}"
            )
        else:
            self.log.info(
                f"Consolidating {total_batches} raw batch files into single raw data file"
            )

        try:
            # Drop existing consolidated table
            self.conn.execute(f"DROP TABLE IF EXISTS {consolidated_table}")

            # Consolidate all raw batch files for this part
            raw_batch_patterns = []
            for batch_idx in range(1, total_batches + 1):
                batch_path_suffix = f"raw_batch_{batch_idx:03d}_of_{total_batches:03d}"

                if ga_batch is not None:
                    # Running as part of split job - use part subdirectory
                    raw_storage_path = f"r2://{self.config.bucket}/bronze/cvr_raw_companies/{timestamp}/part{ga_batch}/{batch_path_suffix}.parquet"
                else:
                    # Running as single job - use flat structure
                    raw_storage_path = f"r2://{self.config.bucket}/bronze/cvr_raw_companies/{timestamp}/{batch_path_suffix}.parquet"

                raw_batch_patterns.append(raw_storage_path)

            if raw_batch_patterns:
                # Create consolidated raw data table
                self.conn.execute(f"""
                    CREATE TABLE {consolidated_table} AS
                    SELECT * FROM read_parquet({raw_batch_patterns})
                    ORDER BY cvr_number, batch_number
                """)

                raw_count = self.conn.execute(
                    f"SELECT COUNT(*) FROM {consolidated_table}"
                ).fetchone()[0]
                self.log.info(f"✅ Consolidated {raw_count:,} raw company records")

                # Save consolidated raw data
                import os

                if ga_batch is not None:
                    # Save part-specific consolidated file
                    raw_final_path = f"{self.config.bucket}/bronze/cvr_raw_companies/{timestamp}/part{ga_batch}/consolidated.parquet"
                    self.storage.upload_from_duckdb_table(
                        consolidated_table,
                        raw_final_path,
                        compression="zstd",
                        row_group_size=100000,
                    )
                    self.log.info(
                        f"✅ Part {ga_batch} consolidated data saved to: {raw_final_path}"
                    )

                    # Save locally for artifact sharing between workflow jobs
                    if os.getenv("GITHUB_ACTIONS") == "true":
                        local_path = f"/tmp/cvr_raw_data_part{ga_batch}.parquet"
                        self.conn.execute(
                            f"COPY {consolidated_table} TO '{local_path}' (FORMAT 'parquet', COMPRESSION 'zstd')"
                        )
                        self.log.info(f"💾 Saved part {ga_batch} data to artifact: {local_path}")
                else:
                    # Save to standard bronze location (single job mode)
                    raw_final_path = f"{self.config.bucket}/bronze/cvr_raw_companies/{timestamp}/consolidated.parquet"
                    self.storage.upload_from_duckdb_table(
                        consolidated_table,
                        raw_final_path,
                        compression="zstd",
                        row_group_size=100000,
                    )

                    # Also save locally for GitHub Actions artifact sharing
                    if os.getenv("GITHUB_ACTIONS") == "true":
                        local_path = "/tmp/cvr_raw_data.parquet"
                        self.conn.execute(
                            f"COPY {consolidated_table} TO '{local_path}' (FORMAT 'parquet', COMPRESSION 'zstd')"
                        )
                        self.log.info(f"💾 Saved consolidated raw data to artifact: {local_path}")

                    self.log.info("✅ Consolidated raw data saved - ready for parsing step")

            else:
                # Create empty table with schema
                self.conn.execute(f"""
                    CREATE TABLE {consolidated_table} (
                        cvr_number INTEGER,
                        raw_json VARCHAR,
                        fetch_timestamp VARCHAR,
                        batch_number INTEGER
                    )
                """)
                self.log.info("✅ Created empty raw data table")

            # Clean up batch files to save storage
            self._cleanup_raw_batch_files(total_batches, timestamp)

            return consolidated_table

        except Exception as e:
            self.log.error(f"❌ Failed to consolidate raw batch files: {e}")
            raise

    def _create_empty_companies_table(self, table_name: str) -> None:
        """Create empty companies table with proper schema."""
        self.conn.execute(f"""
            CREATE TABLE {table_name} (
                cvr_number INTEGER,
                company_uuid VARCHAR,
                company_name VARCHAR,
                company_type_description VARCHAR,
                status VARCHAR,
                founded_date VARCHAR,
                dissolution_date VARCHAR,
                advertisement_protection BOOLEAN,
                pnumber_count INTEGER,
                current_full_address VARCHAR,
                current_street_name VARCHAR,
                current_house_number VARCHAR,
                current_floor VARCHAR,
                current_door VARCHAR,
                current_postal_code INTEGER,
                current_city VARCHAR,
                current_municipality_code INTEGER,
                current_municipality_name VARCHAR,
                current_address_type VARCHAR,
                latitude DOUBLE,
                longitude DOUBLE,
                coordinate_quality VARCHAR,
                coordinate_source VARCHAR,
                dawa_enriched BOOLEAN,
                primary_industry_code VARCHAR,
                primary_industry_description VARCHAR,
                is_agricultural_company BOOLEAN,
                company_data_json VARCHAR,
                processing_timestamp VARCHAR
            )
        """)

    def _create_empty_persons_table(self, table_name: str) -> None:
        """Create empty persons table with proper schema."""
        self.conn.execute(f"""
            CREATE TABLE {table_name} (
                person_uuid VARCHAR,
                cvr_number INTEGER,
                company_uuid VARCHAR,
                leadership_idx INTEGER,
                relation_type VARCHAR,
                person_relation_type VARCHAR,
                person_data_json VARCHAR,
                processing_timestamp VARCHAR
            )
        """)

    def _create_empty_employment_table(self, table_name: str) -> None:
        """Create empty employment table with proper schema."""
        self.conn.execute(f"""
            CREATE TABLE {table_name} (
                employment_uuid VARCHAR,
                cvr_number INTEGER,
                company_uuid VARCHAR,
                employment_idx INTEGER,
                employee_count INTEGER,
                period_start VARCHAR,
                period_end VARCHAR,
                is_current BOOLEAN,
                employment_data_json VARCHAR,
                processing_timestamp VARCHAR
            )
        """)

    def _cleanup_raw_batch_files(self, total_batches: int, timestamp: str) -> None:
        """
        Clean up raw batch files to save storage space.

        Args:
            total_batches: Total number of batch files to clean up
            timestamp: Timestamp for the batch files
        """
        try:
            self.log.info("🧹 Cleaning up raw batch files to save storage space...")

            ga_batch = self.config.shared_config.batch_number
            deleted_count = 0

            for batch_idx in range(1, total_batches + 1):
                batch_path_suffix = f"raw_batch_{batch_idx:03d}_of_{total_batches:03d}"

                if ga_batch is not None:
                    # Running as part of split job - use part subdirectory
                    raw_batch_path = f"{self.config.bucket}/bronze/cvr_raw_companies/{timestamp}/part{ga_batch}/{batch_path_suffix}.parquet"
                else:
                    # Running as single job - use flat structure
                    raw_batch_path = f"{self.config.bucket}/bronze/cvr_raw_companies/{timestamp}/{batch_path_suffix}.parquet"

                try:
                    self.storage.delete_file(raw_batch_path)
                    deleted_count += 1
                except Exception:
                    pass  # Ignore deletion errors

            self.log.info(f"✅ Cleaned up {deleted_count} raw batch files")

        except Exception as e:
            self.log.warning(f"⚠️ Raw batch file cleanup failed (non-critical): {e}")

    @timed(name="Creating and saving ownership table")
    def _create_and_save_ownership_table(self) -> None:
        """
        Create ownership table from all company data and save to GCS.

        This method reads from the main companies table and extracts ownership
        data for companies that have formal ownership percentages registered.
        """
        self.log.info("Creating normalized ownership table from company data")

        # Get all company JSON data that contains ownership information
        companies_with_ownership = self.conn.execute("""
            SELECT company_data_json
            FROM cvr_companies
            WHERE json_array_length(json_extract(company_data_json, '$.ownership')) > 0
        """).fetchall()

        if not companies_with_ownership:
            self.log.info(
                "No companies with ownership data found - skipping ownership table creation"
            )
            return

        # Extract JSON strings for processing
        json_strings = [row[0] for row in companies_with_ownership]

        self.log.info(f"Found {len(companies_with_ownership)} companies with ownership data")

        # Create ownership table using the existing logic
        self._create_ownership_table(json_strings)

        self.log.info("Ownership table creation completed")

    @timed(name="Finalizing data and artifacts")
    def _finalize_data_and_artifacts(self, table_name: str, total_stats: dict[str, Any]) -> None:
        """
        Save final data to GCS and create GitHub Actions artifacts.

        Args:
            table_name: Name of the main companies table
            total_stats: Processing statistics
        """
        # Save main table to GCS
        self._save_data(
            data=table_name,
            dataset="cvr_enrichment_companies",
            bucket=self.config.bucket,
            stage="gold",
            filename="data.parquet",
        )

        # Save persons table to GCS
        self._save_data(
            data="cvr_persons",
            dataset="cvr_persons",
            bucket=self.config.bucket,
            stage="gold",
        )

        # Save employment table to GCS
        self._save_data(
            data="cvr_employment",
            dataset="cvr_employment",
            bucket=self.config.bucket,
            stage="gold",
        )

        # Create and save ownership table to GCS
        self._create_and_save_ownership_table()

        # Save locally for GitHub Actions artifact sharing
        import os

        if os.getenv("GITHUB_ACTIONS") == "true":
            self.log.info(
                "GitHub Actions detected - saving company data locally for artifact sharing"
            )
            local_path = "/tmp/cvr_company_data.parquet"
            self.conn.execute(f"COPY {table_name} TO '{local_path}' (FORMAT PARQUET)")
            self.log.info(f"Saved company data locally to {local_path}")

        # Save summary data
        self._save_summary_data(total_stats)

    def _append_persons_batch(self, json_strings: list[str]) -> None:
        """Append persons data from a batch to the persons table."""
        self.conn.execute(
            """
            INSERT INTO cvr_persons
            WITH leadership_flattened AS (
                SELECT
                    json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                    idx as leadership_idx
                FROM unnest($1) as t(json_data)
                CROSS JOIN generate_series(0::BIGINT,
                    CASE
                        WHEN json_array_length(json_extract(json_data, '$.leadership')) > 0
                        THEN (
                            json_array_length(json_extract(json_data, '$.leadership')) - 1
                        )::BIGINT
                        ELSE 0::BIGINT
                    END) as t(idx)
                WHERE json_extract(json_data, '$.leadership') IS NOT NULL
                AND json_array_length(json_extract(json_data, '$.leadership')) > 0
            ),
            persons_extracted AS (
                SELECT
                    lf.cvr_number,
                    json_extract(
                        t.json_data,
                        '$.leadership[' || lf.leadership_idx || '].person.unit_number'
                    )::BIGINT as unit_number,
                    json_extract(
                        t.json_data,
                        '$.leadership[' || lf.leadership_idx || '].person.person_type'
                    )::VARCHAR as person_type,
                    json_extract(
                        t.json_data,
                        '$.leadership[' || lf.leadership_idx || '].person.names[0].name'
                    )::VARCHAR as current_name,
                    json_extract(
                        t.json_data,
                        '$.leadership[' || lf.leadership_idx || '].person.addresses[0].city'
                    )::VARCHAR as current_city,
                    TRY_CAST(
                        json_extract(
                            t.json_data,
                            '$.leadership[' || lf.leadership_idx ||
                            '].person.addresses[0].postal_code'
                        ) AS INTEGER
                    ) as current_postal_code,
                    json_extract(
                        t.json_data,
                        '$.leadership[' || lf.leadership_idx ||
                        '].person.addresses[0].municipality_name'
                    )::VARCHAR as current_municipality,
                    json_extract(
                        t.json_data,
                        '$.leadership[' || lf.leadership_idx ||
                        '].organization.member_data[0].attributter[0].vaerdier[0].vaerdi'
                    )::VARCHAR as role,
                    json_extract(
                        t.json_data,
                        '$.leadership[' || lf.leadership_idx ||
                        '].organization.member_data[0].attributter[0].vaerdier[0].periode.gyldigFra'
                    )::VARCHAR as role_start_date,
                    json_extract(
                        t.json_data,
                        '$.leadership[' || lf.leadership_idx ||
                        '].organization.member_data[0].attributter[0].vaerdier[0].periode.gyldigTil'
                    )::VARCHAR as role_end_date,
                    json_extract(
                        t.json_data,
                        '$.leadership[' || lf.leadership_idx || '].is_current'
                    )::BOOLEAN as is_current_role,
                    NOW()::VARCHAR as processing_timestamp
                FROM leadership_flattened lf
                JOIN unnest($1) as t(json_data) ON (
                    json_extract(t.json_data, '$.cvr_number')::INTEGER = lf.cvr_number
                )
                WHERE json_extract(
                    t.json_data, '$.leadership[' || lf.leadership_idx || '].person.unit_number'
                ) IS NOT NULL
            )
            SELECT
                md5(unit_number::VARCHAR)::VARCHAR as person_uuid,
                unit_number,
                person_type,
                current_name,
                current_city,
                current_postal_code,
                current_municipality,
                company_uuid(cvr_number) as company_uuid,
                cvr_number,
                role,
                CASE
                    WHEN UPPER(TRIM(role, '"')) = 'DIREKTØR' THEN 'Direktør'
                    WHEN UPPER(TRIM(role, '"')) = 'ADM. DIR.' THEN 'Adm. Dir.'
                    WHEN UPPER(TRIM(role, '"')) = 'FORMAND' THEN 'Formand'
                    WHEN UPPER(TRIM(role, '"')) = 'NÆSTFORMAND' THEN 'Næstformand'
                    WHEN UPPER(TRIM(role, '"')) = 'BESTYRELSESMEDLEM' THEN 'Bestyrelsesmedlem'
                    WHEN UPPER(TRIM(role, '"')) = 'LEDER' THEN 'Leder'
                    WHEN UPPER(TRIM(role, '"')) = 'INTERESSENTER' THEN 'Interessenter'
                    WHEN UPPER(TRIM(role, '"')) = 'REEL EJER' THEN 'Reel Ejer'
                    WHEN UPPER(TRIM(role, '"')) = 'REVISION' THEN 'Revision'
                    WHEN UPPER(TRIM(role, '"')) = 'STIFTERE' THEN 'Stiftere'
                    WHEN UPPER(TRIM(role, '"')) = 'FORENINGSREPRÆSENTANT'
                        THEN 'Foreningsrepræsentant'
                    WHEN UPPER(TRIM(role, '"')) = 'LIKVIDATOR' THEN 'Likvidator'
                    ELSE TRIM(role, '"')
                END as role_formatted,
                role_start_date,
                role_end_date,
                COALESCE(is_current_role, true) as is_current_role,
                CASE
                    WHEN UPPER(TRIM(role, '"')) IN (
                        'DIREKTØR', 'ADM. DIR.', 'FORMAND', 'NÆSTFORMAND',
                        'BESTYRELSESMEDLEM', 'LEDER', 'INTERESSENTER'
                    ) THEN true
                    WHEN UPPER(TRIM(role, '"')) IN (
                        'REEL EJER', 'REVISION', 'STIFTERE', 'FORENINGSREPRÆSENTANT', 'LIKVIDATOR'
                    ) THEN false
                    ELSE NULL
                END as is_leadership,
                CASE
                    WHEN UPPER(TRIM(role, '"')) IN ('REEL EJER', 'INTERESSENTER') THEN true
                    WHEN UPPER(TRIM(role, '"')) IN (
                        'DIREKTØR', 'ADM. DIR.', 'FORMAND', 'NÆSTFORMAND', 'BESTYRELSESMEDLEM',
                        'REVISION', 'STIFTERE', 'FORENINGSREPRÆSENTANT', 'LIKVIDATOR', 'LEDER'
                    ) THEN false
                    ELSE NULL
                END as is_owner,
                processing_timestamp
            FROM persons_extracted
            WHERE unit_number IS NOT NULL
        """,
            [json_strings],
        )

    def _append_employment_batch(self, json_strings: list[str]) -> None:
        """Append employment data from a batch to the employment table."""
        self.conn.execute(
            """
            INSERT INTO cvr_employment
            WITH employment_flattened AS (
                -- Annual employment
                SELECT
                    json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                    idx as employment_idx,
                    'annual' as employment_type
                FROM unnest($1) as t(json_data)
                CROSS JOIN generate_series(0::BIGINT,
                    CASE
                        WHEN json_array_length(
                            json_extract(json_data, '$.employment_data.annual_employment')
                        ) > 0
                        THEN (
                            json_array_length(
                                json_extract(json_data, '$.employment_data.annual_employment')
                            ) - 1
                        )::BIGINT
                        ELSE 0::BIGINT
                    END) as t(idx)
                WHERE json_extract(json_data, '$.employment_data.annual_employment') IS NOT NULL
                AND json_array_length(
                    json_extract(json_data, '$.employment_data.annual_employment')
                ) > 0

                UNION ALL

                -- Quarterly employment
                SELECT
                    json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                    idx as employment_idx,
                    'quarterly' as employment_type
                FROM unnest($1) as t(json_data)
                CROSS JOIN generate_series(0::BIGINT,
                    CASE
                        WHEN json_array_length(
                            json_extract(json_data, '$.employment_data.quarterly_employment')
                        ) > 0
                        THEN (
                            json_array_length(
                                json_extract(json_data, '$.employment_data.quarterly_employment')
                            ) - 1
                        )::BIGINT
                        ELSE 0::BIGINT
                    END) as t(idx)
                WHERE json_extract(json_data, '$.employment_data.quarterly_employment') IS NOT NULL
                AND json_array_length(
                    json_extract(json_data, '$.employment_data.quarterly_employment')
                ) > 0

                UNION ALL

                -- Monthly employment
                SELECT
                    json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                    idx as employment_idx,
                    'monthly' as employment_type
                FROM unnest($1) as t(json_data)
                CROSS JOIN generate_series(0::BIGINT,
                    CASE
                        WHEN json_array_length(
                            json_extract(json_data, '$.employment_data.monthly_employment')
                        ) > 0
                        THEN (
                            json_array_length(
                                json_extract(json_data, '$.employment_data.monthly_employment')
                            ) - 1
                        )::BIGINT
                        ELSE 0::BIGINT
                    END) as t(idx)
                WHERE json_extract(json_data, '$.employment_data.monthly_employment') IS NOT NULL
                AND json_array_length(
                    json_extract(json_data, '$.employment_data.monthly_employment')
                ) > 0

                UNION ALL

                -- Replacement monthly employment
                SELECT
                    json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                    idx as employment_idx,
                    'replacement_monthly' as employment_type
                FROM unnest($1) as t(json_data)
                CROSS JOIN generate_series(0::BIGINT,
                    CASE
                        WHEN json_array_length(
                            json_extract(json_data,
                                '$.employment_data.replacement_monthly_employment')
                        ) > 0
                        THEN (
                            json_array_length(
                                json_extract(json_data,
                                '$.employment_data.replacement_monthly_employment')
                            ) - 1
                        )::BIGINT
                        ELSE 0::BIGINT
                    END) as t(idx)
                WHERE json_extract(json_data,
                    '$.employment_data.replacement_monthly_employment') IS NOT NULL
                AND json_array_length(
                    json_extract(json_data,
                        '$.employment_data.replacement_monthly_employment')
                ) > 0
            )
            SELECT
                md5(CONCAT(ef.cvr_number::VARCHAR, '_', ef.employment_type, '_',
                          COALESCE(
                            CASE ef.employment_type
                                WHEN 'annual' THEN json_extract(
                                    t.json_data,
                                    '$.employment_data.annual_employment[' ||
                                    ef.employment_idx || '].year'
                                )::VARCHAR
                                WHEN 'quarterly' THEN json_extract(
                                    t.json_data,
                                    '$.employment_data.quarterly_employment[' ||
                                    ef.employment_idx || '].year'
                                )::VARCHAR
                                WHEN 'monthly' THEN json_extract(
                                    t.json_data,
                                    '$.employment_data.monthly_employment[' ||
                                    ef.employment_idx || '].year'
                                )::VARCHAR
                                WHEN 'replacement_monthly' THEN json_extract(
                                    t.json_data,
                                    '$.employment_data.replacement_monthly_employment[' ||
                                    ef.employment_idx || '].year'
                                )::VARCHAR
                            END, ''), '_',
                          COALESCE(
                            CASE ef.employment_type
                                WHEN 'quarterly' THEN json_extract(
                            t.json_data,
                            '$.employment_data.quarterly_employment[' ||
                            ef.employment_idx || '].quarter'
                        )::VARCHAR
                                ELSE ''
                            END, ''), '_',
                          COALESCE(
                            CASE ef.employment_type
                                WHEN 'monthly' THEN json_extract(
                            t.json_data,
                            '$.employment_data.monthly_employment[' ||
                            ef.employment_idx || '].month'
                        )::VARCHAR
                                WHEN 'replacement_monthly' THEN json_extract(
                            t.json_data,
                                '$.employment_data.replacement_monthly_employment[' ||
                                    ef.employment_idx || '].month'
                        )::VARCHAR
                                ELSE ''
                            END, '')))::VARCHAR as employment_uuid,
                company_uuid(ef.cvr_number) as company_uuid,
                ef.cvr_number,
                TRY_CAST(
                    CASE ef.employment_type
                        WHEN 'annual' THEN json_extract(
                            t.json_data,
                            '$.employment_data.annual_employment[' ||
                                ef.employment_idx || '].year'
                        )
                        WHEN 'quarterly' THEN json_extract(
                            t.json_data,
                            '$.employment_data.quarterly_employment[' ||
                                ef.employment_idx || '].year'
                        )
                        WHEN 'monthly' THEN json_extract(
                            t.json_data,
                            '$.employment_data.monthly_employment[' ||
                                ef.employment_idx || '].year'
                        )
                        WHEN 'replacement_monthly' THEN json_extract(
                            t.json_data,
                            '$.employment_data.replacement_monthly_employment[' ||
                                ef.employment_idx || '].year'
                        )
                    END AS INTEGER) as year,
                TRY_CAST(
                    CASE ef.employment_type
                        WHEN 'quarterly' THEN json_extract(
                            t.json_data,
                            '$.employment_data.quarterly_employment[' ||
                            ef.employment_idx || '].quarter'
                        )
                        ELSE NULL
                    END AS INTEGER) as quarter,
                TRY_CAST(
                    CASE ef.employment_type
                        WHEN 'monthly' THEN json_extract(
                            t.json_data,
                            '$.employment_data.monthly_employment[' ||
                            ef.employment_idx || '].month'
                        )
                        WHEN 'replacement_monthly' THEN json_extract(
                            t.json_data,
                                '$.employment_data.replacement_monthly_employment[' ||
                                    ef.employment_idx || '].month'
                        )
                        ELSE NULL
                    END AS INTEGER) as month,
                TRY_CAST(
                    CASE ef.employment_type
                        WHEN 'annual' THEN json_extract(
                            t.json_data,
                            '$.employment_data.annual_employment[' ||
                                ef.employment_idx || '].total_employees'
                        )
                        WHEN 'quarterly' THEN json_extract(
                            t.json_data,
                            '$.employment_data.quarterly_employment[' ||
                                ef.employment_idx || '].total_employees'
                        )
                        WHEN 'monthly' THEN json_extract(
                            t.json_data,
                            '$.employment_data.monthly_employment[' ||
                                ef.employment_idx || '].total_employees'
                        )
                        WHEN 'replacement_monthly' THEN json_extract(
                            t.json_data,
                            '$.employment_data.replacement_monthly_employment[' ||
                                ef.employment_idx || '].total_employees'
                        )
                    END AS INTEGER) as total_employees,
                TRY_CAST(
                    CASE ef.employment_type
                        WHEN 'annual' THEN json_extract(
                            t.json_data,
                            '$.employment_data.annual_employment[' ||
                                ef.employment_idx || '].full_time_equivalent'
                        )
                        WHEN 'quarterly' THEN json_extract(
                            t.json_data,
                            '$.employment_data.quarterly_employment[' ||
                                ef.employment_idx || '].full_time_equivalent'
                        )
                        WHEN 'monthly' THEN json_extract(
                            t.json_data,
                            '$.employment_data.monthly_employment[' ||
                                ef.employment_idx || '].full_time_equivalent'
                        )
                        WHEN 'replacement_monthly' THEN json_extract(
                            t.json_data,
                            '$.employment_data.replacement_monthly_employment[' ||
                                ef.employment_idx || '].full_time_equivalent'
                        )
                    END AS DOUBLE) as full_time_equivalent,
                TRY_CAST(
                    CASE ef.employment_type
                        WHEN 'annual' THEN json_extract(
                            t.json_data,
                            '$.employment_data.annual_employment[' ||
                                ef.employment_idx || '].employees_including_owners'
                        )
                        ELSE NULL
                    END AS INTEGER) as employees_including_owners,
                CASE ef.employment_type
                    WHEN 'annual' THEN json_extract(
                        t.json_data,
                        '$.employment_data.annual_employment[' ||
                            ef.employment_idx || '].fte_interval_code'
                    )::VARCHAR
                    WHEN 'quarterly' THEN json_extract(
                        t.json_data,
                        '$.employment_data.quarterly_employment[' ||
                            ef.employment_idx || '].fte_interval_code'
                    )::VARCHAR
                    WHEN 'monthly' THEN json_extract(
                        t.json_data,
                        '$.employment_data.monthly_employment[' ||
                            ef.employment_idx || '].fte_interval_code'
                    )::VARCHAR
                    WHEN 'replacement_monthly' THEN json_extract(
                        t.json_data,
                        '$.employment_data.replacement_monthly_employment[' ||
                            ef.employment_idx || '].fte_interval_code'
                    )::VARCHAR
                END as fte_interval_code,
                CASE ef.employment_type
                    WHEN 'annual' THEN json_extract(
                        t.json_data,
                        '$.employment_data.annual_employment[' ||
                            ef.employment_idx || '].employees_interval_code'
                    )::VARCHAR
                    WHEN 'quarterly' THEN json_extract(
                        t.json_data,
                        '$.employment_data.quarterly_employment[' ||
                            ef.employment_idx || '].employees_interval_code'
                    )::VARCHAR
                    WHEN 'monthly' THEN json_extract(
                        t.json_data,
                        '$.employment_data.monthly_employment[' ||
                            ef.employment_idx || '].employees_interval_code'
                    )::VARCHAR
                    WHEN 'replacement_monthly' THEN json_extract(
                        t.json_data,
                        '$.employment_data.replacement_monthly_employment[' ||
                            ef.employment_idx || '].employees_interval_code'
                    )::VARCHAR
                END as employees_interval_code,
                CASE ef.employment_type
                    WHEN 'annual' THEN json_extract(
                        t.json_data,
                        '$.employment_data.annual_employment[' ||
                            ef.employment_idx || '].owners_interval_code'
                    )::VARCHAR
                    ELSE NULL
                END as owners_interval_code,
                CASE ef.employment_type
                    WHEN 'annual' THEN json_extract(
                        t.json_data,
                        '$.employment_data.annual_employment[' ||
                            ef.employment_idx || '].last_updated'
                    )::VARCHAR
                    WHEN 'quarterly' THEN json_extract(
                        t.json_data,
                        '$.employment_data.quarterly_employment[' ||
                            ef.employment_idx || '].last_updated'
                    )::VARCHAR
                    WHEN 'monthly' THEN json_extract(
                        t.json_data,
                        '$.employment_data.monthly_employment[' ||
                            ef.employment_idx || '].last_updated'
                    )::VARCHAR
                    WHEN 'replacement_monthly' THEN json_extract(
                        t.json_data,
                        '$.employment_data.replacement_monthly_employment[' ||
                            ef.employment_idx || '].last_updated'
                    )::VARCHAR
                END as last_updated,
                ef.employment_type,
                NOW()::VARCHAR as processing_timestamp
            FROM employment_flattened ef
            JOIN unnest($1) as t(json_data) ON
                json_extract(t.json_data, '$.cvr_number')::INTEGER = ef.cvr_number
        """,
            [json_strings],
        )

    def _load_batch_details(self) -> dict[str, Any]:
        """Load batch details from collection step."""
        batch_details_path = f"{self.config.bucket}/gold/cvr_enrichment_collection/{self.date_pattern}/batch_details.json"

        try:
            return self.storage.download_json(batch_details_path)
        except Exception as e:
            self.log.error(f"Failed to load batch details from {batch_details_path}: {e}")
            raise

    @timed(name="Fetching company data")
    async def _fetch_company_data(self, cvr_batch: list[str]) -> dict[str, Any]:
        """
        Fetch company data from CVR register.

        Args:
            cvr_batch: List of CVR numbers to fetch

        Returns:
            Dictionary containing fetched company data
        """
        self.log.info(f"Fetching company data for {len(cvr_batch)} CVR numbers")

        if not cvr_batch:
            self.log.warning("No CVR numbers to fetch")
            return {
                "results": {},
                "summary": {"total": 0, "successful": 0, "failed": 0},
                "fetch_timestamp": datetime.now().isoformat(),
            }

        # Fetch company data using CVR API client with batch optimization (no geocoding)
        company_results = self.cvr_api_client.fetch_multiple_companies(
            cvr_numbers=cvr_batch,
            fetch_all_fields=self.config.fetch_all_fields,
            enrich_with_geometry=False,  # Always False - geocoding handled in
            # Address Geocoding step
            batch_size=self.config.shared_config.api_batch_size,
        )

        self.log.info(
            f"Company data fetch completed: "
            f"{company_results['summary']['successful']} successful, "
            f"{company_results['summary']['failed']} failed"
        )

        return company_results

    @timed(name="Processing company data")
    def _process_company_data(self, company_data: dict[str, Any]) -> dict[str, Any]:
        """
        Process and structure company data.

        Args:
            company_data: Raw company data from CVR API

        Returns:
            Processed company data
        """
        self.log.info("Processing company data")

        company_results = company_data["results"]

        # Process each company's data
        processed_companies = []

        for company_info in company_results.values():
            if company_info:
                # Add processing metadata
                company_info["processing_timestamp"] = datetime.now().isoformat()
                company_info["pipeline_run_id"] = self.date_pattern
                company_info["processing_step"] = CVREnrichmentStep.COMPANY_FETCHING.value
                # company_info["batch_number"] = self.config.batch_number  # No batching

                # Extract P-numbers for subsequent P-number fetching step
                pnumbers = self.cvr_api_client.get_company_pnumbers(company_info)
                company_info["extracted_pnumbers"] = pnumbers
                company_info["pnumber_count"] = len(pnumbers)

                processed_companies.append(company_info)

        # Create summary
        summary = {
            "total_companies": len(company_results),
            "successful_companies": len(processed_companies),
            "failed_companies": len([c for c in company_results.values() if c is None]),
            "total_pnumbers_found": sum(c.get("pnumber_count", 0) for c in processed_companies),
            "companies_with_pnumbers": len(
                [c for c in processed_companies if c.get("pnumber_count", 0) > 0]
            ),
            # "batch_number": self.config.batch_number,  # No batching
            # "total_batches": self.config.total_batches,  # No batching
            "processing_timestamp": datetime.now().isoformat(),
            "api_summary": company_data["summary"],
        }

        processed_data = {
            "companies": processed_companies,
            "summary": summary,
        }

        self.log.info(
            f"Processed {summary['successful_companies']} companies "
            f"(found {summary['total_pnumbers_found']} P-numbers)"
        )

        return processed_data

    def _save_summary_data(self, total_stats: dict[str, Any]) -> None:
        """Save processing summary data."""
        summary_path = f"gold/{self.config.dataset}/{self.date_pattern}/company_summary.json"

        self.storage.upload_json(
            data=total_stats, storage_path=f"{self.config.bucket}/{summary_path}"
        )

        self.log.info(f"Saved processing summary to {summary_path}")

    # Legacy method - replaced by batched processing
    @timed(name="Saving company data")
    def _save_company_data_legacy(self, processed_data: dict[str, Any]) -> str:
        """
        Save processed company data to GCS.

        Args:
            processed_data: Processed company data

        Returns:
            Table name where data was saved
        """
        self.log.info("Saving company data")

        # Create table name (no batching)
        table_name = "cvr_companies"

        companies_data = processed_data["companies"]

        # Create DuckDB table
        self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")

        if companies_data:
            # Convert to JSON strings for DuckDB
            json_strings = [json.dumps(company) for company in companies_data]

            self.conn.execute(
                f"""
                CREATE TABLE {table_name} AS
                SELECT
                    json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                    company_uuid(json_extract(json_data, '$.cvr_number')::INTEGER) as company_uuid,
                    json_extract_string(json_data, '$.company_name') as company_name,
                    json_extract_string(json_data, '$.company_type_description')
                        as company_type_description,
                    json_extract_string(json_data, '$.status') as status,
                    json_extract_string(json_data, '$.founded_date') as founded_date,
                    json_extract_string(json_data, '$.dissolution_date') as dissolution_date,
                    json_extract(json_data, '$.advertisement_protection')::BOOLEAN
                    as advertisement_protection,
                    json_extract(json_data, '$.pnumber_count')::INTEGER as pnumber_count,
                    -- Extract primary address (algorithmically selected by
                    -- primary_address_selector.py)
                    -- Priority: 1) Current addresses,
                    -- 2) Beliggenhedsadresse > Postadresse > Kontaktadresse,
                    -- 3) Best coordinate quality
                    json_extract_string(json_data, '$.primary_address.full_address')
                    as current_full_address,
                    json_extract_string(json_data, '$.primary_address.street_name')
                    as current_street_name,
                    json_extract_string(json_data, '$.primary_address.house_number')
                    as current_house_number,
                    json_extract_string(json_data, '$.primary_address.floor') as current_floor,
                    json_extract_string(json_data, '$.primary_address.door') as current_door,
                    TRY_CAST(json_extract(json_data, '$.primary_address.postal_code') AS INTEGER)
                    as current_postal_code,
                    json_extract_string(json_data, '$.primary_address.city') as current_city,
                    TRY_CAST(json_extract(json_data,
                        '$.primary_address.municipality_code') AS INTEGER)
                    as current_municipality_code,
                    json_extract_string(json_data, '$.primary_address.municipality_name')
                    as current_municipality_name,
                    json_extract_string(json_data, '$.primary_address.address_type')
                    as current_address_type,
                    TRY_CAST(json_extract(json_data, '$.primary_address.latitude') AS DOUBLE)
                    as latitude,
                    TRY_CAST(json_extract(json_data, '$.primary_address.longitude') AS DOUBLE)
                    as longitude,
                    json_extract_string(json_data, '$.primary_address.coordinate_quality')
                    as coordinate_quality,
                    json_extract_string(json_data, '$.primary_address.coordinate_source')
                    as coordinate_source,
                    json_extract(json_data, '$.primary_address.dawa_enriched')::BOOLEAN
                    as dawa_enriched,
                    json_data as company_data_json,  -- Keep for pipeline dependencies (artifacts)
                    json_extract_string(json_data, '$.processing_timestamp')
                        as processing_timestamp
                FROM unnest($1) as t(json_data)
            """,
                [json_strings],
            )

            # Create normalized persons table from leadership data
            self._create_persons_table(json_strings)

            # Create normalized ownership table from ownership data
            self._create_ownership_table(json_strings)

            # Create normalized employment table from employment data
            self._create_employment_table(json_strings)

            self.log.info(f"Created table {table_name} with {len(companies_data)} companies")
        else:
            # Create empty table with schema matching our full SELECT
            self.conn.execute(f"""
                CREATE TABLE {table_name} (
                    cvr_number INTEGER,
                    company_uuid VARCHAR,
                    company_name VARCHAR,
                    company_type_description VARCHAR,
                    status VARCHAR,
                    founded_date VARCHAR,
                    dissolution_date VARCHAR,
                    advertisement_protection BOOLEAN,
                    pnumber_count INTEGER,
                    current_full_address VARCHAR,
                    current_street_name VARCHAR,
                    current_house_number VARCHAR,
                    current_floor VARCHAR,
                    current_door VARCHAR,
                    current_postal_code INTEGER,
                    current_city VARCHAR,
                    current_municipality_code INTEGER,
                    current_municipality_name VARCHAR,
                    current_address_type VARCHAR,
                    latitude DOUBLE,
                    longitude DOUBLE,
                    coordinate_quality VARCHAR,
                    coordinate_source VARCHAR,
                    dawa_enriched BOOLEAN,
                    company_data_json VARCHAR,
                    processing_timestamp VARCHAR
                )
            """)
            self.log.info(f"Created empty table {table_name}")

        # Save to GCS
        self._save_data(
            data=table_name,
            dataset="cvr_enrichment_companies",
            bucket=self.config.bucket,
            stage="gold",
            filename="data.parquet",
        )

        # Also save locally for GitHub Actions artifact sharing
        import os

        if os.getenv("GITHUB_ACTIONS") == "true":
            self.log.info(
                "GitHub Actions detected - saving company data locally for artifact sharing"
            )
            local_path = "/tmp/cvr_company_data.parquet"
            self.conn.execute(f"COPY {table_name} TO '{local_path}' (FORMAT PARQUET)")
            self.log.info(f"Saved company data locally to {local_path}")

        # Save summary data separately
        self._save_summary_data(processed_data["summary"])

        return table_name

    def _create_persons_table(self, json_strings: list[str]) -> None:
        """Create normalized persons table from leadership data."""

        self.log.info("Creating normalized persons table from leadership data")

        # Create persons table
        persons_table = "cvr_persons"
        self.conn.execute(f"DROP TABLE IF EXISTS {persons_table}")

        self.conn.execute(
            f"""
            CREATE TABLE {persons_table} AS
            WITH leadership_flattened AS (
                SELECT
                    json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                    idx as leadership_idx
                FROM unnest($1) as t(json_data)
                CROSS JOIN generate_series(0::BIGINT,
                    CASE
                        WHEN json_array_length(json_extract(json_data, '$.leadership')) > 0
                        THEN (
                            json_array_length(json_extract(json_data, '$.leadership')) - 1
                        )::BIGINT
                        ELSE 0::BIGINT
                    END) as t(idx)
                WHERE json_extract(json_data, '$.leadership') IS NOT NULL
                AND json_array_length(json_extract(json_data, '$.leadership')) > 0
            ),
            persons_extracted AS (
                SELECT
                    lf.cvr_number,
                    json_extract(
                        t.json_data,
                        '$.leadership[' || lf.leadership_idx || '].person.unit_number'
                    )::BIGINT as unit_number,
                    json_extract_string(
                        t.json_data,
                        '$.leadership[' || lf.leadership_idx || '].person.person_type'
                    ) as person_type,
                    -- Get current name (first name marked as current, or first name if none marked)
                    json_extract_string(
                        t.json_data,
                        '$.leadership[' || lf.leadership_idx || '].person.names[0].name'
                    ) as current_name,
                    -- Get current address (first address marked as current,
                    -- or first address if none marked)
                    json_extract_string(
                        t.json_data,
                        '$.leadership[' || lf.leadership_idx || '].person.addresses[0].city'
                    ) as current_city,
                    TRY_CAST(
                        json_extract(
                            t.json_data,
                            '$.leadership[' || lf.leadership_idx ||
                            '].person.addresses[0].postal_code'
                        ) AS INTEGER
                    ) as current_postal_code,
                    json_extract_string(
                        t.json_data,
                        '$.leadership[' || lf.leadership_idx ||
                        '].person.addresses[0].municipality_name'
                    ) as current_municipality,
                    -- Get role from organization
                    json_extract_string(
                        t.json_data,
                        '$.leadership[' || lf.leadership_idx ||
                        '].organization.member_data[0].attributter[0].vaerdier[0].vaerdi'
                    ) as role,
                    json_extract_string(
                        t.json_data,
                        '$.leadership[' || lf.leadership_idx ||
                        '].organization.member_data[0].attributter[0].vaerdier[0].periode.gyldigFra'
                    ) as role_start_date,
                    json_extract_string(
                        t.json_data,
                        '$.leadership[' || lf.leadership_idx ||
                        '].organization.member_data[0].attributter[0].vaerdier[0].periode.gyldigTil'
                    ) as role_end_date,
                    json_extract(
                        t.json_data,
                        '$.leadership[' || lf.leadership_idx || '].is_current'
                    )::BOOLEAN as is_current_role,
                    NOW()::VARCHAR as processing_timestamp
                FROM leadership_flattened lf
                JOIN unnest($1) as t(json_data) ON (
                    json_extract(t.json_data, '$.cvr_number')::INTEGER = lf.cvr_number
                )
                WHERE json_extract(
                    t.json_data, '$.leadership[' || lf.leadership_idx || '].person.unit_number'
                ) IS NOT NULL
            )
            SELECT
                -- Generate person UUID based on unit_number for consistency
                md5(unit_number::VARCHAR)::VARCHAR as person_uuid,
                unit_number,
                person_type,
                current_name,
                current_city,
                current_postal_code,
                current_municipality,
                -- Generate company UUID for consistency with other tables
                company_uuid(cvr_number) as company_uuid,
                cvr_number,
                role,
                -- Create a formatted role for display (simple title case for common roles)
                CASE
                    WHEN UPPER(role) = 'DIREKTØR' THEN 'Direktør'
                    WHEN UPPER(role) = 'ADM. DIR.' THEN 'Adm. Dir.'
                    WHEN UPPER(role) = 'FORMAND' THEN 'Formand'
                    WHEN UPPER(role) = 'NÆSTFORMAND' THEN 'Næstformand'
                    WHEN UPPER(role) = 'BESTYRELSESMEDLEM' THEN 'Bestyrelsesmedlem'
                    WHEN UPPER(role) = 'LEDER' THEN 'Leder'
                    WHEN UPPER(role) = 'INTERESSENTER' THEN 'Interessenter'
                    WHEN UPPER(role) = 'REEL EJER' THEN 'Reel Ejer'
                    WHEN UPPER(role) = 'REVISION' THEN 'Revision'
                    WHEN UPPER(role) = 'STIFTERE' THEN 'Stiftere'
                    WHEN UPPER(role) = 'FORENINGSREPRÆSENTANT'
                        THEN 'Foreningsrepræsentant'
                    WHEN UPPER(role) = 'LIKVIDATOR' THEN 'Likvidator'
                    ELSE role  -- Keep original for unknown/mixed content roles
                END as role_formatted,
                role_start_date,
                role_end_date,
                COALESCE(is_current_role, true) as is_current_role,
                -- Classify as leadership based on role (case-insensitive matching)
                CASE
                    WHEN UPPER(role) IN (
                        'DIREKTØR', 'ADM. DIR.', 'FORMAND', 'NÆSTFORMAND',
                        'BESTYRELSESMEDLEM', 'LEDER', 'INTERESSENTER'
                    ) THEN true
                    WHEN UPPER(role) IN (
                        'REEL EJER', 'REVISION', 'STIFTERE', 'FORENINGSREPRÆSENTANT', 'LIKVIDATOR'
                    ) THEN false
                    ELSE NULL
                END as is_leadership,
                -- Classify as owner based on role (case-insensitive matching)
                CASE
                    WHEN UPPER(role) IN ('REEL EJER', 'INTERESSENTER') THEN true
                    WHEN UPPER(role) IN (
                        'DIREKTØR', 'ADM. DIR.', 'FORMAND', 'NÆSTFORMAND', 'BESTYRELSESMEDLEM',
                        'REVISION', 'STIFTERE', 'FORENINGSREPRÆSENTANT', 'LIKVIDATOR', 'LEDER'
                    ) THEN false
                    ELSE NULL
                END as is_owner,
                processing_timestamp
            FROM persons_extracted
            WHERE unit_number IS NOT NULL
        """,
            [json_strings],
        )

        # Get count for logging
        person_count = self.conn.execute(f"SELECT COUNT(*) FROM {persons_table}").fetchone()[0]
        unique_persons = self.conn.execute(
            f"SELECT COUNT(DISTINCT unit_number) FROM {persons_table}"
        ).fetchone()[0]

        self.log.info(f"Created persons table with {person_count} person-company relationships")
        self.log.info(f"Representing {unique_persons} unique persons")

    def _create_ownership_table(self, json_strings: list[str]) -> None:
        """Create normalized ownership table from ownership data."""

        self.log.info("Creating normalized ownership table from ownership data")

        # Set up crypto extension for UUID generation
        try:
            self.conn.execute("INSTALL crypto FROM community")
            self.conn.execute("LOAD crypto")
        except Exception as e:
            self.log.warning(f"Crypto extension already loaded: {e}")

        # Get the namespace from environment variable
        namespace = os.getenv("LANDBRUGSDATA_UUID_NAMESPACE")
        if not namespace:
            raise ValueError("LANDBRUGSDATA_UUID_NAMESPACE environment variable is required")

        # Create ownership table
        ownership_table = "cvr_ownership"
        self.conn.execute(f"DROP TABLE IF EXISTS {ownership_table}")

        # Build the SQL with namespace interpolation
        ownership_sql = f"""
            CREATE TABLE {ownership_table} AS
            WITH ownership_flattened AS (
                SELECT
                    json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                    idx as ownership_idx
                FROM unnest($1) as t(json_data)
                CROSS JOIN generate_series(0::BIGINT,
                    CASE
                        WHEN json_array_length(json_extract(json_data, '$.ownership')) > 0
                        THEN (
                            json_array_length(json_extract(json_data, '$.ownership')) - 1
                        )::BIGINT
                        ELSE 0::BIGINT
                    END) as t(idx)
                WHERE json_extract(json_data, '$.ownership') IS NOT NULL
                AND json_array_length(json_extract(json_data, '$.ownership')) > 0
            ),
            ownership_extracted AS (
                SELECT
                    of.cvr_number,
                    json_extract_string(
                        t.json_data,
                        '$.ownership[' || of.ownership_idx || '].person_uuid'
                    ) as person_uuid,
                    json_extract(
                        t.json_data,
                        '$.ownership[' || of.ownership_idx || '].unit_number'
                    )::BIGINT as unit_number,
                    json_extract_string(
                        t.json_data,
                        '$.ownership[' || of.ownership_idx || '].owner_name'
                    ) as owner_name,
                    json_extract_string(
                        t.json_data,
                        '$.ownership[' || of.ownership_idx || '].owner_type'
                    ) as owner_type,
                    json_extract(
                        t.json_data,
                        '$.ownership[' || of.ownership_idx || '].ownership_percentage'
                    )::DOUBLE as ownership_percentage,
                    json_extract_string(
                        t.json_data,
                        '$.ownership[' || of.ownership_idx || '].period_start'
                    ) as period_start,
                    json_extract_string(
                        t.json_data,
                        '$.ownership[' || of.ownership_idx || '].period_end'
                    ) as period_end,
                    json_extract(
                        t.json_data,
                        '$.ownership[' || of.ownership_idx || '].is_current'
                    )::BOOLEAN as is_current,
                    NOW()::VARCHAR as processing_timestamp
                FROM ownership_flattened of
                JOIN unnest($1) as t(json_data) ON (
                    json_extract(t.json_data, '$.cvr_number')::INTEGER = of.cvr_number
                )
                WHERE json_extract(
                    t.json_data, '$.ownership[' || of.ownership_idx || '].owner_name'
                ) IS NOT NULL
            )
            SELECT
                -- Generate ownership UUID based on company + unit number + period for consistency
                md5(CONCAT(cvr_number::VARCHAR, '_',
                          COALESCE(unit_number::VARCHAR, 'unknown'), '_',
                          COALESCE(period_start, 'no_start')))::VARCHAR as ownership_uuid,
                -- Generate company UUID using the same method as LandbrugsdataUUID
                CASE
                    WHEN cvr_number IS NULL OR LENGTH(TRIM(CAST(cvr_number AS VARCHAR))) != 8
                         OR NOT REGEXP_MATCHES(TRIM(CAST(cvr_number AS VARCHAR)), '^[1-9][0-9]{7}$')
                    THEN NULL
                    ELSE CONCAT(
                        SUBSTR(crypto_hash('md5', CONCAT('{namespace}', 'company-cvr-',
                               TRIM(CAST(cvr_number AS VARCHAR)))), 1, 8), '-',
                        SUBSTR(crypto_hash('md5', CONCAT('{namespace}', 'company-cvr-',
                               TRIM(CAST(cvr_number AS VARCHAR)))), 9, 4), '-',
                        '5', SUBSTR(crypto_hash('md5', CONCAT('{namespace}', 'company-cvr-',
                                      TRIM(CAST(cvr_number AS VARCHAR)))), 13, 3), '-',
                        CONCAT('8', SUBSTR(crypto_hash('md5', CONCAT('{namespace}', 'company-cvr-',
                                               TRIM(CAST(cvr_number AS VARCHAR)))), 17, 3)), '-',
                        SUBSTR(crypto_hash('md5', CONCAT('{namespace}', 'company-cvr-',
                               TRIM(CAST(cvr_number AS VARCHAR)))), 21, 12)
                    )
                END as company_uuid,
                cvr_number,
                person_uuid,
                unit_number,
                owner_name,
                owner_type,
                ownership_percentage,
                period_start,
                period_end,
                COALESCE(is_current, true) as is_current,
                processing_timestamp
            FROM ownership_extracted
            WHERE owner_name IS NOT NULL
        """

        self.conn.execute(ownership_sql, [json_strings])

        # Get count for logging
        ownership_count = self.conn.execute(f"SELECT COUNT(*) FROM {ownership_table}").fetchone()[0]
        current_ownership_count = self.conn.execute(
            f"SELECT COUNT(*) FROM {ownership_table} WHERE is_current = true"
        ).fetchone()[0]

        self.log.info(f"Created ownership table with {ownership_count} ownership records")
        self.log.info(f"Including {current_ownership_count} current ownership relationships")

        # Save ownership table to GCS
        self._save_data(
            data=ownership_table,
            dataset="cvr_ownership",
            bucket=self.config.bucket,
            stage="gold",
        )

    def _create_employment_table(self, json_strings: list[str]) -> None:
        """Create normalized employment table from employment data."""
        self.log.info("Creating normalized employment table from employment data")

        # Create employment table
        employment_table = "cvr_employment"
        self.conn.execute(f"DROP TABLE IF EXISTS {employment_table}")

        self.conn.execute(
            f"""
            CREATE TABLE {employment_table} AS
            WITH employment_flattened AS (
                -- Annual employment
                SELECT
                    json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                    idx as employment_idx,
                    'annual' as employment_type
                FROM unnest($1) as t(json_data)
                CROSS JOIN generate_series(0::BIGINT,
                    CASE
                        WHEN json_array_length(
                            json_extract(json_data, '$.employment_data.annual_employment')
                        ) > 0
                        THEN (
                            json_array_length(
                                json_extract(json_data, '$.employment_data.annual_employment')
                            ) - 1
                        )::BIGINT
                        ELSE 0::BIGINT
                    END) as t(idx)
                WHERE json_extract(json_data, '$.employment_data.annual_employment') IS NOT NULL
                AND json_array_length(
                    json_extract(json_data, '$.employment_data.annual_employment')
                ) > 0

                UNION ALL

                -- Quarterly employment
                SELECT
                    json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                    idx as employment_idx,
                    'quarterly' as employment_type
                FROM unnest($1) as t(json_data)
                CROSS JOIN generate_series(0::BIGINT,
                    CASE
                        WHEN json_array_length(
                            json_extract(json_data, '$.employment_data.quarterly_employment')
                        ) > 0
                        THEN (
                            json_array_length(
                                json_extract(json_data, '$.employment_data.quarterly_employment')
                            ) - 1
                        )::BIGINT
                        ELSE 0::BIGINT
                    END) as t(idx)
                WHERE json_extract(json_data, '$.employment_data.quarterly_employment') IS NOT NULL
                AND json_array_length(
                    json_extract(json_data, '$.employment_data.quarterly_employment')
                ) > 0

                UNION ALL

                -- Monthly employment
                SELECT
                    json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                    idx as employment_idx,
                    'monthly' as employment_type
                FROM unnest($1) as t(json_data)
                CROSS JOIN generate_series(0::BIGINT,
                    CASE
                        WHEN json_array_length(
                            json_extract(json_data, '$.employment_data.monthly_employment')
                        ) > 0
                        THEN (
                            json_array_length(
                                json_extract(json_data, '$.employment_data.monthly_employment')
                            ) - 1
                        )::BIGINT
                        ELSE 0::BIGINT
                    END) as t(idx)
                WHERE json_extract(json_data, '$.employment_data.monthly_employment') IS NOT NULL
                AND json_array_length(
                    json_extract(json_data, '$.employment_data.monthly_employment')
                ) > 0

                UNION ALL

                -- Replacement monthly employment
                SELECT
                    json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                    idx as employment_idx,
                    'replacement_monthly' as employment_type
                FROM unnest($1) as t(json_data)
                CROSS JOIN generate_series(0::BIGINT,
                    CASE
                        WHEN json_array_length(
                            json_extract(json_data,
                                '$.employment_data.replacement_monthly_employment')
                        ) > 0
                        THEN (
                            json_array_length(
                                json_extract(json_data,
                                '$.employment_data.replacement_monthly_employment')
                            ) - 1
                        )::BIGINT
                        ELSE 0::BIGINT
                    END) as t(idx)
                WHERE json_extract(json_data,
                    '$.employment_data.replacement_monthly_employment') IS NOT NULL
                AND json_array_length(
                    json_extract(json_data,
                        '$.employment_data.replacement_monthly_employment')
                ) > 0
            )
            SELECT
                -- Generate employment UUID for each record
                md5(CONCAT(ef.cvr_number::VARCHAR, '_', ef.employment_type, '_',
                          COALESCE(
                            CASE ef.employment_type
                                WHEN 'annual' THEN json_extract(
                                    t.json_data,
                                    '$.employment_data.annual_employment[' ||
                                    ef.employment_idx || '].year'
                                )::VARCHAR
                                WHEN 'quarterly' THEN json_extract(
                                    t.json_data,
                                    '$.employment_data.quarterly_employment[' ||
                                    ef.employment_idx || '].year'
                                )::VARCHAR
                                WHEN 'monthly' THEN json_extract(
                                    t.json_data,
                                    '$.employment_data.monthly_employment[' ||
                                    ef.employment_idx || '].year'
                                )::VARCHAR
                                WHEN 'replacement_monthly' THEN json_extract(
                                    t.json_data,
                                    '$.employment_data.replacement_monthly_employment[' ||
                                    ef.employment_idx || '].year'
                                )::VARCHAR
                            END, ''), '_',
                          COALESCE(
                            CASE ef.employment_type
                                WHEN 'quarterly' THEN json_extract(
                            t.json_data,
                            '$.employment_data.quarterly_employment[' ||
                            ef.employment_idx || '].quarter'
                        )::VARCHAR
                                ELSE ''
                            END, ''), '_',
                          COALESCE(
                            CASE ef.employment_type
                                WHEN 'monthly' THEN json_extract(
                            t.json_data,
                            '$.employment_data.monthly_employment[' ||
                            ef.employment_idx || '].month'
                        )::VARCHAR
                                WHEN 'replacement_monthly' THEN json_extract(
                            t.json_data,
                                '$.employment_data.replacement_monthly_employment[' ||
                                    ef.employment_idx || '].month'
                        )::VARCHAR
                                ELSE ''
                            END, '')))::VARCHAR as employment_uuid,
                -- Generate company UUID for consistency with other tables
                company_uuid(ef.cvr_number) as company_uuid,
                ef.cvr_number,
                TRY_CAST(
                    CASE ef.employment_type
                        WHEN 'annual' THEN json_extract(
                            t.json_data,
                            '$.employment_data.annual_employment[' ||
                                ef.employment_idx || '].year'
                        )
                        WHEN 'quarterly' THEN json_extract(
                            t.json_data,
                            '$.employment_data.quarterly_employment[' ||
                                ef.employment_idx || '].year'
                        )
                        WHEN 'monthly' THEN json_extract(
                            t.json_data,
                            '$.employment_data.monthly_employment[' ||
                                ef.employment_idx || '].year'
                        )
                        WHEN 'replacement_monthly' THEN json_extract(
                            t.json_data,
                            '$.employment_data.replacement_monthly_employment[' ||
                                ef.employment_idx || '].year'
                        )
                    END AS INTEGER) as year,
                TRY_CAST(
                    CASE ef.employment_type
                        WHEN 'quarterly' THEN json_extract(
                            t.json_data,
                            '$.employment_data.quarterly_employment[' ||
                            ef.employment_idx || '].quarter'
                        )
                        ELSE NULL
                    END AS INTEGER) as quarter,
                TRY_CAST(
                    CASE ef.employment_type
                        WHEN 'monthly' THEN json_extract(
                            t.json_data,
                            '$.employment_data.monthly_employment[' ||
                            ef.employment_idx || '].month'
                        )
                        WHEN 'replacement_monthly' THEN json_extract(
                            t.json_data,
                                '$.employment_data.replacement_monthly_employment[' ||
                                    ef.employment_idx || '].month'
                        )
                        ELSE NULL
                    END AS INTEGER) as month,
                TRY_CAST(
                    CASE ef.employment_type
                        WHEN 'annual' THEN json_extract(
                            t.json_data,
                            '$.employment_data.annual_employment[' ||
                                ef.employment_idx || '].total_employees'
                        )
                        WHEN 'quarterly' THEN json_extract(
                            t.json_data,
                            '$.employment_data.quarterly_employment[' ||
                                ef.employment_idx || '].total_employees'
                        )
                        WHEN 'monthly' THEN json_extract(
                            t.json_data,
                            '$.employment_data.monthly_employment[' ||
                                ef.employment_idx || '].total_employees'
                        )
                        WHEN 'replacement_monthly' THEN json_extract(
                            t.json_data,
                            '$.employment_data.replacement_monthly_employment[' ||
                                ef.employment_idx || '].total_employees'
                        )
                    END AS INTEGER) as total_employees,
                TRY_CAST(
                    CASE ef.employment_type
                        WHEN 'annual' THEN json_extract(
                            t.json_data,
                            '$.employment_data.annual_employment[' ||
                                ef.employment_idx || '].full_time_equivalent'
                        )
                        WHEN 'quarterly' THEN json_extract(
                            t.json_data,
                            '$.employment_data.quarterly_employment[' ||
                                ef.employment_idx || '].full_time_equivalent'
                        )
                        WHEN 'monthly' THEN json_extract(
                            t.json_data,
                            '$.employment_data.monthly_employment[' ||
                                ef.employment_idx || '].full_time_equivalent'
                        )
                        WHEN 'replacement_monthly' THEN json_extract(
                            t.json_data,
                            '$.employment_data.replacement_monthly_employment[' ||
                                ef.employment_idx || '].full_time_equivalent'
                        )
                    END AS DOUBLE) as full_time_equivalent,
                TRY_CAST(
                    CASE ef.employment_type
                        WHEN 'annual' THEN json_extract(
                            t.json_data,
                            '$.employment_data.annual_employment[' ||
                                ef.employment_idx || '].employees_including_owners'
                        )
                        ELSE NULL
                    END AS INTEGER) as employees_including_owners,
                CASE ef.employment_type
                    WHEN 'annual' THEN json_extract(
                        t.json_data,
                        '$.employment_data.annual_employment[' ||
                            ef.employment_idx || '].fte_interval_code'
                    )::VARCHAR
                    WHEN 'quarterly' THEN json_extract(
                        t.json_data,
                        '$.employment_data.quarterly_employment[' ||
                            ef.employment_idx || '].fte_interval_code'
                    )::VARCHAR
                    WHEN 'monthly' THEN json_extract(
                        t.json_data,
                        '$.employment_data.monthly_employment[' ||
                            ef.employment_idx || '].fte_interval_code'
                    )::VARCHAR
                    WHEN 'replacement_monthly' THEN json_extract(
                        t.json_data,
                        '$.employment_data.replacement_monthly_employment[' ||
                            ef.employment_idx || '].fte_interval_code'
                    )::VARCHAR
                END as fte_interval_code,
                CASE ef.employment_type
                    WHEN 'annual' THEN json_extract(
                        t.json_data,
                        '$.employment_data.annual_employment[' ||
                            ef.employment_idx || '].employees_interval_code'
                    )::VARCHAR
                    WHEN 'quarterly' THEN json_extract(
                        t.json_data,
                        '$.employment_data.quarterly_employment[' ||
                            ef.employment_idx || '].employees_interval_code'
                    )::VARCHAR
                    WHEN 'monthly' THEN json_extract(
                        t.json_data,
                        '$.employment_data.monthly_employment[' ||
                            ef.employment_idx || '].employees_interval_code'
                    )::VARCHAR
                    WHEN 'replacement_monthly' THEN json_extract(
                        t.json_data,
                        '$.employment_data.replacement_monthly_employment[' ||
                            ef.employment_idx || '].employees_interval_code'
                    )::VARCHAR
                END as employees_interval_code,
                CASE ef.employment_type
                    WHEN 'annual' THEN json_extract(
                        t.json_data,
                        '$.employment_data.annual_employment[' ||
                            ef.employment_idx || '].owners_interval_code'
                    )::VARCHAR
                    ELSE NULL
                END as owners_interval_code,
                CASE ef.employment_type
                    WHEN 'annual' THEN json_extract(
                        t.json_data,
                        '$.employment_data.annual_employment[' ||
                            ef.employment_idx || '].last_updated'
                    )::VARCHAR
                    WHEN 'quarterly' THEN json_extract(
                        t.json_data,
                        '$.employment_data.quarterly_employment[' ||
                            ef.employment_idx || '].last_updated'
                    )::VARCHAR
                    WHEN 'monthly' THEN json_extract(
                        t.json_data,
                        '$.employment_data.monthly_employment[' ||
                            ef.employment_idx || '].last_updated'
                    )::VARCHAR
                    WHEN 'replacement_monthly' THEN json_extract(
                        t.json_data,
                        '$.employment_data.replacement_monthly_employment[' ||
                            ef.employment_idx || '].last_updated'
                    )::VARCHAR
                END as last_updated,
                ef.employment_type,
                NOW()::VARCHAR as processing_timestamp
            FROM employment_flattened ef
            JOIN unnest($1) as t(json_data) ON
                json_extract(t.json_data, '$.cvr_number')::INTEGER = ef.cvr_number
        """,
            [json_strings],
        )

        # Get count for logging
        employment_count = self.conn.execute(f"SELECT COUNT(*) FROM {employment_table}").fetchone()[
            0
        ]
        unique_companies = self.conn.execute(
            f"SELECT COUNT(DISTINCT cvr_number) FROM {employment_table}"
        ).fetchone()[0]

        # Get counts by type
        type_counts = self.conn.execute(f"""
            SELECT employment_type, COUNT(*) as count
            FROM {employment_table}
            GROUP BY employment_type
            ORDER BY count DESC
        """).fetchall()

        self.log.info(f"Created employment table with {employment_count} employment records")
        self.log.info(f"Covering {unique_companies} companies with employment data")

        for emp_type, count in type_counts:
            self.log.info(f"  {emp_type}: {count} records")

        # Save employment table to GCS
        self._save_data(
            data=employment_table,
            dataset="cvr_employment",
            bucket=self.config.bucket,
            stage="gold",
        )
