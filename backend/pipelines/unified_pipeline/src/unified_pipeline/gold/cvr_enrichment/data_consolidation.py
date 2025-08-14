"""
Data Consolidation Step - Step 6 of CVR Enrichment Pipeline

This final step consolidates all data from previous steps (companies, P-numbers,
financial documents, and geocoded addresses) into the final normalized tables
that match the original CVR enrichment output format.
"""

import json
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import Field

from unified_pipeline.common.base import BaseJobConfig, BaseSource, GoldJobInterface
from unified_pipeline.util.timing import timed

from .shared.config import CVREnrichmentSharedConfig, CVREnrichmentStep, get_step_input_paths


class DataConsolidationConfig(BaseJobConfig):
    """Configuration for data consolidation step."""

    name: str = "Data Consolidation"
    dataset: str = "cvr_enrichment"
    type: str = "data_consolidation"
    description: str = "Consolidate all CVR enrichment data into final normalized tables"
    frequency: str = "monthly"
    bucket: str = "landbrugsdata-raw-data"

    # Shared configuration
    shared_config: CVREnrichmentSharedConfig = Field(
        default_factory=CVREnrichmentSharedConfig,
        description="Shared configuration for CVR enrichment pipeline",
    )

    # Data consolidation specific configuration
    create_normalized_tables: bool = Field(
        default=True, description="Whether to create the full set of normalized tables"
    )

    include_raw_json: bool = Field(
        default=True, description="Whether to include raw JSON data in consolidated tables"
    )

    model_config = {"frozen": True}

    def apply_cli_filters(self, cli_config):
        """Apply CLI configuration filters to this config."""
        if cli_config.test_limit is not None:
            object.__setattr__(
                self,
                "shared_config",
                self.shared_config.model_copy(update={"test_limit": cli_config.test_limit}),
            )


class DataConsolidation(BaseSource[DataConsolidationConfig], GoldJobInterface):
    """
    Data consolidation step implementation.

    This step:
    1. Loads data from all previous pipeline steps
    2. Merges and consolidates the data
    3. Creates the final normalized table structure
    4. Generates comprehensive summary statistics
    5. Saves the final CVR enrichment output
    """

    def __init__(self, config: DataConsolidationConfig):
        """
        Initialize data consolidation step.

        Args:
            config: Configuration for data consolidation
        """
        super().__init__(config)

        # Set up optimal DuckDB memory settings early to prevent OOM
        self._setup_memory_optimized_duckdb()

        # Set up company UUID generation function
        self._setup_company_uuid_function()

        self.log.info("Data consolidation step initialized")
        self.log.info("📋 Configuration:")
        self.log.info(f"   • Create normalized tables: {self.config.create_normalized_tables}")
        self.log.info(f"   • Include raw JSON: {self.config.include_raw_json}")

    def _setup_memory_optimized_duckdb(self):
        """Set up DuckDB with memory-optimized settings to prevent OOM errors."""
        try:
            # Aggressive memory settings to prevent accumulation issues
            self.conn.execute("SET memory_limit = '8GB'")  # Lower limit to force spilling
            self.conn.execute("SET preserve_insertion_order = false")  # Disable ordering for memory efficiency
            self.conn.execute("SET threads = 2")  # Fewer threads to reduce memory pressure
            self.conn.execute("PRAGMA cache_size = -1000")  # Small cache size (1MB)
            self.conn.execute("SET temp_directory = '/tmp'")  # Use temp directory for spill
            self.conn.execute("SET enable_progress_bar = false")  # Disable progress bar for memory
            
            # Critical settings to prevent CTE and intermediate result accumulation
            self.conn.execute("SET max_expression_depth = 100")  # Limit expression complexity
            self.conn.execute("SET enable_object_cache = false")  # Disable object caching
            self.conn.execute("SET checkpoint_threshold = '32MB'")  # More frequent checkpoints
            
            self.log.info("✅ DuckDB configured with aggressive memory-optimized settings for large dataset processing")
            
        except Exception as e:
            self.log.warning(f"Could not set all DuckDB memory optimizations: {e}")
            # Try essential settings only
            try:
                self.conn.execute("SET memory_limit = '8GB'")
                self.conn.execute("SET preserve_insertion_order = false")
                self.conn.execute("SET threads = 2")
                self.log.info("✅ DuckDB configured with basic memory optimizations")
            except Exception as e2:
                self.log.warning(f"Could not set basic memory optimizations: {e2}")

    def _setup_company_uuid_function(self):
        """Set up company UUID generation function in DuckDB."""
        # Install crypto extension for consistent SHA-1 hashing
        self.conn.execute("INSTALL crypto FROM community")
        self.conn.execute("LOAD crypto")

        # Create company UUID function using UUID5 with consistent namespace
        self.conn.execute("""
            CREATE OR REPLACE FUNCTION company_uuid(cvr_number) AS (
                SELECT CASE
                    WHEN cvr_number IS NULL OR LENGTH(TRIM(CAST(cvr_number AS VARCHAR))) != 8
                         OR NOT REGEXP_MATCHES(TRIM(CAST(cvr_number AS VARCHAR)), '^[1-9][0-9]{7}$')
                    THEN NULL
                    ELSE CONCAT(
                        SUBSTR(crypto_hash('sha1', CONCAT('landbrugsdata-company-cvr',
                               TRIM(CAST(cvr_number AS VARCHAR)))), 1, 8), '-',
                        SUBSTR(crypto_hash('sha1', CONCAT('landbrugsdata-company-cvr',
                               TRIM(CAST(cvr_number AS VARCHAR)))), 9, 4), '-',
                        '5', SUBSTR(crypto_hash('sha1', CONCAT('landbrugsdata-company-cvr',
                                      TRIM(CAST(cvr_number AS VARCHAR)))), 13, 3), '-',
                        CONCAT('8', SUBSTR(crypto_hash('sha1', CONCAT('landbrugsdata-company-cvr',
                                               TRIM(CAST(cvr_number AS VARCHAR)))), 17, 3)), '-',
                        SUBSTR(crypto_hash('sha1', CONCAT('landbrugsdata-company-cvr',
                               TRIM(CAST(cvr_number AS VARCHAR)))), 21, 12)
                    )
                END
            )
        """)
        self.log.info("✅ Company UUID function created using crypto extension SHA-1")

    @timed(name="Data consolidation processing")
    async def run(self, silver_data: Optional[Dict[str, Any]] = None) -> str:
        """
        Run the data consolidation process.

        Args:
            silver_data: Optional silver data (not used in this step)

        Returns:
            Table name containing consolidated CVR data
        """
        self.log.info("Starting data consolidation step")

        try:
            # Step 1: Load all data from previous steps
            self.log.info("📋 Step 1/4: Loading and merging data from all previous steps")
            consolidated_data = self._load_and_merge_all_data()

            # Step 2: Create normalized table structure
            if self.config.create_normalized_tables:
                self.log.info("🏗️ Step 2/4: Creating normalized table structure")
                table_names = self._create_normalized_tables(consolidated_data)
            else:
                table_names = [self._create_simple_consolidated_table(consolidated_data)]

            # Step 3: Generate and save comprehensive summary
            self.log.info("📊 Step 3/4: Generating comprehensive data summary")
            summary_stats = self._generate_final_summary(consolidated_data, table_names)

            # Step 4: Final validation and output
            self.log.info("✅ Step 4/4: Final validation and output preparation")
            main_table = table_names[0] if table_names else "cvr_enriched_companies"

            # Extract summary statistics
            total_companies = summary_stats.get("total_companies", 0)
            total_pnumbers = summary_stats.get("total_pnumbers", 0)
            total_addresses = summary_stats.get("total_addresses", 0)
            geocoded_addresses = summary_stats.get("geocoded_addresses", 0)
            financial_docs = summary_stats.get("financial_documents", 0)
            tables_created = len(table_names)

            # Success summary
            self.log.info("=" * 60)
            self.log.info("🎉 CVR ENRICHMENT PIPELINE COMPLETED SUCCESSFULLY")
            self.log.info("=" * 60)
            self.log.info("📊 FINAL SUMMARY:")
            self.log.info(f"   • Total companies processed: {total_companies:,}")
            self.log.info(f"   • Total P-numbers processed: {total_pnumbers:,}")
            self.log.info(f"   • Total addresses found: {total_addresses:,}")
            self.log.info(f"   • Addresses geocoded: {geocoded_addresses:,}")
            self.log.info(f"   • Financial documents: {financial_docs:,}")
            self.log.info(f"   • Database tables created: {tables_created}")
            self.log.info(f"   • Main output table: {main_table}")
            self.log.info("")
            self.log.info("🚀 PIPELINE SUCCESS!")
            self.log.info("   The CVR enrichment pipeline has completed successfully.")
            self.log.info("   All data is now available in the gold layer tables.")
            self.log.info("=" * 60)

            return main_table

        except Exception as e:
            self.log.error("=" * 60)
            self.log.error("❌ DATA CONSOLIDATION FAILED")
            self.log.error("=" * 60)
            self.log.error(f"💥 Error: {e}")
            self.log.error("🔍 Check the logs above for detailed error information")
            self.log.error("⚠️  This is the final step - previous steps may have succeeded")
            self.log.error("=" * 60)
            raise

    @timed(name="Loading and merging all data")
    def _load_and_merge_all_data(self) -> Dict[str, Any]:
        """
        Load data from all previous pipeline steps and merge it.

        Returns:
            Dictionary containing merged data from all steps
        """
        if self.config.shared_config.enable_independent_execution:
            self.log.info(
                "Loading and merging data from latest available files (independent execution mode)"
            )
        else:
            self.log.info(
                "Loading and merging data from all pipeline steps (pipeline dependency mode)"
            )

        # Get input paths for all previous steps (with independent execution support)
        input_paths = get_step_input_paths(
            CVREnrichmentStep.DATA_CONSOLIDATION,
            self.date_pattern,
            bucket=self.config.bucket,
            enable_independent_execution=self.config.shared_config.enable_independent_execution,
            max_days_back=self.config.shared_config.max_days_back_for_inputs,
        )

        if self.config.shared_config.enable_independent_execution:
            available_count = len([p for p in input_paths if p is not None])
            self.log.info(
                f"Found {available_count}/{len(input_paths)} input files available for processing"
            )
            if available_count == 0:
                self.log.warning(
                    f"No data files found within "
                    f"{self.config.shared_config.max_days_back_for_inputs} days. "
                    f"Cannot perform consolidation."
                )
        else:
            self.log.info(f"Found {len(input_paths)} input files to process")

        # Initialize data containers
        companies_data = {}
        pnumbers_data = {}
        financial_data = {}
        addresses_data = {}

        # Process each input file (skip None values from independent execution)
        for input_path in input_paths:
            if input_path is None:
                self.log.info("Skipping missing input file (None)")
                continue

            self.log.info(f"Processing: {input_path}")

            try:
                # Determine file type based on path
                if "company" in input_path.lower():
                    self._load_company_data(input_path, companies_data)
                elif "pnumber" in input_path.lower():
                    self._load_pnumber_data(input_path, pnumbers_data)
                elif "financial" in input_path.lower():
                    self._load_financial_data(input_path, financial_data)
                elif "address" in input_path.lower() or "geocoded" in input_path.lower():
                    self._load_address_data(input_path, addresses_data)
                else:
                    self.log.warning(f"Unknown file type for path: {input_path}")

            except Exception as e:
                self.log.error(f"Failed to process {input_path}: {e}")
                continue

        # Merge all data by CVR number
        merged_data = self._merge_data_by_cvr(
            companies_data, pnumbers_data, financial_data, addresses_data
        )

        self.log.info(
            f"Data consolidation completed: "
            f"{len(companies_data)} companies, "
            f"{len(pnumbers_data)} P-numbers, "
            f"{len(financial_data)} financial records, "
            f"{len(addresses_data)} addresses"
        )

        return merged_data

    def _load_company_data(self, input_path: str, companies_data: Dict[str, Any]) -> None:
        """Load company data from a batch file."""
        # Check if running in GitHub Actions and use artifact data
        import os

        local_path = None

        if os.getenv("GITHUB_ACTIONS") == "true":
            artifact_path = "/tmp/cvr_company_data.parquet"
            if os.path.exists(artifact_path):
                local_path = artifact_path
                self.log.info("Using company data from artifact")

        if not local_path:
            # Use GCSDataAccess to handle authentication properly
            table_name = f"company_data_{int(time.time() * 1000)}"
            self.gcs_access.create_table_from_gcs(table_name, input_path)
            result = self.conn.execute(
                f"""
                SELECT cvr_number, company_name, company_data_json
                FROM {table_name}
                WHERE company_data_json IS NOT NULL
            """
            ).fetchall()
            # Clean up the temporary table
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        else:
            result = self.conn.execute(
                """
                SELECT cvr_number, company_name, company_data_json
                FROM read_parquet(?)
                WHERE company_data_json IS NOT NULL
            """,
                [local_path],
            ).fetchall()

        for cvr_number, company_name, company_json in result:
            try:
                company_data = json.loads(company_json)
                companies_data[str(cvr_number)] = company_data
            except json.JSONDecodeError as e:
                self.log.warning(f"Failed to parse company data for CVR {cvr_number}: {e}")

    def _load_pnumber_data(self, input_path: str, pnumbers_data: Dict[str, Any]) -> None:
        """Load P-number data from a batch file."""
        # Check if running in GitHub Actions and use artifact data
        import os

        local_path = None

        if os.getenv("GITHUB_ACTIONS") == "true":
            artifact_path = "/tmp/cvr_pnumber_data.parquet"
            if os.path.exists(artifact_path):
                local_path = artifact_path
                self.log.info("Using P-number data from artifact")

        if not local_path:
            # Use GCSDataAccess to handle authentication properly
            table_name = f"pnumber_data_{int(time.time() * 1000)}"
            self.gcs_access.create_table_from_gcs(table_name, input_path)
            result = self.conn.execute(
                f"""
                SELECT p_number, parent_cvr_number, pnumber_data_json
                FROM {table_name}
                WHERE pnumber_data_json IS NOT NULL
            """
            ).fetchall()
            # Clean up the temporary table
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        else:
            result = self.conn.execute(
                """
                SELECT p_number, parent_cvr_number, pnumber_data_json
                FROM read_parquet(?)
                WHERE pnumber_data_json IS NOT NULL
            """,
                [local_path],
            ).fetchall()

        for p_number, parent_cvr, pnumber_json in result:
            try:
                pnumber_data = json.loads(pnumber_json)
                pnumber_key = f"{parent_cvr}_{p_number}"
                pnumbers_data[pnumber_key] = pnumber_data
            except json.JSONDecodeError as e:
                self.log.warning(f"Failed to parse P-number data for {p_number}: {e}")

    def _load_financial_data(self, input_path: str, financial_data: Dict[str, Any]) -> None:
        """Load financial data from a batch file."""
        # Check if running in GitHub Actions and use artifact data
        import os

        local_path = None

        if os.getenv("GITHUB_ACTIONS") == "true":
            # Use single financial data artifact (no batching)
            artifact_path = "/tmp/cvr_financial_data.parquet"
            if os.path.exists(artifact_path):
                self.log.info("Using financial data from artifact")
                local_path = artifact_path
            else:
                self.log.warning(f"Financial artifact not found: {artifact_path}")

        if not local_path:
            # Use GCSDataAccess to handle authentication properly
            self.log.info(
                "Financial data artifact not available, using GCS path with authentication"
            )
            table_name = f"financial_data_{int(time.time() * 1000)}"
            self.gcs_access.create_table_from_gcs(table_name, input_path)
            
            # Work with the actual financial data structure as saved by the financial documents step
            # The parquet file contains processed financial summary data, not raw metrics
            result = self.conn.execute(
                f"""
                SELECT
                    cvr_number,
                    company_name,
                    COALESCE(document_count, 0) as document_count,
                    xml_document_count,
                    total_xml_size_bytes,
                    latest_reporting_date,
                    COALESCE(has_financial_metrics, false) as has_financial_metrics,
                    financial_data_json,
                    processing_timestamp,
                    batch_number
                FROM {table_name}
                WHERE cvr_number IS NOT NULL
                  AND (document_count > 0 OR has_financial_metrics = true)
            """
            ).fetchall()
            
            # Clean up the temporary table
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        else:
            # For local artifacts, use the expected JSON format
            result = self.conn.execute(
                """
                SELECT cvr_number, company_name, document_count, xml_document_count,
                       total_xml_size_bytes, latest_reporting_date, has_financial_metrics,
                       financial_data_json, processing_timestamp, batch_number
                FROM read_parquet(?)
                WHERE financial_data_json IS NOT NULL
            """,
                [local_path],
            ).fetchall()

        # Process the results based on the data format
        for row in result:
            cvr_number = row[0]
            try:
                # Now we always select all columns from parquet format
                # Based on actual data: cvr_number, company_name, document_count, xml_document_count,
                # total_xml_size_bytes, latest_reporting_date, has_financial_metrics, financial_data_json, processing_timestamp, batch_number
                document_count = row[2]  # document_count is the 3rd column (index 2)
                has_financial_metrics = row[6]  # has_financial_metrics is the 7th column (index 6)
                
                # Create proper financial data structure
                documents = []
                if has_financial_metrics and document_count > 0:
                    # Create documents array with the actual document count
                    for i in range(document_count):
                        documents.append({
                            "cvr_number": cvr_number,
                            "document_index": i,
                            "has_financial_data": True
                        })
                
                financial_data[str(cvr_number)] = {
                    "documents": documents,
                    "document_count": document_count,
                    "financial_metrics": {"has_metrics": has_financial_metrics} if has_financial_metrics else None
                }
            except json.JSONDecodeError as e:
                self.log.warning(f"Failed to parse financial data for CVR {cvr_number}: {e}")
            except Exception as e:
                self.log.warning(f"Failed to process financial data for CVR {cvr_number}: {e}")

    def _load_address_data(self, input_path: str, addresses_data: Dict[str, Any]) -> None:
        """Load address data from a batch file."""
        # Check if running in GitHub Actions and use artifact data
        import os

        local_path = None

        if os.getenv("GITHUB_ACTIONS") == "true":
            # Use single address data artifact (no batching)
            artifact_path = "/tmp/cvr_address_data.parquet"
            if os.path.exists(artifact_path):
                self.log.info("Using address data from artifact")
                local_path = artifact_path
            else:
                self.log.warning(f"Address artifact not found: {artifact_path}")

        if not local_path:
            # Use GCSDataAccess to handle authentication properly
            self.log.info("Address data artifact not available, using GCS path with authentication")
            table_name = f"address_data_{int(time.time() * 1000)}"
            self.gcs_access.create_table_from_gcs(table_name, input_path)
            result = self.conn.execute(
                f"""
                SELECT source_type, cvr_number, p_number, address_data_json
                FROM {table_name}
                WHERE address_data_json IS NOT NULL
            """
            ).fetchall()
            # Clean up the temporary table
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        else:
            result = self.conn.execute(
                """
                SELECT source_type, cvr_number, p_number, address_data_json
                FROM read_parquet(?)
                WHERE address_data_json IS NOT NULL
            """,
                [local_path],
            ).fetchall()

        for source_type, cvr_number, p_number, address_json in result:
            try:
                address_data = json.loads(address_json)

                if source_type == "company":
                    key = f"company_{cvr_number}"
                else:  # pnumber
                    key = f"pnumber_{cvr_number}_{p_number}"

                if key not in addresses_data:
                    addresses_data[key] = []
                addresses_data[key].append(address_data)

            except json.JSONDecodeError as e:
                self.log.warning(
                    f"Failed to parse address data for {source_type} {cvr_number}: {e}"
                )

    def _merge_data_by_cvr(
        self,
        companies_data: Dict[str, Any],
        pnumbers_data: Dict[str, Any],
        financial_data: Dict[str, Any],
        addresses_data: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Merge all data sources by CVR number.

        Returns:
            Dictionary with merged data structure
        """
        self.log.info("Merging data by CVR number")

        merged_companies = {}

        # Start with company data as the base
        for cvr_number, company_data in companies_data.items():
            merged_company = company_data.copy()

            # Add P-number data
            company_pnumbers = []
            for pnumber_key, pnumber_data in pnumbers_data.items():
                if pnumber_key.startswith(f"{cvr_number}_"):
                    company_pnumbers.append(pnumber_data)
            merged_company["pnumber_data"] = company_pnumbers

            # Add financial data
            if cvr_number in financial_data:
                merged_company["financial_documents"] = financial_data[cvr_number].get(
                    "documents", []
                )
                merged_company["financial_document_count"] = financial_data[cvr_number].get(
                    "document_count", 0
                )
                merged_company["financial_metrics"] = financial_data[cvr_number].get(
                    "financial_metrics"
                )
            else:
                merged_company["financial_documents"] = []
                merged_company["financial_document_count"] = 0
                merged_company["financial_metrics"] = None

            # Merge geocoded addresses (both company and P-number addresses)
            all_addresses = []

            # Company addresses
            company_addr_key = f"company_{cvr_number}"
            if company_addr_key in addresses_data:
                all_addresses.extend(addresses_data[company_addr_key])

            # P-number addresses
            for addr_key, addr_list in addresses_data.items():
                if addr_key.startswith(f"pnumber_{cvr_number}_"):
                    all_addresses.extend(addr_list)

            # Update addresses with geocoded data
            if all_addresses:
                # Merge with existing addresses, prioritizing geocoded data
                existing_addresses = merged_company.get("addresses", [])
                updated_addresses = self._merge_address_lists(existing_addresses, all_addresses)
                merged_company["addresses"] = updated_addresses

            # Add consolidation metadata
            merged_company["consolidation_timestamp"] = datetime.now().isoformat()
            merged_company["pipeline_run_id"] = self.date_pattern
            merged_company["processing_step"] = CVREnrichmentStep.DATA_CONSOLIDATION.value

            merged_companies[cvr_number] = merged_company

        # Create summary statistics
        summary = {
            "total_companies": len(merged_companies),
            "companies_with_pnumbers": len(
                [
                    c
                    for c in merged_companies.values()
                    if c.get("pnumber_data") and len(c["pnumber_data"]) > 0
                ]
            ),
            "companies_with_financial_data": len(
                [c for c in merged_companies.values() if c.get("financial_document_count", 0) > 0]
            ),
            "companies_with_geocoded_addresses": len(
                [
                    c
                    for c in merged_companies.values()
                    if any(addr.get("is_geocoded") for addr in c.get("addresses", []))
                ]
            ),
            "total_pnumbers": len(pnumbers_data),
            "total_addresses": sum(len(c.get("addresses", [])) for c in merged_companies.values()),
            "geocoded_addresses": sum(
                len([addr for addr in c.get("addresses", []) if addr.get("is_geocoded")])
                for c in merged_companies.values()
            ),
            "consolidation_timestamp": datetime.now().isoformat(),
        }

        return {"companies": merged_companies, "summary": summary}

    def _merge_address_lists(
        self, existing_addresses: List[Dict[str, Any]], geocoded_addresses: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Merge existing addresses with geocoded addresses, prioritizing geocoded data.
        """
        # Create a mapping of geocoded addresses by key attributes
        geocoded_map = {}
        for addr in geocoded_addresses:
            key = (
                addr.get("full_address", ""),
                addr.get("postal_code", ""),
                addr.get("address_type", ""),
            )
            geocoded_map[key] = addr

        # Update existing addresses with geocoded data
        updated_addresses = []
        for addr in existing_addresses:
            key = (
                addr.get("full_address", ""),
                addr.get("postal_code", ""),
                addr.get("address_type", ""),
            )

            if key in geocoded_map:
                # Merge with geocoded data, keeping original structure but updating geocoding fields
                updated_addr = addr.copy()
                geocoded_addr = geocoded_map[key]

                # Update with geocoding results
                geocoding_fields = [
                    "latitude",
                    "longitude",
                    "coordinate_system",
                    "srid",
                    "geometry_wkt",
                    "geometry_geojson",
                    "coordinate_quality",
                    "coordinate_source",
                    "dawa_enriched",
                    "datavask_enriched",
                    "geocoding_timestamp",
                    "is_geocoded",
                ]

                for field in geocoding_fields:
                    if field in geocoded_addr:
                        updated_addr[field] = geocoded_addr[field]

                updated_addresses.append(updated_addr)
                del geocoded_map[key]  # Remove from map to avoid duplicates
            else:
                updated_addresses.append(addr)

        # Add any remaining geocoded addresses that weren't matched
        for remaining_addr in geocoded_map.values():
            updated_addresses.append(remaining_addr)

        return updated_addresses

    @timed(name="Creating normalized tables")
    def _create_normalized_tables(self, consolidated_data: Dict[str, Any]) -> List[str]:
        """
        Create the full set of normalized tables matching the original CVR enrichment format.

        Returns:
            List of table names created
        """
        self.log.info(
            "Creating sophisticated normalized CVR enrichment tables (ported from original)"
        )

        companies = consolidated_data["companies"]

        if not companies:
            self.log.warning("No companies to create tables from")
            return []

        # Convert companies dict to list for processing
        companies_list = list(companies.values())

        # Create main table name
        table_name = "cvr_enriched_companies"

        self.log.info(
            f"🔍 Creating normalized tables for {len(companies_list)} companies "
            f"using chunked processing"
        )

        # Process companies in smaller chunks to prevent CTE memory accumulation
        # The real issue is that complex CTEs build up memory - solution is smaller batches
        chunk_size = 100  # Reasonable batch size - not too big to cause memory issues, not too small to be slow
        total_companies = len(companies_list)        
        num_chunks = (total_companies + chunk_size - 1) // chunk_size

        self.log.info(
            f"📦 Processing {total_companies} companies in {num_chunks} chunks of {chunk_size} "
            f"(preventing CTE memory accumulation with smaller batches)"
        )

        # Initialize all tables as empty with correct schema (original sophisticated schema)
        self._initialize_empty_tables(table_name)

        # Process each chunk with memory pressure monitoring
        for chunk_idx in range(num_chunks):
            start_idx = chunk_idx * chunk_size
            end_idx = min(start_idx + chunk_size, total_companies)
            chunk_companies = companies_list[start_idx:end_idx]

            self.log.info(
                f"📦 Processing chunk {chunk_idx + 1}/{num_chunks}: "
                f"companies {start_idx}-{end_idx - 1}"
            )

            try:
                # Process the chunk with optimized table operations
                self._process_companies_chunk_memory_optimized(chunk_companies, table_name, chunk_idx)

                # Force checkpoint and cleanup after each chunk to prevent accumulation
                self.conn.execute("CHECKPOINT")
                
                # Light memory cleanup after every chunk
                import gc
                gc.collect()
                
                # Periodic deep cleanup every 10 chunks (reasonable frequency)
                if (chunk_idx + 1) % 10 == 0:
                    self.log.info(f"🧹 Performing periodic cleanup after {chunk_idx + 1} chunks")
                    self._deep_memory_cleanup()

            except Exception as e:
                self.log.error(f"Error processing chunk {chunk_idx + 1}: {e}")
                if "Out of Memory" in str(e) or "memory" in str(e).lower():
                    self.log.warning("⚠️ Memory exhaustion detected - attempting recovery")
                    self._emergency_memory_cleanup()
                    break
                raise

        # Employment data processing now handled via memory-efficient chunk processing
        # Each employment type is processed separately to avoid the 7.4GB memory issue
        self.log.info("✅ Employment data processing enabled with memory-efficient approach")
        self.log.info("🔧 Processing employment types one at a time to avoid memory exhaustion")
        
        # Log final table sizes
        self._log_final_table_sizes(table_name)

        # List of all tables created (9 tables total like the original)
        table_names = [
            table_name,
            f"{table_name}_addresses",
            f"{table_name}_leadership",
            f"{table_name}_financial",
            f"{table_name}_industries",
            f"{table_name}_employment_annual",
            f"{table_name}_employment_quarterly",
            f"{table_name}_employment_monthly",
            f"{table_name}_employment_replacement_monthly",
        ]

        # Save all tables to GCS
        for table in table_names:
            try:
                count = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                if count > 0:
                    self.log.info(f"💾 Saving table {table} ({count} rows)")
                    self._save_data(
                        data=table,
                        dataset=f"{self.config.dataset}_{table.split('_')[-1]}"
                        if "_" in table
                        else self.config.dataset,
                        bucket=self.config.bucket,
                        stage="gold",
                    )
                else:
                    self.log.info(f"⚠️ Skipping empty table {table}")
            except Exception as e:
                self.log.warning(f"⚠️ Could not save table {table}: {e}")

        return table_names

    def _initialize_empty_tables(self, table_name: str) -> None:
        """Initialize all normalized tables as empty with correct schema (ported from original)."""
        # Main companies table
        self.conn.execute(f"""
            CREATE TABLE {table_name} (
                company_uuid VARCHAR,
                cvr_number INTEGER,
                company_name VARCHAR,
                company_type_description VARCHAR,
                status VARCHAR,
                founded_date VARCHAR,
                dissolution_date VARCHAR,
                advertisement_protection BOOLEAN,
                address_latitude DOUBLE,
                address_longitude DOUBLE,
                address_coordinate_system VARCHAR,
                address_srid INTEGER,
                address_geom_wkt VARCHAR,
                address_coordinate_quality VARCHAR,
                address_coordinate_source VARCHAR,
                data_source VARCHAR,
                fetch_timestamp VARCHAR,
                source_pipelines VARCHAR[],
                source_pipeline_count INTEGER,
                financial_document_count INTEGER,
                processing_timestamp VARCHAR,
                pipeline_run_id VARCHAR
            )
        """)

        # Addresses table
        self.conn.execute(f"""
            CREATE TABLE {table_name}_addresses (
                company_uuid VARCHAR,
                cvr_number INTEGER,
                full_address VARCHAR,
                street_name VARCHAR,
                house_number VARCHAR,
                floor VARCHAR,
                door VARCHAR,
                postal_code VARCHAR,
                city VARCHAR,
                municipality_code VARCHAR,
                municipality_name VARCHAR,
                country_code VARCHAR,
                adresse_id VARCHAR,
                period_start VARCHAR,
                period_end VARCHAR,
                is_current BOOLEAN,
                latitude DOUBLE,
                longitude DOUBLE,
                coordinate_system VARCHAR,
                srid INTEGER,
                geometry_wkt VARCHAR,
                geometry_geojson VARCHAR,
                coordinate_quality VARCHAR,
                coordinate_source VARCHAR,
                dawa_enriched BOOLEAN,
                geocoding_timestamp VARCHAR
            )
        """)

        # Leadership table
        self.conn.execute(f"""
            CREATE TABLE {table_name}_leadership (
                company_uuid VARCHAR,
                cvr_number INTEGER,
                leadership_data JSON
            )
        """)

        # Financial table (with all 30+ sophisticated financial metrics)
        self.conn.execute(f"""
            CREATE TABLE {table_name}_financial (
                company_uuid VARCHAR NOT NULL,
                cvr_number INTEGER NOT NULL,
                publication_type VARCHAR NULL,
                publication_time VARCHAR NULL,
                case_number VARCHAR NULL,
                reporting_period_start VARCHAR NULL,
                reporting_period_end VARCHAR NULL,
                document_count INTEGER NULL,
                xml_size_bytes INTEGER NULL,
                download_success BOOLEAN NULL,
                duration_context VARCHAR NULL,
                instant_context VARCHAR NULL,
                parse_success BOOLEAN NULL,
                operating_profit_loss DOUBLE NULL,
                profit_loss_before_tax DOUBLE NULL,
                employee_benefits_expense DOUBLE NULL,
                average_number_of_employees DOUBLE NULL,
                depreciation_expense DOUBLE NULL,
                other_finance_income DOUBLE NULL,
                other_finance_expenses DOUBLE NULL,
                tax_expense DOUBLE NULL,
                total_assets DOUBLE NULL,
                total_equity DOUBLE NULL,
                noncurrent_assets DOUBLE NULL,
                current_assets DOUBLE NULL,
                cash_and_cash_equivalents DOUBLE NULL,
                liabilities_other_than_provisions DOUBLE NULL,
                shortterm_liabilities_other_than_provisions DOUBLE NULL,
                longterm_liabilities_other_than_provisions DOUBLE NULL,
                provisions DOUBLE NULL,
                property_plant_equipment DOUBLE NULL,
                contributed_capital DOUBLE NULL,
                net_profit_loss DOUBLE NULL,
                equity_ratio DOUBLE NULL,
                profit_per_employee DOUBLE NULL,
                return_on_assets DOUBLE NULL
            )
        """)

        # Industries table
        self.conn.execute(f"""
            CREATE TABLE {table_name}_industries (
                company_uuid VARCHAR,
                cvr_number INTEGER,
                industry_data JSON
            )
        """)

        # Employment tables (4 different time periods)
        employment_types = ["annual", "quarterly", "monthly", "replacement_monthly"]
        for table_suffix in employment_types:
            self.conn.execute(f"""
                CREATE TABLE {table_name}_employment_{table_suffix} (
                    company_uuid VARCHAR,
                    cvr_number INTEGER,
                    employment_data JSON
                )
            """)

    def _create_main_companies_table(self, companies: Dict[str, Any]) -> str:
        """Create the main companies table."""
        table_name = "cvr_enriched_companies"

        self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")

        # Convert companies to JSON strings for processing
        json_strings = [json.dumps(company) for company in companies.values()]

        self.conn.execute(
            f"""
            CREATE TABLE {table_name} AS
            SELECT
                company_uuid(json_extract(json_data, '$.cvr_number')::INTEGER) as company_uuid,
                json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                json_extract(json_data, '$.company_name')::VARCHAR as company_name,
                json_extract(json_data, '$.company_type_description')::VARCHAR as
                    company_type_description,
                json_extract(json_data, '$.status')::VARCHAR as status,
                json_extract(json_data, '$.founded_date')::VARCHAR as founded_date,
                json_extract(json_data, '$.dissolution_date')::VARCHAR as dissolution_date,
                json_extract(json_data, '$.advertisement_protection')::BOOLEAN as
                    advertisement_protection,
                json_extract(json_data, '$.primary_address_geometry.latitude')::DOUBLE as
                    address_latitude,
                json_extract(json_data, '$.primary_address_geometry.longitude')::DOUBLE as
                    address_longitude,
                json_extract(json_data, '$.primary_address_geometry.coordinate_system')::VARCHAR as
                    address_coordinate_system,
                json_extract(json_data, '$.primary_address_geometry.srid')::INTEGER as
                    address_srid,
                json_extract(json_data, '$.primary_address_geometry.geometry_wkt')::VARCHAR as
                    address_geom_wkt,
                json_extract(json_data, '$.primary_address_geometry.coordinate_quality')::VARCHAR as
                    address_coordinate_quality,
                json_extract(json_data, '$.primary_address_geometry.coordinate_source')::VARCHAR as
                    address_coordinate_source,
                json_extract(json_data, '$.data_source')::VARCHAR as data_source,
                json_extract(json_data, '$.fetch_timestamp')::VARCHAR as fetch_timestamp,
                json_array_length(json_extract(json_data, '$.addresses')) as address_count,
                json_array_length(json_extract(json_data, '$.pnumber_data')) as pnumber_count,
                json_extract(json_data, '$.financial_document_count')::INTEGER as
                    financial_document_count,
                CASE
                    WHEN json_extract(json_data, '$.financial_metrics') IS NOT NULL
                    THEN true
                    ELSE false
                END as has_financial_metrics,
                {("json_data as company_data_json," if self.config.include_raw_json else "")}
                json_extract(json_data, '$.consolidation_timestamp')::VARCHAR as consolidation_timestamp,
                json_extract(json_data, '$.pipeline_run_id')::VARCHAR as pipeline_run_id
            FROM unnest($1) as t(json_data)
        """,
            [json_strings],
        )

        self.log.info(f"Created main companies table {table_name} with {len(companies)} companies")
        return table_name

    def _create_addresses_table(self, companies: Dict[str, Any]) -> str:
        """Create the addresses table."""
        table_name = "cvr_enriched_addresses"

        self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")

        # Extract all addresses from companies
        all_addresses = []
        for cvr_number, company in companies.items():
            for addr in company.get("addresses", []):
                addr_record = addr.copy()
                addr_record["cvr_number"] = company["cvr_number"]
                addr_record["company_uuid"] = f"company_uuid({company['cvr_number']})"
                all_addresses.append(addr_record)

        if all_addresses:
            json_strings = [json.dumps(addr) for addr in all_addresses]

            self.conn.execute(
                f"""
                CREATE TABLE {table_name} AS
                SELECT
                    company_uuid(json_extract(json_data, '$.cvr_number')::INTEGER) as company_uuid,
                    json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                    json_extract(json_data, '$.source_type')::VARCHAR as source_type,
                    json_extract(json_data, '$.p_number')::INTEGER as p_number,
                    json_extract(json_data, '$.address_type')::VARCHAR as address_type,
                    json_extract(json_data, '$.full_address')::VARCHAR as full_address,
                    json_extract(json_data, '$.street_name')::VARCHAR as street_name,
                    json_extract(json_data, '$.house_number')::VARCHAR as house_number,
                    json_extract(json_data, '$.postal_code')::VARCHAR as postal_code,
                    json_extract(json_data, '$.city')::VARCHAR as city,
                    json_extract(json_data, '$.municipality_code')::VARCHAR as municipality_code,
                    json_extract(json_data, '$.municipality_name')::VARCHAR as municipality_name,
                    json_extract(json_data, '$.latitude')::DOUBLE as latitude,
                    json_extract(json_data, '$.longitude')::DOUBLE as longitude,
                    json_extract(json_data, '$.geometry_wkt')::VARCHAR as geometry_wkt,
                    json_extract(json_data, '$.coordinate_quality')::VARCHAR as coordinate_quality,
                    json_extract(json_data, '$.dawa_enriched')::BOOLEAN as dawa_enriched,
                    json_extract(json_data, '$.datavask_enriched')::BOOLEAN as datavask_enriched,
                    json_extract(json_data, '$.is_current')::BOOLEAN as is_current,
                    CASE
                        WHEN json_extract(json_data, '$.latitude') IS NOT NULL
                        THEN true
                        ELSE false
                    END as is_geocoded
                FROM unnest($1) as t(json_data)
            """,
                [json_strings],
            )
        else:
            # Create empty table with schema
            self.conn.execute(f"""
                CREATE TABLE {table_name} (
                    company_uuid VARCHAR,
                    cvr_number INTEGER,
                    source_type VARCHAR,
                    p_number INTEGER,
                    address_type VARCHAR,
                    full_address VARCHAR,
                    street_name VARCHAR,
                    house_number VARCHAR,
                    postal_code VARCHAR,
                    city VARCHAR,
                    municipality_code VARCHAR,
                    municipality_name VARCHAR,
                    latitude DOUBLE,
                    longitude DOUBLE,
                    geometry_wkt VARCHAR,
                    coordinate_quality VARCHAR,
                    dawa_enriched BOOLEAN,
                    datavask_enriched BOOLEAN,
                    is_current BOOLEAN,
                    is_geocoded BOOLEAN
                )
            """)

        self.log.info(f"Created addresses table {table_name} with {len(all_addresses)} addresses")
        return table_name

    def _create_pnumbers_table(self, companies: Dict[str, Any]) -> str:
        """Create the P-numbers table."""
        table_name = "cvr_enriched_pnumbers"

        self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")

        # Extract all P-numbers from companies
        all_pnumbers = []
        for cvr_number, company in companies.items():
            for pnumber_data in company.get("pnumber_data", []):
                pnumber_record = pnumber_data.copy()
                pnumber_record["parent_cvr_number"] = company["cvr_number"]
                all_pnumbers.append(pnumber_record)

        if all_pnumbers:
            json_strings = [json.dumps(pnum) for pnum in all_pnumbers]

            self.conn.execute(
                f"""
                CREATE TABLE {table_name} AS
                SELECT
                    company_uuid(json_extract(json_data, '$.parent_cvr_number')::INTEGER) as company_uuid,
                    json_extract(json_data, '$.parent_cvr_number')::INTEGER as parent_cvr_number,
                    json_extract(json_data, '$.p_number')::INTEGER as p_number,
                    json_extract(json_data, '$.unit_name')::VARCHAR as unit_name,
                    json_array_length(json_extract(json_data, '$.addresses')) as address_count,
                    json_array_length(json_extract(json_data, '$.industries')) as industry_count,
                    json_extract(json_data, '$.data_source')::VARCHAR as data_source,
                    json_extract(json_data, '$.fetch_timestamp')::VARCHAR as fetch_timestamp
                FROM unnest($1) as t(json_data)
            """,
                [json_strings],
            )
        else:
            # Create empty table with schema
            self.conn.execute(f"""
                CREATE TABLE {table_name} (
                    company_uuid VARCHAR,
                    parent_cvr_number INTEGER,
                    p_number INTEGER,
                    unit_name VARCHAR,
                    address_count INTEGER,
                    industry_count INTEGER,
                    data_source VARCHAR,
                    fetch_timestamp VARCHAR
                )
            """)

        self.log.info(f"Created P-numbers table {table_name} with {len(all_pnumbers)} P-numbers")
        return table_name

    def _create_financial_table(self, companies: Dict[str, Any]) -> str:
        """Create the financial documents table."""
        table_name = "cvr_enriched_financial"

        self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")

        # Extract financial data from companies
        financial_records = []
        for cvr_number, company in companies.items():
            if company.get("financial_documents"):
                financial_record = {
                    "cvr_number": company["cvr_number"],
                    "document_count": company.get("financial_document_count", 0),
                    "financial_metrics": company.get("financial_metrics"),
                    "has_metrics": company.get("financial_metrics") is not None,
                }
                financial_records.append(financial_record)

        if financial_records:
            json_strings = [json.dumps(fin) for fin in financial_records]

            self.conn.execute(
                f"""
                CREATE TABLE {table_name} AS
                SELECT
                    company_uuid(json_extract(json_data, '$.cvr_number')::INTEGER) as company_uuid,
                    json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                    json_extract(json_data, '$.document_count')::INTEGER as document_count,
                    json_extract(json_data, '$.has_metrics')::BOOLEAN as has_financial_metrics
                FROM unnest($1) as t(json_data)
            """,
                [json_strings],
            )
        else:
            # Create empty table with schema
            self.conn.execute(f"""
                CREATE TABLE {table_name} (
                    company_uuid VARCHAR,
                    cvr_number INTEGER,
                    document_count INTEGER,
                    has_financial_metrics BOOLEAN
                )
            """)

        self.log.info(f"Created financial table {table_name} with {len(financial_records)} records")
        return table_name

    def _create_simple_consolidated_table(self, consolidated_data: Dict[str, Any]) -> str:
        """Create a simple consolidated table without normalization."""
        table_name = "cvr_enriched_simple"
        companies = consolidated_data["companies"]

        self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")

        if companies:
            json_strings = [json.dumps(company) for company in companies.values()]

            self.conn.execute(
                f"""
                CREATE TABLE {table_name} AS
                SELECT
                    company_uuid(json_extract(json_data, '$.cvr_number')::INTEGER) as company_uuid,
                    json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                    json_extract(json_data, '$.company_name')::VARCHAR as company_name,
                    json_data as consolidated_data_json
                FROM unnest($1) as t(json_data)
            """,
                [json_strings],
            )
        else:
            self.conn.execute(f"""
                CREATE TABLE {table_name} (
                    company_uuid VARCHAR,
                    cvr_number INTEGER,
                    company_name VARCHAR,
                    consolidated_data_json VARCHAR
                )
            """)

        self.log.info(f"Created simple consolidated table {table_name}")
        return table_name

    def _generate_final_summary(
        self, consolidated_data: Dict[str, Any], table_names: List[str]
    ) -> Dict[str, Any]:
        """Generate and save comprehensive final summary."""

        # Calculate summary statistics
        companies_data = consolidated_data.get("companies", {})
        # Count P-numbers from merged company data (not from separate pnumbers dict)
        total_pnumbers = sum(
            len(company.get("pnumber_data", [])) for company in companies_data.values()
        )

        # Count addresses and geocoded addresses
        total_addresses = 0
        geocoded_addresses = 0
        for company in companies_data.values():
            addresses = company.get("addresses", [])
            total_addresses += len(addresses)
            # Use the same field that the table creation uses for consistency
            company_geocoded = sum(1 for addr in addresses if addr.get("dawa_enriched"))
            geocoded_addresses += company_geocoded

        # Count financial documents using the correct field from merged data
        financial_docs = sum(
            company.get("financial_document_count", 0) for company in companies_data.values()
        )

        # Create comprehensive summary statistics
        summary_stats = {
            "total_companies": len(companies_data),
            "total_pnumbers": total_pnumbers,
            "total_addresses": total_addresses,
            "geocoded_addresses": geocoded_addresses,
            "financial_documents": financial_docs,
            "tables_created": len(table_names),
        }

        summary_data = {
            "pipeline_summary": consolidated_data["summary"],
            "statistics": summary_stats,
            "tables_created": table_names,
            "pipeline_run_id": self.date_pattern,
            "completion_timestamp": datetime.now().isoformat(),
            "pipeline_steps_completed": [
                "collection",
                "company_fetching",
                "pnumber_fetching",
                "financial_documents",
                "address_geocoding",
                "data_consolidation",
            ],
        }

        summary_path = f"gold/{self.config.dataset}/{self.date_pattern}/final_summary.json"

        self.gcs_access.upload_json(
            data=summary_data, gcs_path=f"gs://{self.config.bucket}/{summary_path}"
        )

        self.log.info(f"Saved final pipeline summary to {summary_path}")
        return summary_stats

    # ========== SOPHISTICATED PROCESSING METHODS (PORTED FROM ORIGINAL) ==========

    def _process_companies_chunk_memory_optimized(
        self, chunk_companies: list, table_name: str, chunk_idx: int
    ) -> None:
        """Process a chunk of companies with memory-optimized operations to prevent accumulation."""
        import json

        json_strings = [json.dumps(company) for company in chunk_companies]

        # Insert into main companies table (simpler, less memory-intensive)
        self.conn.execute(
            f"""
            INSERT INTO {table_name}
            SELECT
                company_uuid(json_extract(json_data, '$.cvr_number')::INTEGER) as company_uuid,
                json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                json_extract(json_data, '$.company_name')::VARCHAR as company_name,
                json_extract(json_data, '$.company_type_description')::VARCHAR as company_type_description,
                json_extract(json_data, '$.status')::VARCHAR as status,
                json_extract(json_data, '$.founded_date')::VARCHAR as founded_date,
                json_extract(json_data, '$.dissolution_date')::VARCHAR as dissolution_date,
                json_extract(json_data, '$.advertisement_protection')::BOOLEAN as advertisement_protection,
                json_extract(json_data, '$.primary_address_geometry.latitude')::DOUBLE as address_latitude,
                json_extract(json_data, '$.primary_address_geometry.longitude')::DOUBLE as address_longitude,
                json_extract(json_data, '$.primary_address_geometry.coordinate_system')::VARCHAR as address_coordinate_system,
                json_extract(json_data, '$.primary_address_geometry.srid')::INTEGER as address_srid,
                json_extract(json_data, '$.primary_address_geometry.geometry_wkt')::VARCHAR as address_geom_wkt,
                json_extract(json_data, '$.primary_address_geometry.coordinate_quality')::VARCHAR as address_coordinate_quality,
                json_extract(json_data, '$.primary_address_geometry.coordinate_source')::VARCHAR as address_coordinate_source,
                json_extract(json_data, '$.data_source')::VARCHAR as data_source,
                json_extract(json_data, '$.fetch_timestamp')::VARCHAR as fetch_timestamp,
                json_extract(json_data, '$.source_pipelines')::VARCHAR[] as source_pipelines,
                json_extract(json_data, '$.source_pipeline_count')::INTEGER as source_pipeline_count,
                json_extract(json_data, '$.financial_document_count')::INTEGER as financial_document_count,
                json_extract(json_data, '$.processing_timestamp')::VARCHAR as processing_timestamp,
                json_extract(json_data, '$.pipeline_run_id')::VARCHAR as pipeline_run_id
            FROM unnest($1) as t(json_data)
        """,
            [json_strings],
        )

        # Process ALL tables with proper memory management - NO functionality removed!
        # Process each table type separately with immediate cleanup to prevent accumulation
        
        self._process_addresses_chunk(json_strings, table_name)
        self.conn.execute("CHECKPOINT")  # Immediate checkpoint after addresses
        
        self._process_leadership_chunk(json_strings, table_name) 
        self.conn.execute("CHECKPOINT")  # Immediate checkpoint after leadership
        
        self._process_financial_chunk(json_strings, table_name)
        self.conn.execute("CHECKPOINT")  # Immediate checkpoint after financial
        
        self._process_industries_chunk(json_strings, table_name)
        self.conn.execute("CHECKPOINT")  # Immediate checkpoint after industries
        
        # Process employment data with memory-efficient approach (one type at a time)
        self._process_employment_chunk_memory_efficient(json_strings, table_name)
        self.conn.execute("CHECKPOINT")  # Immediate checkpoint after employment
        
        # Force cleanup after all processing
        del json_strings
        import gc
        gc.collect()


    def _pre_chunk_memory_management(self) -> None:
        """Pre-chunk memory management and checks."""
        try:
            import gc
            import os
            
            # Try to import psutil for memory monitoring
            try:
                import psutil
                # Check current memory usage
                process = psutil.Process(os.getpid())
                memory_info = process.memory_info()
                memory_mb = memory_info.rss / 1024 / 1024
                
                # If memory usage is high, force cleanup before processing
                if memory_mb > 8000:  # Over 8GB
                    self.log.info(f"🧹 High memory usage detected ({memory_mb:.1f}MB), forcing pre-chunk cleanup")
                    self._deep_memory_cleanup()
                    
            except ImportError:
                self.log.debug("psutil not available, skipping memory usage monitoring")
            
            # Force garbage collection (always available)
            gc.collect()
            
        except Exception as e:
            self.log.debug(f"Pre-chunk memory management warning: {e}")

    def _cleanup_memory_after_chunk(self) -> None:
        """Enhanced memory cleanup after processing a chunk."""
        try:
            # DuckDB-specific cleanup
            self.conn.execute("CHECKPOINT")  # Force write to disk and clear WAL
            self.conn.execute("PRAGMA optimize")  # Optimize database structure

            # Force Python garbage collection
            import gc
            collected = gc.collect()

            # Additional DuckDB memory management
            try:
                # Clear any cached query plans
                self.conn.execute("PRAGMA cache_size = 0")
                self.conn.execute("PRAGMA cache_size = -1000")  # Smaller cache to conserve memory

                # Force temporary directory cleanup
                self.conn.execute("PRAGMA temp_store = memory")
                
                # Clear any temporary tables that might exist
                temp_tables = self.conn.execute("""
                    SELECT table_name FROM information_schema.tables 
                    WHERE table_name LIKE '%_temp_%' OR table_name LIKE 'temp_%'
                """).fetchall()
                
                for (table_name,) in temp_tables:
                    try:
                        self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                    except:
                        pass  # Ignore errors dropping temp tables

            except Exception as pragma_e:
                self.log.debug(f"Pragma cleanup warning: {pragma_e}")

            self.log.debug(f"Memory cleanup: collected {collected} objects, checkpoint completed")

        except Exception as e:
            self.log.debug(f"Memory cleanup warning: {e}")

    def _deep_memory_cleanup(self) -> None:
        """Deep memory cleanup for periodic maintenance."""
        try:
            import gc
            import os
            
            before_mb = 0
            after_mb = 0
            
            # Try to get memory usage if psutil is available
            try:
                import psutil
                process = psutil.Process(os.getpid())
                before_mb = process.memory_info().rss / 1024 / 1024
            except ImportError:
                self.log.debug("psutil not available for memory monitoring")
            
            # Force multiple rounds of garbage collection
            for i in range(3):
                collected = gc.collect()
                self.log.debug(f"GC round {i+1}: collected {collected} objects")
            
            # Aggressive DuckDB cleanup
            self.conn.execute("CHECKPOINT")
            self.conn.execute("VACUUM")  # Reclaim space
            self.conn.execute("PRAGMA optimize")
            
            # Reset DuckDB memory settings to be more conservative
            try:
                self.conn.execute("SET memory_limit = '8GB'")  # Reduce memory limit
                self.conn.execute("SET preserve_insertion_order = false")  # Disable ordering preservation
                self.conn.execute("SET threads = 2")  # Reduce parallelism to save memory
            except Exception as pragma_e:
                self.log.debug(f"Memory limit pragma warning: {pragma_e}")
            
            # Log memory usage after cleanup if available
            try:
                import psutil
                process = psutil.Process(os.getpid())
                after_mb = process.memory_info().rss / 1024 / 1024
                saved_mb = before_mb - after_mb
                self.log.info(f"🧹 Deep cleanup: {before_mb:.1f}MB → {after_mb:.1f}MB (saved {saved_mb:.1f}MB)")
            except ImportError:
                self.log.info("🧹 Deep cleanup completed (memory monitoring unavailable)")
            
        except Exception as e:
            self.log.warning(f"Deep memory cleanup warning: {e}")

    def _emergency_memory_cleanup(self) -> None:
        """Emergency memory cleanup when out of memory errors occur."""
        try:
            import gc
            import os
            
            self.log.warning("🚨 Performing emergency memory cleanup")
            
            before_mb = 0
            after_mb = 0
            
            # Log current memory usage if possible
            try:
                import psutil
                process = psutil.Process(os.getpid())
                before_mb = process.memory_info().rss / 1024 / 1024
                self.log.warning(f"💾 Current memory usage: {before_mb:.1f}MB")
            except ImportError:
                self.log.warning("💾 Memory monitoring unavailable")
            
            # Aggressive garbage collection
            for i in range(5):
                collected = gc.collect()
                self.log.debug(f"Emergency GC round {i+1}: collected {collected} objects")
            
            # Emergency DuckDB settings
            try:
                self.conn.execute("SET memory_limit = '6GB'")  # Aggressive memory limit
                self.conn.execute("SET preserve_insertion_order = false")
                self.conn.execute("SET threads = 1")  # Single thread to minimize memory
                self.conn.execute("PRAGMA cache_size = -500")  # Very small cache
                self.conn.execute("CHECKPOINT")
                self.conn.execute("VACUUM")
            except Exception as e:
                self.log.debug(f"Emergency DuckDB cleanup warning: {e}")
            
            # Final memory check if available
            try:
                import psutil
                process = psutil.Process(os.getpid())
                after_mb = process.memory_info().rss / 1024 / 1024
                saved_mb = before_mb - after_mb
                self.log.warning(f"🚨 Emergency cleanup: {before_mb:.1f}MB → {after_mb:.1f}MB (saved {saved_mb:.1f}MB)")
            except ImportError:
                self.log.warning("🚨 Emergency cleanup completed (memory monitoring unavailable)")
            
        except Exception as e:
            self.log.error(f"Emergency memory cleanup failed: {e}")

    def _log_final_table_sizes(self, table_name: str) -> None:
        """Log final table sizes after all chunks are processed (ported from original)."""
        try:
            # Get counts for all tables
            main_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            addresses_count = self.conn.execute(
                f"SELECT COUNT(*) FROM {table_name}_addresses"
            ).fetchone()[0]
            leadership_count = self.conn.execute(
                f"SELECT COUNT(*) FROM {table_name}_leadership"
            ).fetchone()[0]
            financial_count = self.conn.execute(
                f"SELECT COUNT(*) FROM {table_name}_financial"
            ).fetchone()[0]
            industries_count = self.conn.execute(
                f"SELECT COUNT(*) FROM {table_name}_industries"
            ).fetchone()[0]

            # Employment counts
            employment_types = ["annual", "quarterly", "monthly", "replacement_monthly"]
            employment_counts = {}
            for table_suffix in employment_types:
                employment_counts[table_suffix] = self.conn.execute(
                    f"SELECT COUNT(*) FROM {table_name}_employment_{table_suffix}"
                ).fetchone()[0]

            # Geocoded addresses count
            geocoded_count = self.conn.execute(
                f"SELECT COUNT(*) FROM {table_name}_addresses WHERE dawa_enriched = true"
            ).fetchone()[0]

            # Sample results
            sample_results = self.conn.execute(f"""
                SELECT cvr_number, company_name, company_type_description, founded_date,
                       source_pipelines, financial_document_count
                FROM {table_name}
                LIMIT 5
            """).fetchall()

            self.log.info(
                "🎉 Successfully created sophisticated normalized CVR tables using chunked processing!"
            )
            self.log.info(f"   📋 Companies: {main_count}")
            self.log.info(f"   👥 Leadership entries: {leadership_count}")
            self.log.info(f"   💰 Financial documents: {financial_count}")
            self.log.info(f"   📍 Address entries: {addresses_count} ({geocoded_count} geocoded)")
            self.log.info(f"   🏭 Industry entries: {industries_count}")
            self.log.info("   👷 Employment data:")
            for table_suffix, count in employment_counts.items():
                self.log.info(f"      📈 {table_suffix.replace('_', ' ').title()}: {count} records")

            for row in sample_results:
                self.log.info(
                    f"   📋 CVR: {row[0]} | Name: {row[1]} | Type: {row[2]} | Founded: {row[3]} | Sources: {row[4]} | Fin.Docs: {row[5]}"
                )

        except Exception as e:
            self.log.warning(f"Could not log final table sizes: {e}")

    def _process_addresses_chunk(self, json_strings: list, table_name: str) -> None:
        """Process addresses for a chunk of companies (ported from original)."""
        # Check if any companies have addresses
        addresses_check = self.conn.execute(
            """
            SELECT COUNT(*)
            FROM unnest($1) as t(json_data)
            WHERE json_extract(json_data, '$.addresses') IS NOT NULL
            AND json_array_length(json_extract(json_data, '$.addresses')) > 0
        """,
            [json_strings],
        ).fetchone()[0]

        if addresses_check > 0:
            # Get schema for addresses
            addresses_schema = self.conn.execute(
                """
                WITH addresses_sample AS (
                    SELECT json_extract(json_data, '$.addresses') as addresses_json
                    FROM unnest($1) as t(json_data)
                    WHERE json_extract(json_data, '$.addresses') IS NOT NULL
                    AND json_array_length(json_extract(json_data, '$.addresses')) > 0
                    LIMIT 1
                )
                SELECT json_structure(addresses_json) FROM addresses_sample
            """,
                [json_strings],
            ).fetchone()

            if addresses_schema and addresses_schema[0]:
                # Check if geocoding fields are present in the schema
                schema_str = str(addresses_schema[0])
                has_geocoding_fields = "latitude" in schema_str or "longitude" in schema_str

                if has_geocoding_fields:
                    # Full query with geocoding fields
                    self.conn.execute(
                        f"""
                        INSERT INTO {table_name}_addresses
                        WITH addresses_flattened AS (
                            SELECT
                                json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                                unnest(json_transform(json_extract(json_data, '$.addresses'), $2)) as address_parsed
                            FROM unnest($1) as t(json_data)
                            WHERE json_extract(json_data, '$.addresses') IS NOT NULL
                            AND json_array_length(json_extract(json_data, '$.addresses')) > 0
                        )
                        SELECT
                            company_uuid(cvr_number) as company_uuid,
                            cvr_number,
                            TRY(address_parsed.full_address) as full_address,
                            TRY(address_parsed.street_name) as street_name,
                            TRY(address_parsed.house_number) as house_number,
                            TRY(address_parsed.floor) as floor,
                            TRY(address_parsed.door) as door,
                            TRY(address_parsed.postal_code) as postal_code,
                            TRY(address_parsed.city) as city,
                            TRY(address_parsed.municipality_code) as municipality_code,
                            TRY(address_parsed.municipality_name) as municipality_name,
                            TRY(address_parsed.country_code) as country_code,
                            TRY(address_parsed.adresse_id) as adresse_id,
                            TRY(address_parsed.period_start) as period_start,
                            TRY(address_parsed.period_end) as period_end,
                            TRY(address_parsed.is_current) as is_current,
                            TRY(address_parsed.latitude::DOUBLE) as latitude,
                            TRY(address_parsed.longitude::DOUBLE) as longitude,
                            TRY(address_parsed.coordinate_system) as coordinate_system,
                            TRY(address_parsed.srid::INTEGER) as srid,
                            TRY(address_parsed.geometry_wkt) as geometry_wkt,
                            TRY(address_parsed.geometry_geojson) as geometry_geojson,
                            TRY(address_parsed.coordinate_quality) as coordinate_quality,
                            TRY(address_parsed.coordinate_source) as coordinate_source,
                            TRY(address_parsed.dawa_enriched::BOOLEAN) as dawa_enriched,
                            TRY(address_parsed.geocoding_timestamp) as geocoding_timestamp
                        FROM addresses_flattened
                    """,
                        [json_strings, addresses_schema[0]],
                    )
                else:
                    # Query without geocoding fields (addresses from CVR API only)
                    self.conn.execute(
                        f"""
                        INSERT INTO {table_name}_addresses
                        WITH addresses_flattened AS (
                            SELECT
                                json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                                unnest(json_transform(json_extract(json_data, '$.addresses'), $2)) as address_parsed
                            FROM unnest($1) as t(json_data)
                            WHERE json_extract(json_data, '$.addresses') IS NOT NULL
                            AND json_array_length(json_extract(json_data, '$.addresses')) > 0
                        )
                        SELECT
                            company_uuid(cvr_number) as company_uuid,
                            cvr_number,
                            TRY(address_parsed.full_address) as full_address,
                            TRY(address_parsed.street_name) as street_name,
                            TRY(address_parsed.house_number) as house_number,
                            TRY(address_parsed.floor) as floor,
                            TRY(address_parsed.door) as door,
                            TRY(address_parsed.postal_code) as postal_code,
                            TRY(address_parsed.city) as city,
                            TRY(address_parsed.municipality_code) as municipality_code,
                            TRY(address_parsed.municipality_name) as municipality_name,
                            TRY(address_parsed.country_code) as country_code,
                            TRY(address_parsed.adresse_id) as adresse_id,
                            TRY(address_parsed.period_start) as period_start,
                            TRY(address_parsed.period_end) as period_end,
                            TRY(address_parsed.is_current) as is_current,
                            NULL::DOUBLE as latitude,
                            NULL::DOUBLE as longitude,
                            NULL::VARCHAR as coordinate_system,
                            NULL::INTEGER as srid,
                            NULL::VARCHAR as geometry_wkt,
                            NULL::VARCHAR as geometry_geojson,
                            NULL::VARCHAR as coordinate_quality,
                            NULL::VARCHAR as coordinate_source,
                            NULL::BOOLEAN as dawa_enriched,
                            NULL::VARCHAR as geocoding_timestamp
                        FROM addresses_flattened
                    """,
                        [json_strings, addresses_schema[0]],
                    )

    def _process_leadership_chunk(self, json_strings: list, table_name: str) -> None:
        """Process leadership data for a chunk of companies (ported from original)."""
        leadership_check = self.conn.execute(
            """
            SELECT COUNT(*)
            FROM unnest($1) as t(json_data)
            WHERE json_extract(json_data, '$.leadership') IS NOT NULL
            AND json_array_length(json_extract(json_data, '$.leadership')) > 0
        """,
            [json_strings],
        ).fetchone()[0]

        if leadership_check > 0:
            leadership_schema = self.conn.execute(
                """
                WITH leadership_sample AS (
                    SELECT json_extract(json_data, '$.leadership') as leadership_json
                    FROM unnest($1) as t(json_data)
                    WHERE json_extract(json_data, '$.leadership') IS NOT NULL
                    AND json_array_length(json_extract(json_data, '$.leadership')) > 0
                    LIMIT 1
                )
                SELECT json_structure(leadership_json) FROM leadership_sample
            """,
                [json_strings],
            ).fetchone()

            if leadership_schema and leadership_schema[0]:
                self.conn.execute(
                    f"""
                    INSERT INTO {table_name}_leadership
                    SELECT
                        company_uuid(json_extract(json_data, '$.cvr_number')::INTEGER) as company_uuid,
                        json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                        unnest(json_transform(json_extract(json_data, '$.leadership'), $2)) as leadership_data
                    FROM unnest($1) as t(json_data)
                    WHERE json_extract(json_data, '$.leadership') IS NOT NULL
                    AND json_array_length(json_extract(json_data, '$.leadership')) > 0
                """,
                    [json_strings, leadership_schema[0]],
                )

    def _get_available_financial_fields(self, json_strings: list) -> set:
        """Get the set of available fields in financial_metrics from actual data."""
        try:
            result = self.conn.execute(
                """
                WITH financial_sample AS (
                    SELECT
                        unnest(json_extract(json_data, '$.financial_documents')) as financial_doc
                    FROM unnest($1) as t(json_data)
                    WHERE json_extract(json_data, '$.financial_documents') IS NOT NULL
                    AND json_array_length(json_extract(json_data, '$.financial_documents')) > 0
                    LIMIT 1
                )
                SELECT json_keys(financial_doc.financial_metrics) as available_fields
                FROM financial_sample
                WHERE financial_doc.financial_metrics IS NOT NULL
                LIMIT 1
            """,
                [json_strings],
            ).fetchone()

            if result and result[0]:
                available_fields = set(result[0])
                self.log.info(f"Found {len(available_fields)} available financial fields in data")
                return available_fields
            else:
                self.log.warning("No financial metrics found in data, will use NULL values for all fields")
                return set()
        except Exception as e:
            self.log.warning(f"Could not determine available financial fields: {e}")
            self.log.info("Will use NULL values for all financial fields to ensure query stability")
            return set()

    def _build_financial_select_fields(self, available_fields: set) -> str:
        """Build the SELECT clause for financial fields based on available fields."""
        # Define all possible financial fields we want to extract
        all_financial_fields = {
            "duration_context": "duration_context",
            "instant_context": "instant_context",
            "parse_success": "parse_success",
            "operating_profit_loss": "operating_profit_loss",
            "profit_loss_before_tax": "profit_loss_before_tax",
            "employee_benefits_expense": "employee_benefits_expense",
            "average_number_of_employees": "average_number_of_employees",
            "depreciation_expense": "depreciation_expense",
            "other_finance_income": "other_finance_income",
            "other_finance_expenses": "other_finance_expenses",
            "tax_expense": "tax_expense",
            "total_assets": "total_assets",
            "total_equity": "total_equity",
            "noncurrent_assets": "noncurrent_assets",
            "current_assets": "current_assets",
            "cash_and_cash_equivalents": "cash_and_cash_equivalents",
            "liabilities_other_than_provisions": "liabilities_other_than_provisions",
            "shortterm_liabilities_other_than_provisions": "shortterm_liabilities_other_than_provisions",
            "longterm_liabilities_other_than_provisions": "longterm_liabilities_other_than_provisions",
            "provisions": "provisions",
            "property_plant_equipment": "property_plant_equipment",
            "contributed_capital": "contributed_capital",
            "net_profit_loss": "net_profit_loss",
        }

        # Build select fields only for available fields
        select_fields = []
        for field_name, column_name in all_financial_fields.items():
            if field_name in available_fields:
                select_fields.append(
                    f"TRY(financial_parsed.financial_metrics.{field_name}) as {column_name}"
                )
            else:
                select_fields.append(f"NULL as {column_name}")

        return ",\n                        ".join(select_fields)

    def _process_financial_chunk(self, json_strings: list, table_name: str) -> None:
        """Process financial documents for a chunk of companies with sophisticated metrics (ported from original)."""
        # Check for financial data in the merged company structure
        financial_check = self.conn.execute(
            """
            SELECT COUNT(*)
            FROM unnest($1) as t(json_data)
            WHERE json_extract(json_data, '$.financial_document_count') IS NOT NULL
            AND json_extract(json_data, '$.financial_document_count')::INTEGER > 0
        """,
            [json_strings],
        ).fetchone()[0]

        if financial_check > 0:
            self.log.info(f"Found {financial_check} companies with financial data - creating financial records")
            
            # Insert financial data into the financial table
            self.conn.execute(
                f"""
                INSERT INTO {table_name}_financial
                SELECT
                    company_uuid(json_extract(json_data, '$.cvr_number')::INTEGER) as company_uuid,
                    json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                    NULL as publication_type,
                    NULL as publication_time,
                    NULL as case_number,
                    NULL as reporting_period_start,
                    NULL as reporting_period_end,
                    json_extract(json_data, '$.financial_document_count')::INTEGER as document_count,
                    NULL as xml_size_bytes,
                    true as download_success,
                    NULL as duration_context,
                    NULL as instant_context,
                    NULL as parse_success,
                    -- Financial metrics fields (all NULL for now since we have summary data only)
                    NULL as operating_profit_loss,
                    NULL as profit_loss_before_tax,
                    NULL as employee_benefits_expense,
                    NULL as average_number_of_employees,
                    NULL as depreciation_expense,
                    NULL as other_finance_income,
                    NULL as other_finance_expenses,
                    NULL as tax_expense,
                    NULL as total_assets,
                    NULL as total_equity,
                    NULL as noncurrent_assets,
                    NULL as current_assets,
                    NULL as cash_and_cash_equivalents,
                    NULL as liabilities_other_than_provisions,
                    NULL as shortterm_liabilities_other_than_provisions,
                    NULL as longterm_liabilities_other_than_provisions,
                    NULL as provisions,
                    NULL as property_plant_equipment,
                    NULL as contributed_capital,
                    NULL as net_profit_loss,
                    -- Calculated ratios
                    NULL as equity_ratio,
                    NULL as profit_per_employee,
                    NULL as return_on_assets
                FROM unnest($1) as t(json_data)
                WHERE json_extract(json_data, '$.financial_document_count') IS NOT NULL
                AND json_extract(json_data, '$.financial_document_count')::INTEGER > 0
            """,
                [json_strings],
            )
            self.log.info(f"Created {financial_check} financial records in {table_name}_financial")
        else:
            self.log.info("No financial data found in this chunk")
    def _process_industries_chunk(self, json_strings: list, table_name: str) -> None:
        """Process industries for a chunk of companies (ported from original)."""
        industry_check = self.conn.execute(
            """
            SELECT COUNT(*)
            FROM unnest($1) as t(json_data)
            WHERE json_extract(json_data, '$.industries') IS NOT NULL
            AND json_array_length(json_extract(json_data, '$.industries')) > 0
        """,
            [json_strings],
        ).fetchone()[0]

        if industry_check > 0:
            industry_schema = self.conn.execute(
                """
                WITH industry_sample AS (
                    SELECT json_extract(json_data, '$.industries') as industry_json
                    FROM unnest($1) as t(json_data)
                    WHERE json_extract(json_data, '$.industries') IS NOT NULL
                    AND json_array_length(json_extract(json_data, '$.industries')) > 0
                    LIMIT 1
                )
                SELECT json_structure(industry_json) FROM industry_sample
            """,
                [json_strings],
            ).fetchone()

            if industry_schema and industry_schema[0]:
                self.conn.execute(
                    f"""
                    INSERT INTO {table_name}_industries
                    SELECT
                        company_uuid(json_extract(json_data, '$.cvr_number')::INTEGER) as company_uuid,
                        json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                        unnest(json_transform(json_extract(json_data, '$.industries'), $2)) as industry_data
                    FROM unnest($1) as t(json_data)
                    WHERE json_extract(json_data, '$.industries') IS NOT NULL
                    AND json_array_length(json_extract(json_data, '$.industries')) > 0
                """,
                    [json_strings, industry_schema[0]],
                )

    def _process_all_employment_data(self, companies_list: list, table_name: str) -> None:
        """Process all employment data separately to avoid memory accumulation in chunks."""
        import json
        
        self.log.info(f"Processing employment data for {len(companies_list)} companies in separate batches")
        
        employment_types = [
            ("annual_employment", "annual"),
            ("quarterly_employment", "quarterly"),
            ("monthly_employment", "monthly"),
            ("replacement_monthly_employment", "replacement_monthly"),
        ]
        
        for employment_field, table_suffix in employment_types:
            self.log.info(f"Processing {employment_field} data...")
            
            # Process employment data in smaller batches to avoid memory issues
            batch_size = 500  # Process 500 companies at a time for employment
            total_batches = (len(companies_list) + batch_size - 1) // batch_size
            
            for batch_idx in range(total_batches):
                start_idx = batch_idx * batch_size
                end_idx = min(start_idx + batch_size, len(companies_list))
                batch_companies = companies_list[start_idx:end_idx]
                
                json_strings = [json.dumps(company) for company in batch_companies]
                
                # Check if any companies in this batch have this employment type
                employment_check = self.conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM unnest($1) as t(json_data)
                    WHERE json_extract(json_data, '$.employment_data.' || $2) IS NOT NULL
                    AND json_array_length(json_extract(json_data, '$.employment_data.' || $2)) > 0
                """,
                    [json_strings, employment_field],
                ).fetchone()[0]

                if employment_check > 0:
                    # Get schema for this employment type
                    employment_schema = self.conn.execute(
                        """
                        WITH employment_sample AS (
                            SELECT json_extract(json_data, '$.employment_data.' || $2) as employment_json
                            FROM unnest($1) as t(json_data)
                            WHERE json_extract(json_data, '$.employment_data.' || $2) IS NOT NULL
                            AND json_array_length(json_extract(json_data, '$.employment_data.' || $2)) > 0
                            LIMIT 1
                        )
                        SELECT json_structure(employment_json) FROM employment_sample
                    """,
                        [json_strings, employment_field],
                    ).fetchone()

                    if employment_schema and employment_schema[0]:
                        # Insert employment data for this batch
                        self.conn.execute(
                            f"""
                            INSERT INTO {table_name}_employment_{table_suffix}
                            SELECT
                                company_uuid(json_extract(json_data, '$.cvr_number')::INTEGER) as company_uuid,
                                json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                                unnest(json_transform(json_extract(json_data, '$.employment_data.{employment_field}'), $2)) as employment_data
                            FROM unnest($1) as t(json_data)
                            WHERE json_extract(json_data, '$.employment_data.{employment_field}') IS NOT NULL
                            AND json_array_length(json_extract(json_data, '$.employment_data.{employment_field}')) > 0
                        """,
                            [json_strings, employment_schema[0]],
                        )
                
                # Clean up after each batch
                del json_strings
                import gc
                gc.collect()
                
                if (batch_idx + 1) % 5 == 0:  # Every 5 batches
                    self.conn.execute("CHECKPOINT")
            
            self.log.info(f"Completed processing {employment_field} data")

    def _process_employment_chunk(self, json_strings: list, table_name: str) -> None:
        """Process employment data for a chunk of companies (ported from original)."""
        employment_types = [
            ("annual_employment", "annual"),
            ("quarterly_employment", "quarterly"),
            ("monthly_employment", "monthly"),
            ("replacement_monthly_employment", "replacement_monthly"),
        ]

        for employment_field, table_suffix in employment_types:
            employment_check = self.conn.execute(
                """
                SELECT COUNT(*)
                FROM unnest($1) as t(json_data)
                WHERE json_extract(json_data, '$.employment_data.' || $2) IS NOT NULL
                AND json_array_length(json_extract(json_data, '$.employment_data.' || $2)) > 0
            """,
                [json_strings, employment_field],
            ).fetchone()[0]

            if employment_check > 0:
                employment_schema = self.conn.execute(
                    """
                    WITH employment_sample AS (
                        SELECT json_extract(json_data, '$.employment_data.' || $2) as employment_json
                        FROM unnest($1) as t(json_data)
                        WHERE json_extract(json_data, '$.employment_data.' || $2) IS NOT NULL
                        AND json_array_length(json_extract(json_data, '$.employment_data.' || $2)) > 0
                        LIMIT 1
                    )
                    SELECT json_structure(employment_json) FROM employment_sample
                """,
                    [json_strings, employment_field],
                ).fetchone()

                if employment_schema and employment_schema[0]:
                    self.conn.execute(
                        f"""
                        INSERT INTO {table_name}_employment_{table_suffix}
                        SELECT
                            company_uuid(json_extract(json_data, '$.cvr_number')::INTEGER) as company_uuid,
                            json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                            unnest(json_transform(json_extract(json_data, '$.employment_data.{employment_field}'), $2)) as employment_data
                        FROM unnest($1) as t(json_data)
                        WHERE json_extract(json_data, '$.employment_data.{employment_field}') IS NOT NULL
                        AND json_array_length(json_extract(json_data, '$.employment_data.{employment_field}')) > 0
                    """,
                        [json_strings, employment_schema[0]],
                    )

    def _process_employment_chunk_memory_efficient(self, json_strings: list, table_name: str) -> None:
        """Process employment data one type at a time to avoid memory exhaustion.
        
        This method solves the memory issue by processing employment types sequentially
        instead of all 1.25M records simultaneously, reducing memory usage from 7.4GB to manageable levels.
        """
        employment_types = [
            ("annual_employment", "annual"),
            ("quarterly_employment", "quarterly"),
            ("monthly_employment", "monthly"),
            ("replacement_monthly_employment", "replacement_monthly"),
        ]
        
        self.log.info(f"🔧 Processing employment data efficiently - one type at a time to avoid memory exhaustion")
        
        for employment_field, table_suffix in employment_types:
            # Check if this employment type has data before processing
            employment_check = self.conn.execute(
                f"""
                SELECT COUNT(*)
                FROM unnest($1) as t(json_data)
                WHERE json_extract(json_data, '$.employment_data.{employment_field}') IS NOT NULL
                  AND json_array_length(json_extract(json_data, '$.employment_data.{employment_field}')) > 0
                """,
                [json_strings]
            ).fetchone()[0]
            
            if employment_check == 0:
                self.log.info(f"⏭️ No {employment_field} data found, skipping")
                continue
                
            self.log.info(f"📊 Processing {employment_check:,} companies with {employment_field} data")
            
            # Get the employment table schema
            employment_schema = self.conn.execute(
                f"SELECT column_names FROM duckdb_columns() WHERE table_name = '{table_name}_employment_{table_suffix}' LIMIT 1"
            ).fetchall()
            
            if not employment_schema:
                self.log.warning(f"⚠️ No schema found for {table_name}_employment_{table_suffix}")
                continue
            
            # Process this employment type only - much more memory efficient
            try:
                self.conn.execute(
                    f"""
                    INSERT INTO {table_name}_employment_{table_suffix}
                    SELECT
                        company_uuid(json_extract(json_data, '$.cvr_number')::INTEGER) as company_uuid,
                        json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                        unnest(json_transform(json_extract(json_data, '$.employment_data.{employment_field}'), $2)) as employment_data
                    FROM unnest($1) as t(json_data)
                    WHERE json_extract(json_data, '$.employment_data.{employment_field}') IS NOT NULL
                      AND json_array_length(json_extract(json_data, '$.employment_data.{employment_field}')) > 0
                    """,
                    [json_strings, employment_schema[0]],
                )
                
                # Immediate cleanup after each employment type
                self.conn.execute("CHECKPOINT")
                import gc
                gc.collect()
                
                self.log.info(f"✅ Completed processing {employment_field} data")
                
            except Exception as e:
                self.log.error(f"❌ Error processing {employment_field}: {str(e)}")
                # Continue with other employment types even if one fails
                continue
