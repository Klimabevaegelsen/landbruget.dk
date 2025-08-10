"""
CVR Enrichment Gold Layer

This gold layer pipeline:
1. Collects CVR numbers from all other pipelines' CVR collection files
2. Fetches comprehensive CVR register data from distribution.virk.dk
3. Enriches company data with official CVR information
4. Stores the enriched data for use by other systems

The pipeline follows the CVR collection pattern where each pipeline saves
discovered CVR numbers to a standard location, and this pipeline collects
them all for enrichment.
"""

import json
import os
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any, Dict, Optional, Set
from urllib.parse import urlparse

from pydantic import Field
from tqdm import tqdm

from unified_pipeline.common.base import BaseJobConfig, BaseSource, GoldJobInterface
from unified_pipeline.util.cvr_api_client import CVRAPIClient
from unified_pipeline.util.cvr_collection import CVRCollectionManager
from unified_pipeline.util.timing import timed


class CVREnrichmentGoldConfig(BaseJobConfig):
    """
    Configuration for CVR enrichment gold layer.

    This configuration defines settings for collecting CVR numbers from
    all pipelines and fetching enrichment data from the CVR register.
    """

    name: str = "CVR Register Enrichment"
    dataset: str = "cvr_enrichment"
    type: str = "cvr_api"
    description: str = "CVR register data enrichment from distribution.virk.dk"
    frequency: str = "monthly"
    bucket: str = "landbrugsdata-raw-data"

    # CVR API configuration
    fetch_all_fields: bool = Field(
        default=True, description="Whether to fetch all available fields from CVR API"
    )

    fetch_financial_documents: bool = Field(
        default=True, description="Whether to fetch financial documents for each company"
    )

    max_financial_documents: int = Field(
        default=10, description="Maximum number of financial documents to fetch per company"
    )

    # Processing configuration
    batch_size: int = Field(
        default=50, description="Number of CVR numbers to process in each batch"
    )

    max_concurrent_requests: int = Field(
        default=5, description="Maximum number of concurrent API requests"
    )

    # Output configuration
    save_raw_responses: bool = Field(
        default=True, description="Whether to save raw API responses for debugging"
    )

    # Testing configuration
    test_limit: Optional[int] = Field(
        default=None, description="Limit number of CVR numbers to process for testing (None = no limit)"
    )
    
    parse_financial_xml: bool = Field(
        default=True, description="Whether to download and parse XML financial documents"
    )
    
    # Address geocoding configuration
    enable_address_geocoding: bool = Field(
        default=True, description="Whether to enrich addresses with geometry via DAWA API"
    )
    
    geocoding_current_addresses_only: bool = Field(
        default=True, description="Whether to geocode only current addresses (not historical)"
    )

    model_config = {"frozen": True}

    def apply_cli_filters(self, cli_config) -> None:
        """
        Apply CLI configuration filters to the CVR enrichment config.
        
        Args:
            cli_config: CLI configuration containing CVR-specific parameters
        """
        # Only apply CVR-specific parameters if this is a CVR enrichment job
        if hasattr(cli_config, 'test_limit') and cli_config.test_limit is not None:
            # Create a new config with updated values (since model is frozen)
            object.__setattr__(self, 'test_limit', cli_config.test_limit)
        
        if hasattr(cli_config, 'parse_financial_xml'):
            object.__setattr__(self, 'parse_financial_xml', cli_config.parse_financial_xml)
            
        if hasattr(cli_config, 'max_financial_documents'):
            object.__setattr__(self, 'max_financial_documents', cli_config.max_financial_documents)
            
        if hasattr(cli_config, 'enable_address_geocoding'):
            object.__setattr__(self, 'enable_address_geocoding', cli_config.enable_address_geocoding)
            
        if hasattr(cli_config, 'geocoding_current_addresses_only'):
            object.__setattr__(self, 'geocoding_current_addresses_only', cli_config.geocoding_current_addresses_only)


class CVREnrichmentGold(BaseSource[CVREnrichmentGoldConfig], GoldJobInterface):
    """
    CVR enrichment gold layer implementation.

    This class orchestrates the CVR enrichment process:
    1. Collects CVR numbers from all pipeline CVR collections
    2. Fetches CVR register data for each unique CVR number
    3. Processes and structures the data
    4. Saves enriched data to GCS for consumption by other systems
    """

    def __init__(self, config: CVREnrichmentGoldConfig):
        """
        Initialize CVR enrichment gold layer.

        Args:
            config: Configuration for CVR enrichment
        """
        super().__init__(config)

        # Apply DuckDB memory optimizations for large datasets
        self._apply_memory_optimizations()

        # Initialize CVR collection manager
        self.cvr_collection_manager = CVRCollectionManager(
            gcs_access=self.gcs_access, bucket=self.config.bucket
        )

        # Initialize CVR API client with credentials from memory [[memory:2283672]]
        cvr_username = os.getenv("CVR_USERNAME", "Martin_Collignon_CVR_I_SKYEN")
        cvr_password = os.getenv("CVR_PASSWORD", "3a37d029-9588-4c00-8a09-3d2901452d45")

        self.cvr_api_client = CVRAPIClient(
            username=cvr_username, 
            password=cvr_password,
            enable_geocoding=self.config.enable_address_geocoding,
            geocode_current_only=self.config.geocoding_current_addresses_only
        )

        # Set up company UUID generation
        self._setup_company_uuid_function()

        # Log configuration
        self.log.info("CVR enrichment gold layer initialized")
        self.log.info(f"📋 Configuration:")
        self.log.info(f"   • Fetch all fields: {self.config.fetch_all_fields}")
        self.log.info(f"   • Address geocoding: {'enabled' if self.config.enable_address_geocoding else 'disabled'}")
        self.log.info(f"   • Financial documents: {'enabled' if self.config.fetch_financial_documents else 'disabled'}")
        self.log.info(f"   • Test limit: {self.config.test_limit or 'none'}")

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
                        SUBSTR(crypto_hash('sha1', CONCAT('landbrugsdata-company-cvr', TRIM(CAST(cvr_number AS VARCHAR)))), 1, 8), '-',
                        SUBSTR(crypto_hash('sha1', CONCAT('landbrugsdata-company-cvr', TRIM(CAST(cvr_number AS VARCHAR)))), 9, 4), '-',
                        '5', SUBSTR(crypto_hash('sha1', CONCAT('landbrugsdata-company-cvr', TRIM(CAST(cvr_number AS VARCHAR)))), 13, 3), '-',
                        CONCAT('8', SUBSTR(crypto_hash('sha1', CONCAT('landbrugsdata-company-cvr', TRIM(CAST(cvr_number AS VARCHAR)))), 17, 3)), '-',
                        SUBSTR(crypto_hash('sha1', CONCAT('landbrugsdata-company-cvr', TRIM(CAST(cvr_number AS VARCHAR)))), 21, 12)
                    )
                END
            )
        """)
        self.log.info("✅ Company UUID function created using crypto extension SHA-1")

    def _apply_memory_optimizations(self):
        """
        Apply DuckDB-specific memory optimizations for handling large CVR datasets.
        
        Based on DuckDB performance tuning recommendations:
        https://duckdb.org/docs/stable/guides/performance/how_to_tune_workloads
        """
        try:
            # CRITICAL: Reduce threads for memory-constrained environment (GitHub Actions: 16GB RAM)
            # DuckDB recommendation: Lower thread count reduces memory pressure
            self.conn.execute("SET threads = 2")  # Reduced from default 4 to 2
            
            # CRITICAL: Reduce memory limit to leave more buffer for OS and other processes
            # GitHub Actions runner has ~16GB, we were using 12GB, now use 8GB for safety
            self.conn.execute("SET memory_limit = '8GB'")  # Reduced from 12GB to 8GB
            self.conn.execute("SET max_memory = '8GB'")
            
            # CRITICAL: Enable more aggressive memory management
            # DuckDB recommendation: Disable insertion order preservation for better memory efficiency
            self.conn.execute("SET preserve_insertion_order = false")  # Already set in base, but ensure it's applied
            
            # ADDITIONAL: More frequent checkpoints to clear temporary data
            self.conn.execute("SET checkpoint_threshold = '256MB'")  # More frequent checkpoints
            
            # ADDITIONAL: Disable progress bar to reduce memory overhead
            self.conn.execute("SET enable_progress_bar = false")
            
            # ADDITIONAL: More aggressive temporary directory management
            self.conn.execute("SET temp_directory = '/tmp/duckdb_cvr'")
            self.conn.execute("SET max_temp_directory_size = '4GB'")  # Reduced from 14GB to 4GB
            
            # ADDITIONAL: Enable object cache for better memory reuse
            self.conn.execute("SET enable_object_cache = true")
            
            # ADDITIONAL: More frequent WAL checkpoints to free memory
            self.conn.execute("SET wal_autocheckpoint = 100")  # More frequent than default
            
            self.log.info("✅ Applied DuckDB memory optimizations for CVR enrichment:")
            self.log.info("   • Threads: 2 (reduced for memory efficiency)")
            self.log.info("   • Memory limit: 8GB (conservative for 16GB system)")
            self.log.info("   • Checkpoint threshold: 256MB (frequent cleanup)")
            self.log.info("   • Temp directory: 4GB max (reduced disk usage)")
            self.log.info("   • Progress bar: disabled (reduced overhead)")
            
        except Exception as e:
            self.log.warning(f"Could not apply all memory optimizations: {e}")

    @timed(name="CVR enrichment processing")
    async def run(self, silver_data: Optional[Dict[str, Any]] = None) -> str:
        """
        Run the CVR enrichment process.

        Args:
            silver_data: Optional silver data (not used in this pipeline)

        Returns:
            Table name containing enriched CVR data
        """
        self.log.info("Starting CVR enrichment gold layer processing")

        try:
            # Step 1: Collect all CVR numbers from pipeline collections
            cvr_collection_data = self._collect_cvr_numbers()

            # Step 2: Fetch CVR register data for all collected CVR numbers
            enrichment_data = await self._fetch_cvr_enrichment_data(
                cvr_collection_data["all_cvr_numbers"]
            )

            # Step 3: Process and structure the enriched data
            processed_data = self._process_enrichment_data(enrichment_data, cvr_collection_data)

            # Step 4: Save the enriched data
            table_name = self._save_enriched_data(processed_data)

            self.log.info(f"CVR enrichment completed successfully. Data saved to: {table_name}")
            return table_name

        except Exception as e:
            self.log.error(f"CVR enrichment failed: {e}")
            raise

    @timed(name="Collecting CVR numbers")
    def _collect_cvr_numbers(self) -> Dict[str, Any]:
        """
        Collect CVR numbers from all pipeline CVR collections.

        Returns:
            Dictionary containing collected CVR data
        """
        self.log.info("Collecting CVR numbers from all pipeline collections")

        cvr_collection_data = self.cvr_collection_manager.collect_all_cvr_numbers()

        unique_cvr_count = len(cvr_collection_data["all_cvr_numbers"])
        pipelines_count = cvr_collection_data["collection_summary"]["pipelines_processed"]

        self.log.info(
            f"Collected {unique_cvr_count} unique CVR numbers from {pipelines_count} pipelines"
        )

        # Save collection summary for reference
        self._save_collection_summary(cvr_collection_data)

        return cvr_collection_data

    @timed(name="Fetching CVR enrichment data")
    async def _fetch_cvr_enrichment_data(self, cvr_numbers: Set[str]) -> Dict[str, Any]:
        """
        Fetch CVR register data for all collected CVR numbers.

        Args:
            cvr_numbers: Set of unique CVR numbers to fetch

        Returns:
            Dictionary containing CVR enrichment data
        """
        # Convert to sorted list and validate uniqueness
        cvr_list = sorted(list(cvr_numbers))

        # Apply test limit if configured
        if self.config.test_limit is not None:
            original_list_length = len(cvr_list)
            cvr_list = cvr_list[:self.config.test_limit]
            self.log.info(f"🧪 Test mode: Limited CVR list from {original_list_length} to {len(cvr_list)} entries")

        # Additional validation and logging for deduplication
        original_count = len(cvr_numbers) if isinstance(cvr_numbers, (set, list)) else 0
        unique_count = len(cvr_list)

        self.log.info("🔍 CVR deduplication summary:")
        self.log.info(f"   • Input CVR numbers: {original_count}")
        self.log.info(f"   • Unique CVR numbers: {unique_count}")
        if original_count != unique_count:
            duplicates_removed = original_count - unique_count
            self.log.info(f"   • Duplicates removed: {duplicates_removed}")

        # Validate CVR format before API calls
        valid_cvrs = []
        invalid_cvrs = []

        for cvr in cvr_list:
            if self._is_valid_cvr_format(cvr):
                valid_cvrs.append(cvr)
            else:
                invalid_cvrs.append(cvr)

        if invalid_cvrs:
            self.log.warning(
                f"⚠️ Found {len(invalid_cvrs)} invalid CVR numbers (will skip): {invalid_cvrs[:5]}{'...' if len(invalid_cvrs) > 5 else ''}"
            )

        self.log.info(
            f"🚀 Fetching CVR register data for {len(valid_cvrs)} valid, unique companies"
        )

        # Fetch company data for valid, unique CVR numbers only
        company_results = self.cvr_api_client.fetch_multiple_companies(
            cvr_numbers=valid_cvrs, 
            fetch_all_fields=self.config.fetch_all_fields,
            enrich_with_geometry=self.config.enable_address_geocoding
        )

        # Fetch financial documents if configured
        financial_documents = {}
        if self.config.fetch_financial_documents:
            self.log.info("Fetching financial documents for companies")

            companies_with_data = [cvr for cvr in valid_cvrs if company_results["results"].get(cvr)]
            
            for cvr_number in tqdm(companies_with_data, desc="Fetching financial documents", unit="company"):
                try:
                    docs = self.cvr_api_client.get_financial_documents(
                        cvr_number=cvr_number, max_results=self.config.max_financial_documents
                    )
                    # Download XML content if configured
                    if self.config.parse_financial_xml:
                        docs = self._download_financial_xml_documents(docs)
                    
                    financial_documents[cvr_number] = docs
                except Exception as e:
                    self.log.warning(
                        f"Failed to fetch financial documents for CVR {cvr_number}: {e}"
                    )
                    financial_documents[cvr_number] = []

        enrichment_data = {
            "company_data": company_results,
            "financial_documents": financial_documents,
            "fetch_timestamp": datetime.now().isoformat(),
            "config": {
                "fetch_all_fields": self.config.fetch_all_fields,
                "fetch_financial_documents": self.config.fetch_financial_documents,
                "max_financial_documents": self.config.max_financial_documents,
            },
        }

        # Save raw responses if configured
        if self.config.save_raw_responses:
            self._save_raw_responses(enrichment_data)

        return enrichment_data

    @timed(name="Processing enrichment data")
    def _process_enrichment_data(
        self, enrichment_data: Dict[str, Any], cvr_collection_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Process and structure the enriched CVR data.

        Args:
            enrichment_data: Raw CVR enrichment data from API
            cvr_collection_data: CVR collection data from pipelines

        Returns:
            Processed enrichment data ready for storage
        """
        self.log.info("Processing CVR enrichment data")

        company_results = enrichment_data["company_data"]["results"]
        financial_documents = enrichment_data.get("financial_documents", {})
        pipeline_sources = cvr_collection_data["pipeline_sources"]

        # Process each company's data
        processed_companies = []

        for cvr_number, company_data in company_results.items():
            if company_data:
                # Add pipeline source information
                company_data["source_pipelines"] = pipeline_sources.get(cvr_number, [])
                company_data["source_pipeline_count"] = len(company_data["source_pipelines"])

                # Add financial documents
                company_data["financial_documents"] = financial_documents.get(cvr_number, [])
                company_data["financial_document_count"] = len(company_data["financial_documents"])

                # Add processing metadata
                company_data["processing_timestamp"] = datetime.now().isoformat()
                company_data["pipeline_run_id"] = self.date_pattern

                processed_companies.append(company_data)

        # Create summary statistics
        summary = {
            "total_cvr_numbers": len(company_results),
            "successful_enrichments": len(processed_companies),
            "failed_enrichments": len([c for c in company_results.values() if c is None]),
            "companies_with_financial_docs": len(
                [c for c in processed_companies if c["financial_document_count"] > 0]
            ),
            "total_financial_documents": sum(
                c["financial_document_count"] for c in processed_companies
            ),
            "source_pipeline_count": cvr_collection_data["collection_summary"][
                "pipelines_processed"
            ],
            "processing_timestamp": datetime.now().isoformat(),
        }

        processed_data = {
            "companies": processed_companies,
            "summary": summary,
            "collection_summary": cvr_collection_data["collection_summary"],
            "api_summary": enrichment_data["company_data"]["summary"],
        }

        self.log.info(
            f"Processed {summary['successful_enrichments']} companies with CVR enrichment data"
        )

        return processed_data

    @timed(name="Saving enriched data")
    def _save_enriched_data(self, processed_data: Dict[str, Any]) -> str:
        """
        Save the processed CVR enrichment data to GCS using chunked processing.

        Args:
            processed_data: Processed enrichment data

        Returns:
            Table name where data was saved
        """
        # Create DuckDB table with enriched company data
        companies_data = processed_data["companies"]

        # Convert to DuckDB table
        table_name = "cvr_enriched_companies"

        # Create table structure
        self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")

        if companies_data:
            self.log.info(f"🔍 Creating normalized tables for {len(companies_data)} companies using chunked processing")
            
            # Process companies in chunks to avoid memory issues
            # DuckDB recommendation: Use smaller chunks for memory-constrained environments
            chunk_size = 50  # Process 50 companies at a time (reduced from 100 for safety)
            total_companies = len(companies_data)
            num_chunks = (total_companies + chunk_size - 1) // chunk_size
            
            self.log.info(f"📦 Processing {total_companies} companies in {num_chunks} chunks of {chunk_size}")
            
            # Initialize all tables as empty with correct schema
            self._initialize_empty_tables(table_name)
            
            # Process each chunk
            for chunk_idx in range(num_chunks):
                start_idx = chunk_idx * chunk_size
                end_idx = min(start_idx + chunk_size, total_companies)
                chunk_companies = companies_data[start_idx:end_idx]
                
                self.log.info(f"📦 Processing chunk {chunk_idx + 1}/{num_chunks}: companies {start_idx}-{end_idx-1}")
                
                try:
                    self._process_companies_chunk(chunk_companies, table_name, chunk_idx)
                    
                    # Memory cleanup after each chunk
                    self._cleanup_memory_after_chunk()
                    
                except Exception as e:
                    self.log.error(f"Error processing chunk {chunk_idx + 1}: {e}")
                    if "Out of Memory" in str(e) or "memory" in str(e).lower():
                        self.log.warning("⚠️ Memory exhaustion detected - stopping processing")
                        break
                    raise
            
            # Log final table sizes
            self._log_final_table_sizes(table_name)
        else:
            # Create empty table with schema
            self._create_empty_tables(table_name)
            self.log.warning(f"Created empty table {table_name} - no company data to process")

        # Save all tables to GCS
        tables_to_save = [table_name]
        if companies_data:
            # Add the additional normalized tables
            tables_to_save.extend([
                f"{table_name}_leadership",
                f"{table_name}_financial", 
                f"{table_name}_addresses",
                f"{table_name}_industries",
                f"{table_name}_employment_annual",
                f"{table_name}_employment_quarterly",
                f"{table_name}_employment_monthly",
                f"{table_name}_employment_replacement_monthly"
            ])
        
        for table in tables_to_save:
            # Check if table exists and has data
            try:
                count = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                if count > 0:
                    self.log.info(f"💾 Saving table {table} ({count} rows)")
                    self._save_data(
                        data=table, dataset=f"{self.config.dataset}_{table.split('_')[-1]}" if '_' in table else self.config.dataset, 
                        bucket=self.config.bucket, stage="gold"
                    )
                else:
                    self.log.info(f"⚠️ Skipping empty table {table}")
            except Exception as e:
                self.log.warning(f"⚠️ Could not save table {table}: {e}")

        # Save summary data separately
        self._save_summary_data(processed_data)

        return table_name

    def _save_collection_summary(self, cvr_collection_data: Dict[str, Any]) -> None:
        """Save CVR collection summary data."""
        summary_path = f"gold/{self.config.dataset}/{self.date_pattern}/collection_summary.json"

        self.gcs_access.upload_json(
            data=cvr_collection_data["collection_summary"],
            gcs_path=f"gs://{self.config.bucket}/{summary_path}",
        )

        self.log.info(f"Saved CVR collection summary to {summary_path}")

    def _save_raw_responses(self, enrichment_data: Dict[str, Any]) -> None:
        """Save raw API responses for debugging."""
        raw_path = f"gold/{self.config.dataset}/{self.date_pattern}/raw_responses.json"

        self.gcs_access.upload_json(
            data=enrichment_data, gcs_path=f"gs://{self.config.bucket}/{raw_path}"
        )

        self.log.info(f"Saved raw CVR API responses to {raw_path}")

    def _save_summary_data(self, processed_data: Dict[str, Any]) -> None:
        """Save processing summary data."""
        summary_path = f"gold/{self.config.dataset}/{self.date_pattern}/processing_summary.json"

        summary_data = {
            "summary": processed_data["summary"],
            "collection_summary": processed_data["collection_summary"],
            "api_summary": processed_data["api_summary"],
        }

        self.gcs_access.upload_json(
            data=summary_data, gcs_path=f"gs://{self.config.bucket}/{summary_path}"
        )

        self.log.info(f"Saved processing summary to {summary_path}")

    def _is_valid_cvr_format(self, cvr_number: str) -> bool:
        """
        Validate CVR number format.

        Danish CVR numbers should be exactly 8 digits.

        Args:
            cvr_number: CVR number to validate

        Returns:
            True if valid format, False otherwise
        """
        if not cvr_number or not isinstance(cvr_number, str):
            return False

        # Remove any whitespace
        cvr_clean = cvr_number.strip()

        # Check if exactly 8 digits
        if len(cvr_clean) != 8:
            return False

        # Check if all characters are digits
        if not cvr_clean.isdigit():
            return False

        # Additional validation: CVR numbers shouldn't start with 0
        if cvr_clean.startswith("0"):
            return False

        return True

    def _download_financial_xml_documents(self, financial_docs: list) -> list:
        """
        Download XML financial documents for analysis.
        
        Args:
            financial_docs: List of financial document metadata
            
        Returns:
            List of financial docs with downloaded XML content added
        """
        enriched_docs = []
        
        for doc in financial_docs:
            try:
                # Get the first document URL if available
                if doc.get("documents") and len(doc["documents"]) > 0:
                    document_url = doc["documents"][0].get("document_url")
                    
                    if document_url and document_url.endswith('.xml'):
                        self.log.debug(f"Downloading XML from: {document_url}")
                        
                        # Download XML content
                        response = requests.get(document_url, timeout=30)
                        response.raise_for_status()
                        
                        # Add XML content to document
                        doc_copy = doc.copy()
                        doc_copy["xml_content"] = response.text
                        doc_copy["xml_size_bytes"] = len(response.text)
                        doc_copy["download_success"] = True
                        
                        # Parse financial metrics from XML
                        financial_metrics = self._parse_xbrl_financial_data(response.text)
                        doc_copy["financial_metrics"] = financial_metrics
                        
                        enriched_docs.append(doc_copy)
                    else:
                        # Non-XML or no URL - keep original
                        doc_copy = doc.copy()
                        doc_copy["download_success"] = False
                        doc_copy["download_reason"] = "No XML URL found"
                        enriched_docs.append(doc_copy)
                else:
                    # No documents array
                    doc_copy = doc.copy()
                    doc_copy["download_success"] = False
                    doc_copy["download_reason"] = "No documents array"
                    enriched_docs.append(doc_copy)
                    
            except Exception as e:
                self.log.warning(f"Failed to download XML document: {e}")
                doc_copy = doc.copy()
                doc_copy["download_success"] = False
                doc_copy["download_error"] = str(e)
                enriched_docs.append(doc_copy)
        
        return enriched_docs

    def _parse_xbrl_financial_data(self, xml_content: str) -> dict:
        """
        Parse XBRL financial data from XML content.
        
        Args:
            xml_content: Raw XML content from financial document
            
        Returns:
            Dictionary with parsed financial metrics for current and previous year
        """
        try:
            root = ET.fromstring(xml_content)
            
            # Parse context definitions to understand periods and types
            contexts = {}
            for elem in root.iter():
                if 'context' in elem.tag.lower() and elem.get('id'):
                    context_id = elem.get('id')
                    contexts[context_id] = {
                        'type': None,  # 'duration' or 'instant'
                        'start_date': None,
                        'end_date': None,
                        'instant_date': None,
                        'entity_id': None
                    }
                    
                    # Parse context details
                    for child in elem:
                        if 'entity' in child.tag.lower():
                            for gc in child:
                                if 'identifier' in gc.tag.lower():
                                    contexts[context_id]['entity_id'] = gc.text
                        elif 'period' in child.tag.lower():
                            for gc in child:
                                gc_tag = gc.tag.split('}')[-1] if '}' in gc.tag else gc.tag
                                if gc_tag == 'startDate':
                                    contexts[context_id]['start_date'] = gc.text
                                    contexts[context_id]['type'] = 'duration'
                                elif gc_tag == 'endDate':
                                    contexts[context_id]['end_date'] = gc.text
                                elif gc_tag == 'instant':
                                    contexts[context_id]['instant_date'] = gc.text
                                    contexts[context_id]['type'] = 'instant'
            
            # Also extract reporting period info from elements
            reporting_periods = {}
            for elem in root.iter():
                tag_name = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
                context_ref = elem.get('contextRef')
                
                if tag_name == 'ReportingPeriodStartDate' and context_ref and elem.text:
                    if context_ref not in reporting_periods:
                        reporting_periods[context_ref] = {}
                    reporting_periods[context_ref]['start_date'] = elem.text
                elif tag_name == 'ReportingPeriodEndDate' and context_ref and elem.text:
                    if context_ref not in reporting_periods:
                        reporting_periods[context_ref] = {}
                    reporting_periods[context_ref]['end_date'] = elem.text
            
            # Find current duration context (for income statement items)
            current_duration_context = None
            current_instant_context = None
            
            # Look for the most recent duration context
            duration_contexts = {k: v for k, v in contexts.items() if v['type'] == 'duration'}
            if duration_contexts:
                sorted_duration = sorted(duration_contexts.items(), 
                                       key=lambda x: x[1].get('end_date', ''), reverse=True)
                current_duration_context = sorted_duration[0][0]
            
            # Look for the most recent instant context  
            instant_contexts = {k: v for k, v in contexts.items() if v['type'] == 'instant'}
            if instant_contexts:
                sorted_instant = sorted(instant_contexts.items(),
                                      key=lambda x: x[1].get('instant_date', ''), reverse=True)
                current_instant_context = sorted_instant[0][0]
            
            # Fallbacks
            if not current_duration_context:
                current_duration_context = 'c1'
            if not current_instant_context:
                current_instant_context = 'c4' if 'c4' in contexts else current_duration_context
            
            # Key financial elements to extract (Danish XBRL taxonomy)
            # Based on analysis of 10 XML documents across 5 companies
            # Separated by context type: duration (income statement) vs instant (balance sheet)
            
            duration_elements = {
                # Income Statement items (use duration context)
                'net_profit_loss': 'ProfitLoss',
                'gross_profit_loss': 'GrossProfitLoss', 
                'operating_profit_loss': 'ProfitLossFromOrdinaryOperatingActivities',
                'profit_loss_before_tax': 'ProfitLossFromOrdinaryActivitiesBeforeTax',
                'revenue': 'Revenue',
                'gross_result': 'GrossResult',
                'employee_benefits_expense': 'EmployeeBenefitsExpense',
                'average_number_of_employees': 'AverageNumberOfEmployees',  # IMPORTANT!
                'depreciation_expense': 'DepreciationAmortisationExpenseAndImpairmentLossesOfPropertyPlantAndEquipmentAndIntangibleAssetsRecognisedInProfitOrLoss',
                'other_finance_income': 'OtherFinanceIncome',
                'other_finance_expenses': 'OtherFinanceExpenses', 
                'tax_expense': 'TaxExpense',
            }
            
            instant_elements = {
                # Balance Sheet items (use instant context)
                'total_assets': 'Assets',
                'total_equity': 'Equity',
                'noncurrent_assets': 'NoncurrentAssets',
                'current_assets': 'CurrentAssets', 
                'liabilities_and_equity': 'LiabilitiesAndEquity',
                'contributed_capital': 'ContributedCapital',
                'cash_and_cash_equivalents': 'CashAndCashEquivalents',
                'deferred_income_assets': 'DeferredIncomeAssets',
                'recognised_not_owned_assets': 'RecognisedButNotOwnedAssets',
                'liabilities_other_than_provisions': 'LiabilitiesOtherThanProvisions',
                'shortterm_liabilities_other_than_provisions': 'ShorttermLiabilitiesOtherThanProvisions',
                'longterm_liabilities_other_than_provisions': 'LongtermLiabilitiesOtherThanProvisions',
                'shortterm_debt_to_banks': 'ShorttermDebtToBanks',
                'shortterm_part_of_longterm_liabilities': 'ShorttermPartOfLongtermLiabilitiesOtherThanProvisions',
                'other_payables_including_tax': 'OtherPayablesIncludingTaxPayablesLiabilitiesOtherThanProvisionsShortterm',
                'provisions': 'Provisions',
                'provisions_for_deferred_tax': 'ProvisionsForDeferredTax',
                'property_plant_equipment': 'PropertyPlantAndEquipment',
                'longterm_debt_to_credit_institutions': 'LongtermDebtToOtherCreditInstitutions',
            }
            
            # Extract period information for the data
            duration_period = contexts.get(current_duration_context, {})
            instant_period = contexts.get(current_instant_context, {})
            
            # Extract financial metrics for current period
            financial_metrics = {
                # Context information
                'duration_context': current_duration_context,
                'instant_context': current_instant_context,
                
                # Period dates (for income statement)
                'income_statement_start_date': duration_period.get('start_date'),
                'income_statement_end_date': duration_period.get('end_date'),
                
                # Balance sheet date
                'balance_sheet_date': instant_period.get('instant_date'),
                
                # Entity verification
                'entity_id_duration': duration_period.get('entity_id'),
                'entity_id_instant': instant_period.get('entity_id'),
            }
            
            # Extract duration elements (income statement) using duration context
            for metric_name, element_name in duration_elements.items():
                for elem in root.iter():
                    tag_name = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
                    
                    if (tag_name == element_name and 
                        elem.get('contextRef') == current_duration_context and 
                        elem.text and elem.text.strip()):
                        
                        try:
                            # Parse as number
                            financial_metrics[metric_name] = float(elem.text.strip())
                        except ValueError:
                            # Keep as string if not numeric
                            financial_metrics[metric_name] = elem.text.strip()
                        break
            
            # Extract instant elements (balance sheet) using instant context
            for metric_name, element_name in instant_elements.items():
                for elem in root.iter():
                    tag_name = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
                    
                    if (tag_name == element_name and 
                        elem.get('contextRef') == current_instant_context and 
                        elem.text and elem.text.strip()):
                        
                        try:
                            # Parse as number  
                            financial_metrics[metric_name] = float(elem.text.strip())
                        except ValueError:
                            # Keep as string if not numeric
                            financial_metrics[metric_name] = elem.text.strip()
                        break
            
            return financial_metrics
            
        except Exception as e:
            self.log.warning(f"Failed to parse XBRL data: {e}")
            return {'parse_error': str(e)}

    def _initialize_empty_tables(self, table_name: str) -> None:
        """Initialize all normalized tables as empty with correct schema."""
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
                datavask_enriched BOOLEAN,
                dawa_fetch_timestamp VARCHAR
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
        
        # Financial table
        self.conn.execute(f"""
            CREATE TABLE {table_name}_financial (
                company_uuid VARCHAR,
                cvr_number INTEGER,
                publication_type VARCHAR,
                publication_time VARCHAR,
                case_number VARCHAR,
                reporting_period_start VARCHAR,
                reporting_period_end VARCHAR,
                document_count INTEGER,
                xml_size_bytes INTEGER,
                download_success BOOLEAN,
                duration_context VARCHAR,
                instant_context VARCHAR,
                income_statement_start_date VARCHAR,
                income_statement_end_date VARCHAR,
                balance_sheet_date VARCHAR,
                net_profit_loss DOUBLE,
                gross_profit_loss DOUBLE,
                operating_profit_loss DOUBLE,
                profit_loss_before_tax DOUBLE,
                employee_benefits_expense DOUBLE,
                average_number_of_employees DOUBLE,
                depreciation_expense DOUBLE,
                other_finance_income DOUBLE,
                other_finance_expenses DOUBLE,
                tax_expense DOUBLE,
                total_assets DOUBLE,
                total_equity DOUBLE,
                noncurrent_assets DOUBLE,
                current_assets DOUBLE,
                cash_and_cash_equivalents DOUBLE,
                liabilities_other_than_provisions DOUBLE,
                shortterm_liabilities_other_than_provisions DOUBLE,
                longterm_liabilities_other_than_provisions DOUBLE,
                provisions DOUBLE,
                property_plant_equipment DOUBLE,
                contributed_capital DOUBLE,
                equity_ratio DOUBLE,
                profit_per_employee DOUBLE,
                return_on_assets DOUBLE
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
        
        # Employment tables
        employment_types = ['annual', 'quarterly', 'monthly', 'replacement_monthly']
        for table_suffix in employment_types:
            self.conn.execute(f"""
                CREATE TABLE {table_name}_employment_{table_suffix} (
                    company_uuid VARCHAR,
                    cvr_number INTEGER,
                    employment_data JSON
                )
            """)

    def _process_companies_chunk(self, chunk_companies: list, table_name: str, chunk_idx: int) -> None:
        """Process a chunk of companies and append to existing tables."""
        import json
        
        json_strings = [json.dumps(company) for company in chunk_companies]
        
        # Insert into main companies table
        self.conn.execute(f"""
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
        """, [json_strings])
        
        # Process addresses for this chunk
        self._process_addresses_chunk(json_strings, table_name)
        
        # Process leadership for this chunk
        self._process_leadership_chunk(json_strings, table_name)
        
        # Process financial documents for this chunk
        self._process_financial_chunk(json_strings, table_name)
        
        # Process industries for this chunk
        self._process_industries_chunk(json_strings, table_name)
        
        # Process employment data for this chunk
        self._process_employment_chunk(json_strings, table_name)

    def _process_addresses_chunk(self, json_strings: list, table_name: str) -> None:
        """Process addresses for a chunk of companies."""
        # Check if any companies have addresses
        addresses_check = self.conn.execute("""
            SELECT COUNT(*)
            FROM unnest($1) as t(json_data)
            WHERE json_extract(json_data, '$.addresses') IS NOT NULL
            AND json_array_length(json_extract(json_data, '$.addresses')) > 0
        """, [json_strings]).fetchone()[0]
        
        if addresses_check > 0:
            # Get schema for addresses to handle them properly
            addresses_schema = self.conn.execute("""
                WITH addresses_sample AS (
                    SELECT json_extract(json_data, '$.addresses') as addresses_json
                    FROM unnest($1) as t(json_data)
                    WHERE json_extract(json_data, '$.addresses') IS NOT NULL
                    AND json_array_length(json_extract(json_data, '$.addresses')) > 0
                    LIMIT 1
                )
                SELECT json_structure(addresses_json) FROM addresses_sample
            """, [json_strings]).fetchone()
            
            if addresses_schema and addresses_schema[0]:
                self.conn.execute(f"""
                    INSERT INTO {table_name}_addresses
                    WITH addresses_flattened AS (
                        SELECT
                            json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                            unnest(json_transform(json_extract(json_data, '$.addresses')::JSON, $2)) as address_parsed
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
                        TRY(address_parsed.datavask_enriched::BOOLEAN) as datavask_enriched,
                        TRY(address_parsed.dawa_fetch_timestamp) as dawa_fetch_timestamp
                    FROM addresses_flattened
                """, [json_strings, addresses_schema[0]])
            
            self.log.info(f"Processed addresses for chunk")

    def _process_leadership_chunk(self, json_strings: list, table_name: str) -> None:
        """Process leadership data for a chunk of companies."""
        leadership_check = self.conn.execute("""
            SELECT COUNT(*)
            FROM unnest($1) as t(json_data)
            WHERE json_extract(json_data, '$.leadership') IS NOT NULL
            AND json_array_length(json_extract(json_data, '$.leadership')) > 0
        """, [json_strings]).fetchone()[0]
        
        if leadership_check > 0:
            leadership_schema = self.conn.execute("""
                WITH leadership_sample AS (
                    SELECT json_extract(json_data, '$.leadership') as leadership_json
                    FROM unnest($1) as t(json_data)
                    WHERE json_extract(json_data, '$.leadership') IS NOT NULL
                    AND json_array_length(json_extract(json_data, '$.leadership')) > 0
                    LIMIT 1
                )
                SELECT json_structure(leadership_json) FROM leadership_sample
            """, [json_strings]).fetchone()
            
            if leadership_schema and leadership_schema[0]:
                self.conn.execute(f"""
                    INSERT INTO {table_name}_leadership
                    SELECT 
                        company_uuid(json_extract(json_data, '$.cvr_number')::INTEGER) as company_uuid,
                        json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                        unnest(json_transform(json_extract(json_data, '$.leadership'), $2)) as leadership_data
                    FROM unnest($1) as t(json_data)
                    WHERE json_extract(json_data, '$.leadership') IS NOT NULL
                    AND json_array_length(json_extract(json_data, '$.leadership')) > 0
                """, [json_strings, leadership_schema[0]])

    def _process_financial_chunk(self, json_strings: list, table_name: str) -> None:
        """Process financial documents for a chunk of companies."""
        financial_check = self.conn.execute("""
            SELECT COUNT(*)
            FROM unnest($1) as t(json_data)
            WHERE json_extract(json_data, '$.financial_documents') IS NOT NULL
            AND json_array_length(json_extract(json_data, '$.financial_documents')) > 0
        """, [json_strings]).fetchone()[0]
        
        if financial_check > 0:
            financial_schema = self.conn.execute("""
                WITH financial_sample AS (
                    SELECT json_extract(json_data, '$.financial_documents') as financial_json
                    FROM unnest($1) as t(json_data)
                    WHERE json_extract(json_data, '$.financial_documents') IS NOT NULL
                    AND json_array_length(json_extract(json_data, '$.financial_documents')) > 0
                    LIMIT 1
                )
                SELECT json_structure(financial_json) FROM financial_sample
            """, [json_strings]).fetchone()
            
            if financial_schema and financial_schema[0]:
                # Get the actual schema structure to check what fields exist
                schema_str = financial_schema[0]
                self.log.debug(f"Financial schema structure: {schema_str}")
                
                # Build dynamic field list based on what exists in the schema
                financial_metrics_fields = []
                
                # Define all possible financial metrics fields and their fallback values
                possible_fields = {
                    'duration_context': 'NULL',
                    'instant_context': 'NULL', 
                    'income_statement_start_date': 'NULL',
                    'income_statement_end_date': 'NULL',
                    'balance_sheet_date': 'NULL',
                    'net_profit_loss': 'NULL',
                    'gross_profit_loss': 'NULL',
                    'operating_profit_loss': 'NULL',
                    'profit_loss_before_tax': 'NULL',
                    'employee_benefits_expense': 'NULL',
                    'average_number_of_employees': 'NULL',
                    'depreciation_expense': 'NULL',
                    'other_finance_income': 'NULL',
                    'other_finance_expenses': 'NULL',
                    'tax_expense': 'NULL',
                    'total_assets': 'NULL',
                    'total_equity': 'NULL',
                    'noncurrent_assets': 'NULL',
                    'current_assets': 'NULL',
                    'cash_and_cash_equivalents': 'NULL',
                    'liabilities_other_than_provisions': 'NULL',
                    'shortterm_liabilities_other_than_provisions': 'NULL',
                    'longterm_liabilities_other_than_provisions': 'NULL',
                    'provisions': 'NULL',
                    'property_plant_equipment': 'NULL',
                    'contributed_capital': 'NULL'
                }
                
                # Check which fields exist in the schema
                available_fields = set()
                for field, fallback in possible_fields.items():
                    if f'financial_metrics.{field}' in schema_str or f'"{field}"' in schema_str:
                        financial_metrics_fields.append(f"TRY(financial_parsed.financial_metrics.{field}) as {field}")
                        available_fields.add(field)
                    else:
                        financial_metrics_fields.append(f"{fallback} as {field}")
                        self.log.debug(f"Field 'financial_metrics.{field}' not found in schema, using {fallback}")
                
                # Build calculated fields based on available fields
                calculated_fields = []
                
                # Equity ratio calculation
                if 'total_assets' in available_fields and 'total_equity' in available_fields:
                    calculated_fields.append("""
                        CASE 
                            WHEN TRY(financial_parsed.financial_metrics.total_assets) > 0 
                            THEN TRY(financial_parsed.financial_metrics.total_equity) / TRY(financial_parsed.financial_metrics.total_assets) 
                            ELSE NULL 
                        END as equity_ratio""")
                else:
                    calculated_fields.append("NULL as equity_ratio")
                
                # Profit per employee calculation
                if 'average_number_of_employees' in available_fields and 'net_profit_loss' in available_fields:
                    calculated_fields.append("""
                        CASE 
                            WHEN TRY(financial_parsed.financial_metrics.average_number_of_employees) > 0 
                            THEN TRY(financial_parsed.financial_metrics.net_profit_loss) / TRY(financial_parsed.financial_metrics.average_number_of_employees) 
                            ELSE NULL 
                        END as profit_per_employee""")
                else:
                    calculated_fields.append("NULL as profit_per_employee")
                
                # Return on assets calculation
                if 'total_assets' in available_fields and 'net_profit_loss' in available_fields:
                    calculated_fields.append("""
                        CASE 
                            WHEN TRY(financial_parsed.financial_metrics.total_assets) > 0 
                            THEN TRY(financial_parsed.financial_metrics.net_profit_loss) / TRY(financial_parsed.financial_metrics.total_assets) 
                            ELSE NULL 
                        END as return_on_assets""")
                else:
                    calculated_fields.append("NULL as return_on_assets")
                
                # Join all field expressions
                fields_sql = ',\n                        '.join(financial_metrics_fields)
                calculated_fields_sql = ',\n                        '.join(calculated_fields)
                
                self.conn.execute(f"""
                    INSERT INTO {table_name}_financial
                    WITH financial_flattened AS (
                        SELECT
                            json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                            unnest(json_transform(json_extract(json_data, '$.financial_documents'), $2)) as financial_parsed
                        FROM unnest($1) as t(json_data)
                        WHERE json_extract(json_data, '$.financial_documents') IS NOT NULL
                        AND json_array_length(json_extract(json_data, '$.financial_documents')) > 0
                    )
                    SELECT 
                        company_uuid(cvr_number) as company_uuid,
                        cvr_number,
                        financial_parsed.publication_type,
                        financial_parsed.publication_time,
                        financial_parsed.case_number,
                        TRY(financial_parsed.reporting_period.start_date) as reporting_period_start,
                        TRY(financial_parsed.reporting_period.end_date) as reporting_period_end,
                        financial_parsed.document_count,
                        financial_parsed.xml_size_bytes,
                        financial_parsed.download_success,
                        {fields_sql},
                        {calculated_fields_sql}
                    FROM financial_flattened
                """, [json_strings, financial_schema[0]])

    def _process_industries_chunk(self, json_strings: list, table_name: str) -> None:
        """Process industries for a chunk of companies."""
        industry_check = self.conn.execute("""
            SELECT COUNT(*)
            FROM unnest($1) as t(json_data)
            WHERE json_extract(json_data, '$.industries') IS NOT NULL
            AND json_array_length(json_extract(json_data, '$.industries')) > 0
        """, [json_strings]).fetchone()[0]
        
        if industry_check > 0:
            industry_schema = self.conn.execute("""
                WITH industry_sample AS (
                    SELECT json_extract(json_data, '$.industries') as industry_json
                    FROM unnest($1) as t(json_data)
                    WHERE json_extract(json_data, '$.industries') IS NOT NULL
                    AND json_array_length(json_extract(json_data, '$.industries')) > 0
                    LIMIT 1
                )
                SELECT json_structure(industry_json) FROM industry_sample
            """, [json_strings]).fetchone()
            
            if industry_schema and industry_schema[0]:
                self.conn.execute(f"""
                    INSERT INTO {table_name}_industries
                    SELECT 
                        company_uuid(json_extract(json_data, '$.cvr_number')::INTEGER) as company_uuid,
                        json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                        unnest(json_transform(json_extract(json_data, '$.industries'), $2)) as industry_data
                    FROM unnest($1) as t(json_data)
                    WHERE json_extract(json_data, '$.industries') IS NOT NULL
                    AND json_array_length(json_extract(json_data, '$.industries')) > 0
                """, [json_strings, industry_schema[0]])

    def _process_employment_chunk(self, json_strings: list, table_name: str) -> None:
        """Process employment data for a chunk of companies."""
        employment_types = [
            ('annual_employment', 'annual'),
            ('quarterly_employment', 'quarterly'), 
            ('monthly_employment', 'monthly'),
            ('replacement_monthly_employment', 'replacement_monthly')
        ]
        
        for employment_field, table_suffix in employment_types:
            employment_check = self.conn.execute("""
                SELECT COUNT(*)
                FROM unnest($1) as t(json_data)
                WHERE json_extract(json_data, '$.employment_data.' || $2) IS NOT NULL
                AND json_array_length(json_extract(json_data, '$.employment_data.' || $2)) > 0
            """, [json_strings, employment_field]).fetchone()[0]
            
            if employment_check > 0:
                employment_schema = self.conn.execute("""
                    WITH employment_sample AS (
                        SELECT json_extract(json_data, '$.employment_data.' || $2) as employment_json
                        FROM unnest($1) as t(json_data)
                        WHERE json_extract(json_data, '$.employment_data.' || $2) IS NOT NULL
                        AND json_array_length(json_extract(json_data, '$.employment_data.' || $2)) > 0
                        LIMIT 1
                    )
                    SELECT json_structure(employment_json) FROM employment_sample
                """, [json_strings, employment_field]).fetchone()
                
                if employment_schema and employment_schema[0]:
                    self.conn.execute(f"""
                        INSERT INTO {table_name}_employment_{table_suffix}
                        SELECT
                            company_uuid(json_extract(json_data, '$.cvr_number')::INTEGER) as company_uuid,
                            json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                            unnest(json_transform(json_extract(json_data, '$.employment_data.{employment_field}'), $2)) as employment_data
                        FROM unnest($1) as t(json_data)
                        WHERE json_extract(json_data, '$.employment_data.{employment_field}') IS NOT NULL
                        AND json_array_length(json_extract(json_data, '$.employment_data.{employment_field}')) > 0
                    """, [json_strings, employment_schema[0]])

    def _cleanup_memory_after_chunk(self) -> None:
        """
        Aggressive memory cleanup after processing a chunk.
        
        Based on DuckDB recommendations for memory-constrained environments.
        """
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
                self.conn.execute("PRAGMA cache_size = -2000")  # Reset to reasonable cache
                
                # Force temporary directory cleanup
                self.conn.execute("PRAGMA temp_store = memory")
                
            except Exception as pragma_e:
                self.log.debug(f"Pragma cleanup warning: {pragma_e}")
                
            self.log.debug(f"Memory cleanup: collected {collected} objects, checkpoint completed")
            
        except Exception as e:
            self.log.debug(f"Memory cleanup warning: {e}")

    def _log_final_table_sizes(self, table_name: str) -> None:
        """Log final table sizes after all chunks are processed."""
        try:
            # Get counts for all tables
            main_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            addresses_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}_addresses").fetchone()[0]
            leadership_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}_leadership").fetchone()[0]
            financial_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}_financial").fetchone()[0]
            industries_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}_industries").fetchone()[0]
            
            # Employment counts
            employment_types = ['annual', 'quarterly', 'monthly', 'replacement_monthly']
            employment_counts = {}
            for table_suffix in employment_types:
                employment_counts[table_suffix] = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}_employment_{table_suffix}").fetchone()[0]
            
            # Geocoded addresses count
            geocoded_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}_addresses WHERE dawa_enriched = true").fetchone()[0]
            
            # Sample results
            sample_results = self.conn.execute(f"""
                SELECT cvr_number, company_name, company_type_description, founded_date, 
                       source_pipelines, financial_document_count
                FROM {table_name} 
                LIMIT 5
            """).fetchall()
            
            self.log.info(f"🎉 Successfully created normalized CVR tables using chunked processing!")
            self.log.info(f"   📋 Companies: {main_count}")
            self.log.info(f"   👥 Leadership entries: {leadership_count}")
            self.log.info(f"   💰 Financial documents: {financial_count}")
            self.log.info(f"   📍 Address entries: {addresses_count} ({geocoded_count} geocoded)")
            self.log.info(f"   🏭 Industry entries: {industries_count}")
            self.log.info(f"   👷 Employment data:")
            for table_suffix, count in employment_counts.items():
                self.log.info(f"      📈 {table_suffix.replace('_', ' ').title()}: {count} records")
            
            for row in sample_results:
                self.log.info(f"   📋 CVR: {row[0]} | Name: {row[1]} | Type: {row[2]} | Founded: {row[3]} | Sources: {row[4]} | Fin.Docs: {row[5]}")
                
        except Exception as e:
            self.log.warning(f"Could not log final table sizes: {e}")

    def _create_empty_tables(self, table_name: str) -> None:
        """Create empty tables with schema when no company data is available."""
        self.conn.execute(f"""
            CREATE TABLE {table_name} (
                company_uuid VARCHAR,
                cvr_number INTEGER,
                company_name VARCHAR,
                company_type_description VARCHAR,
                status VARCHAR,
                founded_date VARCHAR,
                dissolution_date VARCHAR,
                data_source VARCHAR,
                fetch_timestamp VARCHAR,
                source_pipelines VARCHAR[],
                source_pipeline_count INTEGER,
                financial_document_count INTEGER,
                processing_timestamp VARCHAR,
                pipeline_run_id VARCHAR
            )
        """)
