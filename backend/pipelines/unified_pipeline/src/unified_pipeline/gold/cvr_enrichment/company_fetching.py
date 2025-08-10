"""
Company Fetching Step - Step 2 of CVR Enrichment Pipeline

This step fetches comprehensive company data from the CVR register for
the collected CVR numbers, processing them in batches for parallel execution.
"""

import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import Field

from unified_pipeline.common.base import BaseJobConfig, BaseSource, GoldJobInterface
from unified_pipeline.util.cvr_api_client import CVRAPIClient
from unified_pipeline.util.timing import timed
from .shared.config import CVREnrichmentSharedConfig, CVREnrichmentStep, get_step_input_paths


class CompanyFetchingConfig(BaseJobConfig):
    """Configuration for company fetching step."""
    
    name: str = "Company Data Fetching"
    dataset: str = "cvr_enrichment_companies"
    type: str = "cvr_api"
    description: str = "Fetch comprehensive company data from CVR register"
    frequency: str = "monthly"
    bucket: str = "landbrugsdata-raw-data"
    
    # Shared configuration
    shared_config: CVREnrichmentSharedConfig = Field(
        default_factory=CVREnrichmentSharedConfig,
        description="Shared configuration for CVR enrichment pipeline"
    )
    
    # Company fetching specific configuration
    batch_number: Optional[int] = Field(
        default=None,
        description="Batch number for parallel processing (1-based)"
    )
    
    total_batches: Optional[int] = Field(
        default=None,
        description="Total number of batches in this step"
    )
    
    fetch_all_fields: bool = Field(
        default=True,
        description="Whether to fetch all available fields from CVR API"
    )
    
    # Note: Address geocoding is handled in separate step
    enable_address_geocoding: bool = Field(
        default=False,
        description="Whether to enrich addresses with geometry (handled in separate step)"
    )
    
    model_config = {"frozen": True}
    
    def apply_cli_filters(self, cli_config):
        """Apply CLI configuration filters to this config."""
        if cli_config.batch_number is not None:
            object.__setattr__(self, 'batch_number', cli_config.batch_number)
        if cli_config.total_batches is not None:
            object.__setattr__(self, 'total_batches', cli_config.total_batches)
        if cli_config.test_limit is not None:
            object.__setattr__(self, 'shared_config', 
                self.shared_config.model_copy(update={'test_limit': cli_config.test_limit}))


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
        
        # Initialize CVR API client
        cvr_username = os.getenv("CVR_USERNAME", "Martin_Collignon_CVR_I_SKYEN")
        cvr_password = os.getenv("CVR_PASSWORD", "3a37d029-9588-4c00-8a09-3d2901452d45")
        
        self.cvr_api_client = CVRAPIClient(
            username=cvr_username,
            password=cvr_password,
            enable_geocoding=self.config.enable_address_geocoding,
            geocode_current_only=self.config.shared_config.geocoding_current_addresses_only
        )
        
        self.log.info("Company fetching step initialized")
        self.log.info(f"📋 Configuration:")
        self.log.info(f"   • Batch: {self.config.batch_number}/{self.config.total_batches}")
        self.log.info(f"   • Fetch all fields: {self.config.fetch_all_fields}")
        self.log.info(f"   • Address geocoding: {'enabled' if self.config.enable_address_geocoding else 'disabled (separate step)'}")
    
    @timed(name="Company fetching processing")
    async def run(self, silver_data: Optional[Dict[str, Any]] = None) -> str:
        """
        Run the company fetching process.
        
        Args:
            silver_data: Optional silver data (not used in this step)
            
        Returns:
            Table name containing company data
        """
        self.log.info("Starting company fetching step")
        
        try:
            # Step 1: Load CVR numbers from collection step
            self.log.info("📋 Step 1/4: Loading CVR batch for processing")
            cvr_batch = self._load_cvr_batch()
            
            # Step 2: Fetch company data from CVR register
            self.log.info("🌐 Step 2/4: Fetching company data from CVR register API")
            company_data = await self._fetch_company_data(cvr_batch)
            
            # Step 3: Process and structure company data
            self.log.info("⚙️ Step 3/4: Processing and structuring company data")
            processed_data = self._process_company_data(company_data)
            
            # Step 4: Save company data
            self.log.info("💾 Step 4/4: Saving company data")
            table_name = self._save_company_data(processed_data)
            
            # Success summary
            total_requested = len(cvr_batch)
            total_successful = company_data.get('summary', {}).get('successful', 0)
            total_failed = company_data.get('summary', {}).get('failed', 0)
            api_calls = company_data.get('summary', {}).get('api_calls', 0)
            efficiency_gain = company_data.get('summary', {}).get('efficiency_gain', 'N/A')
            
            self.log.info("=" * 60)
            self.log.info("✅ COMPANY FETCHING COMPLETED SUCCESSFULLY")
            self.log.info("=" * 60)
            self.log.info(f"📊 BATCH {self.config.batch_number}/{self.config.total_batches} SUMMARY:")
            self.log.info(f"   • CVR numbers requested: {total_requested:,}")
            self.log.info(f"   • Companies found: {total_successful:,}")
            self.log.info(f"   • Companies not found: {total_failed:,}")
            self.log.info(f"   • Success rate: {(total_successful/total_requested*100):.1f}%" if total_requested > 0 else "   • Success rate: N/A")
            self.log.info(f"   • API calls made: {api_calls}")
            self.log.info(f"   • Efficiency gain: {efficiency_gain}")
            self.log.info(f"   • Output table: {table_name}")
            self.log.info(f"   • Ready for next step: P-Number Fetching")
            self.log.info("=" * 60)
            
            return table_name
            
        except Exception as e:
            self.log.error("=" * 60)
            self.log.error("❌ COMPANY FETCHING FAILED")
            self.log.error("=" * 60)
            self.log.error(f"💥 Error: {e}")
            self.log.error(f"🔍 Check the logs above for detailed error information")
            self.log.error(f"📋 Batch: {self.config.batch_number}/{self.config.total_batches}")
            self.log.error("=" * 60)
            raise
    
    @timed(name="Loading CVR batch")
    def _load_cvr_batch(self) -> List[str]:
        """
        Load CVR numbers for this batch from collection step output.
        
        Returns:
            List of CVR numbers to process in this batch
        """
        self.log.info("Loading CVR numbers from collection step")
        
        # Get input paths from collection step
        input_paths = get_step_input_paths(
            CVREnrichmentStep.COMPANY_FETCHING,
            self.date_pattern,
            bucket=self.config.bucket
        )
        
        if not input_paths:
            raise ValueError("No input paths found for company fetching step")
        
        collection_path = input_paths[0]  # Should be collection.parquet
        
        # Load collection data
        self.log.info(f"Loading collection data from: {collection_path}")
        
        try:
            # Download and load the collection data
            local_path = self.gcs_access.download_file(collection_path, "/tmp/collection.parquet")
            
            # Load CVR numbers from the collection
            result = self.conn.execute("""
                SELECT cvr_number, collection_metadata
                FROM read_parquet(?)
                ORDER BY cvr_number
            """, [local_path]).fetchall()
            
            all_cvrs = [row[0] for row in result]
            
            # Get batch details if this is a batch job
            if self.config.batch_number and self.config.total_batches:
                batch_details = self._load_batch_details()
                
                if self.config.batch_number <= len(batch_details["processing_batches"]):
                    cvr_batch = batch_details["processing_batches"][self.config.batch_number - 1]
                    self.log.info(
                        f"Loaded batch {self.config.batch_number}/{self.config.total_batches}: "
                        f"{len(cvr_batch)} CVR numbers"
                    )
                else:
                    raise ValueError(
                        f"Batch number {self.config.batch_number} exceeds available batches "
                        f"({len(batch_details['processing_batches'])})"
                    )
            else:
                # Process all CVRs if not batched
                cvr_batch = all_cvrs
                self.log.info(f"Loaded {len(cvr_batch)} CVR numbers (no batching)")
            
            return cvr_batch
            
        except Exception as e:
            self.log.error(f"Failed to load CVR batch: {e}")
            raise
    
    def _load_batch_details(self) -> Dict[str, Any]:
        """Load batch details from collection step."""
        batch_details_path = f"gs://{self.config.bucket}/gold/cvr_enrichment_collection/{self.date_pattern}/batch_details.json"
        
        try:
            batch_details = self.gcs_access.download_json(batch_details_path)
            return batch_details
        except Exception as e:
            self.log.error(f"Failed to load batch details from {batch_details_path}: {e}")
            raise
    
    @timed(name="Fetching company data")
    async def _fetch_company_data(self, cvr_batch: List[str]) -> Dict[str, Any]:
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
                "fetch_timestamp": datetime.now().isoformat()
            }
        
        # Fetch company data using CVR API client with batch optimization
        company_results = self.cvr_api_client.fetch_multiple_companies(
            cvr_numbers=cvr_batch,
            fetch_all_fields=self.config.fetch_all_fields,
            enrich_with_geometry=self.config.enable_address_geocoding,
            batch_size=self.config.shared_config.api_batch_size
        )
        
        self.log.info(
            f"Company data fetch completed: "
            f"{company_results['summary']['successful']} successful, "
            f"{company_results['summary']['failed']} failed"
        )
        
        return company_results
    
    @timed(name="Processing company data")
    def _process_company_data(self, company_data: Dict[str, Any]) -> Dict[str, Any]:
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
        
        for cvr_number, company_info in company_results.items():
            if company_info:
                # Add processing metadata
                company_info["processing_timestamp"] = datetime.now().isoformat()
                company_info["pipeline_run_id"] = self.date_pattern
                company_info["processing_step"] = CVREnrichmentStep.COMPANY_FETCHING.value
                company_info["batch_number"] = self.config.batch_number
                
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
            "companies_with_pnumbers": len([c for c in processed_companies if c.get("pnumber_count", 0) > 0]),
            "batch_number": self.config.batch_number,
            "total_batches": self.config.total_batches,
            "processing_timestamp": datetime.now().isoformat(),
            "api_summary": company_data["summary"]
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
    
    @timed(name="Saving company data")
    def _save_company_data(self, processed_data: Dict[str, Any]) -> str:
        """
        Save processed company data to GCS.
        
        Args:
            processed_data: Processed company data
            
        Returns:
            Table name where data was saved
        """
        self.log.info("Saving company data")
        
        # Create table name with batch suffix if applicable
        if self.config.batch_number:
            table_name = f"cvr_companies_batch_{self.config.batch_number:03d}"
        else:
            table_name = "cvr_companies"
        
        companies_data = processed_data["companies"]
        
        # Create DuckDB table
        self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        if companies_data:
            # Convert to JSON strings for DuckDB
            json_strings = [json.dumps(company) for company in companies_data]
            
            self.conn.execute(f"""
                CREATE TABLE {table_name} AS
                SELECT 
                    json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                    json_extract(json_data, '$.company_name')::VARCHAR as company_name,
                    json_extract(json_data, '$.company_type_description')::VARCHAR as company_type_description,
                    json_extract(json_data, '$.status')::VARCHAR as status,
                    json_extract(json_data, '$.pnumber_count')::INTEGER as pnumber_count,
                    json_data as company_data_json,
                    json_extract(json_data, '$.processing_timestamp')::VARCHAR as processing_timestamp,
                    json_extract(json_data, '$.batch_number')::INTEGER as batch_number
                FROM unnest($1) as t(json_data)
            """, [json_strings])
            
            self.log.info(f"Created table {table_name} with {len(companies_data)} companies")
        else:
            # Create empty table with schema
            self.conn.execute(f"""
                CREATE TABLE {table_name} (
                    cvr_number INTEGER,
                    company_name VARCHAR,
                    company_type_description VARCHAR,
                    status VARCHAR,
                    pnumber_count INTEGER,
                    company_data_json VARCHAR,
                    processing_timestamp VARCHAR,
                    batch_number INTEGER
                )
            """)
            self.log.info(f"Created empty table {table_name}")
        
        # Save to GCS
        self._save_data(
            data=table_name,
            dataset=self.config.dataset,
            bucket=self.config.bucket,
            stage="gold"
        )
        
        # Save summary data separately
        self._save_summary_data(processed_data["summary"])
        
        return table_name
    
    def _save_summary_data(self, summary: Dict[str, Any]) -> None:
        """Save processing summary data."""
        if self.config.batch_number:
            summary_path = f"gold/{self.config.dataset}/{self.date_pattern}/summary_batch_{self.config.batch_number:03d}.json"
        else:
            summary_path = f"gold/{self.config.dataset}/{self.date_pattern}/summary.json"
        
        self.gcs_access.upload_json(
            data=summary,
            gcs_path=f"gs://{self.config.bucket}/{summary_path}"
        )
        
        self.log.info(f"Saved processing summary to {summary_path}")
