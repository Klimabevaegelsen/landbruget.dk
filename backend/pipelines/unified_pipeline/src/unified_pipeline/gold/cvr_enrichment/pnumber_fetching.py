"""
P-Number Fetching Step - Step 3 of CVR Enrichment Pipeline

This step fetches P-number (production unit) data from the CVR register
for all P-numbers discovered in the company data, providing additional
address coverage for building matching.
"""

import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

from pydantic import Field

from unified_pipeline.common.base import BaseJobConfig, BaseSource, GoldJobInterface
from unified_pipeline.util.cvr_api_client import CVRAPIClient
from unified_pipeline.util.timing import timed
from .shared.config import CVREnrichmentSharedConfig, CVREnrichmentStep, get_step_input_paths


class PNumberFetchingConfig(BaseJobConfig):
    """Configuration for P-number fetching step."""
    
    name: str = "P-Number Data Fetching"
    dataset: str = "cvr_enrichment_pnumbers"
    type: str = "cvr_api"
    description: str = "Fetch P-number (production unit) data from CVR register"
    frequency: str = "monthly"
    bucket: str = "landbrugsdata-raw-data"
    
    # Shared configuration
    shared_config: CVREnrichmentSharedConfig = Field(
        default_factory=CVREnrichmentSharedConfig,
        description="Shared configuration for CVR enrichment pipeline"
    )
    
    # P-number fetching specific configuration
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


class PNumberFetching(BaseSource[PNumberFetchingConfig], GoldJobInterface):
    """
    P-number fetching step implementation.
    
    This step:
    1. Loads company data from company fetching step
    2. Extracts P-numbers from company data
    3. Fetches P-number data from CVR register API
    4. Links P-numbers to parent companies
    5. Saves P-number data for subsequent steps
    """
    
    def __init__(self, config: PNumberFetchingConfig):
        """
        Initialize P-number fetching step.
        
        Args:
            config: Configuration for P-number fetching
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
        
        self.log.info("P-number fetching step initialized")
        self.log.info(f"📋 Configuration:")
        self.log.info(f"   • Batch: {self.config.batch_number}/{self.config.total_batches}")
        self.log.info(f"   • Fetch all fields: {self.config.fetch_all_fields}")
        self.log.info(f"   • Address geocoding: {'enabled' if self.config.enable_address_geocoding else 'disabled (separate step)'}")
    
    @timed(name="P-number fetching processing")
    async def run(self, silver_data: Optional[Dict[str, Any]] = None) -> str:
        """
        Run the P-number fetching process.
        
        Args:
            silver_data: Optional silver data (not used in this step)
            
        Returns:
            Table name containing P-number data
        """
        self.log.info("Starting P-number fetching step")
        
        try:
            # Step 1: Load company data and extract P-numbers
            pnumber_extraction = self._extract_pnumbers_from_companies()
            
            # Step 2: Fetch P-number data from CVR register
            pnumber_data = await self._fetch_pnumber_data(pnumber_extraction)
            
            # Step 3: Process and link P-number data to companies
            processed_data = self._process_pnumber_data(pnumber_data, pnumber_extraction)
            
            # Step 4: Save P-number data
            table_name = self._save_pnumber_data(processed_data)
            
            self.log.info(f"P-number fetching completed successfully. Data saved to: {table_name}")
            return table_name
            
        except Exception as e:
            self.log.error(f"P-number fetching failed: {e}")
            raise
    
    @timed(name="Extracting P-numbers from companies")
    def _extract_pnumbers_from_companies(self) -> Dict[str, Any]:
        """
        Load company data and extract P-numbers for fetching.
        
        Returns:
            Dictionary containing P-numbers and their company relationships
        """
        self.log.info("Extracting P-numbers from company data")
        
        # Get input paths from company fetching step
        input_paths = get_step_input_paths(
            CVREnrichmentStep.PNUMBER_FETCHING,
            self.date_pattern,
            total_batches=self.config.total_batches,
            bucket=self.config.bucket
        )
        
        if not input_paths:
            self.log.warning("No input paths found for P-number fetching step")
            return {
                "pnumbers": set(),
                "pnumber_to_cvr": {},
                "cvr_to_pnumbers": {},
                "total_companies": 0
            }
        
        all_pnumbers = set()
        pnumber_to_cvr = {}  # P-number -> CVR mapping
        cvr_to_pnumbers = {}  # CVR -> list of P-numbers mapping
        total_companies = 0
        
        # Process each company batch file
        for input_path in input_paths:
            self.log.info(f"Processing company data from: {input_path}")
            
            try:
                # Download and load company data
                local_path = self.gcs_access.download_file(input_path, f"/tmp/company_batch_{len(all_pnumbers)}.parquet")
                
                # Load company data
                result = self.conn.execute("""
                    SELECT cvr_number, company_data_json
                    FROM read_parquet(?)
                    WHERE company_data_json IS NOT NULL
                """, [local_path]).fetchall()
                
                # Extract P-numbers from each company
                for cvr_number, company_json in result:
                    total_companies += 1
                    
                    try:
                        company_data = json.loads(company_json)
                        extracted_pnumbers = company_data.get("extracted_pnumbers", [])
                        
                        if extracted_pnumbers:
                            cvr_to_pnumbers[str(cvr_number)] = extracted_pnumbers
                            
                            for pnumber in extracted_pnumbers:
                                all_pnumbers.add(pnumber)
                                pnumber_to_cvr[pnumber] = str(cvr_number)
                    
                    except json.JSONDecodeError as e:
                        self.log.warning(f"Failed to parse company data for CVR {cvr_number}: {e}")
                        continue
            
            except Exception as e:
                self.log.error(f"Failed to process company data from {input_path}: {e}")
                continue
        
        # Filter P-numbers for this batch if batching is enabled
        if self.config.batch_number and self.config.total_batches:
            pnumber_list = sorted(list(all_pnumbers))
            batch_size = len(pnumber_list) // self.config.total_batches
            start_idx = (self.config.batch_number - 1) * batch_size
            
            if self.config.batch_number == self.config.total_batches:
                # Last batch gets remaining items
                end_idx = len(pnumber_list)
            else:
                end_idx = start_idx + batch_size
            
            batch_pnumbers = set(pnumber_list[start_idx:end_idx])
            
            # Filter mappings to only include batch P-numbers
            filtered_pnumber_to_cvr = {
                pnum: cvr for pnum, cvr in pnumber_to_cvr.items() 
                if pnum in batch_pnumbers
            }
            
            self.log.info(
                f"Batch {self.config.batch_number}/{self.config.total_batches}: "
                f"{len(batch_pnumbers)} P-numbers (from {len(all_pnumbers)} total)"
            )
        else:
            batch_pnumbers = all_pnumbers
            filtered_pnumber_to_cvr = pnumber_to_cvr
        
        extraction_result = {
            "pnumbers": batch_pnumbers,
            "pnumber_to_cvr": filtered_pnumber_to_cvr,
            "cvr_to_pnumbers": cvr_to_pnumbers,
            "total_companies": total_companies,
            "total_pnumbers_found": len(all_pnumbers),
            "batch_pnumbers": len(batch_pnumbers),
            "extraction_timestamp": datetime.now().isoformat()
        }
        
        self.log.info(
            f"Extracted {len(batch_pnumbers)} P-numbers from {total_companies} companies "
            f"({len(all_pnumbers)} total P-numbers found)"
        )
        
        return extraction_result
    
    @timed(name="Fetching P-number data")
    async def _fetch_pnumber_data(self, pnumber_extraction: Dict[str, Any]) -> Dict[str, Any]:
        """
        Fetch P-number data from CVR register.
        
        Args:
            pnumber_extraction: P-number extraction results
            
        Returns:
            Dictionary containing fetched P-number data
        """
        pnumbers = pnumber_extraction["pnumbers"]
        
        self.log.info(f"Fetching P-number data for {len(pnumbers)} P-numbers")
        
        if not pnumbers:
            self.log.warning("No P-numbers to fetch")
            return {
                "results": {},
                "summary": {"total": 0, "successful": 0, "failed": 0},
                "fetch_timestamp": datetime.now().isoformat()
            }
        
        # Convert to list for API client
        pnumber_list = list(pnumbers)
        
        # Fetch P-number data using CVR API client
        pnumber_results = self.cvr_api_client.fetch_multiple_pnumbers(
            pnumbers=pnumber_list,
            fetch_all_fields=self.config.fetch_all_fields,
            enrich_with_geometry=self.config.enable_address_geocoding
        )
        
        self.log.info(
            f"P-number data fetch completed: "
            f"{pnumber_results['summary']['successful']} successful, "
            f"{pnumber_results['summary']['failed']} failed"
        )
        
        return pnumber_results
    
    @timed(name="Processing P-number data")
    def _process_pnumber_data(
        self, 
        pnumber_data: Dict[str, Any], 
        pnumber_extraction: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Process and link P-number data to companies.
        
        Args:
            pnumber_data: Raw P-number data from CVR API
            pnumber_extraction: P-number extraction results
            
        Returns:
            Processed P-number data with company relationships
        """
        self.log.info("Processing P-number data")
        
        pnumber_results = pnumber_data["results"]
        pnumber_to_cvr = pnumber_extraction["pnumber_to_cvr"]
        
        # Process each P-number's data
        processed_pnumbers = []
        
        for pnumber, pnumber_info in pnumber_results.items():
            if pnumber_info:
                # Add company relationship
                parent_cvr = pnumber_to_cvr.get(pnumber)
                pnumber_info["parent_cvr_number"] = parent_cvr
                
                # Add processing metadata
                pnumber_info["processing_timestamp"] = datetime.now().isoformat()
                pnumber_info["pipeline_run_id"] = self.date_pattern
                pnumber_info["processing_step"] = CVREnrichmentStep.PNUMBER_FETCHING.value
                pnumber_info["batch_number"] = self.config.batch_number
                
                processed_pnumbers.append(pnumber_info)
        
        # Create summary
        summary = {
            "total_pnumbers": len(pnumber_results),
            "successful_pnumbers": len(processed_pnumbers),
            "failed_pnumbers": len([p for p in pnumber_results.values() if p is None]),
            "pnumbers_with_addresses": len([
                p for p in processed_pnumbers 
                if p.get("addresses") and len(p["addresses"]) > 0
            ]),
            "total_addresses_found": sum(
                len(p.get("addresses", [])) for p in processed_pnumbers
            ),
            "batch_number": self.config.batch_number,
            "total_batches": self.config.total_batches,
            "processing_timestamp": datetime.now().isoformat(),
            "api_summary": pnumber_data["summary"],
            "extraction_summary": {
                "total_companies_processed": pnumber_extraction["total_companies"],
                "total_pnumbers_found": pnumber_extraction["total_pnumbers_found"],
                "batch_pnumbers_processed": pnumber_extraction["batch_pnumbers"]
            }
        }
        
        processed_data = {
            "pnumbers": processed_pnumbers,
            "summary": summary,
        }
        
        self.log.info(
            f"Processed {summary['successful_pnumbers']} P-numbers "
            f"({summary['total_addresses_found']} addresses found)"
        )
        
        return processed_data
    
    @timed(name="Saving P-number data")
    def _save_pnumber_data(self, processed_data: Dict[str, Any]) -> str:
        """
        Save processed P-number data to GCS.
        
        Args:
            processed_data: Processed P-number data
            
        Returns:
            Table name where data was saved
        """
        self.log.info("Saving P-number data")
        
        # Create table name with batch suffix if applicable
        if self.config.batch_number:
            table_name = f"cvr_pnumbers_batch_{self.config.batch_number:03d}"
        else:
            table_name = "cvr_pnumbers"
        
        pnumbers_data = processed_data["pnumbers"]
        
        # Create DuckDB table
        self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        if pnumbers_data:
            # Convert to JSON strings for DuckDB
            json_strings = [json.dumps(pnumber) for pnumber in pnumbers_data]
            
            self.conn.execute(f"""
                CREATE TABLE {table_name} AS
                SELECT 
                    json_extract(json_data, '$.p_number')::INTEGER as p_number,
                    json_extract(json_data, '$.unit_name')::VARCHAR as unit_name,
                    json_extract(json_data, '$.parent_cvr_number')::INTEGER as parent_cvr_number,
                    json_array_length(json_extract(json_data, '$.addresses')) as address_count,
                    json_data as pnumber_data_json,
                    json_extract(json_data, '$.processing_timestamp')::VARCHAR as processing_timestamp,
                    json_extract(json_data, '$.batch_number')::INTEGER as batch_number
                FROM unnest($1) as t(json_data)
            """, [json_strings])
            
            self.log.info(f"Created table {table_name} with {len(pnumbers_data)} P-numbers")
        else:
            # Create empty table with schema
            self.conn.execute(f"""
                CREATE TABLE {table_name} (
                    p_number INTEGER,
                    unit_name VARCHAR,
                    parent_cvr_number INTEGER,
                    address_count INTEGER,
                    pnumber_data_json VARCHAR,
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
