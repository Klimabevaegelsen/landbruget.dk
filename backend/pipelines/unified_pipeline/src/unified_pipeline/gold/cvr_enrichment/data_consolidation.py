"""
Data Consolidation Step - Step 6 of CVR Enrichment Pipeline

This final step consolidates all data from previous steps (companies, P-numbers, 
financial documents, and geocoded addresses) into the final normalized tables
that match the original CVR enrichment output format.
"""

import json
import uuid
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
        description="Shared configuration for CVR enrichment pipeline"
    )
    
    # Data consolidation specific configuration
    create_normalized_tables: bool = Field(
        default=True,
        description="Whether to create the full set of normalized tables"
    )
    
    include_raw_json: bool = Field(
        default=True,
        description="Whether to include raw JSON data in consolidated tables"
    )
    
    model_config = {"frozen": True}
    
    def apply_cli_filters(self, cli_config):
        """Apply CLI configuration filters to this config."""
        if cli_config.test_limit is not None:
            object.__setattr__(self, 'shared_config', 
                self.shared_config.model_copy(update={'test_limit': cli_config.test_limit}))


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
        
        # Set up company UUID generation function
        self._setup_company_uuid_function()
        
        self.log.info("Data consolidation step initialized")
        self.log.info(f"📋 Configuration:")
        self.log.info(f"   • Create normalized tables: {self.config.create_normalized_tables}")
        self.log.info(f"   • Include raw JSON: {self.config.include_raw_json}")
    
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
            total_companies = summary_stats.get('total_companies', 0)
            total_pnumbers = summary_stats.get('total_pnumbers', 0) 
            total_addresses = summary_stats.get('total_addresses', 0)
            geocoded_addresses = summary_stats.get('geocoded_addresses', 0)
            financial_docs = summary_stats.get('financial_documents', 0)
            tables_created = len(table_names)
            
            # Success summary
            self.log.info("=" * 60)
            self.log.info("🎉 CVR ENRICHMENT PIPELINE COMPLETED SUCCESSFULLY")
            self.log.info("=" * 60)
            self.log.info(f"📊 FINAL SUMMARY:")
            self.log.info(f"   • Total companies processed: {total_companies:,}")
            self.log.info(f"   • Total P-numbers processed: {total_pnumbers:,}")
            self.log.info(f"   • Total addresses found: {total_addresses:,}")
            self.log.info(f"   • Addresses geocoded: {geocoded_addresses:,}")
            self.log.info(f"   • Financial documents: {financial_docs:,}")
            self.log.info(f"   • Database tables created: {tables_created}")
            self.log.info(f"   • Main output table: {main_table}")
            self.log.info(f"")
            self.log.info(f"🚀 PIPELINE SUCCESS!")
            self.log.info(f"   The CVR enrichment pipeline has completed successfully.")
            self.log.info(f"   All data is now available in the gold layer tables.")
            self.log.info("=" * 60)
            
            return main_table
            
        except Exception as e:
            self.log.error("=" * 60)
            self.log.error("❌ DATA CONSOLIDATION FAILED")
            self.log.error("=" * 60)
            self.log.error(f"💥 Error: {e}")
            self.log.error(f"🔍 Check the logs above for detailed error information")
            self.log.error(f"⚠️  This is the final step - previous steps may have succeeded")
            self.log.error("=" * 60)
            raise
    
    @timed(name="Loading and merging all data")
    def _load_and_merge_all_data(self) -> Dict[str, Any]:
        """
        Load data from all previous pipeline steps and merge it.
        
        Returns:
            Dictionary containing merged data from all steps
        """
        self.log.info("Loading and merging data from all pipeline steps")
        
        # Get input paths for all previous steps
        input_paths = get_step_input_paths(
            CVREnrichmentStep.DATA_CONSOLIDATION,
            self.date_pattern,
            bucket=self.config.bucket
        )
        
        self.log.info(f"Found {len(input_paths)} input files to process")
        
        # Initialize data containers
        companies_data = {}
        pnumbers_data = {}
        financial_data = {}
        addresses_data = {}
        
        # Process each input file
        for input_path in input_paths:
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
        local_path = self.gcs_access.download_file(input_path, f"/tmp/company_{len(companies_data)}.parquet")
        
        result = self.conn.execute("""
            SELECT cvr_number, company_name, company_data_json
            FROM read_parquet(?)
            WHERE company_data_json IS NOT NULL
        """, [local_path]).fetchall()
        
        for cvr_number, company_name, company_json in result:
            try:
                company_data = json.loads(company_json)
                companies_data[str(cvr_number)] = company_data
            except json.JSONDecodeError as e:
                self.log.warning(f"Failed to parse company data for CVR {cvr_number}: {e}")
    
    def _load_pnumber_data(self, input_path: str, pnumbers_data: Dict[str, Any]) -> None:
        """Load P-number data from a batch file."""
        local_path = self.gcs_access.download_file(input_path, f"/tmp/pnumber_{len(pnumbers_data)}.parquet")
        
        result = self.conn.execute("""
            SELECT p_number, parent_cvr_number, pnumber_data_json
            FROM read_parquet(?)
            WHERE pnumber_data_json IS NOT NULL
        """, [local_path]).fetchall()
        
        for p_number, parent_cvr, pnumber_json in result:
            try:
                pnumber_data = json.loads(pnumber_json)
                pnumber_key = f"{parent_cvr}_{p_number}"
                pnumbers_data[pnumber_key] = pnumber_data
            except json.JSONDecodeError as e:
                self.log.warning(f"Failed to parse P-number data for {p_number}: {e}")
    
    def _load_financial_data(self, input_path: str, financial_data: Dict[str, Any]) -> None:
        """Load financial data from a batch file."""
        local_path = self.gcs_access.download_file(input_path, f"/tmp/financial_{len(financial_data)}.parquet")
        
        result = self.conn.execute("""
            SELECT cvr_number, financial_data_json
            FROM read_parquet(?)
            WHERE financial_data_json IS NOT NULL
        """, [local_path]).fetchall()
        
        for cvr_number, financial_json in result:
            try:
                financial_doc_data = json.loads(financial_json)
                financial_data[str(cvr_number)] = financial_doc_data
            except json.JSONDecodeError as e:
                self.log.warning(f"Failed to parse financial data for CVR {cvr_number}: {e}")
    
    def _load_address_data(self, input_path: str, addresses_data: Dict[str, Any]) -> None:
        """Load address data from a batch file."""
        local_path = self.gcs_access.download_file(input_path, f"/tmp/address_{len(addresses_data)}.parquet")
        
        result = self.conn.execute("""
            SELECT source_type, cvr_number, p_number, address_data_json
            FROM read_parquet(?)
            WHERE address_data_json IS NOT NULL
        """, [local_path]).fetchall()
        
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
                self.log.warning(f"Failed to parse address data for {source_type} {cvr_number}: {e}")
    
    def _merge_data_by_cvr(
        self, 
        companies_data: Dict[str, Any], 
        pnumbers_data: Dict[str, Any], 
        financial_data: Dict[str, Any], 
        addresses_data: Dict[str, Any]
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
                merged_company["financial_documents"] = financial_data[cvr_number].get("documents", [])
                merged_company["financial_document_count"] = financial_data[cvr_number].get("document_count", 0)
                merged_company["latest_financial_metrics"] = financial_data[cvr_number].get("latest_financial_metrics")
            else:
                merged_company["financial_documents"] = []
                merged_company["financial_document_count"] = 0
                merged_company["latest_financial_metrics"] = None
            
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
            "companies_with_pnumbers": len([
                c for c in merged_companies.values() 
                if c.get("pnumber_data") and len(c["pnumber_data"]) > 0
            ]),
            "companies_with_financial_data": len([
                c for c in merged_companies.values() 
                if c.get("financial_document_count", 0) > 0
            ]),
            "companies_with_geocoded_addresses": len([
                c for c in merged_companies.values() 
                if any(addr.get("is_geocoded") for addr in c.get("addresses", []))
            ]),
            "total_pnumbers": len(pnumbers_data),
            "total_addresses": sum(len(c.get("addresses", [])) for c in merged_companies.values()),
            "geocoded_addresses": sum(
                len([addr for addr in c.get("addresses", []) if addr.get("is_geocoded")])
                for c in merged_companies.values()
            ),
            "consolidation_timestamp": datetime.now().isoformat()
        }
        
        return {
            "companies": merged_companies,
            "summary": summary
        }
    
    def _merge_address_lists(
        self, 
        existing_addresses: List[Dict[str, Any]], 
        geocoded_addresses: List[Dict[str, Any]]
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
                addr.get("address_type", "")
            )
            geocoded_map[key] = addr
        
        # Update existing addresses with geocoded data
        updated_addresses = []
        for addr in existing_addresses:
            key = (
                addr.get("full_address", ""),
                addr.get("postal_code", ""),
                addr.get("address_type", "")
            )
            
            if key in geocoded_map:
                # Merge with geocoded data, keeping original structure but updating geocoding fields
                updated_addr = addr.copy()
                geocoded_addr = geocoded_map[key]
                
                # Update with geocoding results
                geocoding_fields = [
                    "latitude", "longitude", "coordinate_system", "srid",
                    "geometry_wkt", "geometry_geojson", "coordinate_quality",
                    "coordinate_source", "dawa_enriched", "datavask_enriched",
                    "geocoding_timestamp", "is_geocoded"
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
        self.log.info("Creating normalized CVR enrichment tables")
        
        companies = consolidated_data["companies"]
        table_names = []
        
        if not companies:
            self.log.warning("No companies to create tables from")
            return []
        
        # Main companies table
        main_table = self._create_main_companies_table(companies)
        table_names.append(main_table)
        
        # Additional normalized tables
        if self.config.create_normalized_tables:
            addresses_table = self._create_addresses_table(companies)
            table_names.append(addresses_table)
            
            pnumbers_table = self._create_pnumbers_table(companies)
            table_names.append(pnumbers_table)
            
            financial_table = self._create_financial_table(companies)
            table_names.append(financial_table)
        
        # Save all tables to GCS
        for table_name in table_names:
            self._save_data(
                data=table_name,
                dataset=self.config.dataset,
                bucket=self.config.bucket,
                stage="gold"
            )
        
        return table_names
    
    def _create_main_companies_table(self, companies: Dict[str, Any]) -> str:
        """Create the main companies table."""
        table_name = "cvr_enriched_companies"
        
        self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        # Convert companies to JSON strings for processing
        json_strings = [json.dumps(company) for company in companies.values()]
        
        self.conn.execute(f"""
            CREATE TABLE {table_name} AS
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
                json_array_length(json_extract(json_data, '$.addresses')) as address_count,
                json_array_length(json_extract(json_data, '$.pnumber_data')) as pnumber_count,
                json_extract(json_data, '$.financial_document_count')::INTEGER as financial_document_count,
                CASE 
                    WHEN json_extract(json_data, '$.latest_financial_metrics') IS NOT NULL 
                    THEN true 
                    ELSE false 
                END as has_financial_metrics,
                {('json_data as company_data_json,' if self.config.include_raw_json else '')}
                json_extract(json_data, '$.consolidation_timestamp')::VARCHAR as consolidation_timestamp,
                json_extract(json_data, '$.pipeline_run_id')::VARCHAR as pipeline_run_id
            FROM unnest($1) as t(json_data)
        """, [json_strings])
        
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
            
            self.conn.execute(f"""
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
            """, [json_strings])
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
            
            self.conn.execute(f"""
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
            """, [json_strings])
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
                    "latest_financial_metrics": company.get("latest_financial_metrics"),
                    "has_metrics": company.get("latest_financial_metrics") is not None
                }
                financial_records.append(financial_record)
        
        if financial_records:
            json_strings = [json.dumps(fin) for fin in financial_records]
            
            self.conn.execute(f"""
                CREATE TABLE {table_name} AS
                SELECT 
                    company_uuid(json_extract(json_data, '$.cvr_number')::INTEGER) as company_uuid,
                    json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                    json_extract(json_data, '$.document_count')::INTEGER as document_count,
                    json_extract(json_data, '$.has_metrics')::BOOLEAN as has_financial_metrics
                FROM unnest($1) as t(json_data)
            """, [json_strings])
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
            
            self.conn.execute(f"""
                CREATE TABLE {table_name} AS
                SELECT 
                    company_uuid(json_extract(json_data, '$.cvr_number')::INTEGER) as company_uuid,
                    json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                    json_extract(json_data, '$.company_name')::VARCHAR as company_name,
                    json_data as consolidated_data_json
                FROM unnest($1) as t(json_data)
            """, [json_strings])
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
    
    def _generate_final_summary(self, consolidated_data: Dict[str, Any], table_names: List[str]) -> Dict[str, Any]:
        """Generate and save comprehensive final summary."""
        
        # Calculate summary statistics
        companies_data = consolidated_data.get("companies", {})
        pnumbers_data = consolidated_data.get("pnumbers", {})
        
        # Count addresses and geocoded addresses
        total_addresses = 0
        geocoded_addresses = 0
        for company in companies_data.values():
            addresses = company.get("addresses", [])
            total_addresses += len(addresses)
            geocoded_addresses += sum(1 for addr in addresses if addr.get("geometry"))
        
        # Count financial documents
        financial_docs = sum(
            len(company.get("financial_documents", [])) 
            for company in companies_data.values()
        )
        
        # Create comprehensive summary statistics
        summary_stats = {
            "total_companies": len(companies_data),
            "total_pnumbers": len(pnumbers_data),
            "total_addresses": total_addresses,
            "geocoded_addresses": geocoded_addresses,
            "financial_documents": financial_docs,
            "tables_created": len(table_names)
        }
        
        summary_data = {
            "pipeline_summary": consolidated_data["summary"],
            "statistics": summary_stats,
            "tables_created": table_names,
            "pipeline_run_id": self.date_pattern,
            "completion_timestamp": datetime.now().isoformat(),
            "pipeline_steps_completed": [
                "collection", "company_fetching", "pnumber_fetching", 
                "financial_documents", "address_geocoding", "data_consolidation"
            ]
        }
        
        summary_path = f"gold/{self.config.dataset}/{self.date_pattern}/final_summary.json"
        
        self.gcs_access.upload_json(
            data=summary_data,
            gcs_path=f"gs://{self.config.bucket}/{summary_path}"
        )
        
        self.log.info(f"Saved final pipeline summary to {summary_path}")
        return summary_stats
