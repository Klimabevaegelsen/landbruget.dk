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

        # Initialize CVR collection manager
        self.cvr_collection_manager = CVRCollectionManager(
            gcs_access=self.gcs_access, bucket=self.config.bucket
        )

        # Initialize CVR API client with credentials from memory [[memory:2283672]]
        cvr_username = os.getenv("CVR_USERNAME", "Martin_Collignon_CVR_I_SKYEN")
        cvr_password = os.getenv("CVR_PASSWORD", "3a37d029-9588-4c00-8a09-3d2901452d45")

        self.cvr_api_client = CVRAPIClient(username=cvr_username, password=cvr_password)

        self.log.info("CVR enrichment gold layer initialized")

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
            cvr_numbers=valid_cvrs, fetch_all_fields=self.config.fetch_all_fields
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
        Save the processed CVR enrichment data to GCS.

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
            self.log.info(f"🔍 Creating normalized tables for {len(companies_data)} companies")
            
            import json
            json_strings = [json.dumps(company) for company in companies_data]
            
            # 1. Main companies table
            self.conn.execute(f"""
                CREATE TABLE {table_name} AS 
                SELECT 
                    json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                    json_extract(json_data, '$.company_name')::VARCHAR as company_name,
                    json_extract(json_data, '$.company_type_description')::VARCHAR as company_type_description,
                    json_extract(json_data, '$.status')::VARCHAR as status,
                    json_extract(json_data, '$.founded_date')::VARCHAR as founded_date,
                    json_extract(json_data, '$.dissolution_date')::VARCHAR as dissolution_date,
                    json_extract(json_data, '$.data_source')::VARCHAR as data_source,
                    json_extract(json_data, '$.fetch_timestamp')::VARCHAR as fetch_timestamp,
                    json_extract(json_data, '$.source_pipelines')::VARCHAR[] as source_pipelines,
                    json_extract(json_data, '$.source_pipeline_count')::INTEGER as source_pipeline_count,
                    json_extract(json_data, '$.financial_document_count')::INTEGER as financial_document_count,
                    json_extract(json_data, '$.processing_timestamp')::VARCHAR as processing_timestamp,
                    json_extract(json_data, '$.pipeline_run_id')::VARCHAR as pipeline_run_id
                FROM unnest($1) as t(json_data)
            """, [json_strings])
            
            # 2. Leadership table - let DuckDB auto-detect and parse the structure
            # First get the schema structure for leadership data
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
                    CREATE TABLE {table_name}_leadership AS
                    SELECT 
                        json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                        unnest(json_transform(json_extract(json_data, '$.leadership'), $2)) as leadership_parsed
                    FROM unnest($1) as t(json_data)
                    WHERE json_extract(json_data, '$.leadership') IS NOT NULL
                    AND json_array_length(json_extract(json_data, '$.leadership')) > 0
                """, [json_strings, leadership_schema[0]])
            else:
                # Fallback: create empty table
                self.conn.execute(f"""
                    CREATE TABLE {table_name}_leadership (
                        cvr_number INTEGER,
                        leadership_data VARCHAR
                    )
                """)
            
            # Extract leadership details
            leadership_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}_leadership").fetchone()[0]
            
            # 3. Financial documents table - flattened structure for easier analysis
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
                self.conn.execute(f"""
                    CREATE TABLE {table_name}_financial AS
                    WITH financial_flattened AS (
                        SELECT
                            json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                            unnest(json_transform(json_extract(json_data, '$.financial_documents'), $2)) as financial_parsed
                        FROM unnest($1) as t(json_data)
                        WHERE json_extract(json_data, '$.financial_documents') IS NOT NULL
                        AND json_array_length(json_extract(json_data, '$.financial_documents')) > 0
                    )
                    SELECT 
                        cvr_number,
                        -- Document metadata
                        financial_parsed.publication_type,
                        financial_parsed.publication_time,
                        financial_parsed.case_number,
                        financial_parsed.reporting_period.start_date as reporting_period_start,
                        financial_parsed.reporting_period.end_date as reporting_period_end,
                        financial_parsed.document_count,
                        financial_parsed.xml_size_bytes,
                        financial_parsed.download_success,
                        -- XBRL Context Information
                        financial_parsed.financial_metrics.duration_context,
                        financial_parsed.financial_metrics.instant_context,
                        financial_parsed.financial_metrics.income_statement_start_date,
                        financial_parsed.financial_metrics.income_statement_end_date,
                        financial_parsed.financial_metrics.balance_sheet_date,
                        -- Key Financial Metrics (Income Statement)
                        financial_parsed.financial_metrics.net_profit_loss,
                        financial_parsed.financial_metrics.gross_profit_loss,
                        financial_parsed.financial_metrics.operating_profit_loss,
                        financial_parsed.financial_metrics.profit_loss_before_tax,
                        financial_parsed.financial_metrics.employee_benefits_expense,
                        financial_parsed.financial_metrics.average_number_of_employees,
                        financial_parsed.financial_metrics.depreciation_expense,
                        financial_parsed.financial_metrics.other_finance_income,
                        financial_parsed.financial_metrics.other_finance_expenses,
                        financial_parsed.financial_metrics.tax_expense,
                        -- Key Financial Metrics (Balance Sheet)
                        financial_parsed.financial_metrics.total_assets,
                        financial_parsed.financial_metrics.total_equity,
                        financial_parsed.financial_metrics.noncurrent_assets,
                        financial_parsed.financial_metrics.current_assets,
                        financial_parsed.financial_metrics.cash_and_cash_equivalents,
                        financial_parsed.financial_metrics.liabilities_other_than_provisions,
                        financial_parsed.financial_metrics.shortterm_liabilities_other_than_provisions,
                        financial_parsed.financial_metrics.longterm_liabilities_other_than_provisions,
                        financial_parsed.financial_metrics.provisions,
                        financial_parsed.financial_metrics.property_plant_equipment,
                        financial_parsed.financial_metrics.contributed_capital,
                        -- Additional calculated fields for analysis
                        CASE 
                            WHEN financial_parsed.financial_metrics.total_assets > 0 
                            THEN financial_parsed.financial_metrics.total_equity / financial_parsed.financial_metrics.total_assets 
                            ELSE NULL 
                        END as equity_ratio,
                        CASE 
                            WHEN financial_parsed.financial_metrics.average_number_of_employees > 0 
                            THEN financial_parsed.financial_metrics.net_profit_loss / financial_parsed.financial_metrics.average_number_of_employees 
                            ELSE NULL 
                        END as profit_per_employee,
                        CASE 
                            WHEN financial_parsed.financial_metrics.total_assets > 0 
                            THEN financial_parsed.financial_metrics.net_profit_loss / financial_parsed.financial_metrics.total_assets 
                            ELSE NULL 
                        END as return_on_assets
                    FROM financial_flattened
                """, [json_strings, financial_schema[0]])
            else:
                # Fallback: create empty table with proper schema
                self.conn.execute(f"""
                    CREATE TABLE {table_name}_financial (
                        cvr_number INTEGER,
                        publication_type VARCHAR,
                        reporting_period_start VARCHAR,
                        reporting_period_end VARCHAR,
                        net_profit_loss DOUBLE,
                        total_assets DOUBLE,
                        total_equity DOUBLE,
                        average_number_of_employees DOUBLE,
                        equity_ratio DOUBLE,
                        profit_per_employee DOUBLE,
                        return_on_assets DOUBLE
                    )
                """)
            
            financial_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}_financial").fetchone()[0]
            
            # 4. Address history table - let DuckDB auto-detect structure
            address_schema = self.conn.execute("""
                WITH address_sample AS (
                    SELECT json_extract(json_data, '$.addresses') as address_json
                    FROM unnest($1) as t(json_data)
                    WHERE json_extract(json_data, '$.addresses') IS NOT NULL
                    AND json_array_length(json_extract(json_data, '$.addresses')) > 0
                    LIMIT 1
                )
                SELECT json_structure(address_json) FROM address_sample
            """, [json_strings]).fetchone()
            
            if address_schema and address_schema[0]:
                self.conn.execute(f"""
                    CREATE TABLE {table_name}_addresses AS
                    SELECT 
                        json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                        unnest(json_transform(json_extract(json_data, '$.addresses'), $2)) as address_parsed
                    FROM unnest($1) as t(json_data)
                    WHERE json_extract(json_data, '$.addresses') IS NOT NULL
                    AND json_array_length(json_extract(json_data, '$.addresses')) > 0
                """, [json_strings, address_schema[0]])
            else:
                # Fallback: create empty table
                self.conn.execute(f"""
                    CREATE TABLE {table_name}_addresses (
                        cvr_number INTEGER,
                        address_data VARCHAR
                    )
                """)
            
            address_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}_addresses").fetchone()[0]
            
            # 5. Industries table - let DuckDB auto-detect structure  
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
                    CREATE TABLE {table_name}_industries AS
                    SELECT 
                        json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                        unnest(json_transform(json_extract(json_data, '$.industries'), $2)) as industry_parsed
                    FROM unnest($1) as t(json_data)
                    WHERE json_extract(json_data, '$.industries') IS NOT NULL
                    AND json_array_length(json_extract(json_data, '$.industries')) > 0
                """, [json_strings, industry_schema[0]])
            else:
                # Fallback: create empty table
                self.conn.execute(f"""
                    CREATE TABLE {table_name}_industries (
                        cvr_number INTEGER,
                        industry_data VARCHAR
                    )
                """)
            
            industry_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}_industries").fetchone()[0]
            
            # 6. Employment data tables - separate tables for each employment type
            employment_types = [
                ('annual_employment', 'annual'),
                ('quarterly_employment', 'quarterly'), 
                ('monthly_employment', 'monthly'),
                ('replacement_monthly_employment', 'replacement_monthly')
            ]
            
            employment_counts = {}
            for employment_field, table_suffix in employment_types:
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
                        CREATE TABLE {table_name}_employment_{table_suffix} AS
                        SELECT
                            json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                            unnest(json_transform(json_extract(json_data, '$.employment_data.{employment_field}'), $2)) as employment_parsed
                        FROM unnest($1) as t(json_data)
                        WHERE json_extract(json_data, '$.employment_data.{employment_field}') IS NOT NULL
                        AND json_array_length(json_extract(json_data, '$.employment_data.{employment_field}')) > 0
                    """, [json_strings, employment_schema[0]])
                else:
                    # Fallback: create empty table
                    self.conn.execute(f"""
                        CREATE TABLE {table_name}_employment_{table_suffix} (
                            cvr_number INTEGER,
                            employment_data VARCHAR
                        )
                    """)
                
                employment_counts[table_suffix] = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}_employment_{table_suffix}").fetchone()[0]
            
            # Show summary results
            sample_results = self.conn.execute(f"""
                SELECT cvr_number, company_name, company_type_description, founded_date, 
                       source_pipelines, financial_document_count
                FROM {table_name} 
                LIMIT 5
            """).fetchall()
            
            self.log.info(f"🎉 Successfully created normalized CVR tables!")
            self.log.info(f"   📋 Companies: {len(companies_data)}")
            self.log.info(f"   👥 Leadership entries: {leadership_count}")
            self.log.info(f"   💰 Financial documents: {financial_count}")
            self.log.info(f"   📍 Address entries: {address_count}")
            self.log.info(f"   🏭 Industry entries: {industry_count}")
            self.log.info(f"   👷 Employment data:")
            for table_suffix, count in employment_counts.items():
                self.log.info(f"      📈 {table_suffix.replace('_', ' ').title()}: {count} records")
            
            for row in sample_results:
                self.log.info(f"   📋 CVR: {row[0]} | Name: {row[1]} | Type: {row[2]} | Founded: {row[3]} | Sources: {row[4]} | Fin.Docs: {row[5]}")
        else:
            # Create empty table with schema
            self.conn.execute(f"""
                CREATE TABLE {table_name} (
                    cvr_number VARCHAR,
                    company_name VARCHAR,
                    company_type VARCHAR,
                    industry_text VARCHAR,
                    fetch_timestamp TIMESTAMP,
                    source_pipelines VARCHAR[],
                    financial_document_count INTEGER
                )
            """)
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
