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
    dataset: str = "cvr_enrichment"
    type: str = "cvr_api"
    description: str = "Fetch comprehensive company data from CVR register"
    frequency: str = "monthly"
    bucket: str = "landbrugsdata-raw-data"

    # Shared configuration
    shared_config: CVREnrichmentSharedConfig = Field(
        default_factory=CVREnrichmentSharedConfig,
        description="Shared configuration for CVR enrichment pipeline",
    )

    # Company fetching specific configuration (no batching)

    fetch_all_fields: bool = Field(
        default=True, description="Whether to fetch all available fields from CVR API"
    )

    # Note: Address geocoding is handled in separate step
    enable_address_geocoding: bool = Field(
        default=False,
        description="Whether to enrich addresses with geometry (handled in separate step)",
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
            geocode_current_only=self.config.shared_config.geocoding_current_addresses_only,
        )

        self.log.info("Company fetching step initialized")
        self.log.info("📋 Configuration:")
        self.log.info("   • Processing mode: Single job (no batching)")
        self.log.info(f"   • Fetch all fields: {self.config.fetch_all_fields}")
        self.log.info(
            f"   • Address geocoding: "
            f"{'enabled' if self.config.enable_address_geocoding else 'disabled (separate step)'}"
        )

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
            total_successful = company_data.get("summary", {}).get("successful", 0)
            total_failed = company_data.get("summary", {}).get("failed", 0)
            api_calls = company_data.get("summary", {}).get("api_calls", 0)
            efficiency_gain = company_data.get("summary", {}).get("efficiency_gain", "N/A")

            self.log.info("=" * 60)
            self.log.info("✅ COMPANY FETCHING COMPLETED SUCCESSFULLY")
            self.log.info("=" * 60)
            self.log.info("📊 PROCESSING SUMMARY:")
            self.log.info(f"   • CVR numbers requested: {total_requested:,}")
            self.log.info(f"   • Companies found: {total_successful:,}")
            self.log.info(f"   • Companies not found: {total_failed:,}")
            self.log.info(
                f"   • Success rate: {(total_successful / total_requested * 100):.1f}%"
                if total_requested > 0
                else "   • Success rate: N/A"
            )
            self.log.info(f"   • API calls made: {api_calls}")
            self.log.info(f"   • Efficiency gain: {efficiency_gain}")
            self.log.info(f"   • Output table: {table_name}")
            self.log.info("   • Ready for next step: P-Number Fetching")
            self.log.info("=" * 60)

            return table_name

        except Exception as e:
            self.log.error("=" * 60)
            self.log.error("❌ COMPANY FETCHING FAILED")
            self.log.error("=" * 60)
            self.log.error(f"💥 Error: {e}")
            self.log.error("🔍 Check the logs above for detailed error information")
            self.log.error("📋 Processing mode: Single job (no batching)")
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
                # Load CVR numbers directly from GCS using DuckDB (fallback)
                result = self.conn.execute(f"""
                    SELECT cvr_number, collection_metadata
                    FROM read_parquet('{collection_path}')
                    ORDER BY cvr_number
                """).fetchall()

            all_cvrs = [row[0] for row in result]

            # Process all CVRs (no batching)
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
                "fetch_timestamp": datetime.now().isoformat(),
            }

        # Fetch company data using CVR API client with batch optimization
        company_results = self.cvr_api_client.fetch_multiple_companies(
            cvr_numbers=cvr_batch,
            fetch_all_fields=self.config.fetch_all_fields,
            enrich_with_geometry=self.config.enable_address_geocoding,
            batch_size=self.config.shared_config.api_batch_size,
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
                    json_extract(json_data, '$.company_name')::VARCHAR as company_name,
                    json_extract(json_data, '$.company_type_description')::VARCHAR
                        as company_type_description,
                    json_extract(json_data, '$.status')::VARCHAR as status,
                    json_extract(json_data, '$.founded_date')::VARCHAR as founded_date,
                    json_extract(json_data, '$.dissolution_date')::VARCHAR as dissolution_date,
                    json_extract(json_data, '$.advertisement_protection')::BOOLEAN as advertisement_protection,
                    json_extract(json_data, '$.pnumber_count')::INTEGER as pnumber_count,
                    json_data as company_data_json,  -- Keep for pipeline dependencies (artifacts)
                    json_extract(json_data, '$.processing_timestamp')::VARCHAR
                        as processing_timestamp
                FROM unnest($1) as t(json_data)
            """,
                [json_strings],
            )

            # Create normalized persons table from leadership data
            self._create_persons_table(json_strings)
            
            # Create normalized employment table from employment data
            self._create_employment_table(json_strings)

            self.log.info(f"Created table {table_name} with {len(companies_data)} companies")
        else:
            # Create empty table with schema
            self.conn.execute(f"""
                CREATE TABLE {table_name} (
                    cvr_number INTEGER,
                    company_name VARCHAR,
                    company_type_description VARCHAR,
                    status VARCHAR,
                    founded_date VARCHAR,
                    dissolution_date VARCHAR,
                    advertisement_protection BOOLEAN,
                    pnumber_count INTEGER,
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

    def _create_persons_table(self, json_strings: List[str]) -> None:
        """Create normalized persons table from leadership data."""
        import uuid
        
        self.log.info("Creating normalized persons table from leadership data")
        
        # Create persons table
        persons_table = "cvr_persons"
        self.conn.execute(f"DROP TABLE IF EXISTS {persons_table}")
        
        self.conn.execute(f"""
            CREATE TABLE {persons_table} AS
            WITH leadership_flattened AS (
                SELECT
                    json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                    idx as leadership_idx
                FROM unnest($1) as t(json_data)
                CROSS JOIN generate_series(0::BIGINT, 
                    CASE 
                        WHEN json_array_length(json_extract(json_data, '$.leadership')) > 0 
                        THEN (json_array_length(json_extract(json_data, '$.leadership')) - 1)::BIGINT
                        ELSE 0::BIGINT
                    END) as t(idx)
                WHERE json_extract(json_data, '$.leadership') IS NOT NULL
                AND json_array_length(json_extract(json_data, '$.leadership')) > 0
            ),
            persons_extracted AS (
                SELECT
                    lf.cvr_number,
                    json_extract(t.json_data, '$.leadership[' || lf.leadership_idx || '].person.unit_number')::BIGINT as unit_number,
                    json_extract(t.json_data, '$.leadership[' || lf.leadership_idx || '].person.person_type')::VARCHAR as person_type,
                    -- Get current name (first name marked as current, or first name if none marked)
                    json_extract(t.json_data, '$.leadership[' || lf.leadership_idx || '].person.names[0].name')::VARCHAR as current_name,
                    -- Get current address (first address marked as current, or first address if none marked)
                    json_extract(t.json_data, '$.leadership[' || lf.leadership_idx || '].person.addresses[0].city')::VARCHAR as current_city,
                    TRY_CAST(json_extract(t.json_data, '$.leadership[' || lf.leadership_idx || '].person.addresses[0].postal_code') AS INTEGER) as current_postal_code,
                    json_extract(t.json_data, '$.leadership[' || lf.leadership_idx || '].person.addresses[0].municipality_name')::VARCHAR as current_municipality,
                    -- Get role from organization
                    json_extract(t.json_data, '$.leadership[' || lf.leadership_idx || '].organization.member_data[0].attributter[0].vaerdier[0].vaerdi')::VARCHAR as role,
                    json_extract(t.json_data, '$.leadership[' || lf.leadership_idx || '].organization.member_data[0].attributter[0].vaerdier[0].periode.gyldigFra')::VARCHAR as role_start_date,
                    json_extract(t.json_data, '$.leadership[' || lf.leadership_idx || '].organization.member_data[0].attributter[0].vaerdier[0].periode.gyldigTil')::VARCHAR as role_end_date,
                    json_extract(t.json_data, '$.leadership[' || lf.leadership_idx || '].is_current')::BOOLEAN as is_current_role,
                    NOW()::VARCHAR as processing_timestamp
                FROM leadership_flattened lf
                JOIN unnest($1) as t(json_data) ON json_extract(t.json_data, '$.cvr_number')::INTEGER = lf.cvr_number
                WHERE json_extract(t.json_data, '$.leadership[' || lf.leadership_idx || '].person.unit_number') IS NOT NULL
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
                md5(cvr_number::VARCHAR)::VARCHAR as company_uuid,
                cvr_number,
                role,
                -- Create a formatted role for display using DuckDB title case function (handle mixed content)
                list_aggr(
                    transform(
                        string_split(TRIM(role, '"'), ' '),
                        x -> CASE 
                            WHEN regexp_matches(x, '^[A-Za-zÀ-ÿ.]+$') 
                            THEN upper(left(x, 1)) || lower(substring(x, 2))
                            ELSE x  -- Keep as-is if contains digits or other special chars
                        END
                    ),
                    ' '
                ) as role_formatted,
                role_start_date,
                role_end_date,
                COALESCE(is_current_role, true) as is_current_role,
                -- Classify as leadership based on role (case-insensitive matching)
                CASE 
                    WHEN UPPER(TRIM(role, '"')) IN ('DIREKTØR', 'ADM. DIR.', 'FORMAND', 'NÆSTFORMAND', 'BESTYRELSESMEDLEM', 'LEDER', 'INTERESSENTER') THEN true
                    WHEN UPPER(TRIM(role, '"')) IN ('REEL EJER', 'REVISION', 'STIFTERE', 'FORENINGSREPRÆSENTANT', 'LIKVIDATOR') THEN false
                    ELSE NULL
                END as is_leadership,
                -- Classify as owner based on role (case-insensitive matching)
                CASE
                    WHEN UPPER(TRIM(role, '"')) IN ('REEL EJER', 'INTERESSENTER') THEN true
                    WHEN UPPER(TRIM(role, '"')) IN ('DIREKTØR', 'ADM. DIR.', 'FORMAND', 'NÆSTFORMAND', 'BESTYRELSESMEDLEM', 'REVISION', 'STIFTERE', 'FORENINGSREPRÆSENTANT', 'LIKVIDATOR', 'LEDER') THEN false
                    ELSE NULL
                END as is_owner,
                processing_timestamp
            FROM persons_extracted
            WHERE unit_number IS NOT NULL
        """, [json_strings])
        
        # Get count for logging
        person_count = self.conn.execute(f"SELECT COUNT(*) FROM {persons_table}").fetchone()[0]
        unique_persons = self.conn.execute(f"SELECT COUNT(DISTINCT unit_number) FROM {persons_table}").fetchone()[0]
        
        self.log.info(f"Created persons table with {person_count} person-company relationships")
        self.log.info(f"Representing {unique_persons} unique persons")
        
        # Save persons table to GCS
        self._save_data(
            data=persons_table,
            dataset="cvr_persons",
            bucket=self.config.bucket,
            stage="gold",
        )

    def _create_employment_table(self, json_strings: List[str]) -> None:
        """Create normalized employment table from employment data."""
        self.log.info("Creating normalized employment table from employment data")
        
        # Create employment table
        employment_table = "cvr_employment"
        self.conn.execute(f"DROP TABLE IF EXISTS {employment_table}")
        
        self.conn.execute(f"""
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
                        WHEN json_array_length(json_extract(json_data, '$.employment_data.annual_employment')) > 0 
                        THEN (json_array_length(json_extract(json_data, '$.employment_data.annual_employment')) - 1)::BIGINT
                        ELSE 0::BIGINT
                    END) as t(idx)
                WHERE json_extract(json_data, '$.employment_data.annual_employment') IS NOT NULL
                AND json_array_length(json_extract(json_data, '$.employment_data.annual_employment')) > 0
                
                UNION ALL
                
                -- Quarterly employment  
                SELECT
                    json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                    idx as employment_idx,
                    'quarterly' as employment_type
                FROM unnest($1) as t(json_data)
                CROSS JOIN generate_series(0::BIGINT, 
                    CASE 
                        WHEN json_array_length(json_extract(json_data, '$.employment_data.quarterly_employment')) > 0 
                        THEN (json_array_length(json_extract(json_data, '$.employment_data.quarterly_employment')) - 1)::BIGINT
                        ELSE 0::BIGINT
                    END) as t(idx)
                WHERE json_extract(json_data, '$.employment_data.quarterly_employment') IS NOT NULL
                AND json_array_length(json_extract(json_data, '$.employment_data.quarterly_employment')) > 0
                
                UNION ALL
                
                -- Monthly employment
                SELECT
                    json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                    idx as employment_idx,
                    'monthly' as employment_type
                FROM unnest($1) as t(json_data)
                CROSS JOIN generate_series(0::BIGINT, 
                    CASE 
                        WHEN json_array_length(json_extract(json_data, '$.employment_data.monthly_employment')) > 0 
                        THEN (json_array_length(json_extract(json_data, '$.employment_data.monthly_employment')) - 1)::BIGINT
                        ELSE 0::BIGINT
                    END) as t(idx)
                WHERE json_extract(json_data, '$.employment_data.monthly_employment') IS NOT NULL
                AND json_array_length(json_extract(json_data, '$.employment_data.monthly_employment')) > 0
                
                UNION ALL
                
                -- Replacement monthly employment
                SELECT
                    json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                    idx as employment_idx,
                    'replacement_monthly' as employment_type
                FROM unnest($1) as t(json_data)
                CROSS JOIN generate_series(0::BIGINT, 
                    CASE 
                        WHEN json_array_length(json_extract(json_data, '$.employment_data.replacement_monthly_employment')) > 0 
                        THEN (json_array_length(json_extract(json_data, '$.employment_data.replacement_monthly_employment')) - 1)::BIGINT
                        ELSE 0::BIGINT
                    END) as t(idx)
                WHERE json_extract(json_data, '$.employment_data.replacement_monthly_employment') IS NOT NULL
                AND json_array_length(json_extract(json_data, '$.employment_data.replacement_monthly_employment')) > 0
            )
            SELECT
                -- Generate employment UUID for each record
                md5(CONCAT(ef.cvr_number::VARCHAR, '_', ef.employment_type, '_', 
                          COALESCE(
                            CASE ef.employment_type
                                WHEN 'annual' THEN json_extract(t.json_data, '$.employment_data.annual_employment[' || ef.employment_idx || '].year')::VARCHAR
                                WHEN 'quarterly' THEN json_extract(t.json_data, '$.employment_data.quarterly_employment[' || ef.employment_idx || '].year')::VARCHAR
                                WHEN 'monthly' THEN json_extract(t.json_data, '$.employment_data.monthly_employment[' || ef.employment_idx || '].year')::VARCHAR
                                WHEN 'replacement_monthly' THEN json_extract(t.json_data, '$.employment_data.replacement_monthly_employment[' || ef.employment_idx || '].year')::VARCHAR
                            END, ''), '_',
                          COALESCE(
                            CASE ef.employment_type
                                WHEN 'quarterly' THEN json_extract(t.json_data, '$.employment_data.quarterly_employment[' || ef.employment_idx || '].quarter')::VARCHAR
                                ELSE ''
                            END, ''), '_',
                          COALESCE(
                            CASE ef.employment_type
                                WHEN 'monthly' THEN json_extract(t.json_data, '$.employment_data.monthly_employment[' || ef.employment_idx || '].month')::VARCHAR
                                WHEN 'replacement_monthly' THEN json_extract(t.json_data, '$.employment_data.replacement_monthly_employment[' || ef.employment_idx || '].month')::VARCHAR
                                ELSE ''
                            END, '')))::VARCHAR as employment_uuid,
                -- Generate company UUID for consistency with other tables
                md5(ef.cvr_number::VARCHAR)::VARCHAR as company_uuid,
                ef.cvr_number,
                TRY_CAST(
                    CASE ef.employment_type
                        WHEN 'annual' THEN json_extract(t.json_data, '$.employment_data.annual_employment[' || ef.employment_idx || '].year')
                        WHEN 'quarterly' THEN json_extract(t.json_data, '$.employment_data.quarterly_employment[' || ef.employment_idx || '].year')
                        WHEN 'monthly' THEN json_extract(t.json_data, '$.employment_data.monthly_employment[' || ef.employment_idx || '].year')
                        WHEN 'replacement_monthly' THEN json_extract(t.json_data, '$.employment_data.replacement_monthly_employment[' || ef.employment_idx || '].year')
                    END AS INTEGER) as year,
                TRY_CAST(
                    CASE ef.employment_type
                        WHEN 'quarterly' THEN json_extract(t.json_data, '$.employment_data.quarterly_employment[' || ef.employment_idx || '].quarter')
                        ELSE NULL
                    END AS INTEGER) as quarter,
                TRY_CAST(
                    CASE ef.employment_type
                        WHEN 'monthly' THEN json_extract(t.json_data, '$.employment_data.monthly_employment[' || ef.employment_idx || '].month')
                        WHEN 'replacement_monthly' THEN json_extract(t.json_data, '$.employment_data.replacement_monthly_employment[' || ef.employment_idx || '].month')
                        ELSE NULL
                    END AS INTEGER) as month,
                TRY_CAST(
                    CASE ef.employment_type
                        WHEN 'annual' THEN json_extract(t.json_data, '$.employment_data.annual_employment[' || ef.employment_idx || '].total_employees')
                        WHEN 'quarterly' THEN json_extract(t.json_data, '$.employment_data.quarterly_employment[' || ef.employment_idx || '].total_employees')
                        WHEN 'monthly' THEN json_extract(t.json_data, '$.employment_data.monthly_employment[' || ef.employment_idx || '].total_employees')
                        WHEN 'replacement_monthly' THEN json_extract(t.json_data, '$.employment_data.replacement_monthly_employment[' || ef.employment_idx || '].total_employees')
                    END AS INTEGER) as total_employees,
                TRY_CAST(
                    CASE ef.employment_type
                        WHEN 'annual' THEN json_extract(t.json_data, '$.employment_data.annual_employment[' || ef.employment_idx || '].full_time_equivalent')
                        WHEN 'quarterly' THEN json_extract(t.json_data, '$.employment_data.quarterly_employment[' || ef.employment_idx || '].full_time_equivalent')
                        WHEN 'monthly' THEN json_extract(t.json_data, '$.employment_data.monthly_employment[' || ef.employment_idx || '].full_time_equivalent')
                        WHEN 'replacement_monthly' THEN json_extract(t.json_data, '$.employment_data.replacement_monthly_employment[' || ef.employment_idx || '].full_time_equivalent')
                    END AS DOUBLE) as full_time_equivalent,
                TRY_CAST(
                    CASE ef.employment_type
                        WHEN 'annual' THEN json_extract(t.json_data, '$.employment_data.annual_employment[' || ef.employment_idx || '].employees_including_owners')
                        ELSE NULL
                    END AS INTEGER) as employees_including_owners,
                CASE ef.employment_type
                    WHEN 'annual' THEN json_extract(t.json_data, '$.employment_data.annual_employment[' || ef.employment_idx || '].fte_interval_code')::VARCHAR
                    WHEN 'quarterly' THEN json_extract(t.json_data, '$.employment_data.quarterly_employment[' || ef.employment_idx || '].fte_interval_code')::VARCHAR
                    WHEN 'monthly' THEN json_extract(t.json_data, '$.employment_data.monthly_employment[' || ef.employment_idx || '].fte_interval_code')::VARCHAR
                    WHEN 'replacement_monthly' THEN json_extract(t.json_data, '$.employment_data.replacement_monthly_employment[' || ef.employment_idx || '].fte_interval_code')::VARCHAR
                END as fte_interval_code,
                CASE ef.employment_type
                    WHEN 'annual' THEN json_extract(t.json_data, '$.employment_data.annual_employment[' || ef.employment_idx || '].employees_interval_code')::VARCHAR
                    WHEN 'quarterly' THEN json_extract(t.json_data, '$.employment_data.quarterly_employment[' || ef.employment_idx || '].employees_interval_code')::VARCHAR
                    WHEN 'monthly' THEN json_extract(t.json_data, '$.employment_data.monthly_employment[' || ef.employment_idx || '].employees_interval_code')::VARCHAR
                    WHEN 'replacement_monthly' THEN json_extract(t.json_data, '$.employment_data.replacement_monthly_employment[' || ef.employment_idx || '].employees_interval_code')::VARCHAR
                END as employees_interval_code,
                CASE ef.employment_type
                    WHEN 'annual' THEN json_extract(t.json_data, '$.employment_data.annual_employment[' || ef.employment_idx || '].owners_interval_code')::VARCHAR
                    ELSE NULL
                END as owners_interval_code,
                CASE ef.employment_type
                    WHEN 'annual' THEN json_extract(t.json_data, '$.employment_data.annual_employment[' || ef.employment_idx || '].last_updated')::VARCHAR
                    WHEN 'quarterly' THEN json_extract(t.json_data, '$.employment_data.quarterly_employment[' || ef.employment_idx || '].last_updated')::VARCHAR
                    WHEN 'monthly' THEN json_extract(t.json_data, '$.employment_data.monthly_employment[' || ef.employment_idx || '].last_updated')::VARCHAR
                    WHEN 'replacement_monthly' THEN json_extract(t.json_data, '$.employment_data.replacement_monthly_employment[' || ef.employment_idx || '].last_updated')::VARCHAR
                END as last_updated,
                ef.employment_type,
                NOW()::VARCHAR as processing_timestamp
            FROM employment_flattened ef
            JOIN unnest($1) as t(json_data) ON json_extract(t.json_data, '$.cvr_number')::INTEGER = ef.cvr_number
        """, [json_strings])
        
        # Get count for logging
        employment_count = self.conn.execute(f"SELECT COUNT(*) FROM {employment_table}").fetchone()[0]
        unique_companies = self.conn.execute(f"SELECT COUNT(DISTINCT cvr_number) FROM {employment_table}").fetchone()[0]
        
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

    def _save_summary_data(self, summary: Dict[str, Any]) -> None:
        """Save processing summary data."""
        # No batching - single summary file
        summary_path = f"gold/{self.config.dataset}/{self.date_pattern}/company_summary.json"

        self.gcs_access.upload_json(
            data=summary, gcs_path=f"gs://{self.config.bucket}/{summary_path}"
        )

        self.log.info(f"Saved processing summary to {summary_path}")
