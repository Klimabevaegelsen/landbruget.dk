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
from datetime import datetime
from typing import Any, Dict, Optional, Set

from pydantic import Field

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
    frequency: str = "weekly"
    bucket: str = "landbrugsdata-raw-data"

    # CVR API configuration
    fetch_all_fields: bool = Field(
        default=True, description="Whether to fetch all available fields from CVR API"
    )

    fetch_financial_documents: bool = Field(
        default=True, description="Whether to fetch financial documents for each company"
    )

    max_financial_documents: int = Field(
        default=5, description="Maximum number of financial documents to fetch per company"
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

    model_config = {"frozen": True}


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

            for cvr_number in valid_cvrs:
                if company_results["results"].get(cvr_number):
                    try:
                        docs = self.cvr_api_client.get_financial_documents(
                            cvr_number=cvr_number, max_results=self.config.max_financial_documents
                        )
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
            # Insert data into DuckDB
            self.conn.execute(
                f"""
                CREATE TABLE {table_name} AS 
                SELECT * FROM read_json_auto($1)
            """,
                [companies_data],
            )

            self.log.info(f"Created DuckDB table {table_name} with {len(companies_data)} companies")
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

        # Save table to GCS
        self._save_data(
            data=table_name, dataset=self.config.dataset, bucket=self.config.bucket, stage="gold"
        )

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
