"""
CVR Bronze Layer Implementation

This module implements the bronze layer data ingestion for Danish CVR register data.
It fetches raw data from the CVR API and stores it in the bronze layer following
the medallion architecture pattern.

The module contains:
- CVRBronzeConfig: Configuration class for the CVR data source
- CVRBronze: Implementation class for fetching and processing CVR company data

The data is fetched for specified CVR numbers with proper error handling,
async processing, and retry logic for robustness.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import ConfigDict

from unified_pipeline.common.base import BaseJobConfig, BaseSource, BronzeJobInterface
from unified_pipeline.util.cvr_api_client import CVRAPIClient
from unified_pipeline.util.timing import timed


class CVRBronzeConfig(BaseJobConfig):
    """
    Configuration for the CVR Bronze source.

    This class defines all configuration parameters needed for fetching company data
    from the Danish CVR register API, including CVR numbers to fetch, API settings,
    and performance parameters.

    Attributes:
        name (str): Human-readable name of the data source
        dataset (str): Dataset name for storage
        type (str): Type of the data source (api)
        description (str): Brief description of the data
        frequency (str): How often the data is updated
        bucket (str): GCS bucket name for raw data storage
        cvr_numbers (List[str]): List of CVR numbers to fetch
        fetch_mode (str): 'full' or 'basic' - controls data verbosity
        include_financial_docs (bool): Whether to fetch financial documents
        max_concurrent_fetches (int): Maximum number of concurrent API requests
    """

    name: str = "Danish CVR Register"
    dataset: str = "cvr"
    type: str = "api"
    description: str = "CVR company data from Danish Business Authority"
    frequency: str = "on-demand"
    bucket: str = "landbrugsdata-raw-data"

    # CVR-specific configuration
    cvr_numbers: List[str] = []
    fetch_mode: str = "basic"  # 'full' or 'basic'
    include_financial_docs: bool = False
    max_concurrent_fetches: int = 5

    model_config = ConfigDict(frozen=True)


class CVRBronze(BaseSource[CVRBronzeConfig], BronzeJobInterface):
    """
    Bronze layer processing for CVR register data.

    This class is responsible for fetching raw company data from the Danish CVR register API
    for specified CVR numbers. It handles API communication, error handling,
    and stores the raw data in Google Cloud Storage for further processing.

    Processing flow:
    1. Initialize CVR API client with configuration
    2. Fetch company data for each CVR number
    3. Optionally fetch financial documents
    4. Store raw responses in GCS
    5. Return structured data for in-memory passing to silver stage
    """

    def __init__(self, config: CVRBronzeConfig):
        """
        Initialize the CVRBronze source.

        Args:
            config (CVRBronzeConfig): Configuration for the data source
        """
        super().__init__(config)
        self.cvr_client = CVRAPIClient()

    @timed(name="Fetching CVR company data")
    def _fetch_company_data(self, cvr_number: str) -> Optional[Dict[str, Any]]:
        """
        Fetch data for a specific CVR number.

        Args:
            cvr_number (str): CVR number to fetch data for

        Returns:
            Optional[Dict[str, Any]]: Dictionary containing company data and metadata,
                                    or None if fetching fails
        """
        try:
            self.log.info(f"Fetching CVR data for {cvr_number}")

            # Fetch company data
            fetch_all_fields = self.config.fetch_mode == "full"
            company_data = self.cvr_client.get_company_data(cvr_number, fetch_all_fields)

            if not company_data:
                self.log.warning(f"No data found for CVR {cvr_number}")
                return None

            # Fetch financial documents if requested
            financial_docs = []
            if self.config.include_financial_docs:
                try:
                    financial_docs = self.cvr_client.get_financial_documents(
                        cvr_number, max_results=10, xml_only=True
                    )
                except Exception as e:
                    self.log.warning(f"Failed to fetch financial docs for CVR {cvr_number}: {e}")

            # Create metadata
            metadata = {
                "cvr_number": cvr_number,
                "fetch_time": datetime.now().isoformat(),
                "pipeline_start_time": self.pipeline_start_time.isoformat(),
                "fetch_mode": self.config.fetch_mode,
                "include_financial_docs": self.config.include_financial_docs,
                "financial_docs_count": len(financial_docs),
                "has_error": "error" in company_data,
            }

            # Save raw data to storage
            self._save_company_data(cvr_number, company_data, financial_docs, metadata)

            return {
                "cvr_number": cvr_number,
                "company_data": company_data,
                "financial_docs": financial_docs,
                "metadata": metadata,
            }

        except Exception as e:
            self.log.error(f"Error fetching data for CVR {cvr_number}: {e}")
            return None

    def _save_company_data(
        self,
        cvr_number: str,
        company_data: Dict[str, Any],
        financial_docs: List[Dict[str, Any]],
        metadata: Dict[str, Any],
    ) -> None:
        """
        Save company data and metadata to storage.

        Args:
            cvr_number (str): CVR number
            company_data (Dict[str, Any]): Raw company data from API
            financial_docs (List[Dict[str, Any]]): Financial documents data
            metadata (Dict[str, Any]): Processing metadata
        """
        try:
            # Save company data
            company_path = f"gs://{self.config.bucket}/bronze/cvr/{self.date_pattern}/{cvr_number}_company.json"
            self.gcs_access.upload_json(company_data, company_path)

            # Save financial documents if any
            if financial_docs:
                financial_path = f"gs://{self.config.bucket}/bronze/cvr/{self.date_pattern}/{cvr_number}_financial.json"
                self.gcs_access.upload_json(financial_docs, financial_path)

            # Save metadata
            metadata_path = f"gs://{self.config.bucket}/bronze/cvr/{self.date_pattern}/{cvr_number}_metadata.json"
            self.gcs_access.upload_json(metadata, metadata_path)

            self.log.info(f"Saved CVR {cvr_number} data to storage")

        except Exception as e:
            self.log.error(f"Error saving data for CVR {cvr_number}: {e}")
            raise

    async def run(self) -> Optional[Dict[str, Any]]:
        """
        Run bronze processing for all configured CVR numbers.

        Returns:
            Optional[Dict[str, Any]]: Dictionary mapping cvr_number to company data,
                                    or None if processing fails
        """
        try:
            self.log.info(
                f"Starting CVR bronze processing for {len(self.config.cvr_numbers)} companies"
            )

            if not self.config.cvr_numbers:
                self.log.warning("No CVR numbers configured for processing")
                return None

            # Process CVR numbers (synchronous for now due to API rate limiting)
            results = {}
            for cvr_number in self.config.cvr_numbers:
                result = self._fetch_company_data(cvr_number)
                if result:
                    results[cvr_number] = result
                else:
                    self.log.warning(f"Failed to fetch data for CVR {cvr_number}")

            if not results:
                self.log.error("No CVR numbers were successfully processed")
                return None

            self.log.info(f"Successfully processed {len(results)} CVR numbers")
            return results

        except Exception as e:
            self.log.error(f"Error in CVR bronze processing: {e}")
            return None
