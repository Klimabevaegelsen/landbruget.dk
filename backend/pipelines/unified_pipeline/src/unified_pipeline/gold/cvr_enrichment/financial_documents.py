"""
Financial Documents Step - Step 4 of CVR Enrichment Pipeline

This step fetches and processes financial documents and XML data from the CVR register
for the companies, processing them in batches for parallel execution.
"""

import json
import os
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import Field

from unified_pipeline.common.base import BaseJobConfig, BaseSource, GoldJobInterface
from unified_pipeline.util.cvr_api_client import CVRAPIClient
from unified_pipeline.util.timing import timed

from .shared.config import CVREnrichmentSharedConfig, CVREnrichmentStep, get_step_input_paths


class FinancialDocumentsConfig(BaseJobConfig):
    """Configuration for financial documents step."""

    name: str = "Financial Documents Fetching"
    dataset: str = "cvr_enrichment_financial"
    type: str = "cvr_api"
    description: str = "Fetch and parse financial documents from CVR register"
    frequency: str = "monthly"
    bucket: str = "landbrugsdata-raw-data"

    # Shared configuration
    shared_config: CVREnrichmentSharedConfig = Field(
        default_factory=CVREnrichmentSharedConfig,
        description="Shared configuration for CVR enrichment pipeline",
    )

    # Financial documents specific configuration
    batch_number: Optional[int] = Field(
        default=None, description="Batch number for parallel processing (1-based)"
    )

    total_batches: Optional[int] = Field(
        default=None, description="Total number of batches in this step"
    )

    max_financial_documents: int = Field(
        default=10, description="Maximum number of financial documents to fetch per company"
    )

    parse_financial_xml: bool = Field(
        default=True, description="Whether to download and parse XML financial documents"
    )

    xml_only: bool = Field(
        default=True, description="Whether to fetch only XML documents (not other formats)"
    )

    model_config = {"frozen": True}

    def apply_cli_filters(self, cli_config):
        """Apply CLI configuration filters to this config."""
        if cli_config.batch_number is not None:
            object.__setattr__(self, "batch_number", cli_config.batch_number)
        if cli_config.total_batches is not None:
            object.__setattr__(self, "total_batches", cli_config.total_batches)
        if cli_config.test_limit is not None:
            object.__setattr__(
                self,
                "shared_config",
                self.shared_config.model_copy(update={"test_limit": cli_config.test_limit}),
            )
        if cli_config.max_financial_documents is not None:
            object.__setattr__(self, "max_financial_documents", cli_config.max_financial_documents)
        if cli_config.parse_financial_xml is not None:
            object.__setattr__(self, "parse_financial_xml", cli_config.parse_financial_xml)


class FinancialDocuments(BaseSource[FinancialDocumentsConfig], GoldJobInterface):
    """
    Financial documents step implementation.

    This step:
    1. Loads company data from company fetching step
    2. Fetches financial documents for each company
    3. Downloads and parses XML documents if configured
    4. Extracts financial metrics from XBRL data
    5. Saves financial data for subsequent steps
    """

    def __init__(self, config: FinancialDocumentsConfig):
        """
        Initialize financial documents step.

        Args:
            config: Configuration for financial documents fetching
        """
        super().__init__(config)

        # Initialize CVR API client
        cvr_username = os.getenv("CVR_USERNAME", "Martin_Collignon_CVR_I_SKYEN")
        cvr_password = os.getenv("CVR_PASSWORD", "3a37d029-9588-4c00-8a09-3d2901452d45")

        self.cvr_api_client = CVRAPIClient(
            username=cvr_username,
            password=cvr_password,
            enable_geocoding=False,  # Not needed for financial documents
            geocode_current_only=True,
        )

        self.log.info("Financial documents step initialized")
        self.log.info("📋 Configuration:")
        self.log.info("   • Processing mode: Single job (no batching)")
        self.log.info(f"   • Max documents per company: {self.config.max_financial_documents}")
        self.log.info(f"   • Parse XML: {self.config.parse_financial_xml}")
        self.log.info(f"   • XML only: {self.config.xml_only}")

    @timed(name="Financial documents processing")
    async def run(self, silver_data: Optional[Dict[str, Any]] = None) -> str:
        """
        Run the financial documents fetching process.

        Args:
            silver_data: Optional silver data (not used in this step)

        Returns:
            Table name containing financial documents data
        """
        self.log.info("Starting financial documents step")

        try:
            # Step 1: Load company data from company fetching step
            company_batch = self._load_company_batch()

            # Step 2: Fetch financial documents for companies
            financial_data = await self._fetch_financial_documents(company_batch)

            # Step 3: Process and parse financial data
            processed_data = self._process_financial_data(financial_data)

            # Step 4: Process financial employment data separately (different from CVR register employment)
            self.log.info(
                "⚙️ Step 4a/5: Processing employment data from financial documents (separate from CVR register employment)"
            )
            self._process_employment_data_directly(company_batch)

            # Step 5: Save financial data
            table_name = self._save_financial_data(processed_data)

            self.log.info(
                f"Financial documents step completed successfully. Data saved to: {table_name}"
            )
            return table_name

        except Exception as e:
            self.log.error(f"Financial documents step failed: {e}")
            raise

    @timed(name="Loading company batch")
    def _load_company_batch(self) -> List[Dict[str, Any]]:
        """
        Load company data for this batch from company fetching step output.

        Returns:
            List of company data to process in this batch
        """
        if self.config.shared_config.enable_independent_execution:
            self.log.info(
                "Loading company data from latest available file (independent execution mode)"
            )
        else:
            self.log.info(
                "Loading company data from company fetching step (pipeline dependency mode)"
            )

        # Get input paths from company fetching step (with independent execution support)
        input_paths = get_step_input_paths(
            CVREnrichmentStep.FINANCIAL_DOCUMENTS,
            self.date_pattern,
            total_batches=5,  # Company fetching creates 5 batch files (ignored in independent mode)
            bucket=self.config.bucket,
            enable_independent_execution=self.config.shared_config.enable_independent_execution,
            max_days_back=self.config.shared_config.max_days_back_for_inputs,
        )

        if not input_paths:
            if self.config.shared_config.enable_independent_execution:
                raise ValueError(
                    f"No company data found within "
                    f"{self.config.shared_config.max_days_back_for_inputs} days. "
                    f"Please run the company fetching step first or disable independent execution."
                )
            else:
                raise ValueError("No input paths found for financial documents step")

        all_companies = []

        # Process each company batch file
        for input_path in input_paths:
            self.log.info(f"Loading company data from: {input_path}")

            try:
                # Check if running in GitHub Actions and use artifact data
                import os

                if os.getenv("GITHUB_ACTIONS") == "true":
                    # Use artifact data in GitHub Actions - look for consolidated company data
                    artifact_path = "/tmp/cvr_company_data.parquet"
                    if os.path.exists(artifact_path):
                        self.log.info("Using company data from artifact")
                        local_path = artifact_path
                    else:
                        self.log.warning(f"Company artifact not found: {artifact_path}")
                        continue

                    # Load company data from artifact
                    result = self.conn.execute(
                        """
                        SELECT cvr_number, company_name, company_data_json
                        FROM read_parquet(?)
                        WHERE company_data_json IS NOT NULL
                    """,
                        [local_path],
                    ).fetchall()

                    # Parse company JSON data
                    for cvr_number, company_name, company_json in result:
                        try:
                            company_data = json.loads(company_json)
                            company_data["cvr_number"] = cvr_number
                            company_data["company_name"] = company_name
                            all_companies.append(company_data)
                        except json.JSONDecodeError as e:
                            self.log.warning(
                                f"Failed to parse company data for CVR {cvr_number}: {e}"
                            )
                            continue

                    # In GitHub Actions, we only need to process the artifact once
                    break

                else:
                    # Use GCS temp download for local development
                    self.log.info(f"Local development - downloading from GCS: {input_path}")
                    with self.gcs_access._temp_download(input_path) as temp_file:
                        # Load company data from temp file
                        result = self.conn.execute(
                            """
                            SELECT cvr_number, company_name, company_data_json
                            FROM read_parquet(?)
                            WHERE company_data_json IS NOT NULL
                        """,
                            [temp_file],
                        ).fetchall()

                        # Parse company JSON data inside the context manager
                        for cvr_number, company_name, company_json in result:
                            try:
                                company_data = json.loads(company_json)
                                company_data["cvr_number"] = cvr_number
                                company_data["company_name"] = company_name
                                all_companies.append(company_data)
                            except json.JSONDecodeError as e:
                                self.log.warning(
                                    f"Failed to parse company data for CVR {cvr_number}: {e}"
                                )
                                continue

            except Exception as e:
                self.log.error(f"Failed to load company data from {input_path}: {e}")
                continue

        # Process all companies (no batching)
        company_batch = all_companies
        self.log.info(f"Loaded {len(company_batch)} companies from all batches")

        return company_batch

    @timed(name="Fetching financial documents")
    async def _fetch_financial_documents(
        self, company_batch: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Fetch financial documents for companies.

        Args:
            company_batch: List of company data to process

        Returns:
            Dictionary containing financial documents data
        """
        self.log.info(f"Fetching financial documents for {len(company_batch)} companies")

        if not company_batch:
            self.log.warning("No companies to process for financial documents")
            return {
                "results": {},
                "summary": {"total": 0, "successful": 0, "failed": 0},
                "fetch_timestamp": datetime.now().isoformat(),
            }

        financial_results = {}
        successful = 0
        failed = 0

        from tqdm import tqdm

        for company_data in tqdm(
            company_batch, desc="Fetching financial documents", unit="company"
        ):
            cvr_number = str(company_data["cvr_number"])

            try:
                # Fetch financial documents using CVR API client
                docs = self.cvr_api_client.get_financial_documents(
                    cvr_number=cvr_number,
                    max_results=self.config.max_financial_documents,
                    xml_only=self.config.xml_only,
                )

                # Download XML content if configured
                if self.config.parse_financial_xml and docs:
                    docs = self._download_and_parse_xml_documents(docs, cvr_number)

                if docs:
                    financial_results[cvr_number] = {
                        "cvr_number": cvr_number,
                        "company_name": company_data.get("company_name"),
                        "documents": docs,
                        "document_count": len(docs),
                        "fetch_timestamp": datetime.now().isoformat(),
                    }
                    successful += 1
                else:
                    failed += 1
                    self.log.debug(f"No financial documents found for CVR: {cvr_number}")

            except Exception as e:
                failed += 1
                self.log.error(f"Error fetching financial documents for CVR {cvr_number}: {e}")

        summary = {
            "total": len(company_batch),
            "successful": successful,
            "failed": failed,
            "success_rate": successful / len(company_batch) if company_batch else 0,
        }

        self.log.info(
            f"Financial documents fetch completed: "
            f"{successful} successful, {failed} failed "
            f"({summary['success_rate']:.1%} success rate)"
        )

        return {
            "results": financial_results,
            "summary": summary,
            "fetch_timestamp": datetime.now().isoformat(),
        }

    def _download_and_parse_xml_documents(
        self, docs: List[Dict[str, Any]], cvr_number: str
    ) -> List[Dict[str, Any]]:
        """
        Download and parse XML financial documents.

        Args:
            docs: List of financial document metadata
            cvr_number: CVR number for logging

        Returns:
            List of documents with XML content and parsed financial metrics
        """
        enriched_docs = []

        for doc in docs:
            try:
                # Get the first document URL if available
                if doc.get("documents") and len(doc["documents"]) > 0:
                    document_url = doc["documents"][0].get("document_url")

                    if document_url and document_url.endswith(".xml"):
                        self.log.debug(f"Downloading XML from: {document_url}")

                        # Download XML content
                        xml_content = self.cvr_api_client.download_financial_document(document_url)

                        # Add XML content to document
                        doc_copy = doc.copy()
                        doc_copy["xml_content"] = xml_content
                        doc_copy["xml_size_bytes"] = len(xml_content)
                        doc_copy["download_success"] = True

                        # Parse financial metrics from XML using proper XBRL parsing
                        try:
                            financial_metrics = self._parse_xbrl_financial_data(xml_content)

                            doc_copy["financial_metrics"] = {
                                "parse_success": True,
                                **financial_metrics,  # Add all extracted XBRL metrics
                            }
                        except Exception as parse_e:
                            self.log.warning(
                                f"Failed to parse XBRL data for CVR {cvr_number}: {parse_e}"
                            )
                            doc_copy["financial_metrics"] = {
                                "parse_success": False,
                                "parse_error": str(parse_e),
                            }

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
                self.log.warning(f"Failed to download XML document for CVR {cvr_number}: {e}")
                doc_copy = doc.copy()
                doc_copy["download_success"] = False
                doc_copy["download_error"] = str(e)
                enriched_docs.append(doc_copy)

        return enriched_docs

    def _parse_xbrl_financial_data(self, xml_content: str) -> dict:
        """
        Parse XBRL financial data from XML content using proper context analysis.

        Args:
            xml_content: Raw XML content from financial document

        Returns:
            Dictionary with parsed financial metrics for current period
        """
        import xml.etree.ElementTree as ET

        try:
            root = ET.fromstring(xml_content)

            # Parse context definitions to understand periods and types
            contexts = {}
            for elem in root.iter():
                if "context" in elem.tag.lower() and elem.get("id"):
                    context_id = elem.get("id")
                    contexts[context_id] = {
                        "type": None,  # 'duration' or 'instant'
                        "start_date": None,
                        "end_date": None,
                        "instant_date": None,
                        "entity_id": None,
                    }

                    # Parse context details
                    for child in elem:
                        if "entity" in child.tag.lower():
                            for gc in child:
                                if "identifier" in gc.tag.lower():
                                    contexts[context_id]["entity_id"] = gc.text
                        elif "period" in child.tag.lower():
                            for gc in child:
                                gc_tag = gc.tag.split("}")[-1] if "}" in gc.tag else gc.tag
                                if gc_tag == "startDate":
                                    contexts[context_id]["start_date"] = gc.text
                                    contexts[context_id]["type"] = "duration"
                                elif gc_tag == "endDate":
                                    contexts[context_id]["end_date"] = gc.text
                                elif gc_tag == "instant":
                                    contexts[context_id]["instant_date"] = gc.text
                                    contexts[context_id]["type"] = "instant"

            # Find current duration context (for income statement items)
            current_duration_context = None
            current_instant_context = None

            # Look for the most recent duration context
            duration_contexts = {k: v for k, v in contexts.items() if v["type"] == "duration"}
            if duration_contexts:
                sorted_duration = sorted(
                    duration_contexts.items(), key=lambda x: x[1].get("end_date", ""), reverse=True
                )
                current_duration_context = sorted_duration[0][0]

            # Look for the most recent instant context
            instant_contexts = {k: v for k, v in contexts.items() if v["type"] == "instant"}
            if instant_contexts:
                sorted_instant = sorted(
                    instant_contexts.items(),
                    key=lambda x: x[1].get("instant_date", ""),
                    reverse=True,
                )
                current_instant_context = sorted_instant[0][0]

            # Fallbacks
            if not current_duration_context:
                current_duration_context = "c1"
            if not current_instant_context:
                current_instant_context = "c4" if "c4" in contexts else current_duration_context

            # Key financial elements to extract (Danish XBRL taxonomy)
            duration_elements = {
                # Income Statement items (use duration context)
                "net_profit_loss": "ProfitLoss",
                "operating_profit_loss": "ProfitLossFromOrdinaryOperatingActivities",
                "profit_loss_before_tax": "ProfitLossFromOrdinaryActivitiesBeforeTax",
                "employee_benefits_expense": "EmployeeBenefitsExpense",
                "average_number_of_employees": "AverageNumberOfEmployees",
                "depreciation_expense": (
                    "DepreciationAmortisationExpenseAndImpairmentLossesOfPropertyPlantAnd"
                    "EquipmentAndIntangibleAssetsRecognisedInProfitOrLoss"
                ),
                "other_finance_income": "OtherFinanceIncome",
                "other_finance_expenses": "OtherFinanceExpenses",
                "tax_expense": "TaxExpense",
            }

            instant_elements = {
                # Balance sheet items (use instant context)
                "total_assets": "Assets",
                "total_equity": "Equity",
                "noncurrent_assets": "NoncurrentAssets",
                "current_assets": "CurrentAssets",
                "contributed_capital": "ContributedCapital",
                "cash_and_cash_equivalents": "CashAndCashEquivalents",
                "liabilities_other_than_provisions": "LiabilitiesOtherThanProvisions",
                "shortterm_liabilities_other_than_provisions": (
                    "ShorttermLiabilitiesOtherThanProvisions"
                ),
                "longterm_liabilities_other_than_provisions": (
                    "LongtermLiabilitiesOtherThanProvisions"
                ),
                "provisions": "Provisions",
                "property_plant_equipment": "PropertyPlantAndEquipment",
            }

            # Extract period information for the data
            duration_period = contexts.get(current_duration_context, {})
            instant_period = contexts.get(current_instant_context, {})

            # Extract financial metrics for current period
            financial_metrics = {
                # Context information
                "duration_context": current_duration_context,
                "instant_context": current_instant_context,
                # Period dates (for income statement)
                "income_statement_start_date": duration_period.get("start_date"),
                "income_statement_end_date": duration_period.get("end_date"),
                # Balance sheet date
                "balance_sheet_date": instant_period.get("instant_date"),
            }

            # Extract duration elements (income statement) using duration context
            for metric_name, element_name in duration_elements.items():
                for elem in root.iter():
                    tag_name = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag

                    if (
                        tag_name == element_name
                        and elem.get("contextRef") == current_duration_context
                        and elem.text
                        and elem.text.strip()
                    ):
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
                    tag_name = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag

                    if (
                        tag_name == element_name
                        and elem.get("contextRef") == current_instant_context
                        and elem.text
                        and elem.text.strip()
                    ):
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
            return {"parse_error": str(e)}

    @timed(name="Processing financial data")
    def _process_financial_data(self, financial_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process and structure financial documents data.

        Args:
            financial_data: Raw financial documents data

        Returns:
            Processed financial data
        """
        self.log.info("Processing financial documents data")

        financial_results = financial_data["results"]

        # Process each company's financial data
        processed_financial = []

        for cvr_number, company_financial in financial_results.items():
            # Add processing metadata
            company_financial["processing_timestamp"] = datetime.now().isoformat()
            company_financial["pipeline_run_id"] = self.date_pattern
            company_financial["processing_step"] = CVREnrichmentStep.FINANCIAL_DOCUMENTS.value
            # company_financial["batch_number"] = self.config.batch_number  # No batching

            # Calculate summary statistics
            documents = company_financial.get("documents", [])
            xml_documents = [d for d in documents if d.get("download_success")]

            company_financial["xml_document_count"] = len(xml_documents)
            company_financial["total_xml_size_bytes"] = sum(
                d.get("xml_size_bytes", 0) for d in xml_documents
            )

            # Extract latest financial metrics if available
            latest_metrics = None
            latest_date = None

            for doc in xml_documents:
                if doc.get("financial_metrics") and doc["financial_metrics"].get("parse_success"):
                    doc_date = doc.get("reporting_period", {}).get("end_date")
                    if doc_date and (not latest_date or doc_date > latest_date):
                        latest_date = doc_date
                        latest_metrics = doc["financial_metrics"]

            company_financial["latest_financial_metrics"] = latest_metrics
            company_financial["latest_reporting_date"] = latest_date

            processed_financial.append(company_financial)

        # Create summary
        summary = {
            "total_companies": len(financial_results),
            "companies_with_documents": len(processed_financial),
            "total_documents": sum(c.get("document_count", 0) for c in processed_financial),
            "total_xml_documents": sum(c.get("xml_document_count", 0) for c in processed_financial),
            "companies_with_financial_metrics": len(
                [c for c in processed_financial if c.get("latest_financial_metrics")]
            ),
            # "batch_number": self.config.batch_number,  # No batching
            # "total_batches": self.config.total_batches,  # No batching
            "processing_timestamp": datetime.now().isoformat(),
            "api_summary": financial_data["summary"],
        }

        processed_data = {
            "financial_documents": processed_financial,
            "summary": summary,
        }

        self.log.info(
            f"Processed {summary['companies_with_documents']} companies with financial documents "
            f"({summary['total_xml_documents']} XML documents, "
            f"{summary['companies_with_financial_metrics']} with parsed metrics)"
        )

        return processed_data

    @timed(name="Saving financial data")
    def _save_financial_data(self, processed_data: Dict[str, Any]) -> str:
        """
        Save processed financial documents data to GCS using batch processing to avoid memory
        issues. Now creates both document metadata table AND comprehensive financial table.

        Args:
            processed_data: Processed financial data

        Returns:
            Table name where data was saved
        """
        self.log.info("Saving financial documents data with comprehensive financial metrics")

        # Create table names
        metadata_table = "cvr_financial_documents"  # Document metadata (for compatibility)
        financial_table = "cvr_financial_statements"  # Comprehensive financial data (NEW!)

        financial_data = processed_data["financial_documents"]

        # Create DuckDB tables
        self.conn.execute(f"DROP TABLE IF EXISTS {metadata_table}")
        self.conn.execute(f"DROP TABLE IF EXISTS {financial_table}")

        if financial_data:
            self.log.info(
                f"🔄 Processing {len(financial_data)} companies in batches to save both metadata and comprehensive financial data"
            )

            # Process in smaller batches for memory efficiency
            batch_size = 25  # Reduced from 50 due to additional financial data
            total_companies = len(financial_data)
            num_batches = (total_companies + batch_size - 1) // batch_size

            # Create document metadata table (existing functionality)
            self.conn.execute(f"""
                CREATE TABLE {metadata_table} (
                    cvr_number INTEGER,
                    company_name VARCHAR,
                    document_count INTEGER,
                    xml_document_count INTEGER,
                    total_xml_size_bytes INTEGER,
                    latest_reporting_date VARCHAR,
                    has_financial_metrics BOOLEAN,
                    processing_timestamp VARCHAR,
                    batch_number INTEGER
                )
            """)

            # Create comprehensive financial statements table (NEW!)
            self.conn.execute(f"""
                CREATE TABLE {financial_table} (
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

            # Process each batch
            for batch_idx in range(num_batches):
                start_idx = batch_idx * batch_size
                end_idx = min(start_idx + batch_size, total_companies)
                batch_data = financial_data[start_idx:end_idx]

                try:
                    # Convert batch to JSON strings for DuckDB
                    json_strings = [json.dumps(financial) for financial in batch_data]

                    # Insert into document metadata table (existing functionality)
                    self.conn.execute(
                        f"""
                        INSERT INTO {metadata_table}
                        SELECT
                            json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                            json_extract(json_data, '$.company_name')::VARCHAR as company_name,
                            json_extract(json_data, '$.document_count')::INTEGER as document_count,
                            json_extract(json_data, '$.xml_document_count')::INTEGER as
                                xml_document_count,
                            json_extract(json_data, '$.total_xml_size_bytes')::INTEGER as
                                total_xml_size_bytes,
                            json_extract(json_data, '$.latest_reporting_date')::VARCHAR as
                                latest_reporting_date,
                            CASE
                                WHEN json_extract(json_data, '$.latest_financial_metrics')
                             IS NOT NULL
                                THEN true
                                ELSE false
                            END as has_financial_metrics,
                            json_extract(json_data, '$.processing_timestamp')::VARCHAR as
                                processing_timestamp,
                            json_extract(json_data, '$.batch_number')::INTEGER as batch_number
                        FROM unnest($1) as t(json_data)
                    """,
                        [json_strings],
                    )

                    # Insert into comprehensive financial statements table (NEW!)
                    self.conn.execute(
                        f"""
                        INSERT INTO {financial_table}
                        SELECT
                            md5(json_extract(json_data, '$.cvr_number')::VARCHAR)::VARCHAR as company_uuid,
                            json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                            'Annual Report' as publication_type,
                            json_extract(json_data, '$.latest_reporting_date')::VARCHAR as publication_time,
                            NULL as case_number,
                            json_extract(json_data, '$.latest_reporting_date')::VARCHAR as reporting_period_start,
                            json_extract(json_data, '$.latest_reporting_date')::VARCHAR as reporting_period_end,
                            json_extract(json_data, '$.document_count')::INTEGER as document_count,
                            json_extract(json_data, '$.total_xml_size_bytes')::INTEGER as xml_size_bytes,
                            true as download_success,
                            'duration' as duration_context,
                            'instant' as instant_context,
                            json_extract(json_data, '$.latest_reporting_date')::VARCHAR as income_statement_start_date,
                            json_extract(json_data, '$.latest_reporting_date')::VARCHAR as income_statement_end_date,
                            json_extract(json_data, '$.latest_reporting_date')::VARCHAR as balance_sheet_date,
                            -- Extract financial metrics from parsed XBRL data
                            COALESCE(
                                TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.net_profit_loss') AS DOUBLE),
                                TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.ResultatEfterSkat') AS DOUBLE),
                                TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.ProfitLoss') AS DOUBLE)
                            ) as net_profit_loss,
                            COALESCE(
                                TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.gross_profit_loss') AS DOUBLE),
                                TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.BruttoResultat') AS DOUBLE)
                            ) as gross_profit_loss,
                            COALESCE(
                                TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.operating_profit_loss') AS DOUBLE),
                                TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.ResultatAfPrimæreDrift') AS DOUBLE)
                            ) as operating_profit_loss,
                            COALESCE(
                                TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.profit_loss_before_tax') AS DOUBLE),
                                TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.ResultatFørSkat') AS DOUBLE)
                            ) as profit_loss_before_tax,
                            COALESCE(
                                TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.employee_benefits_expense') AS DOUBLE),
                                TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.PersonaleOmkostninger') AS DOUBLE)
                            ) as employee_benefits_expense,
                            COALESCE(
                                TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.average_number_of_employees') AS DOUBLE),
                                TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.GennemsnitligtAntalMedarbejdere') AS DOUBLE)
                            ) as average_number_of_employees,
                            COALESCE(
                                TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.depreciation_expense') AS DOUBLE),
                                TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.Afskrivninger') AS DOUBLE)
                            ) as depreciation_expense,
                            TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.other_finance_income') AS DOUBLE) as other_finance_income,
                            TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.other_finance_expenses') AS DOUBLE) as other_finance_expenses,
                            COALESCE(
                                TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.tax_expense') AS DOUBLE),
                                TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.Skat') AS DOUBLE)
                            ) as tax_expense,
                            COALESCE(
                                TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.total_assets') AS DOUBLE),
                                TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.AktiverIAlt') AS DOUBLE),
                                TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.Assets') AS DOUBLE)
                            ) as total_assets,
                            COALESCE(
                                TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.total_equity') AS DOUBLE),
                                TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.EgenkapitalIAlt') AS DOUBLE),
                                TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.Equity') AS DOUBLE)
                            ) as total_equity,
                            COALESCE(
                                TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.noncurrent_assets') AS DOUBLE),
                                TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.AnlægsAktiver') AS DOUBLE)
                            ) as noncurrent_assets,
                            COALESCE(
                                TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.current_assets') AS DOUBLE),
                                TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.OmsætningsAktiver') AS DOUBLE)
                            ) as current_assets,
                            COALESCE(
                                TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.cash_and_cash_equivalents') AS DOUBLE),
                                TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.LikvideBehold') AS DOUBLE),
                                TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.Cash') AS DOUBLE)
                            ) as cash_and_cash_equivalents,
                            COALESCE(
                                TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.liabilities_other_than_provisions') AS DOUBLE),
                                TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.GældIAlt') AS DOUBLE)
                            ) as liabilities_other_than_provisions,
                            TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.shortterm_liabilities_other_than_provisions') AS DOUBLE) as shortterm_liabilities_other_than_provisions,
                            TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.longterm_liabilities_other_than_provisions') AS DOUBLE) as longterm_liabilities_other_than_provisions,
                            TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.provisions') AS DOUBLE) as provisions,
                            COALESCE(
                                TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.property_plant_equipment') AS DOUBLE),
                                TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.MaterialeAktiver') AS DOUBLE)
                            ) as property_plant_equipment,
                            TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.contributed_capital') AS DOUBLE) as contributed_capital,
                            -- Calculate ratios from available data
                            CASE 
                                WHEN TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.total_assets') AS DOUBLE) > 0 
                                THEN TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.total_equity') AS DOUBLE) / 
                                     TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.total_assets') AS DOUBLE)
                                ELSE NULL
                            END as equity_ratio,
                            CASE 
                                WHEN TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.average_number_of_employees') AS DOUBLE) > 0 
                                THEN TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.net_profit_loss') AS DOUBLE) / 
                                     TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.average_number_of_employees') AS DOUBLE)
                                ELSE NULL
                            END as profit_per_employee,
                            CASE 
                                WHEN TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.total_assets') AS DOUBLE) > 0 
                                THEN TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.net_profit_loss') AS DOUBLE) / 
                                     TRY_CAST(json_extract(json_data, '$.latest_financial_metrics.total_assets') AS DOUBLE)
                                ELSE NULL
                            END as return_on_assets
                        FROM unnest($1) as t(json_data)
                        WHERE json_extract(json_data, '$.latest_financial_metrics') IS NOT NULL
                    """,
                        [json_strings],
                    )

                    # Clear batch data from memory
                    json_strings = None
                    batch_data = None

                    # Memory cleanup after each batch
                    self._cleanup_memory_after_batch()

                    self.log.info(f"✅ Processed batch {batch_idx + 1}/{num_batches}")

                except Exception as e:
                    self.log.error(f"Error processing batch {batch_idx + 1}: {e}")
                    if "Out of Memory" in str(e) or "memory" in str(e).lower():
                        self.log.warning("⚠️ Memory exhaustion detected - stopping processing")
                        break
                    raise

        else:
            # Create empty tables with schema
            self.conn.execute(f"""
                CREATE TABLE {metadata_table} (
                    cvr_number INTEGER,
                    company_name VARCHAR,
                    document_count INTEGER,
                    xml_document_count INTEGER,
                    total_xml_size_bytes INTEGER,
                    latest_reporting_date VARCHAR,
                    has_financial_metrics BOOLEAN,
                    processing_timestamp VARCHAR,
                    batch_number INTEGER
                )
            """)

            self.conn.execute(f"""
                CREATE TABLE {financial_table} (
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
            self.log.info(f"Created empty tables {metadata_table} and {financial_table}")

        # Save both tables to GCS
        # Save document metadata table (for compatibility)
        self._save_data(
            data=metadata_table,
            dataset=self.config.dataset,
            bucket=self.config.bucket,
            stage="gold",
            filename="financial_documents.parquet",
        )

        # Save comprehensive financial statements table (NEW!)
        self._save_data(
            data=financial_table,
            dataset=f"{self.config.dataset}_statements",  # Different dataset name
            bucket=self.config.bucket,
            stage="gold",
            filename="financial_statements.parquet",
        )

        # Also save locally for GitHub Actions artifact sharing
        import os

        if os.getenv("GITHUB_ACTIONS") == "true":
            self.log.info(
                "GitHub Actions detected - saving financial data locally for artifact sharing"
            )
            # Save both tables locally
            local_metadata_path = "/tmp/cvr_financial_documents.parquet"
            local_statements_path = "/tmp/cvr_financial_statements.parquet"

            self.conn.execute(f"COPY {metadata_table} TO '{local_metadata_path}' (FORMAT PARQUET)")
            self.conn.execute(
                f"COPY {financial_table} TO '{local_statements_path}' (FORMAT PARQUET)"
            )

            self.log.info(
                f"Saved financial data locally: {local_metadata_path} and {local_statements_path}"
            )

        # Log final statistics
        metadata_count = self.conn.execute(f"SELECT COUNT(*) FROM {metadata_table}").fetchone()[0]
        financial_count = self.conn.execute(f"SELECT COUNT(*) FROM {financial_table}").fetchone()[0]

        self.log.info("✅ Created financial tables:")
        self.log.info(f"   • Document metadata: {metadata_count:,} companies")
        self.log.info(f"   • Comprehensive financial data: {financial_count:,} companies")

        # Save summary data separately
        self._save_summary_data(processed_data["summary"])

        return metadata_table  # Return metadata table for compatibility

    def _save_summary_data(self, summary: Dict[str, Any]) -> None:
        """Save processing summary data."""
        # No batching - single summary file
        summary_path = f"gold/{self.config.dataset}/{self.date_pattern}/financial_summary.json"

        self.gcs_access.upload_json(
            data=summary, gcs_path=f"gs://{self.config.bucket}/{summary_path}"
        )

        self.log.info(f"Saved processing summary to {summary_path}")

    def _process_employment_data_directly(self, companies_data: List[Dict]) -> None:
        """
        Process employment data directly to parquet files.
        Moved from data_consolidation.py to eliminate memory bottleneck.

        This processes 1.25M employment records in batches and saves them directly
        to separate parquet files, avoiding the memory accumulation that caused
        the 7.4GB memory usage in data_consolidation.
        """
        self.log.info(f"🔄 Processing employment data for {len(companies_data)} companies")

        if not companies_data:
            self.log.info("No companies to process for employment data")
            return

        # Set up crypto extension for UUID generation
        try:
            self.conn.execute("INSTALL crypto FROM community")
            self.conn.execute("LOAD crypto")
        except Exception as e:
            self.log.warning(f"Crypto extension already loaded: {e}")

        # Create company UUID function
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

        employment_types = [
            ("annual_employment", "annual"),
            ("quarterly_employment", "quarterly"),
            ("monthly_employment", "monthly"),
            ("replacement_monthly_employment", "replacement_monthly"),
        ]

        for employment_field, table_suffix in employment_types:
            self.log.info(f"📊 Processing {employment_field} data...")
            # Process in small batches to avoid memory issues
            self._process_employment_type_batched(companies_data, employment_field, table_suffix)

    def _process_employment_type_batched(
        self, companies_data: List[Dict], employment_field: str, table_suffix: str
    ):
        """Process one employment type in batches and save directly."""
        batch_size = 500  # Small batches to avoid memory accumulation
        table_name = f"cvr_financial_employment_{table_suffix}"

        # Drop existing table
        self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")

        total_companies = len(companies_data)
        num_batches = (total_companies + batch_size - 1) // batch_size
        total_records = 0

        # Process companies in batches
        for batch_idx in range(num_batches):
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, total_companies)
            batch_companies = companies_data[start_idx:end_idx]

            try:
                employment_records = self._extract_employment_records(
                    batch_companies, employment_field
                )
                if employment_records:
                    batch_record_count = self._save_employment_batch(
                        employment_records, table_name, batch_idx == 0
                    )
                    total_records += batch_record_count
                    self.log.info(
                        f"✅ Batch {batch_idx + 1}: {batch_record_count} {employment_field} records"
                    )

                # Clear batch data from memory
                employment_records = None
                batch_companies = None

                # Memory cleanup after each batch
                self._cleanup_memory_after_batch()

            except Exception as e:
                self.log.error(f"Error processing employment batch {batch_idx + 1}: {e}")
                if "Out of Memory" in str(e) or "memory" in str(e).lower():
                    self.log.warning(
                        "⚠️ Memory exhaustion detected - stopping employment processing"
                    )
                    break
                continue

        # Save employment table to GCS
        if total_records > 0:
            self._save_employment_table_to_gcs(table_name, table_suffix, total_records)
        else:
            self.log.info(f"No {employment_field} records found")

    def _extract_employment_records(
        self, companies: List[Dict], employment_field: str
    ) -> List[Dict]:
        """Extract employment records from company data for a specific employment type."""
        employment_records = []

        for company in companies:
            cvr_number = company.get("cvr_number")
            if not cvr_number:
                continue

            # Extract employment data from company JSON
            employment_data = company.get("employment_data", {})
            employment_list = employment_data.get(employment_field, [])

            if not employment_list:
                continue

            # Process each employment record
            for emp_record in employment_list:
                if not emp_record:
                    continue

                # Create employment record with UUID and proper field mapping
                employment_record = {
                    "employment_uuid": str(uuid.uuid4()),
                    "cvr_number": cvr_number,
                    "company_uuid": None,  # Will be calculated in SQL
                    # Map fields from raw employment data to expected schema
                    "year": emp_record.get("year"),
                    "month": emp_record.get("month"),
                    "quarter": emp_record.get("quarter"),
                    "employee_count": emp_record.get(
                        "total_employees"
                    ),  # Map total_employees -> employee_count
                    "full_time_employee_count": emp_record.get(
                        "full_time_equivalent"
                    ),  # Map full_time_equivalent -> full_time_employee_count
                    "fte_interval_code": emp_record.get("fte_interval_code"),
                    "employees_interval_code": emp_record.get("employees_interval_code"),
                    "owners_interval_code": emp_record.get("owners_interval_code"),
                    "last_updated": emp_record.get("last_updated"),
                    "employees_including_owners": emp_record.get("employees_including_owners"),
                    "employment_type": employment_field,  # Use the field type as employment_type
                    "unit": emp_record.get("unit"),
                    "reporting_period": emp_record.get("reporting_period"),
                }
                employment_records.append(employment_record)

        return employment_records

    def _save_employment_batch(
        self, employment_records: List[Dict], table_name: str, is_first_batch: bool
    ) -> int:
        """Save employment records batch to DuckDB table."""
        if not employment_records:
            return 0

        # Convert to JSON strings for DuckDB
        json_strings = [json.dumps(record) for record in employment_records]

        if is_first_batch:
            # Create table on first batch
            self.conn.execute(
                f"""
                CREATE TABLE {table_name} AS
                SELECT
                    json_extract(json_data, '$.employment_uuid')::VARCHAR as employment_uuid,
                    company_uuid(json_extract(json_data, '$.cvr_number')::INTEGER) as company_uuid,
                    json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                    json_extract(json_data, '$.year')::INTEGER as year,
                    json_extract(json_data, '$.employee_count')::INTEGER as employee_count,
                    json_extract(json_data, '$.full_time_employee_count')::INTEGER as
                        full_time_employee_count,
                    json_extract(json_data, '$.unit')::VARCHAR as unit,
                    json_extract(json_data, '$.quarter')::INTEGER as quarter,
                    json_extract(json_data, '$.month')::INTEGER as month,
                    json_extract(json_data, '$.reporting_period')::VARCHAR as reporting_period,
                    json_extract(json_data, '$.employment_type')::VARCHAR as employment_type,
                    '{self.date_pattern}' as processing_timestamp
                FROM unnest($1) as t(json_data)
            """,
                [json_strings],
            )
        else:
            # Insert into existing table
            self.conn.execute(
                f"""
                INSERT INTO {table_name}
                SELECT
                    json_extract(json_data, '$.employment_uuid')::VARCHAR as employment_uuid,
                    company_uuid(json_extract(json_data, '$.cvr_number')::INTEGER) as company_uuid,
                    json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                    json_extract(json_data, '$.year')::INTEGER as year,
                    json_extract(json_data, '$.employee_count')::INTEGER as employee_count,
                    json_extract(json_data, '$.full_time_employee_count')::INTEGER as
                        full_time_employee_count,
                    json_extract(json_data, '$.unit')::VARCHAR as unit,
                    json_extract(json_data, '$.quarter')::INTEGER as quarter,
                    json_extract(json_data, '$.month')::INTEGER as month,
                    json_extract(json_data, '$.reporting_period')::VARCHAR as reporting_period,
                    json_extract(json_data, '$.employment_type')::VARCHAR as employment_type,
                    '{self.date_pattern}' as processing_timestamp
                FROM unnest($1) as t(json_data)
            """,
                [json_strings],
            )

        return len(employment_records)

    def _save_employment_table_to_gcs(
        self, table_name: str, table_suffix: str, total_records: int
    ) -> None:
        """Save employment table to GCS."""
        self.log.info(f"💾 Saving {total_records} {table_suffix} employment records to GCS")

        # Save to GCS
        self._save_data(
            data=table_name,
            dataset=f"cvr_financial_employment_{table_suffix}",
            bucket=self.config.bucket,
            stage="gold",
            filename="data.parquet",
        )

        # Also save locally for GitHub Actions artifact sharing
        import os

        if os.getenv("GITHUB_ACTIONS") == "true":
            local_path = f"/tmp/cvr_financial_employment_{table_suffix}_data.parquet"
            self.conn.execute(f"COPY {table_name} TO '{local_path}' (FORMAT PARQUET)")
            self.log.info(f"Saved {table_suffix} employment data locally to {local_path}")

    def _cleanup_memory_after_batch(self) -> None:
        """Aggressive memory cleanup after processing a batch (copied from
        data_consolidation.py)."""
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
