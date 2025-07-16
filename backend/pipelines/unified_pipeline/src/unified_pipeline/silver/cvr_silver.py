"""
CVR Silver Layer Implementation

This module implements the silver layer data transformation for Danish CVR register data.
It processes raw bronze layer data from the CVR API and transforms it into
clean, structured silver layer data following the medallion architecture.

The module contains:
- CVRSilverConfig: Configuration class for the CVR silver transformation
- CVRSilver: Implementation class for transforming CVR data using DuckDB

The data transformation includes parsing nested JSON structures, extracting
company information, leadership data, ownership details, and financial documents.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from unified_pipeline.common.base import BaseJobConfig, BaseSource, SilverJobInterface
from unified_pipeline.util.timing import timed


class CVRSilverConfig(BaseJobConfig):
    """
    Configuration for CVR Silver data processing.

    This configuration defines parameters for transforming CVR data
    from raw (bronze) to structured (silver) format, including dataset names,
    storage parameters, and processing configurations.

    Attributes:
        dataset (str): Primary dataset name for silver data collection
        bucket (str): GCS bucket name for storing processed data
        parse_leadership (bool): Whether to parse leadership/board data
        parse_ownership (bool): Whether to parse ownership data
        parse_financial_docs (bool): Whether to parse financial documents
        max_historical_records (int): Maximum historical records to keep per entity
    """

    dataset: str = "cvr"
    bucket: str = "landbrugsdata-raw-data"
    parse_leadership: bool = True
    parse_ownership: bool = True
    parse_financial_docs: bool = True
    max_historical_records: int = 10


class CVRSilver(BaseSource[CVRSilverConfig], SilverJobInterface):
    """
    Silver layer processor for CVR register data using DuckDB.

    This class transforms raw CVR register data from the bronze layer into
    structured business data using DuckDB for all data operations.
    It handles company information, leadership, ownership, and financial documents.

    The processing includes:
    1. Reading raw data from GCS or in-memory bronze data
    2. Extracting company basic information
    3. Parsing nested JSON structures for leadership and ownership
    4. Processing financial documents metadata
    5. Saving processed data to GCS as structured tables
    """

    def __init__(self, config: CVRSilverConfig):
        """
        Initialize the CVRSilver processor.

        Args:
            config: Configuration for the silver processing job
        """
        super().__init__(config)
        # Setup DuckDB with JSON extension
        self._setup_duckdb_json()

    def _setup_duckdb_json(self):
        """Setup DuckDB connection with JSON extensions."""
        try:
            # Install and load JSON extension
            self.conn.execute("INSTALL json")
            self.conn.execute("LOAD json")
            self.log.info("✅ DuckDB-JSON initialized for CVR data processing")
        except Exception as e:
            self.log.error(f"Failed to setup DuckDB JSON extension: {e}")
            raise

    async def _find_latest_bronze_data(
        self, cvr_number: str, bronze_data: Optional[Dict] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Find and load the most recent bronze data for a CVR number.

        Args:
            cvr_number (str): CVR number identifier
            bronze_data (Optional[Dict]): In-memory bronze data from bronze stage

        Returns:
            Optional[Dict[str, Any]]: CVR data and metadata, or None if not found
        """
        # If we have in-memory bronze data, use it directly
        if bronze_data and cvr_number in bronze_data:
            self.log.info(f"Using in-memory bronze data for CVR {cvr_number}")
            return bronze_data[cvr_number]

        # Fallback to storage if no in-memory data
        try:
            # List all bronze data files for this CVR number
            pattern = f"gs://{self.config.bucket}/bronze/cvr/*/{cvr_number}_company.json"
            bronze_files = self.gcs_access.list_files(pattern)

            if not bronze_files:
                self.log.warning(f"No bronze data files found for CVR {cvr_number}")
                return None

            # Sort by path (which includes date) and get the most recent
            bronze_files.sort(reverse=True)
            latest_file = bronze_files[0]

            self.log.info(f"Loading latest bronze data from GCS: {latest_file}")

            # Load the company data
            company_data = self.gcs_access.download_json(latest_file)

            # Try to load financial documents
            financial_docs = []
            try:
                financial_file = latest_file.replace("_company.json", "_financial.json")
                financial_docs = self.gcs_access.download_json(financial_file)
            except Exception as e:
                self.log.debug(f"No financial documents for CVR {cvr_number}: {e}")

            # Try to load metadata
            metadata = None
            try:
                metadata_file = latest_file.replace("_company.json", "_metadata.json")
                metadata = self.gcs_access.download_json(metadata_file)
            except Exception as e:
                self.log.warning(f"Could not load metadata for CVR {cvr_number}: {e}")

            return {
                "cvr_number": cvr_number,
                "company_data": company_data,
                "financial_docs": financial_docs,
                "metadata": metadata,
            }

        except Exception as e:
            self.log.error(f"Failed to load bronze data from GCS for CVR {cvr_number}: {e}")
            return None

    @timed(name="Transforming CVR company data")
    def _transform_company_data(self, raw_data: Dict, cvr_number: str) -> Optional[Any]:
        """
        Transform raw CVR data into structured company information.

        Args:
            raw_data (Dict): Raw CVR data from API
            cvr_number (str): CVR number identifier

        Returns:
            Optional[Any]: DuckDB relation with transformed data, or None if transformation fails
        """
        if not raw_data or "company_data" not in raw_data:
            self.log.warning(f"No company data found for CVR {cvr_number}")
            return None

        try:
            company_data = raw_data["company_data"]

            # Extract basic company information
            company_record = {
                "cvr_number": int(cvr_number),
                "company_name": company_data.get("company_name"),
                "company_type": company_data.get("company_type"),
                "company_type_code": company_data.get("company_type_code"),
                "industry_text": company_data.get("industry_text"),
                "industry_code": company_data.get("industry_code"),
                "fetch_timestamp": company_data.get("fetch_timestamp"),
                "processing_timestamp": datetime.now().isoformat(),
            }

            # Create companies table
            self.conn.execute("""
                CREATE OR REPLACE TABLE cvr_companies (
                    cvr_number BIGINT,
                    company_name VARCHAR,
                    company_type VARCHAR,
                    company_type_code INTEGER,
                    industry_text VARCHAR,
                    industry_code VARCHAR,
                    fetch_timestamp TIMESTAMP,
                    processing_timestamp TIMESTAMP
                )
            """)

            # Insert company data
            self.conn.execute(
                """
                INSERT INTO cvr_companies VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
                [
                    company_record["cvr_number"],
                    company_record["company_name"],
                    company_record["company_type"],
                    company_record["company_type_code"],
                    company_record["industry_text"],
                    company_record["industry_code"],
                    company_record["fetch_timestamp"],
                    company_record["processing_timestamp"],
                ],
            )

            # Process address data
            address_data = company_data.get("address", {})
            if address_data:
                self._process_address_data(cvr_number, address_data)

            # Process leadership data if enabled
            if self.config.parse_leadership:
                raw_cvr_data = company_data.get("raw_data", {})
                if raw_cvr_data:
                    self._process_leadership_data(cvr_number, raw_cvr_data)

            # Process ownership data if enabled
            if self.config.parse_ownership:
                raw_cvr_data = company_data.get("raw_data", {})
                if raw_cvr_data:
                    self._process_ownership_data(cvr_number, raw_cvr_data)

            # Process financial documents if enabled
            if self.config.parse_financial_docs and raw_data.get("financial_docs"):
                self._process_financial_documents(cvr_number, raw_data["financial_docs"])

            return self.conn.execute("SELECT * FROM cvr_companies").fetchdf()

        except Exception as e:
            self.log.error(f"Failed to transform company data for CVR {cvr_number}: {e}")
            return None

    def _process_address_data(self, cvr_number: str, address_data: Dict) -> None:
        """Process and store address data."""
        try:
            # Create addresses table
            self.conn.execute("""
                CREATE OR REPLACE TABLE cvr_addresses (
                    cvr_number BIGINT,
                    street_name VARCHAR,
                    house_number INTEGER,
                    letter VARCHAR,
                    postal_code INTEGER,
                    city VARCHAR,
                    district VARCHAR,
                    country_code VARCHAR,
                    formatted_address VARCHAR,
                    processing_timestamp TIMESTAMP
                )
            """)

            # Insert address data
            self.conn.execute(
                """
                INSERT INTO cvr_addresses VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                [
                    int(cvr_number),
                    address_data.get("street_name"),
                    address_data.get("house_number"),
                    address_data.get("letter"),
                    address_data.get("postal_code"),
                    address_data.get("city"),
                    address_data.get("district"),
                    address_data.get("country_code"),
                    address_data.get("formatted_address"),
                    datetime.now().isoformat(),
                ],
            )

        except Exception as e:
            self.log.error(f"Failed to process address data for CVR {cvr_number}: {e}")

    def _process_leadership_data(self, cvr_number: str, raw_data: Dict) -> None:
        """Process and store leadership/board data."""
        try:
            # Create leadership table
            self.conn.execute("""
                CREATE OR REPLACE TABLE cvr_leadership (
                    cvr_number BIGINT,
                    person_name VARCHAR,
                    organization_name VARCHAR,
                    role VARCHAR,
                    valid_from DATE,
                    valid_to DATE,
                    processing_timestamp TIMESTAMP
                )
            """)

            # Parse deltagerRelation for leadership data
            deltager_relations = raw_data.get("deltagerRelation", [])

            for relation in deltager_relations:
                deltager = relation.get("deltager", {})

                # Get person name
                person_name = None
                for name_entry in deltager.get("navne", []):
                    person_name = name_entry.get("navn")
                    break

                if not person_name:
                    continue

                # Get organization roles
                for org in relation.get("organisationer", []):
                    org_name = None
                    for org_name_entry in org.get("organisationsNavn", []):
                        org_name = org_name_entry.get("navn")
                        break

                    if org_name in ["Direktion", "Bestyrelse"]:
                        # Get validity period
                        valid_from = None
                        valid_to = None
                        for period in org.get("medlemsData", []):
                            periode = period.get("periode", {})
                            valid_from = periode.get("gyldigFra")
                            valid_to = periode.get("gyldigTil")
                            break

                        # Get role
                        role = None
                        for attr in org.get("attributter", []):
                            if attr.get("type") == "FUNKTION":
                                for value_entry in attr.get("vaerdier", []):
                                    role = value_entry.get("vaerdi")
                                    break

                        # Insert leadership record
                        self.conn.execute(
                            """
                            INSERT INTO cvr_leadership VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                            [
                                int(cvr_number),
                                person_name,
                                org_name,
                                role,
                                valid_from,
                                valid_to,
                                datetime.now().isoformat(),
                            ],
                        )

        except Exception as e:
            self.log.error(f"Failed to process leadership data for CVR {cvr_number}: {e}")

    def _process_ownership_data(self, cvr_number: str, raw_data: Dict) -> None:
        """Process and store ownership data."""
        try:
            # Create ownership table
            self.conn.execute("""
                CREATE OR REPLACE TABLE cvr_ownership (
                    cvr_number BIGINT,
                    owner_name VARCHAR,
                    owner_type VARCHAR,
                    ownership_percentage DECIMAL(5,2),
                    valid_from DATE,
                    valid_to DATE,
                    processing_timestamp TIMESTAMP
                )
            """)

            # Parse deltagerRelation for ownership data
            deltager_relations = raw_data.get("deltagerRelation", [])

            for relation in deltager_relations:
                deltager = relation.get("deltager", {})

                # Get owner name
                owner_name = None
                for name_entry in deltager.get("navne", []):
                    owner_name = name_entry.get("navn")
                    break

                if not owner_name:
                    continue

                # Check for ownership (EJERREGISTER)
                for org in relation.get("organisationer", []):
                    org_name = None
                    for org_name_entry in org.get("organisationsNavn", []):
                        org_name = org_name_entry.get("navn")
                        break

                    if org_name == "EJERREGISTER":
                        # Get validity period
                        valid_from = None
                        valid_to = None
                        for period in org.get("medlemsData", []):
                            periode = period.get("periode", {})
                            valid_from = periode.get("gyldigFra")
                            valid_to = periode.get("gyldigTil")
                            break

                        # Get ownership percentage
                        ownership_pct = None
                        for attr in org.get("attributter", []):
                            if attr.get("type") == "EJERANDEL_PROCENT":
                                for value_entry in attr.get("vaerdier", []):
                                    ownership_pct = value_entry.get("vaerdi")
                                    break

                        # Insert ownership record
                        self.conn.execute(
                            """
                            INSERT INTO cvr_ownership VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                            [
                                int(cvr_number),
                                owner_name,
                                "UNKNOWN",  # Would need to determine from data
                                float(ownership_pct) if ownership_pct else None,
                                valid_from,
                                valid_to,
                                datetime.now().isoformat(),
                            ],
                        )

        except Exception as e:
            self.log.error(f"Failed to process ownership data for CVR {cvr_number}: {e}")

    def _process_financial_documents(self, cvr_number: str, financial_docs: List[Dict]) -> None:
        """Process and store financial documents metadata."""
        try:
            # Create financial documents table
            self.conn.execute("""
                CREATE OR REPLACE TABLE cvr_financial_documents (
                    cvr_number BIGINT,
                    case_number VARCHAR,
                    publication_type VARCHAR,
                    publication_time TIMESTAMP,
                    reporting_period_start DATE,
                    reporting_period_end DATE,
                    document_count INTEGER,
                    processing_timestamp TIMESTAMP
                )
            """)

            for doc in financial_docs:
                reporting_period = doc.get("reporting_period", {})
                documents = doc.get("documents", [])

                self.conn.execute(
                    """
                    INSERT INTO cvr_financial_documents VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    [
                        int(cvr_number),
                        doc.get("case_number"),
                        doc.get("publication_type"),
                        doc.get("publication_time"),
                        reporting_period.get("start_date"),
                        reporting_period.get("end_date"),
                        len(documents),
                        datetime.now().isoformat(),
                    ],
                )

        except Exception as e:
            self.log.error(f"Failed to process financial documents for CVR {cvr_number}: {e}")

    async def run(self, bronze_data: Optional[Any] = None) -> Optional[Any]:
        """
        Run silver processing for CVR data.

        Args:
            bronze_data: Optional in-memory bronze data from bronze stage

        Returns:
            Optional[Any]: Processed silver data, or None if processing fails
        """
        try:
            self.log.info("Starting CVR silver processing")

            # If we have in-memory bronze data, process it
            if bronze_data:
                self.log.info(f"Processing {len(bronze_data)} CVR records from bronze stage")

                results = {}
                for cvr_number, cvr_data in bronze_data.items():
                    result = self._transform_company_data(cvr_data, cvr_number)
                    if result is not None:
                        results[cvr_number] = result

                if results:
                    self.log.info(f"Successfully processed {len(results)} CVR records")
                    return results
                else:
                    self.log.warning("No CVR records were successfully processed")
                    return None

            # Fallback to processing from storage
            self.log.info("No in-memory bronze data, processing from storage")

            # This would require configuration of which CVR numbers to process
            # For now, return None to indicate no processing was done
            self.log.warning(
                "Storage-based processing not implemented - requires CVR numbers configuration"
            )
            return None

        except Exception as e:
            self.log.error(f"Error in CVR silver processing: {e}")
            return None
