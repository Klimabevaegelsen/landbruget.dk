"""
Address Geocoding Step - Step 5 of CVR Enrichment Pipeline

This step enriches all addresses (from companies and P-numbers) with geometry
information using the DAWA API and Datavask fallback, processing them in batches
for parallel execution.
"""

import json
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import Field

from unified_pipeline.common.base import BaseJobConfig, BaseSource, GoldJobInterface
from unified_pipeline.util.dawa_api_client import DAWAAPIClient
from unified_pipeline.util.timing import timed

from .shared.config import CVREnrichmentSharedConfig, CVREnrichmentStep, get_step_input_paths


class AddressGeocodingConfig(BaseJobConfig):
    """Configuration for address geocoding step."""

    name: str = "Address Geocoding"
    dataset: str = "cvr_enrichment"
    type: str = "address_geocoding"
    description: str = "Enrich addresses with geometry via DAWA API"
    frequency: str = "monthly"
    bucket: str = "landbrugsdata-raw-data"

    # Shared configuration
    shared_config: CVREnrichmentSharedConfig = Field(
        default_factory=CVREnrichmentSharedConfig,
        description="Shared configuration for CVR enrichment pipeline",
    )

    # Address geocoding specific configuration
    # Batch processing removed - now processes all addresses in single job
    # batch_number: Optional[int] = Field(
    #     default=None,
    #     description="Batch number for parallel processing (1-based)"
    # )
    #
    # total_batches: Optional[int] = Field(
    #     default=None,
    #     description="Total number of batches in this step"
    # )

    geocode_current_only: bool = Field(
        default=True, description="Whether to geocode only current addresses (not historical)"
    )

    max_addresses_per_batch: int = Field(
        default=1000, description="Maximum number of addresses to process per batch"
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


class AddressGeocoding(BaseSource[AddressGeocodingConfig], GoldJobInterface):
    """
    Address geocoding step implementation.

    This step:
    1. Loads company and P-number data from previous steps
    2. Extracts all addresses from both data sources
    3. Enriches addresses with geometry via DAWA API
    4. Falls back to Datavask API for failed geocoding
    5. Saves geocoded addresses for data consolidation step
    """

    def __init__(self, config: AddressGeocodingConfig):
        """
        Initialize address geocoding step.

        Args:
            config: Configuration for address geocoding
        """
        super().__init__(config)

        # Initialize DAWA API client
        self.dawa_client = DAWAAPIClient()

        self.log.info("Address geocoding step initialized")
        self.log.info("📋 Configuration:")
        self.log.info("   • Processing mode: Single job (no batching)")
        self.log.info(f"   • Geocode current only: {self.config.geocode_current_only}")
        self.log.info(f"   • Max addresses per batch: {self.config.max_addresses_per_batch}")

    @timed(name="Address geocoding processing")
    async def run(self, silver_data: Optional[Dict[str, Any]] = None) -> str:
        """
        Run the address geocoding process.

        Args:
            silver_data: Optional silver data (not used in this step)

        Returns:
            Table name containing geocoded addresses
        """
        self.log.info("Starting address geocoding step")

        try:
            # Step 1: Load and extract addresses from company and P-number data
            address_extraction = self._extract_addresses_from_data()

            # Step 2: Geocode addresses using DAWA API
            geocoding_results = await self._geocode_addresses(address_extraction)

            # Step 3: Process and structure geocoded data
            processed_data = self._process_geocoded_data(geocoding_results, address_extraction)

            # Step 4: Create comprehensive addresses table
            addresses_table = self._create_addresses_table(processed_data)
            
            # Step 5: Update company and pnumber tables with geocoding data
            self._update_company_table_with_geocoding(processed_data)
            self._update_pnumber_table_with_geocoding(processed_data)

            self.log.info(
                "Address geocoding completed successfully. Tables updated: companies, pnumbers, addresses"
            )
            return addresses_table

        except Exception as e:
            self.log.error(f"Address geocoding failed: {e}")
            raise

    @timed(name="Extracting addresses from data")
    def _extract_addresses_from_data(self) -> Dict[str, Any]:
        """
        Load company and P-number data and extract all addresses.

        Returns:
            Dictionary containing extracted addresses and metadata
        """
        if self.config.shared_config.enable_independent_execution:
            self.log.info(
                "Extracting addresses from latest available data (independent execution mode)"
            )
        else:
            self.log.info(
                "Extracting addresses from company and P-number data (pipeline dependency mode)"
            )

        # Get input paths for company and P-number data (with independent execution support)
        input_paths = get_step_input_paths(
            CVREnrichmentStep.ADDRESS_GEOCODING,
            self.date_pattern,
            total_batches=None,  # No batching
            bucket=self.config.bucket,
            enable_independent_execution=self.config.shared_config.enable_independent_execution,
            max_days_back=self.config.shared_config.max_days_back_for_inputs,
        )

        if not input_paths:
            if self.config.shared_config.enable_independent_execution:
                self.log.warning(
                    f"No company/P-number data found within "
                    f"{self.config.shared_config.max_days_back_for_inputs} days. "
                    f"Returning empty address data."
                )
            else:
                self.log.warning("No input paths found for address geocoding step")
            return {
                "addresses": [],
                "company_addresses": 0,
                "pnumber_addresses": 0,
                "total_addresses": 0,
            }

        all_addresses = []
        company_addresses = 0
        pnumber_addresses = 0

        # Process each input file
        for input_path in input_paths:
            self.log.info(f"Processing addresses from: {input_path}")

            try:
                # Determine if this is company or P-number data based on path
                is_company_data = "company" in input_path.lower()
                is_pnumber_data = "pnumber" in input_path.lower()

                if not (is_company_data or is_pnumber_data):
                    self.log.warning(f"Cannot determine data type for path: {input_path}")
                    continue

                # Check if running in GitHub Actions and use artifact data
                import os

                if os.getenv("GITHUB_ACTIONS") == "true":
                    # Use artifact data in GitHub Actions
                    if is_company_data:
                        artifact_path = "/tmp/cvr_company_data.parquet"
                        if os.path.exists(artifact_path):
                            self.log.info("Using company data from artifact")
                            local_path = artifact_path
                        else:
                            self.log.warning(f"Company artifact not found: {artifact_path}")
                            continue
                    elif is_pnumber_data:
                        artifact_path = "/tmp/cvr_pnumber_data.parquet"
                        if os.path.exists(artifact_path):
                            self.log.info("Using P-number data from artifact")
                            local_path = artifact_path
                        else:
                            self.log.warning(f"P-number artifact not found: {artifact_path}")
                            continue
                else:
                    # Use GCS temp download for local development
                    self.log.info(f"Local development - downloading from GCS: {input_path}")
                    with self.gcs_access._temp_download(input_path) as temp_file:
                        local_path = temp_file

                        if is_company_data:
                            # Load company data from temp file
                            result = self.conn.execute(
                                """
                                SELECT cvr_number, company_name, company_data_json
                                FROM read_parquet(?)
                                WHERE company_data_json IS NOT NULL
                            """,
                                [local_path],
                            ).fetchall()
                        elif is_pnumber_data:
                            # Load P-number data from temp file
                            result = self.conn.execute(
                                """
                                SELECT p_number, parent_cvr_number, unit_name, pnumber_data_json
                                FROM read_parquet(?)
                                WHERE pnumber_data_json IS NOT NULL
                            """,
                                [local_path],
                            ).fetchall()

                        # Process the results inside the context manager
                        if is_company_data:
                            for cvr_number, company_name, company_json in result:
                                try:
                                    company_data = json.loads(company_json)
                                    addresses = company_data.get("addresses", [])

                                    for addr in addresses:
                                        if self._should_geocode_address(addr):
                                            addr_record = self._create_address_record(
                                                addr, "company", cvr_number, company_name
                                            )
                                            all_addresses.append(addr_record)
                                            company_addresses += 1

                                except json.JSONDecodeError as e:
                                    self.log.warning(
                                        f"Failed to parse company data for CVR {cvr_number}: {e}"
                                    )
                                    continue

                        elif is_pnumber_data:
                            for p_number, parent_cvr, unit_name, pnumber_json in result:
                                try:
                                    pnumber_data = json.loads(pnumber_json)
                                    addresses = pnumber_data.get("addresses", [])

                                    for addr in addresses:
                                        if self._should_geocode_address(addr):
                                            addr_record = self._create_address_record(
                                                addr, "pnumber", parent_cvr, unit_name, p_number
                                            )
                                            all_addresses.append(addr_record)
                                            pnumber_addresses += 1

                                except json.JSONDecodeError as e:
                                    self.log.warning(
                                        f"Failed to parse P-number data for P-number {p_number}: {e}"
                                    )
                                    continue

                    # Continue to next file after processing this one
                    continue

                # GitHub Actions path - process artifact data
                if is_company_data:
                    # Load company data
                    result = self.conn.execute(
                        """
                        SELECT cvr_number, company_name, company_data_json
                        FROM read_parquet(?)
                        WHERE company_data_json IS NOT NULL
                    """,
                        [local_path],
                    ).fetchall()

                    for cvr_number, company_name, company_json in result:
                        try:
                            company_data = json.loads(company_json)
                            addresses = company_data.get("addresses", [])

                            for addr in addresses:
                                if self._should_geocode_address(addr):
                                    addr_record = self._create_address_record(
                                        addr, "company", cvr_number, company_name
                                    )
                                    all_addresses.append(addr_record)
                                    company_addresses += 1

                        except json.JSONDecodeError as e:
                            self.log.warning(
                                f"Failed to parse company data for CVR {cvr_number}: {e}"
                            )
                            continue

                elif is_pnumber_data:
                    # Load P-number data
                    result = self.conn.execute(
                        """
                        SELECT p_number, unit_name, parent_cvr_number, pnumber_data_json
                        FROM read_parquet(?)
                        WHERE pnumber_data_json IS NOT NULL
                    """,
                        [local_path],
                    ).fetchall()

                    for p_number, unit_name, parent_cvr, pnumber_json in result:
                        try:
                            pnumber_data = json.loads(pnumber_json)
                            addresses = pnumber_data.get("addresses", [])

                            for addr in addresses:
                                if self._should_geocode_address(addr):
                                    addr_record = self._create_address_record(
                                        addr, "pnumber", parent_cvr, unit_name, p_number
                                    )
                                    all_addresses.append(addr_record)
                                    pnumber_addresses += 1

                        except json.JSONDecodeError as e:
                            self.log.warning(
                                f"Failed to parse P-number data for P-number {p_number}: {e}"
                            )
                            continue

            except Exception as e:
                self.log.error(f"Failed to process addresses from {input_path}: {e}")
                continue

        # Apply test limit if configured
        if self.config.shared_config.test_limit is not None:
            # Limit addresses based on test_limit (approximate, since we're limiting by address count)
            max_addresses = self.config.shared_config.test_limit * 3  # Rough estimate: 3 addresses per company
            original_count = len(all_addresses)
            if original_count > max_addresses:
                all_addresses = all_addresses[:max_addresses]
                self.log.info(f"Applied test limit: processing {len(all_addresses)} addresses (limited from {original_count} due to test_limit={self.config.shared_config.test_limit})")
        
        # Process all addresses (no batching)
        batch_addresses = all_addresses
        self.log.info(f"Extracted {len(batch_addresses)} addresses from all sources")

        extraction_result = {
            "addresses": batch_addresses,
            "company_addresses": company_addresses,
            "pnumber_addresses": pnumber_addresses,
            "total_addresses": len(all_addresses),
            "batch_addresses": len(batch_addresses),
            "extraction_timestamp": datetime.now().isoformat(),
        }

        self.log.info(
            f"Address extraction completed: "
            f"{company_addresses} company addresses, "
            f"{pnumber_addresses} P-number addresses, "
            f"{len(batch_addresses)} in current batch"
        )

        return extraction_result

    def _should_geocode_address(self, addr: Dict[str, Any]) -> bool:
        """
        Determine if an address should be geocoded.

        Args:
            addr: Address record

        Returns:
            True if address should be geocoded
        """
        # Only geocode current addresses if configured
        if self.config.geocode_current_only and not addr.get("is_current", True):
            return False

        # Must have either adresse_id or full address
        if not (addr.get("adresse_id") or addr.get("full_address")):
            return False

        # Skip if already geocoded
        if addr.get("dawa_enriched") or addr.get("datavask_enriched"):
            return False

        return True

    def _create_address_record(
        self,
        addr: Dict[str, Any],
        source_type: str,
        cvr_number: int,
        entity_name: str,
        p_number: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Create a standardized address record for geocoding.

        Args:
            addr: Original address data
            source_type: "company" or "pnumber"
            cvr_number: CVR number
            entity_name: Company or unit name
            p_number: P-number (for P-number addresses)

        Returns:
            Standardized address record
        """
        return {
            "source_type": source_type,
            "cvr_number": cvr_number,
            "p_number": p_number,
            "entity_name": entity_name,
            "address_type": addr.get("address_type", "unknown"),
            "full_address": addr.get("full_address"),
            "street_name": addr.get("street_name"),
            "house_number": addr.get("house_number"),
            "floor": addr.get("floor"),
            "door": addr.get("door"),
            "postal_code": addr.get("postal_code"),
            "city": addr.get("city"),
            "municipality_code": addr.get("municipality_code"),
            "municipality_name": addr.get("municipality_name"),
            "country_code": addr.get("country_code"),
            "adresse_id": addr.get("adresse_id"),
            "period_start": addr.get("period_start"),
            "period_end": addr.get("period_end"),
            "is_current": addr.get("is_current", True),
            # Geocoding fields (to be populated)
            "latitude": None,
            "longitude": None,
            "coordinate_system": None,
            "srid": None,
            "geometry_wkt": None,
            "geometry_geojson": None,
            "coordinate_quality": None,
            "coordinate_source": None,
            "dawa_enriched": False,
            "datavask_enriched": False,
            "geocoding_attempted": False,
            "geocoding_timestamp": None,
        }

    @timed(name="Geocoding addresses")
    async def _geocode_addresses(self, address_extraction: Dict[str, Any]) -> Dict[str, Any]:
        """
        Geocode addresses using DAWA API with Datavask fallback.

        Args:
            address_extraction: Address extraction results

        Returns:
            Geocoding results
        """
        addresses = address_extraction["addresses"]

        self.log.info(f"Geocoding {len(addresses)} addresses")

        if not addresses:
            return {
                "geocoded_addresses": [],
                "summary": {
                    "total": 0,
                    "dawa_success": 0,
                    "datavask_success": 0,
                    "failed": 0,
                    "success_rate": 0.0,
                },
            }

        geocoded_addresses = []
        dawa_success = 0
        datavask_success = 0
        failed = 0

        from tqdm import tqdm

        for addr in tqdm(addresses, desc="Geocoding addresses", unit="address"):
            geocoded_addr = addr.copy()
            geocoded_addr["geocoding_attempted"] = True
            geocoded_addr["geocoding_timestamp"] = datetime.now().isoformat()

            geocoded = None

            # Try DAWA geocoding first if address has adresse_id
            if addr.get("adresse_id"):
                try:
                    geocoded = self.dawa_client.geocode_address_by_id(addr["adresse_id"])
                    if geocoded:
                        geocoded_addr.update(
                            {
                                "latitude": geocoded["latitude"],
                                "longitude": geocoded["longitude"],
                                "coordinate_system": geocoded.get("coordinate_system", "WGS84"),
                                "srid": geocoded.get("srid", 4326),
                                "geometry_wkt": self.dawa_client.create_geometry_wkt(
                                    geocoded["latitude"], geocoded["longitude"]
                                ),
                                "geometry_geojson": self.dawa_client.create_geometry_geojson(
                                    geocoded["latitude"], geocoded["longitude"]
                                ),
                                "coordinate_quality": geocoded.get("coordinate_quality"),
                                "coordinate_source": geocoded.get("coordinate_source"),
                                "dawa_enriched": True,
                                "dawa_fetch_timestamp": geocoded.get("dawa_fetch_timestamp"),
                            }
                        )
                        dawa_success += 1
                        self.log.debug(f"DAWA geocoded: {addr.get('full_address')}")
                except Exception as e:
                    self.log.debug(f"DAWA geocoding failed for {addr.get('full_address')}: {e}")

            # Fallback to Datavask API if DAWA failed and we have address text
            if not geocoded and addr.get("full_address"):
                try:
                    # Reconstruct complete address with postal code and city
                    complete_address = addr["full_address"]
                    if addr.get("postal_code") and addr.get("city"):
                        complete_address = (
                            f"{addr['full_address']}, {addr['postal_code']} {addr['city']}"
                        )

                    geocoded = self.dawa_client.geocode_with_datavask(complete_address)
                    if geocoded:
                        geocoded_addr.update(
                            {
                                "latitude": geocoded["latitude"],
                                "longitude": geocoded["longitude"],
                                "coordinate_system": geocoded.get("coordinate_system", "WGS84"),
                                "srid": geocoded.get("srid", 4326),
                                "geometry_wkt": self.dawa_client.create_geometry_wkt(
                                    geocoded["latitude"], geocoded["longitude"]
                                ),
                                "geometry_geojson": self.dawa_client.create_geometry_geojson(
                                    geocoded["latitude"], geocoded["longitude"]
                                ),
                                "coordinate_quality": geocoded.get("coordinate_quality"),
                                "coordinate_source": geocoded.get("coordinate_source"),
                                "datavask_enriched": True,
                                "dawa_fetch_timestamp": geocoded.get("dawa_fetch_timestamp"),
                            }
                        )
                        # Update BFE fields if available from Datavask
                        if geocoded.get("floor") is not None:
                            geocoded_addr["floor"] = geocoded["floor"]
                        if geocoded.get("door") is not None:
                            geocoded_addr["door"] = geocoded["door"]

                        datavask_success += 1
                        self.log.debug(f"Datavask geocoded: {complete_address}")
                except Exception as e:
                    self.log.debug(f"Datavask geocoding failed for {addr.get('full_address')}: {e}")

            # Mark as failed if no geocoding succeeded
            if not geocoded:
                failed += 1
                self.log.debug(f"Failed to geocode: {addr.get('full_address')}")

            geocoded_addresses.append(geocoded_addr)

        summary = {
            "total": len(addresses),
            "dawa_success": dawa_success,
            "datavask_success": datavask_success,
            "failed": failed,
            "success_rate": (dawa_success + datavask_success) / len(addresses) if addresses else 0,
        }

        self.log.info(
            f"Geocoding completed: "
            f"DAWA: {dawa_success}, Datavask: {datavask_success}, Failed: {failed} "
            f"({summary['success_rate']:.1%} success rate)"
        )

        return {"geocoded_addresses": geocoded_addresses, "summary": summary}

    @timed(name="Processing geocoded data")
    def _process_geocoded_data(
        self, geocoding_results: Dict[str, Any], address_extraction: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Process and structure geocoded addresses data.

        Args:
            geocoding_results: Geocoding results
            address_extraction: Address extraction results

        Returns:
            Processed geocoded data
        """
        self.log.info("Processing geocoded addresses data")

        geocoded_addresses = geocoding_results["geocoded_addresses"]

        # Add processing metadata to each address
        for addr in geocoded_addresses:
            addr["processing_timestamp"] = datetime.now().isoformat()
            addr["pipeline_run_id"] = self.date_pattern
            addr["processing_step"] = CVREnrichmentStep.ADDRESS_GEOCODING.value
            # addr["batch_number"] = self.config.batch_number  # No batching

        # Create summary
        summary = {
            "total_addresses_processed": len(geocoded_addresses),
            "company_addresses_extracted": address_extraction["company_addresses"],
            "pnumber_addresses_extracted": address_extraction["pnumber_addresses"],
            "batch_addresses_processed": address_extraction["batch_addresses"],
            "geocoding_summary": geocoding_results["summary"],
            # "batch_number": self.config.batch_number,  # No batching
            # "total_batches": self.config.total_batches,  # No batching
            "processing_timestamp": datetime.now().isoformat(),
        }

        processed_data = {
            "geocoded_addresses": geocoded_addresses,
            "summary": summary,
        }

        self.log.info(
            f"Processed {len(geocoded_addresses)} geocoded addresses "
            f"({summary['geocoding_summary']['success_rate']:.1%} geocoding success rate)"
        )

        return processed_data

    @timed(name="Creating addresses table")
    def _create_addresses_table(self, processed_data: Dict[str, Any]) -> str:
        """
        Create comprehensive addresses table with UUIDs and proper normalization.

        Args:
            processed_data: Processed geocoded data

        Returns:
            Table name where addresses were saved
        """
        self.log.info("Creating comprehensive addresses table")

        # Set up crypto extension for UUID generation
        try:
            self.conn.execute("INSTALL crypto FROM community")
            self.conn.execute("LOAD crypto")
        except Exception as e:
            self.log.warning(f"Crypto extension already loaded: {e}")
        
        # Create UUID functions
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
        
        self.conn.execute("""
            CREATE OR REPLACE FUNCTION pnumber_uuid(p_number) AS (
                SELECT CASE
                    WHEN p_number IS NULL
                    THEN NULL
                    ELSE CONCAT(
                        SUBSTR(crypto_hash('sha1', CONCAT('landbrugsdata-pnumber',
                               TRIM(CAST(p_number AS VARCHAR)))), 1, 8), '-',
                        SUBSTR(crypto_hash('sha1', CONCAT('landbrugsdata-pnumber',
                               TRIM(CAST(p_number AS VARCHAR)))), 9, 4), '-',
                        '5', SUBSTR(crypto_hash('sha1', CONCAT('landbrugsdata-pnumber',
                                      TRIM(CAST(p_number AS VARCHAR)))), 13, 3), '-',
                        CONCAT('8', SUBSTR(crypto_hash('sha1', CONCAT('landbrugsdata-pnumber',
                                               TRIM(CAST(p_number AS VARCHAR)))), 17, 3)), '-',
                        SUBSTR(crypto_hash('sha1', CONCAT('landbrugsdata-pnumber',
                               TRIM(CAST(p_number AS VARCHAR)))), 21, 12)
                    )
                END
            )
        """)

        # Create table name
        table_name = "cvr_addresses"
        addresses_data = processed_data["geocoded_addresses"]

        # Create DuckDB table
        self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")

        if addresses_data:
            # Convert to JSON strings for DuckDB
            json_strings = [json.dumps(addr) for addr in addresses_data]

            self.conn.execute(
                f"""
                CREATE TABLE {table_name} AS
                SELECT
                    uuid() as address_uuid,
                    json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                    json_extract(json_data, '$.p_number')::INTEGER as p_number,
                    company_uuid(json_extract(json_data, '$.cvr_number')::INTEGER) as company_uuid,
                    CASE 
                        WHEN json_extract(json_data, '$.p_number') IS NOT NULL 
                        THEN pnumber_uuid(json_extract(json_data, '$.p_number')::INTEGER)
                        ELSE NULL
                    END as pnumber_uuid,
                    json_extract(json_data, '$.address_type')::VARCHAR as address_type,
                    json_extract(json_data, '$.full_address')::VARCHAR as full_address,
                    json_extract(json_data, '$.street_name')::VARCHAR as street_name,
                    json_extract(json_data, '$.house_number')::VARCHAR as house_number,
                    json_extract(json_data, '$.postal_code')::VARCHAR as postal_code,
                    json_extract(json_data, '$.city')::VARCHAR as city,
                    json_extract(json_data, '$.municipality_code')::VARCHAR as municipality_code,
                    json_extract(json_data, '$.latitude')::DOUBLE as latitude,
                    json_extract(json_data, '$.longitude')::DOUBLE as longitude,
                    json_extract(json_data, '$.coordinate_quality')::VARCHAR as coordinate_quality,
                    json_extract(json_data, '$.dawa_enriched')::BOOLEAN as dawa_enriched,
                    json_extract(json_data, '$.geocoding_timestamp')::VARCHAR as geocoding_timestamp
                FROM unnest($1) as t(json_data)
            """,
                [json_strings],
            )

            self.log.info(f"Created addresses table with {len(addresses_data)} addresses")
        else:
            # Create empty table with schema
            self.conn.execute(f"""
                CREATE TABLE {table_name} (
                    address_uuid VARCHAR,
                    cvr_number INTEGER,
                    p_number INTEGER,
                    company_uuid VARCHAR,
                    pnumber_uuid VARCHAR,
                    address_type VARCHAR,
                    full_address VARCHAR,
                    street_name VARCHAR,
                    house_number VARCHAR,
                    postal_code VARCHAR,
                    city VARCHAR,
                    municipality_code VARCHAR,
                    latitude DOUBLE,
                    longitude DOUBLE,
                    coordinate_quality VARCHAR,
                    dawa_enriched BOOLEAN,
                    geocoding_timestamp VARCHAR
                )
            """)
            self.log.info("Created empty addresses table")

        # Save addresses table to GCS using standard CVR enrichment pattern
        self._save_data(
            data=table_name,
            dataset=self.config.dataset,  # cvr_enrichment
            bucket=self.config.bucket,
            stage="gold",
            filename="address_geocoding.parquet",  # Use step name as filename
        )
        
        # Save summary data separately
        self._save_summary_data(processed_data["summary"])

        return table_name
    
    def _update_company_table_with_geocoding(self, processed_data: Dict[str, Any]) -> None:
        """
        Update company table with primary address geocoding information.
        """
        self.log.info("Updating company table with geocoding data")
        
        # Load existing company table from GCS
        company_table = "cvr_companies_with_geocoding"
        
        # Get the company data path - find most recent company data
        from unified_pipeline.gold.cvr_enrichment.shared.config import _find_latest_file_with_pattern
        from unified_pipeline.util.gcs_access import GCSDataAccess
        
        gcs_access = GCSDataAccess()
        company_pattern = f"gs://{self.config.bucket}/gold/cvr_enrichment_companies/*/data.parquet"
        company_input_path = _find_latest_file_with_pattern(
            gcs_access, company_pattern, self.config.shared_config.max_days_back_for_inputs
        )
        
        if not company_input_path:
            self.log.warning(f"No company data found within {self.config.shared_config.max_days_back_for_inputs} days")
            return
        
        try:
            # Create table from GCS company data
            self.gcs_access.create_table_from_gcs("existing_companies", company_input_path)
            
            # 🐛 DEBUG: Check what columns we loaded
            columns_loaded = self.conn.execute("DESCRIBE existing_companies").fetchall()
            self.log.info(f"🔍 DEBUG: Loaded {len(columns_loaded)} columns from existing companies table:")
            for i, row in enumerate(columns_loaded[:10], 1):  # Show first 10
                col_name = row[0]
                col_type = row[1] if len(row) > 1 else "UNKNOWN"
                self.log.info(f"🔍 DEBUG:   {i:2d}. {col_name:<25} {col_type}")
            if len(columns_loaded) > 10:
                self.log.info(f"🔍 DEBUG:   ... and {len(columns_loaded) - 10} more columns")
            
            # Create geocoding lookup from address table
            # Get primary addresses for companies (first geocoded address per company)
            self.conn.execute("""
                CREATE OR REPLACE TABLE company_geocoding AS
                SELECT 
                    cvr_number,
                    latitude,
                    longitude,
                    coordinate_quality,
                    dawa_enriched,
                    ROW_NUMBER() OVER (PARTITION BY cvr_number ORDER BY geocoding_timestamp DESC) as rn
                FROM cvr_addresses
                WHERE cvr_number IS NOT NULL
                  AND p_number IS NULL
                  AND latitude IS NOT NULL
                  AND longitude IS NOT NULL
            """)
            
            # 🔧 FIX: Preserve all existing columns and only update geocoding fields
            # Get all column names from existing companies table
            existing_columns = [row[0] for row in self.conn.execute("DESCRIBE existing_companies").fetchall()]
            
            # Build SELECT clause that preserves all existing columns
            select_clauses = []
            geocoding_fields = {'latitude', 'longitude', 'coordinate_quality', 'dawa_enriched'}
            
            for col in existing_columns:
                if col in geocoding_fields:
                    # Use geocoding data if available, otherwise keep existing value
                    select_clauses.append(f"COALESCE(g.{col}, c.{col}) as {col}")
                else:
                    # Keep existing column as-is
                    select_clauses.append(f"c.{col}")
            
            select_clause = ",\n                    ".join(select_clauses)
            
            self.log.info(f"🔍 DEBUG: Creating table with {len(existing_columns)} columns (preserving all existing schema)")
            
            # Update companies with geocoding data while preserving all existing columns
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE {company_table} AS
                SELECT 
                    {select_clause}
                FROM existing_companies c
                LEFT JOIN (
                    SELECT * FROM company_geocoding WHERE rn = 1
                ) g ON c.cvr_number = g.cvr_number
            """)
            
            # 🐛 DEBUG: Check what columns we're saving
            columns_saving = self.conn.execute(f"DESCRIBE {company_table}").fetchall()
            self.log.info(f"🔍 DEBUG: Saving {len(columns_saving)} columns to companies table:")
            for i, row in enumerate(columns_saving[:10], 1):  # Show first 10
                col_name = row[0]
                col_type = row[1] if len(row) > 1 else "UNKNOWN"
                self.log.info(f"🔍 DEBUG:   {i:2d}. {col_name:<25} {col_type}")
            if len(columns_saving) > 10:
                self.log.info(f"🔍 DEBUG:   ... and {len(columns_saving) - 10} more columns")
            
            # Save updated company table back to GCS
            self._save_data(
                data=company_table,
                dataset="cvr_enrichment_companies",
                bucket=self.config.bucket,
                stage="gold",
                filename="data.parquet",
            )
            
            self.log.info("Updated company table with geocoding data")
            
        except Exception as e:
            self.log.error(f"Failed to update company table with geocoding: {e}")
            
    def _update_pnumber_table_with_geocoding(self, processed_data: Dict[str, Any]) -> None:
        """
        Update pnumber table with address geocoding information.
        """
        self.log.info("Updating pnumber table with geocoding data")
        
        # Load existing pnumber table from GCS
        pnumber_table = "cvr_pnumbers_with_geocoding"
        
        # Get the pnumber data path - find most recent pnumber data
        from unified_pipeline.gold.cvr_enrichment.shared.config import _find_latest_file_with_pattern
        from unified_pipeline.util.gcs_access import GCSDataAccess
        
        gcs_access = GCSDataAccess()
        pnumber_pattern = f"gs://{self.config.bucket}/gold/cvr_enrichment_pnumbers/*/data.parquet"
        pnumber_input_path = _find_latest_file_with_pattern(
            gcs_access, pnumber_pattern, self.config.shared_config.max_days_back_for_inputs
        )
        
        if not pnumber_input_path:
            self.log.warning(f"No pnumber data found within {self.config.shared_config.max_days_back_for_inputs} days")
            return
        
        try:
            # Create table from GCS pnumber data
            self.gcs_access.create_table_from_gcs("existing_pnumbers", pnumber_input_path)
            
            # 🐛 DEBUG: Check what columns we loaded
            columns_loaded = self.conn.execute("DESCRIBE existing_pnumbers").fetchall()
            self.log.info(f"🔍 DEBUG: Loaded {len(columns_loaded)} columns from existing P-numbers table:")
            for i, row in enumerate(columns_loaded[:10], 1):  # Show first 10
                col_name = row[0]
                col_type = row[1] if len(row) > 1 else "UNKNOWN"
                self.log.info(f"🔍 DEBUG:   {i:2d}. {col_name:<25} {col_type}")
            if len(columns_loaded) > 10:
                self.log.info(f"🔍 DEBUG:   ... and {len(columns_loaded) - 10} more columns")
            
            # Create geocoding lookup from address table
            # Get primary addresses for pnumbers (first geocoded address per pnumber)
            self.conn.execute("""
                CREATE OR REPLACE TABLE pnumber_geocoding AS
                SELECT 
                    p_number,
                    latitude,
                    longitude,
                    coordinate_quality,
                    dawa_enriched,
                    ROW_NUMBER() OVER (PARTITION BY p_number ORDER BY geocoding_timestamp DESC) as rn
                FROM cvr_addresses
                WHERE p_number IS NOT NULL
                  AND latitude IS NOT NULL
                  AND longitude IS NOT NULL
            """)
            
            # 🔧 FIX: Preserve all existing columns and only update geocoding fields
            # Get all column names from existing P-numbers table
            existing_columns = [row[0] for row in self.conn.execute("DESCRIBE existing_pnumbers").fetchall()]
            
            # Build SELECT clause that preserves all existing columns
            select_clauses = []
            geocoding_fields = {'latitude', 'longitude', 'coordinate_quality', 'dawa_enriched'}
            
            for col in existing_columns:
                if col in geocoding_fields:
                    # Use geocoding data if available, otherwise keep existing value
                    select_clauses.append(f"COALESCE(g.{col}, p.{col}) as {col}")
                else:
                    # Keep existing column as-is
                    select_clauses.append(f"p.{col}")
            
            select_clause = ",\n                    ".join(select_clauses)
            
            self.log.info(f"🔍 DEBUG: Creating P-number table with {len(existing_columns)} columns (preserving all existing schema)")
            
            # Update pnumbers with geocoding data while preserving all existing columns
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE {pnumber_table} AS
                SELECT 
                    {select_clause}
                FROM existing_pnumbers p
                LEFT JOIN (
                    SELECT * FROM pnumber_geocoding WHERE rn = 1
                ) g ON p.p_number = g.p_number
            """)
            
            # 🐛 DEBUG: Check what columns we're saving
            columns_saving = self.conn.execute(f"DESCRIBE {pnumber_table}").fetchall()
            self.log.info(f"🔍 DEBUG: Saving {len(columns_saving)} columns to P-numbers table:")
            for i, row in enumerate(columns_saving[:10], 1):  # Show first 10
                col_name = row[0]
                col_type = row[1] if len(row) > 1 else "UNKNOWN"
                self.log.info(f"🔍 DEBUG:   {i:2d}. {col_name:<25} {col_type}")
            if len(columns_saving) > 10:
                self.log.info(f"🔍 DEBUG:   ... and {len(columns_saving) - 10} more columns")
            
            # Save updated pnumber table back to GCS
            self._save_data(
                data=pnumber_table,
                dataset="cvr_enrichment_pnumbers",
                bucket=self.config.bucket,
                stage="gold",
                filename="data.parquet",
            )
            
            self.log.info("Updated pnumber table with geocoding data")
            
        except Exception as e:
            self.log.error(f"Failed to update pnumber table with geocoding: {e}")

    def _save_summary_data(self, summary: Dict[str, Any]) -> None:
        """Save processing summary data."""
        # No batching - single summary file
        summary_path = f"gold/{self.config.dataset}/{self.date_pattern}/address_summary.json"

        self.gcs_access.upload_json(
            data=summary, gcs_path=f"gs://{self.config.bucket}/{summary_path}"
        )

        self.log.info(f"Saved processing summary to {summary_path}")
