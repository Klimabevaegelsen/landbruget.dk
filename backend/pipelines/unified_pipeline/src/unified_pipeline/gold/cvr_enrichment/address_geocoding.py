"""
Address Geocoding Step - Step 5 of CVR Enrichment Pipeline

This step enriches all addresses (from companies and P-numbers) with geometry
information using the DAWA API and Datavask fallback, processing them in batches
for parallel execution.
"""

import json
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

            # Step 4: Save geocoded addresses
            table_name = self._save_geocoded_data(processed_data)

            self.log.info(f"Address geocoding completed successfully. Data saved to: {table_name}")
            return table_name

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

    @timed(name="Saving geocoded data")
    def _save_geocoded_data(self, processed_data: Dict[str, Any]) -> str:
        """
        Save processed geocoded addresses to GCS.

        Args:
            processed_data: Processed geocoded data

        Returns:
            Table name where data was saved
        """
        self.log.info("Saving geocoded addresses data")

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
                    json_extract(json_data, '$.source_type')::VARCHAR as source_type,
                    json_extract(json_data, '$.cvr_number')::INTEGER as cvr_number,
                    json_extract(json_data, '$.p_number')::INTEGER as p_number,
                    json_extract(json_data, '$.entity_name')::VARCHAR as entity_name,
                    json_extract(json_data, '$.address_type')::VARCHAR as address_type,
                    json_extract(json_data, '$.full_address')::VARCHAR as full_address,
                    json_extract(json_data, '$.postal_code')::VARCHAR as postal_code,
                    json_extract(json_data, '$.city')::VARCHAR as city,
                    json_extract(json_data, '$.latitude')::DOUBLE as latitude,
                    json_extract(json_data, '$.longitude')::DOUBLE as longitude,
                    json_extract(json_data, '$.geometry_wkt')::VARCHAR as geometry_wkt,
                    json_extract(json_data, '$.dawa_enriched')::BOOLEAN as dawa_enriched,
                    json_extract(json_data, '$.datavask_enriched')::BOOLEAN as datavask_enriched,
                    CASE 
                        WHEN json_extract(json_data, '$.latitude') IS NOT NULL 
                        THEN true 
                        ELSE false 
                    END as is_geocoded,
                    json_data as address_data_json,
                    json_extract(json_data, '$.processing_timestamp')::VARCHAR as processing_timestamp,
                    json_extract(json_data, '$.batch_number')::INTEGER as batch_number
                FROM unnest($1) as t(json_data)
            """,
                [json_strings],
            )

            self.log.info(f"Created table {table_name} with {len(addresses_data)} addresses")
        else:
            # Create empty table with schema
            self.conn.execute(f"""
                CREATE TABLE {table_name} (
                    source_type VARCHAR,
                    cvr_number INTEGER,
                    p_number INTEGER,
                    entity_name VARCHAR,
                    address_type VARCHAR,
                    full_address VARCHAR,
                    postal_code VARCHAR,
                    city VARCHAR,
                    latitude DOUBLE,
                    longitude DOUBLE,
                    geometry_wkt VARCHAR,
                    dawa_enriched BOOLEAN,
                    datavask_enriched BOOLEAN,
                    is_geocoded BOOLEAN,
                    address_data_json VARCHAR,
                    processing_timestamp VARCHAR,
                    batch_number INTEGER
                )
            """)
            self.log.info(f"Created empty table {table_name}")

        # Save to GCS with specific filename for data consolidation step
        from .shared.config import get_step_output_path

        output_path = get_step_output_path(
            CVREnrichmentStep.ADDRESS_GEOCODING, self.date_pattern, bucket=self.config.bucket
        )

        # Upload directly to the expected path
        self.gcs_access.upload_from_duckdb_table(table_name, output_path)

        # Save summary data separately
        self._save_summary_data(processed_data["summary"])

        return table_name

    def _save_summary_data(self, summary: Dict[str, Any]) -> None:
        """Save processing summary data."""
        # No batching - single summary file
        summary_path = f"gold/{self.config.dataset}/{self.date_pattern}/address_summary.json"

        self.gcs_access.upload_json(
            data=summary, gcs_path=f"gs://{self.config.bucket}/{summary_path}"
        )

        self.log.info(f"Saved processing summary to {summary_path}")
