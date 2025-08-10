"""
CVR API Client

Client for accessing the Danish CVR register via distribution.virk.dk API.
Based on the working code provided and the official documentation at:
https://datacvr.virk.dk/artikel/system-til-system-adgang-til-cvr-data

The API uses Elasticsearch-style queries with HTTP Basic Authentication.
"""

import os
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
from requests.auth import HTTPBasicAuth
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential
from tqdm import tqdm

from unified_pipeline.util.cvr_pii_filter import (
    filter_cvr_pii,
)
from unified_pipeline.util.dawa_api_client import DAWAAPIClient
from unified_pipeline.util.log_util import Logger


class CVRAPIClient:
    """
    Client for accessing the Danish CVR register API.

    Provides methods to:
    - Fetch company data by CVR number
    - Fetch financial reports and documents
    - Handle authentication and rate limiting
    """

    def __init__(
        self, 
        username: Optional[str] = None, 
        password: Optional[str] = None, 
        enable_geocoding: bool = True, 
        geocode_current_only: bool = True
    ):
        """
        Initialize CVR API client.

        Args:
            username: CVR API username (defaults to environment variable)
            password: CVR API password (defaults to environment variable)
            enable_geocoding: Whether to enable address geocoding via DAWA API
            geocode_current_only: Whether to geocode only current addresses (not 
                historical)
        """
        self.log = Logger.get_logger()

        # Get credentials from environment if not provided
        self.username = username or os.getenv("CVR_USERNAME")
        self.password = password or os.getenv("CVR_PASSWORD")

        if not self.username or not self.password:
            raise ValueError(
                "CVR credentials not found. Set CVR_USERNAME and CVR_PASSWORD environment "
                "variables "
                "or provide them as constructor arguments."
            )

        # API endpoints
        self.base_url = "http://distribution.virk.dk"
        self.company_endpoint = f"{self.base_url}/cvr-permanent/virksomhed/_search"
        self.documents_endpoint = f"{self.base_url}/offentliggoerelser/_search"

        # Request configuration
        self.auth = HTTPBasicAuth(self.username, self.password)
        self.headers = {"Content-Type": "application/json"}
        self.session = requests.Session()
        self.session.auth = self.auth
        self.session.headers.update(self.headers)

        # Rate limiting
        self.last_request_time = 0
        self.min_request_interval = 0.1  # 100ms between requests

        # Initialize DAWA client for address geocoding
        self.enable_geocoding = enable_geocoding
        self.geocode_current_only = geocode_current_only
        self.dawa_client = DAWAAPIClient() if enable_geocoding else None

        self.log.info("CVR API client initialized")

    def _rate_limit(self):
        """Implement basic rate limiting between requests."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time

        if time_since_last < self.min_request_interval:
            sleep_time = self.min_request_interval - time_since_last
            time.sleep(sleep_time)

        self.last_request_time = time.time()

    @retry(
        retry=retry_if_exception_type(
            (requests.exceptions.RequestException, requests.exceptions.HTTPError)
        ),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
    )
    def _make_request(self, url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Make an authenticated request to the CVR API with retry logic.

        Args:
            url: API endpoint URL
            payload: Request payload

        Returns:
            API response as dictionary

        Raises:
            requests.exceptions.HTTPError: For HTTP errors
            requests.exceptions.RequestException: For other request errors
        """
        self._rate_limit()

        try:
            response = self.session.post(url, json=payload, timeout=30)
            response.raise_for_status()

            return response.json()

        except requests.exceptions.HTTPError:
            if response.status_code == 401:
                self.log.error("CVR API authentication failed. Check credentials.")
            elif response.status_code == 403:
                self.log.error("CVR API access forbidden. Check permissions.")
            else:
                self.log.error(f"CVR API HTTP error {response.status_code}: {response.text}")
            raise

        except requests.exceptions.RequestException as e:
            self.log.error(f"CVR API request error: {e}")
            raise

    def get_company_data(
        self, cvr_number: str, fetch_all_fields: bool = True, enrich_with_geometry: bool = True
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch comprehensive company data for a specific CVR number.

        Args:
            cvr_number: 8-digit CVR number
            fetch_all_fields: Whether to fetch all available fields or just basic ones
            enrich_with_geometry: Whether to enrich addresses with geometry via DAWA API

        Returns:
            Comprehensive company data dictionary or None if not found
        """
        if not self._validate_cvr_number(cvr_number):
            self.log.error(f"Invalid CVR number format: {cvr_number}")
            return None

        try:
            # Build query using the correct structure
            query = {"query": {"term": {"Vrvirksomhed.cvrNummer": cvr_number}}, "size": 1}

            # Add source filtering for performance if not fetching all fields
            if not fetch_all_fields:
                query["_source"] = [
                    "Vrvirksomhed.cvrNummer",
                    "Vrvirksomhed.navne",
                    "Vrvirksomhed.virksomhedsform",
                    "Vrvirksomhed.virksomhedsstatus",
                    "Vrvirksomhed.beliggenhedsadresse",
                    "Vrvirksomhed.hovedbranche",
                    "Vrvirksomhed.attributter",
                    "Vrvirksomhed.reklamebeskyttet",  # Include advertisement protection
                ]

            raw_data = self._make_request(self.company_endpoint, query)

            if not raw_data or "hits" not in raw_data or not raw_data["hits"]["hits"]:
                self.log.debug(f"No data found for CVR: {cvr_number}")
                return None

            # Parse comprehensive data
            parsed_data = self._parse_company_data(raw_data)

            if not parsed_data:
                self.log.error(f"Failed to parse company data for CVR: {cvr_number}")
                return None

            # Enrich with geometry if requested
            if enrich_with_geometry:
                parsed_data = self.enrich_company_with_geometry(parsed_data)

            return parsed_data

        except Exception as e:
            self.log.error(f"Error fetching company data for CVR {cvr_number}: {e}")
            return None

    def get_financial_documents(
        self, cvr_number: str, max_results: int = 10, xml_only: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Fetch financial documents and reports for a specific CVR number.

        Args:
            cvr_number: 8-digit CVR number
            max_results: Maximum number of documents to fetch
            xml_only: If True, only return XML documents (default: True)

        Returns:
            List of financial document data
        """


        # Validate CVR number format
        if not cvr_number or len(cvr_number) != 8 or not cvr_number.isdigit():
            self.log.warning(f"Invalid CVR number format: {cvr_number}")
            return []

        # Build query payload
        query_payload = {
            "size": max_results,
            "query": {"term": {"cvrNummer": cvr_number}},
            "sort": [{"offentliggoerelsesTidspunkt": {"order": "desc"}}],
        }

        try:
            data = self._make_request(self.documents_endpoint, query_payload)

            # Check if documents were found
            if data.get("hits", {}).get("total", 0) == 0:
                self.log.debug(f"No financial documents found for CVR: {cvr_number}")
                return []

            # Parse documents
            documents = []
            for hit in data.get("hits", {}).get("hits", []):
                doc_source = hit.get("_source", {})
                parsed_doc = self._parse_financial_document(doc_source, cvr_number, xml_only)
                if parsed_doc:
                    documents.append(parsed_doc)

            self.log.debug(f"Found {len(documents)} financial documents for CVR: {cvr_number}")
            return documents

        except Exception as e:
            self.log.error(f"Error fetching financial documents for CVR {cvr_number}: {e}")
            return []

    def _parse_company_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse raw company data from CVR API response with comprehensive field extraction.

        Args:
            raw_data: Raw response from CVR API

        Returns:
            Parsed and structured company data
        """
        if not raw_data or "hits" not in raw_data or not raw_data["hits"]["hits"]:
            return {}

        # Extract the nested Vrvirksomhed structure (universal for all CVR records)
        hit_source = raw_data["hits"]["hits"][0]["_source"]
        if "Vrvirksomhed" not in hit_source:
            self.log.error("No Vrvirksomhed structure found in response")
            return {}

        company = hit_source["Vrvirksomhed"]

        # Initialize comprehensive parsed data structure
        parsed_data = {
            "cvr_number": company.get("cvrNummer"),
            "company_name": company.get("navne", [{}])[0].get("navn")
            if company.get("navne")
            else None,
            "company_type": company.get("virksomhedsform", [{}])[0].get("virksomhedsformkode")
            if company.get("virksomhedsform")
            else None,
            "company_type_description": company.get("virksomhedsform", [{}])[0].get(
                "langBeskrivelse"
            )
            if company.get("virksomhedsform")
            else None,
            "status": company.get("virksomhedsstatus", [{}])[0].get("status")
            if company.get("virksomhedsstatus")
            else None,
            "founded_date": None,  # Will extract from livsforloeb
            "dissolution_date": None,  # Will extract from livsforloeb
            "last_updated": company.get("sidstOpdateret"),
            "advertisement_protection": company.get("reklamebeskyttet"),
            "data_source": "CVR Register",
            "fetch_timestamp": datetime.now().isoformat(),
        }

        # Extract all company names (current and historical)
        names = []
        for name_entry in company.get("navne", []):
            names.append(
                {
                    "name": name_entry.get("navn"),
                    "period_start": name_entry.get("periode", {}).get("gyldigFra"),
                    "period_end": name_entry.get("periode", {}).get("gyldigTil"),
                    "is_current": name_entry.get("periode", {}).get("gyldigTil") is None,
                }
            )
        parsed_data["all_names"] = names

        # Extract comprehensive address information
        addresses = []
        for address_entry in company.get("beliggenhedsadresse", []):
            address_parts = []

            # Build address string safely
            if address_entry.get("vejnavn"):
                address_parts.append(str(address_entry["vejnavn"]))
            if address_entry.get("husnummerFra"):
                address_parts.append(str(address_entry["husnummerFra"]))
            if address_entry.get("etage"):
                address_parts.append(f"{address_entry['etage']}.")
            if address_entry.get("sidedoer"):
                address_parts.append(address_entry["sidedoer"])

            addresses.append(
                {
                    "full_address": " ".join(address_parts) if address_parts else None,
                    "street_name": address_entry.get("vejnavn"),
                    "house_number": address_entry.get("husnummerFra"),
                    "floor": address_entry.get("etage"),
                    "door": address_entry.get("sidedoer"),
                    "postal_code": address_entry.get("postnummer"),
                    "city": address_entry.get("postdistrikt"),
                    "municipality_code": address_entry.get("kommune", {}).get("kommuneKode"),
                    "municipality_name": address_entry.get("kommune", {}).get("kommuneNavn"),
                    "country_code": address_entry.get("landekode"),
                    "adresse_id": address_entry.get("adresseId"),  # For DAWA geocoding
                    "period_start": address_entry.get("periode", {}).get("gyldigFra"),
                    "period_end": address_entry.get("periode", {}).get("gyldigTil"),
                    "is_current": address_entry.get("periode", {}).get("gyldigTil") is None,
                }
            )
        parsed_data["addresses"] = addresses

        # Extract contact information
        contact_info = {}
        for contact_entry in company.get("elektroniskPost", []):
            if contact_entry.get("periode", {}).get("gyldigTil") is None:  # Current email
                contact_info["email"] = contact_entry.get("kontaktoplysning")
                break

        for contact_entry in company.get("telefonNummer", []):
            if contact_entry.get("periode", {}).get("gyldigTil") is None:  # Current phone
                contact_info["phone"] = contact_entry.get("kontaktoplysning")
                break

        for contact_entry in company.get("telefaxNummer", []):
            if contact_entry.get("periode", {}).get("gyldigTil") is None:  # Current fax
                contact_info["fax"] = contact_entry.get("kontaktoplysning")
                break

        parsed_data["contact_info"] = contact_info

        # Extract comprehensive industry information (hovedbranche + bibranche1/2/3)
        industries = []

        # Main industry (hovedbranche)
        for industry_entry in company.get("hovedbranche", []):
            industries.append(
                {
                    "industry_code": industry_entry.get("branchekode"),
                    "industry_description": industry_entry.get("branchetekst"),
                    "period_start": industry_entry.get("periode", {}).get("gyldigFra"),
                    "period_end": industry_entry.get("periode", {}).get("gyldigTil"),
                    "is_current": industry_entry.get("periode", {}).get("gyldigTil") is None,
                    "is_main": True,
                }
            )

        # Secondary industries (bibranche1, bibranche2, bibranche3)
        for bibranche_key in ["bibranche1", "bibranche2", "bibranche3"]:
            for industry_entry in company.get(bibranche_key, []):
                industries.append(
                    {
                        "industry_code": industry_entry.get("branchekode"),
                        "industry_description": industry_entry.get("branchetekst"),
                        "period_start": industry_entry.get("periode", {}).get("gyldigFra"),
                        "period_end": industry_entry.get("periode", {}).get("gyldigTil"),
                        "is_current": industry_entry.get("periode", {}).get("gyldigTil") is None,
                        "is_main": False,
                        "bibranche_type": bibranche_key,
                    }
                )

        parsed_data["industries"] = industries

        # Extract company attributes (purpose, capital, etc.)
        attributes = {}
        for attr_entry in company.get("attributter", []):
            attr_type = attr_entry.get("type")
            if attr_type == "FORMAAL":
                attributes["company_purpose"] = attr_entry.get("vaerdier", [{}])[0].get("vaerdi")
            elif attr_type == "SELSKABSKAPITAL":
                cap_data = attr_entry.get("vaerdier", [{}])[0]
                attributes["share_capital"] = {
                    "amount": cap_data.get("vaerdi"),
                    "currency": cap_data.get("valuta", "DKK"),
                }
            elif attr_type == "ANTAL_AKTIER":
                attributes["number_of_shares"] = attr_entry.get("vaerdier", [{}])[0].get("vaerdi")
            elif attr_type == "AKTIEKAPITAL":
                cap_data = attr_entry.get("vaerdier", [{}])[0]
                attributes["share_capital_nominal"] = {
                    "amount": cap_data.get("vaerdi"),
                    "currency": cap_data.get("valuta", "DKK"),
                }
            elif attr_type == "REGNSKABSPERIODE":
                period_data = attr_entry.get("vaerdier", [{}])[0]
                attributes["accounting_period"] = {
                    "start_month": period_data.get("maaned"),
                    "start_day": period_data.get("dag"),
                }

        parsed_data["company_attributes"] = attributes

        # Extract comprehensive leadership information
        leadership = []
        for relation in company.get("deltagerRelation", []):
            if relation.get("periode", {}).get("gyldigTil") is None:  # Current relations only
                # Extract person information (with PII filtering)
                person_data = {}
                if "deltager" in relation:
                    deltager = relation["deltager"]
                    person_data = {
                        "person_type": deltager.get("enhedstype"),
                        "unit_number": deltager.get("enhedsNummer"),
                        "names": [],
                    }

                    # Extract all names
                    for name_entry in deltager.get("navne", []):
                        person_data["names"].append(
                            {
                                "name": name_entry.get("navn"),
                                "period_start": name_entry.get("periode", {}).get("gyldigFra"),
                                "period_end": name_entry.get("periode", {}).get("gyldigTil"),
                                "is_current": name_entry.get("periode", {}).get("gyldigTil")
                                is None,
                            }
                        )

                    # Extract addresses (with PII filtering)
                    person_addresses = []
                    for addr_entry in deltager.get("beliggenhedsadresse", []):
                        person_addresses.append(
                            {
                                "postal_code": addr_entry.get("postnummer"),
                                "city": addr_entry.get("postdistrikt"),
                                "municipality_code": addr_entry.get("kommune", {}).get(
                                    "kommuneKode"
                                ),
                                "municipality_name": addr_entry.get("kommune", {}).get(
                                    "kommuneNavn"
                                ),
                                "country_code": addr_entry.get("landekode"),
                                "period_start": addr_entry.get("periode", {}).get("gyldigFra"),
                                "period_end": addr_entry.get("periode", {}).get("gyldigTil"),
                                "is_current": addr_entry.get("periode", {}).get("gyldigTil")
                                is None,
                            }
                        )
                    person_data["addresses"] = person_addresses

                # Extract organization information
                organization_data = {}
                for org_entry in relation.get("organisationer", []):
                    if (
                        org_entry.get("periode", {}).get("gyldigTil") is None
                    ):  # Current organization
                        organization_data = {
                            "organization_name": org_entry.get("organisationsnavn"),
                            "member_data": org_entry.get("medlemsData", []),
                        }
                        break

                leadership.append(
                    {
                        "relation_type": relation.get("deltagerRelation"),
                        "person": person_data,
                        "organization": organization_data,
                        "period_start": relation.get("periode", {}).get("gyldigFra"),
                        "period_end": relation.get("periode", {}).get("gyldigTil"),
                        "is_current": relation.get("periode", {}).get("gyldigTil") is None,
                    }
                )

        # Apply PII filtering to leadership data
        filtered_leadership = filter_cvr_pii(leadership)
        parsed_data["leadership"] = filtered_leadership

        # Extract company status history
        status_history = []
        for status_entry in company.get("virksomhedsstatus", []):
            status_history.append(
                {
                    "status": status_entry.get("status"),
                    "period_start": status_entry.get("periode", {}).get("gyldigFra"),
                    "period_end": status_entry.get("periode", {}).get("gyldigTil"),
                    "is_current": status_entry.get("periode", {}).get("gyldigTil") is None,
                }
            )
        parsed_data["status_history"] = status_history

        # Extract business form history
        business_form_history = []
        for form_entry in company.get("virksomhedsform", []):
            business_form_history.append(
                {
                    "form_code": form_entry.get("virksomhedsformkode"),
                    "form_description": form_entry.get("langBeskrivelse"),
                    "short_description": form_entry.get("kortBeskrivelse"),
                    "period_start": form_entry.get("periode", {}).get("gyldigFra"),
                    "period_end": form_entry.get("periode", {}).get("gyldigTil"),
                    "is_current": form_entry.get("periode", {}).get("gyldigTil") is None,
                }
            )
        parsed_data["business_form_history"] = business_form_history

        # Extract subsidiary information (penheder)
        subsidiaries = []
        for subsidiary in company.get("penheder", []):
            subsidiaries.append(
                {
                    "p_number": subsidiary.get("pNummer"),
                    "name": subsidiary.get("navne", [{}])[0].get("navn")
                    if subsidiary.get("navne")
                    else None,
                    "main_industry": subsidiary.get("hovedbranche", [{}])[0].get("branchetekst")
                    if subsidiary.get("hovedbranche")
                    else None,
                    "status": subsidiary.get("virksomhedsstatus", [{}])[0].get("status")
                    if subsidiary.get("virksomhedsstatus")
                    else None,
                    "period_start": subsidiary.get("periode", {}).get("gyldigFra"),
                    "period_end": subsidiary.get("periode", {}).get("gyldigTil"),
                    "is_current": subsidiary.get("periode", {}).get("gyldigTil") is None,
                }
            )
        parsed_data["subsidiaries"] = subsidiaries

        # Extract comprehensive employment data
        employment_data = {
            "annual_employment": [],
            "quarterly_employment": [],
            "monthly_employment": [],
            "replacement_monthly_employment": []
        }
        
        # Annual employment (aarsbeskaeftigelse)
        for entry in company.get("aarsbeskaeftigelse", []):
            employment_data["annual_employment"].append({
                "year": entry.get("aar"),
                "full_time_equivalent": entry.get("antalAarsvaerk"),
                "total_employees": entry.get("antalAnsatte"),
                "employees_including_owners": entry.get("antalInklusivEjere"),
                "fte_interval_code": entry.get("intervalKodeAntalAarsvaerk"),
                "employees_interval_code": entry.get("intervalKodeAntalAnsatte"),
                "owners_interval_code": entry.get("intervalKodeAntalInklusivEjere"),
                "last_updated": entry.get("sidstOpdateret")
            })
        
        # Quarterly employment (kvartalsbeskaeftigelse)  
        for entry in company.get("kvartalsbeskaeftigelse", []):
            employment_data["quarterly_employment"].append({
                "year": entry.get("aar"),
                "quarter": entry.get("kvartal"),
                "full_time_equivalent": entry.get("antalAarsvaerk"),
                "total_employees": entry.get("antalAnsatte"),
                "fte_interval_code": entry.get("intervalKodeAntalAarsvaerk"),
                "employees_interval_code": entry.get("intervalKodeAntalAnsatte"),
                "last_updated": entry.get("sidstOpdateret")
            })
            
        # Monthly employment (maanedsbeskaeftigelse)
        for entry in company.get("maanedsbeskaeftigelse", []):
            employment_data["monthly_employment"].append({
                "year": entry.get("aar"),
                "month": entry.get("maaned"),
                "full_time_equivalent": entry.get("antalAarsvaerk"),
                "total_employees": entry.get("antalAnsatte"),
                "fte_interval_code": entry.get("intervalKodeAntalAarsvaerk"),
                "employees_interval_code": entry.get("intervalKodeAntalAnsatte"),
                "last_updated": entry.get("sidstOpdateret")
            })
            
        # Replacement monthly employment (erstMaanedsbeskaeftigelse)
        for entry in company.get("erstMaanedsbeskaeftigelse", []):
            employment_data["replacement_monthly_employment"].append({
                "year": entry.get("aar"),
                "month": entry.get("maaned"),
                "full_time_equivalent": entry.get("antalAarsvaerk"),
                "total_employees": entry.get("antalAnsatte"),
                "fte_interval_code": entry.get("intervalKodeAntalAarsvaerk"),
                "employees_interval_code": entry.get("intervalKodeAntalAnsatte"),
                "last_updated": entry.get("sidstOpdateret")
            })
        
        parsed_data["employment_data"] = employment_data

        # Extract lifecycle information (founding/dissolution dates)
        lifecycle_events = company.get("livsforloeb", [])
        if lifecycle_events:
            # Founded date is typically the first gyldigFra
            first_event = min(
                lifecycle_events, key=lambda x: x.get("periode", {}).get("gyldigFra", "9999-12-31")
            )
            parsed_data["founded_date"] = first_event.get("periode", {}).get("gyldigFra")

            # Dissolution date would be when gyldigTil is set
            dissolved_event = next(
                (event for event in lifecycle_events if event.get("periode", {}).get("gyldigTil")),
                None,
            )
            if dissolved_event:
                parsed_data["dissolution_date"] = dissolved_event.get("periode", {}).get(
                    "gyldigTil"
                )

        # Extract comprehensive metadata
        metadata = {
            "total_fields_in_response": len(str(company).split(",")),  # Rough field count
            "has_current_address": any(addr.get("is_current") for addr in addresses),
            "has_current_industry": any(ind.get("is_current") for ind in industries),
            "has_leadership_data": len(filtered_leadership) > 0,
            "has_subsidiaries": len(subsidiaries) > 0,
            "has_contact_info": bool(contact_info),
            "has_company_attributes": bool(attributes),
            "pii_filtering_applied": True,
            "total_leadership_relations": len(leadership),
            "filtered_leadership_relations": len(filtered_leadership),
            "vrvirksomhed_fields": list(company.keys())[:10],  # Sample of available fields
            "total_vrvirksomhed_fields": len(company.keys()),
        }
        parsed_data["metadata"] = metadata

        return parsed_data

    def enrich_company_with_geometry(self, company_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich company data with address geometry using DAWA API.
        
        Args:
            company_data: Parsed company data from CVR API
            
        Returns:
            Company data enriched with geometry information
        """
        if not self.enable_geocoding or not self.dawa_client:
            self.log.debug("Address geocoding disabled, skipping geometry enrichment")
            return company_data
        
        try:
            addresses = company_data.get("addresses", [])
            if not addresses:
                self.log.debug("No addresses found for geocoding")
                return company_data
            
            # Find current addresses with adresse_id
            geocodable_addresses = [
                addr for addr in addresses 
                if addr.get("is_current") and addr.get("adresse_id")
            ]
            
            if not geocodable_addresses:
                self.log.debug("No current addresses with adresse_id found for geocoding")
                return company_data
            
            # Geocode addresses
            enriched_addresses = []
            for address in addresses:
                enriched_address = address.copy()
                geocoded = None
                
                # Determine if we should geocode this address based on configuration
                should_geocode = not self.geocode_current_only or address.get("is_current")
                
                # Try DAWA geocoding first if address has adresse_id
                if should_geocode and address.get("adresse_id"):
                    geocoded = self.dawa_client.geocode_address_by_id(address["adresse_id"])
                    if geocoded:
                        self.log.debug(f"DAWA geocoded address: {address.get('full_address')}")
                
                # Fallback to Datavask API if DAWA failed and we have address text
                if not geocoded and should_geocode and address.get("full_address"):
                    geocoded = self.dawa_client.geocode_with_datavask(address["full_address"])
                    if geocoded:
                        self.log.debug(f"Datavask geocoded address: {address.get('full_address')}")
                
                # Add geometry data if geocoding succeeded
                if geocoded:
                    enriched_address.update({
                        "latitude": geocoded["latitude"],  # WGS84 latitude
                        "longitude": geocoded["longitude"],  # WGS84 longitude
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
                        "dawa_enriched": geocoded.get("dawa_enriched", True),
                        "datavask_enriched": geocoded.get("datavask_enriched", False),
                        "dawa_fetch_timestamp": geocoded.get("dawa_fetch_timestamp")
                    })
                    # Update BFE fields if available from Datavask
                    if geocoded.get("floor") is not None:
                        enriched_address["floor"] = geocoded["floor"]
                    if geocoded.get("door") is not None:
                        enriched_address["door"] = geocoded["door"]
                else:
                    enriched_address["dawa_enriched"] = False
                    enriched_address["datavask_enriched"] = False
                    self.log.warning(f"Failed to geocode address: {address.get('full_address')}")
                
                enriched_addresses.append(enriched_address)
            
            # Update company data with enriched addresses
            company_data = company_data.copy()
            company_data["addresses"] = enriched_addresses
            
            # Add primary address geometry to top level for easy access
            current_geocoded = [
                addr for addr in enriched_addresses 
                if addr.get("is_current") and addr.get("dawa_enriched")
            ]
            
            if current_geocoded:
                primary_address = current_geocoded[0]  # Use first current geocoded address
                company_data["primary_address_geometry"] = {
                    "latitude": primary_address.get("latitude"),  # WGS84 latitude
                    "longitude": primary_address.get("longitude"),  # WGS84 longitude
                    "coordinate_system": primary_address.get("coordinate_system", "WGS84"),
                    "srid": primary_address.get("srid", 4326),
                    "geometry_wkt": primary_address.get("geometry_wkt"),
                    "geometry_geojson": primary_address.get("geometry_geojson"),
                    "coordinate_quality": primary_address.get("coordinate_quality"),
                    "coordinate_source": primary_address.get("coordinate_source")
                }
            
            return company_data
            
        except Exception as e:
            self.log.error(f"Error enriching company data with geometry: {e}")
            return company_data

    def _parse_address(self, address_obj: Dict[str, Any]) -> Dict[str, Any]:
        """Parse address object from CVR data."""
        try:
            # Build formatted address string - convert all values to strings
            addr_parts = [
                str(address_obj.get("vejnavn", "")) if address_obj.get("vejnavn") else "",
                str(address_obj.get("husnummerFra", "")) if address_obj.get("husnummerFra") else "",
                str(address_obj.get("bogstavFra", "")) if address_obj.get("bogstavFra") else "",
                f"({address_obj.get('etage', '')})" if address_obj.get("etage") else "",
                f"({address_obj.get('sidedoer', '')})" if address_obj.get("sidedoer") else "",
                ",",
                str(address_obj.get("postnummer", "")) if address_obj.get("postnummer") else "",
                str(address_obj.get("postdistrikt", "")) if address_obj.get("postdistrikt") else "",
                f"({address_obj.get('bynavn', '')})" if address_obj.get("bynavn") else "",
                str(address_obj.get("landekode", "")) if address_obj.get("landekode") else "",
            ]

            # Filter out empty strings and join
            full_address = " ".join(part for part in addr_parts if part and part != "()").replace(
                " ,", ","
            )

            return {
                "street_name": address_obj.get("vejnavn", None),
                "house_number": address_obj.get("husnummerFra", None),
                "letter": address_obj.get("bogstavFra", None),
                "floor": address_obj.get("etage", None),
                "door": address_obj.get("sidedoer", None),
                "postal_code": address_obj.get("postnummer", None),
                "city": address_obj.get("postdistrikt", None),
                "district": address_obj.get("bynavn", None),
                "country_code": address_obj.get("landekode", None),
                "full_address": full_address,
            }

        except Exception as e:
            self.log.error(f"Error parsing address: {e}")
            return {"full_address": None, "error": str(e)}

    def _parse_financial_document(
        self, doc_source: Dict[str, Any], cvr_number: str, xml_only: bool = True
    ) -> Optional[Dict[str, Any]]:
        """
        Parse financial document data from CVR API response.

        Args:
            doc_source: Raw document data from API
            cvr_number: CVR number for logging

        Returns:
            Structured document data or None if parsing fails
        """
        try:
            document_data = {
                "cvr_number": cvr_number,
                "publication_type": doc_source.get("offentliggoerelsestype", None),
                "publication_time": doc_source.get("offentliggoerelsesTidspunkt", None),
                "case_number": doc_source.get("sagsNummer", None),
                "fetch_timestamp": datetime.now().isoformat(),
            }

            # Reporting period
            regnskab_info = doc_source.get("regnskab", {})
            periode_info = regnskab_info.get("regnskabsperiode", {})
            document_data["reporting_period"] = {
                "start_date": periode_info.get("startDato", None),
                "end_date": periode_info.get("slutDato", None),
            }

            # Documents within this publication - filter for XML if requested
            documents = []
            for doc in doc_source.get("dokumenter", []):
                mime_type = doc.get("dokumentMimeType", "")

                # Skip non-XML documents if xml_only is True
                if xml_only and mime_type != "application/xml":
                    continue

                documents.append(
                    {
                        "document_type": doc.get("dokumentType", None),
                        "mime_type": mime_type,
                        "document_url": doc.get("dokumentUrl", None),
                    }
                )

            document_data["documents"] = documents
            document_data["document_count"] = len(documents)

            return document_data

        except Exception as e:
            self.log.error(f"Error parsing financial document for CVR {cvr_number}: {e}")
            return None

    def fetch_multiple_companies(
        self, 
        cvr_numbers: List[str], 
        fetch_all_fields: bool = True, 
        enrich_with_geometry: bool = True
    ) -> Dict[str, Any]:
        """
        Fetch company data for multiple CVR numbers efficiently.

        Args:
            cvr_numbers: List of 8-digit CVR numbers
            fetch_all_fields: Whether to fetch all available fields or just basic ones
            enrich_with_geometry: Whether to enrich addresses with geometry via DAWA API

        Returns:
            Dictionary mapping CVR numbers to company data
        """
        self.log.info(
            f"Fetching data for {len(cvr_numbers)} companies "
            f"(geocoding: {'enabled' if enrich_with_geometry else 'disabled'})"
        )

        results = {}
        successful = 0
        failed = 0

        for cvr_number in tqdm(cvr_numbers, desc="Fetching company data", unit="company"):
            try:
                company_data = self.get_company_data(
                    cvr_number, fetch_all_fields, enrich_with_geometry
                )
                if company_data:
                    results[cvr_number] = company_data
                    successful += 1
                else:
                    failed += 1
                    self.log.debug(f"No data found for CVR: {cvr_number}")

            except Exception as e:
                failed += 1
                self.log.error(f"Error fetching CVR {cvr_number}: {e}")

        self.log.info(
            f"Batch fetch completed: {successful} successful, {failed} failed"
        )

        return {
            "results": results,
            "summary": {"total": len(cvr_numbers), "successful": successful, "failed": failed},
            "fetch_timestamp": datetime.now().isoformat(),
        }

    def download_financial_document(self, document_url: str) -> str:
        """
        Download financial document XML content from the provided URL.

        Args:
            document_url: URL to the financial document XML

        Returns:
            Raw XML content as string

        Raises:
            requests.exceptions.RequestException: For download errors
        """
        self.log.debug(f"Downloading financial document: {document_url}")

        try:
            self._rate_limit()

            response = self.session.get(document_url, timeout=30)
            response.raise_for_status()

            xml_content = response.text
            self.log.debug(f"Downloaded financial document: {len(xml_content)} characters")

            return xml_content

        except requests.exceptions.RequestException as e:
            self.log.error(f"Error downloading financial document {document_url}: {e}")
            raise

    def parse_financial_xml(self, xml_content: str) -> List[Dict[str, Any]]:
        """
        Parse financial XML document to extract financial values.

        Args:
            xml_content: Raw XML content string

        Returns:
            List of financial data items with element names and values
        """
        self.log.debug("Parsing financial XML document")

        try:
            root = ET.fromstring(xml_content)
            financial_data = []

            # Parse all elements with numeric values
            for elem in root.iter():
                if elem.text and elem.text.strip():
                    text = elem.text.strip()

                    # Try to parse as number (handle different formats)
                    try:
                        # Remove common formatting characters
                        clean_text = text.replace(",", "").replace(" ", "").replace("\n", "")

                        # Check if it's a valid number
                        if clean_text.replace("-", "").replace(".", "").isdigit():
                            value = float(clean_text)

                            # Only include significant amounts (> 1000) to filter noise
                            if abs(value) > 1000:
                                # Extract element name without namespace
                                tag_name = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
                                namespace = (
                                    elem.tag.split("}")[0].replace("{", "")
                                    if "}" in elem.tag
                                    else "default"
                                )

                                # Find parent context by searching the tree (ElementTree compatible)
                                parent_tag = ""
                                for parent in root.iter():
                                    if elem in parent:
                                        parent_tag = (
                                            parent.tag.split("}")[-1]
                                            if "}" in parent.tag
                                            else parent.tag
                                        )
                                        break

                                financial_data.append(
                                    {
                                        "element": tag_name,
                                        "value": value,
                                        "namespace": namespace,
                                        "parent": parent_tag,
                                        "raw_text": text,
                                    }
                                )

                    except (ValueError, TypeError):
                        # Not a valid number, skip
                        continue

            # Sort by absolute value (largest first)
            financial_data.sort(key=lambda x: abs(x["value"]), reverse=True)

            self.log.debug(f"Parsed {len(financial_data)} financial values from XML")
            return financial_data

        except ET.ParseError as e:
            self.log.error(f"Error parsing financial XML: {e}")
            return []
        except Exception as e:
            self.log.error(f"Unexpected error parsing financial XML: {e}")
            return []

    def get_enriched_company_data(
        self, cvr_number: str, include_financial_data: bool = True
    ) -> Optional[Dict[str, Any]]:
        """
        Get enriched company data including parsed financial information.

        Args:
            cvr_number: 8-digit CVR number
            include_financial_data: Whether to download and parse financial documents

        Returns:
            Enriched company data with financial information
        """


        # Get basic company data
        company_data = self.get_company_data(cvr_number)
        if not company_data:
            return None

        # Add financial data if requested
        if include_financial_data:
            try:
                financial_docs = self.get_financial_documents(cvr_number)

                if financial_docs:
                    enriched_financial_docs = []

                    for doc in financial_docs[:3]:  # Process up to 3 most recent documents
                        enriched_doc = doc.copy()

                        # Download and parse financial XML if available
                        if "documents" in doc and doc["documents"]:
                            xml_doc = doc["documents"][0]
                            if "document_url" in xml_doc:
                                try:
                                    xml_content = self.download_financial_document(
                                        xml_doc["document_url"]
                                    )
                                    financial_values = self.parse_financial_xml(xml_content)

                                    enriched_doc["financial_data"] = financial_values
                                    enriched_doc["financial_summary"] = {
                                        "total_values": len(financial_values),
                                        "largest_value": financial_values[0]["value"]
                                        if financial_values
                                        else 0,
                                        "parsed_successfully": True,
                                    }

                                except Exception as e:
                                    self.log.warning(
                                        f"Failed to parse financial document for CVR "
                                        f"{cvr_number}: {e}"
                                    )
                                    enriched_doc["financial_summary"] = {
                                        "parsed_successfully": False,
                                        "error": str(e),
                                    }

                        enriched_financial_docs.append(enriched_doc)

                    company_data["financial_documents"] = enriched_financial_docs

            except Exception as e:
                self.log.warning(f"Failed to fetch financial data for CVR {cvr_number}: {e}")

        return company_data

    def _validate_cvr_number(self, cvr_number: str) -> bool:
        """
        Validate CVR number format.

        Args:
            cvr_number: CVR number to validate

        Returns:
            True if valid, False otherwise
        """
        if not cvr_number:
            return False

        # CVR numbers should be 8 digits, not starting with 0
        if len(cvr_number) != 8 or not cvr_number.isdigit():
            return False

        # Should not start with 0
        if cvr_number.startswith("0"):
            return False

        return True
