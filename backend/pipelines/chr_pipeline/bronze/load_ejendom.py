"""Module for loading CHR Ejendom data (Properties) - Bronze Layer."""

import logging
import os
import uuid
from typing import Any, Dict, Optional, Tuple

import certifi
from dotenv import load_dotenv
from requests import Session
from zeep import Client
from zeep.transports import Transport
from zeep.wsse.username import UsernameToken

# Import the exporter function
from .export import save_raw_data

# Set up logging
logger = logging.getLogger("backend.pipelines.chr_pipeline.bronze.load_ejendom")

# --- Constants ---

# API Endpoints (WSDL URLs)
ENDPOINTS = {"ejendom": "https://ws.fvst.dk/service/CHR_ejendomWS?wsdl"}

# Default Client ID for SOAP requests
DEFAULT_CLIENT_ID = "LandbrugsData"  # TODO: Confirm if this needs changing

# --- Credential Handling ---


def get_fvm_credentials() -> Tuple[str, str]:
    """Get FVM username and password from environment variables."""
    load_dotenv()  # Load environment variables from .env file

    username = os.getenv("FVM_USERNAME")
    password = os.getenv("FVM_PASSWORD")

    if not username or not password:
        raise ValueError("FVM_USERNAME and FVM_PASSWORD must be set in environment variables")

    return username, password


# --- SOAP Client Creation ---


def create_soap_client(wsdl_url: str, username: str, password: str) -> Client:
    """Create a Zeep SOAP client with WSSE authentication."""
    # Note: Consider moving this to a shared utility module later
    session = Session()
    session.verify = certifi.where()  # Ensure CA certificates are used
    transport = Transport(session=session)
    try:
        client = Client(wsdl_url, transport=transport, wsse=UsernameToken(username, password))
        logger.info(f"Successfully created SOAP client for {wsdl_url}")
        return client
    except Exception as e:
        logger.error(f"Failed to create SOAP client for {wsdl_url}: {e}")
        raise


# --- Base Request Structure ---


def _create_base_request(username: str, session_id: str = "1", track_id: str = "load_ejendom") -> Dict[str, str]:
    """Create the common GLRCHRWSInfoInbound structure."""
    # Note: Consider moving this to a shared utility module later
    return {
        "BrugerNavn": username,
        "KlientId": DEFAULT_CLIENT_ID,
        "SessionId": session_id,
        "IPAdresse": "",  # Typically left blank
        "TrackID": f"{track_id}-{uuid.uuid4()}",  # Add UUID for uniqueness
    }


# --- Generic SOAP Fetcher ---


def fetch_raw_soap_response(client: Client, operation_name: str, request_data: Dict) -> Optional[Any]:
    """Fetch raw response from a SOAP endpoint using Zeep."""
    # Note: Consider moving this to a shared utility module later
    try:
        operation = getattr(client.service, operation_name)
        # Pass request_data as a single positional argument (arg0) based on previous findings
        response = operation(request_data)
        logger.info(f"Successfully fetched raw data from {client.wsdl.location} - {operation_name}")
        # Return the raw Zeep object, serialization happens in export/transform
        return response
    except AttributeError:
        logger.error(f"Operation '{operation_name}' not found on client for {client.wsdl.location}")
    except Exception as e:
        logger.error(f"Error calling {operation_name} on {client.wsdl.location}: {e}")
    return None


# --- Ejendom Loading Functions ---


def load_ejendom_oplysninger(client: Client, username: str, chr_number: int) -> Optional[Any]:
    """Load property details (EjendomsOplysninger) using the 'hentOplysninger' operation."""
    logger.info(f"Fetching property details for CHR: {chr_number}...")

    request_structure = {
        "GLRCHRWSInfoInbound": _create_base_request(username, track_id="load_ejendom_oplysninger"),
        "Request": {
            "ChrNummer": str(chr_number),
            "vis": None,  # Optional: Keep as None unless specific view is needed
        },
    }

    operation_name = "hentOplysninger"

    response = fetch_raw_soap_response(client, operation_name, request_structure)
    if not response:
        logger.warning(f"No response received for {operation_name} (CHR: {chr_number})")
    else:
        # Save the raw response
        save_raw_data(raw_response=response, data_type="ejendom_oplysninger", identifier=f"{chr_number}")
    return response


def load_ejendom_vet_events(client: Client, username: str, chr_number: int) -> Optional[Any]:
    """Load veterinary events (VeterinaereHaendelser) using the 'hentVeterinaereHaendelser' operation."""
    logger.info(f"Fetching veterinary events for CHR: {chr_number}...")

    request_structure = {
        "GLRCHRWSInfoInbound": _create_base_request(username, track_id="load_ejendom_vet_events"),
        "Request": {
            "ChrNummer": str(chr_number)
            # DyreArtKode and DyreArtTekst are also possible here according to WSDL,
            # but likely not needed for a general event list for the property.
            # Add if specific filtering by species is required later.
        },
    }

    operation_name = "hentVeterinaereHaendelser"

    response = fetch_raw_soap_response(client, operation_name, request_structure)
    if not response:
        logger.warning(f"No response received for {operation_name} (CHR: {chr_number})")
    else:
        # Save the raw response
        save_raw_data(raw_response=response, data_type="ejendom_vet_events", identifier=f"{chr_number}")
    return response


# --- Test Execution ---
if __name__ == "__main__":
    logger.info("--- Starting Ejendom Load Test --- ")

    # Replace with a real CHR number for testing
    TEST_CHR_NUMBER = 28400

    try:
        username, password = get_fvm_credentials()
        ejendom_client = create_soap_client(ENDPOINTS["ejendom"], username, password)

        # Test load_ejendom_oplysninger
        logger.info(f"\n--- Testing load_ejendom_oplysninger (CHR: {TEST_CHR_NUMBER}) ---")
        oplysninger_raw = load_ejendom_oplysninger(ejendom_client, username, TEST_CHR_NUMBER)
        if oplysninger_raw:
            logger.info(
                f"Successfully called load_ejendom_oplysninger for CHR {TEST_CHR_NUMBER}. Raw data saved via export module."
            )
        else:
            logger.warning("load_ejendom_oplysninger returned None or empty.")

        # Test load_ejendom_vet_events
        logger.info(f"\n--- Testing load_ejendom_vet_events (CHR: {TEST_CHR_NUMBER}) ---")
        vet_events_raw = load_ejendom_vet_events(ejendom_client, username, TEST_CHR_NUMBER)
        if vet_events_raw:
            logger.info(
                f"Successfully called load_ejendom_vet_events for CHR {TEST_CHR_NUMBER}. Raw data saved via export module."
            )
        else:
            logger.warning("load_ejendom_vet_events returned None or empty.")

        logger.info("\n--- Ejendom Load Test Complete --- ")

    except Exception as e:
        logger.error(f"Error during Ejendom load test: {e}", exc_info=True)
