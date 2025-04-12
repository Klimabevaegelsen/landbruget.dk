"""Module for loading CHR Ejendom data (Properties) - Bronze Layer."""

import logging
import json
import certifi
import uuid
import os
from typing import Dict, Any, List, Tuple, Optional
from dotenv import load_dotenv

from zeep import Client
from zeep.transports import Transport
from requests import Session
from zeep.wsse.username import UsernameToken
from zeep.helpers import serialize_object

# Import the exporter function
from .export import save_raw_data

# Set up logging
logger = logging.getLogger(__name__)

# --- Constants ---

# API Endpoints (WSDL URLs)
ENDPOINTS = {
    'ejendom': 'https://ws.fvst.dk/service/CHR_ejendomWS?wsdl'
}

# Default Client ID for SOAP requests
DEFAULT_CLIENT_ID = 'LandbrugsData' # TODO: Confirm if this needs changing

# --- Credential Handling ---

def get_fvm_credentials() -> Tuple[str, str]:
    """Get FVM username and password from environment variables."""
    load_dotenv()  # Load environment variables from .env file
    
    username = os.getenv('FVM_USERNAME')
    password = os.getenv('FVM_PASSWORD')
    
    if not username or not password:
        raise ValueError("FVM_USERNAME and FVM_PASSWORD must be set in environment variables")
    
    return username, password

# --- SOAP Client Creation ---

def create_soap_client(wsdl_url: str, username: str, password: str) -> Client:
    """Create a Zeep SOAP client with WSSE authentication."""
    session = Session()
    session.verify = certifi.where() # Ensure CA certificates are used
    transport = Transport(session=session)
    try:
        client = Client(
            wsdl_url,
            transport=transport,
            wsse=UsernameToken(username, password)
        )
        logger.debug(f"Created SOAP client for {wsdl_url}")
        return client
    except Exception as e:
        logger.error(f"Failed to create SOAP client for {wsdl_url}. Error: {str(e)}", exc_info=True)
        raise

# --- Base Request Structure ---

def _create_base_request(username: str, session_id: str = '1', track_id: str = 'load_ejendom') -> Dict[str, str]:
    """Create the common GLRCHRWSInfoInbound structure."""
    return {
        'BrugerNavn': username,
        'KlientId': DEFAULT_CLIENT_ID,
        'SessionId': session_id,
        'IPAdresse': '', # Typically left blank
        'TrackID': f"{track_id}-{uuid.uuid4()}" # Add UUID for uniqueness
    }

# --- Generic SOAP Fetcher ---

def fetch_raw_soap_response(client: Client, operation_name: str, request_data: Dict) -> Optional[Any]:
    """Fetch raw response from a SOAP endpoint using Zeep."""
    try:
        operation = getattr(client.service, operation_name)
        logger.debug(f"Calling {operation_name} on {client.wsdl.location}")
        response = operation(request_data)
        logger.debug(f"Successfully fetched data from {operation_name}")
        return response
    except AttributeError as e:
        logger.error(f"Operation '{operation_name}' not found on client for {client.wsdl.location}. Error: {str(e)}")
    except Exception as e:
        logger.error(f"Error calling {operation_name} on {client.wsdl.location}. Error: {str(e)}", exc_info=True)
    return None

# --- Ejendom Loading Functions ---

def load_ejendom_oplysninger(client: Client, username: str, chr_number: int) -> Optional[Any]:
    """Load property details (EjendomsOplysninger) using the 'hentOplysninger' operation."""
    logger.debug(f"Preparing to fetch property details for CHR: {chr_number}")

    request_structure = {
         'GLRCHRWSInfoInbound': _create_base_request(username, track_id='load_ejendom_oplysninger'),
         'Request': {
             'ChrNummer': str(chr_number),
             'vis': None # Optional: Keep as None unless specific view is needed
         }
    }

    operation_name = 'hentOplysninger'

    response = fetch_raw_soap_response(client, operation_name, request_structure)
    if not response:
        logger.warning(f"No response received for {operation_name} (CHR: {chr_number})")
    else:
        logger.info(f"Successfully fetched property details for CHR: {chr_number}")
        # Save the raw response
        save_raw_data(
            raw_response=response,
            data_type='ejendom_oplysninger',
            identifier=f"{chr_number}"
        )
    return response

def load_ejendom_vet_events(client: Client, username: str, chr_number: int) -> Optional[Any]:
    """Load veterinary events (VeterinaereHaendelser) using the 'hentVeterinaereHaendelser' operation."""
    logger.debug(f"Preparing to fetch veterinary events for CHR: {chr_number}")

    request_structure = {
         'GLRCHRWSInfoInbound': _create_base_request(username, track_id='load_ejendom_vet_events'),
         'Request': {
             'ChrNummer': str(chr_number)
             # DyreArtKode and DyreArtTekst are also possible here according to WSDL,
             # but likely not needed for a general event list for the property.
             # Add if specific filtering by species is required later.
         }
    }

    operation_name = 'hentVeterinaereHaendelser'

    response = fetch_raw_soap_response(client, operation_name, request_structure)
    if not response:
        logger.warning(f"No response received for {operation_name} (CHR: {chr_number})")
    else:
        logger.info(f"Successfully fetched veterinary events for CHR: {chr_number}")
        # Save the raw response
        save_raw_data(
            raw_response=response,
            data_type='ejendom_vet_events',
            identifier=f"{chr_number}"
        )
    return response

# --- Test Execution ---
if __name__ == '__main__':
    # Set up console logging for testing
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s | %(levelname)-7s | %(name)s | %(message)s'
    )
    
    logger.info("--- Starting Ejendom Load Test --- ")

    # Replace with a real CHR number for testing
    TEST_CHR_NUMBER = 28400

    try:
        username, password = get_fvm_credentials()
        ejendom_client = create_soap_client(ENDPOINTS['ejendom'], username, password)

        # Test load_ejendom_oplysninger
        logger.info(f"Testing load_ejendom_oplysninger (CHR: {TEST_CHR_NUMBER})")
        oplysninger_raw = load_ejendom_oplysninger(ejendom_client, username, TEST_CHR_NUMBER)
        if oplysninger_raw:
            logger.info(f"Successfully loaded property details for CHR {TEST_CHR_NUMBER}")
        else:
            logger.warning(f"Failed to load property details for CHR {TEST_CHR_NUMBER}")

        # Test load_ejendom_vet_events
        logger.info(f"Testing load_ejendom_vet_events (CHR: {TEST_CHR_NUMBER})")
        vet_events_raw = load_ejendom_vet_events(ejendom_client, username, TEST_CHR_NUMBER)
        if vet_events_raw:
            logger.info(f"Successfully loaded veterinary events for CHR {TEST_CHR_NUMBER}")
        else:
            logger.warning(f"Failed to load veterinary events for CHR {TEST_CHR_NUMBER}")

        logger.info("--- Ejendom Load Test Complete --- ")

    except Exception as e:
        logger.error(f"Error during Ejendom load test: {str(e)}", exc_info=True)