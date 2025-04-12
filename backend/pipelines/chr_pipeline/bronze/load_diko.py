"""Module for loading DIKO (animal movements) data - Bronze Layer."""

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
    'diko': 'https://ws.fvst.dk/service/DIKOWS?wsdl'
}

# Default Client ID for SOAP requests
DEFAULT_CLIENT_ID = 'LandbrugsData'

# Valid species codes for DIKO
VALID_DIKO_SPECIES = {
    12: 'Cattle',
    13: 'Sheep',
    14: 'Goats',
    15: 'Pigs'
}

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
    session.verify = certifi.where()
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
        logger.error(f"Failed to create SOAP client for {wsdl_url}: {e}")
        raise

# --- Base Request Structure ---

def _create_base_request(username: str, session_id: str = '1', track_id: str = 'load_diko') -> Dict[str, str]:
    """Create the common GLRCHRWSInfoInbound structure."""
    # Note: Consider moving this to a shared utility module later
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
        response = operation(request_data)
        logger.debug(f"Fetched data from {operation_name}")
        return response
    except AttributeError:
        logger.error(f"Operation '{operation_name}' not found")
    except Exception as e:
        logger.error(f"Error in {operation_name}: {e}")
    return None

# --- DIKO Loading Functions ---

def load_diko_flytninger(client: Client, username: str, herd_number: int, species_code: int) -> Optional[Any]:
    """Load DIKO movement data for a specific herd."""
    try:
        if species_code not in VALID_DIKO_SPECIES:
            logger.warning(f"Invalid species code {species_code} for DIKO")
            return None
            
        logger.debug(f"Fetching movements for herd {herd_number}")
        
        request_structure = {
            'GLRCHRWSInfoInbound': {
                'BrugerID': username,
                'KlientID': DEFAULT_CLIENT_ID,
                'TransaktionsID': str(uuid.uuid4())
            },
            'Request': {
                'BesaetningsNummer': herd_number,
                'DyreArtKode': species_code
            }
        }

        response = fetch_raw_soap_response(client, 'besaetningListFlytninger', request_structure)
        
        if response:
            save_raw_data(
                raw_response=response,
                data_type='diko_flytninger',
                identifier=f"{herd_number}_{species_code}"
            )
            
        return response

    except Exception as e:
        logger.error(f"Error fetching movements for herd {herd_number}: {e}")
        return None

# --- Test Execution ---
if __name__ == '__main__':
    logger.info("--- Starting DIKO Load Test --- ")

    # Use parameters identified from previous tests
    TEST_HERD_NUMBER = 5392
    TEST_SPECIES_CODE = 15 # Pigs - Note: DIKO might error for pigs, expecting SvineflytningWS call instead
                           # We will test with this first to see the DIKO response/error.
                           # Consider adding a test with a different species (e.g., cattle=12) if available.

    try:
        username, password = get_fvm_credentials()
        diko_client = create_soap_client(ENDPOINTS['diko'], username, password)

        # Test load_diko_flytninger
        logger.info(f"\n--- Testing load_diko_flytninger (Herd: {TEST_HERD_NUMBER}, Species: {TEST_SPECIES_CODE}) ---")
        flytninger_raw = load_diko_flytninger(diko_client, username, TEST_HERD_NUMBER, TEST_SPECIES_CODE)

        if flytninger_raw:
            logger.info(f"Raw DIKO Flytninger Response (Top Level):")
            try:
                logger.info(json.dumps(serialize_object(flytninger_raw), indent=2, default=str))
            except Exception as e:
                logger.error(f"Error serializing or logging DIKO response: {e}")
        else:
            logger.warning("load_diko_flytninger returned None or empty.")

        logger.info("\n--- DIKO Load Test Complete --- ")

    except Exception as e:
        logger.error(f"Error during DIKO load test: {e}", exc_info=True)