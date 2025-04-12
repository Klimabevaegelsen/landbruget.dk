"""Module for loading CHR Besætning data (Herds) - Bronze Layer."""

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
from zeep.exceptions import Fault

# Import the exporter function
from .export import save_raw_data

# Set up logging
logger = logging.getLogger(__name__)

# --- Constants ---

# API Endpoints (WSDL URLs)
ENDPOINTS = {
    'besaetning': 'https://ws.fvst.dk/service/CHR_besaetningWS?wsdl'
}

# Default Client ID for SOAP requests
DEFAULT_CLIENT_ID = 'LandbrugsData'

# --- Credential Handling ---

def get_fvm_credentials() -> Tuple[str, str]:
    """Get FVM username and password from environment variables."""
    load_dotenv()  # Load environment variables from .env file
    
    # Try DATAFORDELER credentials first, fall back to FVM if not found
    username = os.getenv('DATAFORDELER_USERNAME') or os.getenv('FVM_USERNAME')
    password = os.getenv('DATAFORDELER_PASSWORD') or os.getenv('FVM_PASSWORD')
    
    if not username or not password:
        raise ValueError("DATAFORDELER_USERNAME/PASSWORD or FVM_USERNAME/PASSWORD must be set in environment variables")
    
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

def _create_base_request(username: str, session_id: str = '1', track_id: str = 'load_besaetning') -> Dict[str, str]:
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

def fetch_raw_soap_response(client: Client, operation_name: str, request_structure: Dict) -> Optional[Any]:
    """Fetch raw response from a SOAP endpoint using Zeep."""
    try:
        operation = getattr(client.service, operation_name)
        response = operation(request_structure)
        logger.debug(f"Fetched data from {operation_name}")
        return response
    except AttributeError:
        logger.error(f"Operation '{operation_name}' not found")
    except Exception as e:
        logger.error(f"Error in {operation_name}: {e}")
    return None

# --- Besætning Loading Functions ---

def load_herd_list(
    besaetning_client: Client,
    username: str,
    species_code: int,
    usage_code: int,
    start_herd_number: Optional[int] = None,
) -> Tuple[List[int], bool, Optional[int]]:
    """Load list of herds for a given species and usage type."""
    try:
        logger.debug(f"Fetching herds for species {species_code}, usage {usage_code}")
        
        request_structure = {
            'GLRCHRWSInfoInbound': {
                'BrugerID': username,
                'KlientID': DEFAULT_CLIENT_ID,
                'TransaktionsID': str(uuid.uuid4())
            },
            'Request': {
                'DyreArtKode': species_code,
                'BrugsArtKode': usage_code,
                'StartBesaetningsNummer': start_herd_number
            }
        }

        response = fetch_raw_soap_response(besaetning_client, 'listBesaetninger', request_structure)
        
        if not response:
            return [], False, None

        # Save raw response
        save_raw_data(
            raw_response=response,
            data_type='besaetning_list',
            identifier=f"{species_code}_{usage_code}"
        )

        # Parse response
        herd_list = []
        has_more = False
        last_herd_in_batch = None

        if hasattr(response, 'Response'):
            response_data = response.Response
            if isinstance(response_data, list):
                for item in response_data:
                    herd_number = getattr(item, 'BesaetningsNummer', None)
                    if herd_number is not None:
                        herd_list.append(int(herd_number))
                        last_herd_in_batch = int(herd_number)
            else:
                herd_number = getattr(response_data, 'BesaetningsNummer', None)
                if herd_number is not None:
                    herd_list.append(int(herd_number))
                    last_herd_in_batch = int(herd_number)

            has_more = bool(getattr(response_data, 'HarFlere', False))

        logger.debug(f"Found {len(herd_list)} herds for species {species_code}")
        return herd_list, has_more, last_herd_in_batch

    except Exception as e:
        logger.error(f"Error fetching herd list: {e}")
        return [], False, None

def load_herd_details(client: Client, username: str, herd_number: int, species_code: int) -> Optional[Any]:
    """Load detailed information for a specific herd."""
    try:
        logger.debug(f"Fetching details for herd {herd_number}")
        
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

        response = fetch_raw_soap_response(client, 'hentStamoplysninger', request_structure)
        
        if response:
            save_raw_data(
                raw_response=response,
                data_type='besaetning_details',
                identifier=f"{herd_number}_{species_code}"
            )
            
        return response

    except Exception as e:
        logger.error(f"Error fetching herd details for {herd_number}: {e}")
        return None

# --- Test Execution ---
if __name__ == '__main__':
    # # Temporary absolute import for direct script execution - REMOVED
    # from backend.chr_pipeline.bronze.export import save_raw_data
    # import json # Ensure json is imported for the test block - REMOVED

    logger.info("--- Starting Besætning Load Test --- ")

    # Parameters for testing hentStamoplysninger
    TEST_SPECIES_CODE = 15 # Example: Pigs
    TEST_HERD_NUMBER = 5392 # Replace with a real, stable herd number known to exist for the species/usage

    try:
        username, password = get_fvm_credentials()
        besaetning_client = create_soap_client(ENDPOINTS['besaetning'], username, password)

        # --- Test hentStamoplysninger ONLY --- 
        logger.info(f"\n--- Testing load_herd_details ONLY (Herd: {TEST_HERD_NUMBER}, Species: {TEST_SPECIES_CODE}) ---")
        herd_details_raw = load_herd_details(besaetning_client, username, TEST_HERD_NUMBER, TEST_SPECIES_CODE)
        
        if herd_details_raw:
            logger.info(f"Successfully called load_herd_details for herd {TEST_HERD_NUMBER}. Raw data saved via export module.")
            # Serialize and print part of the response for inspection
            try:
                serialized = serialize_object(herd_details_raw, dict)
                logger.info(f"Response Snippet: {json.dumps(serialized, indent=2)[:1000]}...")
                # Attempt to extract CHR num using the logic from main.py for verification
                chr_number = None
                response_body = serialized.get('Response', None)
                if response_body and isinstance(response_body, list) and len(response_body) > 0:
                     besaetning_info = response_body[0].get('Besaetning', None)
                     if besaetning_info:
                         ejendom_list = besaetning_info.get('Ejendom', [])
                         if ejendom_list and isinstance(ejendom_list, list) and len(ejendom_list) > 0:
                             chr_number_str = ejendom_list[0].get('CHRNummer')
                             if chr_number_str:
                                  try: chr_number = int(chr_number_str)
                                  except: pass
                logger.info(f"Attempted CHR extraction from test response: {chr_number}")

            except Exception as ser_err:
                logger.error(f"Could not serialize/inspect test response: {ser_err}")
        else:
            logger.warning(f"load_herd_details returned None or empty for test herd {TEST_HERD_NUMBER}.")
        # --- End hentStamoplysninger ONLY test ---

        # --- Commented out listBesaetningerMedBrugsart test --- 
        # TEST_USAGE_CODE = 11  # Example: Kød, generelt
        # logger.info(f"\n--- Testing load_herd_list (Species: {TEST_SPECIES_CODE}, Usage: {TEST_USAGE_CODE}) ---")
        # herd_list, last_herd_in_batch, has_more = load_herd_list(besaetning_client, username, TEST_SPECIES_CODE, TEST_USAGE_CODE)
        # if herd_list:
        #     logger.info(f"Successfully called load_herd_list. Raw data saved via export module.")
        # else:
        #     logger.warning("load_herd_list returned None or empty.")
        # --- End Commented out listBesaetningerMedBrugsart test --- 

        logger.info("\n--- Besætning Load Test Complete --- ")

    except Exception as e:
        logger.error(f"Error during Besætning load test: {e}", exc_info=True)