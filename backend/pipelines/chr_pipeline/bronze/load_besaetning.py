"""Module for loading CHR Besætning data (Herds) - Bronze Layer."""

import json
import logging
import os
import uuid
from typing import Any, Dict, List, Optional, Tuple

import certifi
from requests import Session
from zeep import Client
from zeep.exceptions import Fault
from zeep.helpers import serialize_object
from zeep.transports import Transport
from zeep.wsse.username import UsernameToken

# Import the exporter function
from .export import save_raw_data

# Set up logging
logger = logging.getLogger("backend.pipelines.chr_pipeline.bronze.load_besaetning")

# --- Constants ---

# API Endpoints (WSDL URLs)
ENDPOINTS = {"besaetning": "https://ws.fvst.dk/service/CHR_besaetningWS?wsdl"}

# Default Client ID for SOAP requests
DEFAULT_CLIENT_ID = "LandbrugsData"

# --- Credential Handling ---


def get_fvm_credentials() -> tuple[str, str]:
    """Get FVM credentials from environment variables."""
    username = os.getenv("FVM_USERNAME")
    password = os.getenv("FVM_PASSWORD")

    if not username or not password:
        raise ValueError("FVM_USERNAME/PASSWORD must be set in environment variables")

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


def _create_base_request(username: str, session_id: str = "1", track_id: str = "load_besaetning") -> Dict[str, str]:
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


def fetch_raw_soap_response(client: Client, operation_name: str, request_structure: Dict) -> Optional[Any]:
    """Fetch raw response from a SOAP endpoint using Zeep."""
    # Note: Consider moving this to a shared utility module later
    try:
        operation = getattr(client.service, operation_name)
        # Pass the dictionary directly, not unpacked with **
        # This assumes the operation expects a single complex type argument matching the dict structure
        response = operation(request_structure)
        logger.info(f"Successfully fetched raw data from {client.wsdl.location} - {operation_name}")
        return response
    except AttributeError:
        logger.error(f"Operation '{operation_name}' not found on client for {client.wsdl.location}")
    except Exception as e:
        logger.error(f"Error calling {operation_name} on {client.wsdl.location}: {e}")
    return None


# --- Besætning Loading Functions ---


def load_herd_list(
    besaetning_client: Client,
    username: str,
    species_code: int,
    usage_code: int,
    start_herd_number: Optional[int] = None,
) -> Tuple[List[int], bool, Optional[int]]:
    """
    Fetches a list of herd numbers for a given species and usage code,
    handling pagination.

    Returns:
        A tuple containing:
        - list: A list of herd numbers found in this batch.
        - bool: True if there are more herds to fetch, False otherwise.
        - Optional[int]: The last herd number received in this batch (TilBesNr),
                         or None if not available or no herds found.
    """
    operation_name = "listBesaetningerMedBrugsart"
    base_request = _create_base_request(username)
    request_data = {
        "DyreArtKode": str(species_code),
        "BrugsArtKode": str(usage_code),
    }
    if start_herd_number and start_herd_number > 0:
        request_data["BesNrFra"] = str(start_herd_number)

    # Create the payload dictionary expected by the operation elements
    payload = {
        "GLRCHRWSInfoInbound": base_request,
        "Request": request_data,
    }

    logger.info(
        f"Fetching herd list for species {species_code}, usage {usage_code}, starting from herd {start_herd_number or 'beginning'}..."
    )

    # --- Construct the request structure precisely according to WSDL/XSD ---
    try:
        # 1. Get the factory for the innermost request parameters type
        RequestParamsFactory = besaetning_client.get_type("ns0:CHR_besaetningListBesaetningerMedBrugsartRequestType")
        request_params = RequestParamsFactory(
            DyreArtKode=species_code, BrugsArtKode=usage_code, FraBesNr=start_herd_number
        )

        # 2. Get the factory for the common inbound header type (Corrected type name)
        GLRCHRWSInfoInboundFactory = besaetning_client.get_type("ns0:GLRCHRWSInfoInboundType")
        common_header = GLRCHRWSInfoInboundFactory(**_create_base_request(username))

        # 3. Combine the header and request parameters into the structure expected by the operation argument
        #    We don't need a factory for the wrapping element itself.
        payload_content = {"GLRCHRWSInfoInbound": common_header, "Request": request_params}

        # 4. Call the operation, passing the constructed payload structure as the value
        #    for the argument named after the element reference in the WSDL message part.
        response = besaetning_client.service.listBesaetningerMedBrugsart(
            CHR_besaetningListBesaetningerMedBrugsartRequest=payload_content
        )

        # --- End of new request structure ---

        if response is None:
            logger.warning(
                f"No response received for species {species_code}, usage {usage_code}, start {start_herd_number}."
            )
            return [], False, None  # Indicate potential error/end

        # Process the response (assuming the structure is now correct)
        serialized_response = serialize_object(response, dict)

        # Save raw data (using the structured response directly)
        save_raw_data(
            data_type="besaetning_list",
            identifier=f"{species_code}_{usage_code}_{start_herd_number or 0}",
            raw_response=serialized_response,  # Save the serialized dict
        )

        # --- Start Parsing Logic ---
        herd_list = []
        has_more = False
        last_herd_in_batch = None  # Initialize to None

        if hasattr(response, "Response"):
            response_body = response.Response
            # Get has_more first, default to False
            has_more = bool(getattr(response_body, "FlereBesaetninger", False))
            # Get TilBesNr only if has_more is True
            if has_more:
                til_bes_nr_str = getattr(response_body, "TilBesNr", None)
                if til_bes_nr_str:
                    try:
                        last_herd_in_batch = int(til_bes_nr_str)
                    except (ValueError, TypeError):
                        logger.warning(f"Could not parse TilBesNr: {til_bes_nr_str}")
                        last_herd_in_batch = None  # Explicitly None if parsing fails
                # If TilBesNr is missing but has_more is True, keep last_herd_in_batch as None
            # If has_more is False, last_herd_in_batch remains None (its initial value)

            if hasattr(response_body, "BesaetningsnummerListe") and hasattr(
                response_body.BesaetningsnummerListe, "BesNrListe"
            ):
                raw_herd_list = response_body.BesaetningsnummerListe.BesNrListe
                if raw_herd_list:
                    # Ensure it's a list
                    if not isinstance(raw_herd_list, list):
                        raw_herd_list = [raw_herd_list]

                    # Extract valid integer herd numbers
                    for herd_num_str in raw_herd_list:
                        try:
                            herd_num_int = int(herd_num_str)
                            if herd_num_int > 0:
                                herd_list.append(herd_num_int)
                        except (ValueError, TypeError):
                            logger.warning(f"Skipping invalid herd number: {herd_num_str}")
            else:
                logger.warning("BesaetningsnummerListe or BesNrListe not found in response.")
        else:
            logger.warning("Response attribute not found in the SOAP response object.")

        logger.info(f"Found {len(herd_list)} herds. Has More: {has_more}. Last Herd: {last_herd_in_batch}")
        # Return herd_list, has_more (bool), and last_herd_in_batch (int or None)
        return herd_list, has_more, last_herd_in_batch
        # --- End Parsing Logic ---

    except Exception as e:
        logger.error(
            f"Failed to load herd list for Species: {species_code}, "
            f"Usage: {usage_code}, Start: {start_herd_number}: {e}",
            exc_info=True,
        )
        # Return empty list and False for has_more on error
        return [], False, None


def load_herd_details(client: Client, username: str, herd_number: int, species_code: int) -> Optional[Any]:
    """Load detailed information for a specific herd using 'hentStamoplysninger'."""
    logger.info(f"Fetching details for Herd: {herd_number}, Species: {species_code}...")

    # --- WSDL CHECK NEEDED ---
    # TODO: Verify the exact request structure required by 'hentStamoplysninger'.
    # Assuming it needs GLRCHRWSInfoInbound and a Request object containing BesaetningsNummer and DyreArtKode.
    # Construct request using factories (similar pattern)
    try:
        # --- Use Factory for Header ---
        GLRCHRWSInfoInboundFactory = client.get_type("ns0:GLRCHRWSInfoInboundType")
        common_header = GLRCHRWSInfoInboundFactory(
            **_create_base_request(username=username, track_id=f"load_details_{herd_number}")
        )

        # --- Use Factory for Request Parameters with Integers ---
        RequestParamsFactory = client.get_type("ns0:CHR_besaetningHentStamoplysningerRequestType")
        request_params = RequestParamsFactory(
            BesaetningsNummer=herd_number,  # Use int
            DyreArtKode=species_code,  # Use int
        )

        # Construct the payload dictionary with the request part using the factory object
        payload_content = {
            "GLRCHRWSInfoInbound": common_header,
            "Request": request_params,  # Use the factory object here
        }

        operation_name = "hentStamoplysninger"  # Confirmed operation name

        # Pass the payload dictionary as the argument
        response = client.service.hentStamoplysninger(CHR_besaetningHentStamoplysningerRequest=payload_content)

        if not response:
            logger.warning(f"No response received for {operation_name} (Herd: {herd_number}, Species: {species_code})")
            return None  # Return None if no response
        else:
            # Save the raw response using the updated function call signature
            save_raw_data(
                raw_response=response,  # Pass the raw Zeep object
                data_type="besaetning_details",
                identifier=f"{herd_number}_{species_code}",
            )
            return response  # Return the raw Zeep response object

    except Fault as f:
        logger.error(f"Fault occurred in load_herd_details: {f}", exc_info=True)
        return None


# --- Test Execution ---
if __name__ == "__main__":
    # # Temporary absolute import for direct script execution - REMOVED
    # from backend.chr_pipeline.bronze.export import save_raw_data
    # import json # Ensure json is imported for the test block - REMOVED

    logger.info("--- Starting Besætning Load Test --- ")

    # Parameters for testing hentStamoplysninger
    TEST_SPECIES_CODE = 15  # Example: Pigs
    TEST_HERD_NUMBER = 5392  # Replace with a real, stable herd number known to exist for the species/usage

    try:
        username, password = get_fvm_credentials()
        besaetning_client = create_soap_client(ENDPOINTS["besaetning"], username, password)

        # --- Test hentStamoplysninger ONLY ---
        logger.info(
            f"\n--- Testing load_herd_details ONLY (Herd: {TEST_HERD_NUMBER}, Species: {TEST_SPECIES_CODE}) ---"
        )
        herd_details_raw = load_herd_details(besaetning_client, username, TEST_HERD_NUMBER, TEST_SPECIES_CODE)

        if herd_details_raw:
            logger.info(
                f"Successfully called load_herd_details for herd {TEST_HERD_NUMBER}. Raw data saved via export module."
            )
            # Serialize and print part of the response for inspection
            try:
                serialized = serialize_object(herd_details_raw, dict)
                logger.info(f"Response Snippet: {json.dumps(serialized, indent=2)[:1000]}...")
                # Attempt to extract CHR num using the logic from main.py for verification
                chr_number = None
                response_body = serialized.get("Response", None)
                if response_body and isinstance(response_body, list) and len(response_body) > 0:
                    besaetning_info = response_body[0].get("Besaetning", None)
                    if besaetning_info:
                        ejendom_list = besaetning_info.get("Ejendom", [])
                        if ejendom_list and isinstance(ejendom_list, list) and len(ejendom_list) > 0:
                            chr_number_str = ejendom_list[0].get("CHRNummer")
                            if chr_number_str:
                                try:
                                    chr_number = int(chr_number_str)
                                except:
                                    pass
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
