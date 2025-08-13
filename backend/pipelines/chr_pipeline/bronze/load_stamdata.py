"""Module for loading CHR stamdata (Species, Usage Types) - Bronze Layer."""

import json
import logging
from typing import Any, Dict, Optional

from zeep import Client
from zeep.helpers import serialize_object

# Import the exporter and auth
from .auth import create_stamdata_client, get_fvm_credentials
from .export import save_raw_data
from .utils import create_base_request

# Set up logging
logger = logging.getLogger("backend.pipelines.chr_pipeline.bronze.load_stamdata")

# --- Generic SOAP Fetcher ---


def fetch_raw_soap_response(client: Client, operation_name: str, request_data: Dict) -> Optional[Any]:
    """Fetch raw response from a SOAP endpoint using Zeep."""
    try:
        operation = getattr(client.service, operation_name)
        # Pass request_data as a single positional argument (arg0)
        response = operation(request_data)
        logger.info(f"Successfully fetched raw data from {client.wsdl.location} - {operation_name}")
        # Return the raw Zeep object, serialization happens in export/transform
        return response
    except AttributeError:
        logger.error(f"Operation '{operation_name}' not found on client for {client.wsdl.location}")
    except Exception as e:
        logger.error(f"Error calling {operation_name} on {client.wsdl.location}: {e}")
        # Consider how to handle errors - raise, return None, return error object?
        # Returning None for now, caller should handle.
    return None


# --- Stamdata Loading Functions ---


def load_species_usage_combinations(client: Client, username: str) -> Optional[Any]:
    """Load raw species and usage combinations (ListDyrearterMedBrugsarter)."""
    logger.info(
        "Fetching species/usage combinations (ListDyrearterMedBrugsarter). This provides all species and usage types."
    )
    request = {
        "GLRCHRWSInfoInbound": create_base_request(username),
        "Request": {},  # Empty request gets all combinations according to WSDL
    }
    # Note: WSDL confirms Request key is needed, containing 
    # CHR_stamdataListDyrearterMedBrugsarterRequestType (which takes optional DyreArtKode)
    response = fetch_raw_soap_response(client, "ListDyrearterMedBrugsarter", request)
    if not response:
        logger.warning("No response received for ListDyrearterMedBrugsarter")
    return response


# --- Helper Functions from original (keep if needed for parsing here, otherwise move) ---


def safe_str(value: Any) -> Optional[str]:
    """Safely convert value to string, return None if empty."""
    if value is None:
        return None
    try:
        val_str = str(value).strip()
        return val_str if val_str else None
    except (ValueError, TypeError, AttributeError):
        return None


def safe_int(value: Any) -> Optional[int]:
    """Safely convert value to int, return None if not possible."""
    if value is None:
        return None
    try:
        val_str = str(value).strip()
        return int(val_str) if val_str else None
    except (ValueError, TypeError, AttributeError):
        return None


# Note: Removed the __main__ block, VetStat functions, and other unrelated load functions.
# Orchestration and calls to these functions will happen in main.py.

# --- Test Execution ---
if __name__ == "__main__":
    logger.info("--- Starting Stamdata Load Test --- ")

    try:
        # Get Credentials and Client
        username, password, certificate, private_key = get_fvm_credentials()

        # Create Client with robust authentication
        stamdata_client = create_stamdata_client()

        # Test load_species_usage_combinations
        logger.info("\n--- Testing load_species_usage_combinations (provides all species and usage types) ---")
        combinations_raw = load_species_usage_combinations(stamdata_client, username)
        if combinations_raw:
            logger.info("Raw Species/Usage Combinations Response (Top Level):")
            # Save the raw data using keyword arguments
            save_raw_data(raw_response=combinations_raw, data_type="stamdata_species_usage", identifier="all")

            # Serialize and pretty print the raw response for inspection
            try:
                serialized_response = serialize_object(combinations_raw)
                logger.info(json.dumps(serialized_response, indent=2, default=str))

                # Example: Log count of combinations received
                response_list = serialized_response.get("Response", [])
                if isinstance(response_list, list):
                    logger.info(f"Received {len(response_list)} species/usage combinations.")
                else:
                    logger.warning("Response format unexpected, could not count combinations.")

            except Exception as e:
                logger.error(f"Error serializing or logging response: {e}")
        else:
            logger.warning("load_species_usage_combinations returned None or empty.")

        logger.info("\n--- Stamdata Load Test Complete --- ")

    except Exception as e:
        logger.error(f"Error during Stamdata load test: {e}", exc_info=True)
