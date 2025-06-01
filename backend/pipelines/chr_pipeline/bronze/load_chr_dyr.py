"""Module for loading CHR_dyr data (Animal Movements) - Bronze Layer."""

import logging
import os
import uuid
from datetime import date, timedelta
from typing import Any, Dict, Optional

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
logger = logging.getLogger("backend.pipelines.chr_pipeline.bronze.load_chr_dyr")

# --- Constants ---

# API Endpoints (WSDL URLs)
ENDPOINTS = {"chr_dyr": "https://ws.fvst.dk/service/CHR_dyrWS?wsdl"}

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
    session = Session()
    session.verify = certifi.where()
    transport = Transport(session=session)
    try:
        client = Client(wsdl_url, transport=transport, wsse=UsernameToken(username, password))
        logger.info(f"Successfully created SOAP client for {wsdl_url}")
        return client
    except Exception as e:
        logger.error(f"Failed to create SOAP client for {wsdl_url}: {e}")
        raise


# --- Base Request Structure ---


def _create_base_request(username: str, session_id: str = "1", track_id: str = "load_chr_dyr") -> Dict[str, str]:
    """Create the common GLRCHRWSInfoInbound structure."""
    return {
        "BrugerNavn": username,
        "KlientId": DEFAULT_CLIENT_ID,
        "SessionId": session_id,
        "IPAdresse": "",
        "TrackID": f"{track_id}-{uuid.uuid4()}",
    }


# --- Animal Movement Loading Functions ---


def load_animal_movements(
    chr_dyr_client: Client,
    username: str,
    herd_number: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> Optional[Any]:
    """
    Fetches animal movement data for a given herd using besListAktOms.

    Args:
        chr_dyr_client: The CHR_dyr SOAP client
        username: Username for authentication
        herd_number: The herd number to fetch data for
        start_date: Optional start date for filtering (if None, gets all available data)
        end_date: Optional end date for filtering (if None, uses today)

    Returns:
        Raw response object or None if failed
    """

    # Set default date range if not provided (last 5 years for reasonable performance)
    if start_date is None and end_date is None:
        end_date = date.today()
        start_date = end_date - timedelta(days=365 * 5)  # 5 years of data
    elif end_date is None:
        end_date = date.today()

    date_suffix = f"_{start_date}_{end_date}" if start_date else "_all"

    logger.info(
        f"Fetching animal movements for herd {herd_number}"
        + (f" from {start_date} to {end_date}" if start_date else " (all available data)")
    )

    try:
        # Create request structure according to WSDL/XSD
        GLRCHRWSInfoInboundFactory = chr_dyr_client.get_type("ns0:GLRCHRWSInfoInboundType")
        common_header = GLRCHRWSInfoInboundFactory(**_create_base_request(username))

        CHR_dyrChrBesListeRequestTypeFactory = chr_dyr_client.get_type("ns0:CHR_dyrChrBesListeRequestType")

        # Build request parameters
        request_params_dict = {"BesaetningsNummer": herd_number}
        if start_date:
            request_params_dict["PeriodeFra"] = start_date
        if end_date:
            request_params_dict["PeriodeTil"] = end_date

        request_params = CHR_dyrChrBesListeRequestTypeFactory(**request_params_dict)

        # Combine into payload
        payload_content = {"GLRCHRWSInfoInbound": common_header, "Request": request_params}

        # Call the operation
        response = chr_dyr_client.service.besListAktOms(CHR_dyrChrBesListeRequest=payload_content)

        if response is None:
            logger.warning(f"No response received for herd {herd_number}")
            return None

        # Process and save the response
        serialized_response = serialize_object(response, dict)

        # Save raw data
        save_raw_data(
            data_type="chr_dyr_animal_movements",
            identifier=f"{herd_number}{date_suffix}",
            raw_response=serialized_response,
        )

        # Log statistics
        if hasattr(response, "Response") and response.Response:
            resp = response.Response[0] if isinstance(response.Response, list) else response.Response
            animals = getattr(resp, "Enkeltdyrsoplysninger", [])
            animal_count = len(animals) if animals else 0

            period_fra = getattr(resp, "PeriodeFra", None)
            period_til = getattr(resp, "PeriodeTil", None)

            logger.info(
                f"Herd {herd_number}: {animal_count} animals found "
                + (f"(period: {period_fra} to {period_til})" if period_fra else "")
            )

            if animal_count > 0:
                logger.debug(f"Sample animal from herd {herd_number}: " + f"CKR={getattr(animals[0], 'CkrNr', 'N/A')}")

        return response

    except Fault as soap_fault:
        logger.error(f"SOAP fault for herd {herd_number}: {soap_fault}")
        return None
    except Exception as e:
        logger.error(f"Error fetching animal movements for herd {herd_number}: {e}")
        return None


def load_animal_movements_task(
    chr_dyr_client: Client, username: str, herd_number: int, start_date: Optional[date], end_date: Optional[date]
) -> Optional[Any]:
    """
    Wrapper function for parallel processing of animal movement loading.

    Args:
        chr_dyr_client: The CHR_dyr SOAP client
        username: Username for authentication
        herd_number: The herd number to fetch data for
        start_date: Optional start date for filtering
        end_date: Optional end date for filtering

    Returns:
        Raw response object or None if failed
    """
    return load_animal_movements(chr_dyr_client, username, herd_number, start_date, end_date)
