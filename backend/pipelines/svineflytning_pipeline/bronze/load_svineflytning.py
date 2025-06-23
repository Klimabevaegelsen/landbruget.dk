"""Module for fetching pig movement data from SvineflytningWS."""

import json
import logging
import os
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Tuple

import certifi
from requests import Session
from tenacity import before_log, retry, retry_if_exception_type, stop_after_attempt, wait_exponential
from tqdm.auto import tqdm
from zeep import Client, Settings
from zeep import exceptions as zeep_exceptions
from zeep.helpers import serialize_object
from zeep.transports import Transport
from zeep.wsse.username import UsernameToken

from .export import DateTimeEncoder, export_movements_optimized

logger = logging.getLogger(__name__)

# Constants
ENDPOINTS = {
    "prod": "https://ws.fvst.dk/service/SvineflytningWS?wsdl",
    "test": "https://wstest.fvst.dk/service/SvineflytningWS?wsdl",
}

DEFAULT_CLIENT_ID = os.getenv("FVM_CLIENT_ID", "LandbrugsData")
MAX_DATE_RANGE_DAYS = 3  # API limit: maximum 3 days per request
VERIFY_SSL = os.getenv("FVM_VERIFY_SSL", "true").lower() == "true"


def get_fvm_credentials() -> tuple[str, str]:
    """
    Get FVM credentials from environment variables.

    Returns:
        tuple[str, str]: A tuple containing the username and password.

    Raises:
        ValueError: If either username or password is not found in environment variables.
    """
    username = os.getenv("FVM_USERNAME")
    password = os.getenv("FVM_PASSWORD")

    if not username or not password:
        raise ValueError("FVM credentials not found in environment variables")

    logger.debug("Successfully retrieved FVM credentials")
    return username, password


def create_client(endpoint: str, username: str, password: str) -> Client:
    """
    Create a SOAP client with authentication.

    Args:
        endpoint: The WSDL endpoint URL.
        username: The username for authentication.
        password: The password for authentication.

    Returns:
        Client: A configured SOAP client.
    """
    logger.debug(f"Creating SOAP client for endpoint: {endpoint}")
    session = Session()
    if VERIFY_SSL:
        session.verify = certifi.where()  # Ensure CA certificates are used
    else:
        session.verify = False
        logger.warning("SSL verification is disabled")

    settings = Settings(strict=False, xml_huge_tree=True)
    transport = Transport(session=session)

    client = Client(endpoint, settings=settings, transport=transport, wsse=UsernameToken(username, password))
    logger.debug("Successfully created SOAP client")
    return client


def validate_date_range(start_date: date, end_date: date) -> None:
    """
    Validate the date range for the request.

    Args:
        start_date: The start date of the range.
        end_date: The end date of the range.

    Raises:
        ValueError: If the date range is invalid or exceeds the maximum allowed days.
    """
    if start_date > end_date:
        raise ValueError("Start date must be before or equal to end date")

    date_diff = (end_date - start_date).days + 1
    if date_diff > MAX_DATE_RANGE_DAYS:
        raise ValueError(f"Date range cannot exceed {MAX_DATE_RANGE_DAYS} days due to API limitations")


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    before=before_log(logger, logging.DEBUG),
    after=before_log(logger, logging.DEBUG),
    retry=retry_if_exception_type((zeep_exceptions.Fault, zeep_exceptions.TransportError)),
)
def fetch_movements(client: Client, start_date: date, end_date: date) -> Dict[str, Any]:
    """
    Fetch movements for a given date range and stream directly to storage.

    Args:
        client: The SOAP client to use.
        start_date: The start date of the range.
        end_date: The end date of the range.

    Returns:
        Dict[str, Any]: A dictionary containing metadata about the export.
    """
    try:
        logger.debug(f"Fetching movements for period {start_date} to {end_date}")

        # Create the request object with the correct structure
        request = {
            "GLRCHRWSInfoInbound": {
                "KlientId": DEFAULT_CLIENT_ID,
                "BrugerNavn": os.getenv("FVM_USERNAME"),
                "SessionId": "1",
                "IPAdresse": "",
                "TrackID": "1",
            },
            "Request": {"RegistreringsDatoFra": start_date.isoformat(), "RegistreringsDatoTil": end_date.isoformat()},
        }

        response = client.service.listAlleFlytningerIPerioden(request)
        response_info = serialize_object(response)

        # Add metadata with dates in ISO format
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "response": response_info,
        }

    except zeep_exceptions.Fault as e:
        logger.error(f"SOAP fault while fetching movements: {str(e)}")
        raise
    except zeep_exceptions.TransportError as e:
        logger.error(f"Transport error while fetching movements: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error fetching movements: {str(e)}")
        raise


def fetch_all_movements(
    client: Client,
    start_date: date,
    end_date: date,
    output_dir: str,
    max_concurrent_fetches: int = 5,
    buffer_size: int = 50,
    show_progress: bool = False,
    test_mode: bool = False,
) -> Dict[str, Any]:
    """
    Fetch all movements for the given date range with parallel processing.

    Args:
        client: The SOAP client to use.
        start_date: The start date of the range.
        end_date: The end date of the range.
        output_dir: The directory to save the responses to.
        max_concurrent_fetches: Maximum number of concurrent API calls.
        buffer_size: Number of responses to accumulate before writing to disk.
        show_progress: Whether to show progress bars.
        test_mode: Whether to run in test mode (limited data).

    Returns:
        Dict[str, Any]: A dictionary containing metadata about the export.
    """
    logger.debug(f"Starting to fetch all movements from {start_date} to {end_date}")

    if test_mode:
        end_date = start_date
        logger.warning("Running in test mode - limiting to single day")

    # Generate all date chunks
    date_chunks = []
    current_date = start_date
    while current_date <= end_date:
        chunk_end = min(current_date + timedelta(days=MAX_DATE_RANGE_DAYS - 1), end_date)
        date_chunks.append((current_date, chunk_end))
        current_date = chunk_end + timedelta(days=1)

    # Create temporary directory for buffers
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_files = []
        current_buffer = []
        buffer_count = 0

        def flush_buffer() -> str:
            nonlocal current_buffer, buffer_count
            if not current_buffer:
                return None

            # Write buffer to temporary file using DateTimeEncoder
            temp_path = Path(temp_dir) / f"buffer_{buffer_count}.json"
            with open(temp_path, "w") as f:
                json.dump(current_buffer, f, cls=DateTimeEncoder)
            temp_files.append(temp_path)
            buffer_count += 1
            current_buffer = []
            return str(temp_path)

        def fetch_chunk(dates: Tuple[date, date]) -> Dict:
            chunk_start, chunk_end = dates
            try:
                response = fetch_movements(client, chunk_start, chunk_end)
                time.sleep(0.1)  # Small delay to avoid overwhelming the server
                return response
            except Exception as e:
                logger.error(f"Error fetching chunk {dates}: {e}")
                raise

        # Progress bar setup
        pbar = tqdm(total=len(date_chunks), desc="Fetching movements", unit="chunks", disable=not show_progress)

        # Fetch data in parallel
        with ThreadPoolExecutor(max_workers=max_concurrent_fetches) as executor:
            futures = [executor.submit(fetch_chunk, chunk) for chunk in date_chunks]

            for future in futures:
                try:
                    response = future.result()
                    current_buffer.append(response)

                    # If buffer reaches size limit, flush to temp file
                    if len(current_buffer) >= buffer_size:
                        flush_buffer()

                    pbar.update(1)
                except Exception as e:
                    logger.error(f"Error in parallel fetch: {e}")
                    pbar.close()
                    raise

        # Flush any remaining data
        if current_buffer:
            flush_buffer()

        pbar.close()

        # Get timestamp for this export run
        export_timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        # Export the data using the optimized export function
        export_result = export_movements_optimized(temp_files, export_timestamp, len(date_chunks))

        result = {
            "export_timestamp": export_timestamp,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "storage_path": export_result["destination"],
            "total_chunks": len(date_chunks),
        }
        logger.debug(f"Pipeline execution completed: {result}")
        return result
