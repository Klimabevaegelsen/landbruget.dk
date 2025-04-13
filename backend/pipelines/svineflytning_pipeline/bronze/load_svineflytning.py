"""Module for fetching pig movement data from SvineflytningWS."""

import logging
import certifi
from datetime import date, datetime, timedelta
from typing import Dict, List, Any, Iterator
from zeep import Client, exceptions as zeep_exceptions, Settings
from zeep.transports import Transport
from zeep.wsse.username import UsernameToken
from zeep.helpers import serialize_object
from requests import Session
import time
import json
from tenacity import retry, stop_after_attempt, wait_exponential, before_log, after_log, retry_if_exception_type
from pathlib import Path
import os
from dotenv import load_dotenv
from tqdm.auto import tqdm
from .export import export_movements

logger = logging.getLogger(__name__)

class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for handling datetime and date objects."""
    def default(self, obj: Any) -> str:
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)

# Constants
ENDPOINTS = {
    'prod': 'https://ws.fvst.dk/service/SvineflytningWS?wsdl',
    'test': 'https://wstest.fvst.dk/service/SvineflytningWS?wsdl'
}

DEFAULT_CLIENT_ID = os.getenv('FVM_CLIENT_ID', 'LandbrugsData')
MAX_DATE_RANGE_DAYS = 3  # API limit: maximum 3 days per request
VERIFY_SSL = os.getenv('FVM_VERIFY_SSL', 'true').lower() == 'true'

def get_fvm_credentials() -> tuple[str, str]:
    """
    Get FVM credentials from environment variables.
    
    Returns:
        tuple[str, str]: A tuple containing the username and password.
        
    Raises:
        ValueError: If either username or password is not found in environment variables.
    """
    username = os.getenv('FVM_USERNAME')
    password = os.getenv('FVM_PASSWORD')
    
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
    
    client = Client(
        endpoint,
        settings=settings,
        transport=transport,
        wsse=UsernameToken(username, password)
    )
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
            'GLRCHRWSInfoInbound': {
                'KlientId': DEFAULT_CLIENT_ID,
                'BrugerNavn': os.getenv('FVM_USERNAME'),
                'SessionId': '1',
                'IPAdresse': '',
                'TrackID': '1'
            },
            'Request': {
                'RegistreringsDatoFra': start_date.isoformat(),
                'RegistreringsDatoTil': end_date.isoformat()
            }
        }
        
        response = client.service.listAlleFlytningerIPerioden(request)
        response_info = serialize_object(response)
        
        return {
            'timestamp': datetime.utcnow().isoformat(),
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'response': response_info
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
    show_progress: bool = False,
    test_mode: bool = False
) -> Dict[str, Any]:
    """
    Fetch all movements for the given date range and stream responses to storage.
    
    Args:
        client: The SOAP client to use.
        start_date: The start date of the range.
        end_date: The end date of the range.
        output_dir: The directory to save the responses to (used only for local storage).
        show_progress: Whether to show progress bars.
        test_mode: Whether to run in test mode (limited data).
        
    Returns:
        Dict[str, Any]: A dictionary containing metadata about the export.
    """
    logger.debug(f"Starting to fetch all movements from {start_date} to {end_date}")
    
    # In test mode, limit to one day
    if test_mode:
        end_date = start_date
        logger.warning("Running in test mode - limiting to single day")
    
    # Calculate total number of chunks for progress bar
    total_days = (end_date - start_date).days + 1
    total_chunks = (total_days + MAX_DATE_RANGE_DAYS - 1) // MAX_DATE_RANGE_DAYS
    
    # Get timestamp for this export run
    export_timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    
    def response_generator() -> Iterator[Dict[str, Any]]:
        """Generate responses one at a time."""
        current_date = start_date
        chunks_processed = 0
        
        # Create progress bar if requested
        pbar = tqdm(
            total=total_chunks,
            desc="Fetching movements",
            unit="chunks",
            disable=not show_progress
        )
        
        while current_date <= end_date:
            # Calculate end date for this chunk (maximum 3 days)
            chunk_end = min(current_date + timedelta(days=MAX_DATE_RANGE_DAYS - 1), end_date)
            
            try:
                response = fetch_movements(client, current_date, chunk_end)
                yield response
                chunks_processed += 1
                pbar.update(1)
                
                logger.debug(f"Sleeping for 0.1s before next chunk")
                time.sleep(0.1)  # Small delay to avoid overwhelming the server
                
            except Exception as e:
                logger.error(f"Error fetching movements for period {current_date} to {chunk_end}: {str(e)}")
                pbar.close()
                raise
            
            current_date = chunk_end + timedelta(days=1)
        
        pbar.close()
    
    # Stream responses directly to storage
    filename = "svineflytning.json"
    export_result = export_movements(response_generator(), export_timestamp, filename)
    
    result = {
        "export_timestamp": export_timestamp,
        "start_date": start_date,
        "end_date": end_date,
        "storage_path": export_result["destination"]
    }
    logger.debug(f"Pipeline execution completed: {result}")
    return result
