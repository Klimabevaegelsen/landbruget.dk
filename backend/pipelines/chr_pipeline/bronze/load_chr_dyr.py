"""Module for loading CHR_dyr data (Animal Movements) - Bronze Layer."""

import logging
import os
import time
import uuid
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

import certifi
import requests
from dotenv import load_dotenv
from requests import Session
from zeep import Client
from zeep.exceptions import Fault
from zeep.transports import Transport
from zeep.wsse.username import UsernameToken

# Import the exporter function

# Import GCS access for persistent storage
try:
    from unified_pipeline.util.gcs_access import GCSDataAccess

    GCS_AVAILABLE = True
except ImportError:
    GCS_AVAILABLE = False
    GCSDataAccess = None

# Load environment variables
load_dotenv()

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

    adapter = requests.adapters.HTTPAdapter()
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    transport = Transport(session=session)
    try:
        client = Client(wsdl_url, transport=transport, wsse=UsernameToken(username, password))
        logger.info(f"Successfully created SOAP client for {wsdl_url}")
        return client
    except Exception as e:
        logger.error(f"Failed to create SOAP client for {wsdl_url}: {e}")
        raise


# Global set to track problematic herds that consistently fail
PROBLEMATIC_HERDS = set()
PROBLEMATIC_HERDS_LOADED = False

# Global dict to track herds that need special handling (smaller date ranges)
HIGH_VOLUME_HERDS = {}  # herd_id -> {"max_days": int, "last_updated": datetime, "volume_estimate": int}
HIGH_VOLUME_HERDS_LOADED = False


def _load_problematic_herds() -> None:
    """Load problematic herds from persistent storage (GCS)."""
    global PROBLEMATIC_HERDS, PROBLEMATIC_HERDS_LOADED

    if PROBLEMATIC_HERDS_LOADED:
        return

    if GCS_AVAILABLE:
        try:
            gcs = GCSDataAccess()
            bucket_name = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")
            problematic_herds_path = "bronze/chr/problematic_herds.json"
            gcs_path = f"gs://{bucket_name}/{problematic_herds_path}"

            try:
                data = gcs.download_json(gcs_path)
                if data and "problematic_herds" in data:
                    PROBLEMATIC_HERDS.update(data["problematic_herds"])
                    logger.info(f"Loaded {len(PROBLEMATIC_HERDS)} problematic herds from GCS")
                else:
                    logger.info("No problematic herds found in GCS - starting with empty set")
            except Exception as e:
                logger.debug(f"Could not load problematic herds from GCS: {e}")
                # This is expected on first run or if file doesn't exist

        except Exception as e:
            logger.debug(f"Failed to access GCS: {e}")
    else:
        logger.debug("GCS access not available - problematic herds will not persist across runs")

    PROBLEMATIC_HERDS_LOADED = True


def _load_high_volume_herds() -> None:
    """Load high-volume herds configuration from GCS or create default."""
    global HIGH_VOLUME_HERDS
    if HIGH_VOLUME_HERDS is not None:
        return

    if GCS_AVAILABLE:
        try:
            gcs_data_access = GCSDataAccess()
            bucket_name = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")
            config_path = f"gs://{bucket_name}/bronze/chr/config/high_volume_herds.json"

            if gcs_data_access.file_exists(config_path):
                HIGH_VOLUME_HERDS = gcs_data_access.download_json(config_path)
                logger.info(f"Loaded {len(HIGH_VOLUME_HERDS)} high-volume herds from GCS")
            else:
                logger.info("No high-volume herds config found in GCS, creating default")
                HIGH_VOLUME_HERDS = {}

            # Initialize known problematic herds if not already configured
            _initialize_known_high_volume_herds()

        except Exception as e:
            logger.warning(f"Failed to load high-volume herds config: {e}, using empty dict")
            HIGH_VOLUME_HERDS = {}
            _initialize_known_high_volume_herds()
    else:
        logger.debug("GCS access not available - using empty high-volume herds dict")
        HIGH_VOLUME_HERDS = {}
        _initialize_known_high_volume_herds()


def _initialize_known_high_volume_herds() -> None:
    """Initialize known high-volume herds with optimized settings."""
    global HIGH_VOLUME_HERDS

    # Known problematic herds from production logs
    # UPDATED: Use larger chunks to reduce request volume and improve performance
    known_high_volume = {
        "112389": {"max_days": 30, "reason": "known_high_volume", "volume_estimate": None},  # Monthly chunks
        "104641": {"max_days": 30, "reason": "known_high_volume", "volume_estimate": None},  # Monthly chunks
    }

    for herd_str, config in known_high_volume.items():
        if herd_str not in HIGH_VOLUME_HERDS:
            HIGH_VOLUME_HERDS[herd_str] = {
                **config,
                "last_updated": datetime.now().isoformat(),
                "auto_initialized": True,
            }
            logger.info(f"Auto-initialized high-volume herd {herd_str} with {config['max_days']}-day chunks")

    # Save if we added any new herds
    if any(config.get("auto_initialized") for config in HIGH_VOLUME_HERDS.values()):
        _save_high_volume_herds()


def _save_problematic_herds() -> None:
    """Save problematic herds to persistent storage (GCS)."""
    global PROBLEMATIC_HERDS

    if not PROBLEMATIC_HERDS:
        return

    if GCS_AVAILABLE:
        try:
            gcs = GCSDataAccess()
            bucket_name = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")
            problematic_herds_path = "bronze/chr/problematic_herds.json"
            gcs_path = f"gs://{bucket_name}/{problematic_herds_path}"

            data = {
                "problematic_herds": list(PROBLEMATIC_HERDS),
                "last_updated": datetime.now().isoformat(),
                "total_count": len(PROBLEMATIC_HERDS),
            }

            gcs.upload_json(data, gcs_path)
            logger.info(f"Saved {len(PROBLEMATIC_HERDS)} problematic herds to GCS")

        except Exception as e:
            logger.warning(f"Could not save problematic herds to GCS: {e}")
    else:
        logger.debug("GCS access not available - cannot save problematic herds")


def _save_high_volume_herds() -> None:
    """Save high-volume herds configuration to GCS."""
    if GCS_AVAILABLE:
        try:
            gcs_data_access = GCSDataAccess()
            bucket_name = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")
            config_path = f"gs://{bucket_name}/bronze/chr/config/high_volume_herds.json"

            # Save the high-volume herds dictionary directly as JSON
            gcs_data_access.upload_json(HIGH_VOLUME_HERDS, config_path)

            logger.info(f"Saved {len(HIGH_VOLUME_HERDS)} high-volume herd configurations to GCS")

        except Exception as e:
            logger.warning(f"Failed to save high-volume herds config to GCS: {e}")
            # Continue execution - this is not critical for pipeline operation
    else:
        logger.debug("GCS access not available - cannot save high-volume herds config")


def add_problematic_herd(herd_number: int) -> None:
    """Add a herd to the problematic herds list."""
    _load_problematic_herds()  # Ensure we have the latest data

    PROBLEMATIC_HERDS.add(herd_number)
    logger.warning(f"Added herd {herd_number} to problematic herds list (will be skipped in future)")

    # Save immediately to persist the change
    _save_problematic_herds()


def add_high_volume_herd(herd_number: int, max_days: int, volume_estimate: int = None) -> None:
    """Add a herd to the high-volume herds list with specific processing constraints."""
    _load_high_volume_herds()

    # Auto-optimize chunk size based on herd number and known patterns
    # UPDATED: Use larger chunks to reduce request volume and improve performance
    if herd_number in [112389, 104641]:  # Known problematic herds from logs
        max_days = min(max_days, 30)  # Monthly chunks instead of weekly for these herds
        logger.warning(f"Herd {herd_number} is a known high-volume herd - using monthly 30-day chunking")
    elif volume_estimate and volume_estimate > 100000:
        max_days = min(max_days, 14)  # Bi-weekly chunks instead of 3-day for massive herds
        logger.warning(f"Herd {herd_number} has massive volume estimate ({volume_estimate}) - using 14-day chunking")
    elif volume_estimate and volume_estimate > 50000:
        max_days = min(max_days, 30)  # Monthly chunks instead of weekly for large herds

    HIGH_VOLUME_HERDS[str(herd_number)] = {
        "max_days": max_days,
        "last_updated": datetime.now().isoformat(),
        "volume_estimate": volume_estimate,
        "reason": "high_volume_detected",
        "auto_optimized": True if herd_number in [112389, 104641] else False,
    }

    logger.warning(f"Added herd {herd_number} to high-volume herds (max {max_days} days per request)")
    _save_high_volume_herds()


def get_optimal_date_range(herd_number: int, requested_start: date, requested_end: date) -> List[tuple]:
    """
    Get optimal date ranges for a herd, splitting into smaller chunks if it's high-volume.

    Returns:
        List of (start_date, end_date) tuples
    """
    _load_high_volume_herds()

    herd_str = str(herd_number)

    # Pre-configure known problematic herds with reasonable chunking
    # UPDATED: Use larger chunks to reduce request volume and improve performance
    if herd_number in [112389, 104641] and herd_str not in HIGH_VOLUME_HERDS:
        logger.warning(f"Auto-configuring known problematic herd {herd_number} with 30-day chunks")
        add_high_volume_herd(herd_number, max_days=30, volume_estimate=None)

    if herd_str in HIGH_VOLUME_HERDS:
        max_days = HIGH_VOLUME_HERDS[herd_str]["max_days"]
        logger.info(f"Herd {herd_number} is high-volume, splitting into {max_days}-day chunks")

        # Split the requested range into smaller chunks
        chunks = []
        current_start = requested_start

        # For very large date ranges, add extra safety by limiting total chunks
        total_days = (requested_end - requested_start).days + 1
        max_chunks = 50  # Reduced from 100 to prevent excessive chunking

        if total_days / max_days > max_chunks:
            logger.warning(
                f"Herd {herd_number}: Date range too large ({total_days} days), limiting to {max_chunks} chunks"
            )
            max_days = max(max_days, total_days // max_chunks)

        while current_start <= requested_end:
            current_end = min(current_start + timedelta(days=max_days - 1), requested_end)
            chunks.append((current_start, current_end))
            current_start = current_end + timedelta(days=1)

        logger.info(f"Split herd {herd_number} date range into {len(chunks)} chunks of max {max_days} days")
        return chunks
    else:
        # Normal processing - single range
        return [(requested_start, requested_end)]


def is_problematic_herd(herd_number: int) -> bool:
    """Check if a herd is in the problematic herds list."""
    _load_problematic_herds()  # Ensure we have the latest data
    return herd_number in PROBLEMATIC_HERDS


def is_high_volume_herd(herd_number: int) -> bool:
    """Check if a herd is in the high-volume herds list."""
    _load_high_volume_herds()
    return str(herd_number) in HIGH_VOLUME_HERDS


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
    max_retries: int = 3,
) -> Optional[Any]:
    """
    Fetches animal movement data for a given herd using besListAktOms.

    Args:
        chr_dyr_client: The CHR_dyr SOAP client
        username: Username for authentication
        herd_number: The herd number to fetch data for
        start_date: Optional start date for filtering (if None, gets all available data)
        end_date: Optional end date for filtering (if None, uses today)
        max_retries: Maximum number of retry attempts for failed requests

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

    # Skip herds that have been identified as problematic
    if is_problematic_herd(herd_number):
        logger.warning(f"Skipping herd {herd_number} (marked as problematic)")
        return {
            "reporting_herd_number": herd_number,
            "movements": [],
            "skipped_reason": "problematic_herd",
            "summary_stats": {"total_animals_processed": 0, "unique_movement_dates": 0, "counterparty_herds": 0},
        }

    logger.info(
        f"Fetching animal movements for herd {herd_number}"
        + (f" from {start_date} to {end_date}" if start_date else " (all available data)")
    )

    # Retry logic for problematic herds
    for attempt in range(max_retries + 1):
        try:
            if attempt > 0:
                wait_time = 2**attempt  # Exponential backoff: 2s, 4s, 8s
                logger.info(
                    f"Retrying herd {herd_number} (attempt {attempt + 1}/{max_retries + 1}) after {wait_time}s delay"
                )
                time.sleep(wait_time)

            # Create request structure according to WSDL/XSD
            logger.debug(f"Herd {herd_number}: Creating SOAP request structure...")
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
            logger.debug(f"Herd {herd_number}: Starting SOAP request...")
            request_start_time = time.time()

            try:
                response = chr_dyr_client.service.besListAktOms(CHR_dyrChrBesListeRequest=payload_content)
                request_duration = time.time() - request_start_time
                logger.debug(f"Herd {herd_number}: SOAP request completed in {request_duration:.1f}s")
            except Exception as soap_error:
                request_duration = time.time() - request_start_time
                logger.error(f"Herd {herd_number}: SOAP request failed after {request_duration:.1f}s: {soap_error}")
                raise

            if response is None:
                logger.warning(f"No response received for herd {herd_number} (attempt {attempt + 1})")
                if attempt < max_retries:
                    continue
                return None

            # Log request timing for monitoring and mark extremely slow herds as problematic
            if request_duration > 1800:  # 30 minutes - mark as problematic
                logger.error(
                    f"Extremely slow request for herd {herd_number}: {request_duration:.1f}s - marking as problematic"
                )
                add_problematic_herd(herd_number)
                return {
                    "reporting_herd_number": herd_number,
                    "movements": [],
                    "skipped_reason": "extremely_slow_processing",
                    "summary_stats": {
                        "total_animals_processed": 0,
                        "unique_movement_dates": 0,
                        "counterparty_herds": 0,
                    },
                }
            elif request_duration > 300:  # Log slow requests (>5 minutes)
                logger.warning(f"Slow request for herd {herd_number}: {request_duration:.1f}s")
            elif request_duration > 60:  # Log moderately slow requests (>1 minute)
                logger.info(f"Moderate request for herd {herd_number}: {request_duration:.1f}s")

            # Process and aggregate the response instead of saving raw individual records
            aggregation_start_time = time.time()
            logger.info(f"Herd {herd_number}: Starting data aggregation...")

            try:
                movement_summaries = _aggregate_cattle_movements(response, herd_number)
                aggregation_duration = time.time() - aggregation_start_time
                logger.info(f"Herd {herd_number}: Data aggregation completed in {aggregation_duration:.1f}s")
            except Exception as agg_error:
                aggregation_duration = time.time() - aggregation_start_time
                logger.error(
                    f"Herd {herd_number}: Data aggregation failed after {aggregation_duration:.1f}s: {agg_error}"
                )
                raise

            # Log memory savings - show what we would have saved vs what we actually save
            if hasattr(response, "Response") and response.Response:
                resp = response.Response[0] if isinstance(response.Response, list) else response.Response
                animals = getattr(resp, "Enkeltdyrsoplysninger", [])
                # Handle case where Enkeltdyrsoplysninger is an integer instead of a list
                if isinstance(animals, int):
                    individual_record_count = animals  # Use the integer value directly
                else:
                    individual_record_count = len(animals) if animals else 0
                summary_record_count = len(movement_summaries.get("movements", []))

                if individual_record_count > 0:
                    reduction_ratio = (individual_record_count - summary_record_count) / individual_record_count * 100
                    logger.info(
                        f"Herd {herd_number}: Reduced {individual_record_count} individual animal records to {summary_record_count} movement summaries ({reduction_ratio:.1f}% reduction)"
                    )

            # Return the data instead of saving to buffer (batch processing will handle saving)
            if movement_summaries and movement_summaries.get("movements"):
                logger.info(f"Herd {herd_number}: Processed {len(movement_summaries['movements'])} movement summaries")
                return movement_summaries
            else:
                # Return minimal record indicating we processed this herd
                minimal_record = {"reporting_herd_number": herd_number, "movements": [], "no_movements_found": True}
                return minimal_record

            # Log statistics
            if hasattr(response, "Response") and response.Response:
                resp = response.Response[0] if isinstance(response.Response, list) else response.Response
                animals = getattr(resp, "Enkeltdyrsoplysninger", [])
                # Handle case where Enkeltdyrsoplysninger is an integer instead of a list
                if isinstance(animals, int):
                    animal_count = animals  # Use the integer value directly
                else:
                    animal_count = len(animals) if animals else 0

                period_fra = getattr(resp, "PeriodeFra", None)
                period_til = getattr(resp, "PeriodeTil", None)

                logger.info(
                    f"Herd {herd_number}: {animal_count} animals found "
                    + (f"(period: {period_fra} to {period_til})" if period_fra else "")
                )

                # FIXED: Comprehensive safety check before accessing animals[0]
                if (
                    animal_count > 0
                    and not isinstance(animals, (int, float, bool, str, bytes, type(None)))
                    and hasattr(animals, "__len__")
                    and hasattr(animals, "__getitem__")
                    and len(animals) > 0
                ):
                    try:
                        logger.debug(
                            f"Sample animal from herd {herd_number}: " + f"CKR={getattr(animals[0], 'CkrNr', 'N/A')}"
                        )
                    except (IndexError, TypeError, AttributeError) as e:
                        logger.debug(f"Could not access sample animal from herd {herd_number}: {e}")

            # Return aggregated summaries instead of massive raw response
            return movement_summaries

        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error for herd {herd_number} (attempt {attempt + 1}): {e}")
            if attempt < max_retries:
                continue
            else:
                logger.error(f"Max retries exceeded for herd {herd_number} due to connection error")
                return None

        except Fault as soap_fault:
            logger.error(f"SOAP fault for herd {herd_number} (attempt {attempt + 1}): {soap_fault}")
            if attempt < max_retries:
                continue
            else:
                logger.error(f"Max retries exceeded for herd {herd_number} due to SOAP fault: {soap_fault}")
                return None

        except Exception as e:
            logger.error(f"Error fetching animal movements for herd {herd_number} (attempt {attempt + 1}): {e}")
            if attempt < max_retries:
                continue
            else:
                logger.error(f"Max retries exceeded for herd {herd_number} due to error: {e}")
                return None

    # Should not reach here, but just in case
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


def load_cattle_movement_summaries(
    chr_dyr_client: Client,
    username: str,
    herd_number: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> Optional[Dict]:
    """
    Fetches cattle movement data using the unified pipeline pattern.

    This function maintains the original interface but uses the new consolidated
    DuckDB processing approach internally.
    """
    # Set reasonable date range (last 5 years for cattle movements)
    if start_date is None and end_date is None:
        end_date = date.today()
        start_date = end_date - timedelta(days=365 * 5)  # 5 years of data
    elif end_date is None:
        end_date = date.today()

    logger.info(f"Fetching cattle movement summaries for herd {herd_number} from {start_date} to {end_date}")

    # Check if this is a high-volume herd and get optimal date ranges
    date_ranges = get_optimal_date_range(herd_number, start_date, end_date)

    if len(date_ranges) > 1:
        logger.info(f"Processing herd {herd_number} in {len(date_ranges)} chunks due to high volume")

    # Process all date ranges and aggregate results
    total_movements = 0
    successful_chunks = 0
    failed_chunks = 0

    try:
        for chunk_idx, (chunk_start, chunk_end) in enumerate(date_ranges):
            logger.info(
                f"Processing herd {herd_number} chunk {chunk_idx + 1}/{len(date_ranges)}: {chunk_start} to {chunk_end}"
            )

            try:
                # Process this chunk - data is added directly to consolidated table
                result = load_animal_movements(chr_dyr_client, username, herd_number, chunk_start, chunk_end)

                if result and result.get("processed_successfully"):
                    successful_chunks += 1
                    total_movements += result.get("movement_count", 0)
                    logger.info(f"Chunk {chunk_idx + 1} completed: {result.get('movement_count', 0)} movements")
                else:
                    failed_chunks += 1
                    if result and result.get("skipped_reason"):
                        logger.warning(f"Chunk {chunk_idx + 1} skipped: {result.get('skipped_reason')}")
                    else:
                        logger.warning(f"Chunk {chunk_idx + 1} failed: no result returned")

            except Exception as e:
                logger.error(f"Error processing chunk {chunk_idx + 1} for herd {herd_number}: {e}")
                failed_chunks += 1
                continue

        # Return summary result
        if successful_chunks > 0:
            return {
                "reporting_herd_number": herd_number,
                "processed_successfully": True,
                "movement_count": total_movements,
                "successful_chunks": successful_chunks,
                "failed_chunks": failed_chunks,
                "total_chunks": len(date_ranges),
            }
        else:
            return {
                "reporting_herd_number": herd_number,
                "processed_successfully": False,
                "movement_count": 0,
                "successful_chunks": 0,
                "failed_chunks": failed_chunks,
                "total_chunks": len(date_ranges),
                "error": "All chunks failed",
            }

    except Exception as e:
        logger.error(f"Error processing herd {herd_number}: {e}")
        return {
            "reporting_herd_number": herd_number,
            "processed_successfully": False,
            "movement_count": 0,
            "error": str(e),
        }


def _aggregate_cattle_movements(response: Any, reporting_herd: int) -> Dict:
    """
    Aggregate individual cattle records into herd-level movement summaries.

    This function processes individual animal records and creates summaries
    similar to pig movement data structure:
    - Groups by movement date and counterparty herd
    - Counts animals moved
    - Determines movement direction (in/out)

    Args:
        response: Raw CHR_dyr response with individual animal records
        reporting_herd: The herd number doing the reporting

    Returns:
        Dict with aggregated movement summaries
    """
    from collections import defaultdict

    # Track aggregation start time for performance monitoring
    aggregation_start_time = time.time()

    movement_summaries = {
        "reporting_herd_number": reporting_herd,
        "movements": [],
        "summary_stats": {
            "total_animals_processed": 0,
            "unique_movement_dates": set(),
            "counterparty_herds": set(),
            "date_range": {"start": None, "end": None},
        },
    }

    try:
        resp = response.Response[0] if isinstance(response.Response, list) else response.Response
        animals = getattr(resp, "Enkeltdyrsoplysninger", [])

        # Fix the 'int' object is not iterable error - COMPREHENSIVE CHECK
        if isinstance(animals, int):
            logger.info(
                f"Herd {reporting_herd}: Enkeltdyrsoplysninger is an integer ({animals}) - API returned count only, no detailed records available"
            )
            return {
                "reporting_herd_number": reporting_herd,
                "movements": [],
                "skipped_reason": "api_returned_count_only",
                "summary_stats": {
                    "total_animals_processed": 0,
                    "unique_movement_dates": 0,
                    "counterparty_herds": 0,
                    "animal_count_from_api": animals,
                },
                "processed_successfully": True,  # This is a valid response, not an error
            }

        # COMPREHENSIVE SAFETY CHECK: Handle all non-iterable types
        if animals is None:
            logger.info(f"Herd {reporting_herd}: Enkeltdyrsoplysninger is None - no animals found")
            animals = []
        elif isinstance(animals, (int, float, bool)):
            logger.warning(
                f"Herd {reporting_herd}: Enkeltdyrsoplysninger is a primitive type ({type(animals).__name__}: {animals}) - converting to empty list"
            )
            animals = []
        elif isinstance(animals, (str, bytes)):
            logger.warning(
                f"Herd {reporting_herd}: Enkeltdyrsoplysninger is a string/bytes (type: {type(animals)}) - converting to empty list"
            )
            animals = []
        elif not hasattr(animals, "__iter__"):
            logger.warning(
                f"Herd {reporting_herd}: Enkeltdyrsoplysninger is not iterable (type: {type(animals)}) - converting to empty list"
            )
            animals = []

        # ADDITIONAL SAFETY CHECK: Ensure animals is a proper list/sequence
        try:
            animals_count = len(animals) if animals else 0
        except TypeError:
            logger.error(
                f"Herd {reporting_herd}: Cannot get length of Enkeltdyrsoplysninger (type: {type(animals)}) - converting to empty list"
            )
            animals = []
            animals_count = 0

        if not animals or animals_count == 0:
            logger.info(f"No animals found for herd {reporting_herd}")
            return movement_summaries

        logger.info(f"Herd {reporting_herd}: Processing {animals_count} individual animals...")

        # Skip herds with extremely large datasets that might cause performance issues
        if animals_count > 100000:  # 100k animals threshold
            logger.warning(
                f"Herd {reporting_herd}: Dataset too large ({animals_count} animals) - skipping to prevent performance issues"
            )
            return {
                "reporting_herd_number": reporting_herd,
                "movements": [],
                "skipped_reason": "dataset_too_large",
                "summary_stats": {
                    "total_animals_processed": 0,
                    "unique_movement_dates": 0,
                    "counterparty_herds": 0,
                    "dataset_size": animals_count,
                },
            }

        # Detect high-volume herds and suggest chunking instead of processing
        # UPDATED: Use more reasonable thresholds and larger chunks
        if animals_count > 50000:  # 50k animals threshold for chunking suggestion (increased from 20k)
            # Calculate suggested chunk size based on volume
            if animals_count > 100000:
                suggested_days = 30  # Very high volume: monthly chunks (increased from 7)
            elif animals_count > 75000:
                suggested_days = 60  # High volume: bi-monthly chunks (increased from 14)
            else:
                suggested_days = 90  # Moderate high volume: quarterly chunks (increased from 30)

            logger.warning(
                f"Herd {reporting_herd}: Large dataset ({animals_count} animals) detected - suggesting {suggested_days}-day chunks for future processing"
            )

            # Auto-add to high-volume list if not already there
            if not is_high_volume_herd(reporting_herd):
                add_high_volume_herd(reporting_herd, suggested_days, volume_estimate=animals_count)
                logger.info(f"Auto-added herd {reporting_herd} to high-volume list with {suggested_days}-day chunks")

            # For very large datasets, still skip this current processing to avoid performance issues
            if animals_count > 100000:  # Increased threshold from 50k to 100k
                return {
                    "reporting_herd_number": reporting_herd,
                    "movements": [],
                    "skipped_reason": "auto_chunking_required",
                    "suggested_chunk_days": suggested_days,
                    "summary_stats": {
                        "total_animals_processed": 0,
                        "unique_movement_dates": 0,
                        "counterparty_herds": 0,
                        "dataset_size": animals_count,
                    },
                }

        # Log dataset size for monitoring
        logger.info(f"Herd {reporting_herd}: Processing {animals_count} animals for aggregation")

        # Group movements by date and counterparty
        # Key: (movement_date, counterparty_herd, movement_type)
        movement_groups = defaultdict(
            lambda: {
                "animal_count": 0,
                "movement_date": None,
                "counterparty_herd": None,
                "movement_type": None,  # "incoming" or "outgoing"
                "movement_reasons": [],  # Store AarsagAfgaaet values
            }
        )

        # FINAL SAFETY CHECK before iteration
        try:
            # Convert to list if it's not already to ensure we can iterate safely
            if not isinstance(animals, (list, tuple)):
                # Double-check that animals is still iterable (catch any edge cases)
                if hasattr(animals, "__iter__") and not isinstance(animals, (str, bytes, int, float, bool)):
                    try:
                        animals = list(animals)
                    except (TypeError, ValueError) as e:
                        logger.error(f"Herd {reporting_herd}: Failed to convert animals to list: {e}")
                        animals = []
                else:
                    logger.warning(
                        f"Herd {reporting_herd}: Animals is not iterable at final check (type: {type(animals)}) - using empty list"
                    )
                    animals = []

            # ULTIMATE SAFETY CHECK: Verify we can actually iterate
            if not isinstance(animals, (list, tuple)):
                logger.error(
                    f"Herd {reporting_herd}: Animals is still not a list/tuple after all checks (type: {type(animals)}) - using empty list"
                )
                animals = []

            for i, animal in enumerate(animals):
                try:
                    # Progress logging for large datasets
                    if i > 0 and i % 5000 == 0:  # More frequent progress updates
                        elapsed_time = time.time() - aggregation_start_time
                        logger.info(
                            f"Herd {reporting_herd}: Processed {i}/{len(animals)} animals ({i / len(animals) * 100:.1f}%) in {elapsed_time:.1f}s"
                        )

                    # Extract key movement information (using ACTUAL field names from CHR_dyr)
                    ckr_nr = getattr(animal, "CkrNr", None)
                    entry_date = getattr(animal, "DatoIndgaaet", None)
                    exit_date = getattr(animal, "DatoAfgaaet", None)
                    source_herd = getattr(animal, "BesaetningsNummerFra", None)
                    dest_herd = getattr(animal, "BesaetningsNummerTil", None)
                    exit_reason = getattr(animal, "AarsagAfgaaet", None)  # Movement reason (e.g., "Slagtning")

                    movement_summaries["summary_stats"]["total_animals_processed"] += 1

                    # Process incoming movements (animal entered this herd)
                    if entry_date and source_herd and source_herd != reporting_herd:
                        movement_date = _parse_date(entry_date)
                        if movement_date:
                            key = (movement_date, source_herd, "incoming")
                            movement_groups[key]["animal_count"] += 1
                            movement_groups[key]["movement_date"] = movement_date
                            movement_groups[key]["counterparty_herd"] = source_herd
                            movement_groups[key]["movement_type"] = "incoming"

                            movement_summaries["summary_stats"]["unique_movement_dates"].add(movement_date)
                            movement_summaries["summary_stats"]["counterparty_herds"].add(source_herd)

                    # Process outgoing movements (animal left this herd)
                    if exit_date and dest_herd and dest_herd != reporting_herd:
                        movement_date = _parse_date(exit_date)
                        if movement_date:
                            key = (movement_date, dest_herd, "outgoing")
                            movement_groups[key]["animal_count"] += 1
                            movement_groups[key]["movement_date"] = movement_date
                            movement_groups[key]["counterparty_herd"] = dest_herd
                            movement_groups[key]["movement_type"] = "outgoing"
                            movement_groups[key]["movement_reasons"].append(exit_reason)  # Save AarsagAfgaaet

                            movement_summaries["summary_stats"]["unique_movement_dates"].add(movement_date)
                            movement_summaries["summary_stats"]["counterparty_herds"].add(dest_herd)

                except Exception as e:
                    logger.debug(f"Error processing individual animal {getattr(animal, 'CkrNr', 'unknown')}: {e}")
                    continue

        except TypeError as e:
            logger.error(
                f"Herd {reporting_herd}: Failed to iterate over animals - {e}. Type: {type(animals)}, Value: {animals}"
            )
            return {
                "reporting_herd_number": reporting_herd,
                "movements": [],
                "skipped_reason": "iteration_failed",
                "summary_stats": {
                    "total_animals_processed": 0,
                    "unique_movement_dates": 0,
                    "counterparty_herds": 0,
                    "error_details": str(e),
                },
            }

        # Convert grouped movements to list format (similar to pig movements)
        for (movement_date, counterparty_herd, movement_type), group_data in movement_groups.items():
            # Get unique movement reasons for this group
            reasons = [r for r in group_data["movement_reasons"] if r]
            unique_reasons = list(set(reasons)) if reasons else []

            movement_summary = {
                "movement_id": f"{reporting_herd}_{counterparty_herd}_{movement_date}_{movement_type}",
                "movement_date": movement_date,
                "reporting_herd_number": reporting_herd,
                "counterparty_herd_number": counterparty_herd,
                "movement_type": movement_type,  # "incoming" or "outgoing"
                "animals_total": group_data["animal_count"],
                "contact_type": "Fra" if movement_type == "outgoing" else "Til",
                # Movement reasons (AarsagAfgaaet)
                "movement_reasons": unique_reasons,
                "primary_reason": unique_reasons[0] if unique_reasons else None,
                # Additional metadata
                "source_data": "chr_dyr_aggregated",
            }
            movement_summaries["movements"].append(movement_summary)

        # Update summary statistics
        dates = list(movement_summaries["summary_stats"]["unique_movement_dates"])
        if dates:
            movement_summaries["summary_stats"]["date_range"]["start"] = min(dates)
            movement_summaries["summary_stats"]["date_range"]["end"] = max(dates)

        # CRITICAL FIX: Keep both sets and counts for chunked processing compatibility
        # The chunked processing needs sets for .update() operations, but final output needs counts
        movement_summaries["summary_stats"]["unique_movement_dates_set"] = movement_summaries["summary_stats"][
            "unique_movement_dates"
        ]
        movement_summaries["summary_stats"]["counterparty_herds_set"] = movement_summaries["summary_stats"][
            "counterparty_herds"
        ]

        # Convert to counts for final output
        movement_summaries["summary_stats"]["unique_movement_dates"] = len(dates)
        movement_summaries["summary_stats"]["counterparty_herds"] = len(
            movement_summaries["summary_stats"]["counterparty_herds_set"]
        )

        logger.info(
            f"Herd {reporting_herd}: Processed {movement_summaries['summary_stats']['total_animals_processed']} "
            f"individual animals into {len(movement_summaries['movements'])} movement summaries"
        )

        return movement_summaries

    except Exception as e:
        logger.error(f"Error aggregating cattle movements for herd {reporting_herd}: {e}")
        return movement_summaries


def _parse_date(date_str):
    """Parse date string from CHR_dyr response."""
    if not date_str:
        return None
    try:
        from datetime import datetime

        # Handle different date formats that might come from CHR_dyr
        if isinstance(date_str, str):
            # Try common formats
            for fmt in ["%Y-%m-%d", "%d-%m-%Y", "%Y%m%d"]:
                try:
                    return datetime.strptime(date_str, fmt).date()
                except ValueError:
                    continue
        elif hasattr(date_str, "date"):
            return date_str.date()
        elif hasattr(date_str, "year"):
            return date_str
    except Exception as e:
        logger.debug(f"Could not parse date {date_str}: {e}")
    return None


# Remove the batch processing system and use proper unified pipeline patterns
# Import the unified GCS access system
try:
    from unified_pipeline.util.gcs_access import GCSDataAccess

    GCS_ACCESS_AVAILABLE = True
except ImportError:
    GCS_ACCESS_AVAILABLE = False

# Global DuckDB connection for consolidated processing
_duckdb_conn = None
_gcs_access = None


def _initialize_consolidated_processing():
    """Initialize DuckDB connection and GCS access for consolidated processing."""
    global _duckdb_conn, _gcs_access

    if not GCS_ACCESS_AVAILABLE:
        logger.error("GCSDataAccess not available - cannot use consolidated processing")
        return False

    try:
        _gcs_access = GCSDataAccess()
        _duckdb_conn = _gcs_access.duckdb_conn

        # Create consolidated table for all movement data
        _duckdb_conn.execute("""
            CREATE TABLE IF NOT EXISTS consolidated_movements (
                reporting_herd_number INTEGER,
                movement_date DATE,
                counterparty_herd INTEGER,
                movement_type VARCHAR,
                animal_count INTEGER,
                animals TEXT,  -- JSON array of animal IDs
                movement_reasons TEXT,  -- JSON array of reasons
                data_source VARCHAR DEFAULT 'chr_dyr'
            )
        """)

        logger.info("✅ Initialized consolidated DuckDB processing")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize consolidated processing: {e}")
        return False


def _add_to_consolidated_table(movement_data):
    """Add movement data directly to consolidated DuckDB table."""
    global _duckdb_conn

    if not _duckdb_conn or not movement_data:
        return

    try:
        # Extract movements from the data
        movements = movement_data.get("movements", [])
        reporting_herd = movement_data.get("reporting_herd_number")

        if not movements:
            return

        # Insert each movement directly into DuckDB table
        for movement in movements:
            _duckdb_conn.execute(
                """
                INSERT INTO consolidated_movements 
                (reporting_herd_number, movement_date, counterparty_herd, movement_type, 
                 animal_count, animals, movement_reasons)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                [
                    reporting_herd,
                    movement.get("movement_date"),
                    movement.get("counterparty_herd"),
                    movement.get("movement_type"),
                    movement.get("animal_count", 0),
                    json.dumps(movement.get("animals", [])),
                    json.dumps(movement.get("movement_reasons", [])),
                ],
            )

        logger.debug(f"Added {len(movements)} movements from herd {reporting_herd} to consolidated table")

    except Exception as e:
        logger.error(f"Failed to add data to consolidated table: {e}")


def _finalize_consolidated_processing():
    """Save consolidated data to GCS and cleanup resources."""
    global _duckdb_conn, _gcs_access

    if not _duckdb_conn or not _gcs_access:
        return False

    try:
        # Get record count
        count_result = _duckdb_conn.execute("SELECT COUNT(*) FROM consolidated_movements").fetchone()
        record_count = count_result[0] if count_result else 0

        if record_count == 0:
            logger.warning("No movement data to save - consolidated table is empty")
            return True

        logger.info(f"Saving {record_count:,} consolidated movement records to GCS")

        # Save consolidated data directly to GCS using the unified pipeline pattern
        from .export import EXPORT_TIMESTAMP

        bucket_name = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")
        gcs_path = f"gs://{bucket_name}/bronze/chr/{EXPORT_TIMESTAMP}/chr_dyr_movement_summaries.parquet"

        # Use the unified GCS access pattern - export directly to parquet
        _gcs_access.export_table_to_gcs_direct("consolidated_movements", gcs_path)

        logger.info(f"✅ Saved consolidated movement data to {gcs_path}")

        # Clean up table
        _duckdb_conn.execute("DROP TABLE IF EXISTS consolidated_movements")

        return True

    except Exception as e:
        logger.error(f"Failed to finalize consolidated processing: {e}")
        return False


# Update the main processing function to use consolidated approach
def load_animal_movements(
    chr_dyr_client: Client,
    username: str,
    herd_number: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    max_retries: int = 3,
) -> Optional[Any]:
    """
    Fetches animal movement data using the unified pipeline pattern.
    Data is added directly to consolidated DuckDB table instead of creating files.
    """
    # Set default date range if not provided
    if start_date is None and end_date is None:
        end_date = date.today()
        start_date = end_date - timedelta(days=365 * 5)  # 5 years of data
    elif end_date is None:
        end_date = date.today()

    # Skip herds that have been identified as problematic
    if is_problematic_herd(herd_number):
        logger.warning(f"Skipping herd {herd_number} (marked as problematic)")
        return {
            "reporting_herd_number": herd_number,
            "movements": [],
            "skipped_reason": "problematic_herd",
        }

    logger.info(f"Fetching animal movements for herd {herd_number} from {start_date} to {end_date}")

    # Retry logic for problematic herds
    for attempt in range(max_retries + 1):
        try:
            if attempt > 0:
                wait_time = 2**attempt
                logger.info(
                    f"Retrying herd {herd_number} (attempt {attempt + 1}/{max_retries + 1}) after {wait_time}s delay"
                )
                time.sleep(wait_time)

            # Create request structure
            logger.debug(f"Herd {herd_number}: Creating SOAP request structure...")
            GLRCHRWSInfoInboundFactory = chr_dyr_client.get_type("ns0:GLRCHRWSInfoInboundType")
            common_header = GLRCHRWSInfoInboundFactory(**_create_base_request(username))

            CHR_dyrChrBesListeRequestTypeFactory = chr_dyr_client.get_type("ns0:CHR_dyrChrBesListeRequestType")

            request_params_dict = {"BesaetningsNummer": herd_number}
            if start_date:
                request_params_dict["PeriodeFra"] = start_date
            if end_date:
                request_params_dict["PeriodeTil"] = end_date

            request_params = CHR_dyrChrBesListeRequestTypeFactory(**request_params_dict)
            payload_content = {"GLRCHRWSInfoInbound": common_header, "Request": request_params}

            # Call the operation
            logger.debug(f"Herd {herd_number}: Starting SOAP request...")
            request_start_time = time.time()

            try:
                response = chr_dyr_client.service.besListAktOms(CHR_dyrChrBesListeRequest=payload_content)
                request_duration = time.time() - request_start_time
                logger.debug(f"Herd {herd_number}: SOAP request completed in {request_duration:.1f}s")
            except Exception as soap_error:
                request_duration = time.time() - request_start_time
                logger.error(f"Herd {herd_number}: SOAP request failed after {request_duration:.1f}s: {soap_error}")
                raise

            if response is None:
                logger.warning(f"No response received for herd {herd_number} (attempt {attempt + 1})")
                if attempt < max_retries:
                    continue
                return None

            # Check for extremely slow requests
            if request_duration > 1800:  # 30 minutes
                logger.error(
                    f"Extremely slow request for herd {herd_number}: {request_duration:.1f}s - marking as problematic"
                )
                add_problematic_herd(herd_number)
                return {
                    "reporting_herd_number": herd_number,
                    "movements": [],
                    "skipped_reason": "extremely_slow_processing",
                }

            # Process and aggregate the response
            aggregation_start_time = time.time()
            logger.info(f"Herd {herd_number}: Starting data aggregation...")

            try:
                movement_summaries = _aggregate_cattle_movements(response, herd_number)
                aggregation_duration = time.time() - aggregation_start_time
                logger.info(f"Herd {herd_number}: Data aggregation completed in {aggregation_duration:.1f}s")
            except Exception as agg_error:
                aggregation_duration = time.time() - aggregation_start_time
                logger.error(
                    f"Herd {herd_number}: Data aggregation failed after {aggregation_duration:.1f}s: {agg_error}"
                )
                raise

            # Add to consolidated table instead of saving individual files
            _add_to_consolidated_table(movement_summaries)

            # Return summary for tracking
            if movement_summaries and movement_summaries.get("movements"):
                logger.info(f"Herd {herd_number}: Processed {len(movement_summaries['movements'])} movement summaries")
                return {
                    "reporting_herd_number": herd_number,
                    "movement_count": len(movement_summaries["movements"]),
                    "processed_successfully": True,
                }
            else:
                return {
                    "reporting_herd_number": herd_number,
                    "movement_count": 0,
                    "processed_successfully": True,
                    "no_movements_found": True,
                }

        except Exception as e:
            logger.error(f"Error fetching animal movements for herd {herd_number} (attempt {attempt + 1}): {e}")
            if attempt < max_retries:
                continue
            else:
                logger.error(f"Max retries exceeded for herd {herd_number} due to error: {e}")
                return None

    return None


def detect_herd_volume(chr_dyr_client: Client, username: str, herd_number: int, sample_days: int = 7) -> Dict[str, Any]:
    """
    Proactively detect if a herd is high-volume by testing a small sample period.

    This helps estimate volume before attempting large requests.

    Args:
        chr_dyr_client: The CHR_dyr SOAP client
        username: Username for authentication
        herd_number: The herd number to test
        sample_days: Number of recent days to sample (default 7)

    Returns:
        Dict with volume estimation and recommended chunk size
    """
    try:
        # Test a small recent sample (last week)
        end_date = date.today()
        start_date = end_date - timedelta(days=sample_days - 1)

        logger.info(f"Volume detection for herd {herd_number}: testing {sample_days} days ({start_date} to {end_date})")

        # Track timing for this test request
        start_time = time.time()

        # Create request structure
        GLRCHRWSInfoInboundFactory = chr_dyr_client.get_type("ns0:GLRCHRWSInfoInboundType")
        common_header = GLRCHRWSInfoInboundFactory(**_create_base_request(username))

        CHR_dyrChrBesListeRequestTypeFactory = chr_dyr_client.get_type("ns0:CHR_dyrChrBesListeRequestType")
        request_params = CHR_dyrChrBesListeRequestTypeFactory(
            **{"BesaetningsNummer": herd_number, "PeriodeFra": start_date, "PeriodeTil": end_date}
        )

        payload_content = {"GLRCHRWSInfoInbound": common_header, "Request": request_params}

        # Make the test request
        response = chr_dyr_client.service.besListAktOms(CHR_dyrChrBesListeRequest=payload_content)
        request_duration = time.time() - start_time

        if response and hasattr(response, "Response") and response.Response:
            resp = response.Response[0] if isinstance(response.Response, list) else response.Response
            animals = getattr(resp, "Enkeltdyrsoplysninger", [])
            # Handle case where Enkeltdyrsoplysninger is an integer instead of a list
            if isinstance(animals, int):
                sample_animal_count = animals  # Use the integer value directly
            else:
                sample_animal_count = len(animals) if animals else 0

            # Calculate estimates
            animals_per_day = sample_animal_count / sample_days
            estimated_yearly = animals_per_day * 365
            estimated_5_year = estimated_yearly * 5

            # Determine volume category and recommended chunk size
            # UPDATED: Use larger chunks to reduce request volume and improve performance
            if estimated_5_year > 200000:  # Very high volume (like slaughterhouses)
                volume_category = "very_high"
                recommended_chunk_days = 30  # Monthly chunks instead of weekly
                risk_level = "extreme"
            elif estimated_5_year > 100000:  # High volume
                volume_category = "high"
                recommended_chunk_days = 60  # Bi-monthly chunks instead of bi-weekly
                risk_level = "high"
            elif estimated_5_year > 50000:  # Moderate high volume
                volume_category = "moderate_high"
                recommended_chunk_days = 90  # Quarterly chunks instead of monthly
                risk_level = "moderate"
            else:  # Normal volume
                volume_category = "normal"
                recommended_chunk_days = None  # No chunking needed
                risk_level = "low"

            result = {
                "herd_number": herd_number,
                "volume_category": volume_category,
                "risk_level": risk_level,
                "sample_stats": {
                    "sample_days": sample_days,
                    "sample_animals": sample_animal_count,
                    "animals_per_day": round(animals_per_day, 1),
                    "request_duration": round(request_duration, 1),
                },
                "estimates": {"yearly_animals": round(estimated_yearly), "five_year_animals": round(estimated_5_year)},
                "recommendation": {
                    "chunk_days": recommended_chunk_days,
                    "auto_configure": risk_level in ["high", "extreme"],
                },
            }

            logger.info(
                f"Herd {herd_number} volume detection: {volume_category} ({estimated_5_year:,.0f} estimated 5-year animals)"
            )

            # Auto-configure high-risk herds
            if result["recommendation"]["auto_configure"]:
                logger.warning(
                    f"Auto-configuring herd {herd_number} as high-volume ({recommended_chunk_days}-day chunks)"
                )
                add_high_volume_herd(herd_number, recommended_chunk_days, round(estimated_5_year))

            return result

        else:
            return {
                "herd_number": herd_number,
                "volume_category": "unknown",
                "risk_level": "unknown",
                "error": "no_response_data",
            }

    except Exception as e:
        logger.warning(f"Volume detection failed for herd {herd_number}: {e}")
        return {"herd_number": herd_number, "volume_category": "unknown", "risk_level": "unknown", "error": str(e)}
