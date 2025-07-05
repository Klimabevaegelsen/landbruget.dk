"""Module for loading CHR_dyr data (Animal Movements) - Bronze Layer."""

import json
import logging
import os
import time
import uuid
from datetime import date, datetime, timedelta
from typing import Any, Dict, Optional

import certifi
import requests
from dotenv import load_dotenv
from requests import Session
from zeep import Client
from zeep.exceptions import Fault
from zeep.transports import Transport
from zeep.wsse.username import UsernameToken

# Import the exporter function

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
    """Create a Zeep SOAP client with WSSE authentication and timeout configuration."""
    session = Session()
    session.verify = certifi.where()

    # Configure timeouts to prevent hanging on slow/unresponsive herds
    # Connection timeout: 30 seconds to establish connection
    # Read timeout: Reduced to 180 seconds (3 minutes) for GitHub Actions compatibility
    # This prevents individual herds from blocking the entire pipeline
    adapter = requests.adapters.HTTPAdapter()
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    # Set aggressive timeouts for GitHub Actions environment
    github_actions = os.getenv("GITHUB_ACTIONS") == "true"
    read_timeout = 180 if github_actions else 300  # 3 minutes in GH Actions, 5 minutes locally
    session.timeout = (30, read_timeout)  # (connect_timeout, read_timeout)

    transport = Transport(session=session)
    try:
        client = Client(wsdl_url, transport=transport, wsse=UsernameToken(username, password))
        logger.info(f"Successfully created SOAP client for {wsdl_url} with 30s connect / {read_timeout}s read timeouts")
        return client
    except Exception as e:
        logger.error(f"Failed to create SOAP client for {wsdl_url}: {e}")
        raise


# Global set to track problematic herds that consistently timeout
PROBLEMATIC_HERDS = set()
PROBLEMATIC_HERDS_LOADED = False


def _load_problematic_herds() -> None:
    """Load problematic herds from persistent storage (GCS)."""
    global PROBLEMATIC_HERDS, PROBLEMATIC_HERDS_LOADED

    if PROBLEMATIC_HERDS_LOADED:
        return

    try:
        # Try to load from GCS using the unified pipeline's GCS access
        from unified_pipeline.util.gcs_access import GCSDataAccess

        gcs = GCSDataAccess()
        bucket_name = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")
        problematic_herds_path = "bronze/chr/problematic_herds.json"

        try:
            data = gcs.download_json(bucket_name, problematic_herds_path)
            if data and "problematic_herds" in data:
                PROBLEMATIC_HERDS.update(data["problematic_herds"])
                logger.info(f"Loaded {len(PROBLEMATIC_HERDS)} problematic herds from GCS")
            else:
                logger.info("No problematic herds found in GCS - starting with empty set")
        except Exception as e:
            logger.debug(f"Could not load problematic herds from GCS: {e}")
            # This is expected on first run or if file doesn't exist

    except ImportError:
        logger.debug("GCS access not available - problematic herds will not persist across runs")

    PROBLEMATIC_HERDS_LOADED = True


def _save_problematic_herds() -> None:
    """Save problematic herds to persistent storage (GCS)."""
    global PROBLEMATIC_HERDS

    if not PROBLEMATIC_HERDS:
        return

    try:
        from unified_pipeline.util.gcs_access import GCSDataAccess

        gcs = GCSDataAccess()
        bucket_name = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")
        problematic_herds_path = "bronze/chr/problematic_herds.json"

        data = {
            "problematic_herds": list(PROBLEMATIC_HERDS),
            "last_updated": datetime.now().isoformat(),
            "total_count": len(PROBLEMATIC_HERDS),
        }

        gcs.upload_json(bucket_name, problematic_herds_path, data)
        logger.info(f"Saved {len(PROBLEMATIC_HERDS)} problematic herds to GCS")

    except Exception as e:
        logger.warning(f"Could not save problematic herds to GCS: {e}")


def add_problematic_herd(herd_number: int) -> None:
    """Add a herd to the problematic herds list."""
    _load_problematic_herds()  # Ensure we have the latest data

    PROBLEMATIC_HERDS.add(herd_number)
    logger.warning(f"Added herd {herd_number} to problematic herds list (will be skipped in future)")

    # Save immediately to persist the change
    _save_problematic_herds()


def is_problematic_herd(herd_number: int) -> bool:
    """Check if a herd is in the problematic herds list."""
    _load_problematic_herds()  # Ensure we have the latest data
    return herd_number in PROBLEMATIC_HERDS


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

            # Call the operation with timeout monitoring
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
                individual_record_count = len(animals) if animals else 0
                summary_record_count = len(movement_summaries.get("movements", []))

                if individual_record_count > 0:
                    reduction_ratio = (individual_record_count - summary_record_count) / individual_record_count * 100
                    logger.info(
                        f"Herd {herd_number}: Reduced {individual_record_count} individual animal records to {summary_record_count} movement summaries ({reduction_ratio:.1f}% reduction)"
                    )

            # Save to streaming buffer to prevent memory buildup while maintaining consolidated output
            if movement_summaries and movement_summaries.get("movements"):
                # Use streaming save to prevent memory accumulation
                _save_to_streaming_buffer(
                    data_type="chr_dyr_movement_summaries",
                    identifier=f"{herd_number}{date_suffix}",
                    data=movement_summaries,
                )
                logger.info(
                    f"Herd {herd_number}: Streamed {len(movement_summaries['movements'])} movement summaries to buffer"
                )
            else:
                # Still save a minimal record indicating we processed this herd
                minimal_record = {"reporting_herd_number": herd_number, "movements": [], "no_movements_found": True}
                _save_to_streaming_buffer(
                    data_type="chr_dyr_movement_summaries",
                    identifier=f"{herd_number}{date_suffix}",
                    data=minimal_record,
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
                    logger.debug(
                        f"Sample animal from herd {herd_number}: " + f"CKR={getattr(animals[0], 'CkrNr', 'N/A')}"
                    )

            # Return aggregated summaries instead of massive raw response
            return movement_summaries

        except requests.exceptions.Timeout as e:
            logger.error(f"Timeout error for herd {herd_number} (attempt {attempt + 1}): {e}")
            if attempt < max_retries:
                continue
            else:
                logger.error(f"Max retries exceeded for herd {herd_number} due to timeout")
                add_problematic_herd(herd_number)  # Mark as problematic
                return None

        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error for herd {herd_number} (attempt {attempt + 1}): {e}")
            if attempt < max_retries:
                continue
            else:
                logger.error(f"Max retries exceeded for herd {herd_number} due to connection error")
                return None

        except Fault as soap_fault:
            logger.error(f"SOAP fault for herd {herd_number} (attempt {attempt + 1}): {soap_fault}")
            if attempt < max_retries and "timeout" in str(soap_fault).lower():
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
    Fetches cattle movement data and aggregates it into herd-level summaries.

    This approach processes individual animal records from CHR_dyr but immediately
    aggregates them into movement summaries similar to pig movements, avoiding
    the storage of massive individual animal datasets.

    Args:
        chr_dyr_client: The CHR_dyr SOAP client
        username: Username for authentication
        herd_number: The herd number to fetch data for
        start_date: Optional start date for filtering
        end_date: Optional end date for filtering

    Returns:
        Dict with aggregated movement summaries or None if failed
    """

    # Set reasonable date range (last 5 years for cattle movements)
    if start_date is None and end_date is None:
        end_date = date.today()
        start_date = end_date - timedelta(days=365 * 5)  # 5 years of data
    elif end_date is None:
        end_date = date.today()

    logger.info(f"Fetching cattle movement summaries for herd {herd_number} from {start_date} to {end_date}")

    try:
        # Get aggregated movement summaries directly from load_animal_movements
        # Note: load_animal_movements now returns aggregated summaries, not raw SOAP responses
        movement_summaries = load_animal_movements(chr_dyr_client, username, herd_number, start_date, end_date)

        if not movement_summaries:
            logger.warning(f"No movement summaries returned for herd {herd_number}")
            return None

        # The movement_summaries are already processed and saved by load_animal_movements
        # Return only lightweight summary for memory efficiency (full data is already in storage)
        movement_count = len(movement_summaries.get("movements", []))
        logger.info(f"Herd {herd_number}: Returned {movement_count} movement summaries")

        # Return lightweight summary instead of full data to reduce memory usage
        lightweight_summary = {
            "reporting_herd_number": movement_summaries.get("reporting_herd_number"),
            "movement_count": movement_count,
            "processed_successfully": True,
            "summary_stats": {
                "total_animals_processed": movement_summaries.get("summary_stats", {}).get(
                    "total_animals_processed", 0
                ),
                "unique_movement_dates": movement_summaries.get("summary_stats", {}).get("unique_movement_dates", 0),
                "counterparty_herds": movement_summaries.get("summary_stats", {}).get("counterparty_herds", 0),
            },
        }

        return lightweight_summary

    except Exception as e:
        logger.error(f"Error processing cattle movement summaries for herd {herd_number}: {e}")
        return None


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

    # Add timeout for aggregation process
    aggregation_start_time = time.time()
    MAX_AGGREGATION_TIME = 600  # 10 minutes max for aggregation

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

        if not animals:
            logger.info(f"No animals found for herd {reporting_herd}")
            return movement_summaries

        logger.info(f"Herd {reporting_herd}: Processing {len(animals)} individual animals...")

        # Skip herds with extremely large datasets that might cause timeouts
        if len(animals) > 100000:  # 100k animals threshold
            logger.warning(
                f"Herd {reporting_herd}: Dataset too large ({len(animals)} animals) - skipping to prevent timeout"
            )
            return {
                "reporting_herd_number": reporting_herd,
                "movements": [],
                "skipped_reason": "dataset_too_large",
                "summary_stats": {
                    "total_animals_processed": 0,
                    "unique_movement_dates": 0,
                    "counterparty_herds": 0,
                    "dataset_size": len(animals),
                },
            }

        # Log dataset size for monitoring
        logger.info(f"Herd {reporting_herd}: Processing {len(animals)} animals for aggregation")

        # Group movements by date and counterparty
        # Key: (movement_date, counterparty_herd, movement_type)
        movement_groups = defaultdict(
            lambda: {
                "animal_count": 0,
                "movement_date": None,
                "counterparty_herd": None,
                "movement_type": None,  # "incoming" or "outgoing"
                "animals": [],  # Store animal IDs for debugging
                "movement_reasons": [],  # Store AarsagAfgaaet values
            }
        )

        for i, animal in enumerate(animals):
            try:
                # Progress logging for large datasets and timeout check
                if i > 0 and i % 5000 == 0:  # More frequent progress updates
                    elapsed_time = time.time() - aggregation_start_time
                    logger.info(
                        f"Herd {reporting_herd}: Processed {i}/{len(animals)} animals ({i / len(animals) * 100:.1f}%) in {elapsed_time:.1f}s"
                    )

                    # Check if aggregation is taking too long
                    if elapsed_time > MAX_AGGREGATION_TIME:
                        logger.error(
                            f"Herd {reporting_herd}: Aggregation timeout after {elapsed_time:.1f}s - stopping at animal {i}/{len(animals)}"
                        )
                        # Mark this herd as problematic before breaking
                        add_problematic_herd(reporting_herd)
                        break

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
                        movement_groups[key]["animals"].append(ckr_nr)

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
                        movement_groups[key]["animals"].append(ckr_nr)
                        movement_groups[key]["movement_reasons"].append(exit_reason)  # Save AarsagAfgaaet

                        movement_summaries["summary_stats"]["unique_movement_dates"].add(movement_date)
                        movement_summaries["summary_stats"]["counterparty_herds"].add(dest_herd)

            except Exception as e:
                logger.debug(f"Error processing individual animal {getattr(animal, 'CkrNr', 'unknown')}: {e}")
                continue

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
                "animal_ids_sample": group_data["animals"][:5],  # Keep sample for debugging
            }
            movement_summaries["movements"].append(movement_summary)

        # Update summary statistics
        dates = list(movement_summaries["summary_stats"]["unique_movement_dates"])
        if dates:
            movement_summaries["summary_stats"]["date_range"]["start"] = min(dates)
            movement_summaries["summary_stats"]["date_range"]["end"] = max(dates)

        movement_summaries["summary_stats"]["unique_movement_dates"] = len(dates)
        movement_summaries["summary_stats"]["counterparty_herds"] = len(
            movement_summaries["summary_stats"]["counterparty_herds"]
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


# Global streaming file handles for consolidated output
_streaming_files = {}


def _append_to_streaming_json(data_type: str, data: Any) -> bool:
    """
    Append data to streaming JSON using simple temporary file approach.
    This prevents memory buildup while maintaining consolidated output.
    """
    try:
        import tempfile

        from .export import EXPORT_TIMESTAMP

        # Create unique stream key
        stream_key = f"{data_type}_{EXPORT_TIMESTAMP}"

        # Initialize temp file for this stream if needed
        if stream_key not in _streaming_files:
            # Use /tmp for temp files to avoid filling up the working directory
            temp_dir = "/tmp" if os.path.exists("/tmp") else None
            temp_file = tempfile.NamedTemporaryFile(
                mode="w",
                suffix=".jsonl",
                delete=False,
                encoding="utf-8",
                dir=temp_dir,
                prefix=f"chr_streaming_{data_type}_",
            )
            _streaming_files[stream_key] = {"temp_file": temp_file, "temp_path": temp_file.name, "count": 0}
            logger.info(f"Initialized streaming temp file for {data_type}: {temp_file.name}")

        # Append data as JSONL (one JSON object per line)
        temp_file = _streaming_files[stream_key]["temp_file"]
        json.dump(data, temp_file, default=str)
        temp_file.write("\n")
        temp_file.flush()  # Ensure data is written

        _streaming_files[stream_key]["count"] += 1

        # Log progress every 1000 records
        count = _streaming_files[stream_key]["count"]
        if count % 1000 == 0:
            logger.info(f"Streamed {count} records for {data_type}")

        # CRITICAL: Periodic cleanup to prevent excessive temp file accumulation
        # Every 5000 records, force a flush and consider finalizing if memory is constrained
        if count % 5000 == 0:
            temp_file.flush()
            os.fsync(temp_file.fileno())  # Force OS to write to disk

            # Check if we're in a memory-constrained environment
            if os.getenv("GITHUB_ACTIONS") == "true" or os.getenv("MEMORY_CONSTRAINED") == "true":
                # Force garbage collection every 5000 records
                import gc

                gc.collect()

                # Log memory usage if possible
                try:
                    import psutil

                    process = psutil.Process(os.getpid())
                    memory_mb = process.memory_info().rss / 1024 / 1024
                    logger.info(f"Memory usage at {count} records for {data_type}: {memory_mb:.1f} MB")
                except Exception:
                    pass

        return True

    except Exception as e:
        logger.error(f"Error appending to streaming {data_type}: {e}")
        return False


def _finalize_streaming_files() -> bool:
    """Convert streaming temp files to final consolidated JSON files."""
    try:
        from .export import save_raw_data

        for stream_key, stream_info in _streaming_files.items():
            data_type = stream_key.split("_")[0]  # Extract data_type from stream_key
            temp_file = stream_info["temp_file"]
            temp_path = stream_info["temp_path"]
            count = stream_info["count"]

            # Close the temp file
            if not temp_file.closed:
                temp_file.close()

            # Read all records from temp file and create consolidated JSON
            consolidated_data = []
            try:
                with open(temp_path, "r", encoding="utf-8") as f:
                    for line in f:
                        if line.strip():
                            consolidated_data.append(json.loads(line.strip()))

                # Save consolidated data using existing export infrastructure
                if consolidated_data:
                    save_raw_data(
                        data_type=data_type,
                        identifier="consolidated",
                        raw_response=consolidated_data,
                    )
                    logger.info(f"✅ Finalized {data_type}: {count} records -> consolidated JSON")

                    # CRITICAL: Clear consolidated_data immediately after saving to prevent memory accumulation
                    consolidated_data.clear()
                    del consolidated_data

            except Exception as e:
                logger.error(f"Error consolidating {data_type}: {e}")
            finally:
                # CRITICAL: Always clean up temp file, even if consolidation fails
                try:
                    if os.path.exists(temp_path):
                        os.unlink(temp_path)
                        logger.debug(f"Cleaned up temp file: {temp_path}")
                except Exception as e:
                    logger.warning(f"Failed to cleanup temp file {temp_path}: {e}")

        # Clear the global registry
        _streaming_files.clear()
        logger.info("✅ Finalized all streaming files")

        # Force garbage collection after finalizing all files
        import gc

        gc.collect()

        return True

    except Exception as e:
        logger.error(f"Error finalizing streaming files: {e}")
        return False


def _save_to_streaming_buffer(data_type: str, identifier: str, data: Any) -> bool:
    """
    Save data using streaming approach to prevent memory buildup.
    """
    return _append_to_streaming_json(data_type, data)
