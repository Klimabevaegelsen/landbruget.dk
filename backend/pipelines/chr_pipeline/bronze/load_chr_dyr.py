"""Module for loading CHR_dyr data (Animal Movements) - Bronze Layer."""

import json
import logging
import os
import uuid
from datetime import date, timedelta
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
    # Read timeout: 300 seconds (5 minutes) to wait for response
    # This is especially important for cattle movement data which can be large
    adapter = requests.adapters.HTTPAdapter()
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    # Set timeouts on the session instead
    session.timeout = (30, 300)  # (connect_timeout, read_timeout)

    transport = Transport(session=session)
    try:
        client = Client(wsdl_url, transport=transport, wsse=UsernameToken(username, password))
        logger.info(f"Successfully created SOAP client for {wsdl_url} with 30s connect / 300s read timeouts")
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

        # Process and aggregate the response instead of saving raw individual records
        movement_summaries = _aggregate_cattle_movements(response, herd_number)

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
                logger.debug(f"Sample animal from herd {herd_number}: " + f"CKR={getattr(animals[0], 'CkrNr', 'N/A')}")

        # Return aggregated summaries instead of massive raw response
        return movement_summaries

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

        for animal in animals:
            try:
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
