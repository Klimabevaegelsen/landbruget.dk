"""Main animal movement loading logic for CHR pipeline."""

import logging
import time
from datetime import date, timedelta
from typing import Any, Dict, Optional

import requests
from zeep.exceptions import Fault

from .data_processing import aggregate_cattle_movements
from .persistence import add_problematic_herd, is_problematic_herd
from .utils import create_base_request
from .volume_management import get_optimal_date_range

# Set up logging
logger = logging.getLogger("backend.pipelines.chr_pipeline.bronze.animal_movements")


def load_animal_movements(
    chr_dyr_client,
    username: str,
    herd_number: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    max_retries: int = 3,
) -> Optional[Dict[str, Any]]:
    """
    Fetches animal movement data for a given herd using besListAktOms.

    Args:
        chr_dyr_client: The CHR_dyr SOAP client
        username: Username for authentication
        herd_number: The herd number to fetch data for
        start_date: Optional start date for filtering (if None, gets last 5 years)
        end_date: Optional end date for filtering (if None, uses today)
        max_retries: Maximum number of retry attempts for failed requests

    Returns:
        Dict with movement data or None if failed
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
            "summary_stats": {"total_animals_processed": 0, "unique_movement_dates": 0, "counterparty_herds": 0},
        }

    logger.info(f"Fetching animal movements for herd {herd_number} from {start_date} to {end_date}")

    # Retry logic for problematic herds
    for attempt in range(max_retries + 1):
        try:
            if attempt > 0:
                wait_time = 2**attempt  # Exponential backoff
                logger.info(
                    f"Retrying herd {herd_number} (attempt {attempt + 1}/{max_retries + 1}) after {wait_time}s delay"
                )
                time.sleep(wait_time)

            # Create request structure
            logger.debug(f"Herd {herd_number}: Creating SOAP request structure...")
            GLRCHRWSInfoInboundFactory = chr_dyr_client.get_type("ns0:GLRCHRWSInfoInboundType")
            common_header = GLRCHRWSInfoInboundFactory(**create_base_request(username))

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

            # Mark extremely slow herds as problematic
            if request_duration > 1800:  # 30 minutes
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

            # Log performance for monitoring
            if request_duration > 300:  # 5 minutes
                logger.warning(f"Slow request for herd {herd_number}: {request_duration:.1f}s")
            elif request_duration > 60:  # 1 minute
                logger.info(f"Moderate request for herd {herd_number}: {request_duration:.1f}s")

            # Process and aggregate the response
            aggregation_start_time = time.time()
            logger.info(f"Herd {herd_number}: Starting data aggregation...")

            try:
                movement_summaries = aggregate_cattle_movements(response, herd_number)
                aggregation_duration = time.time() - aggregation_start_time
                logger.info(f"Herd {herd_number}: Data aggregation completed in {aggregation_duration:.1f}s")
            except Exception as agg_error:
                aggregation_duration = time.time() - aggregation_start_time
                logger.error(
                    f"Herd {herd_number}: Data aggregation failed after {aggregation_duration:.1f}s: {agg_error}"
                )
                raise

            # Log memory/processing statistics
            if hasattr(response, "Response") and response.Response:
                resp = response.Response[0] if isinstance(response.Response, list) else response.Response
                animals = getattr(resp, "Enkeltdyrsoplysninger", [])
                individual_record_count = animals if isinstance(animals, int) else len(animals) if animals else 0
                summary_record_count = len(movement_summaries.get("movements", []))

                if individual_record_count > 0:
                    reduction_ratio = (individual_record_count - summary_record_count) / individual_record_count * 100
                    logger.info(
                        f"Herd {herd_number}: Reduced {individual_record_count} individual animal records to {summary_record_count} movement summaries ({reduction_ratio:.1f}% reduction)"
                    )

            # Return the aggregated data
            if movement_summaries and movement_summaries.get("movements"):
                logger.info(f"Herd {herd_number}: Processed {len(movement_summaries['movements'])} movement summaries")
                return movement_summaries
            else:
                # Return minimal record indicating we processed this herd
                return {"reporting_herd_number": herd_number, "movements": [], "no_movements_found": True}

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

    return None


def load_cattle_movement_summaries(
    chr_dyr_client,
    username: str,
    herd_number: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> Optional[Dict]:
    """
    Fetches cattle movement data using chunked processing for high-volume herds.

    Args:
        chr_dyr_client: The CHR_dyr SOAP client
        username: Username for authentication
        herd_number: The herd number to fetch data for
        start_date: Optional start date for filtering
        end_date: Optional end date for filtering

    Returns:
        Dict with processing results
    """
    # Set reasonable date range
    if start_date is None and end_date is None:
        end_date = date.today()
        start_date = end_date - timedelta(days=365 * 5)  # 5 years of data
    elif end_date is None:
        end_date = date.today()

    logger.info(f"Fetching cattle movement summaries for herd {herd_number} from {start_date} to {end_date}")

    # Get optimal date ranges (handles chunking for high-volume herds)
    date_ranges = get_optimal_date_range(herd_number, start_date, end_date)

    if len(date_ranges) > 1:
        logger.info(f"Processing herd {herd_number} in {len(date_ranges)} chunks due to high volume")

    # Process all date ranges
    total_movements = 0
    successful_chunks = 0
    failed_chunks = 0

    try:
        for chunk_idx, (chunk_start, chunk_end) in enumerate(date_ranges):
            logger.info(
                f"Processing herd {herd_number} chunk {chunk_idx + 1}/{len(date_ranges)}: {chunk_start} to {chunk_end}"
            )

            try:
                result = load_animal_movements(chr_dyr_client, username, herd_number, chunk_start, chunk_end)

                if result and result.get("movements"):
                    successful_chunks += 1
                    total_movements += len(result.get("movements", []))
                    logger.info(f"Chunk {chunk_idx + 1} completed: {len(result.get('movements', []))} movements")
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
