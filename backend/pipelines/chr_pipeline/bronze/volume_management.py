"""Volume management for CHR pipeline - handles high-volume herds and chunking."""

import logging
import os
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from zeep import Client

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
logger = logging.getLogger("backend.pipelines.chr_pipeline.bronze.volume_management")

# Global dict to track herds that need special handling (smaller date ranges)
_HIGH_VOLUME_HERDS: Dict[str, Dict] = {}
_HIGH_VOLUME_HERDS_LOADED = False


def _load_high_volume_herds() -> None:
    """Load high-volume herds configuration from GCS."""
    global _HIGH_VOLUME_HERDS, _HIGH_VOLUME_HERDS_LOADED

    if _HIGH_VOLUME_HERDS_LOADED:
        return

    if GCS_AVAILABLE:
        try:
            gcs_data_access = GCSDataAccess()
            bucket_name = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")
            config_path = f"gs://{bucket_name}/bronze/chr/config/high_volume_herds.json"

            if gcs_data_access.file_exists(config_path):
                _HIGH_VOLUME_HERDS = gcs_data_access.download_json(config_path)
                logger.info(f"Loaded {len(_HIGH_VOLUME_HERDS)} high-volume herds from GCS")
            else:
                logger.info("No high-volume herds config found in GCS, creating default")
                _HIGH_VOLUME_HERDS = {}

            # Initialize known problematic herds if not already configured
            _initialize_known_high_volume_herds()

        except Exception as e:
            logger.warning(f"Failed to load high-volume herds config: {e}, using empty dict")
            _HIGH_VOLUME_HERDS = {}
            _initialize_known_high_volume_herds()
    else:
        logger.debug("GCS access not available - using empty high-volume herds dict")
        _HIGH_VOLUME_HERDS = {}
        _initialize_known_high_volume_herds()

    _HIGH_VOLUME_HERDS_LOADED = True


def _initialize_known_high_volume_herds() -> None:
    """Initialize known high-volume herds with optimized settings."""
    global _HIGH_VOLUME_HERDS

    # Known problematic herds from production logs
    known_high_volume = {
        "112389": {"max_days": 30, "reason": "known_high_volume", "volume_estimate": None},
        "104641": {"max_days": 30, "reason": "known_high_volume", "volume_estimate": None},
    }

    for herd_str, config in known_high_volume.items():
        if herd_str not in _HIGH_VOLUME_HERDS:
            _HIGH_VOLUME_HERDS[herd_str] = {
                **config,
                "last_updated": datetime.now().isoformat(),
                "auto_initialized": True,
            }
            logger.info(f"Auto-initialized high-volume herd {herd_str} with {config['max_days']}-day chunks")

    # Save if we added any new herds
    if any(config.get("auto_initialized") for config in _HIGH_VOLUME_HERDS.values()):
        _save_high_volume_herds()


def _save_high_volume_herds() -> None:
    """Save high-volume herds configuration to GCS."""
    if GCS_AVAILABLE:
        try:
            gcs_data_access = GCSDataAccess()
            bucket_name = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")
            config_path = f"gs://{bucket_name}/bronze/chr/config/high_volume_herds.json"

            gcs_data_access.upload_json(_HIGH_VOLUME_HERDS, config_path)
            logger.info(f"Saved {len(_HIGH_VOLUME_HERDS)} high-volume herd configurations to GCS")

        except Exception as e:
            logger.warning(f"Failed to save high-volume herds config to GCS: {e}")
    else:
        logger.debug("GCS access not available - cannot save high-volume herds config")


def add_high_volume_herd(herd_number: int, max_days: int, volume_estimate: Optional[int] = None) -> None:
    """Add a herd to the high-volume herds list with specific processing constraints."""
    _load_high_volume_herds()

    # Auto-optimize chunk size based on herd number and known patterns
    if herd_number in [112389, 104641]:
        max_days = min(max_days, 30)
        logger.warning(f"Herd {herd_number} is a known high-volume herd - using monthly 30-day chunking")
    elif volume_estimate and volume_estimate > 100000:
        max_days = min(max_days, 14)
        logger.warning(f"Herd {herd_number} has massive volume estimate ({volume_estimate}) - using 14-day chunking")
    elif volume_estimate and volume_estimate > 50000:
        max_days = min(max_days, 30)

    _HIGH_VOLUME_HERDS[str(herd_number)] = {
        "max_days": max_days,
        "last_updated": datetime.now().isoformat(),
        "volume_estimate": volume_estimate,
        "reason": "high_volume_detected",
        "auto_optimized": True if herd_number in [112389, 104641] else False,
    }

    logger.warning(f"Added herd {herd_number} to high-volume herds (max {max_days} days per request)")
    _save_high_volume_herds()


def is_high_volume_herd(herd_number: int) -> bool:
    """Check if a herd is in the high-volume herds list."""
    _load_high_volume_herds()
    return str(herd_number) in _HIGH_VOLUME_HERDS


def get_optimal_date_range(
    herd_number: int, requested_start: date, requested_end: date, discovery_results: Optional[Dict] = None
) -> List[Tuple[date, date]]:
    """
    Get optimal date ranges for a herd, splitting into smaller chunks if it's high-volume.

    Returns:
        List of (start_date, end_date) tuples
    """
    _load_high_volume_herds()

    herd_str = str(herd_number)

    # Simple approach: herds are auto-detected as high-volume when they cause issues
    # The detection happens in the animal_movements module when timeouts/failures occur

    # Pre-configure known problematic herds with reasonable chunking
    if herd_number in [112389, 104641] and herd_str not in _HIGH_VOLUME_HERDS:
        logger.warning(f"Auto-configuring known problematic herd {herd_number} with 30-day chunks")
        add_high_volume_herd(herd_number, max_days=30, volume_estimate=None)

    if herd_str in _HIGH_VOLUME_HERDS:
        max_days = _HIGH_VOLUME_HERDS[herd_str]["max_days"]
        logger.info(f"Herd {herd_number} is high-volume, splitting into {max_days}-day chunks")

        # Split the requested range into smaller chunks
        chunks = []
        current_start = requested_start

        # For very large date ranges, add extra safety by limiting total chunks
        total_days = (requested_end - requested_start).days + 1
        max_chunks = 50

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


def detect_herd_volume(chr_dyr_client: Client, username: str, herd_number: int, sample_days: int = 7) -> Dict[str, Any]:
    """
    Proactively detect if a herd is high-volume by testing a small sample period.

    Args:
        chr_dyr_client: The CHR_dyr SOAP client
        username: Username for authentication
        herd_number: The herd number to test
        sample_days: Number of recent days to sample (default 7)

    Returns:
        Dict with volume estimation and recommended chunk size
    """
    import time

    from .utils import create_base_request

    try:
        # Test a small recent sample (last week)
        end_date = date.today()
        start_date = end_date - timedelta(days=sample_days - 1)

        logger.info(f"Volume detection for herd {herd_number}: testing {sample_days} days ({start_date} to {end_date})")

        # Track timing for this test request
        start_time = time.time()

        # Create request structure
        glr_chr_ws_info_inbound_factory = chr_dyr_client.get_type("ns0:GLRCHRWSInfoInboundType")
        common_header = glr_chr_ws_info_inbound_factory(**create_base_request(username))

        chr_dyr_chr_bes_liste_request_type_factory = chr_dyr_client.get_type("ns0:CHR_dyrChrBesListeRequestType")
        request_params = chr_dyr_chr_bes_liste_request_type_factory(
            **{"BesaetningsNummer": herd_number, "PeriodeFra": start_date, "PeriodeTil": end_date}
        )

        payload_content = {"GLRCHRWSInfoInbound": common_header, "Request": request_params}

        # Make the test request
        response = chr_dyr_client.service.besListAktOms(CHR_dyrChrBesListeRequest=payload_content)
        request_duration = time.time() - start_time

        if response and hasattr(response, "Response") and response.Response:
            resp = response.Response[0] if isinstance(response.Response, list) else response.Response
            animals = getattr(resp, "Enkeltdyrsoplysninger", [])
            sample_animal_count = animals if isinstance(animals, int) else len(animals) if animals else 0

            # Calculate estimates
            animals_per_day = sample_animal_count / sample_days
            estimated_yearly = animals_per_day * 365
            estimated_5_year = estimated_yearly * 5

            # Determine volume category and recommended chunk size
            if estimated_5_year > 200000:
                volume_category = "very_high"
                recommended_chunk_days = 30
                risk_level = "extreme"
            elif estimated_5_year > 100000:
                volume_category = "high"
                recommended_chunk_days = 60
                risk_level = "high"
            elif estimated_5_year > 50000:
                volume_category = "moderate_high"
                recommended_chunk_days = 90
                risk_level = "moderate"
            else:
                volume_category = "normal"
                recommended_chunk_days = None
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
                f"Herd {herd_number} volume detection: {volume_category} "
                f"({estimated_5_year:,.0f} estimated 5-year animals)"
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
