"""Persistence layer for CHR pipeline - handles problematic herds tracking."""

import logging
import os
from datetime import datetime
from typing import Set

from dotenv import load_dotenv

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
logger = logging.getLogger("backend.pipelines.chr_pipeline.bronze.persistence")

# Global set to track problematic herds that consistently fail
_PROBLEMATIC_HERDS: Set[int] = set()
_PROBLEMATIC_HERDS_LOADED = False


def _load_problematic_herds() -> None:
    """Load problematic herds from persistent storage (GCS)."""
    global _PROBLEMATIC_HERDS, _PROBLEMATIC_HERDS_LOADED

    if _PROBLEMATIC_HERDS_LOADED:
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
                    _PROBLEMATIC_HERDS.update(data["problematic_herds"])
                    logger.info(f"Loaded {len(_PROBLEMATIC_HERDS)} problematic herds from GCS")
                else:
                    logger.info("No problematic herds found in GCS - starting with empty set")
            except Exception as e:
                logger.debug(f"Could not load problematic herds from GCS: {e}")
                # This is expected on first run or if file doesn't exist

        except Exception as e:
            logger.debug(f"Failed to access GCS: {e}")
    else:
        logger.debug("GCS access not available - problematic herds will not persist across runs")

    _PROBLEMATIC_HERDS_LOADED = True


def _save_problematic_herds() -> None:
    """Save problematic herds to persistent storage (GCS)."""
    if not _PROBLEMATIC_HERDS:
        return

    if GCS_AVAILABLE:
        try:
            gcs = GCSDataAccess()
            bucket_name = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")
            problematic_herds_path = "bronze/chr/problematic_herds.json"
            gcs_path = f"gs://{bucket_name}/{problematic_herds_path}"

            data = {
                "problematic_herds": list(_PROBLEMATIC_HERDS),
                "last_updated": datetime.now().isoformat(),
                "total_count": len(_PROBLEMATIC_HERDS),
            }

            gcs.upload_json(data, gcs_path)
            logger.info(f"Saved {len(_PROBLEMATIC_HERDS)} problematic herds to GCS")

        except Exception as e:
            logger.warning(f"Could not save problematic herds to GCS: {e}")
    else:
        logger.debug("GCS access not available - cannot save problematic herds")


def add_problematic_herd(herd_number: int) -> None:
    """Add a herd to the problematic herds list."""
    _load_problematic_herds()  # Ensure we have the latest data

    _PROBLEMATIC_HERDS.add(herd_number)
    logger.warning(f"Added herd {herd_number} to problematic herds list (will be skipped in future)")

    # Save immediately to persist the change
    _save_problematic_herds()


def is_problematic_herd(herd_number: int) -> bool:
    """Check if a herd is in the problematic herds list."""
    _load_problematic_herds()  # Ensure we have the latest data
    return herd_number in _PROBLEMATIC_HERDS


def get_problematic_herds() -> Set[int]:
    """Get the set of problematic herds."""
    _load_problematic_herds()
    return _PROBLEMATIC_HERDS.copy()
