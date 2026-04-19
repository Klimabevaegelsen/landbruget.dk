"""Persistence layer for CHR pipeline - handles problematic herds tracking."""

import os
from datetime import datetime

from common.logging_utils import get_pipeline_logger
from dotenv import find_dotenv, load_dotenv

# Import cloud storage access for persistent storage
try:
    from common.storage import StorageAccess

    STORAGE_AVAILABLE = True
except ImportError:
    STORAGE_AVAILABLE = False
    StorageAccess = None

# Load environment variables
load_dotenv(find_dotenv(usecwd=True))

# Set up logging
logger = get_pipeline_logger("backend.pipelines.chr_pipeline.bronze.persistence")

# Global set to track problematic herds that consistently fail
_PROBLEMATIC_HERDS: set[int] = set()
_PROBLEMATIC_HERDS_LOADED = False


def _load_problematic_herds() -> None:
    """Load problematic herds from persistent storage (cloud storage)."""
    global _PROBLEMATIC_HERDS, _PROBLEMATIC_HERDS_LOADED

    if _PROBLEMATIC_HERDS_LOADED:
        return

    if STORAGE_AVAILABLE:
        try:
            storage = StorageAccess()
            bucket_name = (
                os.getenv("STORAGE_BUCKET")
                or os.getenv("R2_BUCKET")
                or os.getenv("GCS_BUCKET", "landbruget-data")
            )
            problematic_herds_path = "bronze/chr/problematic_herds.json"
            storage_path = f"{bucket_name}/{problematic_herds_path}"

            try:
                data = storage.download_json(storage_path)
                if data and "problematic_herds" in data:
                    _PROBLEMATIC_HERDS.update(data["problematic_herds"])
                    logger.info(
                        f"Loaded {len(_PROBLEMATIC_HERDS)} problematic herds from cloud storage"
                    )
                else:
                    logger.info(
                        "No problematic herds found in cloud storage - starting with empty set"
                    )
            except Exception as e:
                logger.debug(f"Could not load problematic herds from cloud storage: {e}")
                # This is expected on first run or if file doesn't exist

        except Exception as e:
            logger.debug(f"Failed to access cloud storage: {e}")
    else:
        logger.debug(
            "Cloud storage access not available - problematic herds will not persist across runs"
        )

    _PROBLEMATIC_HERDS_LOADED = True


def _save_problematic_herds() -> None:
    """Save problematic herds to persistent storage (cloud storage)."""
    if not _PROBLEMATIC_HERDS:
        return

    if STORAGE_AVAILABLE:
        try:
            storage = StorageAccess()
            bucket_name = (
                os.getenv("STORAGE_BUCKET")
                or os.getenv("R2_BUCKET")
                or os.getenv("GCS_BUCKET", "landbruget-data")
            )
            problematic_herds_path = "bronze/chr/problematic_herds.json"
            storage_path = f"{bucket_name}/{problematic_herds_path}"

            data = {
                "problematic_herds": list(_PROBLEMATIC_HERDS),
                "last_updated": datetime.now().isoformat(),
                "total_count": len(_PROBLEMATIC_HERDS),
            }

            storage.upload_json(data, storage_path)
            logger.info(f"Saved {len(_PROBLEMATIC_HERDS)} problematic herds to cloud storage")

        except Exception as e:
            logger.warning(f"Could not save problematic herds to cloud storage: {e}")
    else:
        logger.debug("Cloud storage access not available - cannot save problematic herds")


def add_problematic_herd(herd_number: int) -> None:
    """Add a herd to the problematic herds list."""
    _load_problematic_herds()  # Ensure we have the latest data

    _PROBLEMATIC_HERDS.add(herd_number)
    logger.warning(
        f"Added herd {herd_number} to problematic herds list (will be skipped in future)"
    )

    # Save immediately to persist the change
    _save_problematic_herds()


def is_problematic_herd(herd_number: int) -> bool:
    """Check if a herd is in the problematic herds list."""
    _load_problematic_herds()  # Ensure we have the latest data
    return herd_number in _PROBLEMATIC_HERDS


def get_problematic_herds() -> set[int]:
    """Get the set of problematic herds."""
    _load_problematic_herds()
    return _PROBLEMATIC_HERDS.copy()
