"""Shared utilities for CHR pipeline bronze layer."""

import os
from datetime import date, datetime
from typing import Any

from common.logging_utils import get_pipeline_logger

# Import UUID utilities for deterministic TrackID generation
try:
    from backend.pipelines.unified_pipeline.src.unified_pipeline.common.uuid_utils import (
        LandbrugsdataUUID,
    )
except ImportError:
    try:
        import sys
        from pathlib import Path

        unified_pipeline_path = Path(__file__).parent.parent.parent / "unified_pipeline" / "src"
        if unified_pipeline_path.exists():
            sys.path.append(str(unified_pipeline_path))
        from unified_pipeline.common.uuid_utils import LandbrugsdataUUID
    except ImportError:
        LandbrugsdataUUID = None

# Set up logging
logger = get_pipeline_logger("backend.pipelines.chr_pipeline.bronze.utils")

# Default Client ID for SOAP requests. FVM_CLIENT_ID can override this when FVST
# assigns an account-specific GLR client id.
DEFAULT_CLIENT_ID = "LandbrugsData"


def create_base_request(
    username: str, session_id: str = "1", track_id: str = "chr_pipeline"
) -> dict[str, str]:
    """Create the common GLRCHRWSInfoInbound structure."""
    # Generate deterministic TrackID based on username and session
    if LandbrugsdataUUID:
        request_key = f"{username}-{session_id}-{track_id}"
        deterministic_track_id = (
            f"{track_id}-{LandbrugsdataUUID.generate_deterministic_uuid('chr-track', request_key)}"
        )
    else:
        # Fallback to random UUID if LandbrugsdataUUID not available
        import uuid

        deterministic_track_id = f"{track_id}-{uuid.uuid4()}"

    return {
        "BrugerNavn": username,
        "KlientId": os.getenv("FVM_CLIENT_ID") or DEFAULT_CLIENT_ID,
        "SessionId": session_id,
        "IPAdresse": "",
        "TrackID": deterministic_track_id,
    }


def parse_date(date_str: Any) -> date | None:
    """Parse date string from CHR response."""
    if not date_str:
        return None
    try:
        # Handle different date formats that might come from CHR
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
