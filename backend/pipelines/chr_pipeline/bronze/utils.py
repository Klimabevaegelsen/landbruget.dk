"""Shared utilities for CHR pipeline bronze layer."""

import logging
import uuid
from datetime import date, datetime
from typing import Any, Dict, Optional

# Set up logging
logger = logging.getLogger("backend.pipelines.chr_pipeline.bronze.utils")

# Default Client ID for SOAP requests
DEFAULT_CLIENT_ID = "LandbrugsData"


def create_base_request(username: str, session_id: str = "1", track_id: str = "chr_pipeline") -> Dict[str, str]:
    """Create the common GLRCHRWSInfoInbound structure."""
    return {
        "BrugerNavn": username,
        "KlientId": DEFAULT_CLIENT_ID,
        "SessionId": session_id,
        "IPAdresse": "",
        "TrackID": f"{track_id}-{uuid.uuid4()}",
    }


def parse_date(date_str: Any) -> Optional[date]:
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
