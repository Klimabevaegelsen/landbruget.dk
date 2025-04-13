"""Module for exporting pig movement data."""

import logging
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Iterator
from datetime import datetime, date
from io import BytesIO

from dotenv import load_dotenv
from google.cloud import storage
from google.api_core.exceptions import GoogleAPICallError
from tqdm.auto import tqdm

class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for handling datetime and date objects."""
    def default(self, obj: Any) -> str:
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)

# Load environment variables
load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)

# Initialize storage paths and clients
GCS_BUCKET = os.getenv('GCS_BUCKET')
GOOGLE_CLOUD_PROJECT = os.getenv('GOOGLE_CLOUD_PROJECT')

# Use GCS if we have the required configuration
USE_GCS = bool(GCS_BUCKET and GOOGLE_CLOUD_PROJECT)

# Initialize GCS client if bucket is configured
gcs_client = None
if USE_GCS:
    try:
        gcs_client = storage.Client(project=GOOGLE_CLOUD_PROJECT)
        logger.debug(f"Initialized GCS client for project: {GOOGLE_CLOUD_PROJECT}")
    except Exception as e:
        logger.error(f"Failed to initialize GCS client: {e}")
        logger.warning("Falling back to local storage")
        USE_GCS = False

if not USE_GCS:
    logger.warning("Using local storage in ./data/raw/svineflytning/")

def _save_to_gcs(blob_path: str, data_iterator: Iterator[Dict]) -> str:
    """
    Helper function to stream content to GCS.
    
    Args:
        blob_path: The path to save the blob to.
        data_iterator: Iterator yielding data to stream.
        
    Returns:
        str: The full GCS path where the content was saved.
    """
    bucket = gcs_client.bucket(GCS_BUCKET)
    # Add bronze/svineflytning/{timestamp} prefix to all files
    full_path = f"bronze/svineflytning/{blob_path}"
    blob = bucket.blob(full_path)

    # Create a streaming upload
    with blob.open('w') as f:
        # Write opening bracket for JSON array
        f.write('[\n')
        
        first = True
        for item in data_iterator:
            if not first:
                f.write(',\n')
            else:
                first = False
            json.dump(item, f, indent=2, ensure_ascii=False, cls=DateTimeEncoder)
        
        # Write closing bracket
        f.write('\n]')

    return f"gs://{GCS_BUCKET}/{full_path}"

def _save_locally(filepath: Path, data_iterator: Iterator[Dict]) -> str:
    """
    Helper function to save content locally.
    
    Args:
        filepath: The path to save the file to.
        data_iterator: Iterator yielding data to save.
        
    Returns:
        str: The full local path where the content was saved.
    """
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, 'w', encoding='utf-8') as f:
        # Write opening bracket for JSON array
        f.write('[\n')
        
        first = True
        for item in data_iterator:
            if not first:
                f.write(',\n')
            else:
                first = False
            json.dump(item, f, indent=2, ensure_ascii=False, cls=DateTimeEncoder)
        
        # Write closing bracket
        f.write('\n]')
    
    return str(filepath.absolute())

def export_movements(data_iterator: Iterator[Dict], export_timestamp: str, filename: str) -> Dict[str, Any]:
    """
    Export pig movement data to either GCS or local storage using streaming.
    
    Args:
        data_iterator: Iterator yielding data to export
        export_timestamp: Timestamp string for the export
        filename: Name of the file to export
        
    Returns:
        Dict containing export metadata
    """
    destination = None
    if USE_GCS:
        try:
            logger.debug(f"Streaming data to GCS bucket '{GCS_BUCKET}'")
            destination = _save_to_gcs(
                f"{export_timestamp}/{filename}",
                data_iterator
            )
            logger.debug(f"Successfully exported to GCS: {destination}")
        except Exception as e:
            logger.error(f"Error writing to GCS: {e}")
            logger.warning("Falling back to local storage")
            filepath = Path("./data/raw/svineflytning") / export_timestamp / filename
            destination = _save_locally(
                filepath,
                data_iterator
            )
            logger.debug(f"Successfully saved locally: {destination}")
    else:
        filepath = Path("./data/raw/svineflytning") / export_timestamp / filename
        destination = _save_locally(
            filepath,
            data_iterator
        )
        logger.debug(f"Successfully saved locally: {destination}")
    
    return {
        "export_timestamp": export_timestamp,
        "filename": filename,
        "storage_type": "gcs" if USE_GCS else "local",
        "destination": destination
    }
