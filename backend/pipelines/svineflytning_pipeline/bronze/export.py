"""Module for exporting pig movement data."""

import logging
import json
import os
from pathlib import Path
from typing import Any, Dict, List
from datetime import datetime

from dotenv import load_dotenv
from google.cloud import storage
from google.api_core.exceptions import GoogleAPICallError
from tqdm.auto import tqdm

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

def _save_to_gcs(blob_path: str, content: str) -> str:
    """
    Helper function to save content to GCS.
    
    Args:
        blob_path: The path to save the blob to.
        content: The content to save.
        
    Returns:
        str: The full GCS path where the content was saved.
    """
    bucket = gcs_client.bucket(GCS_BUCKET)
    # Add bronze/svineflytning/{timestamp} prefix to all files
    full_path = f"bronze/svineflytning/{blob_path}"
    blob = bucket.blob(full_path)
    blob.upload_from_string(content, content_type='application/json')
    return f"gs://{GCS_BUCKET}/{full_path}"

def _save_locally(filepath: Path, content: str) -> str:
    """
    Helper function to save content locally.
    
    Args:
        filepath: The path to save the file to.
        content: The content to save.
        
    Returns:
        str: The full local path where the content was saved.
    """
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    return str(filepath.absolute())

def export_movements(data_path: str, export_timestamp: str, filename: str) -> Dict[str, Any]:
    """
    Export pig movement data to either GCS or local storage.
    
    Args:
        data_path: Path to the data directory
        export_timestamp: Timestamp string for the export
        filename: Name of the file to export
        
    Returns:
        Dict containing export metadata
    """
    filepath = Path(data_path) / export_timestamp / filename
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        logger.error(f"Failed to read data from {filepath}: {e}")
        raise
    
    record_count = len(data) if isinstance(data, list) else 0
    logger.debug(f"Loaded {record_count} records from {filepath}")
    
    destination = None
    if USE_GCS:
        try:
            logger.debug(f"Exporting {record_count} records to GCS bucket '{GCS_BUCKET}'")
            destination = _save_to_gcs(
                f"{export_timestamp}/{filename}",
                json.dumps(data, indent=2, ensure_ascii=False)
            )
            logger.debug(f"Successfully exported to GCS: {destination}")
        except Exception as e:
            logger.error(f"Error writing to GCS: {e}")
            logger.warning("Falling back to local storage")
            destination = _save_locally(
                filepath,
                json.dumps(data, indent=2, ensure_ascii=False)
            )
            logger.debug(f"Successfully saved locally: {destination}")
    else:
        destination = _save_locally(
            filepath,
            json.dumps(data, indent=2, ensure_ascii=False)
        )
        logger.debug(f"Successfully saved locally: {destination}")
    
    return {
        "export_timestamp": export_timestamp,
        "filename": filename,
        "record_count": record_count,
        "storage_type": "gcs" if USE_GCS else "local",
        "destination": destination
    }
