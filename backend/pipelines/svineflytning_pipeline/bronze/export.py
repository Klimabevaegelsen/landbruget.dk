"""Module for exporting pig movement data."""

import logging
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
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
        logger.info(f"Using GCS storage with bucket: {GCS_BUCKET}")
    except Exception as e:
        logger.error(f"Failed to initialize GCS client: {e}")
        logger.info("Falling back to local storage")
        USE_GCS = False

if not USE_GCS:
    logger.info("Using local storage in ./data/bronze/")

# --- In-memory buffer for consolidated output ---
_data_buffer: Dict[str, List[Any]] = {}

# Get timestamp for this export run
EXPORT_TIMESTAMP = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

def _save_to_gcs(blob_path: str, content: str) -> str:
    """Save content to GCS."""
    bucket = gcs_client.bucket(GCS_BUCKET)
    # Add bronze/svineflytning/{timestamp} prefix to all files
    full_path = f"bronze/svineflytning/{EXPORT_TIMESTAMP}/{blob_path}"
    blob = bucket.blob(full_path)
    blob.upload_from_string(content, content_type='application/json')
    return f"gs://{GCS_BUCKET}/{full_path}"

def _save_locally(filepath: Path, content: str) -> str:
    """Save content locally."""
    # Add timestamp to the path
    timestamped_path = filepath.parent / EXPORT_TIMESTAMP / filepath.name
    timestamped_path.parent.mkdir(parents=True, exist_ok=True)
    with open(timestamped_path, 'w', encoding='utf-8') as f:
        f.write(content)
    return str(timestamped_path.absolute())

def save_movements(movements: List[Dict[str, Any]]):
    """Buffer movement data for later export."""
    if not movements:
        logger.warning("No movements provided to save")
        return

    if 'movements' not in _data_buffer:
        _data_buffer['movements'] = []

    # Add timestamp to each movement
    for movement in movements:
        if isinstance(movement, dict):
            movement['_export_timestamp'] = EXPORT_TIMESTAMP
        _data_buffer['movements'].extend(movements)

def finalize_export() -> Dict[str, Any]:
    """Write buffered data to consolidated files."""
    if not _data_buffer:
        logger.warning("No data buffered for export")
        return {"status": "no_data"}

    storage_mode = "GCS" if USE_GCS else "local filesystem"
    logger.info(f"Starting export using {storage_mode}")

    result = {
        "export_timestamp": EXPORT_TIMESTAMP,
        "storage_type": "gcs" if USE_GCS else "local",
        "files_written": 0,
        "record_counts": {}
    }

    for data_type, data_list in _data_buffer.items():
        if not data_list:
            continue

        filename = f"{data_type}.json"
        record_count = len(data_list)
        result["record_counts"][data_type] = record_count

        try:
            json_content = json.dumps(data_list, indent=2, ensure_ascii=False)
            
            if USE_GCS:
                logger.info(f"Writing {record_count} records to GCS: {filename}")
                destination = _save_to_gcs(filename, json_content)
            else:
                filepath = Path(f"./data/bronze/svineflytning/{filename}")
                logger.info(f"Writing {record_count} records locally to {filepath}")
                destination = _save_locally(filepath, json_content)

            result["files_written"] += 1
            result["destination"] = destination

        except Exception as e:
            logger.error(f"Error writing file {filename}: {e}")
            raise

    # Clear the buffer after successful export
    _data_buffer.clear()
    
    logger.info(f"Export complete: {result['files_written']} files written to {storage_mode}")
    return result

def clear_buffer():
    """Explicitly clear the data buffer."""
    _data_buffer.clear()
    logger.info("Data buffer cleared")
