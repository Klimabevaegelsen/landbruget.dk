"""Module for exporting pig movement data."""

import logging
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, date
import io

from dotenv import load_dotenv
from google.cloud import storage
from google.api_core.exceptions import GoogleAPICallError

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

# --- In-memory buffer for local storage ---
_data_buffer: Dict[str, List[Any]] = {}

# Get timestamp for this export run
EXPORT_TIMESTAMP = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

class DateTimeEncoder(json.JSONEncoder):
    """JSON encoder that handles datetime and date objects."""
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)

def save_movements(movements: List[Dict[str, Any]]):
    """Save movement data directly or buffer for later export."""
    if not movements:
        logger.warning("No movements provided to save")
        return

    if USE_GCS:
        # Stream directly to GCS
        filename = "movements.json"
        base_path = f"bronze/svineflytning/{EXPORT_TIMESTAMP}"
        full_path = f"{base_path}/{filename}"
        
        # Add timestamp to each movement
        for movement in movements:
            if isinstance(movement, dict):
                movement['_export_timestamp'] = EXPORT_TIMESTAMP
        
        # Create a stream with the data
        stream = io.StringIO()
        json.dump(movements, stream, ensure_ascii=False, cls=DateTimeEncoder)
        stream.seek(0)
        
        # Upload to GCS
        bucket = gcs_client.bucket(GCS_BUCKET)
        blob = bucket.blob(full_path)
        blob.upload_from_file(stream, content_type='application/json')
        
        # Clean up
        stream.close()
        
    else:
        # For local storage, keep the existing buffering logic
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
        if USE_GCS:
            return {
                "export_timestamp": EXPORT_TIMESTAMP,
                "storage_type": "gcs",
                "files_written": 1,  # We already wrote the file in save_movements
                "destination": f"gs://{GCS_BUCKET}/bronze/svineflytning/{EXPORT_TIMESTAMP}"
            }
        logger.warning("No data buffered for export")
        return {"status": "no_data"}

    if not USE_GCS:
        # Local storage export
        result = {
            "export_timestamp": EXPORT_TIMESTAMP,
            "storage_type": "local",
            "files_written": 0,
            "record_counts": {},
        }

        for data_type, data_list in _data_buffer.items():
            if not data_list:
                continue

            record_count = len(data_list)
            result["record_counts"][data_type] = record_count
            
            filename = f"{data_type}.json"
            filepath = Path(f"./data/bronze/svineflytning/{filename}")
            logger.info(f"Writing {record_count} records locally to {filepath}")
            
            filepath.parent.mkdir(parents=True, exist_ok=True)
            timestamped_path = filepath.parent / EXPORT_TIMESTAMP / filepath.name
            timestamped_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(timestamped_path, 'w', encoding='utf-8') as f:
                json.dump(data_list, f, ensure_ascii=False, cls=DateTimeEncoder, indent=2)
            
            destination = str(timestamped_path.absolute())
            result["files_written"] += 1
            result["destination"] = os.path.dirname(destination)

        # Clear the buffer after successful export
        _data_buffer.clear()
        
        logger.info(f"Export complete: {result['files_written']} files written to local storage")
        return result

def clear_buffer():
    """Explicitly clear the data buffer."""
    _data_buffer.clear()
    logger.info("Data buffer cleared")
