"""Module for exporting pig movement data."""

import logging
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from datetime import datetime
import shutil
import psutil
import gc

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

def _append_to_gcs(blob_path: str, content: str, is_first: bool = False, is_last: bool = False) -> str:
    """Append content to a GCS file."""
    bucket = gcs_client.bucket(GCS_BUCKET)
    full_path = f"bronze/svineflytning/{EXPORT_TIMESTAMP}/{blob_path}"
    blob = bucket.blob(full_path)
    
    if is_first:
        blob.upload_from_string('[\n', content_type='application/json')
    elif is_last:
        blob.upload_from_string('\n]', content_type='application/json')
    else:
        # Check if blob exists (not first chunk)
        if blob.exists():
            current_size = blob.size
            blob.upload_from_string(content, content_type='application/json', 
                                  offset=current_size)
        else:
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

def _check_disk_space(path: str = "./") -> Dict[str, float]:
    """Monitor disk space usage."""
    usage = shutil.disk_usage(path)
    return {
        "total_gb": usage.total / (1024**3),
        "used_gb": usage.used / (1024**3),
        "free_gb": usage.free / (1024**3),
        "percent_used": (usage.used / usage.total) * 100
    }

def finalize_export() -> Dict[str, Any]:
    """Write buffered data to consolidated files."""
    if not _data_buffer:
        logger.warning("No data buffered for export")
        return {"status": "no_data"}

    storage_mode = "GCS" if USE_GCS else "local filesystem"
    logger.info(f"Starting export using {storage_mode}")

    # Check initial disk space
    disk_info = _check_disk_space()
    logger.info(f"Initial disk space - Free: {disk_info['free_gb']:.2f}GB, Used: {disk_info['percent_used']:.1f}%")

    if disk_info['free_gb'] < 2.0:  # Alert if less than 2GB free
        logger.warning(f"Low disk space warning! Only {disk_info['free_gb']:.2f}GB available")

    result = {
        "export_timestamp": EXPORT_TIMESTAMP,
        "storage_type": "gcs" if USE_GCS else "local",
        "files_written": 0,
        "record_counts": {},
        "initial_disk_space": disk_info
    }

    CHUNK_SIZE = 10000  # Process 10k records at a time
    for data_type, data_list in _data_buffer.items():
        if not data_list:
            continue

        record_count = len(data_list)
        result["record_counts"][data_type] = record_count
        
        filename = f"{data_type}.json"
        chunks = [data_list[i:i + CHUNK_SIZE] for i in range(0, len(data_list), CHUNK_SIZE)]
        logger.info(f"Processing {len(chunks)} chunks of {CHUNK_SIZE} records each for {data_type}")

        if USE_GCS:
            logger.info(f"Writing {record_count} records to GCS: {filename}")
            
            # Write opening bracket
            destination = _append_to_gcs(filename, '', is_first=True)
            
            # Write chunks
            for i, chunk in enumerate(chunks):
                chunk_json = json.dumps(chunk, indent=2, ensure_ascii=False)[1:-1]  # Remove [ and ]
                if i < len(chunks) - 1:
                    chunk_json += ',\n'
                _append_to_gcs(filename, chunk_json)
                
                # Monitor disk space and memory
                if i % 5 == 0:
                    disk_info = _check_disk_space()
                    logger.info(f"Disk space after chunk {i+1}/{len(chunks)} - Free: {disk_info['free_gb']:.2f}GB")
                    if disk_info['free_gb'] < 0.5:
                        raise RuntimeError(f"Critical disk space: only {disk_info['free_gb']:.2f}GB available")
                
                # Clean up memory
                del chunk_json
                gc.collect()
            
            # Write closing bracket
            _append_to_gcs(filename, '', is_last=True)
            
        else:
            filepath = Path(f"./data/bronze/svineflytning/{filename}")
            logger.info(f"Writing {record_count} records locally to {filepath}")
            
            # Write the opening bracket
            filepath.parent.mkdir(parents=True, exist_ok=True)
            timestamped_path = filepath.parent / EXPORT_TIMESTAMP / filepath.name
            timestamped_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(timestamped_path, 'w', encoding='utf-8') as f:
                f.write('[\n')
            
            # Write chunks
            for i, chunk in enumerate(chunks):
                chunk_json = json.dumps(chunk, indent=2, ensure_ascii=False)[1:-1]  # Remove [ and ]
                with open(timestamped_path, 'a', encoding='utf-8') as f:
                    f.write(chunk_json)
                    if i < len(chunks) - 1:
                        f.write(',\n')
                    
                # Monitor disk space
                if i % 5 == 0:
                    disk_info = _check_disk_space()
                    logger.info(f"Disk space after chunk {i+1}/{len(chunks)} - Free: {disk_info['free_gb']:.2f}GB")
                    if disk_info['free_gb'] < 0.5:
                        raise RuntimeError(f"Critical disk space: only {disk_info['free_gb']:.2f}GB available")
                
                # Clean up memory
                del chunk_json
                gc.collect()
            
            # Write closing bracket
            with open(timestamped_path, 'a', encoding='utf-8') as f:
                f.write('\n]')
            
            destination = str(timestamped_path.absolute())

        result["files_written"] += 1
        result["destination"] = os.path.dirname(destination)

    # Final disk space check
    final_disk_info = _check_disk_space()
    result["final_disk_space"] = final_disk_info
    logger.info(f"Final disk space - Free: {final_disk_info['free_gb']:.2f}GB, Used: {final_disk_info['percent_used']:.1f}%")

    # Clear the buffer after successful export
    _data_buffer.clear()
    
    logger.info(f"Export complete: {result['files_written']} files written to {storage_mode}")
    return result

def clear_buffer():
    """Explicitly clear the data buffer."""
    _data_buffer.clear()
    logger.info("Data buffer cleared")
