"""Module for exporting pig movement data."""

import json
import os
from collections.abc import Iterator
from datetime import date, datetime
from pathlib import Path
from typing import Any

import ijson  # Add this import for streaming JSON parsing
from common.logging_utils import get_pipeline_logger
from dotenv import find_dotenv, load_dotenv

# Import the unified cloud storage access layer
try:
    from common.storage import StorageAccess

    STORAGE_AVAILABLE = True
except ImportError:
    StorageAccess = None
    STORAGE_AVAILABLE = False


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for handling datetime and date objects."""

    def default(self, obj: Any) -> str:
        if isinstance(obj, datetime | date):
            return obj.isoformat()
        # Handle any other custom types that might come from the SOAP response
        try:
            return str(obj)
        except Exception:
            return super().default(obj)


# Load environment variables
load_dotenv(find_dotenv(usecwd=True))

# Configure logging
logger = get_pipeline_logger(__name__)

# Initialize storage paths and clients
GCS_BUCKET = os.getenv("STORAGE_BUCKET") or os.getenv("R2_BUCKET") or os.getenv("GCS_BUCKET")
GOOGLE_CLOUD_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT")

# Use cloud storage if we have the required configuration
USE_CLOUD_STORAGE = bool(GCS_BUCKET and GOOGLE_CLOUD_PROJECT and STORAGE_AVAILABLE)

# Initialize cloud storage access layer if available
storage_access = None
if USE_CLOUD_STORAGE:
    try:
        storage_access = StorageAccess()
        logger.debug(f"Initialized StorageAccess for project: {GOOGLE_CLOUD_PROJECT}")
    except Exception as e:
        logger.error(f"Failed to initialize StorageAccess: {e}")
        logger.warning("Falling back to local storage")
        USE_CLOUD_STORAGE = False

if not USE_CLOUD_STORAGE:
    logger.warning(
        "Using local storage (path will be determined by SVINEFLYTNING_OUTPUT_DIR environment variable)"
    )


def _save_to_storage(blob_path: str, data_iterator: Iterator[dict]) -> str:
    """
    Helper function to stream content to cloud storage using unified StorageAccess.

    Args:
        blob_path: The path to save the blob to.
        data_iterator: Iterator yielding data to stream.

    Returns:
        str: The full cloud storage path where the content was saved.
    """
    # Add bronze/svineflytning/{timestamp} prefix to all files
    full_path = f"bronze/svineflytning/{blob_path}"
    storage_path = f"{GCS_BUCKET}/{full_path}"
    fs_path = f"{GCS_BUCKET}/{full_path}"

    # Create a streaming upload using s3fs
    with storage_access.fs.open(fs_path, "w", encoding="utf-8") as f:
        # Write opening bracket for JSON array
        f.write("[\n")

        first = True
        for item in data_iterator:
            if not first:
                f.write(",\n")
            else:
                first = False
            json.dump(item, f, indent=2, ensure_ascii=False, cls=DateTimeEncoder)

        # Write closing bracket
        f.write("\n]")

    return storage_path


def _save_locally(filepath: Path, data_iterator: Iterator[dict]) -> str:
    """
    Helper function to save content locally.

    Args:
        filepath: The path to save the file to.
        data_iterator: Iterator yielding data to save.

    Returns:
        str: The full local path where the content was saved.
    """
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        # Write opening bracket for JSON array
        f.write("[\n")

        first = True
        for item in data_iterator:
            if not first:
                f.write(",\n")
            else:
                first = False
            json.dump(item, f, indent=2, ensure_ascii=False, cls=DateTimeEncoder)

        # Write closing bracket
        f.write("\n]")

    return str(filepath.absolute())


def export_movements(
    data_iterator: Iterator[dict],
    export_timestamp: str,
    filename: str,
    output_dir: str = "/data/raw/svineflytning",
) -> dict[str, Any]:
    """
    Export pig movement data to either cloud storage or local storage using streaming.

    Args:
        data_iterator: Iterator yielding data to export
        export_timestamp: Timestamp string for the export
        filename: Name of the file to export
        output_dir: Base directory for local storage (default: /data/raw/svineflytning)

    Returns:
        Dict containing export metadata
    """
    destination = None
    if USE_CLOUD_STORAGE:
        try:
            logger.debug(f"Streaming data to storage bucket '{GCS_BUCKET}'")
            destination = _save_to_storage(f"{export_timestamp}/{filename}", data_iterator)
            logger.debug(f"Successfully exported to cloud storage: {destination}")
        except Exception as e:
            logger.error(f"Error writing to cloud storage: {e}")
            logger.warning("Falling back to local storage")
            filepath = Path(output_dir) / export_timestamp / filename
            destination = _save_locally(filepath, data_iterator)
            logger.debug(f"Successfully saved locally: {destination}")
    else:
        filepath = Path(output_dir) / export_timestamp / filename
        destination = _save_locally(filepath, data_iterator)
        logger.debug(f"Successfully saved locally: {destination}")

    return {
        "export_timestamp": export_timestamp,
        "filename": filename,
        "storage_type": "storage" if USE_CLOUD_STORAGE else "local",
        "destination": destination,
    }


def export_movements_optimized(
    temp_files: list[Path],
    export_timestamp: str,
    total_chunks: int,
    output_dir: str = "/data/raw/svineflytning",
) -> dict[str, Any]:
    """
    Export pig movement data using streaming to minimize memory usage.

    Args:
        temp_files: List of temporary files containing the movement data
        export_timestamp: Timestamp string for the export
        total_chunks: Total number of chunks processed
        output_dir: Base directory for local storage (default: /data/raw/svineflytning)

    Returns:
        Dict containing export metadata
    """

    def stream_temp_file(temp_file: Path):
        """Stream contents of a temp file one item at a time."""
        with open(temp_file, "rb") as f:
            parser = ijson.items(f, "item")
            yield from parser

    if USE_CLOUD_STORAGE:
        try:
            logger.debug(f"Starting streaming upload to storage bucket '{GCS_BUCKET}'")

            storage_path = (
                f"{GCS_BUCKET}/bronze/svineflytning/{export_timestamp}/svineflytning.json"
            )
            fs_path = f"{GCS_BUCKET}/bronze/svineflytning/{export_timestamp}/svineflytning.json"

            # Stream directly to cloud storage using s3fs
            with storage_access.fs.open(fs_path, "w", encoding="utf-8") as f:
                f.write("[\n")

                first_item = True
                for temp_file in temp_files:
                    for item in stream_temp_file(temp_file):
                        if not first_item:
                            f.write(",\n")
                        else:
                            first_item = False
                        json.dump(item, f, indent=2, cls=DateTimeEncoder)

                f.write("\n]")

            destination = storage_path
            logger.debug(f"Successfully exported to cloud storage: {destination}")

        except Exception as e:
            logger.error(f"Error writing to cloud storage: {e}")
            logger.warning("Falling back to local storage")

            # Fallback to local storage
            local_dir = Path(output_dir) / export_timestamp
            local_dir.mkdir(parents=True, exist_ok=True)
            output_file = local_dir / "svineflytning.json"

            # Stream to local file
            with open(output_file, "w") as f:
                f.write("[\n")

                first_item = True
                for temp_file in temp_files:
                    for item in stream_temp_file(temp_file):
                        if not first_item:
                            f.write(",\n")
                        else:
                            first_item = False
                        json.dump(item, f, indent=2, cls=DateTimeEncoder)

                f.write("\n]")

            destination = str(output_file.absolute())
            logger.debug(f"Successfully saved locally: {destination}")
    else:
        # Direct local storage
        local_dir = Path(output_dir) / export_timestamp
        local_dir.mkdir(parents=True, exist_ok=True)
        output_file = local_dir / "svineflytning.json"

        # Stream to local file
        with open(output_file, "w") as f:
            f.write("[\n")

            first_item = True
            for temp_file in temp_files:
                for item in stream_temp_file(temp_file):
                    if not first_item:
                        f.write(",\n")
                    else:
                        first_item = False
                    json.dump(item, f, indent=2, cls=DateTimeEncoder)

            f.write("\n]")

        destination = str(output_file.absolute())
        logger.debug(f"Successfully saved locally: {destination}")

    # Enforce retention: keep only the last 3 versions
    if USE_CLOUD_STORAGE and storage_access:
        try:
            storage_access.enforce_retention(f"{GCS_BUCKET}/bronze/svineflytning", keep=3)
        except Exception as e:
            logger.warning(f"Retention cleanup failed for bronze/svineflytning: {e}")

    return {
        "export_timestamp": export_timestamp,
        "storage_type": "storage" if USE_CLOUD_STORAGE else "local",
        "destination": destination,
    }
