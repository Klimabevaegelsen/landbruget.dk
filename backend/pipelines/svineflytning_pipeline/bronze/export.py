"""Module for exporting pig movement data."""

import json
import logging
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List

import ijson  # Add this import for streaming JSON parsing
from dotenv import load_dotenv
from google.cloud import storage


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for handling datetime and date objects."""

    def default(self, obj: Any) -> str:
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        # Handle any other custom types that might come from the SOAP response
        try:
            return str(obj)
        except Exception:
            return super().default(obj)


# Load environment variables
load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)

# Initialize storage paths and clients
GCS_BUCKET = os.getenv("GCS_BUCKET")
GOOGLE_CLOUD_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT")

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
    logger.warning("Using local storage in /data/raw/svineflytning/")


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
    with blob.open("w") as f:
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
            destination = _save_to_gcs(f"{export_timestamp}/{filename}", data_iterator)
            logger.debug(f"Successfully exported to GCS: {destination}")
        except Exception as e:
            logger.error(f"Error writing to GCS: {e}")
            logger.warning("Falling back to local storage")
            filepath = Path("/data/raw/svineflytning") / export_timestamp / filename
            destination = _save_locally(filepath, data_iterator)
            logger.debug(f"Successfully saved locally: {destination}")
    else:
        filepath = Path("/data/raw/svineflytning") / export_timestamp / filename
        destination = _save_locally(filepath, data_iterator)
        logger.debug(f"Successfully saved locally: {destination}")

    return {
        "export_timestamp": export_timestamp,
        "filename": filename,
        "storage_type": "gcs" if USE_GCS else "local",
        "destination": destination,
    }


def export_movements_optimized(temp_files: List[Path], export_timestamp: str, total_chunks: int) -> Dict[str, Any]:
    """
    Export pig movement data using streaming to minimize memory usage.

    Args:
        temp_files: List of temporary files containing the movement data
        export_timestamp: Timestamp string for the export
        total_chunks: Total number of chunks processed

    Returns:
        Dict containing export metadata
    """

    def stream_temp_file(temp_file: Path):
        """Stream contents of a temp file one item at a time."""
        with open(temp_file, "rb") as f:
            parser = ijson.items(f, "item")
            for item in parser:
                yield item

    if USE_GCS:
        try:
            logger.debug(f"Starting streaming upload to GCS bucket '{GCS_BUCKET}'")

            storage_client = storage.Client()
            bucket = storage_client.bucket(GCS_BUCKET)
            blob = bucket.blob(f"bronze/svineflytning/{export_timestamp}/svineflytning.json")

            # Stream directly to GCS
            with blob.open("w") as f:
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

            destination = f"gs://{GCS_BUCKET}/bronze/svineflytning/{export_timestamp}/svineflytning.json"
            logger.debug(f"Successfully exported to GCS: {destination}")

        except Exception as e:
            logger.error(f"Error writing to GCS: {e}")
            logger.warning("Falling back to local storage")

            # Fallback to local storage
            local_dir = Path("/data/raw/svineflytning") / export_timestamp
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
        local_dir = Path("/data/raw/svineflytning") / export_timestamp
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

    return {
        "export_timestamp": export_timestamp,
        "storage_type": "gcs" if USE_GCS else "local",
        "destination": destination,
    }
