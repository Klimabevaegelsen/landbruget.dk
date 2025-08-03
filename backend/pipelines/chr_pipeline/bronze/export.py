"""Module for exporting raw Bronze data (JSON/XML)."""

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from dotenv import load_dotenv
from google.cloud import storage
from zeep.helpers import serialize_object

# Load environment variables
load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)

# Initialize storage paths and clients
GCS_BUCKET = os.getenv("GCS_BUCKET")
GOOGLE_CLOUD_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT")
LOCAL_DATA_PATH = os.getenv("LOCAL_DATA_PATH", "/tmp/data")  # Default to /tmp/data for GitHub Actions

# Use GCS if we have the required configuration
USE_GCS = bool(GCS_BUCKET and GOOGLE_CLOUD_PROJECT)

logger.info(f"GCS Configuration - Bucket: {GCS_BUCKET}, Project: {GOOGLE_CLOUD_PROJECT}, USE_GCS: {USE_GCS}")

# Initialize GCS client if bucket is configured
gcs_client = None
if USE_GCS:
    try:
        logger.info("Attempting to initialize GCS client...")
        gcs_client = storage.Client(project=GOOGLE_CLOUD_PROJECT)
        logger.info(f"Successfully initialized GCS client. Using GCS storage with bucket: {GCS_BUCKET}")
    except Exception as e:
        logger.error(f"Failed to initialize GCS client: {e}", exc_info=True)
        logger.info("Falling back to local storage")
        USE_GCS = False

if not USE_GCS:
    logger.info(f"Using local storage in {LOCAL_DATA_PATH}/bronze/")

# --- In-memory buffer for consolidated output ---
# Structure: { "buffer_key": { "json": [obj1, obj2], "xml": [str1, str2] } }
_data_buffer: Dict[str, Dict[str, List[Any]]] = {}

# Get timestamp for this export run - use shared timestamp from workflow if available
EXPORT_TIMESTAMP = os.getenv("BRONZE_EXPORT_TIMESTAMP") or datetime.utcnow().strftime("%Y%m%d_%H%M%S")
logger.info(
    f"Using export timestamp: {EXPORT_TIMESTAMP} (from {'environment' if os.getenv('BRONZE_EXPORT_TIMESTAMP') else 'current time'})"
)


def save_data_immediately(data_type: str, data: Any, identifier: str = "data") -> bool:
    """
    Save data immediately to GCS without buffering - fixes parallel processing issues.

    This replaces the broken save_raw_data + finalize_export pattern that doesn't work
    with parallel GitHub Actions jobs. Each job now saves its data immediately.

    Args:
        data_type: Type of data (e.g., 'vetstat_antibiotics', 'chr_dyr_movement_summaries')
        data: The data to save (dict, list, or string)
        identifier: Unique identifier for this data batch

    Returns:
        bool: True if save succeeded, False otherwise
    """
    try:
        if not USE_GCS:
            logger.warning("GCS not available - cannot save data immediately")
            return False

        # Determine file format based on data type
        if isinstance(data, str) and data.strip().startswith("<"):
            # XML data
            filename = f"{data_type}_{identifier}.xml"
            content = data
            content_type = "application/xml"
        else:
            # JSON data (dict, list, or already-serialized JSON string)
            filename = f"{data_type}_{identifier}.json"
            if isinstance(data, str):
                content = data  # Already serialized
            else:
                content = json.dumps(data, indent=2, default=str)
            content_type = "application/json"

        # Save directly to GCS
        bucket = gcs_client.bucket(GCS_BUCKET)
        blob_path = f"bronze/chr/{EXPORT_TIMESTAMP}/{filename}"
        blob = bucket.blob(blob_path)

        blob.upload_from_string(content, content_type=content_type)

        logger.info(f"✅ Saved {data_type} data immediately to gs://{GCS_BUCKET}/{blob_path}")
        return True

    except Exception as e:
        logger.error(f"❌ Failed to save {data_type} data immediately: {e}")
        return False


# --- Helper Functions ---


def _ensure_dir(filepath: Path):
    """Ensure the directory for the given filepath exists."""
    try:
        filepath.parent.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        logger.error(f"Error creating directory {filepath.parent}: {e}")
        raise


def _get_final_filename(data_source: str, operation: str, format: str) -> Path:
    """Generate the filename for the final consolidated file."""
    base_path = Path(f"{LOCAL_DATA_PATH}/bronze/{data_source}")
    # Sanitize operation name for filename
    safe_operation = operation.replace(" ", "_").replace("/", "_")
    filename = f"{safe_operation}.{format}"
    return base_path / filename


def _serialize_data(data: Any) -> Optional[str]:
    """Serialize Python/Zeep object to a JSON string."""
    if data is None:
        return None

    # Handle raw XML strings directly
    if isinstance(data, str):
        try:
            # Attempt to parse as JSON first, to ensure validity if it's supposed to be JSON
            json.loads(data)
            return data  # It's already a valid JSON string
        except json.JSONDecodeError:
            # If not JSON, assume it's XML or other raw string, wrap in a simple JSON structure
            return json.dumps({"raw_xml_string": data})

    def json_serializer(obj):
        """Custom JSON serializer for objects not serializable by default json code"""
        from datetime import date, datetime
        from decimal import Decimal

        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return str(obj)
        if hasattr(obj, "__dict__"):
            return obj.__dict__
        return str(obj)

    # Handle Zeep objects or other serializable Python objects
    try:
        serialized_obj = serialize_object(data, target_cls=dict)
        return json.dumps(serialized_obj, default=json_serializer)
    except TypeError as type_error:
        logger.error(f"TypeError during serialization: {type_error}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error during serialization: {e}")
        return None


def _save_to_gcs(blob_path: str, content: str, format_type: str):
    """Helper function to save content to GCS."""
    bucket = gcs_client.bucket(GCS_BUCKET)
    # Add bronze/chr/{timestamp} prefix to all files
    blob = bucket.blob(f"bronze/chr/{EXPORT_TIMESTAMP}/{blob_path}")

    # Set content type based on format
    content_type = "application/json" if format_type == "json" else "application/xml"
    blob.upload_from_string(content, content_type=content_type)


def _save_locally(filepath: Path, content: str, format_type: str):
    """Helper function to save content locally."""
    # Add timestamp to the path
    timestamped_path = filepath.parent / EXPORT_TIMESTAMP / filepath.name
    timestamped_path.parent.mkdir(parents=True, exist_ok=True)
    with open(timestamped_path, "w", encoding="utf-8") as f:
        f.write(content)


def save_raw_data(
    raw_response: Any,
    data_type: str,
    identifier: Optional[Union[str, Dict[str, Any]]] = None,
):
    """Buffer raw data for later consolidated export."""
    if raw_response is None:
        logger.warning(f"Received None for raw_response for data_type '{data_type}'. Skipping save.")
        return

    buffer_key = data_type

    if buffer_key not in _data_buffer:
        _data_buffer[buffer_key] = {"json": [], "xml": []}

    if isinstance(raw_response, str):
        _data_buffer[buffer_key]["xml"].append(raw_response)
    else:
        try:
            serialized_obj = serialize_object(raw_response, target_cls=dict)
            # Add timestamp to the data
            if isinstance(serialized_obj, dict):
                serialized_obj["_export_timestamp"] = EXPORT_TIMESTAMP
            _data_buffer[buffer_key]["json"].append(serialized_obj)
        except Exception as e:
            logger.error(f"Failed to serialize object for {buffer_key}: {e}")


def get_data_buffer() -> Dict[str, Dict[str, List[Any]]]:
    """Get a reference to the current data buffer."""
    return _data_buffer


def finalize_export(clear_buffer: bool = True):
    """Write buffered data to consolidated files with memory-efficient processing."""
    if not _data_buffer:
        logger.warning("No data buffered for export.")
        return

    storage_mode = "GCS (GitHub Actions)" if USE_GCS else "local filesystem"
    logger.info(f"Starting export using {storage_mode}")

    # Check if we're in a memory-constrained environment (like GitHub Actions)
    is_memory_constrained = os.getenv("GITHUB_ACTIONS") == "true" or os.getenv("MEMORY_CONSTRAINED") == "true"

    if is_memory_constrained:
        logger.info("Memory-constrained environment detected - using streaming export")

    total_files = 0
    for buffer_key, format_data in _data_buffer.items():
        data_type = buffer_key

        # Process JSON data with memory-efficient streaming
        json_data_list = format_data.get("json", [])
        if json_data_list:
            filename = f"{data_type}.json"

            # Log data size before processing
            data_count = len(json_data_list)
            logger.info(f"Processing {data_count} records for {filename}")

            # Use streaming approach for large datasets
            if is_memory_constrained and data_count > 1000:
                logger.info(f"Using streaming export for large dataset: {filename}")
                if USE_GCS:
                    try:
                        _save_to_gcs_streaming(filename, json_data_list)
                        total_files += 1
                    except Exception as e:
                        logger.error(f"Error in streaming GCS export for {filename}: {e}")
                else:
                    filepath = Path(f"{LOCAL_DATA_PATH}/bronze/chr/{filename}")
                    try:
                        _save_locally_streaming(filepath, json_data_list)
                        total_files += 1
                    except Exception as e:
                        logger.error(f"Error in streaming local export for {filename}: {e}")
            else:
                # Use original approach for smaller datasets
                if USE_GCS:
                    try:
                        logger.info(f"Writing {data_count} records to GCS bucket '{GCS_BUCKET}': {filename}")
                        json_content = json.dumps(json_data_list, indent=2, default=str)
                        _save_to_gcs(filename, json_content, "json")
                        total_files += 1
                    except Exception as e:
                        logger.error(f"Error writing JSON to GCS {filename}: {e}")
                else:
                    filepath = Path(f"{LOCAL_DATA_PATH}/bronze/chr/{filename}")
                    try:
                        logger.info(f"Writing {data_count} records locally to {filepath}")
                        _save_locally(filepath, json.dumps(json_data_list, indent=2, default=str), "json")
                        total_files += 1
                    except Exception as e:
                        logger.error(f"Error writing JSON file {filepath}: {e}")

        # Process XML data (unchanged - typically smaller)
        xml_data_list = format_data.get("xml", [])
        if xml_data_list:
            filename = f"{data_type}.xml"
            full_xml_content = "\n<!-- RAW_RESPONSE_SEPARATOR -->\n".join(xml_data_list)

            if USE_GCS:
                try:
                    logger.info(f"Writing {len(xml_data_list)} records to GCS bucket '{GCS_BUCKET}': {filename}")
                    _save_to_gcs(filename, full_xml_content, "xml")
                    total_files += 1
                except Exception as e:
                    logger.error(f"Error writing XML to GCS {filename}: {e}")
            else:
                filepath = Path(f"{LOCAL_DATA_PATH}/bronze/chr/{filename}")
                try:
                    logger.info(f"Writing {len(xml_data_list)} records locally to {filepath}")
                    _save_locally(filepath, full_xml_content, "xml")
                    total_files += 1
                except Exception as e:
                    logger.error(f"Error writing XML file {filepath}: {e}")

    logger.info(f"Export complete: {total_files} files written using {storage_mode} in bronze/chr/{EXPORT_TIMESTAMP}/")
    if clear_buffer:
        _data_buffer.clear()

    # CRITICAL: Comprehensive cleanup after export
    try:
        # Force garbage collection to free memory from large datasets
        import gc

        gc.collect()

        # Clean up any orphaned temporary files
        if os.getenv("GITHUB_ACTIONS") == "true" or os.getenv("MEMORY_CONSTRAINED") == "true":
            import glob

            temp_files = glob.glob("/tmp/chr_streaming_*")
            for temp_file in temp_files:
                try:
                    if os.path.exists(temp_file):
                        os.unlink(temp_file)
                        logger.debug(f"Cleaned up orphaned temp file: {temp_file}")
                except Exception as e:
                    logger.debug(f"Could not remove temp file {temp_file}: {e}")

        logger.info("Completed post-export cleanup")

    except Exception as e:
        logger.warning(f"Error during post-export cleanup: {e}")


def _save_to_gcs_streaming(filename: str, data_list: List[Any], append_mode: bool = False):
    """Save large datasets to GCS using streaming to avoid memory issues.
    
    Args:
        filename: Target filename in GCS
        data_list: List of data items to save
        append_mode: If True, append to existing file using JSONL format
    """
    bucket = gcs_client.bucket(GCS_BUCKET)
    blob = bucket.blob(f"bronze/chr/{EXPORT_TIMESTAMP}/{filename}")

    # Estimate data size for logging
    import sys

    estimated_size_mb = sys.getsizeof(data_list) / (1024 * 1024)
    mode_str = "append" if append_mode else "overwrite"
    logger.info(
        f"Starting streaming {mode_str} to GCS for {filename} ({len(data_list)} records, ~{estimated_size_mb:.1f}MB)"
    )

    try:
        if append_mode:
            # FIXED: Use JSONL format for easy appending (one JSON object per line)
            with blob.open("a", content_type="application/x-jsonlines") as f:
                for i, item in enumerate(data_list):
                    # Write each item as a separate line (JSONL format)
                    json.dump(item, f, default=str)
                    f.write("\n")

                    # Log progress more frequently for very large datasets
                    if i > 0 and i % 1000 == 0:
                        logger.info(f"Appended {i}/{len(data_list)} records to GCS ({(i / len(data_list) * 100):.1f}%)")
        else:
            # Original JSON array format for non-append mode
            with blob.open("w", content_type="application/json") as f:
                f.write("[\n")

                for i, item in enumerate(data_list):
                    if i > 0:
                        f.write(",\n")

                    # Serialize one item at a time to avoid memory buildup
                    json.dump(item, f, indent=2, default=str)

                    # Log progress more frequently for very large datasets
                    if i > 0 and i % 1000 == 0:
                        logger.info(f"Streamed {i}/{len(data_list)} records to GCS ({(i / len(data_list) * 100):.1f}%)")

                f.write("\n]")

        logger.info(f"Completed streaming upload to GCS for {filename}")

        # CRITICAL: Clear the data_list immediately after successful upload to free memory
        data_list.clear()

        # Force garbage collection after large uploads
        if estimated_size_mb > 10:  # For uploads > 10MB
            import gc

            gc.collect()
            logger.debug(f"Forced garbage collection after {estimated_size_mb:.1f}MB upload")

    except Exception as e:
        logger.error(f"Error during streaming upload to GCS for {filename}: {e}")
        raise


def _save_locally_streaming(filepath: Path, data_list: List[Any]):
    """Save large datasets locally using streaming to avoid memory issues."""
    # Add timestamp to the path
    timestamped_path = filepath.parent / EXPORT_TIMESTAMP / filepath.name
    timestamped_path.parent.mkdir(parents=True, exist_ok=True)

    # Estimate data size for logging
    import sys

    estimated_size_mb = sys.getsizeof(data_list) / (1024 * 1024)
    logger.info(
        f"Starting streaming write to {timestamped_path} ({len(data_list)} records, ~{estimated_size_mb:.1f}MB)"
    )

    try:
        with open(timestamped_path, "w", encoding="utf-8") as f:
            f.write("[\n")

            for i, item in enumerate(data_list):
                if i > 0:
                    f.write(",\n")

                # Serialize one item at a time to avoid memory buildup
                json.dump(item, f, indent=2, default=str)

                # Log progress more frequently for very large datasets
                if i > 0 and i % 1000 == 0:
                    logger.info(f"Streamed {i}/{len(data_list)} records to file ({(i / len(data_list) * 100):.1f}%)")

            f.write("\n]")

        logger.info(f"Completed streaming write to {timestamped_path}")

        # CRITICAL: Clear the data_list immediately after successful write to free memory
        data_list.clear()

        # Force garbage collection after large writes
        if estimated_size_mb > 10:  # For writes > 10MB
            import gc

            gc.collect()
            logger.debug(f"Forced garbage collection after {estimated_size_mb:.1f}MB write")

    except Exception as e:
        logger.error(f"Error during streaming write to {timestamped_path}: {e}")
        raise


# --- Cleanup Function (Optional) ---
def clear_buffer():
    """Explicitly clears the data buffer if needed before finalization."""
    _data_buffer.clear()
    logger.info("Data buffer explicitly cleared.")


# --- Test Execution ---
if __name__ == "__main__":
    logger.info("--- Starting Export Test --- ")

    # Example Usage (Buffering)
    test_data_1 = {"id": 1, "name": "Test A", "timestamp": datetime.now()}
    test_data_2 = {"id": 2, "name": "Test B", "items": [1, 2, 3]}
    test_data_3 = {"id": 3, "name": "Test C"}
    test_xml_1 = "<root1><item>Value 1</item></root1>"
    test_xml_2 = "<root2><item>Value 2</item></root2>"

    save_raw_data(test_data_1, "test_source", "op_json", {"key": "val1"})
    save_raw_data(test_data_2, "test_source", "op_json", "id_123")
    save_raw_data(test_xml_1, "test_source", "op_xml")
    save_raw_data(test_xml_2, "test_source", "op_xml")
    save_raw_data(test_data_3, "another_source", "other_op_json", {"run": 5})

    logger.info(f"Data buffered. Buffer keys: {list(_data_buffer.keys())}")
    if "test_source_op_json" in _data_buffer:
        logger.info(f"Buffer JSON count for 'test_source_op_json': {len(_data_buffer['test_source_op_json']['json'])}")
    if "test_source_op_xml" in _data_buffer:
        logger.info(f"Buffer XML count for 'test_source_op_xml': {len(_data_buffer['test_source_op_xml']['xml'])}")

    # Finalize the export
    finalize_export()

    # Verify buffer is cleared
    if not _data_buffer:
        logger.info("Buffer successfully cleared after finalize_export.")
    else:
        logger.error("Buffer was not cleared after finalize_export.")

    logger.info("--- Export Test Complete --- ")

# --- Example Usage (for illustration - requires integration) ---
# (Keep example usage commented out)
# if __name__ == '__main__':
#     # Example with a dictionary (simulating serialized Zeep)
#     sample_zeep_like = {'key': 'value', 'nested': {'num': 123}}
#     save_raw_data(sample_zeep_like, 'stamdata', 'ListDyrearterMedBrugsarter')

#     # Example with an XML string
#     sample_xml = '<root><item>TestData</item></root>'
#     save_raw_data(sample_xml, 'vetstat', 'hentAntibiotikaforbrug', {'chr': 99999})

#     # Example with identifiers
#     save_raw_data(sample_zeep_like, 'besaetning', 'hentStamoplysninger', {'herd': 5392, 'species': 15})


def export_context_data(context: Dict[str, Any], export_path: Path):
    """Export pipeline context data for use by dependent jobs."""
    logger.info("Exporting pipeline context data...")

    # Extract serializable context data
    context_data = {
        "export_timestamp": EXPORT_TIMESTAMP,
        "herd_to_species": context.get("herd_to_species", {}),
        "chr_to_species": {},  # Convert sets to lists for JSON serialization
        "combinations": context.get("combinations", []),
        "args": {
            # Preserve all args, converting dates to strings for JSON serialization
            **{k: (str(v) if k in ["start_date", "end_date"] else v) for k, v in context.get("args", {}).items()},
        },
    }

    # Convert chr_to_species sets to lists for JSON serialization
    chr_to_species_raw = context.get("chr_to_species", {})
    for chr_num, species_set in chr_to_species_raw.items():
        if isinstance(species_set, set):
            context_data["chr_to_species"][str(chr_num)] = list(species_set)
        else:
            context_data["chr_to_species"][str(chr_num)] = species_set

    # Save context data
    export_path.parent.mkdir(parents=True, exist_ok=True)
    with open(export_path, "w") as f:
        json.dump(context_data, f, indent=2, default=str)

    logger.info(f"Context data exported to {export_path}")
    logger.info(f"Exported {len(context_data['herd_to_species'])} herd mappings")
    logger.info(f"Exported {len(context_data['chr_to_species'])} CHR mappings")


def import_context_data(import_path: Path) -> Dict[str, Any]:
    """Import pipeline context data from a previous job."""
    if not import_path.exists():
        logger.warning(f"Context data file not found: {import_path}")
        return {}

    logger.info(f"Importing pipeline context data from {import_path}")

    try:
        with open(import_path, "r") as f:
            context_data = json.load(f)

        # Convert chr_to_species lists back to sets
        chr_to_species = {}
        for chr_num, species_list in context_data.get("chr_to_species", {}).items():
            chr_to_species[int(chr_num)] = set(species_list)

        # Convert herd_to_species string keys back to integers
        herd_to_species = {}
        for herd_num, species_code in context_data.get("herd_to_species", {}).items():
            herd_to_species[int(herd_num)] = species_code

        # Convert date strings back to date objects for imported args
        imported_args = context_data.get("args", {}).copy()
        for date_key in ["start_date", "end_date"]:
            if date_key in imported_args and isinstance(imported_args[date_key], str) and imported_args[date_key]:
                try:
                    from datetime import datetime

                    imported_args[date_key] = datetime.strptime(imported_args[date_key], "%Y-%m-%d").date()
                except ValueError:
                    logger.warning(f"Failed to parse {date_key}: {imported_args[date_key]}")
                    imported_args[date_key] = None

        imported_context = {
            "herd_to_species": herd_to_species,
            "chr_to_species": chr_to_species,
            "combinations": context_data.get("combinations", []),
            "export_timestamp": context_data.get("export_timestamp", ""),
            "args": imported_args,
        }

        logger.info(f"Imported {len(imported_context['herd_to_species'])} herd mappings")
        logger.info(f"Imported {len(imported_context['chr_to_species'])} CHR mappings")

        return imported_context

    except Exception as e:
        logger.error(f"Failed to import context data: {e}", exc_info=True)
        return {}


def export_bronze_data_summary() -> Dict[str, Any]:
    """Export a summary of bronze data for verification."""
    summary = {"export_timestamp": EXPORT_TIMESTAMP, "buffer_keys": list(_data_buffer.keys()), "data_counts": {}}

    for key, data in _data_buffer.items():
        summary["data_counts"][key] = {
            "json_records": len(data.get("json", [])),
            "xml_records": len(data.get("xml", [])),
        }

    return summary
