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
        logger.warning(f"TypeError during serialization: {type_error}. Attempting fallback serialization.")
        try:
            # Try direct JSON serialization with our custom serializer
            return json.dumps(data, default=json_serializer)
        except TypeError as direct_error:
            logger.error(f"Direct serialization failed: {direct_error}")
            # As a last resort, try to get a string representation
            return json.dumps({"fallback_repr": repr(data)})
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
    """Write buffered data to consolidated files."""
    if not _data_buffer:
        logger.warning("No data buffered for export.")
        return

    storage_mode = "GCS (GitHub Actions)" if USE_GCS else "local filesystem"
    logger.info(f"Starting export using {storage_mode}")

    total_files = 0
    for buffer_key, format_data in _data_buffer.items():
        data_type = buffer_key

        # Process JSON data
        json_data_list = format_data.get("json", [])
        if json_data_list:
            filename = f"{data_type}.json"
            if USE_GCS:
                try:
                    logger.info(f"Writing {len(json_data_list)} records to GCS bucket '{GCS_BUCKET}': {filename}")
                    json_content = json.dumps(json_data_list, indent=2, default=str)
                    _save_to_gcs(filename, json_content, "json")
                    total_files += 1
                except Exception as e:
                    logger.error(f"Error writing JSON to GCS {filename}: {e}")
            else:
                filepath = Path(f"{LOCAL_DATA_PATH}/bronze/chr/{filename}")
                try:
                    logger.info(f"Writing {len(json_data_list)} records locally to {filepath}")
                    _save_locally(filepath, json.dumps(json_data_list, indent=2, default=str), "json")
                    total_files += 1
                except Exception as e:
                    logger.error(f"Error writing JSON file {filepath}: {e}")

        # Process XML data
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
