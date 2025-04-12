"""Module for exporting raw Bronze data (JSON/XML)."""

import logging
import json
import os
from pathlib import Path
from typing import Any, Optional, Dict, List, Union
from datetime import datetime
from zeep.helpers import serialize_object

from dotenv import load_dotenv
from google.cloud import storage
from google.api_core.exceptions import GoogleAPICallError

# Load environment variables
load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)

# Initialize storage paths and clients
BRONZE_DATA_PATH = os.getenv('BRONZE_DATA_PATH', './data/bronze')
IS_GCS_PATH = BRONZE_DATA_PATH.startswith('gs://')

# Initialize GCS client and paths if using GCS
gcs_client = None
gcs_bucket_name = os.getenv('GCS_BUCKET')  # Get from environment
gcs_prefix = None

if IS_GCS_PATH:
    try:
        gcs_client = storage.Client(project=os.getenv('GOOGLE_CLOUD_PROJECT'))
        path_parts = BRONZE_DATA_PATH[5:].split('/', 1)
        gcs_bucket_name = gcs_bucket_name or path_parts[0]  # Use env var or fallback to path
        gcs_prefix = path_parts[1].strip('/') if len(path_parts) > 1 else ''
        logger.info(f"Initialized GCS client for bucket: {gcs_bucket_name}, prefix: {gcs_prefix}")
    except Exception as e:
        logger.error(f"Failed to initialize GCS client: {e}", exc_info=True)
        raise

# --- In-memory buffer for consolidated output ---
# Structure: { "buffer_key": { "json": [obj1, obj2], "xml": [str1, str2] } }
_data_buffer: Dict[str, Dict[str, List[Any]]] = {}

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
    base_path = Path(BRONZE_DATA_PATH)
    # Sanitize operation name for filename
    safe_operation = operation.replace(" ", "_").replace("/", "_")
    filename = f"{data_source}_{safe_operation}.{format}"
    return base_path / data_source / filename

def _serialize_data(data: Any) -> Optional[str]:
    """Serialize Python/Zeep object to a JSON string."""
    if data is None:
        return None

    # Handle raw XML strings directly
    if isinstance(data, str):
        try:
            # Attempt to parse as JSON first, to ensure validity if it's supposed to be JSON
            json.loads(data)
            return data # It's already a valid JSON string
        except json.JSONDecodeError:
             # If not JSON, assume it's XML or other raw string, wrap in a simple JSON structure
             return json.dumps({"raw_xml_string": data})

    # Handle Zeep objects or other serializable Python objects
    try:
        serialized_obj = serialize_object(data, target_cls=dict)
        # Ensure datetime objects are handled correctly
        return json.dumps(serialized_obj, default=str)
    except (TypeError, Exception) as e: # Use generic Exception
        # Fallback for complex types or potential Zeep errors during serialization
        logger.warning(f"Could not serialize object of type {type(data)} directly: {e}. Falling back to repr().")
        try:
            # As a last resort, try to get a string representation
             # Wrap the repr in a JSON structure for consistency
             return json.dumps({"fallback_repr": repr(data)})
        except Exception as repr_e:
            logger.error(f"Failed even during fallback repr serialization: {repr_e}")
            return None

def _save_to_gcs(blob_path: str, content: str, format_type: str):
    """Helper function to save content to GCS."""
    bucket = gcs_client.bucket(gcs_bucket_name)
    blob = bucket.blob(blob_path)
    
    # Set content type based on format
    content_type = 'application/json' if format_type == 'json' else 'application/xml'
    blob.upload_from_string(content, content_type=content_type)

def _save_locally(filepath: Path, content: str, format_type: str):
    """Helper function to save content locally."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)

def save_raw_data(
    # Renamed arguments for clarity
    raw_response: Any,
    data_type: str,
    identifier: Optional[Union[str, Dict[str, Any]]] = None,
    # operation: str, # No longer needed if data_type is specific enough
    # data_source: str, # No longer needed
):
    """
    Detects data type (XML string vs other) and appends the raw data object
    or string to an in-memory buffer for later writing to consolidated files.

    Args:
        raw_response: The raw data object (e.g., Zeep object, dict) or XML string.
        data_type: A specific identifier for the type of data (e.g., 'stamdata_list',
                   'besaetning_details', 'vetstat_antibiotics'). Used as buffer key.
        identifier: Contextual identifier (e.g., species_usage, herd_species) for logging/metadata.
    """
    if raw_response is None:
        logger.warning(f"Received None for raw_response for data_type '{data_type}'. Skipping save.")
        return

    # Use data_type directly as the buffer key
    buffer_key = data_type

    # Initialize buffer for this key if it doesn't exist
    if buffer_key not in _data_buffer:
        _data_buffer[buffer_key] = {"json": [], "xml": []}

    # Determine format and append to the correct list
    if isinstance(raw_response, str):
        # Assume it's a raw XML string (or already JSON string)
        _data_buffer[buffer_key]["xml"].append(raw_response)
        # logger.debug(f"Buffered XML/String data for {buffer_key}. Identifier: {identifier}. Count: {len(_data_buffer[buffer_key]['xml'])}")
    else:
        # Assume it's a Zeep object or other Python object to be JSON serialized
        try:
            # Perform serialization here to catch errors early
            # We store the *serialized object* (dict/list) in the buffer
            serialized_obj = serialize_object(raw_response, target_cls=dict)
            _data_buffer[buffer_key]["json"].append(serialized_obj)
            # logger.debug(f"Buffered JSON data for {buffer_key}. Identifier: {identifier}. Count: {len(_data_buffer[buffer_key]['json'])}")
        except Exception as e:
             logger.error(f"Failed to serialize object for {buffer_key} (Identifier: {identifier}) to JSON: {e}. Skipping.", exc_info=False)

# --- Final Export Function ---

def finalize_export():
    """
    Writes the buffered data to consolidated JSON or XML files.
    - JSON data is saved as a JSON array `[...]`.
    - XML data is saved as concatenated strings separated by a comment.
    """
    logger.info(f"Finalizing export. Writing buffered data to {BRONZE_DATA_PATH}...")
    if not _data_buffer:
        logger.warning("No data buffered for export.")
        return

    total_files = 0
    for buffer_key, format_data in _data_buffer.items():
        # Use the buffer_key (which is now data_type) for filename
        data_type = buffer_key

        # --- Process JSON data ---
        json_data_list = format_data.get("json", [])
        if json_data_list:
            filename = f"{data_type}.json"
            if IS_GCS_PATH:
                # Construct GCS path by combining prefix (if any) with filename
                blob_path = f"{gcs_prefix}/{filename}" if gcs_prefix else filename
                try:
                    logger.info(f"Writing {len(json_data_list)} JSON objects to GCS: gs://{gcs_bucket_name}/{blob_path}")
                    json_content = json.dumps(json_data_list, indent=2, default=str)
                    _save_to_gcs(blob_path, json_content, 'json')
                    total_files += 1
                    logger.info(f"Successfully wrote to GCS: gs://{gcs_bucket_name}/{blob_path}")
                except Exception as e:
                    logger.error(f"Error writing JSON to GCS {blob_path}: {e}", exc_info=True)
            else:
                filepath = Path(BRONZE_DATA_PATH) / filename
                try:
                    logger.info(f"Writing {len(json_data_list)} JSON objects locally to {filepath}")
                    _save_locally(filepath, json.dumps(json_data_list, indent=2, default=str), 'json')
                    total_files += 1
                    logger.info(f"Successfully wrote {filepath}")
                except Exception as e:
                    logger.error(f"Error writing JSON file {filepath}: {e}", exc_info=True)

        # --- Process XML data ---
        xml_data_list = format_data.get("xml", [])
        if xml_data_list:
            filename = f"{data_type}.xml"
            # Concatenate XML strings with a separator
            full_xml_content = "\n<!-- RAW_RESPONSE_SEPARATOR -->\n".join(xml_data_list)
            
            if IS_GCS_PATH:
                # Construct GCS path by combining prefix (if any) with filename
                blob_path = f"{gcs_prefix}/{filename}" if gcs_prefix else filename
                try:
                    logger.info(f"Writing {len(xml_data_list)} XML/String responses to GCS: gs://{gcs_bucket_name}/{blob_path}")
                    _save_to_gcs(blob_path, full_xml_content, 'xml')
                    total_files += 1
                    logger.info(f"Successfully wrote to GCS: gs://{gcs_bucket_name}/{blob_path}")
                except Exception as e:
                    logger.error(f"Error writing XML to GCS {blob_path}: {e}", exc_info=True)
            else:
                filepath = Path(BRONZE_DATA_PATH) / filename
                try:
                    logger.info(f"Writing {len(xml_data_list)} XML/String responses locally to {filepath}")
                    _save_locally(filepath, full_xml_content, 'xml')
                    total_files += 1
                    logger.info(f"Successfully wrote {filepath}")
                except Exception as e:
                    logger.error(f"Error writing XML file {filepath}: {e}", exc_info=True)

    logger.info(f"Final export complete. Wrote {total_files} consolidated files.")
    # Clear the buffer after writing
    _data_buffer.clear()
    logger.info("Data buffer cleared.")

# --- Cleanup Function (Optional) ---
def clear_buffer():
    """Explicitly clears the data buffer if needed before finalization."""
    _data_buffer.clear()
    logger.info("Data buffer explicitly cleared.")

# --- Test Execution ---
if __name__ == '__main__':
    logger.info("--- Starting Export Test --- ")

    # Example Usage (Buffering)
    test_data_1 = {'id': 1, 'name': 'Test A', 'timestamp': datetime.now()}
    test_data_2 = {'id': 2, 'name': 'Test B', 'items': [1, 2, 3]}
    test_data_3 = {'id': 3, 'name': 'Test C'}
    test_xml_1 = '<root1><item>Value 1</item></root1>'
    test_xml_2 = '<root2><item>Value 2</item></root2>'

    save_raw_data(test_data_1, 'test_source', 'op_json', {'key': 'val1'})
    save_raw_data(test_data_2, 'test_source', 'op_json', 'id_123')
    save_raw_data(test_xml_1, 'test_source', 'op_xml')
    save_raw_data(test_xml_2, 'test_source', 'op_xml')
    save_raw_data(test_data_3, 'another_source', 'other_op_json', {'run': 5})

    logger.info(f"Data buffered. Buffer keys: {list(_data_buffer.keys())}")
    if 'test_source_op_json' in _data_buffer:
         logger.info(f"Buffer JSON count for 'test_source_op_json': {len(_data_buffer['test_source_op_json']['json'])}")
    if 'test_source_op_xml' in _data_buffer:
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