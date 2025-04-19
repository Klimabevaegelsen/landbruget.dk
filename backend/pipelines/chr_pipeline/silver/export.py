import logging
from pathlib import Path
import pandas as pd
import tempfile
from google.cloud import storage
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize storage paths and clients
GCS_BUCKET = os.getenv('GCS_BUCKET')
GOOGLE_CLOUD_PROJECT = os.getenv('GOOGLE_CLOUD_PROJECT')

# DEBUG: Log retrieved environment variables
logging.debug(f"Retrieved GCS_BUCKET: '{GCS_BUCKET}'")
logging.debug(f"Retrieved GOOGLE_CLOUD_PROJECT: '{GOOGLE_CLOUD_PROJECT}'")

# Use GCS if we have the required configuration
USE_GCS = bool(GCS_BUCKET and GOOGLE_CLOUD_PROJECT)

# DEBUG: Log USE_GCS decision
logging.debug(f"USE_GCS determined as: {USE_GCS}")

# Initialize GCS client if bucket is configured
gcs_client = None
if USE_GCS:
    try:
        gcs_client = storage.Client(project=GOOGLE_CLOUD_PROJECT)
        logging.info(f"Using GCS storage with bucket: {GCS_BUCKET}")
    except Exception as e:
        logging.error(f"Failed to initialize GCS client: {e}")
        logging.info("Falling back to local storage")
        USE_GCS = False

if not USE_GCS:
    logging.info("Using local storage in /data/silver/")

def _save_to_gcs(filepath: Path, df: pd.DataFrame, is_geo: bool = False):
    """Save DataFrame to GCS.
    
    Args:
        filepath: Path object representing the file path
        df: DataFrame to save
        is_geo: Whether the DataFrame is a GeoDataFrame
    
    Returns:
        str: The GCS path where the file was saved
    """
    bucket = gcs_client.bucket(GCS_BUCKET)
    # Add silver/chr/{timestamp} prefix
    blob_path = f"silver/chr/{filepath.parent.name}/{filepath.name}"
    blob = bucket.blob(blob_path)
    
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=True) as tmp:
        if is_geo:
            df.to_parquet(tmp.name)
        else:
            df.to_parquet(tmp.name)
        blob.upload_from_filename(tmp.name)
    
    return f"gs://{GCS_BUCKET}/{blob_path}"

def _save_locally(filepath: Path, df: pd.DataFrame, is_geo: bool = False):
    """Save DataFrame locally.
    
    Args:
        filepath: Path object representing the file path
        df: DataFrame to save
        is_geo: Whether the DataFrame is a GeoDataFrame
    
    Returns:
        str: The local path where the file was saved
    """
    filepath.parent.mkdir(parents=True, exist_ok=True)
    if is_geo:
        df.to_parquet(filepath)
    else:
        df.to_parquet(filepath)
    return str(filepath)

def save_table(filepath: Path, df: pd.DataFrame, is_geo: bool = False) -> str:
    """Save a table to either GCS or local storage.
    
    Args:
        filepath: Path object representing the file path
        df: DataFrame to save
        is_geo: Whether the DataFrame is a GeoDataFrame
    
    Returns:
        str: The path where the file was saved (GCS or local)
    """
    if USE_GCS:
        try:
            return _save_to_gcs(filepath, df, is_geo)
        except Exception as e:
            logging.error(f"Error saving to GCS: {e}")
            logging.warning("Falling back to local storage")
            return _save_locally(filepath, df, is_geo)
    else:
        return _save_locally(filepath, df, is_geo) 