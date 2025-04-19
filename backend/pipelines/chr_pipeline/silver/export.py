import logging
from pathlib import Path
import pandas as pd
import geopandas as gpd
import tempfile
from google.cloud import storage
import os
from dotenv import load_dotenv
import gcsfs
import shutil
from typing import Optional

# Load environment variables
load_dotenv()

# Initialize storage paths and clients
GCS_BUCKET = os.getenv('GCS_BUCKET')
GOOGLE_CLOUD_PROJECT = os.getenv('GOOGLE_CLOUD_PROJECT')

# DEBUG: Log retrieved environment variables
logging.info(f"Retrieved GCS_BUCKET: '{GCS_BUCKET}'")
logging.info(f"Retrieved GOOGLE_CLOUD_PROJECT: '{GOOGLE_CLOUD_PROJECT}'")

# Use GCS if we have the required configuration
USE_GCS = bool(GCS_BUCKET and GOOGLE_CLOUD_PROJECT)

# DEBUG: Log USE_GCS decision
logging.info(f"USE_GCS determined as: {USE_GCS}")

# Initialize GCS client and filesystem if bucket is configured
gcs_client = None
gcs_fs = None
if USE_GCS:
    try:
        gcs_client = storage.Client(project=GOOGLE_CLOUD_PROJECT)
        gcs_fs = gcsfs.GCSFileSystem(project=GOOGLE_CLOUD_PROJECT)
        logging.info(f"Using GCS storage with bucket: {GCS_BUCKET}")
    except Exception as e:
        logging.error(f"Failed to initialize GCS client/filesystem: {e}")
        logging.info("Falling back to local storage")
        USE_GCS = False

if not USE_GCS:
    logging.info("Using local storage in /data/silver/")

def _convert_uuid_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Convert UUID columns to strings for parquet compatibility."""
    df = df.copy()  # Create a copy to avoid modifying the original
    for col in df.columns:
        if df[col].dtype == 'object':  # Check if column might contain UUIDs
            # Get first non-null value
            first_value = df[col].dropna().iloc[0] if not df[col].isna().all() else None
            if first_value is not None and hasattr(first_value, 'hex'):  # UUID objects have hex attribute
                # Convert UUIDs to hex strings
                df[col] = df[col].apply(lambda x: x.hex if x is not None and hasattr(x, 'hex') else x)
    return df

def _save_to_gcs(filepath: Path, df: pd.DataFrame, is_geo: bool = False) -> Optional[Path]:
    """Save DataFrame to GCS."""
    try:
        # Convert UUIDs to strings
        df = _convert_uuid_columns(df)
        
        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir) / filepath.name
            
            # Save to temporary file
            if is_geo:
                df.to_parquet(temp_path, index=False, engine='pyarrow')
            else:
                df.to_parquet(temp_path, index=False, engine='pyarrow')
            
            # Copy to final location
            os.makedirs(filepath.parent, exist_ok=True)
            shutil.copy2(temp_path, filepath)
            
            return filepath
    except Exception as e:
        logging.error(f"Error saving to GCS: {e}")
        return None

def _save_locally(filepath: Path, df: pd.DataFrame, is_geo: bool = False) -> Optional[Path]:
    """Save DataFrame locally."""
    try:
        # Convert UUIDs to strings
        df = _convert_uuid_columns(df)
        
        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir) / filepath.name
            
            # Save to temporary file
            if is_geo:
                df.to_parquet(temp_path, index=False, engine='pyarrow')
            else:
                df.to_parquet(temp_path, index=False, engine='pyarrow')
            
            # Copy to final location
            os.makedirs(filepath.parent, exist_ok=True)
            shutil.copy2(temp_path, filepath)
            
            return filepath
    except Exception as e:
        logging.error(f"Error saving locally: {e}")
        return None

def save_table(filepath: Path, df: pd.DataFrame, is_geo: bool = False) -> Optional[Path]:
    """Save a DataFrame to parquet, first attempting GCS then falling back to local storage."""
    try:
        # Try saving to GCS first
        saved_path = _save_to_gcs(filepath, df, is_geo)
        if saved_path is not None:
            return saved_path
            
        # If GCS fails, fall back to local storage
        logging.warning("Falling back to local storage")
        return _save_locally(filepath, df, is_geo)
        
    except Exception as e:
        logging.error(f"Failed to save table: {e}")
        return None 