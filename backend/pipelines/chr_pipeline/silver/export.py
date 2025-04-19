import logging
from pathlib import Path
import pandas as pd
import geopandas as gpd
import tempfile
from google.cloud import storage
import os
from dotenv import load_dotenv
import gcsfs

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

def _save_to_gcs(filepath: Path, df: pd.DataFrame, is_geo: bool = False):
    """Save DataFrame to GCS using gcsfs.
    
    Args:
        filepath: Path object representing the file path
        df: DataFrame to save
        is_geo: Whether the DataFrame is a GeoDataFrame
    
    Returns:
        str: The GCS path where the file was saved
    """
    # Add silver/chr/{timestamp} prefix
    blob_path = f"gs://{GCS_BUCKET}/silver/chr/{filepath.parent.name}/{filepath.name}"
    logging.info(f"Attempting to save to GCS path: {blob_path}")
    
    try:
        if is_geo:
            if not isinstance(df, gpd.GeoDataFrame):
                raise ValueError("DataFrame must be a GeoDataFrame when is_geo=True")
            # Use gcsfs directly with to_parquet
            df.to_parquet(blob_path, filesystem=gcs_fs)
        else:
            if not isinstance(df, pd.DataFrame):
                raise ValueError("DataFrame must be a pandas DataFrame when is_geo=False")
            # Use gcsfs directly with to_parquet
            df.to_parquet(blob_path, filesystem=gcs_fs)
        
        logging.info(f"Successfully saved to GCS: {blob_path}")
        return blob_path
    except Exception as e:
        logging.error(f"Error saving to GCS using gcsfs: {e}")
        raise

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
    logging.info(f"Saving locally to: {filepath}")
    
    try:
        if is_geo:
            if not isinstance(df, gpd.GeoDataFrame):
                raise ValueError("DataFrame must be a GeoDataFrame when is_geo=True")
            df.to_parquet(filepath)
        else:
            if not isinstance(df, pd.DataFrame):
                raise ValueError("DataFrame must be a pandas DataFrame when is_geo=False")
            df.to_parquet(filepath)
        
        logging.info(f"Successfully saved locally: {filepath}")
        return str(filepath)
    except Exception as e:
        logging.error(f"Error saving locally: {e}")
        raise

def save_table(filepath: Path, df: pd.DataFrame, is_geo: bool = False) -> str:
    """Save a table to either GCS or local storage.
    
    Args:
        filepath: Path object representing the file path
        df: DataFrame to save
        is_geo: Whether the DataFrame is a GeoDataFrame
    
    Returns:
        str: The path where the file was saved (GCS or local)
    """
    if df is None or (isinstance(df, (pd.DataFrame, gpd.GeoDataFrame)) and df.empty):
        logging.warning("Attempted to save empty DataFrame - skipping save")
        return None
        
    if USE_GCS:
        try:
            return _save_to_gcs(filepath, df, is_geo)
        except Exception as e:
            logging.error(f"Error saving to GCS: {e}")
            logging.warning("Falling back to local storage")
            return _save_locally(filepath, df, is_geo)
    else:
        return _save_locally(filepath, df, is_geo) 