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
    logging.info(f"DataFrame type: {type(df)}")
    
    try:
        # First save to a temporary local file
        with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as tmp:
            temp_path = Path(tmp.name)
            logging.info(f"Saving to temporary file first: {temp_path}")
            
            if is_geo:
                if not isinstance(df, gpd.GeoDataFrame):
                    raise ValueError("DataFrame must be a GeoDataFrame when is_geo=True")
                if 'geometry' not in df.columns:
                    raise ValueError("GeoDataFrame must have a 'geometry' column")
                logging.info(f"Saving GeoDataFrame with CRS: {df.crs}")
                logging.info(f"GeoDataFrame columns: {df.columns.tolist()}")
                df.to_parquet(temp_path, index=False)
            else:
                if not isinstance(df, pd.DataFrame):
                    raise ValueError("DataFrame must be a pandas DataFrame when is_geo=False")
                df.to_parquet(temp_path, index=False)
            
            # Verify the temporary file was created
            if not temp_path.exists():
                raise FileNotFoundError(f"Failed to create temporary file at {temp_path}")
            logging.info(f"Temporary file created successfully, size: {temp_path.stat().st_size} bytes")
            
            # Now upload to GCS
            bucket = gcs_client.bucket(GCS_BUCKET)
            blob = bucket.blob(f"silver/chr/{filepath.parent.name}/{filepath.name}")
            blob.upload_from_filename(temp_path)
            
            # Clean up temporary file
            temp_path.unlink()
            
        logging.info(f"Successfully saved to GCS: {blob_path}")
        return blob_path
    except Exception as e:
        logging.error(f"Error saving to GCS: {e}", exc_info=True)
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
    # Ensure the directory exists
    filepath.parent.mkdir(parents=True, exist_ok=True)
    logging.info(f"Saving locally to: {filepath}")
    logging.info(f"DataFrame type: {type(df)}")
    
    try:
        # First save to a temporary file in the same directory
        temp_path = filepath.parent / f"temp_{filepath.name}"
        logging.info(f"Saving to temporary file first: {temp_path}")
        
        if is_geo:
            if not isinstance(df, gpd.GeoDataFrame):
                raise ValueError("DataFrame must be a GeoDataFrame when is_geo=True")
            if 'geometry' not in df.columns:
                raise ValueError("GeoDataFrame must have a 'geometry' column")
            logging.info(f"Saving GeoDataFrame with CRS: {df.crs}")
            logging.info(f"GeoDataFrame columns: {df.columns.tolist()}")
            df.to_parquet(temp_path, index=False)
        else:
            if not isinstance(df, pd.DataFrame):
                raise ValueError("DataFrame must be a pandas DataFrame when is_geo=False")
            df.to_parquet(temp_path, index=False)
        
        # Verify the temporary file was created
        if not temp_path.exists():
            raise FileNotFoundError(f"Failed to create temporary file at {temp_path}")
        logging.info(f"Temporary file created successfully, size: {temp_path.stat().st_size} bytes")
        
        # Move the temporary file to the final location
        temp_path.rename(filepath)
        
        logging.info(f"Successfully saved locally: {filepath}")
        if filepath.exists():
            logging.info(f"File size: {filepath.stat().st_size} bytes")
        else:
            raise FileNotFoundError(f"File was not created at {filepath}")
        return str(filepath)
    except Exception as e:
        logging.error(f"Error saving locally: {e}", exc_info=True)
        # Clean up temporary file if it exists
        if temp_path.exists():
            try:
                temp_path.unlink()
            except Exception:
                pass
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
    
    # Log the save attempt
    logging.info(f"Attempting to save table to {'GCS' if USE_GCS else 'local storage'}")
    logging.info(f"Target path: {filepath}")
    logging.info(f"Is GeoDataFrame: {is_geo}")
    
    if USE_GCS:
        try:
            return _save_to_gcs(filepath, df, is_geo)
        except Exception as e:
            logging.error(f"Error saving to GCS: {e}")
            logging.warning("Falling back to local storage")
            return _save_locally(filepath, df, is_geo)
    else:
        return _save_locally(filepath, df, is_geo) 