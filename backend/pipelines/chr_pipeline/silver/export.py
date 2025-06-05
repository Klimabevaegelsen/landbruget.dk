import logging
import os
import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Optional

import gcsfs
import pandas as pd
from dotenv import load_dotenv
from google.cloud import storage

# Load environment variables
load_dotenv()

# Initialize storage paths and clients
GCS_BUCKET = os.getenv("GCS_BUCKET")
GOOGLE_CLOUD_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT")

# DEBUG: Log retrieved environment variables
logging.info(f"Retrieved GCS_BUCKET: '{GCS_BUCKET}'")
logging.info(f"Retrieved GOOGLE_CLOUD_PROJECT: '{GOOGLE_CLOUD_PROJECT}'")

# Use GCS if we have the required configuration
USE_GCS = bool(GCS_BUCKET and GOOGLE_CLOUD_PROJECT)

# DEBUG: Log USE_GCS decision
logging.info(f"USE_GCS determined as: {USE_GCS}")

# Get timestamp for this export run
EXPORT_TIMESTAMP = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

# Initialize GCS client and filesystem if bucket is configured
gcs_client = None
gcs_fs = None
if USE_GCS:
    try:
        logging.info("Attempting to initialize GCS client and filesystem...")
        gcs_client = storage.Client(project=GOOGLE_CLOUD_PROJECT)
        gcs_fs = gcsfs.GCSFileSystem(project=GOOGLE_CLOUD_PROJECT)
        # Test GCS connection
        try:
            bucket = gcs_client.bucket(GCS_BUCKET)
            if bucket.exists():
                logging.info(f"Successfully connected to GCS bucket: {GCS_BUCKET}")
            else:
                logging.error(f"GCS bucket {GCS_BUCKET} does not exist")
                USE_GCS = False
        except Exception as bucket_err:
            logging.error(f"Failed to verify GCS bucket: {bucket_err}")
            USE_GCS = False
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
        if df[col].dtype == "object":  # Check if column might contain UUIDs
            # Get first non-null value
            first_value = df[col].dropna().iloc[0] if not df[col].isna().all() else None
            if first_value is not None and hasattr(first_value, "hex"):  # UUID objects have hex attribute
                # Convert UUIDs to hex strings
                df[col] = df[col].apply(lambda x: x.hex if x is not None and hasattr(x, "hex") else x)
    return df


def _save_to_gcs(filepath: Path, df: pd.DataFrame, is_geo: bool = False) -> Optional[Path]:
    """Save DataFrame to GCS."""
    if not USE_GCS or not GCS_BUCKET:
        logging.warning("GCS not configured, cannot save to GCS")
        return None

    try:
        # Convert UUIDs to strings
        df = _convert_uuid_columns(df)

        # Create a temporary directory for local staging
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir) / filepath.name

            # Save to temporary file
            if is_geo:
                df.to_parquet(temp_path, index=False, engine="pyarrow")
            else:
                df.to_parquet(temp_path, index=False, engine="pyarrow")

            # Define GCS path with timestamp
            gcs_path = f"gs://{GCS_BUCKET}/silver/chr/{EXPORT_TIMESTAMP}/{filepath.name}"

            try:
                # Upload to GCS using gcsfs
                with open(temp_path, "rb") as local_file:
                    with gcs_fs.open(gcs_path, "wb") as gcs_file:
                        gcs_file.write(local_file.read())
                logging.info(f"Successfully uploaded {filepath.name} to GCS at {gcs_path}")
                return filepath
            except Exception as gcs_err:
                logging.error(f"Failed to upload to GCS: {gcs_err}")
                return None

    except Exception as e:
        logging.error(f"Error in GCS save process: {e}")
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
                df.to_parquet(temp_path, index=False, engine="pyarrow")
            else:
                df.to_parquet(temp_path, index=False, engine="pyarrow")

            # Ensure the parent directory exists
            os.makedirs(filepath.parent, exist_ok=True)
            # Copy directly to the target path without adding timestamp again
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
