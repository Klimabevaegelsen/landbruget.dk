"""CHR Gold Layer Export functionality using DuckDB."""

import logging
from pathlib import Path
from typing import Optional

# Try to import GCS utilities
try:
    from unified_pipeline.util.gcs_access import GCSDataAccess

    GCS_AVAILABLE = True
except ImportError:
    GCS_AVAILABLE = False
    GCSDataAccess = None


def save_table(output_path: Path, con, table_name: str) -> Optional[Path]:
    """
    Save table data to parquet file using DuckDB.

    Args:
        output_path: Path where to save the parquet file
        con: DuckDB connection
        table_name: Name of the table to export

    Returns:
        Path to the saved file if successful, None otherwise
    """
    try:
        # Ensure parent directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Use DuckDB COPY to save to parquet
        copy_sql = f"COPY {table_name} TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        con.execute(copy_sql)

        logging.info(f"✅ Saved {table_name} to {output_path}")
        return output_path

    except Exception as e:
        logging.error(f"❌ Failed to save {table_name} to {output_path}: {e}")
        return None


def upload_to_gcs(local_path: Path, gcs_path: str, bucket: str = "landbrugsdata-raw-data") -> bool:
    """
    Upload a local file to Google Cloud Storage.

    Args:
        local_path: Local file path
        gcs_path: GCS destination path (without gs:// prefix)
        bucket: GCS bucket name

    Returns:
        True if successful, False otherwise
    """
    if not GCS_AVAILABLE:
        logging.warning("GCS utilities not available, skipping upload")
        return False

    try:
        gcs = GCSDataAccess()
        success = gcs.upload_file(str(local_path), f"gs://{bucket}/{gcs_path}")

        if success:
            logging.info(f"✅ Uploaded {local_path} to gs://{bucket}/{gcs_path}")
        else:
            logging.error(f"❌ Failed to upload {local_path} to GCS")

        return success

    except Exception as e:
        logging.error(f"❌ Error uploading to GCS: {e}")
        return False


def export_gold_table(con, table_name: str, timestamp: str, output_dir: Path, upload_to_cloud: bool = True) -> bool:
    """
    Export a gold table locally and optionally to GCS.

    Args:
        con: DuckDB connection
        table_name: Name of the table to export
        timestamp: Export timestamp for GCS path
        output_dir: Local output directory
        upload_to_cloud: Whether to upload to GCS

    Returns:
        True if successful, False otherwise
    """
    try:
        # Local export
        local_path = output_dir / f"{table_name}.parquet"
        if not save_table(local_path, con, table_name):
            return False

        # GCS export
        if upload_to_cloud and GCS_AVAILABLE:
            gcs_path = f"gold/chr/{timestamp}/{table_name}.parquet"
            upload_to_gcs(local_path, gcs_path)

        return True

    except Exception as e:
        logging.error(f"❌ Failed to export {table_name}: {e}")
        return False
