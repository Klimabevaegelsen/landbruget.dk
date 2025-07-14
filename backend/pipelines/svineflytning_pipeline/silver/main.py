"""
Main silver processing module for Svineflytning pipeline.

This module provides the main entry point for silver layer processing,
integrating with the existing pipeline architecture and handling both
local and GCS data sources.
"""

import logging
import os
from datetime import datetime
from typing import Any, Dict, Optional

from .transform import process_svineflytning_silver

logger = logging.getLogger(__name__)


def get_latest_bronze_data_path() -> Optional[str]:
    """
    Get the path to the latest bronze data from GCS.

    Returns:
        String path to the latest bronze data or None if not found
    """
    try:
        from google.cloud import storage

        bucket_name = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")
        client = storage.Client()
        bucket = client.bucket(bucket_name)

        # List all bronze svineflytning directories
        prefix = "bronze/svineflytning/"
        blobs = list(bucket.list_blobs(prefix=prefix, delimiter="/"))

        # Get all directory timestamps
        directories = []
        for blob in bucket.list_blobs(prefix=prefix, delimiter="/"):
            if blob.name.endswith("/"):
                timestamp = blob.name.replace(prefix, "").rstrip("/")
                if timestamp:  # Skip empty strings
                    directories.append(timestamp)

        if not directories:
            logger.warning("No bronze data directories found")
            return None

        # Sort by timestamp and get the latest
        latest_timestamp = sorted(directories)[-1]
        latest_path = f"gs://{bucket_name}/bronze/svineflytning/{latest_timestamp}/svineflytning.json"

        logger.info(f"Found latest bronze data: {latest_path}")
        return latest_path

    except Exception as e:
        logger.error(f"Failed to find latest bronze data: {e}")
        return None


def run_silver_processing(
    bronze_data_path: Optional[str] = None,
    output_dir: Optional[str] = None,
    export_timestamp: Optional[str] = None,
    use_latest_bronze: bool = True,
) -> Dict[str, Any]:
    """
    Run the complete silver processing pipeline.

    Args:
        bronze_data_path: Specific path to bronze data (optional)
        output_dir: Output directory for silver data (optional)
        export_timestamp: Timestamp for this export (optional)
        use_latest_bronze: Whether to automatically find latest bronze data

    Returns:
        Dict containing processing results
    """
    logger.info("Starting Svineflytning silver processing")

    # Generate timestamp if not provided
    if export_timestamp is None:
        export_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Determine bronze data path
    if bronze_data_path is None and use_latest_bronze:
        bronze_data_path = get_latest_bronze_data_path()
        if bronze_data_path is None:
            return {"success": False, "error": "No bronze data found and no specific path provided"}
    elif bronze_data_path is None:
        return {"success": False, "error": "No bronze data path provided and auto-discovery disabled"}

    # Set default output directory
    if output_dir is None:
        output_dir = os.getenv("SVINEFLYTNING_SILVER_OUTPUT_DIR", "/data/silver/svineflytning")

    logger.info(f"Processing bronze data from: {bronze_data_path}")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Export timestamp: {export_timestamp}")

    try:
        # Run the silver processing
        result = process_svineflytning_silver(
            bronze_data_path=bronze_data_path, output_dir=output_dir, export_timestamp=export_timestamp
        )

        if result["success"]:
            logger.info("✅ Silver processing completed successfully")
            logger.info(f"📊 Processed {result['processed_movements']} movements")
            logger.info(f"💾 Output: {result['output_path']}")
        else:
            logger.error(f"❌ Silver processing failed: {result['error']}")

        return result

    except Exception as e:
        logger.error(f"Unexpected error in silver processing: {e}", exc_info=True)
        return {"success": False, "error": f"Unexpected error: {str(e)}"}


def process_specific_bronze_timestamp(bronze_timestamp: str, output_dir: Optional[str] = None) -> Dict[str, Any]:
    """
    Process a specific bronze timestamp to silver.

    Args:
        bronze_timestamp: The bronze timestamp to process (YYYYMMDD_HHMMSS)
        output_dir: Output directory for silver data

    Returns:
        Dict containing processing results
    """
    bucket_name = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")
    bronze_path = f"gs://{bucket_name}/bronze/svineflytning/{bronze_timestamp}/svineflytning.json"

    return run_silver_processing(
        bronze_data_path=bronze_path,
        output_dir=output_dir,
        export_timestamp=bronze_timestamp,  # Use same timestamp for silver
        use_latest_bronze=False,
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run Svineflytning silver processing")
    parser.add_argument("--bronze-path", help="Specific bronze data path")
    parser.add_argument("--bronze-timestamp", help="Specific bronze timestamp to process")
    parser.add_argument("--output-dir", help="Output directory for silver data")
    parser.add_argument("--export-timestamp", help="Export timestamp")
    parser.add_argument("--no-auto-bronze", action="store_true", help="Disable automatic bronze data discovery")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])

    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level), format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Determine processing mode
    if args.bronze_timestamp:
        # Process specific timestamp
        result = process_specific_bronze_timestamp(bronze_timestamp=args.bronze_timestamp, output_dir=args.output_dir)
    else:
        # General processing
        result = run_silver_processing(
            bronze_data_path=args.bronze_path,
            output_dir=args.output_dir,
            export_timestamp=args.export_timestamp,
            use_latest_bronze=not args.no_auto_bronze,
        )

    # Exit with appropriate code
    if result["success"]:
        print("✅ Silver processing completed successfully")
        exit(0)
    else:
        print(f"❌ Silver processing failed: {result['error']}")
        exit(1)
