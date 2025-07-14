#!/usr/bin/env python3
"""
Consolidate CHR Animal Movement Data

This script consolidates monthly CHR animal movement JSON files into a single
consolidated file for silver layer processing.

Usage:
    python consolidate_animal_movements.py --timestamp 20250714_075448
"""

import argparse
import logging
import sys
from pathlib import Path

# Add the backend directory to Python path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))

from pipelines.unified_pipeline.src.unified_pipeline.util.gcs_access import GCSDataAccess


def setup_logging():
    """Setup logging configuration."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def consolidate_monthly_files(timestamp: str, bucket_name: str = "landbrugsdata-raw-data") -> bool:
    """
    Consolidate monthly CHR animal movement files into a single file.

    Args:
        timestamp: Bronze layer timestamp (e.g., "20250714_075448")
        bucket_name: GCS bucket name

    Returns:
        bool: True if consolidation successful, False otherwise
    """
    try:
        gcs_access = GCSDataAccess(bucket_name)
        base_path = f"bronze/chr/{timestamp}"

        logging.info(f"🔍 Looking for monthly animal movement files in gs://{bucket_name}/{base_path}/")

        # List all files in the bronze CHR directory
        files = gcs_access.list_files(base_path)

        # Find all monthly animal movement files
        monthly_files = [f for f in files if f.startswith("chr_dyr_movement_summaries_") and f.endswith(".json")]

        if not monthly_files:
            logging.warning("⚠️ No monthly animal movement files found!")
            return False

        logging.info(f"📁 Found {len(monthly_files)} monthly files: {monthly_files}")

        # Download and consolidate all monthly files
        consolidated_data = []
        total_records = 0

        for monthly_file in sorted(monthly_files):
            file_path = f"{base_path}/{monthly_file}"
            logging.info(f"📥 Processing {monthly_file}...")

            try:
                # Download file content
                content = gcs_access.download_json(file_path)

                if isinstance(content, list):
                    consolidated_data.extend(content)
                    total_records += len(content)
                    logging.info(f"  ✅ Added {len(content)} records from {monthly_file}")
                else:
                    logging.warning(f"  ⚠️ Unexpected data format in {monthly_file}")

            except Exception as e:
                logging.error(f"  ❌ Error processing {monthly_file}: {e}")
                continue

        if not consolidated_data:
            logging.error("❌ No data found in any monthly files!")
            return False

        logging.info(f"📊 Consolidated {total_records} total records from {len(monthly_files)} files")

        # Upload consolidated file
        consolidated_file_path = f"{base_path}/chr_dyr_movement_summaries.json"
        logging.info(f"📤 Uploading consolidated file to gs://{bucket_name}/{consolidated_file_path}")

        success = gcs_access.upload_json(consolidated_data, consolidated_file_path)

        if success:
            logging.info(f"✅ Successfully created consolidated file with {total_records} records")

            # Verify the file was created
            if gcs_access.file_exists(f"gs://{bucket_name}/{consolidated_file_path}"):
                file_size = gcs_access.get_file_size(f"gs://{bucket_name}/{consolidated_file_path}")
                logging.info(f"✅ Verified: Consolidated file exists ({file_size / (1024 * 1024):.1f} MB)")
                return True
            else:
                logging.error("❌ Consolidated file verification failed!")
                return False
        else:
            logging.error("❌ Failed to upload consolidated file!")
            return False

    except Exception as e:
        logging.error(f"❌ Error during consolidation: {e}")
        return False


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Consolidate CHR animal movement data")
    parser.add_argument("--timestamp", required=True, help="Bronze layer timestamp (e.g., '20250714_075448')")
    parser.add_argument(
        "--bucket", default="landbrugsdata-raw-data", help="GCS bucket name (default: landbrugsdata-raw-data)"
    )

    args = parser.parse_args()

    setup_logging()

    logging.info(f"🚀 Starting CHR animal movement consolidation for timestamp: {args.timestamp}")

    success = consolidate_monthly_files(args.timestamp, args.bucket)

    if success:
        logging.info("🎉 Consolidation completed successfully!")
        sys.exit(0)
    else:
        logging.error("💥 Consolidation failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
