#!/usr/bin/env python3
"""
BMD Scraper Pipeline main entry point.
This script orchestrates the Bronze and Silver stage processing for BMD data.
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

import dotenv
from bronze import BMDScraper
from bronze.export import GCSStorage
from silver import BMDTransformer, upload_to_gcs

# Load environment variables
dotenv.load_dotenv()

# Configure logging
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("bmd_pipeline")


def setup_directories() -> Tuple[Path, Path]:
    """Set up output directories and return their paths."""
    bronze_dir = Path(os.getenv("BRONZE_OUTPUT_DIR", "bronze/bmd/data"))
    silver_dir = Path(os.getenv("SILVER_OUTPUT_DIR", "silver/bmd/data"))

    bronze_dir.mkdir(parents=True, exist_ok=True)
    silver_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Bronze output directory: {bronze_dir}")
    logger.info(f"Silver output directory: {silver_dir}")

    return bronze_dir, silver_dir


def run_bronze_stage(bronze_dir: Path) -> Optional[Path]:
    """
    Run the Bronze stage of the BMD pipeline.

    This stage extracts raw data from the BMD portal and saves it to the bronze directory
    with proper timestamp subdirectory and metadata.

    Args:
        bronze_dir: Directory to store bronze stage output

    Returns:
        Path to the downloaded Excel file if successful, None otherwise
    """
    logger.info("Starting Bronze stage processing")

    try:
        # Initialize the BMD scraper
        scraper = BMDScraper(
            base_url=os.getenv("BMD_BASE_URL", "https://bmd.mst.dk"),
            output_dir=str(bronze_dir),
        )

        # Execute the scraping process
        excel_file_path = scraper.scrape()

        if not excel_file_path:
            logger.error("Bronze stage failed to download the Excel file")
            return None

        logger.info(f"Bronze stage: Raw data downloaded to {excel_file_path}")

        # If in production environment, upload to GCS
        if os.getenv("ENVIRONMENT") == "production":
            bucket_name = os.getenv("GCS_BUCKET")
            if bucket_name:
                logger.info(f"Uploading bronze data to GCS bucket {bucket_name}")
                storage = GCSStorage(bucket_name=bucket_name)

                # Upload the Excel file
                success = storage.upload_file(excel_file_path)

                # Upload the metadata file
                metadata_path = os.path.join(os.path.dirname(excel_file_path), "metadata.json")
                if os.path.exists(metadata_path):
                    storage.upload_file(metadata_path)

                if not success:
                    logger.warning("Failed to upload to GCS, but continuing with local file")
            else:
                logger.warning("GCS_BUCKET not set, skipping GCS upload")

        logger.info(f"Bronze stage completed successfully. File saved to {excel_file_path}")
        return Path(excel_file_path)

    except Exception as e:
        logger.exception(f"Error in Bronze stage: {e}")
        return None


def find_latest_bronze_file(bronze_dir: Path) -> Optional[Tuple[Path, Path]]:
    """
    Find the latest bronze stage output file and its directory.

    Args:
        bronze_dir: Base directory for bronze stage output

    Returns:
        Tuple of (timestamp_dir, excel_file) if found, None otherwise
    """
    try:
        # Look for timestamp directories
        timestamp_dirs = [d for d in bronze_dir.iterdir() if d.is_dir()]
        if not timestamp_dirs:
            logger.error("No timestamp directories found in bronze directory")
            return None

        # Get the most recent timestamp directory
        latest_dir = max(timestamp_dirs, key=lambda d: d.name)

        # Find the Excel file in the directory
        excel_file = latest_dir / "bmd_raw.xlsx"
        if not excel_file.exists():
            logger.error(f"No bmd_raw.xlsx file found in {latest_dir}")
            return None

        return latest_dir, excel_file
    except Exception as e:
        logger.exception(f"Error finding latest bronze file: {e}")
        return None


def run_silver_stage(bronze_file: Path, silver_dir: Path) -> Optional[Path]:
    """
    Run the Silver stage of the BMD pipeline.

    This stage transforms the raw BMD data into a structured format.

    Args:
        bronze_file: Path to the input file from bronze stage
        silver_dir: Directory to store silver stage output

    Returns:
        Path to the processed file if successful, None otherwise
    """
    logger.info("Starting Silver stage processing")

    try:
        # Get the timestamp from the bronze file's parent directory
        timestamp = bronze_file.parent.name

        # Create timestamp directory in silver
        silver_timestamp_dir = silver_dir / timestamp
        silver_timestamp_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"Processing bronze file {bronze_file} to silver directory {silver_timestamp_dir}")

        # Initialize and run the transformer
        transformer = BMDTransformer(input_file=bronze_file, output_dir=silver_timestamp_dir)
        parquet_file = transformer.transform()

        if not parquet_file or not parquet_file.exists():
            logger.error("Silver stage transformation failed to produce output file")
            return None

        logger.info(f"Silver stage transformation completed: {parquet_file}")

        # If in production environment, upload to GCS
        if os.getenv("ENVIRONMENT") == "production":
            bucket_name = os.getenv("GCS_BUCKET")
            if bucket_name:
                logger.info(f"Uploading silver data to GCS bucket {bucket_name}")
                success = upload_to_gcs(parquet_file, bucket_name)

                if not success:
                    logger.warning("Failed to upload silver data to GCS, but continuing with local file")
            else:
                logger.warning("GCS_BUCKET not set, skipping GCS upload")

        return parquet_file

    except Exception as e:
        logger.exception(f"Error in Silver stage: {e}")
        return None


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="BMD Scraper Pipeline")
    parser.add_argument(
        "--stage",
        choices=["bronze", "silver", "all"],
        default="all",
        help="Pipeline stage to run (default: all)",
    )
    return parser.parse_args()


def main():
    """Main entry point for the BMD Scraper pipeline."""
    args = parse_args()
    logger.info(f"Starting BMD Scraper pipeline (stage: {args.stage})")

    # Setup directories
    bronze_dir, silver_dir = setup_directories()

    # Track pipeline start time
    start_time = datetime.now()

    # Run selected stages
    bronze_file = None
    silver_file = None

    if args.stage in ["bronze", "all"]:
        bronze_file = run_bronze_stage(bronze_dir)
        if not bronze_file and args.stage == "all":
            logger.error("Bronze stage failed, cannot proceed to Silver stage")
            sys.exit(1)

    if args.stage in ["silver", "all"] and (bronze_file or args.stage == "silver"):
        # If we're only running silver stage, we need to find the latest bronze file
        if not bronze_file and args.stage == "silver":
            result = find_latest_bronze_file(bronze_dir)
            if result:
                _, bronze_file = result
                logger.info(f"Using latest bronze file: {bronze_file}")
            else:
                logger.error("No bronze files found to process in silver stage")
                sys.exit(1)

        silver_file = run_silver_stage(bronze_file, silver_dir)

    # Calculate and log execution time
    execution_time = datetime.now() - start_time
    logger.info(f"Pipeline execution completed in {execution_time}")

    # Return success/failure code
    if args.stage == "bronze" and bronze_file:
        sys.exit(0)
    elif args.stage == "silver" and silver_file:
        sys.exit(0)
    elif args.stage == "all" and bronze_file and silver_file:
        sys.exit(0)
    elif args.stage == "all" and bronze_file and not silver_file:
        # If only bronze succeeded but silver was attempted, return error
        logger.error("Pipeline partially completed - bronze succeeded but silver failed")
        sys.exit(1)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
