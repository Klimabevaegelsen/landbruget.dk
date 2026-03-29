#!/usr/bin/env python3
"""
BMD Scraper Pipeline main entry point.
This script orchestrates the Bronze and Silver stage processing for BMD data.
"""

import os
import sys
from pathlib import Path

import click
import dotenv
from common.cli import PipelineRun, common_options, stage_options
from common.logging_utils import setup_pipeline_logger

from bronze import BMDScraper
from bronze.export import CloudStorage
from silver import BMDTransformer, upload_to_storage

# Load environment variables
dotenv.load_dotenv()

logger = setup_pipeline_logger("bmd_pipeline", level=os.getenv("LOG_LEVEL", "INFO"))


def setup_directories() -> tuple[Path, Path]:
    """Set up output directories and return their paths."""
    bronze_dir = Path(os.getenv("BRONZE_OUTPUT_DIR", "bronze/bmd/data"))
    silver_dir = Path(os.getenv("SILVER_OUTPUT_DIR", "silver/bmd/data"))

    bronze_dir.mkdir(parents=True, exist_ok=True)
    silver_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Bronze output directory: {bronze_dir}")
    logger.info(f"Silver output directory: {silver_dir}")

    return bronze_dir, silver_dir


def run_bronze_stage(bronze_dir: Path) -> Path | None:
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

        # If in production environment, upload to cloud storage
        if os.getenv("ENVIRONMENT") == "production":
            bucket_name = (
                os.getenv("STORAGE_BUCKET") or os.getenv("R2_BUCKET") or os.getenv("GCS_BUCKET")
            )
            if bucket_name:
                logger.info(f"Uploading bronze data to cloud storage bucket {bucket_name}")
                storage = CloudStorage(bucket_name=bucket_name)

                # Upload the Excel file
                success = storage.upload_file(excel_file_path)

                # Upload the metadata file
                metadata_path = os.path.join(os.path.dirname(excel_file_path), "metadata.json")
                if os.path.exists(metadata_path):
                    storage.upload_file(metadata_path)

                if not success:
                    logger.warning(
                        "Failed to upload to cloud storage, but continuing with local file"
                    )
            else:
                logger.warning("Storage bucket not set, skipping cloud storage upload")

        logger.info(f"Bronze stage completed successfully. File saved to {excel_file_path}")
        return Path(excel_file_path)

    except Exception as e:
        logger.exception(f"Error in Bronze stage: {e}")
        return None


def find_latest_bronze_file(bronze_dir: Path) -> tuple[Path, Path] | None:
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


def run_silver_stage(bronze_file: Path, silver_dir: Path) -> Path | None:
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

        logger.info(
            f"Processing bronze file {bronze_file} to silver directory {silver_timestamp_dir}"
        )

        # Initialize and run the transformer
        transformer = BMDTransformer(input_file=bronze_file, output_dir=silver_timestamp_dir)
        parquet_file = transformer.transform()

        if not parquet_file or not parquet_file.exists():
            logger.error("Silver stage transformation failed to produce output file")
            return None

        logger.info(f"Silver stage transformation completed: {parquet_file}")

        # If in production environment, upload to cloud storage
        if os.getenv("ENVIRONMENT") == "production":
            bucket_name = (
                os.getenv("STORAGE_BUCKET") or os.getenv("R2_BUCKET") or os.getenv("GCS_BUCKET")
            )
            if bucket_name:
                logger.info(f"Uploading silver data to cloud storage bucket {bucket_name}")
                success = upload_to_storage(parquet_file, bucket_name)

                if not success:
                    logger.warning(
                        "Failed to upload silver data to cloud storage, but continuing with local file"
                    )
            else:
                logger.warning("Storage bucket not set, skipping cloud storage upload")

        return parquet_file

    except Exception as e:
        logger.exception(f"Error in Silver stage: {e}")
        return None


@click.command()
@stage_options()
@common_options
def main(stage, log_level) -> None:
    """Main entry point for the BMD Scraper pipeline."""
    global logger
    logger = setup_pipeline_logger("bmd_pipeline", level=log_level)
    logger.info(f"Starting BMD Scraper pipeline (stage: {stage})")

    # Setup directories
    bronze_dir, silver_dir = setup_directories()

    # Initialize pipeline metadata tracking
    pipeline_run = PipelineRun("bmd_pesticide_database", logger=logger)

    # Run selected stages
    bronze_file = None
    silver_file = None

    if stage in ["bronze", "all"]:
        bronze_file = run_bronze_stage(bronze_dir)
        if not bronze_file and stage == "all":
            logger.error("Bronze stage failed, cannot proceed to Silver stage")
            sys.exit(1)

    if stage in ["silver", "all"] and (bronze_file or stage == "silver"):
        # If we're only running silver stage, we need to find the latest bronze file
        if not bronze_file and stage == "silver":
            result = find_latest_bronze_file(bronze_dir)
            if result:
                _, bronze_file = result
                logger.info(f"Using latest bronze file: {bronze_file}")
            else:
                logger.error("No bronze files found to process in silver stage")
                sys.exit(1)

        silver_file = run_silver_stage(bronze_file, silver_dir)

    # Log execution time
    logger.info(f"Pipeline execution completed in {pipeline_run.elapsed:.1f}s")

    # Generate schema documentation if silver stage completed successfully
    if silver_file and silver_file.exists():
        try:
            from datetime import datetime

            from common.schema_utils import generate_schema_docs

            timestamp_str = silver_file.parent.name
            pipeline_start_time = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")

            generate_schema_docs(
                parquet_path=silver_file,
                pipeline_name="bmd_scraper",
                table_name="bmd_processed",
                pipeline_start_time=pipeline_start_time,
                stage="silver",
                logger=logger,
            )
        except Exception as e:
            logger.error(f"Failed to generate BMD schema documentation: {e}", exc_info=True)

    # Save pipeline metadata
    if bronze_file or silver_file:
        try:
            import duckdb

            record_count = None
            if silver_file and silver_file.exists():
                try:
                    conn = duckdb.connect()
                    result = conn.execute(
                        f"SELECT COUNT(*) FROM read_parquet('{silver_file}')"
                    ).fetchone()
                    record_count = result[0] if result else None
                    conn.close()
                except Exception:
                    pass

            metadata_dir = silver_file.parent if silver_file else bronze_file.parent
            pipeline_run.finish(
                record_count=record_count,
                output_path=metadata_dir / "bmd_pesticide_database_metadata.json",
            )
        except Exception as e:
            logger.error(f"Failed to create BMD pipeline metadata: {e}")

    # Return success/failure code
    if (
        (stage == "bronze" and bronze_file)
        or (stage == "silver" and silver_file)
        or (stage == "all" and bronze_file and silver_file)
    ):
        sys.exit(0)
    elif stage == "all" and bronze_file and not silver_file:
        logger.error("Pipeline partially completed - bronze succeeded but silver failed")
        sys.exit(1)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
