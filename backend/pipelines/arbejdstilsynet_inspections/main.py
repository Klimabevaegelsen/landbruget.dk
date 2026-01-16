import argparse
import logging
import os
import sys
from datetime import datetime

import bronze.export
import silver.transform
from dotenv import load_dotenv

# Import pipeline metadata system for data tracing
try:
    from backend.common.pipeline_metadata import MetadataManager as PipelineMetadataManager

    PIPELINE_METADATA_AVAILABLE = True
except ImportError:
    print("⚠️  Pipeline metadata system not available - continuing without data tracing")
    PipelineMetadataManager = None
    PIPELINE_METADATA_AVAILABLE = False

PIPELINE_ROOT = os.path.dirname(os.path.abspath(__file__))
print("[DEBUG] DISPLAY =", os.environ.get("DISPLAY"))
print("[DEBUG] DOCKER_ENV =", os.environ.get("DOCKER_ENV"))


def parse_args() -> argparse.Namespace:
    """Parse command line arguments for the pipeline.

    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(description="Run the Arbejdstilsynet Inspections pipeline")

    # Calculate default start_date (6 months ago)

    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Start date in YYYY-MM-DD format (default: 6 months ago)",
    )

    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="End date in YYYY-MM-DD format (default: today)",
    )

    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR)",
    )

    parser.add_argument("--gcs-bucket", type=str, help="Google Cloud Storage bucket for export")
    parser.add_argument("--stage", type=str, choices=["all", "bronze", "silver"], default="all")

    return parser.parse_args()


if __name__ == "__main__":
    # Record start time for execution tracking
    start_time = datetime.now()

    # Parse command line arguments
    args = parse_args()
    load_dotenv()

    # Set logging level
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)
    logger.info(f"Starting pipeline with args: {args}")

    # Initialize pipeline metadata manager
    pipeline_metadata_manager = None
    if PIPELINE_METADATA_AVAILABLE:
        pipeline_metadata_manager = PipelineMetadataManager()
        logger.info("✅ Pipeline metadata system initialized")
    else:
        logger.warning("⚠️ Pipeline metadata system not available - continuing without data tracing")

    # Determine GCS bucket to use
    actual_gcs_bucket = args.gcs_bucket
    if not actual_gcs_bucket:
        actual_gcs_bucket = os.getenv("GCS_BUCKET")
        if actual_gcs_bucket:
            logger.info(f"Using GCS_BUCKET from environment variable: {actual_gcs_bucket}")
        else:
            logger.warning(
                "GCS_BUCKET not provided via --gcs-bucket argument or GCS_BUCKET environment variable. "
                "GCS uploads will be skipped."
            )

    bronze_success = True
    silver_success = True

    try:
        # Run Bronze Layer
        if args.stage in ["all", "bronze"]:
            print("[main.py] Running Bronze Layer: export.py ...")
            bronze.export.main(log_level=args.log_level, gcs_bucket=actual_gcs_bucket)
            print("[main.py] Bronze Layer complete.")
        else:
            logger.info("Skipping Bronze Layer due to --stage setting.")

    except Exception as e:
        logger.error(f"Bronze Layer failed: {e}", exc_info=True)
        bronze_success = False
        # If bronze fails, we might not want to proceed to silver, or make it conditional
        # For now, we'll mark as failed and let it try silver if 'all' or 'silver' is specified
        # but the overall pipeline will fail.

    if args.stage in ["all", "silver"]:
        if not bronze_success and args.stage == "all":
            logger.warning("Skipping Silver Layer because Bronze Layer failed.")
            silver_success = False  # Ensure silver is also marked as failed if bronze did
        else:
            try:
                # Run Silver Layer
                print("[main.py] Running Silver Layer: transform.py ...")
                silver.transform.main(
                    start_date=args.start_date,
                    end_date=args.end_date,
                    gcs_bucket=actual_gcs_bucket,
                    log_level=args.log_level,
                )
                print("[main.py] Silver Layer complete.")
            except RuntimeError as e:  # Catching the specific exception from silver.transform.main
                logger.error(f"Silver Layer failed: {e}", exc_info=True)
                silver_success = False
            except Exception as e:  # Catch any other unexpected errors from silver
                logger.error(f"Silver Layer failed with an unexpected error: {e}", exc_info=True)
                silver_success = False
    else:
        logger.info("Skipping Silver Layer due to --stage setting.")

    # Create and save metadata for Arbejdstilsynet Inspections data
    if pipeline_metadata_manager and (bronze_success or silver_success):
        try:
            import time

            processing_duration = time.time() - start_time.timestamp()

            # Create metadata for Arbejdstilsynet inspections
            arbejdstilsynet_metadata = pipeline_metadata_manager.create_metadata(
                source_key="arbejdstilsynet_inspections",
                record_count=None,  # Could be enhanced to track actual record count from bronze/silver results
                processing_duration=processing_duration,
                file_size_bytes=None,  # Will be calculated automatically
                source_datasets=None,
            )

            # Save metadata to output directory
            output_dir = os.getenv("OUTPUT_DIR", "data/output")
            timestamp = start_time.strftime("%Y%m%d_%H%M%S")
            metadata_dir = os.path.join(output_dir, "arbejdstilsynet_inspections", timestamp)
            os.makedirs(metadata_dir, exist_ok=True)

            from pathlib import Path

            metadata_path = pipeline_metadata_manager.save_metadata(
                arbejdstilsynet_metadata, Path(metadata_dir) / "arbejdstilsynet_inspections_metadata.json"
            )
            logger.info(f"✅ Arbejdstilsynet inspections metadata saved to {metadata_path}")

        except Exception as e:
            logger.error(f"❌ Failed to create Arbejdstilsynet pipeline metadata: {e}")

    if bronze_success and silver_success:
        print("[main.py] Pipeline finished successfully.")
        sys.exit(0)
    else:
        print("[main.py] Pipeline finished with errors.")
        sys.exit(1)
