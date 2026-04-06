import logging
import os
import sys
from pathlib import Path

import click
from common.cli import PipelineRun, common_options, resolve_bucket, stage_options
from dotenv import load_dotenv

import bronze.export
import silver.transform

PIPELINE_ROOT = os.path.dirname(os.path.abspath(__file__))
print("[DEBUG] DISPLAY =", os.environ.get("DISPLAY"))
print("[DEBUG] DOCKER_ENV =", os.environ.get("DOCKER_ENV"))


@click.command()
@stage_options(stages=["all", "bronze", "silver"])
@click.option(
    "--start-date",
    type=str,
    default=None,
    help="Start date in YYYY-MM-DD format (default: 6 months ago)",
)
@click.option(
    "--end-date", type=str, default=None, help="End date in YYYY-MM-DD format (default: today)"
)
@click.option("--storage-bucket", type=str, default=None, help="Cloud storage bucket for export")
@common_options
def main(stage, start_date, end_date, storage_bucket, log_level):
    """Run the Arbejdstilsynet Inspections pipeline."""
    load_dotenv()
    from common.secrets import init_secrets

    init_secrets()

    # Set logging level
    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)
    logger.info(f"Starting pipeline (stage={stage}, start_date={start_date}, end_date={end_date})")

    # Initialize pipeline metadata tracking
    pipeline_run = PipelineRun("arbejdstilsynet_inspections", logger=logger)

    # Determine storage bucket to use
    actual_storage_bucket = resolve_bucket(storage_bucket)
    if actual_storage_bucket == "landbruget-data" and not storage_bucket:
        # Only warn if no explicit bucket was provided and we fell back to default
        if not os.getenv("R2_BUCKET") and not os.getenv("GCS_BUCKET"):
            logger.warning(
                "Storage bucket not provided via --storage-bucket argument or environment variable. "
                "Cloud storage uploads will be skipped."
            )
            actual_storage_bucket = None
        else:
            logger.info(f"Using bucket from environment variable: {actual_storage_bucket}")

    bronze_success = True
    silver_success = True

    try:
        # Run Bronze Layer
        if stage in ["all", "bronze"]:
            print("[main.py] Running Bronze Layer: export.py ...")
            bronze.export.main(log_level=log_level, storage_bucket=actual_storage_bucket)
            print("[main.py] Bronze Layer complete.")
        else:
            logger.info("Skipping Bronze Layer due to --stage setting.")

    except Exception as e:
        logger.error(f"Bronze Layer failed: {e}", exc_info=True)
        bronze_success = False

    if stage in ["all", "silver"]:
        if not bronze_success and stage == "all":
            logger.warning("Skipping Silver Layer because Bronze Layer failed.")
            silver_success = False
        else:
            try:
                # Run Silver Layer
                print("[main.py] Running Silver Layer: transform.py ...")
                silver.transform.main(
                    start_date=start_date,
                    end_date=end_date,
                    storage_bucket=actual_storage_bucket,
                    log_level=log_level,
                )
                print("[main.py] Silver Layer complete.")
            except RuntimeError as e:
                logger.error(f"Silver Layer failed: {e}", exc_info=True)
                silver_success = False
            except Exception as e:
                logger.error(f"Silver Layer failed with an unexpected error: {e}", exc_info=True)
                silver_success = False
    else:
        logger.info("Skipping Silver Layer due to --stage setting.")

    # Save pipeline metadata
    if bronze_success or silver_success:
        try:
            output_dir = os.getenv("OUTPUT_DIR", "data/output")
            timestamp = __import__("datetime").datetime.now().strftime("%Y%m%d_%H%M%S")
            metadata_dir = os.path.join(output_dir, "arbejdstilsynet_inspections", timestamp)

            pipeline_run.finish(
                output_path=Path(metadata_dir) / "arbejdstilsynet_inspections_metadata.json",
            )
        except Exception as e:
            logger.error(f"Failed to create Arbejdstilsynet pipeline metadata: {e}")

    if bronze_success and silver_success:
        print("[main.py] Pipeline finished successfully.")
        sys.exit(0)
    else:
        print("[main.py] Pipeline finished with errors.")
        sys.exit(1)


if __name__ == "__main__":
    main()
