"""Svineflytning Pipeline for fetching and processing pig movement data."""

import logging
import os
import sys
from datetime import date, datetime
from typing import Any

import click
from common.cli import PipelineRun, common_options, stage_options
from common.logging_utils import setup_pipeline_logger
from tqdm.contrib.logging import logging_redirect_tqdm

from bronze.load_svineflytning import (
    ENDPOINTS,
    create_client,
    fetch_all_movements,
    get_fvm_credentials,
)

logger = logging.getLogger(__name__)  # Replaced in setup_logging()

# Constants for resource management
DEFAULT_MAX_CONCURRENT_FETCHES = 5  # Number of parallel API calls
DEFAULT_BUFFER_SIZE = 50  # Number of responses to accumulate before writing to disk
# Assuming avg response size of 10MB, this means ~500MB peak memory/disk usage


def setup_logging(log_level: str):
    """Configure logging with the specified level."""
    global logger
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)

    # Set up pipeline logger using common utility
    logger = setup_pipeline_logger("svineflytning_pipeline", level=log_level)

    # Configure bronze module loggers
    bronze_logger = logging.getLogger("svineflytning_pipeline.bronze")
    bronze_logger.setLevel(numeric_level if numeric_level <= logging.INFO else logging.WARNING)

    # Set third-party loggers to WARNING or higher
    for logger_name in ["zeep", "urllib3", "google", "requests"]:
        third_party_logger = logging.getLogger(logger_name)
        third_party_logger.setLevel(logging.WARNING)
        if numeric_level > logging.DEBUG:
            third_party_logger.propagate = False


def get_default_dates() -> tuple[date, date]:
    """Get default start and end dates (last 5 years for full mode, 2 months for incremental)."""
    today = date.today()
    end_date = today

    # Check processing mode to determine default date range
    processing_mode = os.getenv("SVINEFLYTNING_PROCESSING_MODE", "incremental").lower()

    if processing_mode == "incremental":
        # Last 2 months for incremental (as per CHR plan)
        from datetime import timedelta

        start_date = today - timedelta(days=60)
        logger.info(f"Incremental mode: using 2-month range ({start_date} to {end_date})")
    elif processing_mode == "full":
        # Full backfill: 3 years (as per CHR plan)
        start_date = today.replace(year=today.year - 3)
        logger.info(f"Full mode: using 3-year range ({start_date} to {end_date})")
    else:
        # Default fallback
        start_date = today.replace(year=today.year - 5)
        logger.info(f"Default mode: using 5-year range ({start_date} to {end_date})")

    return start_date, end_date


def _parse_date(ctx, param, value):
    """Click callback to parse date strings."""
    if value is None:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as e:
        raise click.BadParameter(f"Date must be in YYYY-MM-DD format: {e}") from e


def run_bronze_stage(
    *,
    start_date: date,
    end_date: date,
    environment: str,
    max_concurrent_fetches: int,
    buffer_size: int,
    progress: bool,
    test: bool,
) -> dict[str, Any]:
    """Run the bronze stage of the pipeline."""
    logger.warning("Starting bronze stage")
    if progress:
        logger.warning(f"Processing date range: {start_date} to {end_date}")
        logger.warning(f"Using {max_concurrent_fetches} concurrent fetches")
        logger.warning(f"Buffer size: {buffer_size} responses")

    # Get credentials and create client
    username, password = get_fvm_credentials()
    client = create_client(ENDPOINTS[environment], username, password)

    # Use environment variable for output directory, with fallback to relative path
    output_dir = os.getenv("SVINEFLYTNING_OUTPUT_DIR", "./data/raw/svineflytning")

    # Fetch and stream all movements
    with logging_redirect_tqdm():
        result = fetch_all_movements(
            client=client,
            start_date=start_date,
            end_date=end_date,
            output_dir=output_dir,
            max_concurrent_fetches=max_concurrent_fetches,
            buffer_size=buffer_size,
            show_progress=progress,
            test_mode=test,
        )

    logger.warning("Bronze stage completed successfully")
    if progress:
        logger.warning(f"Bronze data exported to: {result['storage_path']}")

    return result


def run_silver_stage(
    *,
    bronze_timestamp: str | None = None,
    progress: bool = False,
    bronze_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run the silver stage of the pipeline."""
    logger.warning("Starting silver stage")

    try:
        from silver.main import process_specific_bronze_timestamp, run_silver_processing

        # Determine how to run silver processing
        if bronze_timestamp:
            # Process specific bronze timestamp
            logger.warning(f"Processing specific bronze timestamp: {bronze_timestamp}")
            result = process_specific_bronze_timestamp(bronze_timestamp=bronze_timestamp)
        elif bronze_result and "storage_path" in bronze_result:
            # Use bronze result from current run
            logger.warning("Processing bronze data from current pipeline run")
            bronze_path = bronze_result["storage_path"]

            # Extract timestamp from bronze result for consistent naming
            export_timestamp = bronze_result.get("export_timestamp")

            result = run_silver_processing(
                bronze_data_path=bronze_path,
                export_timestamp=export_timestamp,
                use_latest_bronze=False,
            )
        else:
            # Auto-discover latest bronze data
            logger.warning("Auto-discovering latest bronze data")
            result = run_silver_processing(use_latest_bronze=True)

        if result["success"]:
            logger.warning("Silver stage completed successfully")
            if progress:
                logger.warning(f"Silver data exported to: {result['output_path']}")
                logger.warning(f"Processed {result['processed_movements']} movements")
        else:
            logger.error(f"Silver stage failed: {result['error']}")

        return result

    except ImportError as e:
        logger.error(f"Failed to import silver processing modules: {e}")
        return {"success": False, "error": f"Silver processing not available: {e}"}
    except Exception as e:
        logger.error(f"Silver stage failed: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@click.command()
@stage_options()
@click.option(
    "--start-date", type=str, default=None, callback=_parse_date, help="Start date (YYYY-MM-DD)"
)
@click.option(
    "--end-date", type=str, default=None, callback=_parse_date, help="End date (YYYY-MM-DD)"
)
@click.option(
    "--bronze-timestamp",
    default=None,
    help="Specific bronze timestamp to process for silver stage (YYYYMMDD_HHMMSS)",
)
@click.option("--progress", is_flag=True, help="Show progress information")
@click.option(
    "--environment", type=click.Choice(["prod", "test"]), default="prod", help="Environment to use"
)
@click.option("--test", is_flag=True, help="Run in test mode with limited data")
@click.option(
    "--max-concurrent-fetches",
    type=int,
    default=DEFAULT_MAX_CONCURRENT_FETCHES,
    help="Maximum number of concurrent API calls",
)
@click.option(
    "--buffer-size",
    type=int,
    default=DEFAULT_BUFFER_SIZE,
    help="Number of responses to accumulate before writing to disk",
)
@common_options
def main(
    stage,
    start_date,
    end_date,
    bronze_timestamp,
    progress,
    environment,
    test,
    max_concurrent_fetches,
    buffer_size,
    log_level,
):
    """Run the Svineflytning Data Pipeline."""
    setup_logging(log_level)

    # Apply default dates if not provided
    default_start, default_end = get_default_dates()
    if start_date is None:
        start_date = default_start
    if end_date is None:
        end_date = default_end

    # Validate resource usage parameters
    total_memory_estimate = buffer_size * 10
    if total_memory_estimate > 1000:
        logger.warning(
            f"Warning: Current settings might use up to {total_memory_estimate}MB of memory. "
            f"Consider reducing --buffer-size if this is too high."
        )

    # Initialize pipeline metadata tracking
    pipeline_run = PipelineRun("svineflytning_movements", logger=logger)

    logger.warning(f"Starting Svineflytning pipeline - Stage: {stage}")

    bronze_result = None
    silver_result = None

    try:
        # Run bronze stage if needed
        if stage in ["bronze", "all"]:
            bronze_result = run_bronze_stage(
                start_date=start_date,
                end_date=end_date,
                environment=environment,
                max_concurrent_fetches=max_concurrent_fetches,
                buffer_size=buffer_size,
                progress=progress,
                test=test,
            )
            if not bronze_result:
                logger.error("Bronze stage failed")
                sys.exit(1)

        # Run silver stage if needed
        if stage in ["silver", "all"]:
            silver_result = run_silver_stage(
                bronze_timestamp=bronze_timestamp,
                progress=progress,
                bronze_result=bronze_result,
            )
            if not silver_result or not silver_result["success"]:
                logger.error("Silver stage failed")
                sys.exit(1)

        # Save pipeline metadata
        if bronze_result or silver_result:
            try:
                record_count = None
                if silver_result and "processed_movements" in silver_result:
                    record_count = silver_result["processed_movements"]
                elif bronze_result and "total_movements" in bronze_result:
                    record_count = bronze_result["total_movements"]

                metadata_dir = None
                if silver_result and "output_path" in silver_result:
                    metadata_dir = silver_result["output_path"]
                elif bronze_result and "storage_path" in bronze_result:
                    metadata_dir = bronze_result["storage_path"]

                if metadata_dir:
                    from pathlib import Path

                    pipeline_run.finish(
                        record_count=record_count,
                        output_path=Path(metadata_dir) / "svineflytning_movements_metadata.json",
                    )
            except Exception as e:
                logger.error(f"Failed to create Svineflytning pipeline metadata: {e}")

        # Summary
        logger.warning("Pipeline completed successfully")
        if progress:
            if bronze_result:
                logger.warning(f"Bronze: {bronze_result.get('storage_path', 'N/A')}")
            if silver_result:
                logger.warning(f"Silver: {silver_result.get('output_path', 'N/A')}")
                logger.warning(
                    f"Movements processed: {silver_result.get('processed_movements', 'N/A')}"
                )

    except Exception as e:
        logger.error(f"Pipeline failed: {e!s}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
