"""Svineflytning Pipeline for fetching and processing pig movement data."""

import argparse
import logging
import os
import sys
from datetime import date, datetime
from typing import Any, Dict

from bronze.load_svineflytning import ENDPOINTS, create_client, fetch_all_movements, get_fvm_credentials
from tqdm.contrib.logging import logging_redirect_tqdm

logger = logging.getLogger(__name__)

# Constants for resource management
DEFAULT_MAX_CONCURRENT_FETCHES = 5  # Number of parallel API calls
DEFAULT_BUFFER_SIZE = 50  # Number of responses to accumulate before writing to disk
# Assuming avg response size of 10MB, this means ~500MB peak memory/disk usage


def setup_logging(log_level: str):
    """Configure logging with the specified level."""
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)

    # Remove all existing handlers to start fresh
    root = logging.getLogger()
    for handler in root.handlers[:]:
        root.removeHandler(handler)

    # Set up root logger at WARNING by default with a format that works well with tqdm
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # Configure our pipeline loggers
    pipeline_logger = logging.getLogger("svineflytning_pipeline")
    pipeline_logger.setLevel(numeric_level)

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
    """Get default start and end dates (last 5 years)."""
    today = date.today()
    end_date = today
    start_date = today.replace(year=today.year - 5)  # 5 years ago from today
    return start_date, end_date


def parse_args() -> Dict[str, Any]:
    """Parse command line arguments."""
    start_date_def, end_date_def = get_default_dates()

    parser = argparse.ArgumentParser(description="Run the Svineflytning Data Pipeline.")
    parser.add_argument(
        "--stage", choices=["bronze", "silver", "all"], default="all", help="Pipeline stage to run (default: all)"
    )
    parser.add_argument(
        "--start-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        default=start_date_def,
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        default=end_date_def,
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--bronze-timestamp", help="Specific bronze timestamp to process for silver stage (YYYYMMDD_HHMMSS)"
    )
    parser.add_argument(
        "--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="WARNING", help="Logging level"
    )
    parser.add_argument("--progress", action="store_true", help="Show progress information")
    parser.add_argument("--environment", choices=["prod", "test"], default="prod", help="Environment to use")
    parser.add_argument("--test", action="store_true", help="Run in test mode with limited data")
    parser.add_argument(
        "--max-concurrent-fetches",
        type=int,
        default=DEFAULT_MAX_CONCURRENT_FETCHES,
        help="Maximum number of concurrent API calls",
    )
    parser.add_argument(
        "--buffer-size",
        type=int,
        default=DEFAULT_BUFFER_SIZE,
        help="Number of responses to accumulate before writing to disk",
    )

    args = parser.parse_args()

    # Validate resource usage parameters
    total_memory_estimate = args.buffer_size * 10  # Rough estimate: 10MB per response
    if total_memory_estimate > 1000:  # Warning if estimated usage > 1GB
        logger.warning(
            f"Warning: Current settings might use up to {total_memory_estimate}MB of memory. "
            f"Consider reducing --buffer-size if this is too high."
        )

    return vars(args)


def run_bronze_stage(args: Dict[str, Any]) -> Dict[str, Any]:
    """Run the bronze stage of the pipeline."""
    logger.warning("Starting bronze stage")
    if args["progress"]:
        logger.warning(f"Processing date range: {args['start_date']} to {args['end_date']}")
        logger.warning(f"Using {args['max_concurrent_fetches']} concurrent fetches")
        logger.warning(f"Buffer size: {args['buffer_size']} responses")

    # Get credentials and create client
    username, password = get_fvm_credentials()
    client = create_client(ENDPOINTS[args["environment"]], username, password)

    # Use environment variable for output directory, with fallback to relative path
    output_dir = os.getenv("SVINEFLYTNING_OUTPUT_DIR", "./data/raw/svineflytning")

    # Fetch and stream all movements
    with logging_redirect_tqdm():
        result = fetch_all_movements(
            client=client,
            start_date=args["start_date"],
            end_date=args["end_date"],
            output_dir=output_dir,
            max_concurrent_fetches=args["max_concurrent_fetches"],
            buffer_size=args["buffer_size"],
            show_progress=args["progress"],
            test_mode=args["test"],
        )

    logger.warning("Bronze stage completed successfully")
    if args["progress"]:
        logger.warning(f"Bronze data exported to: {result['storage_path']}")

    return result


def run_silver_stage(args: Dict[str, Any], bronze_result: Dict[str, Any] = None) -> Dict[str, Any]:
    """Run the silver stage of the pipeline."""
    logger.warning("Starting silver stage")

    try:
        from silver.main import process_specific_bronze_timestamp, run_silver_processing

        # Determine how to run silver processing
        if args.get("bronze_timestamp"):
            # Process specific bronze timestamp
            logger.warning(f"Processing specific bronze timestamp: {args['bronze_timestamp']}")
            result = process_specific_bronze_timestamp(bronze_timestamp=args["bronze_timestamp"])
        elif bronze_result and "storage_path" in bronze_result:
            # Use bronze result from current run
            logger.warning("Processing bronze data from current pipeline run")
            bronze_path = bronze_result["storage_path"]

            # Extract timestamp from bronze result for consistent naming
            export_timestamp = bronze_result.get("export_timestamp")

            result = run_silver_processing(
                bronze_data_path=bronze_path, export_timestamp=export_timestamp, use_latest_bronze=False
            )
        else:
            # Auto-discover latest bronze data
            logger.warning("Auto-discovering latest bronze data")
            result = run_silver_processing(use_latest_bronze=True)

        if result["success"]:
            logger.warning("Silver stage completed successfully")
            if args["progress"]:
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


def main():
    """Main pipeline execution."""
    args = parse_args()
    setup_logging(args["log_level"])

    logger.warning(f"Starting Svineflytning pipeline - Stage: {args['stage']}")

    bronze_result = None
    silver_result = None

    try:
        # Run bronze stage if needed
        if args["stage"] in ["bronze", "all"]:
            bronze_result = run_bronze_stage(args)
            if not bronze_result:
                logger.error("Bronze stage failed")
                sys.exit(1)

        # Run silver stage if needed
        if args["stage"] in ["silver", "all"]:
            silver_result = run_silver_stage(args, bronze_result)
            if not silver_result or not silver_result["success"]:
                logger.error("Silver stage failed")
                sys.exit(1)

        # Summary
        logger.warning("Pipeline completed successfully")
        if args["progress"]:
            if bronze_result:
                logger.warning(f"Bronze: {bronze_result.get('storage_path', 'N/A')}")
            if silver_result:
                logger.warning(f"Silver: {silver_result.get('output_path', 'N/A')}")
                logger.warning(f"Movements processed: {silver_result.get('processed_movements', 'N/A')}")

    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
