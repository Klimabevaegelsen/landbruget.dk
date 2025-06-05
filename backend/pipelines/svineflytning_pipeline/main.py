"""Svineflytning Pipeline for fetching and processing pig movement data."""

import argparse
import logging
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


def main():
    """Main pipeline execution."""
    args = parse_args()
    setup_logging(args["log_level"])

    logger.warning("Starting Svineflytning pipeline")
    if args["progress"]:
        logger.warning(f"Processing date range: {args['start_date']} to {args['end_date']}")
        logger.warning(f"Using {args['max_concurrent_fetches']} concurrent fetches")
        logger.warning(f"Buffer size: {args['buffer_size']} responses")

    try:
        # Get credentials and create client
        username, password = get_fvm_credentials()
        client = create_client(ENDPOINTS[args["environment"]], username, password)

        # Fetch and stream all movements
        with logging_redirect_tqdm():
            result = fetch_all_movements(
                client=client,
                start_date=args["start_date"],
                end_date=args["end_date"],
                output_dir="/data/raw/svineflytning",
                max_concurrent_fetches=args["max_concurrent_fetches"],
                buffer_size=args["buffer_size"],
                show_progress=args["progress"],
                test_mode=args["test"],
            )

        # Print information about the export
        logger.warning("Pipeline completed successfully")
        if args["progress"]:
            logger.warning(f"Data exported to: {result['storage_path']}")

    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
