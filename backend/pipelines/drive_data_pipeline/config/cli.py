"""Command-line interface for Google Drive Data Pipeline."""

import argparse
import datetime


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the pipeline.

    Returns:
        argparse.Namespace: Parsed command-line arguments
    """
    parser = argparse.ArgumentParser(description="Google Drive Data Pipeline")

    # Folder and file selection
    parser.add_argument(
        "--subfolders",
        type=str,
        help="Specific subfolders to process (comma-separated)",
        default=None,
    )
    parser.add_argument(
        "--file-types",
        type=str,
        help="Specific file types to process (comma-separated, e.g., pdf,xlsx)",
        default=None,
    )

    # Date filtering
    parser.add_argument(
        "--start-date",
        type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d").date(),
        help="Process only files modified after this date (YYYY-MM-DD)",
        default=None,
    )
    parser.add_argument(
        "--end-date",
        type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d").date(),
        help="Process only files modified before this date (YYYY-MM-DD)",
        default=None,
    )

    # Processing flags
    parser.add_argument(
        "--bronze-only",
        action="store_true",
        help="Run only the Bronze layer processing",
    )
    parser.add_argument(
        "--silver-only",
        action="store_true",
        help="Run only the Silver layer processing (requires existing Bronze data)",
    )

    # Output options
    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output with detailed progress information",
    )
    output_group.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress non-essential output messages",
    )

    # Other options
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Set logging level",
        default=None,
    )

    # Parse and return args
    return parser.parse_args()


def split_comma_separated(value: str | None) -> list[str] | None:
    """Split a comma-separated string into a list of strings.

    Args:
        value: Comma-separated string to split or None

    Returns:
        List of strings or None if input is None
    """
    if value is None:
        return None
    return [item.strip() for item in value.split(",")]
