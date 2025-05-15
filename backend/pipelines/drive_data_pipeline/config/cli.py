"""Command-line interface for Google Drive Data Pipeline."""

import argparse
import datetime
import json
from pathlib import Path


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

    # Configuration
    parser.add_argument(
        "--config-file",
        type=str,
        help="Path to JSON configuration file with pipeline parameters",
        default=None,
    )

    # Other options
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Set logging level",
        default=None,
    )

    # Parse args
    args = parser.parse_args()
    
    # Handle config file if specified
    if args.config_file:
        args = merge_config_file(args)
        
    return args


def merge_config_file(args: argparse.Namespace) -> argparse.Namespace:
    """Merge configuration from file with command-line arguments.
    
    Command-line arguments take precedence over config file settings.
    
    Args:
        args: Parsed command-line arguments
        
    Returns:
        Updated arguments with config file settings merged in
    """
    config_path = Path(args.config_file)
    if not config_path.exists():
        print(f"Warning: Config file not found at {args.config_file}")
        return args
        
    try:
        with open(config_path) as f:
            config = json.load(f)
            
        # Convert config dict to namespace, preserving command line args
        args_dict = vars(args)
        for key, value in config.items():
            # Only use config value if not provided in command line
            if key in args_dict and args_dict[key] is None:
                args_dict[key] = value
                
        # Handle special cases for date fields
        if "start_date" in config and args.start_date is None:
            args_dict["start_date"] = datetime.datetime.strptime(
                config["start_date"], "%Y-%m-%d"
            ).date()
            
        if "end_date" in config and args.end_date is None:
            args_dict["end_date"] = datetime.datetime.strptime(
                config["end_date"], "%Y-%m-%d"
            ).date()
                
        return argparse.Namespace(**args_dict)
    except (json.JSONDecodeError, TypeError) as e:
        print(f"Error reading config file: {e}")
        return args


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


def create_config_file(args: argparse.Namespace, file_path: str) -> None:
    """Create a configuration file from the current arguments.
    
    Args:
        args: Current arguments
        file_path: Path to save the configuration file
    """
    config = {}
    args_dict = vars(args)
    
    # Add all non-None and non-default values to config
    for key, value in args_dict.items():
        if value is not None and key != "config_file":
            # Handle special types
            if isinstance(value, datetime.date):
                config[key] = value.strftime("%Y-%m-%d")
            else:
                config[key] = value
    
    # Save to file
    with open(file_path, 'w') as f:
        json.dump(config, f, indent=2) 