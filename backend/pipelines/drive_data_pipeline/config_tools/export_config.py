#!/usr/bin/env python
"""Tool to export pipeline configuration to a JSON file.

This script allows users to export their current configuration settings to a 
JSON file that can be used with the --config-file option in the main pipeline.
"""

import argparse
import json
import os
import sys

# Add the parent directory to sys.path to enable imports
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from drive_data_pipeline.config.cli import create_config_file


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the configuration export tool.

    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Export pipeline configuration to a JSON file"
    )
    
    # Required output file
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Path to the output JSON configuration file"
    )
    
    # Optional configuration values
    parser.add_argument(
        "--subfolders",
        type=str,
        help="Specific subfolders to process (comma-separated)",
    )
    parser.add_argument(
        "--file-types",
        type=str,
        help="Specific file types to process (comma-separated, e.g., pdf,xlsx)",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        help="Process only files modified after this date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="Process only files modified before this date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--bronze-only",
        action="store_true",
        help="Run only the Bronze layer processing",
    )
    parser.add_argument(
        "--silver-only",
        action="store_true",
        help="Run only the Silver layer processing",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress non-essential output",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Set logging level",
    )
    
    return parser.parse_args()


def include_environment_settings(config: dict) -> dict:
    """Include relevant environment settings in the configuration.
    
    Args:
        config: Existing configuration dictionary
        
    Returns:
        Updated configuration dictionary with environment settings
    """
    # List of environment variables to include (if set)
    env_vars = [
        "GOOGLE_DRIVE_FOLDER_ID",
        "MAX_WORKERS",
        "LOG_LEVEL"
    ]
    
    # Add environment variables to config if they're set
    for var in env_vars:
        if var in os.environ and var.lower() not in config:
            config[var.lower()] = os.environ[var]
    
    return config


def main() -> int:
    """Main entry point for the configuration export tool.
    
    Returns:
        Exit code (0 for success, non-zero for errors)
    """
    args = parse_args()
    
    try:
        # Create configuration from command-line arguments
        create_config_file(args, args.output)
        
        # Read the created file
        with open(args.output) as f:
            config = json.load(f)
        
        # Add environment settings if requested
        config = include_environment_settings(config)
        
        # Write back to file
        with open(args.output, 'w') as f:
            json.dump(config, f, indent=2)
            
        print(f"Configuration exported to {args.output}")
        return 0
    
    except Exception as e:
        print(f"Error exporting configuration: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main()) 