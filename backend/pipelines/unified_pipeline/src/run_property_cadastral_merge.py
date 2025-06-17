#!/usr/bin/env python3
"""
Runner script for the Property-Cadastral merge pipeline.

This script merges property owners data with cadastral parcels data using BFE number joins.
It reads data from the silver layer and outputs the merged results.

Usage:
    python run_property_cadastral_merge.py [--output-dataset DATASET_NAME]

Environment Variables:
    GCS_BUCKET: Google Cloud Storage bucket name
    SAVE_LOCAL: Whether to save data locally (default: False)
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

# Add the src directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

from unified_pipeline.silver.property_cadastral_merge import (
    PropertyCadastralMerge,
    PropertyCadastralMergeConfig,
)
from unified_pipeline.util.gcs_util import GCSUtil

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("property_cadastral_merge.log"),
    ],
)

logger = logging.getLogger(__name__)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Merge property owners data with cadastral parcels"
    )

    parser.add_argument(
        "--join-method",
        type=str,
        choices=["inner"],
        default="inner",
        help="BFE join method - only 'inner' is supported (default: inner)",
    )

    parser.add_argument(
        "--validate-bfe",
        action="store_true",
        default=True,
        help="Validate BFE number format and consistency (default: True)",
    )

    parser.add_argument(
        "--include-metadata",
        action="store_true",
        default=True,
        help="Include merge metadata in output (default: True)",
    )

    parser.add_argument(
        "--property-owners-path",
        type=str,
        default="silver/property_owners/",
        help="Path to property owners data in GCS (default: silver/property_owners/)",
    )

    parser.add_argument(
        "--cadastral-path",
        type=str,
        default="silver/cadastral/",
        help="Path to cadastral data in GCS (default: silver/cadastral/)",
    )

    parser.add_argument(
        "--output-dataset",
        type=str,
        default="property_cadastral_merged",
        help="Output dataset name (default: property_cadastral_merged)",
    )

    parser.add_argument(
        "--dry-run", action="store_true", help="Perform dry run without saving results"
    )

    return parser.parse_args()


def create_config(args) -> PropertyCadastralMergeConfig:
    """Create configuration from command line arguments."""

    config_dict = {
        "dataset": args.output_dataset,
        "join_method": args.join_method,
        "validate_bfe_numbers": args.validate_bfe,
        "include_merge_metadata": args.include_metadata,
        "property_owners_silver_path": args.property_owners_path,
        "cadastral_silver_path": args.cadastral_path,
    }

    return PropertyCadastralMergeConfig(**config_dict)


async def main():
    """Main function to run the property-cadastral merge pipeline."""

    logger.info("Starting Property-Cadastral Merge Pipeline")

    try:
        # Parse arguments
        args = parse_args()

        # Create configuration
        config = create_config(args)

        logger.info("Configuration:")
        logger.info(f"  Dataset: {config.dataset}")
        logger.info(f"  BFE join method: {config.join_method}")
        logger.info(f"  Validate BFE numbers: {config.validate_bfe_numbers}")
        logger.info(f"  Include merge metadata: {config.include_merge_metadata}")
        logger.info(f"  Property owners path: {config.property_owners_silver_path}")
        logger.info(f"  Cadastral path: {config.cadastral_silver_path}")
        logger.info(f"  GCS bucket: {config.bucket}")
        logger.info(f"  Dry run: {args.dry_run}")

        if args.dry_run:
            logger.info("DRY RUN MODE - No data will be saved")

        # Initialize GCS utility
        gcs_util = GCSUtil()

        # Create and run the merge pipeline
        merge_pipeline = PropertyCadastralMerge(config, gcs_util)

        if args.dry_run:
            # For dry run, we'll just validate the inputs exist
            logger.info("Validating input data availability...")

            # Check property owners data
            property_files = gcs_util.list_files(
                bucket_name=config.bucket, prefix=config.property_owners_silver_path
            )

            if not property_files:
                logger.error(
                    f"No property owners files found at {config.property_owners_silver_path}"
                )
                return False

            latest_property_file = max(property_files, key=lambda x: x.time_created)
            logger.info(f"Latest property owners file: {latest_property_file.name}")

            # Check cadastral data
            cadastral_files = gcs_util.list_files(
                bucket_name=config.bucket, prefix=config.cadastral_silver_path
            )

            if not cadastral_files:
                logger.error(f"No cadastral files found at {config.cadastral_silver_path}")
                return False

            latest_cadastral_file = max(cadastral_files, key=lambda x: x.time_created)
            logger.info(f"Latest cadastral file: {latest_cadastral_file.name}")

            logger.info("✅ Dry run completed successfully - all input data is available")
            return True
        else:
            # Run the actual merge
            await merge_pipeline.run()
            logger.info("✅ Property-Cadastral merge completed successfully")
            return True

    except Exception as e:
        logger.error(f"❌ Property-Cadastral merge failed: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
