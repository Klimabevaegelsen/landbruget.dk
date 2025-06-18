#!/usr/bin/env python3
"""
BBR Buildings Pipeline - Main Entry Point

This pipeline fetches and processes Danish building data from Bygnings- og Boligregistret (BBR)
to support agricultural and public health analyses.
"""

import argparse
import logging
import sys
from pathlib import Path

from bronze.geodanmark_wfs_fetcher import GeoDanmarkWFSFetcher
from bronze.inspire_bbr_fetcher import InspireBBRFetcher
from config import Settings, get_settings
from silver.building_processor import BuildingProcessor
from utils.logger import setup_logger


def main():
    """Main entry point for the BBR buildings pipeline."""
    parser = argparse.ArgumentParser(
        description="BBR Buildings Data Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--layer",
        choices=["bronze", "silver", "both"],
        required=True,
        help="Pipeline layer to execute",
    )

    parser.add_argument(
        "--source",
        choices=["inspire_bbr", "geodanmark_wfs", "both"],
        help="Data source for bronze layer (required for bronze layer)",
    )

    parser.add_argument(
        "--input-dir", type=Path, help="Input directory (required for silver layer)"
    )

    parser.add_argument(
        "--output-dir", type=Path, default=Path("data"), help="Output directory (default: data)"
    )

    parser.add_argument("--sample-size", type=int, help="Sample size for testing")

    parser.add_argument(
        "--enhance-classification",
        action="store_true",
        help="Enable enhanced classification using GeoDanmark WFS",
    )

    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level",
    )

    args = parser.parse_args()

    # Setup logging
    logger = setup_logger(level=args.log_level)

    # Load configuration
    settings = get_settings()

    try:
        if args.layer == "bronze":
            if not args.source:
                logger.error("--source is required for bronze layer")
                sys.exit(1)

            run_bronze_layer(args, settings, logger)

        elif args.layer == "silver":
            if not args.input_dir:
                logger.error("--input-dir is required for silver layer")
                sys.exit(1)

            run_silver_layer(args, settings, logger)

        elif args.layer == "both":
            if not args.source:
                logger.error("--source is required for bronze layer")
                sys.exit(1)

            # Run bronze layer and get data in memory
            logger.info(
                "Running both layers - bronze will export and pass data to silver in memory"
            )
            bronze_data = run_bronze_layer(args, settings, logger, return_data=True)

            # Run silver layer with in-memory data
            run_silver_layer(args, settings, logger, bronze_data=bronze_data)

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)


def run_bronze_layer(
    args: argparse.Namespace, settings: Settings, logger: logging.Logger, return_data: bool = False
):
    """Execute bronze layer processing."""
    logger.info(f"Starting bronze layer for source: {args.source}")

    output_dir = args.output_dir / "bronze"
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.source == "inspire_bbr":
        fetcher = InspireBBRFetcher(settings, logger)
        result = fetcher.fetch_data(
            output_dir, sample_size=args.sample_size, return_data=return_data
        )

    elif args.source == "geodanmark_wfs":
        fetcher = GeoDanmarkWFSFetcher(settings, logger)
        result = fetcher.fetch_samples(output_dir, return_data=return_data)

    elif args.source == "both":
        # Run both sources in coordinated fashion
        logger.info("Running coordinated data sources...")
        result = None

        # First run INSPIRE BBR to get building attributes and IDs
        logger.info("Fetching INSPIRE BBR building attributes...")
        inspire_fetcher = InspireBBRFetcher(settings, logger)
        inspire_result = inspire_fetcher.fetch_data(
            output_dir, sample_size=args.sample_size, return_data=return_data
        )

        # Then run GeoDanmark WFS to get geometries for the filtered buildings
        if return_data and inspire_result:
            logger.info("Fetching GeoDanmark WFS geometries for filtered buildings...")
            geodanmark_fetcher = GeoDanmarkWFSFetcher(settings, logger)

            # Extract building IDs from INSPIRE BBR result
            # Handle the new return structure from inspire_bbr_fetcher
            if isinstance(inspire_result, dict) and "building_ids" in inspire_result:
                building_ids = inspire_result["building_ids"]
                attributes_df = inspire_result["attributes_df"]
            else:
                # Fallback for old structure
                building_ids = inspire_result.get("data", {}).get("building_ids", [])
                attributes_df = inspire_result.get("data", {}).get("attributes_df", None)

            logger.info(f"Requesting geometries for {len(building_ids):,} buildings")

            geodanmark_result = geodanmark_fetcher.fetch_building_geometries(
                output_dir, building_ids, return_data=True
            )

            # Combine both results for silver layer
            result = {
                "data": {
                    "attributes": attributes_df,
                    "geometries": geodanmark_result["geometries"] if geodanmark_result else [],
                    "building_ids": building_ids,
                },
                "metadata": {
                    "inspire_metadata": getattr(inspire_result, "metadata", None),
                    "geodanmark_metadata": geodanmark_result["metadata"]
                    if geodanmark_result
                    else None,
                    "source": "both",
                },
            }
        else:
            # If not returning data, still run GeoDanmark WFS for consistency
            logger.info("Fetching sample GeoDanmark WFS data...")
            geodanmark_fetcher = GeoDanmarkWFSFetcher(settings, logger)
            geodanmark_result = geodanmark_fetcher.fetch_samples(
                output_dir, return_data=return_data
            )

    logger.info("Bronze layer processing completed successfully")

    if return_data:
        return result
    return None


def run_silver_layer(
    args: argparse.Namespace, settings: Settings, logger: logging.Logger, bronze_data=None
):
    """Execute silver layer processing."""
    logger.info("Starting silver layer processing")

    output_dir = args.output_dir / "silver"
    output_dir.mkdir(parents=True, exist_ok=True)

    processor = BuildingProcessor(settings, logger)

    if bronze_data is not None:
        # Use data directly from bronze layer (in-memory processing)
        logger.info("Using bronze data from memory - skipping disk I/O")
        processor.process_buildings_from_data(
            bronze_data=bronze_data,
            output_dir=output_dir,
            enhance_classification=args.enhance_classification,
        )
    else:
        # Traditional mode: read from disk
        processor.process_buildings(
            input_dir=args.input_dir,
            output_dir=output_dir,
            enhance_classification=args.enhance_classification,
        )

    logger.info("Silver layer processing completed successfully")


if __name__ == "__main__":
    main()
