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
        "--layer", choices=["bronze", "silver"], required=True, help="Pipeline layer to execute"
    )

    parser.add_argument(
        "--source",
        choices=["inspire_bbr", "geodanmark_wfs"],
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

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)


def run_bronze_layer(args: argparse.Namespace, settings: Settings, logger: logging.Logger):
    """Execute bronze layer processing."""
    logger.info(f"Starting bronze layer for source: {args.source}")

    output_dir = args.output_dir / "bronze"
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.source == "inspire_bbr":
        fetcher = InspireBBRFetcher(settings, logger)
        fetcher.fetch_data(output_dir, sample_size=args.sample_size)

    elif args.source == "geodanmark_wfs":
        fetcher = GeoDanmarkWFSFetcher(settings, logger)
        fetcher.fetch_samples(output_dir)

    logger.info("Bronze layer processing completed successfully")


def run_silver_layer(args: argparse.Namespace, settings: Settings, logger: logging.Logger):
    """Execute silver layer processing."""
    logger.info("Starting silver layer processing")

    output_dir = args.output_dir / "silver"
    output_dir.mkdir(parents=True, exist_ok=True)

    processor = BuildingProcessor(settings, logger)
    processor.process_buildings(
        input_dir=args.input_dir,
        output_dir=output_dir,
        enhance_classification=args.enhance_classification,
    )

    logger.info("Silver layer processing completed successfully")


if __name__ == "__main__":
    main()
