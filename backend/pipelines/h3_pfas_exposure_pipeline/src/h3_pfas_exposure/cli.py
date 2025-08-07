#!/usr/bin/env python3
"""
CLI for H3 PFAS Processor - Refactored Version
"""

import argparse
import asyncio
import sys

from loguru import logger

from .gold.analysis_functions import (
    run_multi_year_analysis,
    run_multi_year_kommune_analysis,
    test_refactored_processor,
)


async def main():
    """Main CLI function."""
    parser = argparse.ArgumentParser(description="H3 PFAS Processor - Refactored Version")
    parser.add_argument(
        "--mode",
        choices=["test", "gcs", "years", "kommune"],
        default="test",
        help="Mode: 'test' for local test data, 'gcs' for all years from GCS, 'years' for specific years, 'kommune' for kommune-level analysis",
    )
    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        help="Specific years to process (e.g., --years 2020 2021 2022)",
    )
    parser.add_argument(
        "--memory-limit", default="12GB", help="DuckDB memory limit (default: 12GB)"
    )
    parser.add_argument(
        "--thread-count", type=int, default=4, help="DuckDB thread count (default: 4)"
    )
    parser.add_argument(
        "--h3-resolution", type=int, default=10, help="H3 resolution level (default: 10)"
    )

    args = parser.parse_args()

    if args.mode == "test":
        logger.info("🧪 Running local test mode")
        success = await test_refactored_processor()
    elif args.mode == "gcs":
        logger.info("☁️ Running H3 GCS mode for all available years")
        success = await run_multi_year_analysis()
    elif args.mode == "years":
        if not args.years:
            logger.error("❌ --years must be specified when using 'years' mode")
            sys.exit(1)
        logger.info(f"☁️ Running H3 GCS mode for years: {args.years}")
        success = await run_multi_year_analysis(args.years)
    elif args.mode == "kommune":
        if args.years:
            logger.info(f"🏛️ Running kommune analysis for years: {args.years}")
            success = await run_multi_year_kommune_analysis(args.years)
        else:
            logger.info("🏛️ Running kommune analysis for all available years")
            success = await run_multi_year_kommune_analysis()
    else:
        logger.error(f"❌ Unknown mode: {args.mode}")
        sys.exit(1)

    if success:
        logger.info("✅ Analysis completed successfully!")
        sys.exit(0)
    else:
        logger.error("❌ Analysis failed!")
        sys.exit(1)


if __name__ == "__main__":
    # Run the main function
    asyncio.run(main())
