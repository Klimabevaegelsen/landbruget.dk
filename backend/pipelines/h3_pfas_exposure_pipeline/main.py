#!/usr/bin/env python3
"""
H3 PFAS Exposure Pipeline main entry point.

This pipeline creates H3-based PFAS exposure analysis by joining:
- Pesticide disaggregation data (from gold layer)
- Field geometries (from silver layer)
- BMD pesticide data with PFAS indicators (from silver layer)
- H3 hexagons at resolution 10 (~1.5 hectares per hexagon)
"""

import argparse
import asyncio
import os
import sys
from datetime import datetime
from pathlib import Path

import dotenv
from loguru import logger

# Load environment variables
# Only load .env file if it exists (for local development)
# In GitHub Actions, environment variables are set directly
env_path = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(env_path):
    dotenv.load_dotenv(env_path)
    print(f"Loaded environment variables from {env_path}")
else:
    print("No .env file found, using environment variables directly")

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))


# Import the new refactored modules
from h3_pfas_exposure.config import H3SpatialConfig
from h3_pfas_exposure.gold import (
    run_multi_year_analysis,
    run_multi_year_kommune_analysis,
)


def setup_logging():
    """Set up logging configuration."""
    log_level = os.getenv("LOG_LEVEL", "INFO")
    logger.remove()  # Remove default handler
    logger.add(
        sys.stdout,
        level=log_level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan> | <level>{message}</level>",
        colorize=True,
    )


def setup_directories() -> Path:
    """Set up output directories and return their paths."""
    output_dir = Path(os.getenv("OUTPUT_DIR", "data/gold/h3_pfas_exposure"))
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Output directory: {output_dir}")
    return output_dir


async def run_pipeline(
    mode: str = "h3",
    years: list[int] | None = None,
    h3_resolution: int = 10,
    memory_limit: str = "14GB",
    thread_count: int = 4,
    chunk_size: int = 10000,
) -> bool:
    """
    Run the H3 PFAS exposure analysis pipeline.

    Args:
        mode: Analysis mode ('h3' or 'kommune')
        years: List of years to process (None for all available)
        h3_resolution: H3 resolution for analysis (only used in h3 mode)
        memory_limit: Memory limit for processing
        thread_count: Number of threads to use
        chunk_size: Chunk size for processing

    Returns:
        True if successful, False otherwise
    """
    pipeline_start_time = datetime.now()
    logger.info(f"🚀 Starting H3 PFAS exposure analysis pipeline in {mode} mode")

    try:
        # Create configuration optimized for GitHub Actions
        config = H3SpatialConfig(
            h3_resolution=h3_resolution,
            chunk_size=chunk_size,
            memory_limit=memory_limit,
            thread_count=thread_count,
            github_actions_mode=True,
            enable_memory_monitoring=True,
            enable_disk_monitoring=True,
            enable_time_monitoring=True,
        )

        logger.info(f"📊 Configuration: {mode} mode, resolution {h3_resolution}")
        logger.info(
            f"⚙️ Resources: {memory_limit} memory, {thread_count} threads, {chunk_size} chunk size"
        )

        # Run the appropriate analysis
        success = False
        if mode == "h3":
            success = await run_multi_year_analysis(years)
        elif mode == "kommune":
            success = await run_multi_year_kommune_analysis(years)
        else:
            logger.error(f"❌ Unknown analysis mode: {mode}")
            return False

        if success:
            logger.info(f"✅ {mode} analysis completed successfully!")
            return True
        else:
            logger.error(f"❌ {mode} analysis failed!")
            return False

    except Exception as e:
        logger.error(f"❌ Pipeline failed with error: {e}")
        return False


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="H3 PFAS Exposure Analysis Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run H3 analysis for all available years at resolution 10
  python main.py --mode h3

  # Run kommune analysis for specific years
  python main.py --mode kommune --years 2022 2023

  # Run H3 analysis with custom parameters
  python main.py --mode h3 --h3-resolution 9 --memory-limit 12GB --thread-count 2

  # Run with verbose logging
  python main.py --mode h3 --verbose --years 2022
        """,
    )

    parser.add_argument(
        "--mode",
        choices=["h3", "kommune"],
        default="h3",
        help="Analysis mode (default: h3)",
    )

    parser.add_argument(
        "--years",
        nargs="*",
        type=int,
        help="Years to process (default: all available years)",
    )

    parser.add_argument(
        "--h3-resolution",
        type=int,
        default=10,
        choices=[7, 8, 9, 10],
        help="H3 resolution for analysis (default: 10, only used in h3 mode)",
    )

    parser.add_argument(
        "--memory-limit",
        default="14GB",
        help="Memory limit for processing (default: 14GB)",
    )

    parser.add_argument(
        "--thread-count",
        type=int,
        default=4,
        help="Number of threads to use (default: 4)",
    )

    parser.add_argument(
        "--chunk-size",
        type=int,
        default=10000,
        help="Chunk size for processing (default: 10000)",
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()

    # Set up logging
    if args.verbose:
        os.environ["LOG_LEVEL"] = "DEBUG"
    setup_logging()

    # Set up directories
    setup_directories()

    logger.info("🏗️ H3 PFAS Exposure Analysis Pipeline")
    logger.info(f"📊 Mode: {args.mode}")
    if args.years:
        logger.info(f"📅 Years: {args.years}")
    else:
        logger.info("📅 Years: all available")

    # Run the pipeline
    try:
        success = asyncio.run(
            run_pipeline(
                mode=args.mode,
                years=args.years,
                h3_resolution=args.h3_resolution,
                memory_limit=args.memory_limit,
                thread_count=args.thread_count,
                chunk_size=args.chunk_size,
            )
        )

        if success:
            logger.info("🎉 Pipeline completed successfully!")
            sys.exit(0)
        else:
            logger.error("❌ Pipeline failed!")
            sys.exit(1)

    except KeyboardInterrupt:
        logger.warning("⚠️ Pipeline interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"❌ Pipeline failed with unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
