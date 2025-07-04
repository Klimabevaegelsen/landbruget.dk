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

from h3_pfas_exposure.config import H3PFASConfig  # noqa: E402
from h3_pfas_exposure.gold import H3PFASPipeline  # noqa: E402


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
    config: H3PFASConfig | None = None,
    parallel: bool = False,
) -> bool:
    """
    Run the H3 PFAS exposure pipeline.

    Args:
        mode: Analysis mode ('h3', 'kommune', or 'all')
        years: List of years to process. If None, processes all available years.
        config: Pipeline configuration. If None, loads from environment.
        parallel: If True and mode is 'all', run H3 and kommune analyses in parallel.

    Returns:
        True if successful, False otherwise
    """
    if config is None:
        config = H3PFASConfig.from_env()

    logger.info(f"Starting H3 PFAS exposure pipeline in {mode} mode")

    # Create pipeline
    pipeline = H3PFASPipeline(config)

    try:
        if mode == "h3":
            success = await pipeline.run_h3_analysis(years)
        elif mode == "kommune":
            success = await pipeline.run_kommune_analysis(years)
        elif mode == "all":
            success = await pipeline.run_all_analyses(years, parallel=parallel)
        else:
            logger.error(f"Unknown mode: {mode}")
            return False

        if success:
            logger.info("H3 PFAS exposure pipeline completed successfully")
        else:
            logger.error("H3 PFAS exposure pipeline failed")

        return success

    except Exception as e:
        logger.exception(f"Error in H3 PFAS pipeline: {e}")
        return False


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="H3 PFAS Exposure Pipeline")

    parser.add_argument(
        "--mode",
        choices=["h3", "kommune", "all"],
        default="h3",
        help="Analysis mode: 'h3' for H3 hexagon analysis, 'kommune' for municipality analysis, 'all' for both",
    )

    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        help="Years to process (e.g., --years 2020 2021 2022). If not specified, processes all available years.",
    )

    parser.add_argument(
        "--h3-resolution",
        type=int,
        default=10,
        choices=[7, 8, 9, 10],
        help="H3 resolution level (7=~516ha, 8=~74ha, 9=~11ha, 10=~1.5ha per hexagon)",
    )

    parser.add_argument(
        "--memory-limit",
        default="12GB",
        help="DuckDB memory limit (default: 12GB)",
    )

    parser.add_argument(
        "--thread-count",
        type=int,
        default=4,
        help="DuckDB thread count (default: 4)",
    )

    parser.add_argument(
        "--chunk-size",
        type=int,
        default=25000,
        help="H3 cells per processing chunk (default: 25000)",
    )

    parser.add_argument(
        "--bucket",
        default="landbrugsdata-raw-data",
        help="GCS bucket name (default: landbrugsdata-raw-data)",
    )

    parser.add_argument(
        "--output-dir",
        help="Local output directory (optional, mainly for development)",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Perform a dry run without actually processing data",
    )

    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Reduce logging output",
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Increase logging output",
    )

    parser.add_argument(
        "--parallel",
        action="store_true",
        help="Run H3 and kommune analyses in parallel when using --mode all",
    )

    return parser.parse_args()


def main():
    """Main pipeline execution."""
    args = parse_args()

    # Set up logging
    if args.quiet:
        os.environ["LOG_LEVEL"] = "WARNING"
    elif args.verbose:
        os.environ["LOG_LEVEL"] = "DEBUG"
    else:
        os.environ["LOG_LEVEL"] = "INFO"

    setup_logging()

    # Set environment variables from command line arguments
    if args.h3_resolution:
        os.environ["H3_RESOLUTION"] = str(args.h3_resolution)
    if args.memory_limit:
        os.environ["MEMORY_LIMIT"] = args.memory_limit
    if args.thread_count:
        os.environ["THREAD_COUNT"] = str(args.thread_count)
    if args.chunk_size:
        os.environ["CHUNK_SIZE"] = str(args.chunk_size)
    if args.bucket:
        os.environ["GCS_BUCKET"] = args.bucket
    if args.output_dir:
        os.environ["OUTPUT_DIR"] = args.output_dir

    # Setup directories
    output_dir = setup_directories()

    logger.info("=" * 80)
    logger.info("H3 PFAS EXPOSURE PIPELINE")
    logger.info("=" * 80)
    logger.info(f"Start time: {datetime.now()}")
    logger.info(f"Mode: {args.mode}")
    logger.info(f"Years to process: {args.years or 'all available'}")
    logger.info(f"H3 resolution: {args.h3_resolution}")
    logger.info(f"Memory limit: {args.memory_limit}")
    logger.info(f"Thread count: {args.thread_count}")
    logger.info(f"Chunk size: {args.chunk_size}")
    logger.info(f"GCS bucket: {args.bucket}")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Dry run: {args.dry_run}")
    if args.mode == "all":
        logger.info(f"Parallel execution: {args.parallel}")
    logger.info("=" * 80)

    if args.dry_run:
        logger.info("DRY RUN MODE - No actual processing will be performed")
        logger.info("Configuration validated successfully")
        return 0

    try:
        # Create config after setting environment variables
        config = H3PFASConfig.from_env()

        # Run the pipeline
        success = asyncio.run(
            run_pipeline(mode=args.mode, years=args.years, config=config, parallel=args.parallel)
        )

        if success:
            logger.info("=" * 80)
            logger.info("PIPELINE COMPLETED SUCCESSFULLY")
            logger.info(f"End time: {datetime.now()}")
            logger.info("=" * 80)
            return 0
        else:
            logger.error("=" * 80)
            logger.error("PIPELINE FAILED")
            logger.error(f"End time: {datetime.now()}")
            logger.error("=" * 80)
            return 1

    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user")
        return 130
    except Exception as e:
        logger.exception(f"Unexpected error in pipeline: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
