#!/usr/bin/env python3
"""
H3 PFAS Exposure Pipeline - Main Entry Point

=== WHAT THIS PIPELINE DOES (FOR NON-TECHNICAL READERS) ===

This pipeline analyzes PFAS (Per- and polyfluoroalkyl substances) contamination in Danish agriculture.
PFAS are "forever chemicals" found in pesticides that don't break down naturally and can contaminate
soil, water, and food crops.

The pipeline:
1. Loads pesticide registration data to identify which products contain PFAS
2. Loads field-by-field pesticide application records from Danish farms
3. Loads agricultural field boundaries and locations
4. Creates a hexagonal grid over Denmark (H3 system) for consistent mapping
5. Calculates PFAS exposure levels for each hexagon based on pesticide usage
6. Generates maps and data files showing contamination hotspots

The results help environmental agencies, farmers, researchers, and policymakers understand
where PFAS contamination is occurring and make informed decisions about pesticide use.

=== TECHNICAL DETAILS ===

This pipeline creates H3-based PFAS exposure analysis by joining:
- Pesticide disaggregation data (from gold layer) - actual pesticide usage records
- Field geometries (from silver layer) - farm field boundaries and locations
- BMD pesticide data with PFAS indicators (from silver layer) - which pesticides contain PFAS
- H3 hexagons at resolution 10 (~1.5 hectares per hexagon) - mapping grid system

The pipeline can run in different modes:
- 'h3': Creates hexagonal grid analysis (default, most detailed)
- 'kommune': Creates municipality-level analysis (administrative boundaries)
- 'cumulative': Combines data from multiple years for long-term impact analysis
"""

import argparse
import asyncio
import os
import sys
from datetime import datetime
from pathlib import Path

import dotenv
from loguru import logger

# === ENVIRONMENT SETUP ===
# Load environment variables from .env file for local development
# In production (GitHub Actions), environment variables are set directly
env_path = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(env_path):
    dotenv.load_dotenv(env_path)
    print(f"Loaded environment variables from {env_path}")
else:
    print("No .env file found, using environment variables directly")

# Add the source code directory to Python's path so we can import our modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

# Import our analysis functions (the main workhorses of the pipeline)
from h3_pfas_exposure.gold import (
    run_combined_analysis,  # Runs multiple analysis types together
    run_cumulative_analysis,  # Combines data from multiple years
    run_multi_year_kommune_analysis,  # Municipality-level analysis
)


def setup_logging():
    """
    Configure logging to show what the pipeline is doing.

    Logging helps us track progress and debug issues. This sets up:
    - Log level (how much detail to show)
    - Log format (how messages look)
    - Colored output for easier reading
    """
    log_level = os.getenv("LOG_LEVEL", "INFO")
    logger.remove()  # Remove default handler
    logger.add(
        sys.stdout,
        level=log_level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan> | <level>{message}</level>",
        colorize=True,
    )


def setup_directories() -> Path:
    """
    Create the directories where we'll save our results.

    This ensures the output directory exists before we try to save files there.
    Returns the path where results will be saved.
    """
    output_dir = Path(os.getenv("OUTPUT_DIR", "data/gold/h3_pfas_exposure"))
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Output directory: {output_dir}")
    return output_dir


async def run_pipeline(
    mode: str = "h3",
    years: list[int] | None = None,
    h3_resolution: str = "10",
    memory_limit: str = "14GB",
    thread_count: int = 4,
    chunk_size: int = 10000,
    include_kommune: bool = False,
) -> bool:
    """
    Run the H3 PFAS exposure analysis pipeline.

    This is the main function that orchestrates the entire analysis process.

    === PARAMETERS EXPLAINED ===

    mode: What type of analysis to run
        - 'h3': Creates hexagonal grid maps (most detailed, default)
        - 'kommune': Creates municipality-level maps (administrative boundaries)
        - 'cumulative': Combines multiple years of data for long-term analysis

    years: Which years to analyze (e.g., [2022, 2023])
        - None means analyze all available years
        - Specific years like [2022] analyzes only that year
        - Data availability depends on what's in our database

    h3_resolution: How detailed the hexagonal grid should be
        - "10": ~1.5 hectares per hexagon (field-level detail, default)
        - "9": ~11 hectares per hexagon (farm-level detail)
        - "8": ~74 hectares per hexagon (regional detail)
        - "7": ~516 hectares per hexagon (county-level detail)
        - Higher resolution = more detail but slower processing

    memory_limit: How much computer memory to use
        - "14GB": Uses up to 14 gigabytes of RAM (default for GitHub Actions)
        - "8GB": Uses less memory for smaller computers
        - More memory = faster processing but needs more powerful hardware

    thread_count: How many CPU cores to use for parallel processing
        - 4: Uses 4 CPU cores (default, good for most systems)
        - 1: Uses single-threaded processing (slower but uses less resources)
        - More threads = faster processing on multi-core systems

    chunk_size: How many hexagons to process at once
        - 10000: Process 10,000 hexagons per batch (default)
        - 5000: Smaller batches use less memory but may be slower
        - Larger chunks = faster processing but more memory usage

    include_kommune: Whether to also create municipality-level analysis
        - False: Only create the requested analysis type (default)
        - True: Also create municipality-level analysis for comparison

    === WHAT HAPPENS DURING PROCESSING ===

    1. Load pesticide registration data (which products contain PFAS)
    2. Load field application data (where and how much pesticide was used)
    3. Load field boundary data (geographic locations of farms)
    4. Create hexagonal grid over Denmark
    5. For each hexagon:
       - Find which farm fields overlap with it
       - Calculate how much of each field is in the hexagon
       - Sum up PFAS exposure from all pesticide applications
       - Calculate exposure per hectare
    6. Save results as maps and data files

    Returns:
        True if analysis completed successfully, False if there were errors
    """
    pipeline_start_time = datetime.now()
    logger.info(f"🚀 Starting H3 PFAS exposure analysis pipeline in {mode} mode")

    # Log the parameters so we know what settings were used
    logger.info(f"📊 Analysis mode: {mode}")
    logger.info(f"📅 Years: {years if years else 'all available'}")
    logger.info(f"🔷 H3 resolution: {h3_resolution}")
    logger.info(f"💾 Memory limit: {memory_limit}")
    logger.info(f"🔄 Thread count: {thread_count}")
    logger.info(f"📦 Chunk size: {chunk_size}")
    logger.info(f"🏛️ Include kommune: {include_kommune}")

    try:
        # Parse H3 resolution(s) - can be single value or comma-separated list
        h3_resolutions = [int(r.strip()) for r in h3_resolution.split(",")]

        # Validate that resolutions are supported
        for res in h3_resolutions:
            if res not in [7, 8, 9, 10]:
                logger.error(f"❌ Unsupported H3 resolution: {res}. Must be 7, 8, 9, or 10.")
                return False

        # Run the appropriate analysis based on mode
        if mode == "h3":
            # Standard hexagonal grid analysis
            logger.info("🔷 Running H3 hexagonal grid analysis")
            success = await run_combined_analysis(
                years=years,
                h3_resolutions=h3_resolutions,
                include_kommune=include_kommune,
            )
        elif mode == "kommune":
            # Municipality-level analysis
            logger.info("🏛️ Running kommune (municipality) analysis")
            success = await run_multi_year_kommune_analysis(years=years)
        elif mode == "cumulative":
            # Multi-year cumulative analysis
            logger.info("📈 Running cumulative analysis across multiple years")
            success = await run_cumulative_analysis(
                years=years,
                h3_resolutions=h3_resolutions,
                include_kommune=include_kommune,
            )
        else:
            logger.error(f"❌ Unknown analysis mode: {mode}")
            return False

        # Log the final result
        pipeline_end_time = datetime.now()
        duration = pipeline_end_time - pipeline_start_time

        if success:
            logger.info(f"✅ Pipeline completed successfully in {duration}")
            logger.info("📊 Results saved to Google Cloud Storage")
            logger.info("🗺️ Maps and data files are ready for visualization")
        else:
            logger.error(f"❌ Pipeline failed after {duration}")

        return success

    except Exception as e:
        logger.error(f"❌ Pipeline failed with error: {e}")
        import traceback

        logger.error(f"Full error details: {traceback.format_exc()}")
        return False


def parse_args():
    """
    Parse command-line arguments to configure the pipeline.

    This function defines all the command-line options that users can specify
    when running the pipeline. It provides help text and default values.
    """
    parser = argparse.ArgumentParser(
        description="H3 PFAS Exposure Analysis Pipeline - Analyze PFAS contamination in Danish agriculture",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run basic hexagonal analysis for all available years
  python main.py
  
  # Run analysis for specific years only
  python main.py --years 2022 2023
  
  # Run municipality-level analysis
  python main.py --mode kommune
  
  # Run cumulative analysis combining multiple years
  python main.py --mode cumulative --years 2020 2021 2022 2023
  
  # Run with multiple H3 resolutions (creates maps at different detail levels)
  python main.py --h3-resolution "8,9,10"
  
  # Run with lower memory usage for smaller computers
  python main.py --memory-limit 8GB --thread-count 2 --chunk-size 5000

Analysis Modes:
  h3         - Hexagonal grid analysis (default, most detailed)
  kommune    - Municipality-level analysis (administrative boundaries)
  cumulative - Multi-year cumulative analysis (long-term impact)

H3 Resolution Levels:
  7  - ~516 hectares per hexagon (regional level)
  8  - ~74 hectares per hexagon (county level)
  9  - ~11 hectares per hexagon (municipal level)
  10 - ~1.5 hectares per hexagon (field level, default)
        """,
    )

    parser.add_argument(
        "--mode",
        choices=["h3", "kommune", "cumulative"],
        default="h3",
        help="Analysis mode: 'h3' for hexagonal grid (default), 'kommune' for municipalities, 'cumulative' for multi-year analysis",
    )

    parser.add_argument(
        "--years",
        type=int,
        nargs="*",
        help="Years to analyze (e.g., 2022 2023). If not specified, analyzes all available years",
    )

    parser.add_argument(
        "--h3-resolution",
        type=str,
        default="10",
        help="H3 resolution level(s) - single value or comma-separated (e.g., '10' or '8,9,10'). Higher = more detail",
    )

    parser.add_argument(
        "--memory-limit",
        type=str,
        default="14GB",
        help="Memory limit for processing (e.g., '8GB', '14GB'). More memory = faster processing",
    )

    parser.add_argument(
        "--thread-count",
        type=int,
        default=4,
        help="Number of CPU threads to use for parallel processing. More threads = faster on multi-core systems",
    )

    parser.add_argument(
        "--chunk-size",
        type=int,
        default=10000,
        help="Number of hexagons to process per batch. Larger = faster but uses more memory",
    )

    parser.add_argument(
        "--include-kommune",
        action="store_true",
        help="Also generate municipality-level analysis when running H3 mode",
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging to see more detailed progress information",
    )

    return parser.parse_args()


def main():
    """
    Main entry point for the pipeline.

    This function:
    1. Parses command-line arguments
    2. Sets up logging and directories
    3. Runs the pipeline with the specified settings
    4. Reports success or failure
    """
    args = parse_args()

    # Set up logging level based on verbose flag
    if args.verbose:
        os.environ["LOG_LEVEL"] = "DEBUG"
    setup_logging()

    # Create output directories
    setup_directories()

    # Log startup information
    logger.info("🏗️ H3 PFAS Exposure Analysis Pipeline")
    logger.info("=" * 50)
    logger.info("🧪 Analyzing PFAS contamination in Danish agriculture")
    logger.info("🗺️ Creating detailed exposure maps using hexagonal grids")
    logger.info("=" * 50)
    logger.info(f"📊 Mode: {args.mode}")
    if args.years:
        logger.info(f"📅 Years: {args.years}")
    else:
        logger.info("📅 Years: all available")
    logger.info(f"🔷 H3 resolution: {args.h3_resolution}")
    logger.info(f"💾 Memory limit: {args.memory_limit}")
    logger.info(f"🔄 Threads: {args.thread_count}")
    logger.info(f"📦 Chunk size: {args.chunk_size}")

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
                include_kommune=args.include_kommune,
            )
        )

        if success:
            logger.info("🎉 Pipeline completed successfully!")
            logger.info("📊 Check the output directory for results")
            logger.info("🗺️ Maps and data are ready for visualization")
            sys.exit(0)
        else:
            logger.error("❌ Pipeline failed!")
            logger.error("🔍 Check the logs above for error details")
            sys.exit(1)

    except KeyboardInterrupt:
        logger.warning("⚠️ Pipeline interrupted by user (Ctrl+C)")
        sys.exit(130)
    except Exception as e:
        logger.error(f"❌ Pipeline failed with unexpected error: {e}")
        logger.error("🔍 This might be a bug - please report it with the error details")
        sys.exit(1)


if __name__ == "__main__":
    main()
