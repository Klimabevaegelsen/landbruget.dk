#!/usr/bin/env python3
"""
H3 PFAS Exposure Pipeline main entry point.

This pipeline creates H3-based PFAS exposure analysis by joining:
- Pesticide disaggregation data (from gold layer)
- Field geometries (from silver layer)
- BMD pesticide data with PFAS indicators (from silver layer)
- H3 hexagons at resolution 10 (~1.5 hectares per hexagon)
"""

import asyncio
import os
import sys
from datetime import datetime
from pathlib import Path

import click
from common.cli import PipelineRun, common_options
from dotenv import find_dotenv, load_dotenv
from loguru import logger

# Load environment variables (walks up to find root .env)
load_dotenv(find_dotenv(usecwd=True))
from common.secrets import init_secrets  # noqa: E402

init_secrets()

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

# Import the new refactored modules
from h3_pfas_exposure.gold import (  # noqa: E402
    run_combined_analysis,
    run_cumulative_analysis,
    run_cumulative_analysis_from_artifacts,
    run_cumulative_analysis_github_actions_optimized,
    run_cumulative_analysis_optimized,
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
    h3_resolution: str = "10",
    memory_limit: str = "14GB",
    thread_count: int = 4,
    chunk_size: int = 10000,
    include_kommune: bool = False,
    use_legacy_cumulative: bool = False,
) -> bool:
    """
    Run the H3 PFAS exposure analysis pipeline.

    Args:
        mode: Analysis mode ('h3', 'kommune', or 'cumulative')
        years: List of years to process (None for all available)
        h3_resolution: H3 resolution(s) for analysis (comma-separated for multiple)
        memory_limit: Memory limit for processing
        thread_count: Number of threads to use
        chunk_size: Chunk size for processing
        include_kommune: Include kommune analysis when running H3 mode
        use_legacy_cumulative: Use legacy cumulative analysis instead of optimized version

    Returns:
        True if successful, False otherwise
    """
    datetime.now()
    logger.info(f"🚀 Starting H3 PFAS exposure analysis pipeline in {mode} mode")

    try:
        # Parse H3 resolutions
        h3_resolutions = []
        if mode in ["h3", "cumulative"]:
            resolutions_str = h3_resolution.split(",")
            for res_str in resolutions_str:
                res = int(res_str.strip())
                if res not in [7, 8, 9, 10]:
                    logger.error(f"❌ Invalid H3 resolution: {res}. Must be 7, 8, 9, or 10")
                    return False
                h3_resolutions.append(res)
        else:
            # For kommune mode, use default resolution for any H3 operations
            h3_resolutions = [10]

        logger.info(f"📊 Configuration: {mode} mode")
        if mode in ["h3", "cumulative"]:
            logger.info(f"   🎯 H3 resolutions: {h3_resolutions}")
            if include_kommune:
                logger.info("   🏛️ Including kommune analysis")
        logger.info(
            f"⚙️ Resources: {memory_limit} memory, {thread_count} threads, {chunk_size} chunk size"
        )

        # Run analyses for each resolution
        all_success = True

        if mode == "h3":
            # Use combined analysis for efficiency (shared data loading)
            success = await run_combined_analysis(
                years=years,
                h3_resolutions=h3_resolutions,
                include_kommune=include_kommune,
            )
            all_success = success

        elif mode == "cumulative":
            # Choose between different cumulative analysis strategies
            if use_legacy_cumulative:
                logger.info("🔄 Using legacy cumulative analysis (reprocesses all data)")
                success = await run_cumulative_analysis(
                    years=years,
                    h3_resolutions=h3_resolutions,
                    include_kommune=include_kommune,
                )
            else:
                # Detect if running in GitHub Actions and check for artifacts
                is_github_actions = os.getenv("GITHUB_ACTIONS") == "true"
                artifacts_dir = "downloaded-artifacts"

                if is_github_actions and os.path.exists(artifacts_dir):
                    logger.info(
                        "📦 Using GitHub Actions artifact-based cumulative analysis (fastest - no searching)"
                    )
                    success = await run_cumulative_analysis_from_artifacts(
                        years=years,
                        h3_resolutions=h3_resolutions,
                        include_kommune=include_kommune,
                        artifacts_dir=artifacts_dir,
                    )
                elif is_github_actions:
                    logger.info(
                        "⚡ Using GitHub Actions optimized cumulative analysis (leverages fresh workflow results)"
                    )
                    success = await run_cumulative_analysis_github_actions_optimized(
                        years=years,
                        h3_resolutions=h3_resolutions,
                        include_kommune=include_kommune,
                    )
                else:
                    logger.info("⚡ Using optimized cumulative analysis (loads existing results)")
                    success = await run_cumulative_analysis_optimized(
                        years=years,
                        h3_resolutions=h3_resolutions,
                        include_kommune=include_kommune,
                    )
            all_success = success

        elif mode == "kommune":
            success = await run_multi_year_kommune_analysis(years)
            if not success:
                logger.error("❌ Kommune analysis failed")
                all_success = False
        else:
            logger.error(f"❌ Unknown analysis mode: {mode}")
            return False

        if all_success:
            logger.info("✅ All analyses completed successfully!")
            return True
        logger.error("❌ Some analyses failed!")
        return False

    except Exception as e:
        logger.error(f"❌ Pipeline failed with error: {e}")
        return False


@click.command()
@click.option(
    "--mode",
    type=click.Choice(["h3", "kommune", "cumulative"]),
    default="h3",
    help="Analysis mode (default: h3)",
)
@click.option(
    "--years",
    type=str,
    default=None,
    help="Years to process, comma-separated (default: all available years)",
)
@click.option(
    "--h3-resolution",
    default="10",
    help="H3 resolution(s) for analysis (default: 10, comma-separated for multiple: 7,8,9,10)",
)
@click.option(
    "--include-kommune", is_flag=True, help="Include kommune analysis when running H3 mode"
)
@click.option(
    "--use-legacy-cumulative",
    is_flag=True,
    help="Use legacy cumulative analysis instead of optimized version",
)
@click.option("--memory-limit", default="14GB", help="Memory limit for processing (default: 14GB)")
@click.option("--thread-count", type=int, default=4, help="Number of threads to use (default: 4)")
@click.option(
    "--chunk-size", type=int, default=10000, help="Chunk size for processing (default: 10000)"
)
@click.option("--verbose", is_flag=True, help="Enable verbose logging")
@common_options
def main(
    mode,
    years,
    h3_resolution,
    include_kommune,
    use_legacy_cumulative,
    memory_limit,
    thread_count,
    chunk_size,
    verbose,
    log_level,
):
    """H3 PFAS Exposure Analysis Pipeline."""
    # Parse years from comma-separated string for backward compat
    parsed_years = None
    if years:
        parsed_years = [int(y.strip()) for y in years.split(",")]

    # Set up logging
    if verbose:
        os.environ["LOG_LEVEL"] = "DEBUG"
    setup_logging()

    # Set up directories
    output_dir = setup_directories()

    # Initialize pipeline metadata tracking
    pipeline_run = PipelineRun("h3_pfas_exposure", logger=logger)

    logger.info("H3 PFAS Exposure Analysis Pipeline")
    logger.info(f"Mode: {mode}")
    if parsed_years:
        logger.info(f"Years: {parsed_years}")
    else:
        logger.info("Years: all available")

    # Run the pipeline
    success = False
    try:
        success = asyncio.run(
            run_pipeline(
                mode=mode,
                years=parsed_years,
                h3_resolution=h3_resolution,
                memory_limit=memory_limit,
                thread_count=thread_count,
                chunk_size=chunk_size,
                include_kommune=include_kommune,
                use_legacy_cumulative=use_legacy_cumulative,
            )
        )

        # Save pipeline metadata
        if success:
            try:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                metadata_dir = output_dir / timestamp

                pipeline_run.finish(
                    output_path=metadata_dir / "h3_pfas_exposure_metadata.json",
                    source_datasets=[
                        "pesticide",
                        "agricultural_fields",
                        "bmd_pesticide_database",
                    ],
                )
            except Exception as e:
                logger.error(f"Failed to create H3 PFAS pipeline metadata: {e}")

        if success:
            logger.info("Pipeline completed successfully!")
            sys.exit(0)
        else:
            logger.error("Pipeline failed!")
            sys.exit(1)

    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Pipeline failed with unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
