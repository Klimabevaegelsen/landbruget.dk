"""Test script to verify year detection works correctly."""

import asyncio
import logging
import os
import sys

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from common.storage import StorageAccess

from unified_pipeline.gold.pmtiles_generator.config import PMTilesGeneratorConfig
from unified_pipeline.gold.pmtiles_generator.year_detector import DataSourceYearDetector

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def main():
    """Test year detection."""
    logger.info("=" * 80)
    logger.info("Testing Year Detection")
    logger.info("=" * 80)

    # Create minimal config
    config = PMTilesGeneratorConfig(
        storage_bucket="landbruget-data",
        temp_dir="/tmp/pmtiles_test",
        cloudflare_r2_account_id="",  # Not needed for testing
        cloudflare_r2_access_key_id="",
        cloudflare_r2_secret_access_key="",
        cloudflare_r2_bucket="",
    )

    # Initialize GCS access (creates its own DuckDB connection) and year detector
    storage_access = StorageAccess()  # No connection parameter - creates new one
    year_detector = DataSourceYearDetector(config, storage_access)

    # Detect available years
    logger.info("\n" + "=" * 80)
    logger.info("DETECTING AVAILABLE YEARS")
    logger.info("=" * 80)
    available_years = await year_detector.detect_all_available_years()

    # Print results
    logger.info("\n" + "=" * 80)
    logger.info("RESULTS")
    logger.info("=" * 80)
    for source_name, years in available_years.items():
        logger.info(f"{source_name:25s}: {len(years):3d} years -> {sorted(years)}")

    # Get years to process
    logger.info("\n" + "=" * 80)
    logger.info("YEARS TO PROCESS")
    logger.info("=" * 80)
    years_to_process = year_detector.get_years_to_process(available_years)
    logger.info(f"Total years to process: {len(years_to_process)}")
    logger.info(f"Years: {years_to_process}")

    # Get optimal year ranges
    logger.info("\n" + "=" * 80)
    logger.info("OPTIMAL YEAR RANGES")
    logger.info("=" * 80)
    year_ranges = await year_detector.get_optimal_year_ranges()
    for range_name, (start, end, sources) in year_ranges.items():
        logger.info(
            f"{range_name:20s}: {start}-{end} ({len(sources)} sources: {', '.join(sources)})"
        )

    logger.info("\n" + "=" * 80)
    logger.info("TEST COMPLETE")
    logger.info("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
