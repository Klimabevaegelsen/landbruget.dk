"""
Standalone analysis functions for H3 PFAS exposure analysis.
"""

from pathlib import Path

from loguru import logger

from ..config import H3SpatialConfig
from .processor import H3PFASProcessorRefactored


async def run_multi_year_analysis(years: list[int] | None = None) -> bool:
    """Run multi-year H3 PFAS analysis from GCS data."""
    logger.info("🚀 Starting multi-year H3 PFAS-containing active ingredient analysis from GCS")

    # Create configuration optimized for GitHub Actions free tier (16GB RAM, 4 CPUs)
    config = H3SpatialConfig(
        h3_resolution=10,
        chunk_size=10000,  # Increased for 16GB RAM capacity
        memory_limit="14GB",  # Utilize most of the 16GB available
        thread_count=4,  # Use all 4 CPU cores
        github_actions_mode=True,
        enable_memory_monitoring=True,
        enable_disk_monitoring=True,
        enable_time_monitoring=True,
        aggressive_cleanup=True,
        duckdb_memory_limit="12GB",  # Generous DuckDB memory allocation
        duckdb_threads=4,  # Use all cores for DuckDB
    )

    processor = H3PFASProcessorRefactored(config, local_data_dir=None)

    try:
        success = await processor.run_analysis_multi_year(years)
        if success:
            logger.success("✅ Multi-year H3 PFAS analysis completed successfully")
        else:
            logger.error("❌ Multi-year H3 PFAS analysis failed")
        return success
    except Exception as e:
        logger.error(f"❌ Error in multi-year analysis: {e}")
        return False


async def run_multi_year_kommune_analysis(years: list[int] | None = None) -> bool:
    """Run multi-year kommune-level PFAS analysis from GCS data."""
    logger.info("🚀 Starting multi-year kommune-level PFAS analysis from GCS")

    # Create configuration optimized for GitHub Actions free tier (16GB RAM, 4 CPUs)
    config = H3SpatialConfig(
        h3_resolution=10,
        chunk_size=15000,  # Larger chunks for kommune analysis with 16GB RAM
        memory_limit="14GB",  # Utilize most of the 16GB available
        thread_count=4,  # Use all 4 CPU cores
        github_actions_mode=True,
        enable_memory_monitoring=True,
        enable_disk_monitoring=True,
        enable_time_monitoring=True,
        aggressive_cleanup=True,
        duckdb_memory_limit="12GB",  # Generous DuckDB memory allocation
        duckdb_threads=4,  # Use all cores for DuckDB
    )

    processor = H3PFASProcessorRefactored(config, local_data_dir=None)

    try:
        success = await processor.run_kommune_analysis_multi_year(years)
        if success:
            logger.success("✅ Multi-year kommune PFAS analysis completed successfully")
        else:
            logger.error("❌ Multi-year kommune PFAS analysis failed")
        return success
    except Exception as e:
        logger.error(f"❌ Error in multi-year kommune analysis: {e}")
        return False


async def test_refactored_processor(test_data_dir: Path | str | None = None) -> bool:
    """Test the refactored processor with local data."""
    logger.info("🧪 Testing refactored H3 PFAS processor with local data")

    # Create test configuration optimized for GitHub Actions (16GB RAM, 4 CPUs)
    config = H3SpatialConfig(
        h3_resolution=10,
        chunk_size=5000,  # Smaller chunks for testing
        memory_limit="14GB",  # Full capacity available
        thread_count=4,  # Use all 4 CPU cores
        github_actions_mode=True,
        enable_memory_monitoring=True,
        enable_disk_monitoring=True,
        enable_time_monitoring=True,
        aggressive_cleanup=True,
        duckdb_memory_limit="12GB",  # Generous DuckDB memory allocation
        duckdb_threads=4,  # Use all cores for DuckDB
    )

    if test_data_dir:
        test_data_path = Path(test_data_dir)
    else:
        test_data_path = Path("data") / "test"

    processor = H3PFASProcessorRefactored(config, local_data_dir=test_data_path)

    try:
        # Use the run_analysis method for local testing
        results_table = await processor.run_analysis(year=2022)
        if results_table:
            logger.success("✅ Refactored processor test completed successfully")
            return True
        else:
            logger.error("❌ Refactored processor test failed")
            return False
    except Exception as e:
        logger.error(f"❌ Error in processor test: {e}")
        return False
