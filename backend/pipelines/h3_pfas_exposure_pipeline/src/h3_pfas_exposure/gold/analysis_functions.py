"""
Standalone analysis functions for H3 PFAS exposure analysis.
"""

from pathlib import Path

from loguru import logger

from ..config import H3SpatialConfig
from .processor import H3PFASProcessorRefactored


async def run_multi_year_analysis(years: list[int] | None = None, h3_resolution: int = 10) -> bool:
    """Run multi-year H3 PFAS analysis from GCS data."""
    logger.info("🚀 Starting multi-year H3 PFAS-containing active ingredient analysis from GCS")

    # Create configuration optimized for GitHub Actions free tier (16GB RAM, 4 CPUs)
    config = H3SpatialConfig(
        h3_resolution=h3_resolution,
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


async def run_combined_analysis(
    years: list[int] | None = None,
    h3_resolutions: list[int] | None = None,
    include_kommune: bool = False,
) -> bool:
    """Run combined H3 and kommune analysis with shared data loading to avoid redundancy."""
    logger.info("🚀 Starting combined H3 and kommune PFAS analysis with shared data loading")

    if h3_resolutions is None:
        h3_resolutions = [10]

    logger.info(f"   🎯 H3 resolutions: {h3_resolutions}")
    if include_kommune:
        logger.info("   🏛️ Including kommune analysis")

    # Use the highest resolution for the base configuration
    base_resolution = max(h3_resolutions)

    # Create configuration optimized for GitHub Actions free tier (16GB RAM, 4 CPUs)
    config = H3SpatialConfig(
        h3_resolution=base_resolution,
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
        # Setup DuckDB once for all analyses
        processor.setup_duckdb()

        # Import dependencies
        from .data_loader import H3DataLoader
        from .result_saver import H3ResultSaver

        # Initialize components
        data_loader = H3DataLoader(processor.conn, processor.config, processor.gcs_access)
        result_saver = H3ResultSaver(processor.conn, processor.config, processor.gcs_access)

        # Load BMD data once for all analyses (shared across all resolutions and modes)
        logger.info("📊 Loading shared BMD data for all analyses")
        bmd_table = data_loader.load_bmd_data_from_gcs()
        processor._protect_table(bmd_table)

        # Load kommune boundaries once if needed
        kommune_table = None
        if include_kommune:
            logger.info("📊 Loading shared kommune boundaries")
            kommune_table = processor._load_kommune_boundaries()
            processor._protect_table(kommune_table)

        # Determine years to process
        if years is None:
            years = data_loader.get_available_years()
            logger.info(f"📅 Processing all available years: {years}")
        else:
            logger.info(f"📅 Processing specified years: {years}")

        all_success = True

        # Process each year
        for year in years:
            logger.info(f"📅 Processing year {year} with all requested analyses")

            # Load year-specific data once
            logger.info(f"📊 Loading shared data for year {year}")
            field_year = year + 1
            fields_table = data_loader.load_and_prepare_fields_from_gcs(field_year, year)
            fields_table = processor.coordinate_transformer.prepare_geometries(fields_table)

            pesticide_table = data_loader.load_pesticide_disaggregation_from_gcs(year)
            pesticide_pfas_table = data_loader.join_pesticide_with_bmd_pfas(
                pesticide_table, bmd_table, year
            )

            # Run H3 analysis for each resolution
            for resolution in h3_resolutions:
                logger.info(f"🔷 Running H3 analysis at resolution {resolution} for year {year}")

                # Update processor resolution for this analysis
                processor.config.update_resolution(resolution)

                # Generate H3 grid for this resolution (cached)
                h3_grid_table = processor.generate_h3_grid()

                # Perform spatial join
                results_table = processor.spatial_joiner.perform_chunked_spatial_join(
                    h3_grid_table, fields_table, pesticide_pfas_table, year
                )

                # Validate and save results
                processor._validate_results(results_table)
                result_count = result_saver.save_year_results_kepler_compatible(results_table, year)

                if result_count > 0:
                    logger.info(
                        f"✅ H3 analysis completed for year {year}, resolution {resolution}: {result_count:,} records"
                    )
                else:
                    logger.error(f"❌ H3 analysis failed for year {year}, resolution {resolution}")
                    all_success = False

            # Run kommune analysis if requested
            if include_kommune:
                logger.info(f"🏛️ Running kommune analysis for year {year}")

                # Reset processor resolution to base
                processor.config.update_resolution(base_resolution)

                # Perform kommune spatial join
                kommune_results_table = processor._perform_kommune_spatial_join(
                    kommune_table, fields_table, pesticide_pfas_table, year
                )

                # Save kommune results
                result_count = result_saver.save_kommune_results(kommune_results_table, year)

                if result_count > 0:
                    logger.info(
                        f"✅ Kommune analysis completed for year {year}: {result_count:,} records"
                    )
                else:
                    logger.error(f"❌ Kommune analysis failed for year {year}")
                    all_success = False

            # Clean up year-specific tables
            processor._cleanup_year_tables(year)

        # Final cleanup
        processor._aggressive_cleanup()

        if all_success:
            logger.success("✅ Combined analysis completed successfully")
        else:
            logger.error("❌ Some analyses failed")
        return all_success

    except Exception as e:
        logger.error(f"❌ Error in combined analysis: {e}")
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
