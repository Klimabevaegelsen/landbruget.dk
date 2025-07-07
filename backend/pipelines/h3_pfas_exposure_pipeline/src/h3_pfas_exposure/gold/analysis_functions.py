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


async def run_cumulative_analysis(
    years: list[int] | None = None,
    h3_resolutions: list[int] | None = None,
    include_kommune: bool = False,
) -> bool:
    """
    Run cumulative analysis that aggregates pesticide data across all years.

    This creates a 'total' dataset that sums up pesticide usage from all available years,
    useful for showing cumulative impact over time in the frontend.

    Args:
        years: List of years to include in cumulative analysis (None = all available)
        h3_resolutions: List of H3 resolutions to generate (default: [10])
        include_kommune: Whether to include kommune-level cumulative analysis

    Returns:
        bool: True if successful, False otherwise
    """
    logger.info("🚀 Starting cumulative H3 PFAS analysis (sum across all years)")

    if h3_resolutions is None:
        h3_resolutions = [10]

    logger.info(f"   🎯 H3 resolutions: {h3_resolutions}")
    if include_kommune:
        logger.info("   🏛️ Including kommune cumulative analysis")

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

        # Load BMD data once for all analyses
        logger.info("📊 Loading shared BMD data for cumulative analysis")
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
            logger.info(f"📅 Creating cumulative analysis from all available years: {years}")
        else:
            logger.info(f"📅 Creating cumulative analysis from specified years: {years}")

        if not years:
            logger.error("❌ No years available for cumulative analysis")
            return False

        # Load and aggregate data from all years
        logger.info("📊 Loading and aggregating data from all years")

        # Create cumulative tables for each H3 resolution
        for resolution in h3_resolutions:
            logger.info(f"🔷 Creating cumulative H3 analysis at resolution {resolution}")

            # Update processor resolution for this analysis
            processor.config.update_resolution(resolution)

            # Generate H3 grid for this resolution
            h3_grid_table = processor.generate_h3_grid()

            # Initialize cumulative results table
            cumulative_table = f"cumulative_h3_results_res{resolution}"
            processor.conn.execute(f"""
                CREATE OR REPLACE TABLE {cumulative_table} AS
                SELECT 
                    h3_cell,
                    center_lat,
                    center_lon,
                    h3_area_ha,
                    CAST(0.0 AS DOUBLE) as total_intersection_area_ha,
                    CAST(0.0 AS DOUBLE) as actual_coverage_ratio,
                    0 as unique_field_count,
                    CAST(0.0 AS DOUBLE) as total_pfas_containing_active_ingredient_grams,
                    CAST(0.0 AS DOUBLE) as total_diquat_containing_active_ingredient_grams,
                    CAST(0.0 AS DOUBLE) as total_glyphosate_containing_active_ingredient_grams,
                    CAST(0.0 AS DOUBLE) as total_pesticide_belastning,
                    CAST(0.0 AS DOUBLE) as total_pfas_pesticide_belastning,
                    CAST(0.0 AS DOUBLE) as total_diquat_pesticide_belastning,
                    CAST(0.0 AS DOUBLE) as total_glyphosate_pesticide_belastning,
                    0 as total_pesticide_applications,
                    0 as pfas_containing_applications,
                    0 as diquat_containing_applications,
                    0 as glyphosate_containing_applications,
                    '' as crop_types,
                    0 as crop_diversity,
                    CAST(0.0 AS DOUBLE) as pfas_containing_active_ingredient_intensity_grams_per_ha,
                    CAST(0.0 AS DOUBLE) as diquat_containing_active_ingredient_intensity_grams_per_ha,
                    CAST(0.0 AS DOUBLE) as glyphosate_containing_active_ingredient_intensity_grams_per_ha,
                    CAST(0.0 AS DOUBLE) as pesticide_belastning_per_ha,
                    CURRENT_TIMESTAMP as created_at
                FROM {h3_grid_table}
                WHERE FALSE  -- Start with empty table
            """)

            # Aggregate data from each year
            for year in years:
                logger.info(f"   📅 Adding year {year} to cumulative analysis")

                # Load year-specific data
                field_year = year + 1
                fields_table = data_loader.load_and_prepare_fields_from_gcs(field_year, year)
                fields_table = processor.coordinate_transformer.prepare_geometries(fields_table)

                pesticide_table = data_loader.load_pesticide_disaggregation_from_gcs(year)
                pesticide_pfas_table = data_loader.join_pesticide_with_bmd_pfas(
                    pesticide_table, bmd_table, year
                )

                # Perform spatial join for this year
                year_results_table = processor.spatial_joiner.perform_chunked_spatial_join(
                    h3_grid_table, fields_table, pesticide_pfas_table, year
                )

                # Add this year's data to cumulative results
                processor.conn.execute(f"""
                    INSERT INTO {cumulative_table}
                    SELECT 
                        h3_cell,
                        center_lat,
                        center_lon,
                        h3_area_ha,
                        total_intersection_area_ha,
                        actual_coverage_ratio,
                        unique_field_count,
                        total_pfas_containing_active_ingredient_grams,
                        total_diquat_containing_active_ingredient_grams,
                        total_glyphosate_containing_active_ingredient_grams,
                        total_pesticide_belastning,
                        total_pfas_pesticide_belastning,
                        total_diquat_pesticide_belastning,
                        total_glyphosate_pesticide_belastning,
                        total_pesticide_applications,
                        pfas_containing_applications,
                        diquat_containing_applications,
                        glyphosate_containing_applications,
                        crop_types,
                        crop_diversity,
                        pfas_containing_active_ingredient_intensity_grams_per_ha,
                        diquat_containing_active_ingredient_intensity_grams_per_ha,
                        glyphosate_containing_active_ingredient_intensity_grams_per_ha,
                        pesticide_belastning_per_ha,
                        created_at
                    FROM {year_results_table}
                """)

                # Clean up year-specific tables
                processor._cleanup_year_tables(year)

            # Aggregate the cumulative data by H3 cell
            final_cumulative_table = f"final_cumulative_h3_res{resolution}"
            processor.conn.execute(f"""
                CREATE OR REPLACE TABLE {final_cumulative_table} AS
                SELECT
                    h3_cell,
                    ANY_VALUE(center_lat) as center_lat,
                    ANY_VALUE(center_lon) as center_lon,
                    ANY_VALUE(h3_area_ha) as h3_area_ha,
                    MAX(total_intersection_area_ha) as total_intersection_area_ha,
                    MAX(actual_coverage_ratio) as actual_coverage_ratio,
                    MAX(unique_field_count) as unique_field_count,
                    SUM(total_pfas_containing_active_ingredient_grams) as total_pfas_containing_active_ingredient_grams,
                    SUM(total_diquat_containing_active_ingredient_grams) as total_diquat_containing_active_ingredient_grams,
                    SUM(total_glyphosate_containing_active_ingredient_grams) as total_glyphosate_containing_active_ingredient_grams,
                    SUM(total_pesticide_belastning) as total_pesticide_belastning,
                    SUM(total_pfas_pesticide_belastning) as total_pfas_pesticide_belastning,
                    SUM(total_diquat_pesticide_belastning) as total_diquat_pesticide_belastning,
                    SUM(total_glyphosate_pesticide_belastning) as total_glyphosate_pesticide_belastning,
                    SUM(total_pesticide_applications) as total_pesticide_applications,
                    SUM(pfas_containing_applications) as pfas_containing_applications,
                    SUM(diquat_containing_applications) as diquat_containing_applications,
                    SUM(glyphosate_containing_applications) as glyphosate_containing_applications,
                    STRING_AGG(DISTINCT crop_types, '; ') as crop_types,
                    MAX(crop_diversity) as crop_diversity,
                    -- Recalculate intensity based on cumulative totals
                    CASE 
                        WHEN MAX(total_intersection_area_ha) > 0 THEN 
                            SUM(total_pfas_containing_active_ingredient_grams) / MAX(total_intersection_area_ha)
                        ELSE 0 
                    END as pfas_containing_active_ingredient_intensity_grams_per_ha,
                    CASE 
                        WHEN MAX(total_intersection_area_ha) > 0 THEN 
                            SUM(total_diquat_containing_active_ingredient_grams) / MAX(total_intersection_area_ha)
                        ELSE 0 
                    END as diquat_containing_active_ingredient_intensity_grams_per_ha,
                    CASE 
                        WHEN MAX(total_intersection_area_ha) > 0 THEN 
                            SUM(total_glyphosate_containing_active_ingredient_grams) / MAX(total_intersection_area_ha)
                        ELSE 0 
                    END as glyphosate_containing_active_ingredient_intensity_grams_per_ha,
                    CASE 
                        WHEN MAX(total_intersection_area_ha) > 0 THEN 
                            SUM(total_pesticide_belastning) / MAX(total_intersection_area_ha)
                        ELSE 0 
                    END as pesticide_belastning_per_ha,
                    CURRENT_TIMESTAMP as created_at
                FROM {cumulative_table}
                GROUP BY h3_cell
                HAVING SUM(total_pesticide_belastning) > 0 OR SUM(total_pfas_containing_active_ingredient_grams) > 0
            """)

            # Validate cumulative results
            processor._validate_results(final_cumulative_table)

            # Save cumulative results with special "total" year identifier
            result_count = result_saver.save_cumulative_results(
                final_cumulative_table, resolution, years
            )

            if result_count > 0:
                logger.info(
                    f"✅ Cumulative H3 analysis completed for resolution {resolution}: {result_count:,} records"
                )
            else:
                logger.error(f"❌ Cumulative H3 analysis failed for resolution {resolution}")
                return False

        # Handle kommune cumulative analysis if requested
        if include_kommune:
            logger.info("🏛️ Creating cumulative kommune analysis")

            # Reset processor resolution to base
            processor.config.update_resolution(base_resolution)

            # Initialize cumulative kommune results table
            cumulative_kommune_table = "cumulative_kommune_results"
            processor.conn.execute(f"""
                CREATE OR REPLACE TABLE {cumulative_kommune_table} AS
                SELECT 
                    kommune_code,
                    kommune_name,
                    region_code,
                    CAST(0.0 AS DOUBLE) as kommune_area_ha,
                    CAST(0.0 AS DOUBLE) as kommune_centroid_x,
                    CAST(0.0 AS DOUBLE) as kommune_centroid_y,
                    CAST(0.0 AS DOUBLE) as total_agricultural_area_ha,
                    0 as unique_field_count,
                    0 as unique_company_count,
                    CAST(0.0 AS DOUBLE) as avg_field_coverage_ratio,
                    CAST(0.0 AS DOUBLE) as max_field_coverage_ratio,
                    CAST(0.0 AS DOUBLE) as min_field_coverage_ratio,
                    0 as crop_diversity,
                    '' as crop_types,
                    CAST(0.0 AS DOUBLE) as total_pfas_containing_active_ingredient_grams,
                    CAST(0.0 AS DOUBLE) as total_diquat_containing_active_ingredient_grams,
                    CAST(0.0 AS DOUBLE) as total_glyphosate_containing_active_ingredient_grams,
                    CAST(0.0 AS DOUBLE) as total_pesticide_belastning,
                    CAST(0.0 AS DOUBLE) as total_pfas_pesticide_belastning,
                    CAST(0.0 AS DOUBLE) as total_diquat_pesticide_belastning,
                    CAST(0.0 AS DOUBLE) as total_glyphosate_pesticide_belastning,
                    0 as total_pesticide_applications,
                    0 as pfas_containing_applications,
                    0 as diquat_containing_applications,
                    0 as glyphosate_containing_applications,
                    0 as unique_pfas_products,
                    0 as unique_diquat_products,
                    0 as unique_glyphosate_products,
                    0 as unique_pesticide_products,
                    CAST(0.0 AS DOUBLE) as pfas_containing_active_ingredient_intensity_grams_per_ha,
                    CAST(0.0 AS DOUBLE) as diquat_containing_active_ingredient_intensity_grams_per_ha,
                    CAST(0.0 AS DOUBLE) as glyphosate_containing_active_ingredient_intensity_grams_per_ha,
                    CAST(0.0 AS DOUBLE) as pesticide_belastning_per_ha,
                    CAST(0.0 AS DOUBLE) as pfas_pesticide_belastning_per_ha,
                    CAST(0.0 AS DOUBLE) as diquat_pesticide_belastning_per_ha,
                    CAST(0.0 AS DOUBLE) as glyphosate_pesticide_belastning_per_ha,
                    CAST(0.0 AS DOUBLE) as agricultural_coverage_pct,
                    CURRENT_TIMESTAMP as created_at
                FROM {kommune_table}
                WHERE FALSE  -- Start with empty table
            """)

            # Aggregate kommune data from each year
            for year in years:
                logger.info(f"   📅 Adding year {year} to cumulative kommune analysis")

                # Load year-specific data
                field_year = year + 1
                fields_table = data_loader.load_and_prepare_fields_from_gcs(field_year, year)
                fields_table = processor.coordinate_transformer.prepare_geometries(fields_table)

                pesticide_table = data_loader.load_pesticide_disaggregation_from_gcs(year)
                pesticide_pfas_table = data_loader.join_pesticide_with_bmd_pfas(
                    pesticide_table, bmd_table, year
                )

                # Perform kommune spatial join for this year
                year_kommune_results = processor._perform_kommune_spatial_join(
                    kommune_table, fields_table, pesticide_pfas_table, year
                )

                # Add this year's data to cumulative results
                processor.conn.execute(f"""
                    INSERT INTO {cumulative_kommune_table}
                    SELECT * FROM {year_kommune_results}
                """)

                # Clean up year-specific tables
                processor._cleanup_year_tables(year)

            # Aggregate the cumulative kommune data
            final_cumulative_kommune_table = "final_cumulative_kommune"
            processor.conn.execute(f"""
                CREATE OR REPLACE TABLE {final_cumulative_kommune_table} AS
                SELECT
                    kommune_code,
                    ANY_VALUE(kommune_name) as kommune_name,
                    ANY_VALUE(region_code) as region_code,
                    ANY_VALUE(kommune_area_ha) as kommune_area_ha,
                    ANY_VALUE(kommune_centroid_x) as kommune_centroid_x,
                    ANY_VALUE(kommune_centroid_y) as kommune_centroid_y,
                    MAX(total_agricultural_area_ha) as total_agricultural_area_ha,
                    MAX(unique_field_count) as unique_field_count,
                    MAX(unique_company_count) as unique_company_count,
                    AVG(avg_field_coverage_ratio) as avg_field_coverage_ratio,
                    MAX(max_field_coverage_ratio) as max_field_coverage_ratio,
                    MIN(min_field_coverage_ratio) as min_field_coverage_ratio,
                    MAX(crop_diversity) as crop_diversity,
                    STRING_AGG(DISTINCT crop_types, '; ') as crop_types,
                    SUM(total_pfas_containing_active_ingredient_grams) as total_pfas_containing_active_ingredient_grams,
                    SUM(total_diquat_containing_active_ingredient_grams) as total_diquat_containing_active_ingredient_grams,
                    SUM(total_glyphosate_containing_active_ingredient_grams) as total_glyphosate_containing_active_ingredient_grams,
                    SUM(total_pesticide_belastning) as total_pesticide_belastning,
                    SUM(total_pfas_pesticide_belastning) as total_pfas_pesticide_belastning,
                    SUM(total_diquat_pesticide_belastning) as total_diquat_pesticide_belastning,
                    SUM(total_glyphosate_pesticide_belastning) as total_glyphosate_pesticide_belastning,
                    SUM(total_pesticide_applications) as total_pesticide_applications,
                    SUM(pfas_containing_applications) as pfas_containing_applications,
                    SUM(diquat_containing_applications) as diquat_containing_applications,
                    SUM(glyphosate_containing_applications) as glyphosate_containing_applications,
                    MAX(unique_pfas_products) as unique_pfas_products,
                    MAX(unique_diquat_products) as unique_diquat_products,
                    MAX(unique_glyphosate_products) as unique_glyphosate_products,
                    MAX(unique_pesticide_products) as unique_pesticide_products,
                    -- Recalculate intensity based on cumulative totals
                    CASE 
                        WHEN MAX(total_agricultural_area_ha) > 0 THEN 
                            SUM(total_pfas_containing_active_ingredient_grams) / MAX(total_agricultural_area_ha)
                        ELSE 0 
                    END as pfas_containing_active_ingredient_intensity_grams_per_ha,
                    CASE 
                        WHEN MAX(total_agricultural_area_ha) > 0 THEN 
                            SUM(total_diquat_containing_active_ingredient_grams) / MAX(total_agricultural_area_ha)
                        ELSE 0 
                    END as diquat_containing_active_ingredient_intensity_grams_per_ha,
                    CASE 
                        WHEN MAX(total_agricultural_area_ha) > 0 THEN 
                            SUM(total_glyphosate_containing_active_ingredient_grams) / MAX(total_agricultural_area_ha)
                        ELSE 0 
                    END as glyphosate_containing_active_ingredient_intensity_grams_per_ha,
                    CASE 
                        WHEN MAX(total_agricultural_area_ha) > 0 THEN 
                            SUM(total_pesticide_belastning) / MAX(total_agricultural_area_ha)
                        ELSE 0 
                    END as pesticide_belastning_per_ha,
                    CASE 
                        WHEN MAX(total_agricultural_area_ha) > 0 THEN 
                            SUM(total_pfas_pesticide_belastning) / MAX(total_agricultural_area_ha)
                        ELSE 0 
                    END as pfas_pesticide_belastning_per_ha,
                    CASE 
                        WHEN MAX(total_agricultural_area_ha) > 0 THEN 
                            SUM(total_diquat_pesticide_belastning) / MAX(total_agricultural_area_ha)
                        ELSE 0 
                    END as diquat_pesticide_belastning_per_ha,
                    CASE 
                        WHEN MAX(total_agricultural_area_ha) > 0 THEN 
                            SUM(total_glyphosate_pesticide_belastning) / MAX(total_agricultural_area_ha)
                        ELSE 0 
                    END as glyphosate_pesticide_belastning_per_ha,
                    CASE 
                        WHEN ANY_VALUE(kommune_area_ha) > 0 THEN 
                            (MAX(total_agricultural_area_ha) / ANY_VALUE(kommune_area_ha)) * 100.0
                        ELSE 0 
                    END as agricultural_coverage_pct,
                    CURRENT_TIMESTAMP as created_at
                FROM {cumulative_kommune_table}
                GROUP BY kommune_code
                HAVING SUM(total_pesticide_belastning) > 0 OR SUM(total_pfas_containing_active_ingredient_grams) > 0
            """)

            # Save cumulative kommune results
            result_count = result_saver.save_cumulative_kommune_results(
                final_cumulative_kommune_table, years
            )

            if result_count > 0:
                logger.info(f"✅ Cumulative kommune analysis completed: {result_count:,} records")
            else:
                logger.error("❌ Cumulative kommune analysis failed")
                return False

        # Final cleanup
        processor._aggressive_cleanup()

        logger.success("✅ Cumulative analysis completed successfully")
        return True

    except Exception as e:
        logger.error(f"❌ Error in cumulative analysis: {e}")
        return False


async def run_cumulative_analysis_optimized(
    years: list[int] | None = None,
    h3_resolutions: list[int] | None = None,
    include_kommune: bool = False,
) -> bool:
    """
    Run optimized cumulative analysis that loads existing year-specific results from GCS
    and aggregates them, instead of reprocessing all the raw data.

    This is much faster and more efficient than the original cumulative analysis,
    as it leverages existing processed results.

    Args:
        years: List of years to include in cumulative analysis (None = all available)
        h3_resolutions: List of H3 resolutions to generate (default: [10])
        include_kommune: Whether to include kommune-level cumulative analysis

    Returns:
        bool: True if successful, False otherwise
    """
    logger.info("🚀 Starting optimized cumulative H3 PFAS analysis (loading existing results)")

    if h3_resolutions is None:
        h3_resolutions = [10]

    logger.info(f"   🎯 H3 resolutions: {h3_resolutions}")
    if include_kommune:
        logger.info("   🏛️ Including kommune cumulative analysis")

    # Use the highest resolution for the base configuration
    base_resolution = max(h3_resolutions)

    # Create lightweight configuration for aggregation only
    config = H3SpatialConfig(
        h3_resolution=base_resolution,
        chunk_size=10000,
        memory_limit="14GB",
        thread_count=4,
        github_actions_mode=True,
        enable_memory_monitoring=True,
        enable_disk_monitoring=True,
        enable_time_monitoring=True,
        aggressive_cleanup=True,
        duckdb_memory_limit="12GB",
        duckdb_threads=4,
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

        # Determine years to process
        if years is None:
            years = data_loader.get_available_years()
            logger.info(f"📅 Creating cumulative analysis from all available years: {years}")
        else:
            logger.info(f"📅 Creating cumulative analysis from specified years: {years}")

        if not years:
            logger.error("❌ No years available for cumulative analysis")
            return False

        # Process each H3 resolution
        all_success = True
        for resolution in h3_resolutions:
            logger.info(f"🔷 Creating optimized cumulative H3 analysis at resolution {resolution}")

            # Update processor resolution for this analysis
            processor.config.update_resolution(resolution)

            # Check which years have existing results with improved path discovery
            available_year_results = []
            for year in years:
                # Try multiple patterns to find existing H3 results
                patterns = [
                    # New standardized format
                    f"gs://{config.bucket}/gold/h3_pesticide_{year}_res{resolution}/*/h3_pesticide_{year}_res{resolution}.parquet",
                    # Alternative format with different timestamp structure
                    f"gs://{config.bucket}/gold/h3_pesticide_{year}_res{resolution}/*/*_res{resolution}.parquet",
                    # Legacy format fallback
                    f"gs://{config.bucket}/gold/h3_pesticide_{year}/*/h3_pesticide_{year}_res{resolution}.parquet",
                    # Kepler format as fallback (can be used for aggregation)
                    f"gs://{config.bucket}/gold/h3_pesticide_{year}_res{resolution}/*/h3_pesticide_{year}_res{resolution}_kepler.parquet",
                ]

                found_file = None
                for pattern in patterns:
                    try:
                        files = processor.gcs_access.list_files(pattern)
                        if files:
                            found_file = sorted(files)[-1]  # Get the most recent result
                            logger.info(
                                f"   ✅ Found existing results for year {year} at resolution {resolution}: {found_file}"
                            )
                            break
                    except Exception as e:
                        logger.debug(f"   🔍 Pattern {pattern} failed: {e}")
                        continue

                if found_file:
                    available_year_results.append((year, found_file))
                else:
                    logger.warning(
                        f"   ⚠️  No existing results found for year {year} at resolution {resolution}"
                    )

            if not available_year_results:
                logger.error(
                    f"❌ No existing results found for any year at resolution {resolution}"
                )
                logger.info(
                    f"   💡 Hint: Run individual year analyses first with: python main.py --mode h3 --h3-resolution {resolution}"
                )
                all_success = False
                continue

            logger.info(
                f"📊 Loading and aggregating {len(available_year_results)} year(s) of existing results for resolution {resolution}"
            )

            # Initialize cumulative results table
            cumulative_table = f"cumulative_h3_results_res{resolution}"
            processor.conn.execute(f"""
                CREATE OR REPLACE TABLE {cumulative_table} (
                    h3_cell BIGINT,
                    center_lat DOUBLE,
                    center_lon DOUBLE,
                    h3_area_ha DOUBLE,
                    total_intersection_area_ha DOUBLE,
                    actual_coverage_ratio DOUBLE,
                    unique_field_count INTEGER,
                    total_pfas_containing_active_ingredient_grams DOUBLE,
                    total_diquat_containing_active_ingredient_grams DOUBLE,
                    total_glyphosate_containing_active_ingredient_grams DOUBLE,
                    total_pesticide_belastning DOUBLE,
                    total_pfas_pesticide_belastning DOUBLE,
                    total_diquat_pesticide_belastning DOUBLE,
                    total_glyphosate_pesticide_belastning DOUBLE,
                    total_pesticide_applications INTEGER,
                    pfas_containing_applications INTEGER,
                    diquat_containing_applications INTEGER,
                    glyphosate_containing_applications INTEGER,
                    crop_types VARCHAR,
                    crop_diversity INTEGER,
                    pfas_containing_active_ingredient_intensity_grams_per_ha DOUBLE,
                    diquat_containing_active_ingredient_intensity_grams_per_ha DOUBLE,
                    glyphosate_containing_active_ingredient_intensity_grams_per_ha DOUBLE,
                    pesticide_belastning_per_ha DOUBLE,
                    created_at TIMESTAMP
                )
            """)

            # Load and aggregate each year's results
            for year, file_path in available_year_results:
                logger.info(f"   📅 Loading results for year {year} from GCS")

                # Load year results from GCS
                year_table = f"year_results_{year}_res{resolution}"
                try:
                    processor.gcs_access.create_table_from_gcs(year_table, file_path)

                    # Check if we have the expected columns (handle both regular and kepler formats)
                    columns = processor.conn.execute(f"PRAGMA table_info({year_table})").fetchall()
                    column_names = [col[1] for col in columns]

                    # Map column names based on format
                    h3_col = "h3_cell" if "h3_cell" in column_names else "h3_id"

                    # Insert into cumulative table with column mapping
                    processor.conn.execute(f"""
                        INSERT INTO {cumulative_table}
                        SELECT 
                            {h3_col} as h3_cell,
                            center_lat,
                            center_lon,
                            {"h3_area_ha" if "h3_area_ha" in column_names else "h3_cell_area_ha"} as h3_area_ha,
                            {"total_intersection_area_ha" if "total_intersection_area_ha" in column_names else "agricultural_area_ha"} as total_intersection_area_ha,
                            {"actual_coverage_ratio" if "actual_coverage_ratio" in column_names else "0.0"} as actual_coverage_ratio,
                            unique_field_count,
                            total_pfas_containing_active_ingredient_grams,
                            total_diquat_containing_active_ingredient_grams,
                            total_glyphosate_containing_active_ingredient_grams,
                            total_pesticide_belastning,
                            {"total_pfas_pesticide_belastning" if "total_pfas_pesticide_belastning" in column_names else "0.0"} as total_pfas_pesticide_belastning,
                            {"total_diquat_pesticide_belastning" if "total_diquat_pesticide_belastning" in column_names else "0.0"} as total_diquat_pesticide_belastning,
                            {"total_glyphosate_pesticide_belastning" if "total_glyphosate_pesticide_belastning" in column_names else "0.0"} as total_glyphosate_pesticide_belastning,
                            total_pesticide_applications,
                            pfas_containing_applications,
                            diquat_containing_applications,
                            glyphosate_containing_applications,
                            crop_types,
                            crop_diversity,
                            pfas_containing_active_ingredient_intensity_grams_per_ha,
                            diquat_containing_active_ingredient_intensity_grams_per_ha,
                            glyphosate_containing_active_ingredient_intensity_grams_per_ha,
                            {"pesticide_belastning_per_ha" if "pesticide_belastning_per_ha" in column_names else "pesticide_intensity"} as pesticide_belastning_per_ha,
                            {"created_at" if "created_at" in column_names else "CURRENT_TIMESTAMP"} as created_at
                        FROM {year_table}
                    """)

                    # Get count for logging
                    count = processor.conn.execute(f"SELECT COUNT(*) FROM {year_table}").fetchone()[
                        0
                    ]
                    logger.info(f"     📊 Loaded {count:,} records from year {year}")

                except Exception as e:
                    logger.error(f"   ❌ Failed to load year {year} data: {e}")
                    all_success = False
                finally:
                    # Clean up year table
                    processor.conn.execute(f"DROP TABLE IF EXISTS {year_table}")

            # Aggregate the cumulative data by H3 cell
            final_cumulative_table = f"final_cumulative_h3_res{resolution}"
            processor.conn.execute(f"""
                CREATE OR REPLACE TABLE {final_cumulative_table} AS
                SELECT
                    h3_cell,
                    ANY_VALUE(center_lat) as center_lat,
                    ANY_VALUE(center_lon) as center_lon,
                    ANY_VALUE(h3_area_ha) as h3_area_ha,
                    MAX(total_intersection_area_ha) as total_intersection_area_ha,
                    MAX(actual_coverage_ratio) as actual_coverage_ratio,
                    MAX(unique_field_count) as unique_field_count,
                    SUM(total_pfas_containing_active_ingredient_grams) as total_pfas_containing_active_ingredient_grams,
                    SUM(total_diquat_containing_active_ingredient_grams) as total_diquat_containing_active_ingredient_grams,
                    SUM(total_glyphosate_containing_active_ingredient_grams) as total_glyphosate_containing_active_ingredient_grams,
                    SUM(total_pesticide_belastning) as total_pesticide_belastning,
                    SUM(total_pfas_pesticide_belastning) as total_pfas_pesticide_belastning,
                    SUM(total_diquat_pesticide_belastning) as total_diquat_pesticide_belastning,
                    SUM(total_glyphosate_pesticide_belastning) as total_glyphosate_pesticide_belastning,
                    SUM(total_pesticide_applications) as total_pesticide_applications,
                    SUM(pfas_containing_applications) as pfas_containing_applications,
                    SUM(diquat_containing_applications) as diquat_containing_applications,
                    SUM(glyphosate_containing_applications) as glyphosate_containing_applications,
                    STRING_AGG(DISTINCT crop_types, '; ') as crop_types,
                    MAX(crop_diversity) as crop_diversity,
                    -- Recalculate intensity based on cumulative totals
                    CASE 
                        WHEN MAX(total_intersection_area_ha) > 0 THEN 
                            SUM(total_pfas_containing_active_ingredient_grams) / MAX(total_intersection_area_ha)
                        ELSE 0 
                    END as pfas_containing_active_ingredient_intensity_grams_per_ha,
                    CASE 
                        WHEN MAX(total_intersection_area_ha) > 0 THEN 
                            SUM(total_diquat_containing_active_ingredient_grams) / MAX(total_intersection_area_ha)
                        ELSE 0 
                    END as diquat_containing_active_ingredient_intensity_grams_per_ha,
                    CASE 
                        WHEN MAX(total_intersection_area_ha) > 0 THEN 
                            SUM(total_glyphosate_containing_active_ingredient_grams) / MAX(total_intersection_area_ha)
                        ELSE 0 
                    END as glyphosate_containing_active_ingredient_intensity_grams_per_ha,
                    CASE 
                        WHEN MAX(total_intersection_area_ha) > 0 THEN 
                            SUM(total_pesticide_belastning) / MAX(total_intersection_area_ha)
                        ELSE 0 
                    END as pesticide_belastning_per_ha,
                    CURRENT_TIMESTAMP as created_at
                FROM {cumulative_table}
                GROUP BY h3_cell
                HAVING SUM(total_pesticide_belastning) > 0 OR SUM(total_pfas_containing_active_ingredient_grams) > 0
            """)

            # Validate cumulative results
            try:
                processor._validate_results(final_cumulative_table)
            except Exception as e:
                logger.warning(f"   ⚠️  Validation warning for resolution {resolution}: {e}")

            # Save cumulative results with special "total" year identifier
            result_count = result_saver.save_cumulative_results(
                final_cumulative_table, resolution, years
            )

            if result_count > 0:
                logger.info(
                    f"✅ Optimized cumulative H3 analysis completed for resolution {resolution}: {result_count:,} records"
                )
            else:
                logger.error(
                    f"❌ Optimized cumulative H3 analysis failed for resolution {resolution}"
                )
                all_success = False

            # Clean up intermediate tables
            processor.conn.execute(f"DROP TABLE IF EXISTS {cumulative_table}")
            processor.conn.execute(f"DROP TABLE IF EXISTS {final_cumulative_table}")

        # Handle kommune cumulative analysis if requested
        if include_kommune:
            logger.info("🏛️ Creating optimized cumulative kommune analysis")

            # Check which years have existing kommune results with improved path discovery
            available_kommune_results = []
            for year in years:
                patterns = [
                    f"gs://{config.bucket}/gold/kommune_pesticide_{year}/*/kommune_pesticide_{year}.parquet",
                    f"gs://{config.bucket}/gold/kommune_pesticide_{year}/*/*kommune*.parquet",
                    f"gs://{config.bucket}/gold/kommune_{year}/*/kommune_{year}.parquet",
                ]

                found_file = None
                for pattern in patterns:
                    try:
                        files = processor.gcs_access.list_files(pattern)
                        if files:
                            found_file = sorted(files)[-1]
                            logger.info(
                                f"   ✅ Found existing kommune results for year {year}: {found_file}"
                            )
                            break
                    except Exception as e:
                        logger.debug(f"   🔍 Kommune pattern {pattern} failed: {e}")
                        continue

                if found_file:
                    available_kommune_results.append((year, found_file))
                else:
                    logger.warning(f"   ⚠️  No existing kommune results found for year {year}")

            if not available_kommune_results:
                logger.warning(
                    "⚠️  No existing kommune results found for any year - skipping kommune cumulative analysis"
                )
                logger.info(
                    "   💡 Hint: Run kommune analyses first with: python main.py --mode kommune"
                )
            else:
                logger.info(
                    f"📊 Loading and aggregating {len(available_kommune_results)} year(s) of existing kommune results"
                )

                # Initialize cumulative kommune results table
                cumulative_kommune_table = "cumulative_kommune_results"
                processor.conn.execute(f"""
                    CREATE OR REPLACE TABLE {cumulative_kommune_table} AS
                    SELECT
                        kommune_code,
                        ANY_VALUE(kommune_name) as kommune_name,
                        ANY_VALUE(region_code) as region_code,
                        ANY_VALUE(kommune_area_ha) as kommune_area_ha,
                        ANY_VALUE(kommune_centroid_x) as kommune_centroid_x,
                        ANY_VALUE(kommune_centroid_y) as kommune_centroid_y,
                        MAX(total_agricultural_area_ha) as total_agricultural_area_ha,
                        MAX(unique_field_count) as unique_field_count,
                        MAX(unique_company_count) as unique_company_count,
                        AVG(avg_field_coverage_ratio) as avg_field_coverage_ratio,
                        MAX(max_field_coverage_ratio) as max_field_coverage_ratio,
                        MIN(min_field_coverage_ratio) as min_field_coverage_ratio,
                        MAX(crop_diversity) as crop_diversity,
                        STRING_AGG(DISTINCT crop_types, '; ') as crop_types,
                        SUM(total_pfas_containing_active_ingredient_grams) as total_pfas_containing_active_ingredient_grams,
                        SUM(total_diquat_containing_active_ingredient_grams) as total_diquat_containing_active_ingredient_grams,
                        SUM(total_glyphosate_containing_active_ingredient_grams) as total_glyphosate_containing_active_ingredient_grams,
                        SUM(total_pesticide_belastning) as total_pesticide_belastning,
                        SUM(total_pfas_pesticide_belastning) as total_pfas_pesticide_belastning,
                        SUM(total_diquat_pesticide_belastning) as total_diquat_pesticide_belastning,
                        SUM(total_glyphosate_pesticide_belastning) as total_glyphosate_pesticide_belastning,
                        SUM(total_pesticide_applications) as total_pesticide_applications,
                        SUM(pfas_containing_applications) as pfas_containing_applications,
                        SUM(diquat_containing_applications) as diquat_containing_applications,
                        SUM(glyphosate_containing_applications) as glyphosate_containing_applications,
                        MAX(unique_pfas_products) as unique_pfas_products,
                        MAX(unique_diquat_products) as unique_diquat_products,
                        MAX(unique_glyphosate_products) as unique_glyphosate_products,
                        MAX(unique_pesticide_products) as unique_pesticide_products,
                        -- Recalculate intensity based on cumulative totals
                        CASE 
                            WHEN MAX(total_agricultural_area_ha) > 0 THEN 
                                SUM(total_pfas_containing_active_ingredient_grams) / MAX(total_agricultural_area_ha)
                            ELSE 0 
                        END as pfas_containing_active_ingredient_intensity_grams_per_ha,
                        CASE 
                            WHEN MAX(total_agricultural_area_ha) > 0 THEN 
                                SUM(total_diquat_containing_active_ingredient_grams) / MAX(total_agricultural_area_ha)
                            ELSE 0 
                        END as diquat_containing_active_ingredient_intensity_grams_per_ha,
                        CASE 
                            WHEN MAX(total_agricultural_area_ha) > 0 THEN 
                                SUM(total_glyphosate_containing_active_ingredient_grams) / MAX(total_agricultural_area_ha)
                            ELSE 0 
                        END as glyphosate_containing_active_ingredient_intensity_grams_per_ha,
                        CASE 
                            WHEN MAX(total_agricultural_area_ha) > 0 THEN 
                                SUM(total_pesticide_belastning) / MAX(total_agricultural_area_ha)
                            ELSE 0 
                        END as pesticide_belastning_per_ha,
                        CASE 
                            WHEN MAX(total_agricultural_area_ha) > 0 THEN 
                                SUM(total_pfas_pesticide_belastning) / MAX(total_agricultural_area_ha)
                            ELSE 0 
                        END as pfas_pesticide_belastning_per_ha,
                        CASE 
                            WHEN MAX(total_agricultural_area_ha) > 0 THEN 
                                SUM(total_diquat_pesticide_belastning) / MAX(total_agricultural_area_ha)
                            ELSE 0 
                        END as diquat_pesticide_belastning_per_ha,
                        CASE 
                            WHEN MAX(total_agricultural_area_ha) > 0 THEN 
                                SUM(total_glyphosate_pesticide_belastning) / MAX(total_agricultural_area_ha)
                            ELSE 0 
                        END as glyphosate_pesticide_belastning_per_ha,
                        CASE 
                            WHEN ANY_VALUE(kommune_area_ha) > 0 THEN 
                                (MAX(total_agricultural_area_ha) / ANY_VALUE(kommune_area_ha)) * 100.0
                            ELSE 0 
                        END as agricultural_coverage_pct,
                        CURRENT_TIMESTAMP as created_at
                    FROM {cumulative_kommune_table}
                    GROUP BY kommune_code
                    HAVING SUM(total_pesticide_belastning) > 0 OR SUM(total_pfas_containing_active_ingredient_grams) > 0
                """)

                # Save cumulative kommune results
                result_count = result_saver.save_cumulative_kommune_results(
                    final_cumulative_kommune_table, years
                )

                if result_count > 0:
                    logger.info(
                        f"✅ Optimized cumulative kommune analysis completed: {result_count:,} records"
                    )
                else:
                    logger.error("❌ Optimized cumulative kommune analysis failed")
                    all_success = False

                # Clean up intermediate tables
                processor.conn.execute(f"DROP TABLE IF EXISTS {cumulative_kommune_table}")
                processor.conn.execute(f"DROP TABLE IF EXISTS {final_cumulative_kommune_table}")

        # Final cleanup
        processor._aggressive_cleanup()

        if all_success:
            logger.success("✅ Optimized cumulative analysis completed successfully")
        else:
            logger.warning("⚠️  Optimized cumulative analysis completed with some warnings/errors")

        return all_success

    except Exception as e:
        logger.error(f"❌ Error in optimized cumulative analysis: {e}")
        return False


async def run_cumulative_analysis_github_actions_optimized(
    years: list[int] | None = None,
    h3_resolutions: list[int] | None = None,
    include_kommune: bool = False,
) -> bool:
    """
    Run optimized cumulative analysis specifically designed for GitHub Actions workflow.

    This function leverages the fact that individual year jobs have just completed
    and looks for the most recent results from those jobs, using timestamp-based
    discovery to find the freshest data.

    Args:
        years: List of years to include in cumulative analysis (None = all available)
        h3_resolutions: List of H3 resolutions to generate (default: [10])
        include_kommune: Whether to include kommune-level cumulative analysis

    Returns:
        bool: True if successful, False otherwise
    """
    logger.info("🚀 Starting GitHub Actions optimized cumulative H3 PFAS analysis")
    logger.info("   ⚡ Leveraging freshly completed individual year jobs")

    if h3_resolutions is None:
        h3_resolutions = [10]

    logger.info(f"   🎯 H3 resolutions: {h3_resolutions}")
    if include_kommune:
        logger.info("   🏛️ Including kommune cumulative analysis")

    # Use the highest resolution for the base configuration
    base_resolution = max(h3_resolutions)

    # Create lightweight configuration for aggregation only
    config = H3SpatialConfig(
        h3_resolution=base_resolution,
        chunk_size=10000,
        memory_limit="14GB",
        thread_count=4,
        github_actions_mode=True,
        enable_memory_monitoring=True,
        enable_disk_monitoring=True,
        enable_time_monitoring=True,
        aggressive_cleanup=True,
        duckdb_memory_limit="12GB",
        duckdb_threads=4,
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

        # Determine years to process
        if years is None:
            years = data_loader.get_available_years()
            logger.info(f"📅 Creating cumulative analysis from all available years: {years}")
        else:
            logger.info(f"📅 Creating cumulative analysis from specified years: {years}")

        if not years:
            logger.error("❌ No years available for cumulative analysis")
            return False

        # Process each H3 resolution
        all_success = True
        for resolution in h3_resolutions:
            logger.info(
                f"🔷 Creating GitHub Actions optimized cumulative H3 analysis at resolution {resolution}"
            )

            # Update processor resolution for this analysis
            processor.config.update_resolution(resolution)

            # Look for freshly completed results using GitHub Actions workflow patterns
            available_year_results = []
            for year in years:
                # GitHub Actions workflow stores results with timestamps
                # Look for the most recent results (likely from the current workflow run)

                # Get current timestamp for recency filtering (within last 24 hours)
                from datetime import datetime, timedelta

                recent_threshold = datetime.now() - timedelta(hours=24)

                # Primary patterns used by GitHub Actions workflow
                patterns = [
                    # Standard format from individual year jobs
                    f"gs://{config.bucket}/gold/h3_pesticide_{year}_res{resolution}/*/h3_pesticide_{year}_res{resolution}.parquet",
                    # Alternative timestamp patterns
                    f"gs://{config.bucket}/gold/h3_pesticide_{year}_res{resolution}/*/h3_pesticide_{year}_res{resolution}_*.parquet",
                ]

                found_file = None
                for pattern in patterns:
                    try:
                        # List files with timestamps
                        files = processor.gcs_access.list_files_with_timestamps(pattern)
                        if files:
                            # Sort by timestamp (most recent first) and take the freshest
                            files_sorted = sorted(files, key=lambda x: x[1], reverse=True)
                            most_recent_file, timestamp = files_sorted[0]

                            # Prefer very recent files (likely from current workflow run)
                            if timestamp > recent_threshold:
                                found_file = most_recent_file
                                logger.info(
                                    f"   ✅ Found fresh results for year {year} at resolution {resolution}: {found_file}"
                                )
                                logger.info(f"      🕐 Timestamp: {timestamp} (within last 24h)")
                                break
                            else:
                                # Fall back to most recent file even if older
                                if found_file is None:
                                    found_file = most_recent_file
                                    logger.info(
                                        f"   ⚠️  Found older results for year {year} at resolution {resolution}: {found_file}"
                                    )
                                    logger.info(f"      🕐 Timestamp: {timestamp}")
                    except Exception as e:
                        logger.debug(f"   🔍 Pattern {pattern} failed: {e}")
                        continue

                if found_file:
                    available_year_results.append((year, found_file))
                else:
                    logger.warning(
                        f"   ⚠️  No results found for year {year} at resolution {resolution}"
                    )

            if not available_year_results:
                logger.error(f"❌ No results found for any year at resolution {resolution}")
                logger.info(
                    "   💡 This usually means the individual year jobs failed or haven't completed yet"
                )
                all_success = False
                continue

            logger.info(
                f"📊 Loading and aggregating {len(available_year_results)} year(s) of fresh results for resolution {resolution}"
            )

            # Use the same aggregation logic as the regular optimized function
            # Initialize cumulative results table
            cumulative_table = f"cumulative_h3_results_res{resolution}"
            processor.conn.execute(f"""
                CREATE OR REPLACE TABLE {cumulative_table} (
                    h3_cell BIGINT,
                    center_lat DOUBLE,
                    center_lon DOUBLE,
                    h3_area_ha DOUBLE,
                    total_intersection_area_ha DOUBLE,
                    actual_coverage_ratio DOUBLE,
                    unique_field_count INTEGER,
                    total_pfas_containing_active_ingredient_grams DOUBLE,
                    total_diquat_containing_active_ingredient_grams DOUBLE,
                    total_glyphosate_containing_active_ingredient_grams DOUBLE,
                    total_pesticide_belastning DOUBLE,
                    total_pfas_pesticide_belastning DOUBLE,
                    total_diquat_pesticide_belastning DOUBLE,
                    total_glyphosate_pesticide_belastning DOUBLE,
                    total_pesticide_applications INTEGER,
                    pfas_containing_applications INTEGER,
                    diquat_containing_applications INTEGER,
                    glyphosate_containing_applications INTEGER,
                    crop_types VARCHAR,
                    crop_diversity INTEGER,
                    pfas_containing_active_ingredient_intensity_grams_per_ha DOUBLE,
                    diquat_containing_active_ingredient_intensity_grams_per_ha DOUBLE,
                    glyphosate_containing_active_ingredient_intensity_grams_per_ha DOUBLE,
                    pesticide_belastning_per_ha DOUBLE,
                    created_at TIMESTAMP
                )
            """)

            # Load and aggregate each year's results
            for year, file_path in available_year_results:
                logger.info(f"   📅 Loading fresh results for year {year} from GCS")

                # Load year results from GCS
                year_table = f"year_results_{year}_res{resolution}"
                try:
                    processor.gcs_access.create_table_from_gcs(year_table, file_path)

                    # Check if we have the expected columns (handle both regular and kepler formats)
                    columns = processor.conn.execute(f"PRAGMA table_info({year_table})").fetchall()
                    column_names = [col[1] for col in columns]

                    # Map column names based on format
                    h3_col = "h3_cell" if "h3_cell" in column_names else "h3_id"

                    # Insert into cumulative table with column mapping
                    processor.conn.execute(f"""
                        INSERT INTO {cumulative_table}
                        SELECT 
                            {h3_col} as h3_cell,
                            center_lat,
                            center_lon,
                            {"h3_area_ha" if "h3_area_ha" in column_names else "h3_cell_area_ha"} as h3_area_ha,
                            {"total_intersection_area_ha" if "total_intersection_area_ha" in column_names else "agricultural_area_ha"} as total_intersection_area_ha,
                            {"actual_coverage_ratio" if "actual_coverage_ratio" in column_names else "0.0"} as actual_coverage_ratio,
                            unique_field_count,
                            total_pfas_containing_active_ingredient_grams,
                            total_diquat_containing_active_ingredient_grams,
                            total_glyphosate_containing_active_ingredient_grams,
                            total_pesticide_belastning,
                            {"total_pfas_pesticide_belastning" if "total_pfas_pesticide_belastning" in column_names else "0.0"} as total_pfas_pesticide_belastning,
                            {"total_diquat_pesticide_belastning" if "total_diquat_pesticide_belastning" in column_names else "0.0"} as total_diquat_pesticide_belastning,
                            {"total_glyphosate_pesticide_belastning" if "total_glyphosate_pesticide_belastning" in column_names else "0.0"} as total_glyphosate_pesticide_belastning,
                            total_pesticide_applications,
                            pfas_containing_applications,
                            diquat_containing_applications,
                            glyphosate_containing_applications,
                            crop_types,
                            crop_diversity,
                            pfas_containing_active_ingredient_intensity_grams_per_ha,
                            diquat_containing_active_ingredient_intensity_grams_per_ha,
                            glyphosate_containing_active_ingredient_intensity_grams_per_ha,
                            {"pesticide_belastning_per_ha" if "pesticide_belastning_per_ha" in column_names else "pesticide_intensity"} as pesticide_belastning_per_ha,
                            {"created_at" if "created_at" in column_names else "CURRENT_TIMESTAMP"} as created_at
                        FROM {year_table}
                    """)

                    # Get count for logging
                    count = processor.conn.execute(f"SELECT COUNT(*) FROM {year_table}").fetchone()[
                        0
                    ]
                    logger.info(f"     📊 Loaded {count:,} records from year {year}")

                except Exception as e:
                    logger.error(f"   ❌ Failed to load year {year} data: {e}")
                    all_success = False
                finally:
                    # Clean up year table
                    processor.conn.execute(f"DROP TABLE IF EXISTS {year_table}")

            # Aggregate the cumulative data by H3 cell (same logic as regular optimized function)
            final_cumulative_table = f"final_cumulative_h3_res{resolution}"
            processor.conn.execute(f"""
                CREATE OR REPLACE TABLE {final_cumulative_table} AS
                SELECT
                    h3_cell,
                    ANY_VALUE(center_lat) as center_lat,
                    ANY_VALUE(center_lon) as center_lon,
                    ANY_VALUE(h3_area_ha) as h3_area_ha,
                    MAX(total_intersection_area_ha) as total_intersection_area_ha,
                    MAX(actual_coverage_ratio) as actual_coverage_ratio,
                    MAX(unique_field_count) as unique_field_count,
                    SUM(total_pfas_containing_active_ingredient_grams) as total_pfas_containing_active_ingredient_grams,
                    SUM(total_diquat_containing_active_ingredient_grams) as total_diquat_containing_active_ingredient_grams,
                    SUM(total_glyphosate_containing_active_ingredient_grams) as total_glyphosate_containing_active_ingredient_grams,
                    SUM(total_pesticide_belastning) as total_pesticide_belastning,
                    SUM(total_pfas_pesticide_belastning) as total_pfas_pesticide_belastning,
                    SUM(total_diquat_pesticide_belastning) as total_diquat_pesticide_belastning,
                    SUM(total_glyphosate_pesticide_belastning) as total_glyphosate_pesticide_belastning,
                    SUM(total_pesticide_applications) as total_pesticide_applications,
                    SUM(pfas_containing_applications) as pfas_containing_applications,
                    SUM(diquat_containing_applications) as diquat_containing_applications,
                    SUM(glyphosate_containing_applications) as glyphosate_containing_applications,
                    STRING_AGG(DISTINCT crop_types, '; ') as crop_types,
                    MAX(crop_diversity) as crop_diversity,
                    -- Recalculate intensity based on cumulative totals
                    CASE 
                        WHEN MAX(total_intersection_area_ha) > 0 THEN 
                            SUM(total_pfas_containing_active_ingredient_grams) / MAX(total_intersection_area_ha)
                        ELSE 0 
                    END as pfas_containing_active_ingredient_intensity_grams_per_ha,
                    CASE 
                        WHEN MAX(total_intersection_area_ha) > 0 THEN 
                            SUM(total_diquat_containing_active_ingredient_grams) / MAX(total_intersection_area_ha)
                        ELSE 0 
                    END as diquat_containing_active_ingredient_intensity_grams_per_ha,
                    CASE 
                        WHEN MAX(total_intersection_area_ha) > 0 THEN 
                            SUM(total_glyphosate_containing_active_ingredient_grams) / MAX(total_intersection_area_ha)
                        ELSE 0 
                    END as glyphosate_containing_active_ingredient_intensity_grams_per_ha,
                    CASE 
                        WHEN MAX(total_intersection_area_ha) > 0 THEN 
                            SUM(total_pesticide_belastning) / MAX(total_intersection_area_ha)
                        ELSE 0 
                    END as pesticide_belastning_per_ha,
                    CURRENT_TIMESTAMP as created_at
                FROM {cumulative_table}
                GROUP BY h3_cell
                HAVING SUM(total_pesticide_belastning) > 0 OR SUM(total_pfas_containing_active_ingredient_grams) > 0
            """)

            # Validate cumulative results
            try:
                processor._validate_results(final_cumulative_table)
            except Exception as e:
                logger.warning(f"   ⚠️  Validation warning for resolution {resolution}: {e}")

            # Save cumulative results with special "total" year identifier
            result_count = result_saver.save_cumulative_results(
                final_cumulative_table, resolution, years
            )

            if result_count > 0:
                logger.info(
                    f"✅ GitHub Actions optimized cumulative H3 analysis completed for resolution {resolution}: {result_count:,} records"
                )
            else:
                logger.error(
                    f"❌ GitHub Actions optimized cumulative H3 analysis failed for resolution {resolution}"
                )
                all_success = False

            # Clean up intermediate tables
            processor.conn.execute(f"DROP TABLE IF EXISTS {cumulative_table}")
            processor.conn.execute(f"DROP TABLE IF EXISTS {final_cumulative_table}")

        # Handle kommune cumulative analysis if requested (similar logic)
        if include_kommune:
            logger.info("🏛️ Creating GitHub Actions optimized cumulative kommune analysis")

            # Look for fresh kommune results
            available_kommune_results = []
            for year in years:
                patterns = [
                    f"gs://{config.bucket}/gold/kommune_pesticide_{year}/*/kommune_pesticide_{year}.parquet",
                    f"gs://{config.bucket}/gold/kommune_pesticide_{year}/*/kommune_pesticide_{year}_*.parquet",
                ]

                found_file = None
                for pattern in patterns:
                    try:
                        files = processor.gcs_access.list_files_with_timestamps(pattern)
                        if files:
                            files_sorted = sorted(files, key=lambda x: x[1], reverse=True)
                            most_recent_file, timestamp = files_sorted[0]
                            found_file = most_recent_file
                            logger.info(
                                f"   ✅ Found fresh kommune results for year {year}: {found_file}"
                            )
                            break
                    except Exception as e:
                        logger.debug(f"   🔍 Kommune pattern {pattern} failed: {e}")
                        continue

                if found_file:
                    available_kommune_results.append((year, found_file))
                else:
                    logger.warning(f"   ⚠️  No kommune results found for year {year}")

            if not available_kommune_results:
                logger.warning(
                    "⚠️  No fresh kommune results found - skipping kommune cumulative analysis"
                )
            else:
                logger.info(
                    f"📊 Loading and aggregating {len(available_kommune_results)} year(s) of fresh kommune results"
                )

                # Initialize cumulative kommune results table
                cumulative_kommune_table = "cumulative_kommune_results"
                processor.conn.execute(f"""
                    CREATE OR REPLACE TABLE {cumulative_kommune_table} (
                        kommune_code INTEGER,
                        kommune_name VARCHAR,
                        region_code INTEGER,
                        kommune_area_ha DOUBLE,
                        kommune_centroid_x DOUBLE,
                        kommune_centroid_y DOUBLE,
                        total_agricultural_area_ha DOUBLE,
                        unique_field_count INTEGER,
                        unique_company_count INTEGER,
                        avg_field_coverage_ratio DOUBLE,
                        max_field_coverage_ratio DOUBLE,
                        min_field_coverage_ratio DOUBLE,
                        crop_diversity INTEGER,
                        crop_types VARCHAR,
                        total_pfas_containing_active_ingredient_grams DOUBLE,
                        total_diquat_containing_active_ingredient_grams DOUBLE,
                        total_glyphosate_containing_active_ingredient_grams DOUBLE,
                        total_pesticide_belastning DOUBLE,
                        total_pfas_pesticide_belastning DOUBLE,
                        total_diquat_pesticide_belastning DOUBLE,
                        total_glyphosate_pesticide_belastning DOUBLE,
                        total_pesticide_applications INTEGER,
                        pfas_containing_applications INTEGER,
                        diquat_containing_applications INTEGER,
                        glyphosate_containing_applications INTEGER,
                        unique_pfas_products INTEGER,
                        unique_diquat_products INTEGER,
                        unique_glyphosate_products INTEGER,
                        unique_pesticide_products INTEGER,
                        pfas_containing_active_ingredient_intensity_grams_per_ha DOUBLE,
                        diquat_containing_active_ingredient_intensity_grams_per_ha DOUBLE,
                        glyphosate_containing_active_ingredient_intensity_grams_per_ha DOUBLE,
                        pesticide_belastning_per_ha DOUBLE,
                        pfas_pesticide_belastning_per_ha DOUBLE,
                        diquat_pesticide_belastning_per_ha DOUBLE,
                        glyphosate_pesticide_belastning_per_ha DOUBLE,
                        agricultural_coverage_pct DOUBLE,
                        created_at TIMESTAMP
                    )
                """)

                # Load and aggregate each year's kommune results
                for year, file_path in available_kommune_results:
                    logger.info(f"   📅 Loading fresh kommune results for year {year} from GCS")

                    # Load year kommune results from GCS
                    year_kommune_table = f"year_kommune_results_{year}"
                    try:
                        processor.gcs_access.create_table_from_gcs(year_kommune_table, file_path)

                        # Insert into cumulative table
                        processor.conn.execute(f"""
                            INSERT INTO {cumulative_kommune_table}
                            SELECT * FROM {year_kommune_table}
                        """)

                        # Get count for logging
                        count = processor.conn.execute(
                            f"SELECT COUNT(*) FROM {year_kommune_table}"
                        ).fetchone()[0]
                        logger.info(f"     📊 Loaded {count:,} kommune records from year {year}")

                    except Exception as e:
                        logger.error(f"   ❌ Failed to load kommune year {year} data: {e}")
                        all_success = False
                    finally:
                        # Clean up year table
                        processor.conn.execute(f"DROP TABLE IF EXISTS {year_kommune_table}")

                # Aggregate the cumulative kommune data (same logic as before)
                final_cumulative_kommune_table = "final_cumulative_kommune"
                processor.conn.execute(f"""
                    CREATE OR REPLACE TABLE {final_cumulative_kommune_table} AS
                    SELECT
                        kommune_code,
                        ANY_VALUE(kommune_name) as kommune_name,
                        ANY_VALUE(region_code) as region_code,
                        ANY_VALUE(kommune_area_ha) as kommune_area_ha,
                        ANY_VALUE(kommune_centroid_x) as kommune_centroid_x,
                        ANY_VALUE(kommune_centroid_y) as kommune_centroid_y,
                        MAX(total_agricultural_area_ha) as total_agricultural_area_ha,
                        MAX(unique_field_count) as unique_field_count,
                        MAX(unique_company_count) as unique_company_count,
                        AVG(avg_field_coverage_ratio) as avg_field_coverage_ratio,
                        MAX(max_field_coverage_ratio) as max_field_coverage_ratio,
                        MIN(min_field_coverage_ratio) as min_field_coverage_ratio,
                        MAX(crop_diversity) as crop_diversity,
                        STRING_AGG(DISTINCT crop_types, '; ') as crop_types,
                        SUM(total_pfas_containing_active_ingredient_grams) as total_pfas_containing_active_ingredient_grams,
                        SUM(total_diquat_containing_active_ingredient_grams) as total_diquat_containing_active_ingredient_grams,
                        SUM(total_glyphosate_containing_active_ingredient_grams) as total_glyphosate_containing_active_ingredient_grams,
                        SUM(total_pesticide_belastning) as total_pesticide_belastning,
                        SUM(total_pfas_pesticide_belastning) as total_pfas_pesticide_belastning,
                        SUM(total_diquat_pesticide_belastning) as total_diquat_pesticide_belastning,
                        SUM(total_glyphosate_pesticide_belastning) as total_glyphosate_pesticide_belastning,
                        SUM(total_pesticide_applications) as total_pesticide_applications,
                        SUM(pfas_containing_applications) as pfas_containing_applications,
                        SUM(diquat_containing_applications) as diquat_containing_applications,
                        SUM(glyphosate_containing_applications) as glyphosate_containing_applications,
                        MAX(unique_pfas_products) as unique_pfas_products,
                        MAX(unique_diquat_products) as unique_diquat_products,
                        MAX(unique_glyphosate_products) as unique_glyphosate_products,
                        MAX(unique_pesticide_products) as unique_pesticide_products,
                        -- Recalculate intensity based on cumulative totals
                        CASE 
                            WHEN MAX(total_agricultural_area_ha) > 0 THEN 
                                SUM(total_pfas_containing_active_ingredient_grams) / MAX(total_agricultural_area_ha)
                            ELSE 0 
                        END as pfas_containing_active_ingredient_intensity_grams_per_ha,
                        CASE 
                            WHEN MAX(total_agricultural_area_ha) > 0 THEN 
                                SUM(total_diquat_containing_active_ingredient_grams) / MAX(total_agricultural_area_ha)
                            ELSE 0 
                        END as diquat_containing_active_ingredient_intensity_grams_per_ha,
                        CASE 
                            WHEN MAX(total_agricultural_area_ha) > 0 THEN 
                                SUM(total_glyphosate_containing_active_ingredient_grams) / MAX(total_agricultural_area_ha)
                            ELSE 0 
                        END as glyphosate_containing_active_ingredient_intensity_grams_per_ha,
                        CASE 
                            WHEN MAX(total_agricultural_area_ha) > 0 THEN 
                                SUM(total_pesticide_belastning) / MAX(total_agricultural_area_ha)
                            ELSE 0 
                        END as pesticide_belastning_per_ha,
                        CASE 
                            WHEN MAX(total_agricultural_area_ha) > 0 THEN 
                                SUM(total_pfas_pesticide_belastning) / MAX(total_agricultural_area_ha)
                            ELSE 0 
                        END as pfas_pesticide_belastning_per_ha,
                        CASE 
                            WHEN MAX(total_agricultural_area_ha) > 0 THEN 
                                SUM(total_diquat_pesticide_belastning) / MAX(total_agricultural_area_ha)
                            ELSE 0 
                        END as diquat_pesticide_belastning_per_ha,
                        CASE 
                            WHEN MAX(total_agricultural_area_ha) > 0 THEN 
                                SUM(total_glyphosate_pesticide_belastning) / MAX(total_agricultural_area_ha)
                            ELSE 0 
                        END as glyphosate_pesticide_belastning_per_ha,
                        CASE 
                            WHEN ANY_VALUE(kommune_area_ha) > 0 THEN 
                                (MAX(total_agricultural_area_ha) / ANY_VALUE(kommune_area_ha)) * 100.0
                            ELSE 0 
                        END as agricultural_coverage_pct,
                        CURRENT_TIMESTAMP as created_at
                    FROM {cumulative_kommune_table}
                    GROUP BY kommune_code
                    HAVING SUM(total_pesticide_belastning) > 0 OR SUM(total_pfas_containing_active_ingredient_grams) > 0
                """)

                # Save cumulative kommune results
                result_count = result_saver.save_cumulative_kommune_results(
                    final_cumulative_kommune_table, years
                )

                if result_count > 0:
                    logger.info(
                        f"✅ GitHub Actions optimized cumulative kommune analysis completed: {result_count:,} records"
                    )
                else:
                    logger.error("❌ GitHub Actions optimized cumulative kommune analysis failed")
                    all_success = False

                # Clean up intermediate tables
                processor.conn.execute(f"DROP TABLE IF EXISTS {cumulative_kommune_table}")
                processor.conn.execute(f"DROP TABLE IF EXISTS {final_cumulative_kommune_table}")

        # Final cleanup
        processor._aggressive_cleanup()

        if all_success:
            logger.success("✅ GitHub Actions optimized cumulative analysis completed successfully")
        else:
            logger.warning(
                "⚠️  GitHub Actions optimized cumulative analysis completed with some warnings/errors"
            )

        return all_success

    except Exception as e:
        logger.error(f"❌ Error in GitHub Actions optimized cumulative analysis: {e}")
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


async def run_cumulative_analysis_from_artifacts(
    years: list[int] | None = None,
    h3_resolutions: list[int] | None = None,
    include_kommune: bool = False,
    artifacts_dir: str = "downloaded-artifacts",
) -> bool:
    """
    Run cumulative analysis using GCS paths from GitHub Actions artifacts.

    This is the most efficient approach for GitHub Actions workflows:
    1. Individual year jobs save GCS paths as artifacts
    2. Cumulative job downloads artifacts and reads exact paths
    3. No GCS searching or pattern matching needed

    Args:
        years: List of years to include in cumulative analysis (None = all available)
        h3_resolutions: List of H3 resolutions to generate (default: [10])
        include_kommune: Whether to include kommune-level cumulative analysis
        artifacts_dir: Directory containing downloaded GitHub Actions artifacts

    Returns:
        bool: True if successful, False otherwise
    """
    logger.info("🚀 Starting GitHub Actions artifact-based cumulative H3 PFAS analysis")
    logger.info("   📦 Reading GCS paths from workflow artifacts (no searching needed)")

    if h3_resolutions is None:
        h3_resolutions = [10]

    logger.info(f"   🎯 H3 resolutions: {h3_resolutions}")
    if include_kommune:
        logger.info("   🏛️ Including kommune cumulative analysis")

    # Use the highest resolution for the base configuration
    base_resolution = max(h3_resolutions)

    # Create lightweight configuration for aggregation only
    config = H3SpatialConfig(
        h3_resolution=base_resolution,
        chunk_size=10000,
        memory_limit="14GB",
        thread_count=4,
        github_actions_mode=True,
        enable_memory_monitoring=True,
        enable_disk_monitoring=True,
        enable_time_monitoring=True,
        aggressive_cleanup=True,
        duckdb_memory_limit="12GB",
        duckdb_threads=4,
    )

    processor = H3PFASProcessorRefactored(config, local_data_dir=None)

    try:
        # Setup DuckDB once for all analyses
        processor.setup_duckdb()

        # Import dependencies
        import json
        from pathlib import Path

        from .data_loader import H3DataLoader
        from .result_saver import H3ResultSaver

        # Initialize components
        data_loader = H3DataLoader(processor.conn, processor.config, processor.gcs_access)
        result_saver = H3ResultSaver(processor.conn, processor.config, processor.gcs_access)

        # Read artifacts to get GCS paths
        artifacts_path = Path(artifacts_dir)
        if not artifacts_path.exists():
            logger.error(f"❌ Artifacts directory not found: {artifacts_dir}")
            return False

        # Load all summary files from artifacts
        summary_files = list(artifacts_path.glob("summary_*.json"))
        if not summary_files:
            logger.error(f"❌ No summary files found in {artifacts_dir}")
            return False

        logger.info(f"📦 Found {len(summary_files)} artifact summary files")

        # Parse artifacts to get available data
        available_years = []
        h3_files_by_year = {}
        kommune_files_by_year = {}

        for summary_file in summary_files:
            try:
                with open(summary_file) as f:
                    summary = json.load(f)

                year = summary["year"]
                h3_files = summary.get("h3_files", [])
                kommune_files = summary.get("kommune_files", [])

                available_years.append(year)
                if h3_files:
                    h3_files_by_year[year] = h3_files
                if kommune_files:
                    kommune_files_by_year[year] = kommune_files

                logger.info(
                    f"   📅 Year {year}: {len(h3_files)} H3 files, {len(kommune_files)} kommune files"
                )

            except Exception as e:
                logger.warning(f"   ⚠️  Failed to parse {summary_file}: {e}")

        # Determine years to process
        if years is None:
            years = sorted(available_years)
            logger.info(f"📅 Creating cumulative analysis from all available years: {years}")
        else:
            # Filter to only years we have data for
            years = [y for y in years if y in available_years]
            logger.info(f"📅 Creating cumulative analysis from specified years: {years}")

        if not years:
            logger.error("❌ No years available for cumulative analysis")
            return False

        # Process each H3 resolution
        all_success = True
        for resolution in h3_resolutions:
            logger.info(
                f"🔷 Creating artifact-based cumulative H3 analysis at resolution {resolution}"
            )

            # Update processor resolution for this analysis
            processor.config.update_resolution(resolution)

            # Get H3 files for this resolution from artifacts
            available_year_results = []
            for year in years:
                if year not in h3_files_by_year:
                    logger.warning(f"   ⚠️  No H3 files found for year {year}")
                    continue

                # Find files matching this resolution
                matching_files = [
                    f
                    for f in h3_files_by_year[year]
                    if f"_res{resolution}" in f and f.endswith(".parquet")
                ]

                if matching_files:
                    # Take the first matching file (should only be one per year/resolution)
                    file_path = matching_files[0]
                    available_year_results.append((year, file_path))
                    logger.info(f"   ✅ Year {year} resolution {resolution}: {file_path}")
                else:
                    logger.warning(f"   ⚠️  No resolution {resolution} files found for year {year}")

            if not available_year_results:
                logger.error(f"❌ No H3 files found for resolution {resolution}")
                all_success = False
                continue

            logger.info(
                f"📊 Loading and aggregating {len(available_year_results)} year(s) from artifacts for resolution {resolution}"
            )

            # Use the same aggregation logic as before
            # Initialize cumulative results table
            cumulative_table = f"cumulative_h3_results_res{resolution}"
            processor.conn.execute(f"""
                CREATE OR REPLACE TABLE {cumulative_table} (
                    h3_cell BIGINT,
                    center_lat DOUBLE,
                    center_lon DOUBLE,
                    h3_area_ha DOUBLE,
                    total_intersection_area_ha DOUBLE,
                    actual_coverage_ratio DOUBLE,
                    unique_field_count INTEGER,
                    total_pfas_containing_active_ingredient_grams DOUBLE,
                    total_diquat_containing_active_ingredient_grams DOUBLE,
                    total_glyphosate_containing_active_ingredient_grams DOUBLE,
                    total_pesticide_belastning DOUBLE,
                    total_pfas_pesticide_belastning DOUBLE,
                    total_diquat_pesticide_belastning DOUBLE,
                    total_glyphosate_pesticide_belastning DOUBLE,
                    total_pesticide_applications INTEGER,
                    pfas_containing_applications INTEGER,
                    diquat_containing_applications INTEGER,
                    glyphosate_containing_applications INTEGER,
                    crop_types VARCHAR,
                    crop_diversity INTEGER,
                    pfas_containing_active_ingredient_intensity_grams_per_ha DOUBLE,
                    diquat_containing_active_ingredient_intensity_grams_per_ha DOUBLE,
                    glyphosate_containing_active_ingredient_intensity_grams_per_ha DOUBLE,
                    pesticide_belastning_per_ha DOUBLE,
                    created_at TIMESTAMP
                )
            """)

            # Load and aggregate each year's results
            for year, file_path in available_year_results:
                logger.info(f"   📅 Loading results for year {year} from artifact path")

                # Load year results from GCS
                year_table = f"year_results_{year}_res{resolution}"
                try:
                    processor.gcs_access.create_table_from_gcs(year_table, file_path)

                    # Check if we have the expected columns (handle both regular and kepler formats)
                    columns = processor.conn.execute(f"PRAGMA table_info({year_table})").fetchall()
                    column_names = [col[1] for col in columns]

                    # Map column names based on format
                    h3_col = "h3_cell" if "h3_cell" in column_names else "h3_id"

                    # Insert into cumulative table with column mapping
                    processor.conn.execute(f"""
                        INSERT INTO {cumulative_table}
                        SELECT 
                            {h3_col} as h3_cell,
                            center_lat,
                            center_lon,
                            {"h3_area_ha" if "h3_area_ha" in column_names else "h3_cell_area_ha"} as h3_area_ha,
                            {"total_intersection_area_ha" if "total_intersection_area_ha" in column_names else "agricultural_area_ha"} as total_intersection_area_ha,
                            {"actual_coverage_ratio" if "actual_coverage_ratio" in column_names else "0.0"} as actual_coverage_ratio,
                            unique_field_count,
                            total_pfas_containing_active_ingredient_grams,
                            total_diquat_containing_active_ingredient_grams,
                            total_glyphosate_containing_active_ingredient_grams,
                            total_pesticide_belastning,
                            {"total_pfas_pesticide_belastning" if "total_pfas_pesticide_belastning" in column_names else "0.0"} as total_pfas_pesticide_belastning,
                            {"total_diquat_pesticide_belastning" if "total_diquat_pesticide_belastning" in column_names else "0.0"} as total_diquat_pesticide_belastning,
                            {"total_glyphosate_pesticide_belastning" if "total_glyphosate_pesticide_belastning" in column_names else "0.0"} as total_glyphosate_pesticide_belastning,
                            total_pesticide_applications,
                            pfas_containing_applications,
                            diquat_containing_applications,
                            glyphosate_containing_applications,
                            crop_types,
                            crop_diversity,
                            pfas_containing_active_ingredient_intensity_grams_per_ha,
                            diquat_containing_active_ingredient_intensity_grams_per_ha,
                            glyphosate_containing_active_ingredient_intensity_grams_per_ha,
                            {"pesticide_belastning_per_ha" if "pesticide_belastning_per_ha" in column_names else "pesticide_intensity"} as pesticide_belastning_per_ha,
                            {"created_at" if "created_at" in column_names else "CURRENT_TIMESTAMP"} as created_at
                        FROM {year_table}
                    """)

                    # Get count for logging
                    count = processor.conn.execute(f"SELECT COUNT(*) FROM {year_table}").fetchone()[
                        0
                    ]
                    logger.info(f"     📊 Loaded {count:,} records from year {year}")

                except Exception as e:
                    logger.error(f"   ❌ Failed to load year {year} data: {e}")
                    all_success = False
                finally:
                    # Clean up year table
                    processor.conn.execute(f"DROP TABLE IF EXISTS {year_table}")

            # Aggregate the cumulative data by H3 cell
            final_cumulative_table = f"final_cumulative_h3_res{resolution}"
            processor.conn.execute(f"""
                CREATE OR REPLACE TABLE {final_cumulative_table} AS
                SELECT
                    h3_cell,
                    ANY_VALUE(center_lat) as center_lat,
                    ANY_VALUE(center_lon) as center_lon,
                    ANY_VALUE(h3_area_ha) as h3_area_ha,
                    MAX(total_intersection_area_ha) as total_intersection_area_ha,
                    MAX(actual_coverage_ratio) as actual_coverage_ratio,
                    MAX(unique_field_count) as unique_field_count,
                    SUM(total_pfas_containing_active_ingredient_grams) as total_pfas_containing_active_ingredient_grams,
                    SUM(total_diquat_containing_active_ingredient_grams) as total_diquat_containing_active_ingredient_grams,
                    SUM(total_glyphosate_containing_active_ingredient_grams) as total_glyphosate_containing_active_ingredient_grams,
                    SUM(total_pesticide_belastning) as total_pesticide_belastning,
                    SUM(total_pfas_pesticide_belastning) as total_pfas_pesticide_belastning,
                    SUM(total_diquat_pesticide_belastning) as total_diquat_pesticide_belastning,
                    SUM(total_glyphosate_pesticide_belastning) as total_glyphosate_pesticide_belastning,
                    SUM(total_pesticide_applications) as total_pesticide_applications,
                    SUM(pfas_containing_applications) as pfas_containing_applications,
                    SUM(diquat_containing_applications) as diquat_containing_applications,
                    SUM(glyphosate_containing_applications) as glyphosate_containing_applications,
                    STRING_AGG(DISTINCT crop_types, '; ') as crop_types,
                    MAX(crop_diversity) as crop_diversity,
                    -- Recalculate intensity based on cumulative totals
                    CASE 
                        WHEN MAX(total_intersection_area_ha) > 0 THEN 
                            SUM(total_pfas_containing_active_ingredient_grams) / MAX(total_intersection_area_ha)
                        ELSE 0 
                    END as pfas_containing_active_ingredient_intensity_grams_per_ha,
                    CASE 
                        WHEN MAX(total_intersection_area_ha) > 0 THEN 
                            SUM(total_diquat_containing_active_ingredient_grams) / MAX(total_intersection_area_ha)
                        ELSE 0 
                    END as diquat_containing_active_ingredient_intensity_grams_per_ha,
                    CASE 
                        WHEN MAX(total_intersection_area_ha) > 0 THEN 
                            SUM(total_glyphosate_containing_active_ingredient_grams) / MAX(total_intersection_area_ha)
                        ELSE 0 
                    END as glyphosate_containing_active_ingredient_intensity_grams_per_ha,
                    CASE 
                        WHEN MAX(total_intersection_area_ha) > 0 THEN 
                            SUM(total_pesticide_belastning) / MAX(total_intersection_area_ha)
                        ELSE 0 
                    END as pesticide_belastning_per_ha,
                    CURRENT_TIMESTAMP as created_at
                FROM {cumulative_table}
                GROUP BY h3_cell
                HAVING SUM(total_pesticide_belastning) > 0 OR SUM(total_pfas_containing_active_ingredient_grams) > 0
            """)

            # Validate cumulative results
            try:
                processor._validate_results(final_cumulative_table)
            except Exception as e:
                logger.warning(f"   ⚠️  Validation warning for resolution {resolution}: {e}")

            # Save cumulative results with special "total" year identifier
            result_count = result_saver.save_cumulative_results(
                final_cumulative_table, resolution, years
            )

            if result_count > 0:
                logger.info(
                    f"✅ Artifact-based cumulative H3 analysis completed for resolution {resolution}: {result_count:,} records"
                )
            else:
                logger.error(
                    f"❌ Artifact-based cumulative H3 analysis failed for resolution {resolution}"
                )
                all_success = False

            # Clean up intermediate tables
            processor.conn.execute(f"DROP TABLE IF EXISTS {cumulative_table}")
            processor.conn.execute(f"DROP TABLE IF EXISTS {final_cumulative_table}")

        # Handle kommune cumulative analysis if requested
        if include_kommune:
            logger.info("🏛️ Creating artifact-based cumulative kommune analysis")

            # Get kommune files from artifacts
            available_kommune_results = []
            for year in years:
                if year not in kommune_files_by_year:
                    logger.warning(f"   ⚠️  No kommune files found for year {year}")
                    continue

                # Take the first kommune file for this year
                if kommune_files_by_year[year]:
                    file_path = kommune_files_by_year[year][0]
                    available_kommune_results.append((year, file_path))
                    logger.info(f"   ✅ Year {year} kommune: {file_path}")

            if not available_kommune_results:
                logger.warning(
                    "⚠️  No kommune files found in artifacts - skipping kommune cumulative analysis"
                )
            else:
                logger.info(
                    f"📊 Loading and aggregating {len(available_kommune_results)} year(s) of kommune results from artifacts"
                )

                # Initialize cumulative kommune results table
                cumulative_kommune_table = "cumulative_kommune_results"
                processor.conn.execute(f"""
                    CREATE OR REPLACE TABLE {cumulative_kommune_table} (
                        kommune_code INTEGER,
                        kommune_name VARCHAR,
                        region_code INTEGER,
                        kommune_area_ha DOUBLE,
                        kommune_centroid_x DOUBLE,
                        kommune_centroid_y DOUBLE,
                        total_agricultural_area_ha DOUBLE,
                        unique_field_count INTEGER,
                        unique_company_count INTEGER,
                        avg_field_coverage_ratio DOUBLE,
                        max_field_coverage_ratio DOUBLE,
                        min_field_coverage_ratio DOUBLE,
                        crop_diversity INTEGER,
                        crop_types VARCHAR,
                        total_pfas_containing_active_ingredient_grams DOUBLE,
                        total_diquat_containing_active_ingredient_grams DOUBLE,
                        total_glyphosate_containing_active_ingredient_grams DOUBLE,
                        total_pesticide_belastning DOUBLE,
                        total_pfas_pesticide_belastning DOUBLE,
                        total_diquat_pesticide_belastning DOUBLE,
                        total_glyphosate_pesticide_belastning DOUBLE,
                        total_pesticide_applications INTEGER,
                        pfas_containing_applications INTEGER,
                        diquat_containing_applications INTEGER,
                        glyphosate_containing_applications INTEGER,
                        unique_pfas_products INTEGER,
                        unique_diquat_products INTEGER,
                        unique_glyphosate_products INTEGER,
                        unique_pesticide_products INTEGER,
                        pfas_containing_active_ingredient_intensity_grams_per_ha DOUBLE,
                        diquat_containing_active_ingredient_intensity_grams_per_ha DOUBLE,
                        glyphosate_containing_active_ingredient_intensity_grams_per_ha DOUBLE,
                        pesticide_belastning_per_ha DOUBLE,
                        pfas_pesticide_belastning_per_ha DOUBLE,
                        diquat_pesticide_belastning_per_ha DOUBLE,
                        glyphosate_pesticide_belastning_per_ha DOUBLE,
                        agricultural_coverage_pct DOUBLE,
                        created_at TIMESTAMP
                    )
                """)

                # Load and aggregate each year's kommune results
                for year, file_path in available_kommune_results:
                    logger.info(f"   📅 Loading kommune results for year {year} from artifact path")

                    # Load year kommune results from GCS
                    year_kommune_table = f"year_kommune_results_{year}"
                    try:
                        processor.gcs_access.create_table_from_gcs(year_kommune_table, file_path)

                        # Insert into cumulative table
                        processor.conn.execute(f"""
                            INSERT INTO {cumulative_kommune_table}
                            SELECT * FROM {year_kommune_table}
                        """)

                        # Get count for logging
                        count = processor.conn.execute(
                            f"SELECT COUNT(*) FROM {year_kommune_table}"
                        ).fetchone()[0]
                        logger.info(f"     📊 Loaded {count:,} kommune records from year {year}")

                    except Exception as e:
                        logger.error(f"   ❌ Failed to load kommune year {year} data: {e}")
                        all_success = False
                    finally:
                        # Clean up year table
                        processor.conn.execute(f"DROP TABLE IF EXISTS {year_kommune_table}")

                # Aggregate the cumulative kommune data (same logic as before)
                final_cumulative_kommune_table = "final_cumulative_kommune"
                processor.conn.execute(f"""
                    CREATE OR REPLACE TABLE {final_cumulative_kommune_table} AS
                    SELECT
                        kommune_code,
                        ANY_VALUE(kommune_name) as kommune_name,
                        ANY_VALUE(region_code) as region_code,
                        ANY_VALUE(kommune_area_ha) as kommune_area_ha,
                        ANY_VALUE(kommune_centroid_x) as kommune_centroid_x,
                        ANY_VALUE(kommune_centroid_y) as kommune_centroid_y,
                        MAX(total_agricultural_area_ha) as total_agricultural_area_ha,
                        MAX(unique_field_count) as unique_field_count,
                        MAX(unique_company_count) as unique_company_count,
                        AVG(avg_field_coverage_ratio) as avg_field_coverage_ratio,
                        MAX(max_field_coverage_ratio) as max_field_coverage_ratio,
                        MIN(min_field_coverage_ratio) as min_field_coverage_ratio,
                        MAX(crop_diversity) as crop_diversity,
                        STRING_AGG(DISTINCT crop_types, '; ') as crop_types,
                        SUM(total_pfas_containing_active_ingredient_grams) as total_pfas_containing_active_ingredient_grams,
                        SUM(total_diquat_containing_active_ingredient_grams) as total_diquat_containing_active_ingredient_grams,
                        SUM(total_glyphosate_containing_active_ingredient_grams) as total_glyphosate_containing_active_ingredient_grams,
                        SUM(total_pesticide_belastning) as total_pesticide_belastning,
                        SUM(total_pfas_pesticide_belastning) as total_pfas_pesticide_belastning,
                        SUM(total_diquat_pesticide_belastning) as total_diquat_pesticide_belastning,
                        SUM(total_glyphosate_pesticide_belastning) as total_glyphosate_pesticide_belastning,
                        SUM(total_pesticide_applications) as total_pesticide_applications,
                        SUM(pfas_containing_applications) as pfas_containing_applications,
                        SUM(diquat_containing_applications) as diquat_containing_applications,
                        SUM(glyphosate_containing_applications) as glyphosate_containing_applications,
                        MAX(unique_pfas_products) as unique_pfas_products,
                        MAX(unique_diquat_products) as unique_diquat_products,
                        MAX(unique_glyphosate_products) as unique_glyphosate_products,
                        MAX(unique_pesticide_products) as unique_pesticide_products,
                        -- Recalculate intensity based on cumulative totals
                        CASE 
                            WHEN MAX(total_agricultural_area_ha) > 0 THEN 
                                SUM(total_pfas_containing_active_ingredient_grams) / MAX(total_agricultural_area_ha)
                            ELSE 0 
                        END as pfas_containing_active_ingredient_intensity_grams_per_ha,
                        CASE 
                            WHEN MAX(total_agricultural_area_ha) > 0 THEN 
                                SUM(total_diquat_containing_active_ingredient_grams) / MAX(total_agricultural_area_ha)
                            ELSE 0 
                        END as diquat_containing_active_ingredient_intensity_grams_per_ha,
                        CASE 
                            WHEN MAX(total_agricultural_area_ha) > 0 THEN 
                                SUM(total_glyphosate_containing_active_ingredient_grams) / MAX(total_agricultural_area_ha)
                            ELSE 0 
                        END as glyphosate_containing_active_ingredient_intensity_grams_per_ha,
                        CASE 
                            WHEN MAX(total_agricultural_area_ha) > 0 THEN 
                                SUM(total_pesticide_belastning) / MAX(total_agricultural_area_ha)
                            ELSE 0 
                        END as pesticide_belastning_per_ha,
                        CASE 
                            WHEN MAX(total_agricultural_area_ha) > 0 THEN 
                                SUM(total_pfas_pesticide_belastning) / MAX(total_agricultural_area_ha)
                            ELSE 0 
                        END as pfas_pesticide_belastning_per_ha,
                        CASE 
                            WHEN MAX(total_agricultural_area_ha) > 0 THEN 
                                SUM(total_diquat_pesticide_belastning) / MAX(total_agricultural_area_ha)
                            ELSE 0 
                        END as diquat_pesticide_belastning_per_ha,
                        CASE 
                            WHEN MAX(total_agricultural_area_ha) > 0 THEN 
                                SUM(total_glyphosate_pesticide_belastning) / MAX(total_agricultural_area_ha)
                            ELSE 0 
                        END as glyphosate_pesticide_belastning_per_ha,
                        CASE 
                            WHEN ANY_VALUE(kommune_area_ha) > 0 THEN 
                                (MAX(total_agricultural_area_ha) / ANY_VALUE(kommune_area_ha)) * 100.0
                            ELSE 0 
                        END as agricultural_coverage_pct,
                        CURRENT_TIMESTAMP as created_at
                    FROM {cumulative_kommune_table}
                    GROUP BY kommune_code
                    HAVING SUM(total_pesticide_belastning) > 0 OR SUM(total_pfas_containing_active_ingredient_grams) > 0
                """)

                # Save cumulative kommune results
                result_count = result_saver.save_cumulative_kommune_results(
                    final_cumulative_kommune_table, years
                )

                if result_count > 0:
                    logger.info(
                        f"✅ Artifact-based cumulative kommune analysis completed: {result_count:,} records"
                    )
                else:
                    logger.error("❌ Artifact-based cumulative kommune analysis failed")
                    all_success = False

                # Clean up intermediate tables
                processor.conn.execute(f"DROP TABLE IF EXISTS {cumulative_kommune_table}")
                processor.conn.execute(f"DROP TABLE IF EXISTS {final_cumulative_kommune_table}")

        # Final cleanup
        processor._aggressive_cleanup()

        if all_success:
            logger.success("✅ Artifact-based cumulative analysis completed successfully")
        else:
            logger.warning(
                "⚠️  Artifact-based cumulative analysis completed with some warnings/errors"
            )

        return all_success

    except Exception as e:
        logger.error(f"❌ Error in artifact-based cumulative analysis: {e}")
        return False
