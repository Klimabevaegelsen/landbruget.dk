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
