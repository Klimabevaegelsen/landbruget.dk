"""Main pipeline class for H3 PFAS exposure analysis."""

import asyncio

from loguru import logger

from ..config import H3PFASConfig
from .h3_processor import H3PFASProcessorRefactored, H3SpatialConfig


class H3PFASPipeline:
    """Main pipeline for H3 PFAS exposure analysis."""

    def __init__(self, config: H3PFASConfig):
        self.config = config
        self.log = logger.bind(pipeline="H3PFASPipeline")

    async def run_h3_analysis(self, years: list[int] | None = None) -> bool:
        """Run H3-based PFAS analysis for specified years."""
        self.log.info("🚀 Starting H3 PFAS exposure analysis")

        # DEBUG: Log the memory_limit from H3PFASConfig
        self.log.info(f"🔍 DEBUG: H3PFASConfig memory_limit = '{self.config.memory_limit}'")

        # Convert config to H3SpatialConfig
        h3_config = H3SpatialConfig(
            h3_resolution=self.config.h3_resolution,
            denmark_bounds=self.config.denmark_bounds,
            chunk_size=self.config.chunk_size,
            memory_limit=self.config.memory_limit,
            thread_count=self.config.thread_count,
            bucket=self.config.bucket,
            available_years=years or self.config.available_years,
            min_h3_area_ha=self.config.min_h3_area_ha,
            max_h3_area_ha=self.config.max_h3_area_ha,
            theoretical_avg_area_ha=self.config.theoretical_avg_area_ha,
            max_area_deviation_pct=self.config.max_area_deviation_pct,
            enable_progress_tracking=self.config.enable_progress_tracking,
            log_chunk_details=self.config.log_chunk_details,
            log_stage_timings=self.config.log_stage_timings,
        )

        # DEBUG: Log the memory_limit from H3SpatialConfig
        self.log.info(f"🔍 DEBUG: H3SpatialConfig memory_limit = '{h3_config.memory_limit}'")

        # Create processor
        processor = H3PFASProcessorRefactored(h3_config, local_data_dir=None)

        try:
            # Run multi-year analysis
            success = await processor.run_analysis_multi_year(years)

            if success:
                self.log.info("✅ H3 PFAS exposure analysis completed successfully")
            else:
                self.log.error("❌ H3 PFAS exposure analysis failed")

            return success

        except Exception as e:
            self.log.error(f"❌ H3 PFAS exposure analysis failed: {e}")
            import traceback

            self.log.error(f"Traceback: {traceback.format_exc()}")
            return False

    async def run_kommune_analysis(self, years: list[int] | None = None) -> bool:
        """Run kommune-level PFAS analysis for specified years."""
        self.log.info("🏛️ Starting kommune-level PFAS analysis")

        # Convert config to H3SpatialConfig
        h3_config = H3SpatialConfig(
            h3_resolution=self.config.h3_resolution,
            denmark_bounds=self.config.denmark_bounds,
            chunk_size=self.config.chunk_size,
            memory_limit=self.config.memory_limit,
            thread_count=self.config.thread_count,
            bucket=self.config.bucket,
            available_years=years or self.config.available_years,
            min_h3_area_ha=self.config.min_h3_area_ha,
            max_h3_area_ha=self.config.max_h3_area_ha,
            theoretical_avg_area_ha=self.config.theoretical_avg_area_ha,
            max_area_deviation_pct=self.config.max_area_deviation_pct,
            enable_progress_tracking=self.config.enable_progress_tracking,
            log_chunk_details=self.config.log_chunk_details,
            log_stage_timings=self.config.log_stage_timings,
        )

        # Create processor
        processor = H3PFASProcessorRefactored(h3_config, local_data_dir=None)

        try:
            # Run multi-year kommune analysis
            success = await processor.run_kommune_analysis_multi_year(years)

            if success:
                self.log.info("✅ Kommune-level PFAS analysis completed successfully")
            else:
                self.log.error("❌ Kommune-level PFAS analysis failed")

            return success

        except Exception as e:
            self.log.error(f"❌ Kommune-level PFAS analysis failed: {e}")
            import traceback

            self.log.error(f"Traceback: {traceback.format_exc()}")
            return False

    async def run_all_analyses(
        self, years: list[int] | None = None, parallel: bool = False
    ) -> bool:
        """Run both H3 and kommune-level analyses.

        Args:
            years: List of years to process
            parallel: If True, run analyses in parallel; if False, run sequentially
        """
        if parallel:
            return await self.run_all_analyses_parallel(years)
        else:
            return await self.run_all_analyses_sequential(years)

    async def run_all_analyses_sequential(self, years: list[int] | None = None) -> bool:
        """Run both H3 and kommune-level analyses sequentially."""
        self.log.info("🔄 Starting all PFAS analyses (sequential)")

        # Run H3 analysis first
        h3_success = await self.run_h3_analysis(years)

        # Run kommune analysis
        kommune_success = await self.run_kommune_analysis(years)

        overall_success = h3_success and kommune_success

        if overall_success:
            self.log.info("✅ All PFAS analyses completed successfully")
        else:
            self.log.error("❌ Some PFAS analyses failed")

        return overall_success

    async def run_all_analyses_parallel(self, years: list[int] | None = None) -> bool:
        """Run both H3 and kommune-level analyses in parallel."""
        self.log.info("🔄 Starting all PFAS analyses (parallel)")

        try:
            # Run both analyses concurrently
            h3_task = asyncio.create_task(self.run_h3_analysis(years))
            kommune_task = asyncio.create_task(self.run_kommune_analysis(years))

            # Wait for both to complete
            h3_success, kommune_success = await asyncio.gather(h3_task, kommune_task)

            overall_success = h3_success and kommune_success

            if overall_success:
                self.log.info("✅ All PFAS analyses completed successfully (parallel)")
            else:
                self.log.error("❌ Some PFAS analyses failed (parallel)")

            return overall_success

        except Exception as e:
            self.log.error(f"❌ Parallel PFAS analyses failed: {e}")
            import traceback

            self.log.error(f"Traceback: {traceback.format_exc()}")
            return False
