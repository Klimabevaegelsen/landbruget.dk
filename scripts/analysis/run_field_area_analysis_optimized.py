#!/usr/bin/env python3
"""
Run optimized field area analysis with memory monitoring.

This script runs the field area analysis gold layer with optimizations for:
- DuckDB Spatial v1.2.2 spatial join operator
- Memory-efficient sequential processing
- Aggressive cleanup between operations
- Memory and disk usage monitoring
"""

import asyncio
import sys
import time
from pathlib import Path

# Add the unified pipeline to the path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "backend" / "pipelines" / "unified_pipeline" / "src"))

from unified_pipeline.gold.field_area_analysis import FieldAreaAnalysisGold, FieldAreaAnalysisGoldConfig
from unified_pipeline.util.log_util import Logger


async def run_optimized_analysis():
    """Run the optimized field area analysis."""
    log = Logger.get_logger()

    log.info("🚀 Starting Optimized Field Area Analysis")
    log.info("=" * 80)

    # Create optimized configuration for 14GB system
    config = FieldAreaAnalysisGoldConfig(
        memory_limit="6GB",  # Very conservative memory limit
        thread_count=1,  # Single thread to minimize memory usage
        batch_size=2500,  # Reasonable batch size
        min_area_threshold=0.01,  # 1% minimum area threshold
    )

    log.info("🔧 Configuration:")
    log.info(f"  Memory limit: {config.memory_limit}")
    log.info(f"  Thread count: {config.thread_count}")
    log.info(f"  Batch size: {config.batch_size:,}")
    log.info(f"  Min area threshold: {config.min_area_threshold}%")

    # Initialize processor
    processor = FieldAreaAnalysisGold(config)

    try:
        start_time = time.time()

        # Run the analysis
        log.info("🚀 Starting field area analysis processing...")
        await processor.run()

        end_time = time.time()
        duration = end_time - start_time

        log.info("=" * 80)
        log.info("✅ Field Area Analysis completed successfully!")
        log.info(f"   Total processing time: {duration:.1f} seconds ({duration / 60:.1f} minutes)")
        log.info("   Results saved to GCS by year to avoid memory overflow")

    except Exception as e:
        log.error(f"❌ Field Area Analysis failed: {e}")
        raise
    finally:
        # Ensure cleanup
        if hasattr(processor, "_final_cleanup_temp_files"):
            processor._final_cleanup_temp_files()


if __name__ == "__main__":
    asyncio.run(run_optimized_analysis())
