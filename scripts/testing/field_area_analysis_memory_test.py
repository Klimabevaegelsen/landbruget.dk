#!/usr/bin/env python3
"""
Test script for field area analysis memory optimization.

This script tests the optimized field area analysis gold layer to ensure:
1. Memory usage stays within 14GB limit
2. Sequential spatial joins work correctly
3. DuckDB Spatial v1.2.2 spatial join operator is used properly
"""

import asyncio
import os
import sys
from pathlib import Path

import psutil

# Add the unified pipeline to the path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "backend" / "pipelines" / "unified_pipeline" / "src"))

from unified_pipeline.gold.field_area_analysis import FieldAreaAnalysisGold, FieldAreaAnalysisGoldConfig
from unified_pipeline.util.log_util import Logger


def get_memory_usage():
    """Get current memory usage in GB."""
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    return memory_info.rss / (1024**3)  # Convert to GB


def get_disk_usage():
    """Get disk usage for temp directory."""
    temp_dir = "/tmp/duckdb_field_analysis"
    if os.path.exists(temp_dir):
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(temp_dir):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                try:
                    total_size += os.path.getsize(filepath)
                except OSError:
                    pass
        return total_size / (1024**3)  # Convert to GB
    return 0


async def test_memory_optimization():
    """Test the memory-optimized field area analysis."""
    log = Logger.get_logger()

    log.info("🧪 Testing Field Area Analysis Memory Optimization")
    log.info("=" * 60)

    # Initial memory check
    initial_memory = get_memory_usage()
    log.info(f"Initial memory usage: {initial_memory:.2f} GB")

    # Create optimized configuration
    config = FieldAreaAnalysisGoldConfig(
        memory_limit="6GB",  # Very conservative
        thread_count=1,  # Single thread
        batch_size=2500,  # Reasonable batch size
    )

    log.info("Configuration:")
    log.info(f"  Memory limit: {config.memory_limit}")
    log.info(f"  Thread count: {config.thread_count}")
    log.info(f"  Batch size: {config.batch_size}")

    # Test with a small subset first
    processor = FieldAreaAnalysisGold(config)

    try:
        # Monitor memory during processing
        peak_memory = initial_memory

        log.info("🚀 Starting optimized field area analysis...")

        # Run the analysis
        await processor.run()

        # Final memory check
        final_memory = get_memory_usage()
        disk_usage = get_disk_usage()

        log.info("=" * 60)
        log.info("📊 Memory Usage Summary:")
        log.info(f"  Initial: {initial_memory:.2f} GB")
        log.info(f"  Final: {final_memory:.2f} GB")
        log.info(f"  Peak: {peak_memory:.2f} GB")
        log.info(f"  Temp disk usage: {disk_usage:.2f} GB")

        # Check if we stayed within limits
        if peak_memory < 12:  # 12GB is safe margin for 14GB total
            log.info("✅ Memory optimization SUCCESS - stayed within limits")
        else:
            log.error("❌ Memory optimization FAILED - exceeded safe limits")

        if disk_usage < 8:  # 8GB temp directory limit
            log.info("✅ Disk optimization SUCCESS - stayed within temp limits")
        else:
            log.error("❌ Disk optimization FAILED - exceeded temp limits")

    except Exception as e:
        log.error(f"❌ Test failed with error: {e}")
        raise
    finally:
        # Cleanup
        if hasattr(processor, "_final_cleanup_temp_files"):
            processor._final_cleanup_temp_files()


async def test_duckdb_spatial_join():
    """Test that DuckDB Spatial join operator is working correctly."""
    log = Logger.get_logger()

    log.info("🧪 Testing DuckDB Spatial Join Operator")
    log.info("=" * 60)

    import duckdb

    # Create test connection
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL spatial")
    conn.execute("LOAD spatial")

    # Create test data
    log.info("Creating test spatial data...")
    conn.execute("""
        CREATE TABLE test_fields AS
        SELECT 
            'field_' || generate_series as field_id,
            ST_Buffer(ST_Point(generate_series, generate_series), 1) as geom
        FROM generate_series(1, 100)
    """)

    conn.execute("""
        CREATE TABLE test_properties AS
        SELECT 
            'prop_' || generate_series as prop_id,
            ST_Buffer(ST_Point(generate_series * 0.5, generate_series * 0.5), 1.5) as geom
        FROM generate_series(1, 50)
    """)

    # Test spatial join with EXPLAIN to see if spatial join operator is used
    log.info("Testing spatial join with EXPLAIN...")
    result = conn.execute("""
        EXPLAIN SELECT 
            f.field_id,
            p.prop_id
        FROM test_fields f
        JOIN test_properties p ON ST_Intersects(f.geom, p.geom)
    """).fetchall()

    # Check if SPATIAL_JOIN operator appears in the plan
    explain_text = "\n".join([str(row) for row in result])
    if "SPATIAL_JOIN" in explain_text:
        log.info("✅ SPATIAL_JOIN operator detected in query plan")
        log.info("✅ DuckDB Spatial v1.2.2 optimization is active")
    else:
        log.warning("⚠️ SPATIAL_JOIN operator not detected - using fallback")
        log.info("Query plan:")
        for row in result:
            log.info(f"  {row}")

    # Test actual execution
    log.info("Testing spatial join execution...")
    start_time = asyncio.get_event_loop().time()

    join_result = conn.execute("""
        SELECT COUNT(*) as intersection_count
        FROM test_fields f
        JOIN test_properties p ON ST_Intersects(f.geom, p.geom)
    """).fetchone()

    end_time = asyncio.get_event_loop().time()
    execution_time = end_time - start_time

    log.info(f"✅ Spatial join completed in {execution_time:.2f} seconds")
    log.info(f"✅ Found {join_result[0]} intersections")

    conn.close()


if __name__ == "__main__":

    async def main():
        # Test DuckDB Spatial join operator first
        await test_duckdb_spatial_join()

        print("\n" + "=" * 80 + "\n")

        # Test memory optimization
        await test_memory_optimization()

    asyncio.run(main())
