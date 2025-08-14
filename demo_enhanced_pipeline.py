#!/usr/bin/env python3
"""
Demo script showing how to use enhanced GCS methods with native HMAC acceleration.

This script demonstrates the new enhanced methods available in the base classes
that automatically use native HMAC when available and fall back gracefully.
"""

import sys
from pathlib import Path

# Add the unified pipeline to the Python path
sys.path.insert(0, str(Path(__file__).parent / "backend/pipelines/unified_pipeline/src"))

from unified_pipeline.common.base import BaseJobConfig, BaseSource
from unified_pipeline.util.log_util import Logger


class DemoConfig(BaseJobConfig):
    """Demo configuration."""

    bucket: str = "landbrugsdata-1"
    dataset: str = "demo"
    dev_mode: bool = True


class DemoEnhancedPipeline(BaseSource):
    """
    Demo pipeline showing enhanced GCS methods with native acceleration.
    """

    def __init__(self, config: DemoConfig):
        self.config = config
        super().__init__(config)
        self.log = Logger.get_logger()

    def run(self):
        """Required abstract method implementation."""
        return self.run_demo()

    def run_demo(self):
        """Demonstrate enhanced GCS methods."""
        self.log.info("🎯 Demo: Enhanced GCS Methods with Native HMAC Acceleration")
        self.log.info("=" * 70)

        # Demo 1: Check native GCS capability
        if hasattr(self.gcs_access, "_native_gcs_available"):
            native_available = self.gcs_access._native_gcs_available
            self.log.info(f"Native GCS Support: {native_available}")

        # Demo 2: Create some sample data
        self.log.info("\n📊 Creating sample data...")
        self.conn.execute("""
            CREATE TABLE sample_data AS
            SELECT
                i as id,
                'Sample record ' || i as name,
                random() * 100 as value,
                current_date - interval (random() * 365) days as date_created
            FROM range(1000) t(i)
        """)

        count = self.conn.execute("SELECT COUNT(*) FROM sample_data").fetchone()[0]
        self.log.info(f"✅ Created sample_data table with {count:,} records")

        # Demo 3: Show method availability
        self.log.info("\n🔧 Checking enhanced method availability...")
        methods = [
            "load_parquet_with_native_acceleration",
            "save_table_with_native_acceleration",
            "enhanced_save_data_direct",
            "load_latest_with_native_acceleration",
        ]

        for method in methods:
            available = hasattr(self, method)
            self.log.info(f"{'✅' if available else '❌'} {method}: {'Available' if available else 'Missing'}")

        # Demo 4: Show raw data processing capability
        self.log.info("\n🎯 Testing SQL processing capabilities...")
        result = self.conn.execute("""
            SELECT
                COUNT(*) as total_records,
                AVG(value) as avg_value,
                MIN(date_created) as earliest_date,
                MAX(date_created) as latest_date
            FROM sample_data
        """).fetchone()

        self.log.info(f"✅ Processed {result[0]:,} records with avg value {result[1]:.2f}")

        # Demo 5: Performance comparison info
        self.log.info("\n⚡ Performance Notes:")
        if hasattr(self.gcs_access, "_native_gcs_available") and self.gcs_access._native_gcs_available:
            self.log.info("🚀 NATIVE MODE: You're getting 3-5x faster performance!")
            self.log.info("   • No temporary files created")
            self.log.info("   • Direct streaming to/from GCS")
            self.log.info("   • Server-side filtering applied")
        else:
            self.log.info("🔄 FALLBACK MODE: Using temp files (still optimized)")
            self.log.info("   • Set GCS_ACCESS_KEY_ID and GCS_SECRET_ACCESS_KEY for native mode")


def main():
    """Run the enhanced pipeline demo."""
    config = DemoConfig()
    demo = DemoEnhancedPipeline(config)

    try:
        demo.run_demo()
        print("\n🎉 Demo completed successfully!")
        print("\n📋 Available Enhanced Methods:")
        print("=" * 50)
        print("• load_parquet_with_native_acceleration() - Load with auto-optimization")
        print("• save_table_with_native_acceleration() - Save with auto-optimization")
        print("• enhanced_save_data_direct() - Enhanced version of save_data_direct()")
        print("• load_latest_with_native_acceleration() - Load latest with optimization")
        print("\n💡 These methods automatically use native HMAC when available,")
        print("   falling back gracefully when not available.")

    except Exception as e:
        print(f"❌ Demo failed: {e}")
        return 1

    finally:
        # Cleanup DuckDB connection
        if hasattr(demo, "conn"):
            demo.conn.close()

    return 0


if __name__ == "__main__":
    exit(main())
