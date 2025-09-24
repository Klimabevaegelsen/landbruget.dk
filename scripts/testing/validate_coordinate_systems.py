#!/usr/bin/env python3
"""
Coordinate System Validation Script for Buildings PMTiles Investigation.

This script validates the coordinate systems of BBR buildings and FVM marker data
to identify and resolve spatial join issues.
"""

import asyncio
import logging
import sys
from pathlib import Path

# Add the unified pipeline to the path
sys.path.append(str(Path(__file__).parent.parent.parent / "backend" / "pipelines" / "unified_pipeline" / "src"))

import duckdb
from unified_pipeline.gold.pmtiles_generator.config import PMTilesConfig
from unified_pipeline.gold.pmtiles_generator.data_loader import DataLoader

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class CoordinateSystemValidator:
    """Validates coordinate systems between BBR buildings and FVM marker data."""

    def __init__(self):
        self.config = PMTilesConfig()
        self.conn = duckdb.connect()
        self.data_loader = DataLoader(self.config, self.conn)

    async def validate_coordinate_systems(self):
        """Main validation process."""
        logger.info("🔍 Starting coordinate system validation...")

        try:
            # 1. Load and analyze BBR buildings
            await self._analyze_bbr_buildings()

            # 2. Load and analyze FVM marker data
            await self._analyze_fvm_marker_data()

            # 3. Test spatial joins
            await self._test_spatial_joins()

            # 4. Test coordinate flipping
            await self._test_coordinate_flipping()

            logger.info("✅ Coordinate system validation completed")

        except Exception as e:
            logger.error(f"❌ Validation failed: {e}")
            raise
        finally:
            self.conn.close()

    async def _analyze_bbr_buildings(self):
        """Analyze BBR buildings coordinate system."""
        logger.info("📊 Analyzing BBR buildings coordinate system...")

        # Load environmental layers to get buildings
        environmental_layers = await self.data_loader.load_environmental_layers()

        if "bbr_buildings" not in environmental_layers:
            logger.error("❌ BBR buildings data not available")
            return

        buildings_table = environmental_layers["bbr_buildings"]
        logger.info(f"📋 BBR buildings table: {buildings_table}")

        # Get sample of building coordinates
        result = await asyncio.to_thread(
            self.conn.execute,
            f"""
            SELECT
                COUNT(*) as total_buildings,
                MIN(ST_X(geometry)) as min_x,
                MAX(ST_X(geometry)) as max_x,
                MIN(ST_Y(geometry)) as min_y,
                MAX(ST_Y(geometry)) as max_y,
                AVG(ST_X(geometry)) as avg_x,
                AVG(ST_Y(geometry)) as avg_y
            FROM {buildings_table}
            WHERE geometry IS NOT NULL
            LIMIT 1000
            """,
        )

        stats = result.fetchone()
        logger.info("🏢 BBR Buildings Stats:")
        logger.info(f"   Total buildings (sample): {stats[0]:,}")
        logger.info(f"   X coordinates: {stats[1]:.6f} to {stats[2]:.6f} (avg: {stats[5]:.6f})")
        logger.info(f"   Y coordinates: {stats[3]:.6f} to {stats[4]:.6f} (avg: {stats[6]:.6f})")

        # Determine likely coordinate system
        if stats[1] > 0 and stats[2] < 20 and stats[3] > 50 and stats[4] < 60:
            logger.info("   🗺️ Likely coordinate system: Geographic (lon, lat)")
        elif stats[1] > 400000 and stats[2] < 900000:
            logger.info("   🗺️ Likely coordinate system: UTM or similar projected")
        else:
            logger.warning("   ⚠️ Coordinate system unclear from ranges")

        # Sample specific coordinates
        sample_result = await asyncio.to_thread(
            self.conn.execute,
            f"""
            SELECT
                building_uuid,
                ST_X(geometry) as x,
                ST_Y(geometry) as y,
                address
            FROM {buildings_table}
            WHERE geometry IS NOT NULL
            LIMIT 5
            """,
        )

        logger.info("🔍 Sample building coordinates:")
        for row in sample_result.fetchall():
            logger.info(f"   {row[0]}: ({row[1]:.6f}, {row[2]:.6f}) - {row[3]}")

    async def _analyze_fvm_marker_data(self):
        """Analyze FVM marker data coordinate system."""
        logger.info("📊 Analyzing FVM marker data coordinate system...")

        # Load agricultural fields data
        try:
            base_path = f"gs://{self.config.gcs_bucket}/{self.config.fvm_marker_path}"
            latest_path = await self.data_loader._find_latest_timestamped_path(base_path)

            if not latest_path:
                logger.error("❌ FVM marker data not found")
                return

            logger.info(f"📋 FVM marker path: {latest_path}")

            # Load data
            await asyncio.to_thread(
                self.conn.execute,
                f"""
                CREATE OR REPLACE TABLE fvm_marker_sample AS
                SELECT *
                FROM read_parquet('{latest_path}/**/*.parquet')
                WHERE geometry IS NOT NULL
                LIMIT 1000
                """,
            )

            # Get coordinate statistics
            result = await asyncio.to_thread(
                self.conn.execute,
                """
                SELECT
                    COUNT(*) as total_fields,
                    MIN(ST_X(geometry)) as min_x,
                    MAX(ST_X(geometry)) as max_x,
                    MIN(ST_Y(geometry)) as min_y,
                    MAX(ST_Y(geometry)) as max_y,
                    AVG(ST_X(geometry)) as avg_x,
                    AVG(ST_Y(geometry)) as avg_y
                FROM fvm_marker_sample
                """,
            )

            stats = result.fetchone()
            logger.info("🌾 FVM Marker Stats:")
            logger.info(f"   Total fields (sample): {stats[0]:,}")
            logger.info(f"   X coordinates: {stats[1]:.6f} to {stats[2]:.6f} (avg: {stats[5]:.6f})")
            logger.info(f"   Y coordinates: {stats[3]:.6f} to {stats[4]:.6f} (avg: {stats[6]:.6f})")

            # Determine likely coordinate system
            if stats[1] > 0 and stats[2] < 20 and stats[3] > 50 and stats[4] < 60:
                logger.info("   🗺️ Likely coordinate system: Geographic (lon, lat)")
            elif stats[3] > 0 and stats[4] < 20 and stats[1] > 50 and stats[2] < 60:
                logger.info("   🗺️ Likely coordinate system: Geographic (lat, lon) - SWAPPED")
            elif stats[1] > 400000 and stats[2] < 900000:
                logger.info("   🗺️ Likely coordinate system: UTM or similar projected")
            else:
                logger.warning("   ⚠️ Coordinate system unclear from ranges")

            # Sample specific coordinates
            sample_result = await asyncio.to_thread(
                self.conn.execute,
                """
                SELECT
                    field_uuid,
                    ST_X(geometry) as x,
                    ST_Y(geometry) as y,
                    cvr_number,
                    block_id
                FROM fvm_marker_sample
                LIMIT 5
                """,
            )

            logger.info("🔍 Sample field coordinates:")
            for row in sample_result.fetchall():
                logger.info(f"   {row[0]}: ({row[1]:.6f}, {row[2]:.6f}) - CVR: {row[3]}, Block: {row[4]}")

        except Exception as e:
            logger.error(f"❌ Error analyzing FVM marker data: {e}")

    async def _test_spatial_joins(self):
        """Test spatial joins between buildings and fields."""
        logger.info("🔗 Testing spatial joins...")

        try:
            # Test original coordinates
            result_original = await asyncio.to_thread(
                self.conn.execute,
                """
                SELECT COUNT(*)
                FROM (
                    SELECT b.building_uuid
                    FROM bbr_buildings b
                    JOIN fvm_marker_sample f ON ST_Intersects(b.geometry, f.geometry)
                    LIMIT 100
                ) subq
                """,
            )
            original_count = result_original.fetchone()[0]
            logger.info(f"   Original coordinates intersections: {original_count}")

            # Test with buildings coordinates flipped
            result_flipped = await asyncio.to_thread(
                self.conn.execute,
                """
                SELECT COUNT(*)
                FROM (
                    SELECT b.building_uuid
                    FROM bbr_buildings b
                    JOIN fvm_marker_sample f ON ST_Intersects(ST_FlipCoordinates(b.geometry), f.geometry)
                    LIMIT 100
                ) subq
                """,
            )
            flipped_count = result_flipped.fetchone()[0]
            logger.info(f"   Flipped buildings coordinates intersections: {flipped_count}")

            # Test with field coordinates flipped
            result_fields_flipped = await asyncio.to_thread(
                self.conn.execute,
                """
                SELECT COUNT(*)
                FROM (
                    SELECT b.building_uuid
                    FROM bbr_buildings b
                    JOIN fvm_marker_sample f ON ST_Intersects(b.geometry, ST_FlipCoordinates(f.geometry))
                    LIMIT 100
                ) subq
                """,
            )
            fields_flipped_count = result_fields_flipped.fetchone()[0]
            logger.info(f"   Flipped fields coordinates intersections: {fields_flipped_count}")

            # Recommend best approach
            best_count = max(original_count, flipped_count, fields_flipped_count)
            if best_count == original_count:
                logger.info("   ✅ Recommendation: Use original coordinates")
            elif best_count == flipped_count:
                logger.info("   ✅ Recommendation: Flip buildings coordinates")
            elif best_count == fields_flipped_count:
                logger.info("   ✅ Recommendation: Flip fields coordinates")
            else:
                logger.warning("   ⚠️ No intersections found with any coordinate combination")

        except Exception as e:
            logger.error(f"❌ Error testing spatial joins: {e}")

    async def _test_coordinate_flipping(self):
        """Test coordinate flipping effects."""
        logger.info("🔄 Testing coordinate flipping effects...")

        try:
            # Sample building before and after flipping
            result = await asyncio.to_thread(
                self.conn.execute,
                """
                SELECT
                    building_uuid,
                    ST_X(geometry) as orig_x,
                    ST_Y(geometry) as orig_y,
                    ST_X(ST_FlipCoordinates(geometry)) as flipped_x,
                    ST_Y(ST_FlipCoordinates(geometry)) as flipped_y
                FROM bbr_buildings
                WHERE geometry IS NOT NULL
                LIMIT 3
                """,
            )

            logger.info("🔄 Coordinate flipping examples:")
            for row in result.fetchall():
                logger.info(f"   {row[0]}:")
                logger.info(f"     Original: ({row[1]:.6f}, {row[2]:.6f})")
                logger.info(f"     Flipped:  ({row[3]:.6f}, {row[4]:.6f})")

        except Exception as e:
            logger.error(f"❌ Error testing coordinate flipping: {e}")


async def main():
    """Main function."""
    validator = CoordinateSystemValidator()
    await validator.validate_coordinate_systems()


if __name__ == "__main__":
    asyncio.run(main())
