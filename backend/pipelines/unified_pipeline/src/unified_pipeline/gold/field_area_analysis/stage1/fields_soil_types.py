"""Stage 1D: Fields × Soil Types Base Intersection - OPTIMIZED

Optimized field analysis with soil type intersections using DuckDB Spatial v1.2.2 SPATIAL_JOIN operator.
Creates foundation dataset for environmental coverage calculations.

MAJOR OPTIMIZATIONS:
1. Uses Stage 0 pre-filtered soil types (~8K instead of 13K polygons)
2. Single spatial predicate (ST_Intersects only) for SPATIAL_JOIN operator
3. No redundant geometry storage or expensive duplicate calculations
4. Foundation data output with soil_id for efficient downstream joins
5. Efficient single-pass processing instead of tiny batches
6. Keep fields as original multipolygons for consistency with other stages

References: https://github.com/duckdb/duckdb-spatial/pull/545
"""

import time
from typing import Any, Dict

from ..base import FieldAnalysisStageBase, FieldAnalysisStageConfig
from ..config import CONFIG


class FieldsSoilTypesIntersection(FieldAnalysisStageBase):
    """Analyze agricultural fields with soil type intersections using SPATIAL_JOIN optimization."""

    def __init__(self, config: FieldAnalysisStageConfig = None):
        if config is None:
            config = FieldAnalysisStageConfig()
        super().__init__(config, "Stage 1D: Fields × Soil Types (OPTIMIZED)")

    def _load_input_data(self):
        """Load agricultural fields and Stage 0 pre-filtered soil types."""
        # Load agricultural fields (600K fields)
        self._load_silver_dataset(CONFIG.get_agricultural_fields_dataset(), "agricultural_fields_raw")

        # Keep agricultural fields as original multipolygons for consistency with other stages
        self.log.info("Preparing agricultural fields (keeping original multipolygons)...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE agricultural_fields AS
            SELECT 
                field_id,
                block_id,
                cvr_number,
                year,
                field_uuid,
                geometry,
                ST_Area_Spheroid(geometry) as field_area_m2
            FROM agricultural_fields_raw
        """)

        # Load Stage 0 pre-filtered soil types (~8K polygons instead of 13K)
        self.log.info("Loading Stage 0 pre-filtered soil types (major performance boost)...")
        stage0_soil_types_dataset = CONFIG.stage_outputs["soil_types_prefiltered"]
        stage0_soil_types_path = self._get_latest_gold_path(stage0_soil_types_dataset)
        self.gcs_access.query_parquet_direct(stage0_soil_types_path, "SELECT *", "soil_types_raw")

        # OPTIMIZATION: Decompose soil types with ST_Dump for optimal spatial indexing
        self.log.info("Decomposing soil types with ST_Dump for optimal spatial indexing...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE soil_types AS
            SELECT 
                soil_id,
                soil_code,
                soil_description,
                soil_type_category,
                UNNEST(ST_Dump(geometry)).geom as geometry,
                ST_Area_Spheroid(UNNEST(ST_Dump(geometry)).geom) as soil_area_m2
            FROM soil_types_raw
        """)

        # Log dataset sizes for performance tracking
        fields_count = self.conn.execute("SELECT COUNT(*) FROM agricultural_fields").fetchone()[0]
        soil_count = self.conn.execute("SELECT COUNT(*) FROM soil_types").fetchone()[0]

        self.log.info(f"✅ Loaded {fields_count:,} agricultural fields (original multipolygons)")
        self.log.info(f"✅ Loaded {soil_count:,} soil type polygons (after ST_Dump)")
        self.log.info(
            f"🚀 Processing {fields_count * soil_count:,} potential combinations (optimized)"
        )

    async def _execute_stage_processing(self) -> Dict[str, Any]:
        """
        Create field-soil intersections using optimized single-pass processing.

        DuckDB Spatial v1.2.2 SPATIAL_JOIN OPTIMIZATIONS:
        1. Single spatial predicate (ST_Intersects only) to trigger SPATIAL_JOIN operator
        2. Soil types (smaller, pre-filtered) as BUILD side for spatial indexing
        3. Agricultural fields as PROBE side
        4. No WHERE clauses with spatial predicates - all filtering post-join
        5. Foundation data with soil_id for efficient downstream joins
        6. Stream processing to avoid memory accumulation
        """

        start_time = time.time()
        self.log.info("🚀 OPTIMIZED SINGLE-PASS PROCESSING with SPATIAL_JOIN operator")
        self.log.info("✅ DuckDB Spatial v1.2.2: Single spatial predicate only")

        # STEP 1: SPATIAL_JOIN - Soil types (BUILD) × Fields (PROBE)
        # This triggers the SPATIAL_JOIN operator for massive performance gains
        spatial_start = time.time()
        self.log.info("STEP 1: Executing SPATIAL_JOIN (soil types BUILD × fields PROBE)...")

        self.conn.execute("""
            CREATE OR REPLACE TABLE field_soil_intersections AS
            SELECT 
                f.field_id,
                f.block_id,
                f.cvr_number,
                f.year,
                f.field_uuid,
                f.field_area_m2,
                s.soil_id,
                s.soil_code,
                s.soil_description,
                s.soil_type_category,
                -- Calculate intersection geometry and area in single operation
                ST_Intersection(f.geometry, s.geometry) as intersection_geometry,
                ST_Area_Spheroid(ST_Intersection(f.geometry, s.geometry)) as soil_intersection_area_m2
            FROM soil_types s  -- BUILD side (smaller, pre-filtered dataset - gets spatial indexed)
            JOIN agricultural_fields f ON ST_Intersects(s.geometry, f.geometry)  -- PROBE side (larger dataset)
        """)

        spatial_time = time.time() - spatial_start
        raw_intersections = self.conn.execute(
            "SELECT COUNT(*) FROM field_soil_intersections"
        ).fetchone()[0]

        self.log.info(
            f"✅ SPATIAL_JOIN completed in {spatial_time:.1f}s: {raw_intersections:,} raw intersections"
        )

        # STEP 2: Post-join filtering (NO SPATIAL WHERE CLAUSES)
        # DuckDB Spatial PR #545 compliance: Move all filtering to post-join processing
        filter_start = time.time()
        self.log.info("STEP 2: Post-join area filtering (no spatial WHERE clauses)...")

        self.conn.execute("""
            CREATE OR REPLACE TABLE field_soil_meaningful AS
            SELECT 
                field_id,
                block_id,
                cvr_number,
                year,
                field_uuid,
                field_area_m2,
                soil_id,
                soil_code,
                soil_description,
                soil_type_category,
                intersection_geometry,
                soil_intersection_area_m2,
                (soil_intersection_area_m2 / field_area_m2) * 100 as soil_area_share_pct
            FROM field_soil_intersections
            WHERE 
                -- Area filtering to remove noise (post-join, non-spatial)
                soil_intersection_area_m2 > 100  -- Minimum 100m² intersection
                AND (soil_intersection_area_m2 / field_area_m2) > 0.01  -- Minimum 1% coverage
        """)

        filter_time = time.time() - filter_start
        meaningful_intersections = self.conn.execute(
            "SELECT COUNT(*) FROM field_soil_meaningful"
        ).fetchone()[0]
        filtered_out = raw_intersections - meaningful_intersections

        self.log.info(f"✅ Post-join filtering completed in {filter_time:.1f}s")
        self.log.info(f"   Meaningful intersections: {meaningful_intersections:,}")
        self.log.info(
            f"   Noise filtered: {filtered_out:,} ({filtered_out / raw_intersections * 100:.1f}%)"
        )

        # STEP 3: Create foundation data for downstream stages
        foundation_start = time.time()
        self.log.info(
            "STEP 3: Creating foundation data with soil_id for efficient downstream joins..."
        )

        self.conn.execute("""
            CREATE OR REPLACE TABLE field_soil_foundation AS
            SELECT 
                field_id,
                block_id,
                cvr_number,
                year,
                field_uuid,
                soil_id,  -- Foundation data: enables ID-based joins in later stages
                soil_code,
                soil_description,
                soil_type_category,
                soil_intersection_area_m2,
                soil_area_share_pct,
                field_area_m2,
                intersection_geometry  -- Preserve for property-level analysis
            FROM field_soil_meaningful
            ORDER BY field_uuid, year, soil_intersection_area_m2 DESC
        """)

        # STEP 4: Create simplified areas table (backward compatibility)
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_soil_areas AS
            SELECT 
                field_id,
                block_id,
                cvr_number,
                year,
                field_uuid,
                soil_code,
                soil_description,
                soil_type_category,
                soil_intersection_area_m2 as soil_area_m2,
                soil_area_share_pct,
                field_area_m2
            FROM field_soil_foundation
        """)

        foundation_time = time.time() - foundation_start
        foundation_count = self.conn.execute(
            "SELECT COUNT(*) FROM field_soil_foundation"
        ).fetchone()[0]

        self.log.info(
            f"✅ Foundation data created in {foundation_time:.1f}s: {foundation_count:,} records"
        )

        # STEP 5: Generate comprehensive statistics
        stats_start = time.time()
        self.log.info("STEP 5: Generating performance and coverage statistics...")

        # Get soil type statistics
        soil_stats = self.conn.execute("""
            SELECT 
                soil_type_category,
                COUNT(*) as intersection_count,
                COUNT(DISTINCT field_uuid) as field_count,
                SUM(soil_intersection_area_m2) / 1000000 as total_area_km2,
                AVG(soil_area_share_pct) as avg_coverage_pct
            FROM field_soil_foundation
            GROUP BY soil_type_category
            ORDER BY field_count DESC
        """).fetchall()

        # Get coverage statistics
        coverage_stats = self.conn.execute("""
            SELECT 
                COUNT(*) as total_intersections,
                COUNT(DISTINCT field_uuid) as fields_with_soil,
                AVG(soil_area_share_pct) as avg_soil_coverage,
                COUNT(DISTINCT soil_id) as unique_soil_polygons,
                COUNT(DISTINCT soil_code) as unique_soil_codes
            FROM field_soil_foundation
        """).fetchone()

        (
            total_intersections,
            fields_with_soil,
            avg_coverage,
            unique_soil_polygons,
            unique_soil_codes,
        ) = coverage_stats

        stats_time = time.time() - stats_start
        total_time = time.time() - start_time

        self.log.info(f"✅ Statistics generated in {stats_time:.1f}s")
        self.log.info("=" * 60)
        self.log.info("🎯 OPTIMIZED SOIL TYPES PROCESSING COMPLETED")
        self.log.info(f"   Total processing time: {total_time:.1f}s")
        self.log.info(
            f"   SPATIAL_JOIN time: {spatial_time:.1f}s ({spatial_time / total_time * 100:.1f}%)"
        )
        self.log.info(f"   Post-processing time: {total_time - spatial_time:.1f}s")
        self.log.info("=" * 60)
        self.log.info("📊 COVERAGE STATISTICS:")
        self.log.info(f"   Total field-soil intersections: {total_intersections:,}")
        self.log.info(f"   Fields with soil data: {fields_with_soil:,}")
        self.log.info(f"   Average soil coverage: {avg_coverage:.1f}%")
        self.log.info(f"   Unique soil polygons: {unique_soil_polygons:,}")
        self.log.info(f"   Unique soil codes: {unique_soil_codes:,}")
        self.log.info("=" * 60)
        self.log.info("🏆 TOP SOIL CATEGORIES by field count:")
        for soil_category, intersection_count, field_count, area_km2, avg_pct in soil_stats[:5]:
            self.log.info(
                f"   {soil_category}: {field_count:,} fields, {intersection_count:,} intersections, {area_km2:.1f} km², {avg_pct:.1f}% avg coverage"
            )

        # Clean up intermediate tables to save memory
        self.conn.execute("DROP TABLE IF EXISTS field_soil_intersections")
        self.conn.execute("DROP TABLE IF EXISTS field_soil_meaningful")

        return {
            "total_intersections": total_intersections,
            "fields_with_soil": fields_with_soil,
            "avg_soil_coverage": avg_coverage,
            "unique_soil_polygons": unique_soil_polygons,
            "unique_soil_codes": unique_soil_codes,
            "soil_category_stats": soil_stats,
            "processing_time": total_time,
            "spatial_join_time": spatial_time,
            "raw_intersections": raw_intersections,
            "meaningful_intersections": meaningful_intersections,
            "noise_filtered": filtered_out,
        }

    def _save_output_data(self, result: Dict[str, Any]):
        """Save simplified areas to GCS for Stage 4 compatibility."""
        # Save simplified areas - this is what Stage 4 expects
        self.log.info("Saving field soil areas for Stage 4 compatibility...")
        self._save_stage_output("field_soil_areas", "field_soil_intersections")
