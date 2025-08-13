"""Stage 0E: Soil Types Pre-filtering

Reduce soil types polygons to only those that intersect with agricultural fields.
Moderate dataset but important for memory optimization and SPATIAL_JOIN performance.

EXPECTED REDUCTION: 13K → ~8K soil type polygons (estimated 40% reduction)
PERFORMANCE IMPACT: Reduces Stage 1D soil types processing complexity significantly
"""

import time
from typing import Any, Dict

from ..config import CONFIG
from .base import PreFilteringStageBase


class SoilTypesPreFilter(PreFilteringStageBase):
    """Pre-filter soil types polygons to only those intersecting with agricultural fields."""

    def __init__(self, config=None):
        if config is None:
            from ..base import FieldAnalysisStageConfig

            config = FieldAnalysisStageConfig()
        super().__init__(config, "Soil Types Pre-filtering")

    def _load_input_data(self):
        """Load agricultural fields and full soil types dataset."""
        # Load fields for filtering (BUILD side)
        self._load_fields_for_filtering()

        # Load full soil types dataset
        self.log.info("Loading full soil types dataset...")
        self._load_silver_dataset(CONFIG.soil_types_dataset, "soil_types_raw")

        # CRITICAL: Decompose soil types with ST_Dump BEFORE spatial join to prevent memory issues
        self.log.info(
            "Decomposing soil types MultiPolygons with ST_Dump to prevent memory overflow..."
        )
        self.conn.execute("""
            CREATE OR REPLACE TABLE soil_types_full AS
            SELECT 
                soil_code,
                soil_description,  -- Only meaningful soil data (8 Danish soil types)
                UNNEST(ST_Dump(geometry)).geom as geometry
            FROM soil_types_raw
        """)

        # Log dataset sizes
        raw_count = self.conn.execute("SELECT COUNT(*) FROM soil_types_raw").fetchone()[0]
        decomposed_count = self.conn.execute("SELECT COUNT(*) FROM soil_types_full").fetchone()[0]
        self.log.info(
            f"📊 Input: {raw_count:,} soil types MultiPolygons → "
            f"{decomposed_count:,} individual polygons after ST_Dump"
        )

    async def _execute_stage_processing(self) -> Dict[str, Any]:
        """
        Pre-filter soil types polygons using spatial intersection with fields.

        OPTIMIZATION STRATEGY:
        1. Soil types dataset is moderate (13K), so we can process in single operation
        2. Fields (600K) as BUILD side - gets spatial indexed
        3. Soil types as PROBE side
        4. Only keep soil types polygons that intersect with ANY field
        5. Decompose with ST_Dump for optimal downstream processing
        """

        start_time = time.time()

        total_soil_types = self.conn.execute("SELECT COUNT(*) FROM soil_types_full").fetchone()[0]
        self.log.info(
            f"🚀 Pre-filtering {total_soil_types:,} soil types polygons against {600000:,} fields"
        )

        # DuckDB Spatial v1.2.2 COMPLIANT: Single spatial predicate (ST_Intersects only)
        self.log.info("Filtering soil types polygons that intersect with agricultural fields...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE soil_types_intersecting AS
            SELECT DISTINCT
                s.soil_code,
                s.soil_description,  -- Only meaningful Danish soil types
                s.geometry
            FROM fields_for_filtering f
            JOIN soil_types_full s ON ST_Intersects(f.geometry, s.geometry)
        """)

        self.conn.execute("SELECT COUNT(*) FROM soil_types_intersecting").fetchone()[0]

        # Add unique IDs with spatial ordering (ST_Dump already done in _load_input_data)
        self.log.info("Adding unique IDs to filtered soil types polygons...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE soil_types_filtered AS
            SELECT 
                ROW_NUMBER() OVER (ORDER BY soil_description, soil_code, ST_X(ST_Centroid(geometry)), ST_Y(ST_Centroid(geometry))) as soil_id,
                soil_code,
                soil_description,  -- Only meaningful Danish soil types
                geometry,
                ST_Area_Spheroid(geometry) as soil_area_m2
            FROM soil_types_intersecting
        """)

        total_filtered = self.conn.execute("SELECT COUNT(*) FROM soil_types_filtered").fetchone()[0]
        reduction_pct = ((total_soil_types - total_filtered) / total_soil_types) * 100
        processing_time = time.time() - start_time

        self.log.info("✅ Soil types pre-filtering completed:")
        self.log.info(f"   Original: {total_soil_types:,} soil type polygons")
        self.log.info(f"   Filtered: {total_filtered:,} soil type polygons")
        self.log.info(
            f"   Reduction: {reduction_pct:.1f}% ({total_soil_types - total_filtered:,} polygons removed)"
        )
        self.log.info(f"   Processing time: {processing_time:.1f} seconds")
        self.log.info(f"   🚀 Stage 1D will be {total_soil_types / total_filtered:.1f}x faster")

        # Get Danish soil type breakdown
        soil_type_stats = self.conn.execute("""
            SELECT 
                soil_description,
                COUNT(*) as polygon_count,
                SUM(soil_area_m2) / 1000000 as total_area_km2
            FROM soil_types_filtered
            GROUP BY soil_description
            ORDER BY polygon_count DESC
        """).fetchall()

        self.log.info("   🇩🇰 Filtered Danish soil types:")
        for soil_type, count, area_km2 in soil_type_stats[:8]:
            self.log.info(f"     {soil_type}: {count:,} polygons, {area_km2:.1f} km²")

        # Export filtered soil types using standard pipeline pattern (like other Stage 0 jobs)
        output_path = self._get_stage0_output_path("stage0_soil_types_filtered")
        self.gcs_access.export_table_to_gcs_direct("soil_types_filtered", output_path)

        return {
            "original_count": total_soil_types,
            "filtered_count": total_filtered,
            "reduction_percentage": reduction_pct,
            "processing_time": processing_time,
            "output_path": output_path,
            "soil_type_breakdown": soil_type_stats,
            "performance_improvement": f"{reduction_pct:.1f}% reduction in Stage 1D soil types processing",
        }

    def _save_output_data(self, result: Dict[str, Any]):
        """Save output data - already handled in _execute_stage_processing for Stage 0."""
        # Stage 0 classes handle export directly in _execute_stage_processing
        # to use custom output paths and naming conventions
        self.log.info("✅ Soil types pre-filtering data already saved to GCS")
