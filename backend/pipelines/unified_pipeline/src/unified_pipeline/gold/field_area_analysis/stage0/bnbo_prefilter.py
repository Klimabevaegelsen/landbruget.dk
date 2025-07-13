"""Stage 0B: BNBO Pre-filtering

Reduce BNBO polygons to only those that intersect with agricultural fields.
Smaller dataset but still important for memory optimization.

EXPECTED REDUCTION: 3.7K → ~1K BNBO polygons (estimated 70% reduction)
PERFORMANCE IMPACT: Reduces Stage 2 BNBO processing complexity significantly
"""

import time
from typing import Any, Dict

from ..config import CONFIG
from .base import PreFilteringStageBase


class BNBOPreFilter(PreFilteringStageBase):
    """Pre-filter BNBO polygons to only those intersecting with agricultural fields."""

    def __init__(self, config=None):
        if config is None:
            from ..base import FieldAnalysisStageConfig

            config = FieldAnalysisStageConfig()
        super().__init__(config, "BNBO Pre-filtering")

    def _load_input_data(self):
        """Load agricultural fields and full BNBO dataset."""
        # Load fields for filtering (BUILD side)
        self._load_fields_for_filtering()

        # Load full BNBO dataset
        self.log.info("Loading full BNBO status dataset...")
        self._load_silver_dataset(CONFIG.bnbo_status_dataset, "bnbo_status_full")

        # Log dataset sizes
        bnbo_count = self.conn.execute("SELECT COUNT(*) FROM bnbo_status_full").fetchone()[0]
        self.log.info(f"📊 Input: {bnbo_count:,} BNBO polygons to filter")

    async def _execute_stage_processing(self) -> Dict[str, Any]:
        """
        Pre-filter BNBO polygons using spatial intersection with fields.

        OPTIMIZATION STRATEGY:
        1. BNBO dataset is smaller (3.7K), so we can process in single operation
        2. Fields (600K) as BUILD side - gets spatial indexed
        3. BNBO as PROBE side
        4. Only keep BNBO polygons that intersect with ANY field
        5. Decompose with ST_Dump for optimal downstream processing
        """

        start_time = time.time()

        total_bnbo = self.conn.execute("SELECT COUNT(*) FROM bnbo_status_full").fetchone()[0]
        self.log.info(f"🚀 Pre-filtering {total_bnbo:,} BNBO polygons against {600000:,} fields")

        # DuckDB Spatial v1.2.2 COMPLIANT: Single spatial predicate (ST_Intersects only)
        self.log.info("Filtering BNBO polygons that intersect with agricultural fields...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE bnbo_intersecting AS
            SELECT DISTINCT
                b.status_category,
                b.geometry
            FROM fields_for_filtering f
            JOIN bnbo_status_full b ON ST_Intersects(f.geometry, b.geometry)
        """)

        intersecting_count = self.conn.execute("SELECT COUNT(*) FROM bnbo_intersecting").fetchone()[
            0
        ]

        # Decompose with ST_Dump and add unique IDs for downstream processing
        self.log.info("Decomposing filtered BNBO with ST_Dump and adding unique IDs...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE bnbo_filtered AS
            SELECT 
                ROW_NUMBER() OVER (ORDER BY status_category, ST_X(ST_Centroid(UNNEST(ST_Dump(geometry)).geom)), ST_Y(ST_Centroid(UNNEST(ST_Dump(geometry)).geom))) as bnbo_id,
                status_category,
                UNNEST(ST_Dump(geometry)).geom as geometry,
                ST_Area_Spheroid(UNNEST(ST_Dump(geometry)).geom) as bnbo_area_m2
            FROM bnbo_intersecting
        """)

        total_filtered = self.conn.execute("SELECT COUNT(*) FROM bnbo_filtered").fetchone()[0]

        # Final statistics
        processing_time = time.time() - start_time
        reduction_pct = (1 - intersecting_count / total_bnbo) * 100

        self.log.info(
            f"🎯 BNBO REDUCTION: {total_bnbo:,} → {intersecting_count:,} polygons ({reduction_pct:.1f}% reduction)"
        )
        self.log.info(f"📐 After ST_Dump: {total_filtered:,} BNBO pieces for downstream processing")

        # Export filtered BNBO using standard pipeline pattern
        output_path = self._get_stage0_output_path("stage0_bnbo_filtered")
        self.gcs_access.export_table_to_gcs_direct("bnbo_filtered", output_path)

        return {
            "input_bnbo_polygons": total_bnbo,
            "filtered_bnbo_polygons": intersecting_count,
            "decomposed_bnbo_pieces": total_filtered,
            "reduction_percentage": reduction_pct,
            "processing_time_seconds": processing_time,
            "output_path": output_path,
            "performance_improvement": f"{reduction_pct:.1f}% reduction in Stage 2 BNBO processing",
        }

    def _save_output_data(self, result: Dict[str, Any]):
        """Save output data - already handled in _execute_stage_processing for Stage 0."""
        # Stage 0 classes handle export directly in _execute_stage_processing
        # to use custom output paths and naming conventions
        self.log.info("✅ BNBO pre-filtering data already saved to GCS")
