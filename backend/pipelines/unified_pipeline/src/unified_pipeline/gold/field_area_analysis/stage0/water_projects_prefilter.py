"""Stage 0D: Water Projects Pre-filtering

Reduce water projects to only those that intersect with agricultural field areas.
Smallest dataset but important for consistency and memory optimization.

EXPECTED REDUCTION: 2.4K → ~500 water projects (estimated 80% reduction)
PERFORMANCE IMPACT: Reduces Stage 1 water project intersection complexity
"""

import time
from typing import Any, Dict

from ..config import CONFIG
from .base import PreFilteringStageBase


class WaterProjectsPreFilter(PreFilteringStageBase):
    """Pre-filter water projects to only those intersecting with agricultural field areas."""

    def __init__(self, config=None):
        if config is None:
            from ..base import FieldAnalysisStageConfig

            config = FieldAnalysisStageConfig()
        super().__init__(config, "Water Projects Pre-filtering")

    def _load_input_data(self):
        """Load agricultural fields and full water projects dataset."""
        # Load fields for filtering (BUILD side)
        self._load_fields_for_filtering()

        # Load full water projects dataset
        self.log.info("Loading full water projects dataset...")
        self._load_silver_dataset(CONFIG.water_projects_dataset, "water_projects_full")

        # Log dataset sizes
        projects_count = self.conn.execute("SELECT COUNT(*) FROM water_projects_full").fetchone()[0]
        self.log.info(f"📊 Input: {projects_count:,} water projects to filter")

    async def _execute_stage_processing(self) -> Dict[str, Any]:
        """
        Pre-filter water projects using spatial intersection with field areas.

        OPTIMIZATION STRATEGY:
        1. Water projects dataset is small (2.4K), so we can process in single operation
        2. Fields (600K) as BUILD side - gets spatial indexed
        3. Water projects as PROBE side
        4. Only keep water projects that intersect with ANY field area
        5. This ensures we only analyze water projects relevant to agricultural areas
        6. Decompose with ST_Dump for optimal downstream processing
        """

        start_time = time.time()

        total_projects = self.conn.execute("SELECT COUNT(*) FROM water_projects_full").fetchone()[0]
        self.log.info(
            f"🚀 Pre-filtering {total_projects:,} water projects against {600000:,} fields"
        )

        # DuckDB Spatial v1.2.2 COMPLIANT: Single spatial predicate (ST_Intersects only)
        self.log.info("Filtering water projects that intersect with agricultural field areas...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE water_projects_intersecting AS
            SELECT DISTINCT
                wp.project_id,
                wp.geometry
            FROM fields_for_filtering f
            JOIN water_projects_full wp ON ST_Intersects(f.geometry, wp.geometry)
        """)

        intersecting_count = self.conn.execute(
            "SELECT COUNT(*) FROM water_projects_intersecting"
        ).fetchone()[0]

        # Decompose with ST_Dump for optimal downstream processing
        self.log.info("Decomposing filtered water projects with ST_Dump...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE water_projects_filtered AS
            SELECT 
                project_id,
                UNNEST(ST_Dump(geometry)).geom as geometry,
                ST_Area_Spheroid(UNNEST(ST_Dump(geometry)).geom) as project_area_m2
            FROM water_projects_intersecting
        """)

        total_filtered = self.conn.execute(
            "SELECT COUNT(*) FROM water_projects_filtered"
        ).fetchone()[0]

        # Final statistics
        processing_time = time.time() - start_time
        reduction_pct = (1 - intersecting_count / total_projects) * 100

        self.log.info(
            f"🎯 WATER PROJECTS REDUCTION: {total_projects:,} → {intersecting_count:,} projects ({reduction_pct:.1f}% reduction)"
        )
        self.log.info(
            f"📐 After ST_Dump: {total_filtered:,} water project pieces for downstream processing"
        )
        self.log.info("✅ Only analyzing water projects relevant to agricultural areas")

        # Export filtered water projects using standard pipeline pattern
        output_path = self._get_stage0_output_path("stage0_water_projects_filtered")
        self.gcs_access.export_table_to_gcs_direct("water_projects_filtered", output_path)

        return {
            "input_water_projects": total_projects,
            "filtered_water_projects": intersecting_count,
            "decomposed_project_pieces": total_filtered,
            "reduction_percentage": reduction_pct,
            "processing_time_seconds": processing_time,
            "output_path": output_path,
            "performance_improvement": f"{reduction_pct:.1f}% reduction in Stage 1 water project processing",
        }
