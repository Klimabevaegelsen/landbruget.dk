"""Stage 0F: Grukos Pre-filtering

Reduce Grukos dissolved geometries to only those that intersect with agricultural fields.
Grukos contains groundwater action areas (indsatsområder) for nitrate-sensitive (NFI) and
pesticide-sensitive (SFI) groundwater intake areas.

EXPECTED REDUCTION: 1 dissolved polygon → filtered intersection geometry
PERFORMANCE IMPACT: Reduces Stage 2 Grukos processing complexity significantly
"""

import time
from typing import Any

from ..config import CONFIG
from .base import PreFilteringStageBase


class GrukosPreFilter(PreFilteringStageBase):
    """Pre-filter Grukos dissolved geometry to only intersecting portions with agricultural fields."""

    def __init__(self, config=None):
        if config is None:
            from ..base import FieldAnalysisStageConfig

            config = FieldAnalysisStageConfig()
        super().__init__(config, "Grukos Pre-filtering")

    def _load_input_data(self):
        """Load agricultural fields and full Grukos dissolved dataset."""
        # Load fields for filtering (BUILD side)
        self._load_fields_for_filtering()

        # Load full Grukos dissolved dataset
        self.log.info("Loading full Grukos dissolved dataset...")
        self._load_silver_dataset(CONFIG.grukos_dataset, "grukos_raw")

        # Log dataset sizes
        raw_count = self.conn.execute("SELECT COUNT(*) FROM grukos_raw").fetchone()[0]
        self.log.info(f"Input: {raw_count:,} Grukos dissolved geometries loaded")

        # Check geometry validity
        self.log.info("Validating Grukos geometry...")
        try:
            coord_validation = self.conn.execute("""
                SELECT
                    MIN(ST_XMin(geometry)) as min_x,
                    MAX(ST_XMax(geometry)) as max_x,
                    MIN(ST_YMin(geometry)) as min_y,
                    MAX(ST_YMax(geometry)) as max_y
                FROM grukos_raw
                WHERE geometry IS NOT NULL
            """).fetchone()

            if coord_validation:
                min_x, max_x, min_y, max_y = coord_validation
                self.log.info(
                    f"Grukos bounds: X({min_x:.2f}, {max_x:.2f}), Y({min_y:.2f}, {max_y:.2f})"
                )

                # Check if coordinates are in expected ranges for Denmark (WGS84)
                if min_x >= 7 and max_x <= 16 and min_y >= 54 and max_y <= 58:
                    self.log.info("Grukos coordinates in WGS84 (EPSG:4326) - Denmark bounds OK")
                else:
                    self.log.warning("Grukos coordinates outside expected Denmark WGS84 bounds!")

        except Exception as e:
            self.log.warning(f"Could not validate Grukos coordinate bounds: {e}")

    async def _execute_stage_processing(self) -> dict[str, Any]:
        """
        Pre-filter Grukos geometries using spatial intersection with fields.

        OPTIMIZATION STRATEGY:
        1. Grukos dataset is a single dissolved geometry covering all indsatsområder
        2. Fields (600K) as BUILD side - gets spatial indexed
        3. Grukos as PROBE side
        4. Only keep portions of Grukos that intersect with ANY field
        5. Use ST_Dump to decompose any resulting MultiPolygons
        """

        start_time = time.time()

        total_grukos = self.conn.execute("SELECT COUNT(*) FROM grukos_raw").fetchone()[0]
        self.log.info(f"Pre-filtering {total_grukos:,} Grukos geometries against ~600K fields")

        # DuckDB Spatial v1.2.2 COMPLIANT: Single spatial predicate (ST_Intersects only)
        self.log.info(
            "Filtering Grukos geometry to portions intersecting with agricultural fields..."
        )

        # Create intersection geometries - keeping only the parts that overlap with fields
        self.conn.execute("""
            CREATE OR REPLACE TABLE grukos_intersecting AS
            SELECT DISTINCT
                g.geometry
            FROM fields_for_filtering f
            JOIN grukos_raw g ON ST_Intersects(f.geometry, g.geometry)
        """)

        intersecting_count = self.conn.execute(
            "SELECT COUNT(*) FROM grukos_intersecting"
        ).fetchone()[0]

        # Decompose with ST_Dump and add unique IDs
        self.log.info("Decomposing with ST_Dump and adding unique IDs...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE grukos_filtered AS
            SELECT
                ROW_NUMBER() OVER (
                    ORDER BY ST_X(ST_Centroid(geom)),
                             ST_Y(ST_Centroid(geom))
                ) as grukos_id,
                geom as geometry,
                ST_Area_Spheroid(geom) as grukos_area_m2
            FROM (
                SELECT UNNEST(ST_Dump(geometry)).geom as geom
                FROM grukos_intersecting
                WHERE geometry IS NOT NULL
            )
        """)

        total_filtered = self.conn.execute("SELECT COUNT(*) FROM grukos_filtered").fetchone()[0]

        # Final statistics
        processing_time = time.time() - start_time
        reduction_pct = (1 - total_filtered / total_grukos) * 100 if total_grukos > 0 else 0

        self.log.info(
            f"GRUKOS REDUCTION: {total_grukos:,} → {intersecting_count:,} intersecting polygons "
            f"({reduction_pct:.1f}% reduction)"
        )
        self.log.info(f"After ST_Dump: {total_filtered:,} Grukos pieces for downstream processing")

        # Export filtered Grukos using standard pipeline pattern
        output_path = self._get_stage0_output_path("stage0_grukos_filtered")
        self.gcs_access.export_table_to_gcs_direct("grukos_filtered", output_path)

        return {
            "input_grukos_geometries": total_grukos,
            "filtered_grukos_geometries": intersecting_count,
            "decomposed_grukos_pieces": total_filtered,
            "reduction_percentage": reduction_pct,
            "processing_time_seconds": processing_time,
            "output_path": output_path,
            "performance_improvement": f"{reduction_pct:.1f}% reduction in Stage 2 Grukos processing",
        }

    def _save_output_data(self, result: dict[str, Any]):
        """Save output data - already handled in _execute_stage_processing for Stage 0."""
        # Stage 0 classes handle export directly in _execute_stage_processing
        # to use custom output paths and naming conventions
        self.log.info("Grukos pre-filtering data already saved to GCS")
