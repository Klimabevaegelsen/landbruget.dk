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
        1. Grukos is typically a single dissolved geometry — ST_Dump it first
           into individual polygons to avoid a 600K×1 spatial join that returns
           the same giant geometry for every matching field (timeout on CI).
        2. Filter the dumped pieces against fields (many small × many small).
        3. Add unique IDs for downstream joins.
        """

        start_time = time.time()

        total_grukos = self.conn.execute("SELECT COUNT(*) FROM grukos_raw").fetchone()[0]
        self.log.info(f"Pre-filtering {total_grukos:,} Grukos geometries against ~600K fields")

        # Step 1: Decompose the dissolved geometry into individual polygons FIRST.
        # This avoids a 600K×1 spatial join that produces 600K duplicate rows.
        self.log.info("Decomposing Grukos with ST_Dump before spatial filtering...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE grukos_dumped AS
            SELECT
                UNNEST(ST_Dump(geometry)).geom as geometry
            FROM grukos_raw
            WHERE geometry IS NOT NULL
        """)

        dumped_count = self.conn.execute("SELECT COUNT(*) FROM grukos_dumped").fetchone()[0]
        self.log.info(
            f"ST_Dump produced {dumped_count:,} individual polygons from {total_grukos:,} input rows"
        )

        # Step 2: Spatial filter — keep only pieces that intersect with any field
        self.log.info("Filtering Grukos pieces against agricultural fields...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE grukos_intersecting AS
            SELECT DISTINCT
                g.geometry
            FROM fields_for_filtering f
            JOIN grukos_dumped g ON ST_Intersects(f.geometry, g.geometry)
        """)

        intersecting_count = self.conn.execute(
            "SELECT COUNT(*) FROM grukos_intersecting"
        ).fetchone()[0]

        # Step 3: Add unique IDs
        self.log.info("Adding unique IDs to filtered Grukos polygons...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE grukos_filtered AS
            SELECT
                ROW_NUMBER() OVER (
                    ORDER BY ST_X(ST_Centroid(geometry)),
                             ST_Y(ST_Centroid(geometry))
                ) as grukos_id,
                geometry,
                ST_Area_Spheroid(geometry) as grukos_area_m2
            FROM grukos_intersecting
            WHERE geometry IS NOT NULL
        """)

        total_filtered = self.conn.execute("SELECT COUNT(*) FROM grukos_filtered").fetchone()[0]

        # Final statistics
        processing_time = time.time() - start_time
        reduction_pct = (1 - intersecting_count / dumped_count) * 100 if dumped_count > 0 else 0

        self.log.info(
            f"GRUKOS REDUCTION: {dumped_count:,} dumped → {intersecting_count:,} intersecting polygons "
            f"({reduction_pct:.1f}% reduction)"
        )
        self.log.info(f"Final: {total_filtered:,} Grukos pieces with IDs for downstream processing")

        # Export filtered Grukos using standard pipeline pattern
        output_path = self._get_stage0_output_path("stage0_grukos_filtered")
        self.storage.export_table_to_storage_direct("grukos_filtered", output_path)

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
        self.log.info("Grukos pre-filtering data already saved to cloud storage")
