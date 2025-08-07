"""Stage 0C: Wetlands Pre-filtering

Reduce 1.6M wetland polygons to only those that intersect with agricultural fields.
This is the second-largest dataset reduction after properties.

EXPECTED REDUCTION: 1.6M → ~200K wetland polygons (estimated 85% reduction)
PERFORMANCE IMPACT: Dramatically reduces Stage 2 wetlands processing complexity
"""

import time
from typing import Any, Dict

from ..config import CONFIG
from .base import PreFilteringStageBase


class WetlandsPreFilter(PreFilteringStageBase):
    """Pre-filter wetland polygons to only those intersecting with agricultural fields."""

    def __init__(self, config=None):
        if config is None:
            from ..base import FieldAnalysisStageConfig

            config = FieldAnalysisStageConfig()
        super().__init__(config, "Wetlands Pre-filtering")

    def _load_input_data(self):
        """Load agricultural fields and full wetlands dataset."""
        # Load fields for filtering (BUILD side)
        self._load_fields_for_filtering()

        # Load full wetlands dataset
        self.log.info("Loading full wetlands dataset (1.6M polygons)...")
        self._load_silver_dataset(CONFIG.wetlands_dataset, "wetlands_raw")

        # CRITICAL: Decompose wetlands with ST_Dump BEFORE spatial join to prevent memory issues
        self.log.info(
            "Decomposing wetland MultiPolygons with ST_Dump to prevent memory overflow..."
        )
        self.conn.execute("""
            CREATE OR REPLACE TABLE wetlands_full AS
            SELECT 
                toerv_pct,
                UNNEST(ST_Dump(geometry)).geom as geometry
            FROM wetlands_raw
        """)

        # Log dataset sizes
        raw_count = self.conn.execute("SELECT COUNT(*) FROM wetlands_raw").fetchone()[0]
        decomposed_count = self.conn.execute("SELECT COUNT(*) FROM wetlands_full").fetchone()[0]
        self.log.info(
            f"📊 Input: {raw_count:,} wetland MultiPolygons → {decomposed_count:,} individual polygons after ST_Dump"
        )

    async def _execute_stage_processing(self) -> Dict[str, Any]:
        """
        Pre-filter wetlands using chunked processing to manage memory.

        OPTIMIZATION STRATEGY:
        1. Process wetlands in chunks (100K at a time) - smaller than properties due to complexity
        2. Fields (600K) as BUILD side - gets spatial indexed per chunk
        3. Wetlands chunks as PROBE side - processed incrementally
        4. Only keep wetlands that intersect with ANY field
        5. Preserve toerv_pct attribute for downstream analysis
        6. Stream results to avoid memory accumulation
        """

        start_time = time.time()

        # Get total wetlands count for chunking
        total_wetlands = self.conn.execute("SELECT COUNT(*) FROM wetlands_full").fetchone()[0]
        chunk_size = 100000  # Smaller chunks for wetlands processing
        num_chunks = (total_wetlands + chunk_size - 1) // chunk_size

        self.log.info(
            f"🚀 Pre-filtering {total_wetlands:,} wetlands in {num_chunks} chunks of {chunk_size:,}"
        )

        # Initialize filtered wetlands table
        self.conn.execute("""
            CREATE OR REPLACE TABLE wetlands_intersecting AS
            SELECT 
                toerv_pct,
                geometry
            FROM wetlands_full
            WHERE FALSE  -- Empty table with correct schema
        """)

        total_intersecting = 0

        # Process each chunk
        for chunk_num in range(num_chunks):
            offset = chunk_num * chunk_size
            progress_pct = ((chunk_num + 1) / num_chunks) * 100

            chunk_start = time.time()
            self.log.info(
                f"📦 Chunk {chunk_num + 1}/{num_chunks} (offset: {offset:,}) - {progress_pct:.1f}%"
            )

            # Create wetlands chunk
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE wetlands_chunk AS
                SELECT 
                    toerv_pct,
                    geometry
                FROM wetlands_full
                LIMIT {chunk_size} OFFSET {offset}
            """)

            chunk_count = self.conn.execute("SELECT COUNT(*) FROM wetlands_chunk").fetchone()[0]
            if chunk_count == 0:
                break

            # DuckDB Spatial v1.2.2 COMPLIANT: Single spatial predicate (ST_Intersects only)
            self.log.info(f"  Filtering {chunk_count:,} wetlands against {600000:,} fields...")
            self.conn.execute("""
                CREATE OR REPLACE TABLE chunk_filtered AS
                SELECT DISTINCT
                    w.toerv_pct,
                    w.geometry
                FROM fields_for_filtering f
                JOIN wetlands_chunk w ON ST_Intersects(f.geometry, w.geometry)
            """)

            chunk_filtered = self.conn.execute("SELECT COUNT(*) FROM chunk_filtered").fetchone()[0]

            # Append to main filtered table
            if chunk_filtered > 0:
                self.conn.execute("""
                    INSERT INTO wetlands_intersecting 
                    SELECT * FROM chunk_filtered
                """)

            total_intersecting += chunk_filtered
            chunk_time = time.time() - chunk_start

            self.log.info(
                f"  ✅ Chunk {chunk_num + 1}: {chunk_filtered:,}/{chunk_count:,} wetlands kept ({chunk_filtered / chunk_count * 100:.1f}%) - {chunk_time:.1f}s"
            )

            # Add unique IDs with spatial ordering (ST_Dump already done in _load_input_data)
        self.log.info("Adding deterministic keys to filtered wetland polygons (and keeping legacy IDs)...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE wetlands_filtered AS
            SELECT 
                -- Deterministic fragment key based on initial decomposed geometry
                md5(CAST(ST_AsWKB(geometry) AS VARCHAR)) AS wetland_key,
                -- Legacy numeric ID retained for backward compatibility (non-deterministic ordering)
                ROW_NUMBER() OVER (
                    ORDER BY toerv_pct, ST_X(ST_Centroid(geometry)), ST_Y(ST_Centroid(geometry))
                ) AS wetland_id,
                toerv_pct,
                geometry,
                ST_Area_Spheroid(geometry) as wetland_area_m2
            FROM wetlands_intersecting
        """)

        total_filtered = self.conn.execute("SELECT COUNT(*) FROM wetlands_filtered").fetchone()[0]

        # Final statistics
        processing_time = time.time() - start_time
        reduction_pct = (1 - total_intersecting / total_wetlands) * 100

        self.log.info(
            f"🎯 MASSIVE WETLANDS REDUCTION: {total_wetlands:,} → {total_intersecting:,} polygons ({reduction_pct:.1f}% reduction)"
        )
        self.log.info(
            f"📐 After ST_Dump: {total_filtered:,} wetland pieces for downstream processing"
        )
        self.log.info(f"⚡ Stage 2 wetlands complexity reduced by {reduction_pct:.1f}%")

        # Export filtered wetlands using standard pipeline pattern
        output_path = self._get_stage0_output_path("stage0_wetlands_filtered")
        self.gcs_access.export_table_to_gcs_direct("wetlands_filtered", output_path)

        return {
            "input_wetlands": total_wetlands,
            "filtered_wetlands": total_intersecting,
            "decomposed_wetland_pieces": total_filtered,
            "reduction_percentage": reduction_pct,
            "processing_time_seconds": processing_time,
            "output_path": output_path,
            "performance_improvement": f"{reduction_pct:.1f}% reduction in Stage 2 wetlands processing",
        }

    def _save_output_data(self, result: Dict[str, Any]):
        """Save output data - already handled in _execute_stage_processing for Stage 0."""
        # Stage 0 classes handle export directly in _execute_stage_processing
        # to use custom output paths and naming conventions
        self.log.info("✅ Wetlands pre-filtering data already saved to GCS")
