"""Stage 0A: Properties Pre-filtering

Reduce 6.5M properties to only those that intersect with agricultural fields.
This is the most critical optimization as it reduces the largest dataset by ~90%.

EXPECTED REDUCTION: 6.5M → ~500K properties (90%+ reduction)
PERFORMANCE IMPACT: Reduces Stage 1 complexity from 3.9B to 300M combinations (13x improvement)
"""

import time
from typing import Any, Dict

from ..config import CONFIG
from .base import PreFilteringStageBase


class PropertiesPreFilter(PreFilteringStageBase):
    """Pre-filter properties to only those intersecting with agricultural fields."""

    def __init__(self, config=None):
        if config is None:
            from ..base import FieldAnalysisStageConfig

            config = FieldAnalysisStageConfig()
        super().__init__(config, "Properties Pre-filtering")

    def _load_input_data(self):
        """Load agricultural fields and full properties dataset."""
        # Load fields for filtering (BUILD side)
        self._load_fields_for_filtering()

        # Load full properties dataset (PROBE side - the massive dataset)
        self.log.info("Loading full properties dataset (6.5M properties)...")
        self._load_silver_dataset(CONFIG.properties_dataset, "properties_full")

        # Log dataset sizes
        properties_count = self.conn.execute("SELECT COUNT(*) FROM properties_full").fetchone()[0]
        self.log.info(f"📊 Input: {properties_count:,} properties to filter")

    async def _execute_stage_processing(self) -> Dict[str, Any]:
        """
        Pre-filter properties using chunked processing to manage memory.

        OPTIMIZATION STRATEGY:
        1. Process properties in chunks (500K at a time)
        2. Fields (600K) as BUILD side - gets spatial indexed per chunk
        3. Properties chunks as PROBE side - processed incrementally
        4. Only keep properties that intersect with ANY field
        5. Stream results to avoid memory accumulation
        """

        start_time = time.time()

        # Get total property count for chunking
        total_properties = self.conn.execute("SELECT COUNT(*) FROM properties_full").fetchone()[0]
        chunk_size = 500000  # Same as original Stage 1C optimization
        num_chunks = (total_properties + chunk_size - 1) // chunk_size

        self.log.info(
            f"🚀 Pre-filtering {total_properties:,} properties in {num_chunks} chunks of {chunk_size:,}"
        )

        # Initialize filtered properties table
        self.conn.execute("""
            CREATE OR REPLACE TABLE properties_filtered AS
            SELECT 
                bestemtFastEjendomBFENr,
                geometry,
                ST_Area_Spheroid(geometry) as property_area_m2
            FROM properties_full
            WHERE FALSE  -- Empty table with correct schema
        """)

        total_filtered = 0

        # Process each chunk
        for chunk_num in range(num_chunks):
            offset = chunk_num * chunk_size
            progress_pct = ((chunk_num + 1) / num_chunks) * 100

            chunk_start = time.time()
            self.log.info(
                f"📦 Chunk {chunk_num + 1}/{num_chunks} (offset: {offset:,}) - {progress_pct:.1f}%"
            )

            # Create property chunk
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE properties_chunk AS
                SELECT 
                    bestemtFastEjendomBFENr,
                    geometry,
                    ST_Area_Spheroid(geometry) as property_area_m2
                FROM properties_full
                LIMIT {chunk_size} OFFSET {offset}
            """)

            chunk_count = self.conn.execute("SELECT COUNT(*) FROM properties_chunk").fetchone()[0]
            if chunk_count == 0:
                break

            # DuckDB Spatial v1.2.2 COMPLIANT: Single spatial predicate (ST_Intersects only)
            self.log.info(f"  Filtering {chunk_count:,} properties against {600000:,} fields...")
            self.conn.execute("""
                CREATE OR REPLACE TABLE chunk_filtered AS
                SELECT DISTINCT
                    p.bestemtFastEjendomBFENr,
                    p.geometry,
                    p.property_area_m2
                FROM fields_for_filtering f
                JOIN properties_chunk p ON ST_Intersects(f.geometry, p.geometry)
            """)

            chunk_filtered = self.conn.execute("SELECT COUNT(*) FROM chunk_filtered").fetchone()[0]

            # Append to main filtered table
            if chunk_filtered > 0:
                self.conn.execute("""
                    INSERT INTO properties_filtered 
                    SELECT * FROM chunk_filtered
                """)

            total_filtered += chunk_filtered
            chunk_time = time.time() - chunk_start

            self.log.info(
                f"  ✅ Chunk {chunk_num + 1}: {chunk_filtered:,}/{chunk_count:,} properties kept ({chunk_filtered / chunk_count * 100:.1f}%) - {chunk_time:.1f}s"
            )

        # Final statistics
        processing_time = time.time() - start_time
        reduction_pct = (1 - total_filtered / total_properties) * 100

        self.log.info(
            f"🎯 MASSIVE REDUCTION: {total_properties:,} → {total_filtered:,} properties ({reduction_pct:.1f}% reduction)"
        )
        self.log.info(
            f"⚡ Stage 1 complexity reduced from {total_properties * 600000 / 1e9:.1f}B to {total_filtered * 600000 / 1e9:.1f}B combinations"
        )

        # Export filtered properties using standard pipeline pattern
        output_path = self._get_stage0_output_path("stage0_properties_filtered")
        self.gcs_access.export_table_to_gcs_direct("properties_filtered", output_path)

        return {
            "input_properties": total_properties,
            "filtered_properties": total_filtered,
            "reduction_percentage": reduction_pct,
            "processing_time_seconds": processing_time,
            "output_path": output_path,
            "performance_improvement": f"{total_properties / total_filtered:.1f}x reduction in Stage 1 complexity",
        }
