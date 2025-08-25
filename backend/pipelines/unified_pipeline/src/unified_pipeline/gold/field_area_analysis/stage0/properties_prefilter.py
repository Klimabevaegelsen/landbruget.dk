"""Stage 0A: Properties Pre-filtering

Reduce 6.5M properties to only those that intersect with agricultural fields.
This is the most critical optimization as it reduces the largest dataset by ~90%.

EXPECTED REDUCTION: 6.5M → ~500K properties (90%+ reduction)
PERFORMANCE IMPACT: Reduces Stage 1 complexity from 3.9B to 300M combinations (13x improvement)
"""

import time
from typing import Any, Dict

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

        # Load full cadastral dataset (PROBE side - the massive dataset)
        self.log.info("Loading full cadastral dataset (6.5M properties)...")
        self._load_silver_dataset("cadastral", "properties_raw")

        # Add memory optimization and basic geometry validation
        self.log.info("Preparing properties dataset with memory optimization...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE properties_full AS
            SELECT
                bfe_number,
                geometry
            FROM properties_raw
            WHERE geometry IS NOT NULL
              AND bfe_number IS NOT NULL
        """)

        # Log dataset sizes and validate data integrity
        properties_count = self.conn.execute("SELECT COUNT(*) FROM properties_full").fetchone()[0]

        # 🔍 COORDINATE ORDER VERIFICATION: Extract raw coordinate pairs to verify actual order
        try:
            self.log.info("🧭 PROPERTIES COORDINATE ORDER CHECK - Fetching sample centroids...")
            sample_wkt = self.conn.execute("""
                SELECT
                    ST_AsText(ST_Centroid(geometry)) as wkt_centroid
                FROM properties_full
                WHERE geometry IS NOT NULL
                LIMIT 5
            """).fetchall()

            if sample_wkt:
                self.log.info(
                    f"🧭 PROPERTIES COORDINATE ORDER CHECK - "
                    f"Found {len(sample_wkt)} sample centroids:"
                )
                coord_pairs = []
                for i, (wkt,) in enumerate(sample_wkt[:3]):
                    self.log.info(f"   Raw WKT {i+1}: {wkt}")
                    # Extract coordinates from "POINT(x y)" format
                    if wkt and "POINT(" in wkt:
                        coords_str = wkt.replace("POINT(", "").replace(")", "")
                        try:
                            coord_parts = coords_str.split()
                            if len(coord_parts) >= 2:
                                first_val, second_val = float(coord_parts[0]), float(coord_parts[1])
                                coord_pairs.append((first_val, second_val))
                                self.log.info(
                                    f"   Property {i+1}: POINT({first_val:.6f} {second_val:.6f})"
                                )
                            else:
                                self.log.warning(
                                    f"   Property {i+1}: Invalid coordinate format: {coords_str}"
                                )
                        except Exception as parse_e:
                            self.log.warning(f"   Property {i+1}: Parse error: {parse_e}")
                            continue
                    else:
                        self.log.warning(f"   Property {i+1}: Not a POINT geometry: {wkt}")

                if coord_pairs:
                    self.log.info(
                        f"🧭 PROPERTIES: Successfully parsed {len(coord_pairs)} coordinate pairs"
                    )
                    # Analyze the pattern - first value in coordinate pair
                    first_vals = [pair[0] for pair in coord_pairs]
                    second_vals = [pair[1] for pair in coord_pairs]
                    first_range = (min(first_vals), max(first_vals))
                    second_range = (min(second_vals), max(second_vals))

                    self.log.info(
                        f"🧭 PROPERTIES: First values range: "
                        f"{first_range[0]:.2f} to {first_range[1]:.2f}"
                    )
                    self.log.info(
                        f"🧭 PROPERTIES: Second values range: "
                        f"{second_range[0]:.2f} to {second_range[1]:.2f}"
                    )

                    if (
                        8 <= first_range[0] <= 15
                        and 8 <= first_range[1] <= 15
                        and 54 <= second_range[0] <= 58
                        and 54 <= second_range[1] <= 58
                    ):
                        self.log.info(
                            f"✅ PROPERTIES CONFIRMED: Data stored as (LON, LAT) - "
                            f"({first_range[0]:.2f}-{first_range[1]:.2f}, "
                            f"{second_range[0]:.2f}-{second_range[1]:.2f})"
                        )
                    elif (
                        54 <= first_range[0] <= 58
                        and 54 <= first_range[1] <= 58
                        and 8 <= second_range[0] <= 15
                        and 8 <= second_range[1] <= 15
                    ):
                        self.log.warning(
                            f"⚠️ PROPERTIES ALERT: Data stored as (LAT, LON) - "
                            f"({first_range[0]:.2f}-{first_range[1]:.2f}, "
                            f"{second_range[0]:.2f}-{second_range[1]:.2f})"
                        )
                    else:
                        self.log.warning(
                            f"❓ PROPERTIES UNCLEAR: Coordinate order unclear - "
                            f"({first_range[0]:.2f}-{first_range[1]:.2f}, "
                            f"{second_range[0]:.2f}-{second_range[1]:.2f})"
                        )
                else:
                    self.log.warning(
                        "🧭 PROPERTIES: No valid coordinate pairs extracted from centroids"
                    )
            else:
                self.log.warning("🧭 PROPERTIES: No sample WKT centroids returned from query")
        except Exception as e:
            self.log.warning(f"⚠️ Could not verify properties coordinate order: {e}")
            self.log.warning(f"   Exception type: {type(e).__name__}")
            import traceback

            self.log.warning(f"   Traceback: {traceback.format_exc()}")
        self.log.info(f"📊 Input: {properties_count:,} properties to filter")

        # Validate dataset integrity to prevent segfaults
        self.log.info("🔍 Validating properties dataset integrity...")
        null_geom_count = self.conn.execute(
            "SELECT COUNT(*) FROM properties_full WHERE geometry IS NULL"
        ).fetchone()[0]
        null_bfe_count = self.conn.execute(
            "SELECT COUNT(*) FROM properties_full WHERE bfe_number IS NULL"
        ).fetchone()[0]

        if null_geom_count > 0:
            self.log.warning(f"⚠️ Found {null_geom_count} properties with NULL geometry")
        if null_bfe_count > 0:
            self.log.warning(f"⚠️ Found {null_bfe_count} properties with NULL bfe_number")

        self.log.info(f"✅ Dataset validation complete: {properties_count:,} valid properties")

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

        # Configure DuckDB for memory-constrained processing
        self.log.info("Configuring DuckDB for memory-constrained large dataset processing...")
        self.conn.execute("SET max_temp_directory_size='8GB'")  # Reduced from 12GB
        self.conn.execute("SET threads=2")  # Reduced from 4 to prevent memory pressure
        self.conn.execute("SET preserve_insertion_order=false")
        self.conn.execute("SET memory_limit='4GB'")  # Set explicit memory limit

        # Get total property count for chunking
        total_properties = self.conn.execute("SELECT COUNT(*) FROM properties_full").fetchone()[0]
        chunk_size = 250000  # Reduced chunk size to prevent memory exhaustion
        num_chunks = (total_properties + chunk_size - 1) // chunk_size

        self.log.info(
            f"🚀 Pre-filtering {total_properties:,} properties in {num_chunks} chunks "
            f"of {chunk_size:,}"
        )

        # Initialize filtered properties table
        self.conn.execute("""
            CREATE OR REPLACE TABLE properties_filtered AS
            SELECT
                bfe_number,
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
                    bfe_number,
                    geometry,
                    ST_Area_Spheroid(geometry) as property_area_m2
                FROM properties_full
                ORDER BY bfe_number
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
                    p.bfe_number,
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
                f"  ✅ Chunk {chunk_num + 1}: {chunk_filtered:,}/{chunk_count:,} properties kept "
                f"({chunk_filtered / chunk_count * 100:.1f}%) - {chunk_time:.1f}s"
            )

        # Final statistics
        processing_time = time.time() - start_time
        reduction_pct = (1 - total_filtered / total_properties) * 100

        self.log.info(
            f"🎯 MASSIVE REDUCTION: {total_properties:,} → {total_filtered:,} properties "
            f"({reduction_pct:.1f}% reduction)"
        )
        self.log.info(
            f"⚡ Stage 1 complexity reduced from {total_properties * 600000 / 1e9:.1f}B to "
            f"{total_filtered * 600000 / 1e9:.1f}B combinations"
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
            "performance_improvement": (
                f"{total_properties / total_filtered:.1f}x reduction in Stage 1 complexity"
            ),
        }

    def _save_output_data(self, result: Dict[str, Any]):
        """Save output data - already handled in _execute_stage_processing for Stage 0."""
        # Stage 0 classes handle export directly in _execute_stage_processing
        # to use custom output paths and naming conventions
        self.log.info("✅ Properties pre-filtering data already saved to GCS")
