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
        
        # First check the geometry column format to handle different data types properly
        geometry_type_check = self.conn.execute("""
            SELECT 
                typeof(geometry) as geom_type,
                COUNT(*) as count
            FROM wetlands_raw 
            WHERE geometry IS NOT NULL 
            GROUP BY typeof(geometry)
            LIMIT 10
        """).fetchall()
        
        if geometry_type_check:
            geom_types = [(row[0], row[1]) for row in geometry_type_check]
            self.log.info(f"🔍 Geometry column types detected: {geom_types}")
        
        # COORDINATE VALIDATION: Check coordinate bounds to ensure proper lon/lat order
        self.log.info("🌍 Validating coordinate bounds and order...")
        
        # Try coordinate validation with error handling
        # Since we know geometry is GEOMETRY type, use direct approach first
        try:
            coord_validation = self.conn.execute("""
                SELECT 
                    MIN(ST_XMin(geometry)) as min_x,
                    MAX(ST_XMax(geometry)) as max_x,
                    MIN(ST_YMin(geometry)) as min_y,
                    MAX(ST_YMax(geometry)) as max_y
                FROM wetlands_raw 
                WHERE geometry IS NOT NULL 
                LIMIT 1000
            """).fetchone()
        except Exception as e:
            self.log.warning(f"⚠️ Direct coordinate validation failed: {e}")
            self.log.info("🔄 Trying with type-safe CASE statement...")
            try:
                coord_validation = self.conn.execute("""
                    SELECT 
                        MIN(ST_XMin(
                            CASE 
                                WHEN typeof(geometry) = 'VARCHAR' AND geometry != '' THEN
                                    ST_GeomFromText(geometry)
                                WHEN typeof(geometry) = 'BLOB' THEN
                                    ST_GeomFromWKB(geometry)
                                ELSE geometry
                            END
                        )) as min_x,
                        MAX(ST_XMax(
                            CASE 
                                WHEN typeof(geometry) = 'VARCHAR' AND geometry != '' THEN
                                    ST_GeomFromText(geometry)
                                WHEN typeof(geometry) = 'BLOB' THEN
                                    ST_GeomFromWKB(geometry)
                                ELSE geometry
                            END
                        )) as max_x,
                        MIN(ST_YMin(
                            CASE 
                                WHEN typeof(geometry) = 'VARCHAR' AND geometry != '' THEN
                                    ST_GeomFromText(geometry)
                                WHEN typeof(geometry) = 'BLOB' THEN
                                    ST_GeomFromWKB(geometry)
                                ELSE geometry
                            END
                        )) as min_y,
                        MAX(ST_YMax(
                            CASE 
                                WHEN typeof(geometry) = 'VARCHAR' AND geometry != '' THEN
                                    ST_GeomFromText(geometry)
                                WHEN typeof(geometry) = 'BLOB' THEN
                                    ST_GeomFromWKB(geometry)
                                ELSE geometry
                            END
                        )) as max_y
                    FROM wetlands_raw 
                    WHERE geometry IS NOT NULL 
                    LIMIT 1000
                """).fetchone()
            except Exception as e2:
                self.log.warning(f"⚠️ Fallback coordinate validation also failed: {e2}")
                coord_validation = None
        
        if coord_validation:
            min_x, max_x, min_y, max_y = coord_validation
            self.log.info(
                f"📍 Coordinate bounds: X({min_x:.2f}, {max_x:.2f}), "
                f"Y({min_y:.2f}, {max_y:.2f})"
            )
            
            # Check if coordinates are in expected ranges for Denmark
            if min_x >= 8 and max_x <= 16 and min_y >= 54 and max_y <= 58:
                self.log.info(
                    "✅ Coordinates appear to be in WGS84 (EPSG:4326) - Denmark bounds OK"
                )
            elif min_x >= 440000 and max_x <= 900000 and min_y >= 6040000 and max_y <= 6420000:
                self.log.info(
                    "✅ Coordinates appear to be in UTM Zone 32N (EPSG:25832) - "
                    "Denmark bounds OK"
                )
            else:
                self.log.warning("⚠️ Coordinates outside expected Denmark bounds!")
                self.log.warning("   WGS84 expected: X(8-16), Y(54-58)")
                self.log.warning("   UTM32N expected: X(440000-900000), Y(6040000-6420000)")
                self.log.warning(
                    f"   Actual: X({min_x:.2f}-{max_x:.2f}), Y({min_y:.2f}-{max_y:.2f})"
                )
        else:
            self.log.warning("⚠️ Could not validate coordinate bounds")
        
        # 🔍 COORDINATE ORDER VERIFICATION: Extract raw coordinate pairs to verify actual order
        try:
            sample_wkt = self.conn.execute("""
                SELECT 
                    ST_AsText(ST_Centroid(geometry)) as wkt_centroid
                FROM wetlands_raw 
                WHERE geometry IS NOT NULL 
                LIMIT 5
            """).fetchall()
            
            if sample_wkt:
                self.log.info("🧭 WETLANDS COORDINATE ORDER CHECK - Raw WKT centroids:")
                coord_pairs = []
                for i, (wkt,) in enumerate(sample_wkt[:3]):
                    # Extract coordinates from "POINT(x y)" format
                    if wkt and 'POINT(' in wkt:
                        coords_str = wkt.replace('POINT(', '').replace(')', '')
                        try:
                            first_val, second_val = map(float, coords_str.split())
                            coord_pairs.append((first_val, second_val))
                            self.log.info(
                                f"   Wetland {i+1}: POINT({first_val:.6f} {second_val:.6f})"
                            )
                        except Exception:
                            continue
                
                if coord_pairs:
                    # Analyze the pattern - first value in coordinate pair
                    first_vals = [pair[0] for pair in coord_pairs]
                    second_vals = [pair[1] for pair in coord_pairs]
                    first_range = (min(first_vals), max(first_vals))
                    second_range = (min(second_vals), max(second_vals))
                    
                    if (8 <= first_range[0] <= 15 and 8 <= first_range[1] <= 15 and 
                        54 <= second_range[0] <= 58 and 54 <= second_range[1] <= 58):
                        self.log.info(f"✅ WETLANDS CONFIRMED: Data stored as (LON, LAT) - "
                                    f"({first_range[0]:.2f}-{first_range[1]:.2f}, "
                                    f"{second_range[0]:.2f}-{second_range[1]:.2f})")
                    elif (54 <= first_range[0] <= 58 and 54 <= first_range[1] <= 58 and 
                          8 <= second_range[0] <= 15 and 8 <= second_range[1] <= 15):
                        self.log.warning(f"⚠️ WETLANDS ALERT: Data stored as (LAT, LON) - "
                                       f"({first_range[0]:.2f}-{first_range[1]:.2f}, "
                                       f"{second_range[0]:.2f}-{second_range[1]:.2f})")
                    else:
                        self.log.warning(f"❓ WETLANDS UNCLEAR: Coordinate order unclear - "
                                       f"({first_range[0]:.2f}-{first_range[1]:.2f}, "
                                       f"{second_range[0]:.2f}-{second_range[1]:.2f})")
        except Exception as e:
            self.log.warning(f"⚠️ Could not verify wetlands coordinate order: {e}")
        
        # Handle different geometry formats (WKT string, WKB binary, or already parsed geometry)
        # Use simpler approach: since we know geometry is GEOMETRY type, use it directly
        try:
            self.conn.execute("""
                CREATE OR REPLACE TABLE wetlands_full AS
                SELECT
                    toerv_pct,
                    UNNEST(ST_Dump(geometry)).geom as geometry
                FROM wetlands_raw
                WHERE geometry IS NOT NULL 
            """)
        except Exception as e:
            self.log.warning(f"⚠️ Failed with direct geometry approach: {e}")
            self.log.info("🔄 Trying with type-safe CASE statement...")
            
            # Fallback: Use type-safe approach with explicit type checking
            self.conn.execute("""
                CREATE OR REPLACE TABLE wetlands_full AS
                SELECT
                    toerv_pct,
                    UNNEST(ST_Dump(
                        CASE 
                            WHEN typeof(geometry) = 'VARCHAR' AND geometry != '' THEN
                                ST_GeomFromText(geometry)
                            WHEN typeof(geometry) = 'BLOB' THEN
                                ST_GeomFromWKB(geometry)
                            ELSE
                                geometry
                        END
                    )).geom as geometry
                FROM wetlands_raw
                WHERE geometry IS NOT NULL 
            """)

        # Log dataset sizes
        raw_count = self.conn.execute("SELECT COUNT(*) FROM wetlands_raw").fetchone()[0]
        decomposed_count = self.conn.execute("SELECT COUNT(*) FROM wetlands_full").fetchone()[0]
        self.log.info(
            f"📊 Input: {raw_count:,} wetland MultiPolygons → {decomposed_count:,} "
            f"individual polygons after ST_Dump"
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

            # Create wetlands chunk with deterministic ordering to prevent cross-chunk duplicates
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE wetlands_chunk AS
                SELECT
                    toerv_pct,
                    geometry
                FROM wetlands_full
                ORDER BY toerv_pct, ST_X(ST_Centroid(geometry)), ST_Y(ST_Centroid(geometry))
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
                f"  ✅ Chunk {chunk_num + 1}: {chunk_filtered:,}/{chunk_count:,} wetlands kept "
                f"({chunk_filtered / chunk_count * 100:.1f}%) - {chunk_time:.1f}s"
            )

            # Add unique IDs with spatial ordering (ST_Dump already done in _load_input_data)
        self.log.info(
            "Adding deterministic keys to filtered wetland polygons (and keeping legacy IDs)..."
        )
        self.conn.execute("""
            CREATE OR REPLACE TABLE wetlands_filtered AS
            SELECT
                -- Deterministic fragment key based on initial decomposed geometry
                md5(CAST(ST_AsWKB(geometry) AS VARCHAR)) AS wetland_key,
                -- Legacy numeric ID retained for backward compatibility
                -- (non-deterministic ordering)
                ROW_NUMBER() OVER (
                    ORDER BY toerv_pct, ST_X(ST_Centroid(geometry)), ST_Y(ST_Centroid(geometry))
                ) AS wetland_id,
                toerv_pct,
                geometry,
                ST_Area_Spheroid(geometry) as wetland_area_m2
            FROM wetlands_intersecting
        """)
        # DIAGNOSTIC: Assert wetland_key exists immediately; fail fast to expose root cause
        cols = [
            r[0]
            for r in self.conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'wetlands_filtered'"
            ).fetchall()
        ]
        if "wetland_key" not in [c.lower() for c in cols]:
            schema = self.conn.execute("DESCRIBE wetlands_filtered").fetchall()
            self.log.error(f"wetlands_filtered schema: {schema}")
            raise RuntimeError("Stage 0: wetlands_filtered missing wetland_key after creation")

        total_filtered = self.conn.execute("SELECT COUNT(*) FROM wetlands_filtered").fetchone()[0]

        # Final statistics
        processing_time = time.time() - start_time
        reduction_pct = (1 - total_intersecting / total_wetlands) * 100

        self.log.info(
            f"🎯 MASSIVE WETLANDS REDUCTION: {total_wetlands:,} → "
            f"{total_intersecting:,} polygons ({reduction_pct:.1f}% reduction)"
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
            "performance_improvement": (
                f"{reduction_pct:.1f}% reduction in Stage 2 wetlands processing"
            ),
        }

    def _save_output_data(self, result: Dict[str, Any]):
        """Save output data - already handled in _execute_stage_processing for Stage 0."""
        # Stage 0 classes handle export directly in _execute_stage_processing
        # to use custom output paths and naming conventions
        self.log.info("✅ Wetlands pre-filtering data already saved to GCS")
