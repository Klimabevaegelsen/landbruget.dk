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
        self._load_silver_dataset(CONFIG.bnbo_status_dataset, "bnbo_status_raw")

        # CRITICAL: Decompose BNBO with ST_Dump BEFORE spatial join to prevent memory issues
        self.log.info("Decomposing BNBO MultiPolygons with ST_Dump to prevent memory overflow...")
        
        # First check the geometry column format to handle different data types properly
        geometry_type_check = self.conn.execute("""
            SELECT 
                typeof(geometry) as geom_type,
                COUNT(*) as count
            FROM bnbo_status_raw 
            WHERE geometry IS NOT NULL 
            GROUP BY typeof(geometry)
            LIMIT 10
        """).fetchall()
        
        if geometry_type_check:
            geom_types = [(row[0], row[1]) for row in geometry_type_check]
            self.log.info(f"🔍 BNBO geometry column types detected: {geom_types}")
        
        # COORDINATE VALIDATION: Check coordinate bounds to ensure proper lon/lat order
        self.log.info("🌍 Validating BNBO coordinate bounds and order...")
        
        # Try coordinate validation with error handling
        # Since we know geometry is GEOMETRY type, use direct approach first
        try:
            coord_validation = self.conn.execute("""
                SELECT 
                    MIN(ST_XMin(geometry)) as min_x,
                    MAX(ST_XMax(geometry)) as max_x,
                    MIN(ST_YMin(geometry)) as min_y,
                    MAX(ST_YMax(geometry)) as max_y
                FROM bnbo_status_raw 
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
                    FROM bnbo_status_raw 
                    WHERE geometry IS NOT NULL 
                    LIMIT 1000
                """).fetchone()
            except Exception as e2:
                self.log.warning(f"⚠️ Fallback coordinate validation also failed: {e2}")
                coord_validation = None
        
        if coord_validation:
            min_x, max_x, min_y, max_y = coord_validation
            self.log.info(
                f"📍 BNBO bounds: X({min_x:.2f}, {max_x:.2f}), Y({min_y:.2f}, {max_y:.2f})"
            )
            
            # Check if coordinates are in expected ranges for Denmark
            if min_x >= 8 and max_x <= 16 and min_y >= 54 and max_y <= 58:
                self.log.info(
                    "✅ BNBO coordinates in WGS84 (EPSG:4326) - Denmark bounds OK"
                )
            elif min_x >= 440000 and max_x <= 900000 and min_y >= 6040000 and max_y <= 6420000:
                self.log.info(
                    "✅ BNBO coordinates in UTM Zone 32N (EPSG:25832) - Denmark bounds OK"
                )
            else:
                self.log.warning("⚠️ BNBO coordinates outside expected Denmark bounds!")
                self.log.warning(f"   WGS84 expected: X(8-16), Y(54-58)")
                self.log.warning(f"   UTM32N expected: X(440000-900000), Y(6040000-6420000)")
                self.log.warning(f"   Actual: X({min_x:.2f}-{max_x:.2f}), Y({min_y:.2f}-{max_y:.2f})")
        
        # Handle different geometry formats (WKT string, WKB binary, or already parsed geometry)
        # Use simpler approach: since we know geometry is GEOMETRY type, use it directly
        try:
            self.conn.execute("""
                CREATE OR REPLACE TABLE bnbo_status_full AS
                SELECT
                    status_category,
                    UNNEST(ST_Dump(geometry)).geom as geometry
                FROM bnbo_status_raw
                WHERE geometry IS NOT NULL 
            """)
        except Exception as e:
            self.log.warning(f"⚠️ Failed with direct geometry approach: {e}")
            self.log.info("🔄 Trying with type-safe CASE statement...")
            
            # Fallback: Use type-safe approach with explicit type checking
            self.conn.execute("""
                CREATE OR REPLACE TABLE bnbo_status_full AS
                SELECT
                    status_category,
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
                FROM bnbo_status_raw
                WHERE geometry IS NOT NULL 
            """)

        # Log dataset sizes
        raw_count = self.conn.execute("SELECT COUNT(*) FROM bnbo_status_raw").fetchone()[0]
        decomposed_count = self.conn.execute("SELECT COUNT(*) FROM bnbo_status_full").fetchone()[0]
        self.log.info(
            f"📊 Input: {raw_count:,} BNBO MultiPolygons → "
            f"{decomposed_count:,} individual polygons after ST_Dump"
        )

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

        # Add unique IDs with spatial ordering (ST_Dump already done in _load_input_data)
        self.log.info("Adding unique IDs to filtered BNBO polygons...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE bnbo_filtered AS
            SELECT
                ROW_NUMBER() OVER (
                    ORDER BY status_category, 
                             ST_X(ST_Centroid(geometry)), 
                             ST_Y(ST_Centroid(geometry))
                ) as bnbo_id,
                status_category,
                geometry,
                ST_Area_Spheroid(geometry) as bnbo_area_m2
            FROM bnbo_intersecting
        """)

        total_filtered = self.conn.execute("SELECT COUNT(*) FROM bnbo_filtered").fetchone()[0]

        # Final statistics
        processing_time = time.time() - start_time
        reduction_pct = (1 - intersecting_count / total_bnbo) * 100

        self.log.info(
            f"🎯 BNBO REDUCTION: {total_bnbo:,} → {intersecting_count:,} polygons "
            f"({reduction_pct:.1f}% reduction)"
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
