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
        self._load_silver_dataset(CONFIG.water_projects_dataset, "water_projects_raw")

        # CRITICAL: Decompose water projects with ST_Dump BEFORE spatial join
        # to prevent memory issues
        self.log.info(
            "Decomposing water project MultiPolygons with ST_Dump to prevent memory overflow..."
        )

        # First check the geometry column format to handle different data types properly
        geometry_type_check = self.conn.execute("""
            SELECT
                typeof(geometry) as geom_type,
                COUNT(*) as count
            FROM water_projects_raw
            WHERE geometry IS NOT NULL
            GROUP BY typeof(geometry)
            LIMIT 10
        """).fetchall()

        if geometry_type_check:
            geom_types = [(row[0], row[1]) for row in geometry_type_check]
            self.log.info(f"🔍 Water projects geometry column types detected: {geom_types}")

        # COORDINATE VALIDATION: Check coordinate bounds to ensure proper lon/lat order
        self.log.info("🌍 Validating water projects coordinate bounds and order...")

        # Try coordinate validation with error handling
        # Since we know geometry is GEOMETRY type, use direct approach first
        try:
            coord_validation = self.conn.execute("""
                SELECT
                    MIN(ST_XMin(geometry)) as min_x,
                    MAX(ST_XMax(geometry)) as max_x,
                    MIN(ST_YMin(geometry)) as min_y,
                    MAX(ST_YMax(geometry)) as max_y
                FROM water_projects_raw
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
                    FROM water_projects_raw
                    WHERE geometry IS NOT NULL
                    LIMIT 1000
                """).fetchone()
            except Exception as e2:
                self.log.warning(f"⚠️ Fallback coordinate validation also failed: {e2}")
                coord_validation = None

        if coord_validation:
            min_x, max_x, min_y, max_y = coord_validation
            self.log.info(
                f"📍 Water projects bounds: X({min_x:.2f}, {max_x:.2f}), "
                f"Y({min_y:.2f}, {max_y:.2f})"
            )

            # Check if coordinates are in expected ranges for Denmark
            if min_x >= 8 and max_x <= 15 and min_y >= 54 and max_y <= 58:
                self.log.info(
                    "✅ Water projects coordinates in WGS84 (EPSG:4326) - Denmark bounds OK"
                )
            elif min_x >= 440000 and max_x <= 900000 and min_y >= 6040000 and max_y <= 6420000:
                self.log.info(
                    "✅ Water projects coordinates in UTM Zone 32N (EPSG:25832) - Denmark bounds OK"
                )
            else:
                self.log.warning("⚠️ Water projects coordinates outside expected Denmark bounds!")
                self.log.warning(
                    f"   Actual: X({min_x:.2f}-{max_x:.2f}), Y({min_y:.2f}-{max_y:.2f})"
                )
        else:
            self.log.warning("⚠️ Could not validate water projects coordinate bounds")

        # 🔍 COORDINATE ORDER VERIFICATION: Extract raw coordinate pairs to verify actual order
        try:
            self.log.info("🧭 WATER PROJECTS COORDINATE ORDER CHECK - Fetching sample centroids...")
            sample_wkt = self.conn.execute("""
                SELECT
                    ST_AsText(ST_Centroid(geometry)) as wkt_centroid
                FROM water_projects_raw
                WHERE geometry IS NOT NULL
                LIMIT 5
            """).fetchall()

            if sample_wkt:
                self.log.info(
                    f"🧭 WATER PROJECTS COORDINATE ORDER CHECK - "
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
                                    f"   Water Project {i+1}: "
                                    f"POINT({first_val:.6f} {second_val:.6f})"
                                )
                            else:
                                self.log.warning(
                                    f"   Water Project {i+1}: "
                                    f"Invalid coordinate format: {coords_str}"
                                )
                        except Exception as parse_e:
                            self.log.warning(f"   Water Project {i+1}: Parse error: {parse_e}")
                            continue
                    else:
                        self.log.warning(f"   Water Project {i+1}: Not a POINT geometry: {wkt}")

                if coord_pairs:
                    self.log.info(
                        f"🧭 WATER PROJECTS: Successfully parsed "
                        f"{len(coord_pairs)} coordinate pairs"
                    )
                    # Analyze the pattern - first value in coordinate pair
                    first_vals = [pair[0] for pair in coord_pairs]
                    second_vals = [pair[1] for pair in coord_pairs]
                    first_range = (min(first_vals), max(first_vals))
                    second_range = (min(second_vals), max(second_vals))

                    self.log.info(
                        f"🧭 WATER PROJECTS: First values range: "
                        f"{first_range[0]:.2f} to {first_range[1]:.2f}"
                    )
                    self.log.info(
                        f"🧭 WATER PROJECTS: Second values range: "
                        f"{second_range[0]:.2f} to {second_range[1]:.2f}"
                    )

                    if (
                        8 <= first_range[0] <= 15
                        and 8 <= first_range[1] <= 15
                        and 54 <= second_range[0] <= 58
                        and 54 <= second_range[1] <= 58
                    ):
                        self.log.info(
                            f"✅ WATER PROJECTS CONFIRMED: Data stored as (LON, LAT) - "
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
                            f"⚠️ WATER PROJECTS ALERT: Data stored as (LAT, LON) - "
                            f"({first_range[0]:.2f}-{first_range[1]:.2f}, "
                            f"{second_range[0]:.2f}-{second_range[1]:.2f})"
                        )
                    else:
                        self.log.warning(
                            f"❓ WATER PROJECTS UNCLEAR: Coordinate order unclear - "
                            f"({first_range[0]:.2f}-{first_range[1]:.2f}, "
                            f"{second_range[0]:.2f}-{second_range[1]:.2f})"
                        )
                else:
                    self.log.warning(
                        "🧭 WATER PROJECTS: No valid coordinate pairs extracted from centroids"
                    )
            else:
                self.log.warning("🧭 WATER PROJECTS: No sample WKT centroids returned from query")
        except Exception as e:
            self.log.warning(f"⚠️ Could not verify water projects coordinate order: {e}")
            self.log.warning(f"   Exception type: {type(e).__name__}")
            import traceback

            self.log.warning(f"   Traceback: {traceback.format_exc()}")

        # Handle different geometry formats (WKT string, WKB binary, or already parsed geometry)
        # Use simpler approach: since we know geometry is GEOMETRY type, use it directly
        try:
            self.conn.execute("""
                CREATE OR REPLACE TABLE water_projects_full AS
                SELECT
                    project_id,
                    UNNEST(ST_Dump(geometry)).geom as geometry
                FROM water_projects_raw
                WHERE geometry IS NOT NULL
            """)
        except Exception as e:
            self.log.warning(f"⚠️ Failed with direct geometry approach: {e}")
            self.log.info("🔄 Trying with type-safe CASE statement...")

            # Fallback: Use type-safe approach with explicit type checking
            self.conn.execute("""
                CREATE OR REPLACE TABLE water_projects_full AS
                SELECT
                    project_id,
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
                FROM water_projects_raw
                WHERE geometry IS NOT NULL
            """)

        # Log dataset sizes
        raw_count = self.conn.execute("SELECT COUNT(*) FROM water_projects_raw").fetchone()[0]
        decomposed_count = self.conn.execute("SELECT COUNT(*) FROM water_projects_full").fetchone()[
            0
        ]
        self.log.info(
            f"📊 Input: {raw_count:,} water project MultiPolygons → "
            f"{decomposed_count:,} individual polygons after ST_Dump"
        )

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

        # Add areas to filtered water projects (ST_Dump already done in _load_input_data)
        self.log.info("Adding areas to filtered water projects...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE water_projects_filtered AS
            SELECT
                project_id,
                geometry,
                ST_Area_Spheroid(geometry) as project_area_m2
            FROM water_projects_intersecting
        """)

        total_filtered = self.conn.execute(
            "SELECT COUNT(*) FROM water_projects_filtered"
        ).fetchone()[0]

        # Final statistics
        processing_time = time.time() - start_time
        reduction_pct = (1 - intersecting_count / total_projects) * 100

        self.log.info(
            f"🎯 WATER PROJECTS REDUCTION: {total_projects:,} → {intersecting_count:,} "
            f"projects ({reduction_pct:.1f}% reduction)"
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
            "performance_improvement": (
                f"{reduction_pct:.1f}% reduction in Stage 1 water project processing"
            ),
        }

    def _save_output_data(self, result: Dict[str, Any]):
        """Save output data - already handled in _execute_stage_processing for Stage 0."""
        # Stage 0 classes handle export directly in _execute_stage_processing
        # to use custom output paths and naming conventions
        self.log.info("✅ Water projects pre-filtering data already saved to GCS")
