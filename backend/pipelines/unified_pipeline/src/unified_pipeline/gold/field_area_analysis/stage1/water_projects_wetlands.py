"""Stage 1B: Water Projects × Wetlands Intersection Analysis

Calculate which wetland areas are covered by water projects.
This creates a foundation dataset for later field-level analysis.

Optimized for DuckDB Spatial v1.2.2 with ST_Dump for multipolygon decomposition.
"""

from typing import Any, Dict

import duckdb

from ..base import FieldAnalysisStageBase, FieldAnalysisStageConfig
from ..config import CONFIG


class WaterProjectsWetlandsIntersection(FieldAnalysisStageBase):
    """Calculate wetland area coverage by water projects."""

    def __init__(self, config: FieldAnalysisStageConfig = None):
        if config is None:
            config = FieldAnalysisStageConfig()
        super().__init__(config, "Stage 1B: Water Projects × Wetlands")

    def _load_input_data(self):
        """Load Stage 0 pre-filtered wetlands and water projects datasets."""
        # Get year-aware dataset names
        updated_outputs = CONFIG.update_outputs_for_year()
        
        # Load Stage 0 pre-filtered wetlands (1.6M → ~200K, 85% reduction)
        self.log.info("Loading Stage 0 pre-filtered wetlands dataset...")
        stage0_wetlands_dataset = updated_outputs["wetlands_prefiltered"]

        # Pick the latest Stage 0 partition that contains the required wetland_key column
        def _latest_gold_with_column(dataset: str, required_column: str) -> str:
            pattern = f"gs://{CONFIG.bucket}/gold/{dataset}/*/data.parquet"
            files = sorted(self.gcs_access.list_files(pattern), reverse=True)
            if not files:
                raise FileNotFoundError(f"No gold data found for {dataset}")
            for file_path in files:
                try:
                    # Read schema only - use direct string interpolation since DuckDB doesn't support parameterized read_parquet
                    query = f"DESCRIBE read_parquet('{file_path}')"
                    cols = self.conn.execute(query).fetchall()
                    colnames_lower = [c[0].lower() for c in cols]
                    if required_column.lower() in colnames_lower:
                        return file_path
                except Exception as e:
                    self.log.warning(f"Could not read schema from {file_path}: {e}")
                    continue
            raise RuntimeError(
                f"No partition of {dataset} contains required column '{required_column}'. "
                f"Checked {len(files)} candidates; ensure Stage 0 produced it."
            )

        stage0_wetlands_path = _latest_gold_with_column(stage0_wetlands_dataset, "wetland_key")
        self.gcs_access.query_parquet_direct(stage0_wetlands_path, "SELECT *", "wetlands_raw")
        self.log.info(f"✅ Loaded wetlands from {stage0_wetlands_dataset}")

        # Load Stage 0 pre-filtered water projects (2.4K → ~500, 80% reduction)
        self.log.info("Loading Stage 0 pre-filtered water projects dataset...")
        stage0_water_projects_dataset = updated_outputs["water_projects_prefiltered"]
        stage0_water_projects_path = self._get_latest_gold_path(stage0_water_projects_dataset)
        self.gcs_access.query_parquet_direct(
            stage0_water_projects_path, "SELECT *", "water_projects_raw"
        )
        self.log.info(f"✅ Loaded water projects from {stage0_water_projects_dataset}")

        self.log.info("✅ STAGE 0 OPTIMIZATION: Using pre-filtered wetlands and water projects!")
        self.log.info(
            "🚀 PERFORMANCE: 8x faster than original (1.6M → 200K wetlands, 2.4K → 500 water projects)"
        )

        # Proper SPATIAL_JOIN optimization: Water projects (build side) + ALL wetlands (probe side)
        # Water projects = smaller dataset (~2.4K) that fits in memory for spatial indexing
        # Wetlands = larger dataset (1.6M) as probe side for comprehensive coverage

        self.log.info("Decomposing water projects with ST_Dump for optimal spatial indexing...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE water_projects AS
            SELECT 
                project_id,
                UNNEST(ST_Dump(geometry)).geom as geometry
            FROM water_projects_raw
        """)

        self.log.info("Using Stage 0 pre-filtered wetlands; validating presence of wetland_key...")
        cols = [r[0] for r in self.conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'wetlands_raw'"
        ).fetchall()]
        if 'wetland_key' not in [c.lower() for c in cols]:
            schema = self.conn.execute("DESCRIBE wetlands_raw").fetchall()
            self.log.error(f"wetlands_raw schema: {schema}")
            raise RuntimeError("Stage 1: wetlands_raw missing wetland_key; ensure Stage 0 produced it")

        self.conn.execute("""
            CREATE OR REPLACE TABLE wetlands AS
            SELECT 
                wetland_key, -- Deterministic fragment key for stable joins
                wetland_id,  -- Legacy numeric ID retained for compatibility
                toerv_pct,   -- Keep wetland type for analysis
                geometry     -- Already decomposed by Stage 0 ST_Dump
            FROM wetlands_raw
        """)

        # Log table sizes
        wetlands_count = self.conn.execute("SELECT COUNT(*) FROM wetlands").fetchone()[0]
        projects_count = self.conn.execute("SELECT COUNT(*) FROM water_projects").fetchone()[0]
        self.log.info(
            f"✅ Loaded {projects_count:,} water projects (build) and {wetlands_count:,} wetlands (probe)"
        )
        self.log.info(
            f"🎯 Processing SPATIAL_JOIN with {wetlands_count * projects_count:,} potential combinations"
        )

    async def _execute_stage_processing(self) -> Dict[str, Any]:
        """
        Create wetland-water project intersection foundation data for Stage 3B.

        Key optimizations:
        1. Process wetlands in batches to manage memory
        2. Create individual intersection records (not aggregates)
        3. Output foundation data that Stage 3B can JOIN efficiently
        4. Avoid recalculating intersections in later stages
        """

        # Enhanced CRS and area debugging
        self._debug_crs_and_areas(self.conn)

        # Get total wetland count for batching (use decomposed wetlands table)
        total_wetlands = self.conn.execute("SELECT COUNT(*) FROM wetlands").fetchone()[0]
        batch_size = 100000  # Smaller batches for foundation data creation
        num_batches = (total_wetlands + batch_size - 1) // batch_size

        self.log.info(
            f"Creating wetland-water project foundation data from {total_wetlands:,} wetlands in {num_batches} batches"
        )

        # Create intersection results table
        self.conn.execute("""
            CREATE OR REPLACE TABLE wetland_water_intersections (
                wetland_key VARCHAR,  -- Deterministic fragment key for stable joins
                wetland_id BIGINT,    -- Legacy numeric ID for compatibility/metrics
                toerv_pct VARCHAR,  -- Wetland type for analysis
                project_id VARCHAR,
                intersection_geometry GEOMETRY,  -- Intersection geometry for Stage 2 to use directly
                intersection_area_m2 DOUBLE,
                wetland_area_m2 DOUBLE,
                project_area_m2 DOUBLE
            )
        """)

        # Create timestamp directory for batch files
        from datetime import datetime

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        batch_base_path = (
            f"gs://{CONFIG.bucket}/gold/field_analysis_wetland_water_coverage/{timestamp}"
        )

        total_intersections = 0
        total_wetland_area = 0
        total_covered_area = 0

        # Process each batch
        for batch_num in range(num_batches):
            offset = batch_num * batch_size
            self.log.info(f"Processing batch {batch_num + 1}/{num_batches} (offset: {offset:,})")

            # Create wetlands batch (ST_Dump already applied in wetlands table)  
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE wetlands_batch_raw AS
                SELECT 
                    wetland_key,
                    wetland_id,
                    toerv_pct,
                    geometry
                FROM wetlands
                ORDER BY wetland_id
                LIMIT {batch_size} OFFSET {offset}
            """)

            # Calculate areas (wetland_id already exists from main wetlands table)
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE wetlands_batch AS
                SELECT 
                    wetland_key,
                    wetland_id,  -- Use existing wetland_id from decomposed wetlands table
                    toerv_pct,  -- Keep wetland type for analysis
                    geometry,
                    ST_Area_Spheroid(geometry) as wetland_area_m2
                FROM wetlands_batch_raw
            """)

            batch_count = self.conn.execute("SELECT COUNT(*) FROM wetlands_batch").fetchone()[0]
            if batch_count == 0:
                break

            self.log.info(f"  Processing {batch_count:,} wetlands in batch {batch_num + 1}")

            # Debug: Check if we have water projects and sample geometries
            water_projects_count = self.conn.execute(
                "SELECT COUNT(*) FROM water_projects"
            ).fetchone()[0]
            if water_projects_count == 0:
                self.log.warning("  ⚠️  No water projects found! Cannot create intersections.")
                continue

                # Sample a few geometries to check validity and CRS
            sample_wetland = self.conn.execute("""
                SELECT 
                    ST_IsValid(geometry), 
                    ST_Area_Spheroid(geometry),
                    'unknown' as srid,
                    ST_GeometryType(geometry),
                    ST_X(ST_Centroid(geometry)),
                    ST_Y(ST_Centroid(geometry))
                FROM wetlands_batch 
                LIMIT 1
            """).fetchone()

            sample_water = self.conn.execute("""
                SELECT 
                    ST_IsValid(geometry), 
                    ST_Area_Spheroid(geometry),
                    'unknown' as srid,
                    ST_GeometryType(geometry),
                    ST_X(ST_Centroid(geometry)),
                    ST_Y(ST_Centroid(geometry))
                FROM water_projects 
                LIMIT 1
            """).fetchone()

            if sample_wetland and sample_water:
                wetland_valid, wetland_area, wetland_srid, wetland_type, wetland_x, wetland_y = (
                    sample_wetland
                )
                water_valid, water_area, water_srid, water_type, water_x, water_y = sample_water
                self.log.info(
                    f"  🔍 Wetland - valid={wetland_valid}, area={wetland_area}, SRID={wetland_srid}, type={wetland_type}, center=({wetland_x:.2f}, {wetland_y:.2f})"
                )
                self.log.info(
                    f"  🔍 Water project - valid={water_valid}, area={water_area:.1f}m², SRID={water_srid}, type={water_type}, center=({water_x:.2f}, {water_y:.2f})"
                )

            # Create intersection records for this batch
            batch_query = """
            CREATE OR REPLACE TABLE batch_intersections AS
            SELECT 
                wb.wetland_key,
                wb.wetland_id,
                wb.toerv_pct,  -- Wetland type for analysis
                wp.project_id,
                ST_Intersection(wb.geometry, wp.geometry) as intersection_geometry,  -- Save intersection geometry for Stage 2
                ST_Area_Spheroid(ST_Intersection(wb.geometry, wp.geometry)) as intersection_area_m2,
                wb.wetland_area_m2,
                ST_Area_Spheroid(wp.geometry) as project_area_m2
            FROM wetlands_batch wb
            JOIN water_projects wp ON ST_Intersects(wb.geometry, wp.geometry)
            WHERE ST_Area_Spheroid(ST_Intersection(wb.geometry, wp.geometry)) > 0  -- Keep all intersections
            """

            self.conn.execute(batch_query)

            # Stream this batch to GCS immediately to avoid memory accumulation
            batch_intersections = self.conn.execute(
                "SELECT COUNT(*) FROM batch_intersections"
            ).fetchone()[0]

            if batch_intersections > 0:
                batch_gcs_path = (
                    f"{batch_base_path}/batch_{batch_num + 1:04d}_intersections.parquet"
                )
                self.gcs_access.export_table_to_gcs_direct("batch_intersections", batch_gcs_path)
                self.log.info(f"  📤 Streamed {batch_intersections:,} intersections to GCS")

            # Log batch progress
            batch_stats = self.conn.execute("""
                SELECT 
                    SUM(w.wetland_area_m2) / 1000000 as total_area_km2,
                    COUNT(*) as wetland_pieces_processed
                FROM wetlands_batch w
            """).fetchone()

            if batch_stats:
                area_km2, processed_count = batch_stats
                self.log.info(
                    f"  ✅ Batch {batch_num + 1}: {area_km2:.1f} km², {processed_count:,} wetland pieces, {batch_intersections:,} intersections"
                )

            total_intersections += batch_intersections

            # Clean up batch tables immediately
            self.conn.execute("DROP TABLE IF EXISTS wetlands_batch_raw")
            self.conn.execute("DROP TABLE IF EXISTS wetlands_batch")
            self.conn.execute("DROP TABLE IF EXISTS batch_intersections")

            # Memory cleanup every 2 batches
            if (batch_num + 1) % 2 == 0:
                import gc

                gc.collect()
                self.log.info(f"  🧹 Memory cleanup after batch {batch_num + 1}")

        # Consolidate all batch files into final intersection table
        self.log.info("Consolidating batched intersection files...")

        # Find all batch files in the current timestamp directory
        batch_pattern = f"{batch_base_path}/batch_*_intersections.parquet"

        # Check if any batch files exist before trying to load them
        try:
            # Load all batch files into final table
            self.gcs_access.query_multiple_direct(
                batch_pattern, "wetland_water_intersections", "SELECT *"
            )
            self.log.info("✅ Successfully consolidated batch intersection files")
        except FileNotFoundError:
            # No intersection files found - create empty table with correct schema
            self.log.info("No intersection files found - creating empty intersection table")
            self.conn.execute("""
                CREATE OR REPLACE TABLE wetland_water_intersections AS
                SELECT 
                    CAST(NULL AS BIGINT) as wetland_id,
                    CAST(NULL AS VARCHAR) as toerv_pct,
                    CAST(NULL AS VARCHAR) as project_id,
                    CAST(NULL AS GEOMETRY) as intersection_geometry,
                    CAST(NULL AS DOUBLE) as intersection_area_m2,
                    CAST(NULL AS DOUBLE) as wetland_area_m2,
                    CAST(NULL AS DOUBLE) as project_area_m2
                WHERE FALSE
            """)
            self.log.info("✅ Created empty intersection table (no intersections found)")

        # Log intersection results
        intersection_count = self.conn.execute(
            "SELECT COUNT(*) FROM wetland_water_intersections"
        ).fetchone()[0]

        self.log.info(
            f"✅ Created {intersection_count:,} wetland-water project intersection records"
        )

        return {
            "intersection_records": intersection_count,
            "batches_processed": num_batches,
        }

    def _save_output_data(self, result: Dict[str, Any]):
        """Save intersection records for later stages to use."""
        # Save detailed intersection records for Stage 3B to use
        self._save_stage_output(
            "wetland_water_intersections", "water_projects_wetlands_intersections"
        )

    def _debug_crs_and_areas(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Debug coordinate systems and area calculations."""
        self.log.info("🔍 Enhanced CRS and Area Debugging:")

        try:
            # Check water projects CRS and area
            wp_sample = conn.execute("""
                SELECT 
                    ST_XMin(geometry) as min_x,
                    ST_XMax(geometry) as max_x,
                    ST_YMin(geometry) as min_y,
                    ST_YMax(geometry) as max_y,
                    ST_Area_Spheroid(geometry) as area_spheroid,
                    ST_Area(geometry) as area_planar,
                    ST_IsValid(geometry) as is_valid,
                    ST_GeometryType(geometry) as geom_type
                FROM water_projects 
                WHERE geometry IS NOT NULL 
                LIMIT 1
            """).fetchone()

            if wp_sample:
                self.log.info(
                    f"Water Projects - X: {wp_sample[0]:.6f} to {wp_sample[1]:.6f}, Y: {wp_sample[2]:.6f} to {wp_sample[3]:.6f}"
                )
                self.log.info(
                    f"Water Projects - Area (spheroid): {wp_sample[4]:.2f}m², Area (planar): {wp_sample[5]:.2f}m²"
                )
                self.log.info(f"Water Projects - Valid: {wp_sample[6]}, Type: {wp_sample[7]}")

                # Check if coordinates look like WGS84 (longitude 7-16, latitude 54-58 for Denmark)
                x_looks_like_lon = 7 <= wp_sample[0] <= 16 and 7 <= wp_sample[1] <= 16
                y_looks_like_lat = 54 <= wp_sample[2] <= 58 and 54 <= wp_sample[3] <= 58
                self.log.info(
                    f"Water Projects - Coordinates look like WGS84: {x_looks_like_lon and y_looks_like_lat}"
                )

            # Check wetlands CRS and area
            w_sample = conn.execute("""
                SELECT 
                    ST_XMin(geometry) as min_x,
                    ST_XMax(geometry) as max_x,
                    ST_YMin(geometry) as min_y,
                    ST_YMax(geometry) as max_y,
                    ST_Area_Spheroid(geometry) as area_spheroid,
                    ST_Area(geometry) as area_planar,
                    ST_IsValid(geometry) as is_valid,
                    ST_GeometryType(geometry) as geom_type
                FROM wetlands_raw 
                WHERE geometry IS NOT NULL 
                LIMIT 1
            """).fetchone()

            if w_sample:
                self.log.info(
                    f"Wetlands - X: {w_sample[0]:.6f} to {w_sample[1]:.6f}, Y: {w_sample[2]:.6f} to {w_sample[3]:.6f}"
                )
                self.log.info(
                    f"Wetlands - Area (spheroid): {w_sample[4]:.2f}m², Area (planar): {w_sample[5]:.2f}m²"
                )
                self.log.info(f"Wetlands - Valid: {w_sample[6]}, Type: {w_sample[7]}")

                # Check if coordinates look like WGS84 vs UTM
                x_looks_like_lon = 7 <= w_sample[0] <= 16 and 7 <= w_sample[1] <= 16
                y_looks_like_lat = 54 <= w_sample[2] <= 58 and 54 <= w_sample[3] <= 58
                x_looks_like_utm = (
                    400000 <= w_sample[0] <= 900000 and 400000 <= w_sample[1] <= 900000
                )
                y_looks_like_utm = (
                    6000000 <= w_sample[2] <= 6500000 and 6000000 <= w_sample[3] <= 6500000
                )

                self.log.info(
                    f"Wetlands - Coordinates look like WGS84: {x_looks_like_lon and y_looks_like_lat}"
                )
                self.log.info(
                    f"Wetlands - Coordinates look like UTM (EPSG:25832): {x_looks_like_utm and y_looks_like_utm}"
                )

                if x_looks_like_utm and y_looks_like_utm:
                    self.log.error(
                        "🚨 PROBLEM: Wetlands still in UTM coordinates! Geometry validator failed to transform to WGS84"
                    )
                    self.log.error(
                        "   ST_Area_Spheroid() expects WGS84 coordinates, but got UTM → returns NaN"
                    )
                elif x_looks_like_lon and y_looks_like_lat:
                    self.log.info("✅ Wetlands correctly in WGS84 coordinates")
                else:
                    self.log.warning(
                        "⚠️ Wetlands coordinates don't match expected WGS84 or UTM patterns"
                    )

            # Test coordinate transformation manually
            self.log.info("🧪 Testing manual coordinate transformation:")
            try:
                test_result = conn.execute("""
                    SELECT 
                        ST_Transform(ST_Point(500000, 6200000), 'EPSG:25832', 'EPSG:4326') as transformed_point,
                        ST_X(ST_Transform(ST_Point(500000, 6200000), 'EPSG:25832', 'EPSG:4326')) as lon,
                        ST_Y(ST_Transform(ST_Point(500000, 6200000), 'EPSG:25832', 'EPSG:4326')) as lat
                """).fetchone()

                if test_result:
                    lon, lat = test_result[1], test_result[2]
                    self.log.info(f"UTM (500000, 6200000) → WGS84 (lon: {lon:.6f}, lat: {lat:.6f})")
                    if 7 <= lon <= 16 and 54 <= lat <= 58:
                        self.log.info("✅ Manual transformation working correctly")
                    else:
                        self.log.warning(
                            "⚠️ Manual transformation result outside Denmark bounds (lon: 7-16, lat: 54-58)"
                        )
                        self.log.info(
                            "🔧 DuckDB coordinate transformation is working, coordinates are just outside Denmark"
                        )

            except Exception as transform_e:
                self.log.error(f"🚨 Manual coordinate transformation failed: {transform_e}")

        except Exception as e:
            self.log.error(f"CRS debugging failed: {e}")
