"""Stage 2B: Fields × Wetland Water Coverage Analysis

Calculate wetland coverage by water projects for each field.
Uses pre-computed water project intersections from Stage 1B and fields from Stage 1D.

Optimized for DuckDB Spatial v1.2.2 with foundation data approach.
Based on successful Stage 1 implementations.
"""

from typing import Any, Dict

from ..base import FieldAnalysisStageBase, FieldAnalysisStageConfig
from ..config import CONFIG


class FieldsWetlandWaterCoverage(FieldAnalysisStageBase):
    """Calculate wetland coverage by water projects for each field using foundation data."""

    def __init__(self, config: FieldAnalysisStageConfig = None):
        if config is None:
            config = FieldAnalysisStageConfig()
        super().__init__(config, "Stage 2B: Fields × Wetland Water Coverage")

    def _load_input_data(self):
        """Load field data and water project wetland intersections from Stage 1."""
        # Load agricultural fields
        self._load_silver_dataset(CONFIG.agricultural_fields_dataset, "fields_raw")

        # Load original wetlands data for field intersections
        self._load_silver_dataset(CONFIG.wetlands_dataset, "wetlands_raw")

        # Load water project × wetland intersections from Stage 1B
        # This contains the actual intersection geometries we need
        stage1b_path = f"gs://{CONFIG.bucket}/gold/{CONFIG.stage_outputs['water_projects_wetlands_intersections']}/{CONFIG.stage_outputs['water_projects_wetlands_intersections']}.parquet"
        self.gcs_access.query_parquet_direct(
            stage1b_path, "SELECT *", "water_projects_wetlands_intersections"
        )

        # We also need the actual wetland-water project intersection geometries
        # But Stage 1B only saved statistics, not geometries. We need to recreate the covered wetland areas.
        # Load water projects to recreate intersection geometries
        self._load_silver_dataset(CONFIG.water_projects_dataset, "water_projects_raw")

        # Create the actual wetland areas covered by water projects
        self.log.info("Creating wetland areas covered by water projects...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE wetlands_covered_by_water AS
            SELECT 
                w.toerv_pct,
                ST_Intersection(w.geometry, wp.geometry) as covered_wetland_geometry,
                ST_Area_Spheroid(ST_Intersection(w.geometry, wp.geometry)) as covered_area_m2
            FROM wetlands_raw w
            JOIN water_projects_raw wp ON ST_Intersects(w.geometry, wp.geometry)
            WHERE ST_Area_Spheroid(ST_Intersection(w.geometry, wp.geometry)) > 100
        """)

        # Log loaded data
        fields_count = self.conn.execute("SELECT COUNT(*) FROM fields_raw").fetchone()[0]
        wetlands_count = self.conn.execute("SELECT COUNT(*) FROM wetlands_raw").fetchone()[0]
        covered_count = self.conn.execute(
            "SELECT COUNT(*) FROM wetlands_covered_by_water"
        ).fetchone()[0]
        intersections_count = self.conn.execute(
            "SELECT COUNT(*) FROM water_projects_wetlands_intersections"
        ).fetchone()[0]

        self.log.info("✅ Loaded data for field-level analysis:")
        self.log.info(f"   Agricultural fields: {fields_count:,}")
        self.log.info(f"   Wetland polygons: {wetlands_count:,}")
        self.log.info(f"   Wetland areas covered by water projects: {covered_count:,}")
        self.log.info(f"   Intersection records (for validation): {intersections_count:,}")

    async def _execute_stage_processing(self) -> Dict[str, Any]:
        """
        Calculate wetland coverage by water projects for each field.

        CORRECT APPROACH:
        1. Fields × Wetlands: Get total wetland area within each field
        2. Fields × (Wetlands covered by water projects): Get covered wetland area within each field
        3. Calculate field-level percentages: % wetlands in field, % of those covered by water projects

        We only care about water projects that intersect with wetlands.
        """

        self.log.info("🎯 FIELD-LEVEL WETLAND WATER COVERAGE ANALYSIS")
        self.log.info("🔧 Using wetland-water project intersection geometries")
        self.log.info("✅ Following DuckDB Spatial PR #545: Single spatial predicate joins")

        # Get total field count for batching
        total_fields = self.conn.execute("SELECT COUNT(*) FROM fields_raw").fetchone()[0]
        batch_size = CONFIG.stage2_batch_size
        num_batches = (total_fields + batch_size - 1) // batch_size

        self.log.info(
            f"Processing {total_fields:,} fields in {num_batches} batches of {batch_size:,}"
        )

        # Initialize result table
        self.conn.execute("""
            CREATE OR REPLACE TABLE fields_wetland_water AS
            SELECT 
                CAST(NULL AS VARCHAR) as field_id,
                CAST(NULL AS VARCHAR) as block_id,
                CAST(NULL AS VARCHAR) as cvr_number,
                CAST(NULL AS INTEGER) as year,
                CAST(NULL AS GEOMETRY) as geometry,
                CAST(NULL AS DOUBLE) as field_area_m2,
                CAST(NULL AS DOUBLE) as total_wetland_area_m2,
                CAST(NULL AS DOUBLE) as wetland_covered_by_water_projects_m2,

                CAST(NULL AS DOUBLE) as wetland_covered_by_water_projects_pct,
                CAST(NULL AS DOUBLE) as wetland_not_covered_by_water_projects_pct,
                CAST(NULL AS DOUBLE) as field_wetland_coverage_pct
            WHERE FALSE
        """)

        total_processed = 0

        # Process each batch
        for batch_num in range(num_batches):
            offset = batch_num * batch_size
            progress_pct = ((batch_num + 1) / num_batches) * 100
            self.log.info(f"📦 Batch {batch_num + 1}/{num_batches} - {progress_pct:.1f}% complete")

            # Create field batch
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE fields_batch AS
                SELECT * FROM fields_raw
                LIMIT {batch_size} OFFSET {offset}
            """)

            batch_count = self.conn.execute("SELECT COUNT(*) FROM fields_batch").fetchone()[0]
            if batch_count == 0:
                break

            # Step 1: Fields × Wetlands (total wetland area within each field)
            self.log.info(f"  Step 1: {batch_count:,} fields × wetlands (total area)")
            self.conn.execute("""
                CREATE OR REPLACE TABLE batch_field_wetland_total AS
                SELECT 
                    f.field_id,
                    f.block_id,
                    f.cvr_number,
                    f.year,
                    f.geometry as field_geometry,
                    ST_Area_Spheroid(f.geometry) as field_area_m2,
                    w.toerv_pct,
                    ST_Area_Spheroid(ST_Intersection(f.geometry, w.geometry)) as field_wetland_area_m2
                FROM fields_batch f
                JOIN wetlands_raw w ON ST_Intersects(f.geometry, w.geometry)
                WHERE ST_Area_Spheroid(ST_Intersection(f.geometry, w.geometry)) > 100
            """)

            # Step 2: Fields × (Wetlands covered by water projects)
            self.log.info(f"  Step 2: {batch_count:,} fields × covered wetlands")
            self.conn.execute("""
                CREATE OR REPLACE TABLE batch_field_wetland_covered AS
                SELECT 
                    f.field_id,
                    f.block_id,
                    f.cvr_number,
                    f.year,
                    wc.toerv_pct,
                    ST_Area_Spheroid(ST_Intersection(f.geometry, wc.covered_wetland_geometry)) as field_covered_wetland_area_m2
                FROM fields_batch f
                JOIN wetlands_covered_by_water wc ON ST_Intersects(f.geometry, wc.covered_wetland_geometry)
                WHERE ST_Area_Spheroid(ST_Intersection(f.geometry, wc.covered_wetland_geometry)) > 100
            """)

            total_wetland_intersections = self.conn.execute(
                "SELECT COUNT(*) FROM batch_field_wetland_total"
            ).fetchone()[0]
            covered_wetland_intersections = self.conn.execute(
                "SELECT COUNT(*) FROM batch_field_wetland_covered"
            ).fetchone()[0]

            self.log.info(
                f"  Found {total_wetland_intersections:,} total field-wetland intersections"
            )
            self.log.info(
                f"  Found {covered_wetland_intersections:,} field-covered wetland intersections"
            )

            if total_wetland_intersections == 0:
                # Handle fields with no wetlands
                self.conn.execute("""
                    INSERT INTO fields_wetland_water
                    SELECT 
                        field_id, block_id, cvr_number, year, geometry, 
                        ST_Area_Spheroid(geometry) as field_area_m2,
                        0 as total_wetland_area_m2, 
                        0 as wetland_covered_by_water_projects_m2,
                        0 as wetland_covered_by_water_projects_pct,
                        0 as wetland_not_covered_by_water_projects_pct, 
                        0 as field_wetland_coverage_pct
                    FROM fields_batch
                """)
                total_processed += batch_count
                continue

            # Step 3: Aggregate per field and calculate percentages
            self.log.info("  Step 3: Aggregating and calculating field-level percentages")
            self.conn.execute("""
                INSERT INTO fields_wetland_water
                SELECT 
                    t.field_id,
                    t.block_id,
                    t.cvr_number,
                    t.year,
                    t.field_geometry as geometry,
                    t.field_area_m2,
                    
                    -- Total wetland area within field
                    SUM(t.field_wetland_area_m2) as total_wetland_area_m2,
                    
                    -- Wetland area covered by water projects within field
                    COALESCE(SUM(c.field_covered_wetland_area_m2), 0) as wetland_covered_by_water_projects_m2,
                    
                    -- Calculate percentages
                    CASE 
                        WHEN SUM(t.field_wetland_area_m2) > 0 
                        THEN (COALESCE(SUM(c.field_covered_wetland_area_m2), 0) / SUM(t.field_wetland_area_m2)) * 100
                        ELSE 0 
                    END as wetland_covered_by_water_projects_pct,
                    
                    CASE 
                        WHEN SUM(t.field_wetland_area_m2) > 0 
                        THEN (100 - (COALESCE(SUM(c.field_covered_wetland_area_m2), 0) / SUM(t.field_wetland_area_m2)) * 100)
                        ELSE 0 
                    END as wetland_not_covered_by_water_projects_pct,
                    
                    (SUM(t.field_wetland_area_m2) / t.field_area_m2) * 100 as field_wetland_coverage_pct
                    
                FROM batch_field_wetland_total t
                LEFT JOIN batch_field_wetland_covered c 
                    ON t.field_id = c.field_id 
                    AND t.block_id = c.block_id 
                    AND t.cvr_number = c.cvr_number
                    AND t.toerv_pct = c.toerv_pct
                GROUP BY 
                    t.field_id, t.block_id, t.cvr_number, t.year, t.field_geometry, t.field_area_m2
            """)

            batch_processed = self.conn.execute(
                "SELECT COUNT(*) FROM fields_wetland_water WHERE field_id IN (SELECT field_id FROM fields_batch)"
            ).fetchone()[0]
            total_processed += batch_processed

            self.log.info(f"  ✅ Batch {batch_num + 1}: {batch_processed:,} fields processed")

            # Clean up batch tables
            self.conn.execute("DROP TABLE IF EXISTS fields_batch")
            self.conn.execute("DROP TABLE IF EXISTS batch_field_wetland_total")
            self.conn.execute("DROP TABLE IF EXISTS batch_field_wetland_covered")

            # Memory cleanup every few batches
            if (batch_num + 1) % CONFIG.memory_cleanup_frequency == 0:
                import gc

                gc.collect()

        # Final count
        final_count = self.conn.execute("SELECT COUNT(*) FROM fields_wetland_water").fetchone()[0]
        self.log.info(f"✅ Processed {final_count:,} fields for wetland water coverage analysis")

        return {
            "total_fields": final_count,
        }

    def _save_output_data(self, result: Dict[str, Any]):
        """Save fields with wetland water coverage to GCS."""
        self._save_stage_output("fields_wetland_water", "fields_wetland_water")
