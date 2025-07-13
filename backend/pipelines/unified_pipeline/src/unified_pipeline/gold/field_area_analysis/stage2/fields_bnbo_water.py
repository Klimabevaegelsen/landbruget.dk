"""Stage 2A: Fields × BNBO Water Coverage Analysis

Calculate BNBO coverage by water projects for each field.
Uses pre-computed water project intersections from Stage 1A and fields from Stage 1D.

Optimized for DuckDB Spatial v1.2.2 with foundation data approach.
Based on successful Stage 1 implementations.
"""

from typing import Any, Dict

from ..base import FieldAnalysisStageBase, FieldAnalysisStageConfig
from ..config import CONFIG


class FieldsBNBOWaterCoverage(FieldAnalysisStageBase):
    """Calculate BNBO coverage by water projects for each field using foundation data."""

    def __init__(self, config: FieldAnalysisStageConfig = None):
        if config is None:
            config = FieldAnalysisStageConfig()
        super().__init__(config, "Stage 2A: Fields × BNBO Water Coverage")

    def _load_input_data(self):
        """Load field data and water project BNBO intersections from Stage 1."""
        # Load agricultural fields
        self._load_silver_dataset(CONFIG.agricultural_fields_dataset, "agricultural_fields")

        # Load BNBO status data for field intersections
        self._load_silver_dataset(CONFIG.bnbo_status_dataset, "bnbo_for_fields")

        # Load water project × BNBO intersections from Stage 1A
        # This contains the actual intersection data we need
        stage1a_dataset = CONFIG.stage_outputs["water_projects_bnbo_intersections"]
        stage1a_path = self._get_latest_gold_path(stage1a_dataset)
        self.gcs_access.query_parquet_direct(
            stage1a_path, "SELECT *", "water_projects_bnbo_intersections"
        )

        # We need the actual BNBO areas covered by water projects
        # But Stage 1A only saved statistics, not geometries. We need to recreate the covered BNBO areas.
        # Load water projects to recreate intersection geometries
        self._load_silver_dataset(CONFIG.water_projects_dataset, "water_projects_raw")

        # Create the actual BNBO areas covered by water projects
        self.log.info("Creating BNBO areas covered by water projects...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE bnbo_covered_by_water AS
            SELECT 
                b.status_category,
                ST_Intersection(b.geometry, wp.geometry) as covered_bnbo_geometry,
                ST_Area_Spheroid(ST_Intersection(b.geometry, wp.geometry)) as covered_area_m2
            FROM bnbo_for_fields b
            JOIN water_projects_raw wp ON ST_Intersects(b.geometry, wp.geometry)
            WHERE ST_Area_Spheroid(ST_Intersection(b.geometry, wp.geometry)) > 100
        """)

        # Log loaded data
        fields_count = self.conn.execute("SELECT COUNT(*) FROM agricultural_fields").fetchone()[0]
        bnbo_count = self.conn.execute("SELECT COUNT(*) FROM bnbo_for_fields").fetchone()[0]
        covered_count = self.conn.execute("SELECT COUNT(*) FROM bnbo_covered_by_water").fetchone()[
            0
        ]
        intersections_count = self.conn.execute(
            "SELECT COUNT(*) FROM water_projects_bnbo_intersections"
        ).fetchone()[0]

        self.log.info("✅ Loaded data for field-level analysis:")
        self.log.info(f"   Agricultural fields: {fields_count:,}")
        self.log.info(f"   BNBO polygons: {bnbo_count:,}")
        self.log.info(f"   BNBO areas covered by water projects: {covered_count:,}")
        self.log.info(f"   Intersection records (for validation): {intersections_count:,}")

    async def _execute_stage_processing(self) -> Dict[str, Any]:
        """
        Calculate BNBO coverage by water projects for each field.

        CORRECT APPROACH:
        1. Fields × BNBO: Get total BNBO area within each field
        2. Fields × (BNBO covered by water projects): Get covered BNBO area within each field
        3. Calculate field-level percentages: % BNBO in field, % of those covered by water projects

        We only care about water projects that intersect with BNBO areas.
        """

        self.log.info("🎯 FIELD-LEVEL BNBO WATER COVERAGE ANALYSIS")
        self.log.info("🔧 Using BNBO-water project intersection geometries")
        self.log.info("✅ Following DuckDB Spatial PR #545: Single spatial predicate joins")

        # Get total field count for batching
        total_fields = self.conn.execute("SELECT COUNT(*) FROM agricultural_fields").fetchone()[0]
        batch_size = CONFIG.batch_size
        num_batches = (total_fields + batch_size - 1) // batch_size

        self.log.info(
            f"Processing {total_fields:,} fields in {num_batches} batches of {batch_size:,}"
        )

        # Initialize result table
        self.conn.execute("""
            CREATE OR REPLACE TABLE fields_bnbo_water AS
            SELECT 
                CAST(NULL AS VARCHAR) as field_id,
                CAST(NULL AS VARCHAR) as block_id,
                CAST(NULL AS VARCHAR) as cvr_number,
                CAST(NULL AS INTEGER) as year,
                CAST(NULL AS GEOMETRY) as geometry,
                CAST(NULL AS DOUBLE) as field_area_m2,
                CAST(NULL AS DOUBLE) as total_bnbo_area_m2,
                CAST(NULL AS DOUBLE) as bnbo_covered_by_water_projects_m2,
                CAST(NULL AS VARCHAR) as dominant_bnbo_status,
                CAST(NULL AS DOUBLE) as bnbo_covered_by_water_projects_pct,
                CAST(NULL AS DOUBLE) as bnbo_not_covered_by_water_projects_pct,
                CAST(NULL AS DOUBLE) as field_bnbo_coverage_pct
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
                SELECT * FROM agricultural_fields
                LIMIT {batch_size} OFFSET {offset}
            """)

            batch_count = self.conn.execute("SELECT COUNT(*) FROM fields_batch").fetchone()[0]
            if batch_count == 0:
                break

            # Step 1: Fields × BNBO (total BNBO area within each field)
            self.log.info(f"  Step 1: {batch_count:,} fields × BNBO (total area)")
            self.conn.execute("""
                CREATE OR REPLACE TABLE batch_field_bnbo_total AS
                SELECT 
                    f.field_id,
                    f.block_id,
                    f.cvr_number,
                    f.year,
                    f.geometry as field_geometry,
                    ST_Area_Spheroid(f.geometry) as field_area_m2,
                    b.status_category,
                    ST_Area_Spheroid(ST_Intersection(f.geometry, b.geometry)) as field_bnbo_area_m2
                FROM fields_batch f
                JOIN bnbo_for_fields b ON ST_Intersects(f.geometry, b.geometry)
                WHERE ST_Area_Spheroid(ST_Intersection(f.geometry, b.geometry)) > 100
            """)

            # Step 2: Fields × (BNBO covered by water projects)
            self.log.info(f"  Step 2: {batch_count:,} fields × covered BNBO")
            self.conn.execute("""
                CREATE OR REPLACE TABLE batch_field_bnbo_covered AS
                SELECT 
                    f.field_id,
                    f.block_id,
                    f.cvr_number,
                    f.year,
                    bc.status_category,
                    ST_Area_Spheroid(ST_Intersection(f.geometry, bc.covered_bnbo_geometry)) as field_covered_bnbo_area_m2
                FROM fields_batch f
                JOIN bnbo_covered_by_water bc ON ST_Intersects(f.geometry, bc.covered_bnbo_geometry)
                WHERE ST_Area_Spheroid(ST_Intersection(f.geometry, bc.covered_bnbo_geometry)) > 100
            """)

            total_bnbo_intersections = self.conn.execute(
                "SELECT COUNT(*) FROM batch_field_bnbo_total"
            ).fetchone()[0]
            covered_bnbo_intersections = self.conn.execute(
                "SELECT COUNT(*) FROM batch_field_bnbo_covered"
            ).fetchone()[0]

            self.log.info(f"  Found {total_bnbo_intersections:,} total field-BNBO intersections")
            self.log.info(
                f"  Found {covered_bnbo_intersections:,} field-covered BNBO intersections"
            )

            if total_bnbo_intersections == 0:
                # Handle fields with no BNBO
                self.conn.execute("""
                    INSERT INTO fields_bnbo_water
                    SELECT 
                        field_id, block_id, cvr_number, year, geometry, 
                        ST_Area_Spheroid(geometry) as field_area_m2,
                        0 as total_bnbo_area_m2, 
                        0 as bnbo_covered_by_water_projects_m2, 
                        NULL as dominant_bnbo_status, 
                        0 as bnbo_covered_by_water_projects_pct,
                        0 as bnbo_not_covered_by_water_projects_pct, 
                        0 as field_bnbo_coverage_pct
                    FROM fields_batch
                """)
                total_processed += batch_count
                continue

            # Step 3: Aggregate per field and calculate percentages
            self.log.info("  Step 3: Aggregating and calculating field-level percentages")
            self.conn.execute("""
                INSERT INTO fields_bnbo_water
                SELECT 
                    t.field_id,
                    t.block_id,
                    t.cvr_number,
                    t.year,
                    t.field_geometry as geometry,
                    t.field_area_m2,
                    
                    -- Total BNBO area within field
                    SUM(t.field_bnbo_area_m2) as total_bnbo_area_m2,
                    
                    -- BNBO area covered by water projects within field
                    COALESCE(SUM(c.field_covered_bnbo_area_m2), 0) as bnbo_covered_by_water_projects_m2,
                    
                    -- Dominant BNBO status (largest area)
                    (
                        SELECT status_category 
                        FROM batch_field_bnbo_total t2 
                        WHERE t2.field_id = t.field_id AND t2.block_id = t.block_id AND t2.cvr_number = t.cvr_number
                        ORDER BY t2.field_bnbo_area_m2 DESC
                        LIMIT 1
                    ) as dominant_bnbo_status,
                    
                    -- Calculate percentages
                    CASE 
                        WHEN SUM(t.field_bnbo_area_m2) > 0 
                        THEN (COALESCE(SUM(c.field_covered_bnbo_area_m2), 0) / SUM(t.field_bnbo_area_m2)) * 100
                        ELSE 0 
                    END as bnbo_covered_by_water_projects_pct,
                    
                    CASE 
                        WHEN SUM(t.field_bnbo_area_m2) > 0 
                        THEN (100 - (COALESCE(SUM(c.field_covered_bnbo_area_m2), 0) / SUM(t.field_bnbo_area_m2)) * 100)
                        ELSE 0 
                    END as bnbo_not_covered_by_water_projects_pct,
                    
                    (SUM(t.field_bnbo_area_m2) / t.field_area_m2) * 100 as field_bnbo_coverage_pct
                    
                FROM batch_field_bnbo_total t
                LEFT JOIN batch_field_bnbo_covered c 
                    ON t.field_id = c.field_id 
                    AND t.block_id = c.block_id 
                    AND t.cvr_number = c.cvr_number
                    AND t.status_category = c.status_category
                GROUP BY 
                    t.field_id, t.block_id, t.cvr_number, t.year, t.field_geometry, t.field_area_m2
            """)

            batch_processed = self.conn.execute(
                "SELECT COUNT(*) FROM fields_bnbo_water WHERE field_id IN (SELECT field_id FROM fields_batch)"
            ).fetchone()[0]
            total_processed += batch_processed

            self.log.info(f"  ✅ Batch {batch_num + 1}: {batch_processed:,} fields processed")

            # Clean up batch tables
            self.conn.execute("DROP TABLE IF EXISTS fields_batch")
            self.conn.execute("DROP TABLE IF EXISTS batch_field_bnbo_total")
            self.conn.execute("DROP TABLE IF EXISTS batch_field_bnbo_covered")

            # Memory cleanup every few batches
            if (batch_num + 1) % CONFIG.memory_cleanup_frequency == 0:
                import gc

                gc.collect()

        # Final count
        final_count = self.conn.execute("SELECT COUNT(*) FROM fields_bnbo_water").fetchone()[0]
        self.log.info(f"✅ Processed {final_count:,} fields for BNBO water coverage analysis")

        return {
            "total_fields": final_count,
        }

    def _save_output_data(self, result: Dict[str, Any]):
        """Save fields with BNBO water coverage to GCS."""
        self._save_stage_output("fields_bnbo_water", "fields_bnbo_water")
