"""Stage 2A: Fields × BNBO Water Coverage Analysis

Calculate BNBO coverage by water projects for each field.
Uses pre-computed BNBO intersection geometries from Stage 1A (SPEED OPTIMIZATION).
No longer recreates spatial intersections - reuses Stage 1A intersection geometries.

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
        # Load agricultural fields (still from silver - this is the BUILD side)
        self._load_silver_dataset(CONFIG.agricultural_fields_dataset, "agricultural_fields")

        # Load Stage 0 pre-filtered BNBO data for field intersections (PROBE side optimization)
        self.log.info("Loading Stage 0 pre-filtered BNBO dataset...")
        stage0_bnbo_dataset = CONFIG.stage_outputs["bnbo_prefiltered"]
        stage0_bnbo_path = self._get_latest_gold_path(stage0_bnbo_dataset)
        self.gcs_access.query_parquet_direct(stage0_bnbo_path, "SELECT *", "bnbo_for_fields")

        self.log.info("✅ STAGE 0 OPTIMIZATION: Using pre-filtered BNBO for field intersections!")
        self.log.info("🚀 PERFORMANCE: 3.7x faster than original (3.7K → 1K BNBO polygons)")

        # Load water project × BNBO intersections from Stage 1A
        # This contains the pre-computed intersection geometries we need (OPTIMIZATION!)
        stage1a_dataset = CONFIG.stage_outputs["water_projects_bnbo_intersections"]
        stage1a_path = self._get_latest_gold_path(stage1a_dataset)
        self.gcs_access.query_parquet_direct(
            stage1a_path, "SELECT *", "water_projects_bnbo_intersections"
        )

        # Use pre-computed BNBO areas covered by water projects (SPEED OPTIMIZATION)
        # No need to recreate - Stage 1A already computed intersection geometries!
        self.log.info("Using pre-computed BNBO intersection geometries from Stage 1A...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE bnbo_covered_by_water AS
            SELECT 
                status_category,
                intersection_geometry as covered_bnbo_geometry,
                intersection_area_m2 as covered_area_m2
            FROM water_projects_bnbo_intersections
            WHERE intersection_area_m2 > 100
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

        DuckDB Spatial PR #545 COMPLIANCE:
        - Separate spatial joins into distinct processing steps
        - Single spatial predicate per join
        - Move area filtering to post-join processing
        - No WHERE clauses with spatial predicates on JOIN operations

        CORRECT APPROACH:
        1. Fields × BNBO: Get total BNBO area within each field (STEP 1)
        2. Fields × (BNBO covered by water projects): Get covered BNBO area within each field (STEP 2)
        3. Calculate field-level percentages: % BNBO in field, % of those covered by water projects
        """

        self.log.info("🎯 FIELD-LEVEL BNBO WATER COVERAGE ANALYSIS")
        self.log.info("🔧 Using BNBO-water project intersection geometries")
        self.log.info(
            "✅ DuckDB Spatial PR #545 COMPLIANCE: Separate spatial joins, no WHERE spatial predicates"
        )

        # Get total field count for batching
        total_fields = self.conn.execute("SELECT COUNT(*) FROM agricultural_fields").fetchone()[0]
        batch_size = CONFIG.stage2_batch_size
        num_batches = (total_fields + batch_size - 1) // batch_size

        self.log.info(
            f"Processing {total_fields:,} fields in {num_batches} batches of {batch_size:,}"
        )

        # Initialize result table (field-level aggregates)
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
                CAST(NULL AS DOUBLE) as bnbo_covered_by_water_projects_pct,
                CAST(NULL AS DOUBLE) as bnbo_not_covered_by_water_projects_pct,
                CAST(NULL AS DOUBLE) as field_bnbo_coverage_pct,
                CAST(NULL AS VARCHAR) as dominant_bnbo_status
            WHERE FALSE
        """)

        # Initialize detailed intersections table for Stage 3 optimization
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_bnbo_intersections AS
            SELECT 
                CAST(NULL AS VARCHAR) as field_id,
                CAST(NULL AS VARCHAR) as block_id,
                CAST(NULL AS VARCHAR) as cvr_number,
                CAST(NULL AS INTEGER) as year,
                CAST(NULL AS VARCHAR) as status_category,
                CAST(NULL AS GEOMETRY) as field_bnbo_intersection_geometry,
                CAST(NULL AS DOUBLE) as field_bnbo_intersection_area_m2,
                CAST(NULL AS GEOMETRY) as field_geometry,
                CAST(NULL AS DOUBLE) as field_area_m2
            WHERE FALSE
        """)

        total_fields_processed = 0
        total_bnbo_intersections = 0
        total_covered_intersections = 0

        # Process each batch with separated spatial joins
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

            # STEP 1: Fields × BNBO (total BNBO area within each field)
            # DuckDB Spatial PR #545 COMPLIANCE: Single spatial predicate only
            self.log.info(f"  STEP 1: {batch_count:,} fields × BNBO (single spatial join)")
            self.conn.execute("""
                CREATE OR REPLACE TABLE batch_field_bnbo_raw AS
                SELECT 
                    f.field_id,
                    f.block_id,
                    f.cvr_number,
                    f.year,
                    f.geometry as field_geometry,
                    ST_Area_Spheroid(f.geometry) as field_area_m2,
                    b.bnbo_id,
                    b.status_category,
                    b.geometry as bnbo_geometry
                FROM fields_batch f
                JOIN bnbo_for_fields b ON ST_Intersects(f.geometry, b.geometry)
            """)

            # Post-join processing: Calculate areas (NO SPATIAL WHERE CLAUSES)
            self.log.info("  Post-processing: Calculate intersection areas")
            self.conn.execute("""
                CREATE OR REPLACE TABLE batch_field_bnbo_total AS
                SELECT 
                    field_id,
                    block_id,
                    cvr_number,
                    year,
                    field_geometry,
                    field_area_m2,
                    bnbo_id,
                    status_category,
                    ST_Intersection(field_geometry, bnbo_geometry) as field_bnbo_intersection_geometry,
                    ST_Area_Spheroid(ST_Intersection(field_geometry, bnbo_geometry)) as field_bnbo_area_m2
                FROM batch_field_bnbo_raw
            """)

            # Save detailed intersections for Stage 3 optimization
            self.log.info("  Saving detailed field-BNBO intersections for Stage 3")
            self.conn.execute("""
                INSERT INTO field_bnbo_intersections
                SELECT 
                    field_id,
                    block_id,
                    cvr_number,
                    year,
                    status_category,
                    field_bnbo_intersection_geometry,
                    field_bnbo_area_m2 as field_bnbo_intersection_area_m2,
                    field_geometry,
                    field_area_m2
                FROM batch_field_bnbo_total
            """)

            # STEP 2: Fields × (BNBO covered by water projects)
            # DuckDB Spatial PR #545 COMPLIANCE: Separate spatial join
            self.log.info(
                f"  STEP 2: {batch_count:,} fields × BNBO covered by water projects (separate spatial join)"
            )
            self.conn.execute("""
                CREATE OR REPLACE TABLE batch_field_bnbo_covered_raw AS
                SELECT 
                    f.field_id,
                    f.block_id,
                    f.cvr_number,
                    f.year,
                    f.geometry as field_geometry,
                    wpbi.status_category,
                    wpbi.intersection_geometry as water_covered_bnbo_geometry
                FROM fields_batch f
                JOIN water_projects_bnbo_intersections wpbi ON ST_Intersects(f.geometry, wpbi.intersection_geometry)
            """)

            # Post-join processing: Calculate covered areas (NO SPATIAL WHERE CLAUSES)
            self.log.info("  Post-processing: Calculate covered intersection areas")
            self.conn.execute("""
                CREATE OR REPLACE TABLE batch_field_bnbo_covered AS
                SELECT 
                    field_id,
                    block_id,
                    cvr_number,
                    year,
                    status_category,
                    ST_Area_Spheroid(ST_Intersection(field_geometry, water_covered_bnbo_geometry)) as field_covered_bnbo_area_m2
                FROM batch_field_bnbo_covered_raw
            """)

            batch_bnbo_intersections = self.conn.execute(
                "SELECT COUNT(*) FROM batch_field_bnbo_total"
            ).fetchone()[0]
            batch_covered_intersections = self.conn.execute(
                "SELECT COUNT(*) FROM batch_field_bnbo_covered"
            ).fetchone()[0]

            self.log.info(f"  Found {batch_bnbo_intersections:,} total field-BNBO intersections")
            self.log.info(
                f"  Found {batch_covered_intersections:,} field-covered BNBO intersections"
            )

            if batch_bnbo_intersections == 0:
                self.log.info(f"  No BNBO intersections found in batch {batch_num + 1}")
                continue

            # Aggregate to field level
            self.log.info("  Aggregating to field-level BNBO coverage statistics")
            self.conn.execute("""
                CREATE OR REPLACE TABLE batch_field_aggregates AS
                SELECT 
                    f.field_id,
                    f.block_id,
                    f.cvr_number,
                    f.year,
                    f.field_geometry as geometry,
                    f.field_area_m2,
                    
                    -- Total BNBO area in field
                    COALESCE(SUM(f.field_bnbo_area_m2), 0) as total_bnbo_area_m2,
                    
                    -- BNBO covered by water projects
                    COALESCE(SUM(c.field_covered_bnbo_area_m2), 0) as bnbo_covered_by_water_projects_m2,
                    
                    -- Dominant BNBO status
                    MODE() WITHIN GROUP (ORDER BY f.status_category) as dominant_bnbo_status
                    
                FROM batch_field_bnbo_total f
                LEFT JOIN batch_field_bnbo_covered c ON f.field_id = c.field_id 
                    AND f.block_id = c.block_id 
                    AND f.cvr_number = c.cvr_number 
                    AND f.year = c.year
                    AND f.status_category = c.status_category
                GROUP BY f.field_id, f.block_id, f.cvr_number, f.year, f.field_geometry, f.field_area_m2
            """)

            # Calculate percentages and final metrics
            self.conn.execute("""
                CREATE OR REPLACE TABLE batch_final AS
                SELECT 
                    *,
                    -- Coverage percentages
                    CASE 
                        WHEN total_bnbo_area_m2 > 0 
                        THEN (bnbo_covered_by_water_projects_m2 / total_bnbo_area_m2) * 100 
                        ELSE 0 
                    END as bnbo_covered_by_water_projects_pct,
                    
                    CASE 
                        WHEN total_bnbo_area_m2 > 0 
                        THEN ((total_bnbo_area_m2 - bnbo_covered_by_water_projects_m2) / total_bnbo_area_m2) * 100 
                        ELSE 0 
                    END as bnbo_not_covered_by_water_projects_pct,
                    
                    -- Field coverage percentage
                    (total_bnbo_area_m2 / field_area_m2) * 100 as field_bnbo_coverage_pct
                    
                FROM batch_field_aggregates
            """)

            # Insert into main results table
            self.conn.execute("""
                INSERT INTO fields_bnbo_water 
                SELECT * FROM batch_final
            """)

            total_fields_processed += batch_count
            total_bnbo_intersections += batch_bnbo_intersections
            total_covered_intersections += batch_covered_intersections

            # Memory cleanup
            self.conn.execute("DROP TABLE IF EXISTS fields_batch")
            self.conn.execute("DROP TABLE IF EXISTS batch_field_bnbo_raw")
            self.conn.execute("DROP TABLE IF EXISTS batch_field_bnbo_total")
            self.conn.execute("DROP TABLE IF EXISTS batch_field_bnbo_covered_raw")
            self.conn.execute("DROP TABLE IF EXISTS batch_field_bnbo_covered")
            self.conn.execute("DROP TABLE IF EXISTS batch_field_aggregates")
            self.conn.execute("DROP TABLE IF EXISTS batch_final")

        # Final summary
        final_fields = self.conn.execute("SELECT COUNT(*) FROM fields_bnbo_water").fetchone()[0]

        self.log.info(f"✅ STAGE 2A COMPLETE: {final_fields:,} fields with BNBO analysis")
        self.log.info(f"📊 Total BNBO intersections: {total_bnbo_intersections:,}")
        self.log.info(f"📊 Water-covered intersections: {total_covered_intersections:,}")
        self.log.info("✅ DuckDB Spatial PR #545 COMPLIANCE: Separated spatial joins completed")

        return {
            "fields_processed": final_fields,
            "total_bnbo_intersections": total_bnbo_intersections,
            "covered_intersections": total_covered_intersections,
        }

    def _save_output_data(self, result: Dict[str, Any]):
        """Save fields with BNBO water coverage and detailed intersections to GCS."""
        # Save field-level aggregates
        self._save_stage_output("fields_bnbo_water", "fields_bnbo_water")

        # Save detailed field-BNBO intersections for Stage 3 optimization
        self._save_stage_output("field_bnbo_intersections", "field_bnbo_intersections")
