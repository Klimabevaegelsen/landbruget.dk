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
        """Load agricultural fields and pre-computed foundation data from Stage 1."""
        # Load agricultural fields directly from silver
        self._load_silver_dataset(CONFIG.agricultural_fields_dataset, "agricultural_fields")

        # Load pre-computed water project-BNBO intersections from Stage 1A (foundation data)
        stage1a_path = f"gs://{CONFIG.bucket}/gold/{CONFIG.stage_outputs['water_projects_bnbo_intersections']}/{CONFIG.stage_outputs['water_projects_bnbo_intersections']}.parquet"
        self.gcs_access.query_parquet_direct(
            stage1a_path, "SELECT *", "water_projects_bnbo_intersections"
        )

        # Load BNBO data only for field intersections (minimal data needed)
        self._load_silver_dataset(CONFIG.bnbo_dataset, "bnbo_raw")
        self.conn.execute("""
            CREATE OR REPLACE TABLE bnbo_for_fields AS
            SELECT 
                id as bnbo_id,
                status_category,
                geometry
            FROM bnbo_raw
        """)

        # Log loaded data
        fields_count = self.conn.execute("SELECT COUNT(*) FROM agricultural_fields").fetchone()[0]
        intersections_count = self.conn.execute(
            "SELECT COUNT(*) FROM water_projects_bnbo_intersections"
        ).fetchone()[0]
        bnbo_count = self.conn.execute("SELECT COUNT(*) FROM bnbo_for_fields").fetchone()[0]

        self.log.info("✅ Loaded foundation data:")
        self.log.info(f"   Agricultural fields: {fields_count:,}")
        self.log.info(f"   Water project-BNBO intersections (foundation): {intersections_count:,}")
        self.log.info(f"   BNBO polygons: {bnbo_count:,}")

    async def _execute_stage_processing(self) -> Dict[str, Any]:
        """
        Calculate BNBO coverage by water projects for each field using foundation data.

        FOUNDATION DATA APPROACH (PR #545 compliant):
        1. Single spatial join: Fields × BNBO (single predicate: ST_Intersects)
        2. Use pre-computed water project coverage from Stage 1A
        3. Scale coverage proportionally to field intersection area
        """

        self.log.info("🎯 FOUNDATION DATA APPROACH: Field-level BNBO water coverage")
        self.log.info("🔧 Using pre-computed intersections from Stage 1A")
        self.log.info("✅ Following DuckDB Spatial PR #545: Single spatial predicate joins")

        # Get total field count for batching
        total_fields = self.conn.execute("SELECT COUNT(*) FROM agricultural_fields").fetchone()[0]
        batch_size = CONFIG.stage3_batch_size
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

        # Process each batch using foundation data approach
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

            # SINGLE SPATIAL JOIN: Fields × BNBO (PR #545 compliant)
            self.log.info(f"  Single spatial join: {batch_count:,} fields × BNBO")
            self.conn.execute("""
                CREATE OR REPLACE TABLE batch_field_bnbo AS
                SELECT 
                    f.field_id,
                    f.block_id,
                    f.cvr_number,
                    f.year,
                    f.geometry as field_geometry,
                    ST_Area_Spheroid(f.geometry) as field_area_m2,
                    b.bnbo_id,
                    b.status_category,
                    ST_Area_Spheroid(ST_Intersection(f.geometry, b.geometry)) as field_bnbo_area_m2
                FROM fields_batch f
                JOIN bnbo_for_fields b ON ST_Intersects(f.geometry, b.geometry)
                WHERE ST_Area_Spheroid(ST_Intersection(f.geometry, b.geometry)) > 100
            """)

            field_bnbo_count = self.conn.execute(
                "SELECT COUNT(*) FROM batch_field_bnbo"
            ).fetchone()[0]
            self.log.info(f"  Found {field_bnbo_count:,} field-BNBO intersections")

            if field_bnbo_count == 0:
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

            # Apply foundation data to calculate water project coverage
            self.log.info("  Applying foundation data for water project coverage")
            self.conn.execute("""
                INSERT INTO fields_bnbo_water
                SELECT 
                    field_id,
                    block_id,
                    cvr_number,
                    year,
                    field_geometry as geometry,
                    field_area_m2,
                    
                    -- Total BNBO area within field
                    SUM(field_bnbo_area_m2) as total_bnbo_area_m2,
                    
                    -- BNBO area covered by water projects (using foundation data)
                    SUM(COALESCE(wpi.intersection_area_m2, 0)) as bnbo_covered_by_water_projects_m2,
                    
                    -- Dominant BNBO status (largest area)
                    (
                        SELECT status_category 
                        FROM batch_field_bnbo fb2 
                        WHERE fb2.field_id = fb.field_id AND fb2.block_id = fb.block_id AND fb2.cvr_number = fb.cvr_number
                        ORDER BY fb2.field_bnbo_area_m2 DESC
                        LIMIT 1
                    ) as dominant_bnbo_status,
                    
                    -- Calculate percentages
                    CASE 
                        WHEN SUM(field_bnbo_area_m2) > 0 
                        THEN (SUM(COALESCE(wpi.intersection_area_m2, 0)) / SUM(field_bnbo_area_m2)) * 100
                        ELSE 0 
                    END as bnbo_covered_by_water_projects_pct,
                    
                    CASE 
                        WHEN SUM(field_bnbo_area_m2) > 0 
                        THEN ((SUM(field_bnbo_area_m2) - SUM(COALESCE(wpi.intersection_area_m2, 0))) / SUM(field_bnbo_area_m2)) * 100
                        ELSE 0 
                    END as bnbo_not_covered_by_water_projects_pct,
                    
                    (SUM(field_bnbo_area_m2) / field_area_m2) * 100 as field_bnbo_coverage_pct
                    
                FROM batch_field_bnbo fb
                LEFT JOIN water_projects_bnbo_intersections wpi 
                    ON fb.status_category = wpi.status_category
                GROUP BY 
                    field_id, block_id, cvr_number, year, field_geometry, field_area_m2
            """)

            batch_processed = self.conn.execute(
                "SELECT COUNT(*) FROM fields_bnbo_water WHERE field_id IN (SELECT field_id FROM fields_batch)"
            ).fetchone()[0]
            total_processed += batch_processed

            self.log.info(f"  ✅ Batch {batch_num + 1}: {batch_processed:,} fields processed")

            # Clean up batch tables
            self.conn.execute("DROP TABLE IF EXISTS fields_batch")
            self.conn.execute("DROP TABLE IF EXISTS batch_field_bnbo")

            # Memory cleanup every few batches
            if (batch_num + 1) % CONFIG.memory_cleanup_frequency == 0:
                import gc

                gc.collect()

        # Final statistics
        final_count = self.conn.execute("SELECT COUNT(*) FROM fields_bnbo_water").fetchone()[0]
        coverage_stats = self.conn.execute("""
            SELECT 
                COUNT(*) as total_fields,
                COUNT(CASE WHEN total_bnbo_area_m2 > 0 THEN 1 END) as fields_with_bnbo,
                AVG(CASE WHEN total_bnbo_area_m2 > 0 THEN field_bnbo_coverage_pct END) as avg_bnbo_coverage,
                AVG(CASE WHEN total_bnbo_area_m2 > 0 THEN bnbo_covered_by_water_projects_pct END) as avg_water_project_coverage
            FROM fields_bnbo_water
        """).fetchone()

        total_fields, fields_with_bnbo, avg_bnbo_coverage, avg_water_project_coverage = (
            coverage_stats
        )

        self.log.info("✅ BNBO water coverage analysis completed:")
        self.log.info(f"   Total fields processed: {total_fields:,}")
        self.log.info(
            f"   Fields with BNBO: {fields_with_bnbo:,} ({(fields_with_bnbo / total_fields) * 100:.1f}%)"
        )
        if avg_bnbo_coverage:
            self.log.info(f"   Average BNBO coverage: {avg_bnbo_coverage:.1f}%")
        if avg_water_project_coverage:
            self.log.info(f"   Average water project coverage: {avg_water_project_coverage:.1f}%")

        return {
            "total_fields": total_fields,
            "fields_with_bnbo": fields_with_bnbo,
            "avg_bnbo_coverage": avg_bnbo_coverage,
            "avg_water_project_coverage": avg_water_project_coverage,
        }

    def _save_output_data(self, result: Dict[str, Any]):
        """Save fields with BNBO water coverage to GCS."""
        self._save_stage_output("fields_bnbo_water", "fields_bnbo_water")
