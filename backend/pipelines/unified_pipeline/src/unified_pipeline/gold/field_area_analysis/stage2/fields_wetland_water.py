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
        """Load agricultural fields and pre-computed foundation data from Stage 1."""
        # Load agricultural fields directly from silver
        self._load_silver_dataset(CONFIG.agricultural_fields_dataset, "agricultural_fields")

        # Load pre-computed water project-wetland intersections from Stage 1B (foundation data)
        stage1b_path = f"gs://{CONFIG.bucket}/gold/{CONFIG.stage_outputs['water_projects_wetlands_intersections']}/{CONFIG.stage_outputs['water_projects_wetlands_intersections']}.parquet"
        self.gcs_access.query_parquet_direct(
            stage1b_path, "SELECT *", "water_projects_wetlands_intersections"
        )

        # Load wetlands data only for field intersections (minimal data needed)
        self._load_silver_dataset(CONFIG.wetlands_dataset, "wetlands_raw")
        self.conn.execute("""
            CREATE OR REPLACE TABLE wetlands_for_fields AS
            SELECT 
                id as wetland_id,
                gridcode,
                toerv_pct,
                geometry
            FROM wetlands_raw
        """)

        # Log loaded data
        fields_count = self.conn.execute("SELECT COUNT(*) FROM agricultural_fields").fetchone()[0]
        intersections_count = self.conn.execute(
            "SELECT COUNT(*) FROM water_projects_wetlands_intersections"
        ).fetchone()[0]
        wetlands_count = self.conn.execute("SELECT COUNT(*) FROM wetlands_for_fields").fetchone()[0]

        self.log.info("✅ Loaded foundation data:")
        self.log.info(f"   Agricultural fields: {fields_count:,}")
        self.log.info(
            f"   Water project-wetland intersections (foundation): {intersections_count:,}"
        )
        self.log.info(f"   Wetland polygons: {wetlands_count:,}")

    async def _execute_stage_processing(self) -> Dict[str, Any]:
        """
        Calculate wetland coverage by water projects for each field using foundation data.

        FOUNDATION DATA APPROACH (PR #545 compliant):
        1. Single spatial join: Fields × Wetlands (single predicate: ST_Intersects)
        2. Use pre-computed water project coverage from Stage 1B
        3. Scale coverage proportionally to field intersection area
        """

        self.log.info("🎯 FOUNDATION DATA APPROACH: Field-level wetland water coverage")
        self.log.info("🔧 Using pre-computed intersections from Stage 1B")
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
                CAST(NULL AS INTEGER) as dominant_wetland_gridcode,
                CAST(NULL AS INTEGER) as wetland_polygon_count,
                CAST(NULL AS DOUBLE) as wetland_covered_by_water_projects_pct,
                CAST(NULL AS DOUBLE) as wetland_not_covered_by_water_projects_pct,
                CAST(NULL AS DOUBLE) as field_wetland_coverage_pct
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

            # SINGLE SPATIAL JOIN: Fields × Wetlands (PR #545 compliant)
            self.log.info(f"  Single spatial join: {batch_count:,} fields × wetlands")
            self.conn.execute("""
                CREATE OR REPLACE TABLE batch_field_wetland AS
                SELECT 
                    f.field_id,
                    f.block_id,
                    f.cvr_number,
                    f.year,
                    f.geometry as field_geometry,
                    ST_Area_Spheroid(f.geometry) as field_area_m2,
                    w.wetland_id,
                    w.gridcode,
                    w.toerv_pct,
                    ST_Area_Spheroid(ST_Intersection(f.geometry, w.geometry)) as field_wetland_area_m2
                FROM fields_batch f
                JOIN wetlands_for_fields w ON ST_Intersects(f.geometry, w.geometry)
                WHERE ST_Area_Spheroid(ST_Intersection(f.geometry, w.geometry)) > 100
            """)

            field_wetland_count = self.conn.execute(
                "SELECT COUNT(*) FROM batch_field_wetland"
            ).fetchone()[0]
            self.log.info(f"  Found {field_wetland_count:,} field-wetland intersections")

            if field_wetland_count == 0:
                # Handle fields with no wetlands
                self.conn.execute("""
                    INSERT INTO fields_wetland_water
                    SELECT 
                        field_id, block_id, cvr_number, year, geometry, 
                        ST_Area_Spheroid(geometry) as field_area_m2,
                        0 as total_wetland_area_m2, 
                        0 as wetland_covered_by_water_projects_m2, 
                        NULL as dominant_wetland_gridcode, 
                        0 as wetland_polygon_count,
                        0 as wetland_covered_by_water_projects_pct,
                        0 as wetland_not_covered_by_water_projects_pct, 
                        0 as field_wetland_coverage_pct
                    FROM fields_batch
                """)
                total_processed += batch_count
                continue

            # Apply foundation data to calculate water project coverage
            self.log.info("  Applying foundation data for water project coverage")
            self.conn.execute("""
                INSERT INTO fields_wetland_water
                SELECT 
                    field_id,
                    block_id,
                    cvr_number,
                    year,
                    field_geometry as geometry,
                    field_area_m2,
                    
                    -- Total wetland area within field
                    SUM(field_wetland_area_m2) as total_wetland_area_m2,
                    
                    -- Wetland area covered by water projects (using foundation data)
                    SUM(COALESCE(wwi.intersection_area_m2, 0)) as wetland_covered_by_water_projects_m2,
                    
                    -- Dominant wetland gridcode (largest area)
                    (
                        SELECT gridcode 
                        FROM batch_field_wetland fw2 
                        WHERE fw2.field_id = fw.field_id AND fw2.block_id = fw.block_id AND fw2.cvr_number = fw.cvr_number
                        ORDER BY fw2.field_wetland_area_m2 DESC
                        LIMIT 1
                    ) as dominant_wetland_gridcode,
                    
                    -- Count distinct wetland polygons
                    COUNT(DISTINCT wetland_id) as wetland_polygon_count,
                    
                    -- Calculate percentages
                    CASE 
                        WHEN SUM(field_wetland_area_m2) > 0 
                        THEN (SUM(COALESCE(wwi.intersection_area_m2, 0)) / SUM(field_wetland_area_m2)) * 100
                        ELSE 0 
                    END as wetland_covered_by_water_projects_pct,
                    
                    CASE 
                        WHEN SUM(field_wetland_area_m2) > 0 
                        THEN ((SUM(field_wetland_area_m2) - SUM(COALESCE(wwi.intersection_area_m2, 0))) / SUM(field_wetland_area_m2)) * 100
                        ELSE 0 
                    END as wetland_not_covered_by_water_projects_pct,
                    
                    (SUM(field_wetland_area_m2) / field_area_m2) * 100 as field_wetland_coverage_pct
                    
                FROM batch_field_wetland fw
                LEFT JOIN water_projects_wetlands_intersections wwi 
                    ON fw.wetland_id = wwi.wetland_id
                GROUP BY 
                    field_id, block_id, cvr_number, year, field_geometry, field_area_m2
            """)

            batch_processed = self.conn.execute(
                "SELECT COUNT(*) FROM fields_wetland_water WHERE field_id IN (SELECT field_id FROM fields_batch)"
            ).fetchone()[0]
            total_processed += batch_processed

            self.log.info(f"  ✅ Batch {batch_num + 1}: {batch_processed:,} fields processed")

            # Clean up batch tables
            self.conn.execute("DROP TABLE IF EXISTS fields_batch")
            self.conn.execute("DROP TABLE IF EXISTS batch_field_wetland")

            # Memory cleanup every few batches
            if (batch_num + 1) % CONFIG.memory_cleanup_frequency == 0:
                import gc

                gc.collect()

        # Final statistics
        final_count = self.conn.execute("SELECT COUNT(*) FROM fields_wetland_water").fetchone()[0]
        coverage_stats = self.conn.execute("""
            SELECT 
                COUNT(*) as total_fields,
                COUNT(CASE WHEN total_wetland_area_m2 > 0 THEN 1 END) as fields_with_wetlands,
                AVG(CASE WHEN total_wetland_area_m2 > 0 THEN field_wetland_coverage_pct END) as avg_wetland_coverage,
                AVG(CASE WHEN total_wetland_area_m2 > 0 THEN wetland_covered_by_water_projects_pct END) as avg_water_project_coverage
            FROM fields_wetland_water
        """).fetchone()

        total_fields, fields_with_wetlands, avg_wetland_coverage, avg_water_project_coverage = (
            coverage_stats
        )

        self.log.info("✅ Wetland water coverage analysis completed:")
        self.log.info(f"   Total fields processed: {total_fields:,}")
        self.log.info(
            f"   Fields with wetlands: {fields_with_wetlands:,} ({(fields_with_wetlands / total_fields) * 100:.1f}%)"
        )
        if avg_wetland_coverage:
            self.log.info(f"   Average wetland coverage: {avg_wetland_coverage:.1f}%")
        if avg_water_project_coverage:
            self.log.info(f"   Average water project coverage: {avg_water_project_coverage:.1f}%")

        return {
            "total_fields": total_fields,
            "fields_with_wetlands": fields_with_wetlands,
            "avg_wetland_coverage": avg_wetland_coverage,
            "avg_water_project_coverage": avg_water_project_coverage,
        }

    def _save_output_data(self, result: Dict[str, Any]):
        """Save fields with wetland water coverage to GCS."""
        self._save_stage_output("fields_wetland_water", "fields_wetland_water")
