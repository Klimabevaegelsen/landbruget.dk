"""Stage 3A: Fields × BNBO Water Coverage Analysis

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
        super().__init__(config, "Stage 3A: Fields × BNBO Water Coverage")

    def _load_input_data(self):
        """Load fields with soil data and pre-computed BNBO-water project intersections."""
        # Load fields with soil from Stage 1D (moved from Stage 2)
        stage1d_path = f"gs://{CONFIG.bucket}/gold/{CONFIG.stage_outputs['fields_with_soil']}/{CONFIG.stage_outputs['fields_with_soil']}.parquet"
        self.gcs_access.query_parquet_direct(stage1d_path, "SELECT *", "fields_with_soil")

        # Load pre-computed water project-BNBO intersections from Stage 1A
        stage1a_path = f"gs://{CONFIG.bucket}/gold/{CONFIG.stage_outputs['water_projects_bnbo_intersections']}/{CONFIG.stage_outputs['water_projects_bnbo_intersections']}.parquet"
        self.gcs_access.query_parquet_direct(
            stage1a_path, "SELECT *", "water_projects_bnbo_intersections"
        )

        # Load BNBO status for field-level intersections (with ST_Dump optimization)
        self._load_silver_dataset(CONFIG.bnbo_status_dataset, "bnbo_status_raw")
        self.conn.execute("""
            CREATE OR REPLACE TABLE bnbo_status AS
            SELECT 
                status_category,
                UNNEST(ST_Dump(geometry)).geom as geometry
            FROM bnbo_status_raw
        """)

        # Load water projects for field-level intersections (with ST_Dump optimization)
        self._load_silver_dataset(CONFIG.water_projects_dataset, "water_projects_raw")
        self.conn.execute("""
            CREATE OR REPLACE TABLE water_projects AS
            SELECT 
                project_id,
                UNNEST(ST_Dump(geometry)).geom as geometry
            FROM water_projects_raw
        """)

        # Log loaded data
        fields_count = self.conn.execute("SELECT COUNT(*) FROM fields_with_soil").fetchone()[0]
        intersections_count = self.conn.execute(
            "SELECT COUNT(*) FROM water_projects_bnbo_intersections"
        ).fetchone()[0]
        bnbo_count = self.conn.execute("SELECT COUNT(*) FROM bnbo_status").fetchone()[0]
        projects_count = self.conn.execute("SELECT COUNT(*) FROM water_projects").fetchone()[0]

        self.log.info("✅ Loaded foundation data:")
        self.log.info(f"   Fields with soil: {fields_count:,}")
        self.log.info(f"   Water project-BNBO intersections: {intersections_count:,}")
        self.log.info(f"   BNBO polygons: {bnbo_count:,}")
        self.log.info(f"   Water project polygons: {projects_count:,}")

    async def _execute_stage_processing(self) -> Dict[str, Any]:
        """
        Calculate BNBO coverage by water projects for each field using foundation data.

        Key optimizations:
        1. Use pre-computed water project-BNBO intersections from Stage 1A
        2. Process fields in batches to manage memory (600K fields)
        3. Single spatial predicates for SPATIAL_JOIN operator
        4. Leverage foundation data to avoid redundant calculations
        """

        self.log.info("Calculating field-level BNBO water coverage using foundation data...")

        # Get total field count for batching
        total_fields = self.conn.execute("SELECT COUNT(*) FROM fields_with_soil").fetchone()[0]
        batch_size = 50000  # Smaller batches for complex spatial analysis
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
                CAST(NULL AS VARCHAR) as dominant_soil_code,
                CAST(NULL AS VARCHAR) as dominant_soil_description,
                CAST(NULL AS VARCHAR) as dominant_soil_category,
                CAST(NULL AS DOUBLE) as dominant_soil_share_pct,
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
            self.log.info(f"Processing batch {batch_num + 1}/{num_batches} (offset: {offset:,})")

            # Create field batch
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE fields_batch AS
                SELECT * FROM fields_with_soil
                LIMIT {batch_size} OFFSET {offset}
            """)

            batch_count = self.conn.execute("SELECT COUNT(*) FROM fields_batch").fetchone()[0]
            if batch_count == 0:
                break

            # Step 1: Calculate BNBO areas within each field (using SPATIAL_JOIN)
            self.conn.execute("""
                CREATE OR REPLACE TABLE batch_bnbo_areas AS
                SELECT 
                    f.field_id,
                    f.block_id,
                    f.cvr_number,
                    f.year,
                    f.geometry,
                    f.field_area_m2,
                    f.dominant_soil_code,
                    f.dominant_soil_description,
                    f.dominant_soil_category,
                    f.dominant_soil_share_pct,
                    
                    -- BNBO area calculations for this field
                    COALESCE(SUM(
                        ST_Area_Spheroid(ST_Intersection(f.geometry, b.geometry))
                    ), 0) as total_bnbo_area_m2,
                    
                    -- Dominant BNBO status category for this field
                    (
                        SELECT b2.status_category 
                        FROM bnbo_status b2 
                        WHERE ST_Intersects(f.geometry, b2.geometry)
                        ORDER BY ST_Area_Spheroid(ST_Intersection(f.geometry, b2.geometry)) DESC
                        LIMIT 1
                    ) as dominant_bnbo_status
                    
                FROM fields_batch f
                LEFT JOIN bnbo_status b ON ST_Intersects(f.geometry, b.geometry)
                WHERE ST_Area_Spheroid(ST_Intersection(f.geometry, b.geometry)) > 100  -- Filter noise
                GROUP BY 
                    f.field_id, f.block_id, f.cvr_number, f.year, f.geometry, f.field_area_m2,
                    f.dominant_soil_code, f.dominant_soil_description, f.dominant_soil_category, f.dominant_soil_share_pct
            """)

            # Step 2: Calculate water project coverage using foundation data
            self.conn.execute("""
                CREATE OR REPLACE TABLE batch_water_coverage AS
                SELECT 
                    ba.*,
                    
                    -- Use foundation data to calculate BNBO areas covered by water projects
                    COALESCE(SUM(
                        CASE WHEN wp.project_id IS NOT NULL AND b.status_category IS NOT NULL
                        THEN 
                            -- Calculate the field portion of the pre-computed intersection
                            LEAST(
                                ST_Area_Spheroid(ST_Intersection(ba.geometry, b.geometry)),
                                -- Scale by the intersection area from foundation data
                                wpi.intersection_area_m2 * 
                                (ST_Area_Spheroid(ST_Intersection(ba.geometry, b.geometry)) / 
                                 ST_Area_Spheroid(b.geometry))
                            )
                        ELSE 0 END
                    ), 0) as bnbo_covered_by_water_projects_m2
                    
                FROM batch_bnbo_areas ba
                LEFT JOIN bnbo_status b ON ST_Intersects(ba.geometry, b.geometry)
                LEFT JOIN water_projects wp ON ST_Intersects(ba.geometry, wp.geometry)
                LEFT JOIN water_projects_bnbo_intersections wpi 
                    ON wp.project_id = wpi.project_id 
                    AND b.status_category = wpi.status_category
                GROUP BY 
                    ba.field_id, ba.block_id, ba.cvr_number, ba.year, ba.geometry, ba.field_area_m2,
                    ba.dominant_soil_code, ba.dominant_soil_description, ba.dominant_soil_category, ba.dominant_soil_share_pct,
                    ba.total_bnbo_area_m2, ba.dominant_bnbo_status
            """)

            # Step 3: Calculate percentages and append to main table
            self.conn.execute("""
                INSERT INTO fields_bnbo_water
                SELECT 
                    field_id,
                    block_id,
                    cvr_number,
                    year,
                    geometry,
                    field_area_m2,
                    dominant_soil_code,
                    dominant_soil_description,
                    dominant_soil_category,
                    dominant_soil_share_pct,
                    total_bnbo_area_m2,
                    bnbo_covered_by_water_projects_m2,
                    dominant_bnbo_status,
                    
                    -- Calculate BNBO coverage percentages
                    CASE WHEN total_bnbo_area_m2 > 0 
                    THEN (bnbo_covered_by_water_projects_m2 / total_bnbo_area_m2) * 100
                    ELSE 0 END as bnbo_covered_by_water_projects_pct,
                    
                    CASE WHEN total_bnbo_area_m2 > 0 
                    THEN ((total_bnbo_area_m2 - bnbo_covered_by_water_projects_m2) / total_bnbo_area_m2) * 100
                    ELSE 0 END as bnbo_not_covered_by_water_projects_pct,
                    
                    -- Field-level BNBO coverage
                    (total_bnbo_area_m2 / field_area_m2) * 100 as field_bnbo_coverage_pct
                    
                FROM batch_water_coverage
            """)

            total_processed += batch_count
            self.log.info(f"  ✅ Batch {batch_num + 1}: {batch_count:,} fields processed")

            # Clean up batch tables
            self.conn.execute("DROP TABLE IF EXISTS fields_batch")
            self.conn.execute("DROP TABLE IF EXISTS batch_bnbo_areas")
            self.conn.execute("DROP TABLE IF EXISTS batch_water_coverage")

            # Memory cleanup every 5 batches
            if (batch_num + 1) % 5 == 0:
                import gc

                gc.collect()
                self.log.info(f"  🧹 Memory cleanup after batch {batch_num + 1}")

        # Add fields with no BNBO data
        self.log.info("Adding fields with no BNBO data...")
        self.conn.execute("""
            INSERT INTO fields_bnbo_water
            SELECT 
                field_id,
                block_id,
                cvr_number,
                year,
                geometry,
                field_area_m2,
                dominant_soil_code,
                dominant_soil_description,
                dominant_soil_category,
                dominant_soil_share_pct,
                0 as total_bnbo_area_m2,
                0 as bnbo_covered_by_water_projects_m2,
                NULL as dominant_bnbo_status,
                0 as bnbo_covered_by_water_projects_pct,
                0 as bnbo_not_covered_by_water_projects_pct,
                0 as field_bnbo_coverage_pct
            FROM fields_with_soil f
            WHERE NOT EXISTS (
                SELECT 1 FROM fields_bnbo_water fb 
                WHERE f.field_id = fb.field_id 
                AND f.block_id = fb.block_id 
                AND f.cvr_number = fb.cvr_number
            )
        """)

        # Log results
        result_count = self.conn.execute("SELECT COUNT(*) FROM fields_bnbo_water").fetchone()[0]

        # Get coverage statistics
        stats = self.conn.execute("""
            SELECT 
                COUNT(*) as total_fields,
                COUNT(CASE WHEN total_bnbo_area_m2 > 0 THEN 1 END) as fields_with_bnbo,
                COUNT(CASE WHEN bnbo_covered_by_water_projects_m2 > 0 THEN 1 END) as fields_with_bnbo_water_coverage,
                AVG(CASE WHEN total_bnbo_area_m2 > 0 THEN field_bnbo_coverage_pct END) as avg_field_bnbo_pct,
                AVG(CASE WHEN total_bnbo_area_m2 > 0 THEN bnbo_covered_by_water_projects_pct END) as avg_bnbo_water_coverage_pct,
                SUM(total_bnbo_area_m2) / 1000000 as total_bnbo_km2,
                SUM(bnbo_covered_by_water_projects_m2) / 1000000 as total_bnbo_covered_km2
            FROM fields_bnbo_water
        """).fetchone()

        (
            total_fields,
            fields_with_bnbo,
            fields_with_coverage,
            avg_field_bnbo,
            avg_water_coverage,
            total_bnbo_km2,
            covered_km2,
        ) = stats

        # Get BNBO status breakdown
        status_breakdown = self.conn.execute("""
            SELECT 
                COALESCE(dominant_bnbo_status, 'No BNBO') as status,
                COUNT(*) as field_count,
                AVG(CASE WHEN total_bnbo_area_m2 > 0 THEN bnbo_covered_by_water_projects_pct END) as avg_coverage_pct
            FROM fields_bnbo_water
            GROUP BY COALESCE(dominant_bnbo_status, 'No BNBO')
            ORDER BY field_count DESC
        """).fetchall()

        self.log.info("✅ Created field-level BNBO water coverage analysis:")
        self.log.info(f"   Total fields: {total_fields:,}")
        self.log.info(
            f"   Fields with BNBO: {fields_with_bnbo:,} ({(fields_with_bnbo / total_fields) * 100:.1f}%)"
        )
        self.log.info(f"   Fields with BNBO water coverage: {fields_with_coverage:,}")
        self.log.info(f"   Average field BNBO coverage: {avg_field_bnbo:.1f}%")
        self.log.info(f"   Average BNBO water project coverage: {avg_water_coverage:.1f}%")
        self.log.info(f"   Total BNBO area: {total_bnbo_km2:.1f} km²")
        self.log.info(f"   BNBO area covered by water projects: {covered_km2:.1f} km²")

        self.log.info("   BNBO status breakdown:")
        for status, count, avg_pct in status_breakdown:
            self.log.info(f"     {status}: {count:,} fields, {avg_pct:.1f}% avg water coverage")

        return {
            "total_fields": total_fields,
            "fields_with_bnbo": fields_with_bnbo,
            "fields_with_water_coverage": fields_with_coverage,
            "avg_field_bnbo_pct": avg_field_bnbo,
            "avg_water_coverage_pct": avg_water_coverage,
            "total_bnbo_km2": total_bnbo_km2,
            "covered_bnbo_km2": covered_km2,
            "status_breakdown": status_breakdown,
            "batches_processed": num_batches,
        }

    def _save_output_data(self, result: Dict[str, Any]):
        """Save fields with BNBO water coverage to GCS."""
        self._save_stage_output("fields_bnbo_water", "fields_bnbo_water")
