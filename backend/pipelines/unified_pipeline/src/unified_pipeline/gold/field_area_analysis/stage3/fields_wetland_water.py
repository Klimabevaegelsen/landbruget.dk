"""Stage 3B: Fields × Wetland Water Coverage Analysis

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
        super().__init__(config, "Stage 3B: Fields × Wetland Water Coverage")

    def _load_input_data(self):
        """Load fields with soil data and pre-computed wetland-water project intersections."""
        # Load fields with soil from Stage 1D (moved from Stage 2)
        stage1d_path = f"gs://{CONFIG.bucket}/gold/{CONFIG.stage_outputs['fields_with_soil']}/{CONFIG.stage_outputs['fields_with_soil']}.parquet"
        self.gcs_access.query_parquet_direct(stage1d_path, "SELECT *", "fields_with_soil")

        # Load pre-computed wetland-water project intersections from Stage 1B
        stage1b_path = f"gs://{CONFIG.bucket}/gold/{CONFIG.stage_outputs['water_projects_wetlands_intersections']}/{CONFIG.stage_outputs['water_projects_wetlands_intersections']}.parquet"
        self.gcs_access.query_parquet_direct(
            stage1b_path, "SELECT *", "wetland_water_intersections"
        )

        # Load wetlands for field-level intersections (with ST_Dump optimization)
        self._load_silver_dataset(CONFIG.wetlands_dataset, "wetlands_raw")
        self.conn.execute("""
            CREATE OR REPLACE TABLE wetlands AS
            SELECT 
                wetland_id,
                UNNEST(ST_Dump(geometry)).geom as geometry
            FROM wetlands_raw
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
            "SELECT COUNT(*) FROM wetland_water_intersections"
        ).fetchone()[0]
        wetlands_count = self.conn.execute("SELECT COUNT(*) FROM wetlands").fetchone()[0]
        projects_count = self.conn.execute("SELECT COUNT(*) FROM water_projects").fetchone()[0]

        self.log.info("✅ Loaded foundation data:")
        self.log.info(f"   Fields with soil: {fields_count:,}")
        self.log.info(f"   Wetland-water project intersections: {intersections_count:,}")
        self.log.info(f"   Wetland polygons: {wetlands_count:,}")
        self.log.info(f"   Water project polygons: {projects_count:,}")

    async def _execute_stage_processing(self) -> Dict[str, Any]:
        """
        Calculate wetland coverage by water projects for each field using foundation data.

        Key optimizations:
        1. Use pre-computed wetland-water project intersections from Stage 1B
        2. Process fields in batches to manage memory (600K fields)
        3. Single spatial predicates for SPATIAL_JOIN operator
        4. Leverage foundation data to avoid redundant calculations
        """

        self.log.info("Calculating field-level wetland water coverage using foundation data...")

        # Get total field count for batching
        total_fields = self.conn.execute("SELECT COUNT(*) FROM fields_with_soil").fetchone()[0]
        batch_size = 50000  # Smaller batches for complex spatial analysis
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
                CAST(NULL AS VARCHAR) as dominant_soil_code,
                CAST(NULL AS VARCHAR) as dominant_soil_description,
                CAST(NULL AS VARCHAR) as dominant_soil_category,
                CAST(NULL AS DOUBLE) as dominant_soil_share_pct,
                CAST(NULL AS DOUBLE) as total_wetland_area_m2,
                CAST(NULL AS DOUBLE) as wetland_covered_by_water_projects_m2,
                CAST(NULL AS VARCHAR) as dominant_wetland_type,
                CAST(NULL AS INTEGER) as wetland_polygon_count,
                CAST(NULL AS DOUBLE) as wetland_covered_by_water_projects_pct,
                CAST(NULL AS DOUBLE) as wetland_not_covered_by_water_projects_pct,
                CAST(NULL AS DOUBLE) as field_wetland_coverage_pct
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

            # Step 1: Calculate wetland areas within each field (using SPATIAL_JOIN)
            self.conn.execute("""
                CREATE OR REPLACE TABLE batch_wetland_areas AS
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
                    
                    -- Wetland area calculations for this field
                    COALESCE(SUM(
                        ST_Area_Spheroid(ST_Intersection(f.geometry, w.geometry))
                    ), 0) as total_wetland_area_m2,
                    
                    -- Count of wetland polygons in this field
                    COUNT(DISTINCT w.wetland_id) as wetland_polygon_count,
                    
                    -- Dominant wetland type (if available from wetland attributes)
                    'Mixed Wetlands' as dominant_wetland_type  -- Simplified for now
                    
                FROM fields_batch f
                LEFT JOIN wetlands w ON ST_Intersects(f.geometry, w.geometry)
                WHERE ST_Area_Spheroid(ST_Intersection(f.geometry, w.geometry)) > 100  -- Filter noise
                GROUP BY 
                    f.field_id, f.block_id, f.cvr_number, f.year, f.geometry, f.field_area_m2,
                    f.dominant_soil_code, f.dominant_soil_description, f.dominant_soil_category, f.dominant_soil_share_pct
            """)

            # Step 2: Calculate water project coverage using foundation data
            self.conn.execute("""
                CREATE OR REPLACE TABLE batch_water_coverage AS
                SELECT 
                    ba.*,
                    
                    -- Use foundation data to calculate wetland areas covered by water projects
                    COALESCE(SUM(
                        CASE WHEN wp.project_id IS NOT NULL AND w.wetland_id IS NOT NULL
                        THEN 
                            -- Calculate the field portion of the pre-computed intersection
                            LEAST(
                                ST_Area_Spheroid(ST_Intersection(ba.geometry, w.geometry)),
                                -- Scale by the intersection area from foundation data
                                wwi.intersection_area_m2 * 
                                (ST_Area_Spheroid(ST_Intersection(ba.geometry, w.geometry)) / 
                                 wwi.wetland_area_m2)
                            )
                        ELSE 0 END
                    ), 0) as wetland_covered_by_water_projects_m2
                    
                FROM batch_wetland_areas ba
                LEFT JOIN wetlands w ON ST_Intersects(ba.geometry, w.geometry)
                LEFT JOIN water_projects wp ON ST_Intersects(ba.geometry, wp.geometry)
                LEFT JOIN wetland_water_intersections wwi 
                    ON wp.project_id = wwi.project_id 
                    AND w.wetland_id = wwi.wetland_id
                GROUP BY 
                    ba.field_id, ba.block_id, ba.cvr_number, ba.year, ba.geometry, ba.field_area_m2,
                    ba.dominant_soil_code, ba.dominant_soil_description, ba.dominant_soil_category, ba.dominant_soil_share_pct,
                    ba.total_wetland_area_m2, ba.wetland_polygon_count, ba.dominant_wetland_type
            """)

            # Step 3: Calculate percentages and append to main table
            self.conn.execute("""
                INSERT INTO fields_wetland_water
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
                    total_wetland_area_m2,
                    wetland_covered_by_water_projects_m2,
                    dominant_wetland_type,
                    wetland_polygon_count,
                    
                    -- Calculate wetland coverage percentages
                    CASE WHEN total_wetland_area_m2 > 0 
                    THEN (wetland_covered_by_water_projects_m2 / total_wetland_area_m2) * 100
                    ELSE 0 END as wetland_covered_by_water_projects_pct,
                    
                    CASE WHEN total_wetland_area_m2 > 0 
                    THEN ((total_wetland_area_m2 - wetland_covered_by_water_projects_m2) / total_wetland_area_m2) * 100
                    ELSE 0 END as wetland_not_covered_by_water_projects_pct,
                    
                    -- Field-level wetland coverage
                    (total_wetland_area_m2 / field_area_m2) * 100 as field_wetland_coverage_pct
                    
                FROM batch_water_coverage
            """)

            total_processed += batch_count
            self.log.info(f"  ✅ Batch {batch_num + 1}: {batch_count:,} fields processed")

            # Clean up batch tables
            self.conn.execute("DROP TABLE IF EXISTS fields_batch")
            self.conn.execute("DROP TABLE IF EXISTS batch_wetland_areas")
            self.conn.execute("DROP TABLE IF EXISTS batch_water_coverage")

            # Memory cleanup every 5 batches
            if (batch_num + 1) % 5 == 0:
                import gc

                gc.collect()
                self.log.info(f"  🧹 Memory cleanup after batch {batch_num + 1}")

        # Add fields with no wetland data
        self.log.info("Adding fields with no wetland data...")
        self.conn.execute("""
            INSERT INTO fields_wetland_water
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
                0 as total_wetland_area_m2,
                0 as wetland_covered_by_water_projects_m2,
                NULL as dominant_wetland_type,
                0 as wetland_polygon_count,
                0 as wetland_covered_by_water_projects_pct,
                0 as wetland_not_covered_by_water_projects_pct,
                0 as field_wetland_coverage_pct
            FROM fields_with_soil f
            WHERE NOT EXISTS (
                SELECT 1 FROM fields_wetland_water fw 
                WHERE f.field_id = fw.field_id 
                AND f.block_id = fw.block_id 
                AND f.cvr_number = fw.cvr_number
            )
        """)

        # Log results
        result_count = self.conn.execute("SELECT COUNT(*) FROM fields_wetland_water").fetchone()[0]

        # Get coverage statistics
        stats = self.conn.execute("""
            SELECT 
                COUNT(*) as total_fields,
                COUNT(CASE WHEN total_wetland_area_m2 > 0 THEN 1 END) as fields_with_wetlands,
                COUNT(CASE WHEN wetland_covered_by_water_projects_m2 > 0 THEN 1 END) as fields_with_wetland_water_coverage,
                AVG(CASE WHEN total_wetland_area_m2 > 0 THEN field_wetland_coverage_pct END) as avg_field_wetland_pct,
                AVG(CASE WHEN total_wetland_area_m2 > 0 THEN wetland_covered_by_water_projects_pct END) as avg_wetland_water_coverage_pct,
                SUM(total_wetland_area_m2) / 1000000 as total_wetland_km2,
                SUM(wetland_covered_by_water_projects_m2) / 1000000 as total_wetland_covered_km2,
                AVG(wetland_polygon_count) as avg_wetland_polygons_per_field
            FROM fields_wetland_water
        """).fetchone()

        (
            total_fields,
            fields_with_wetlands,
            fields_with_coverage,
            avg_field_wetland,
            avg_water_coverage,
            total_wetland_km2,
            covered_km2,
            avg_polygons,
        ) = stats

        # Get wetland type breakdown
        type_breakdown = self.conn.execute("""
            SELECT 
                COALESCE(dominant_wetland_type, 'No Wetlands') as wetland_type,
                COUNT(*) as field_count,
                AVG(CASE WHEN total_wetland_area_m2 > 0 THEN wetland_covered_by_water_projects_pct END) as avg_coverage_pct,
                AVG(CASE WHEN total_wetland_area_m2 > 0 THEN field_wetland_coverage_pct END) as avg_field_coverage_pct
            FROM fields_wetland_water
            GROUP BY COALESCE(dominant_wetland_type, 'No Wetlands')
            ORDER BY field_count DESC
        """).fetchall()

        self.log.info("✅ Created field-level wetland water coverage analysis:")
        self.log.info(f"   Total fields: {total_fields:,}")
        self.log.info(
            f"   Fields with wetlands: {fields_with_wetlands:,} ({(fields_with_wetlands / total_fields) * 100:.1f}%)"
        )
        self.log.info(f"   Fields with wetland water coverage: {fields_with_coverage:,}")
        self.log.info(f"   Average field wetland coverage: {avg_field_wetland:.1f}%")
        self.log.info(f"   Average wetland water project coverage: {avg_water_coverage:.1f}%")
        self.log.info(f"   Total wetland area: {total_wetland_km2:.1f} km²")
        self.log.info(f"   Wetland area covered by water projects: {covered_km2:.1f} km²")
        self.log.info(f"   Average wetland polygons per field: {avg_polygons:.1f}")

        self.log.info("   Wetland type breakdown:")
        for wetland_type, count, avg_pct, avg_field_pct in type_breakdown[:5]:  # Top 5
            self.log.info(
                f"     {wetland_type}: {count:,} fields, {avg_pct:.1f}% avg water coverage, {avg_field_pct:.1f}% avg field coverage"
            )

        return {
            "total_fields": total_fields,
            "fields_with_wetlands": fields_with_wetlands,
            "fields_with_water_coverage": fields_with_coverage,
            "avg_field_wetland_pct": avg_field_wetland,
            "avg_water_coverage_pct": avg_water_coverage,
            "total_wetland_km2": total_wetland_km2,
            "covered_wetland_km2": covered_km2,
            "avg_polygons_per_field": avg_polygons,
            "type_breakdown": type_breakdown,
            "batches_processed": num_batches,
        }

    def _save_output_data(self, result: Dict[str, Any]):
        """Save fields with wetland water coverage to GCS."""
        self._save_stage_output("fields_wetland_water", "fields_wetland_water")
