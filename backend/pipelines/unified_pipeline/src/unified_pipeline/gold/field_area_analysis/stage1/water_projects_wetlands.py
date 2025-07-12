"""Stage 1B: Water Projects × Wetlands Intersection Analysis

Calculate which wetland areas are covered by water projects.
This creates a foundation dataset for later field-level analysis.

Optimized for DuckDB Spatial v1.2.2 with ST_Dump for multipolygon decomposition.
"""

from typing import Any, Dict

from ..base import FieldAnalysisStageBase, FieldAnalysisStageConfig
from ..config import CONFIG


class WaterProjectsWetlandsIntersection(FieldAnalysisStageBase):
    """Calculate wetland area coverage by water projects."""

    def __init__(self, config: FieldAnalysisStageConfig = None):
        if config is None:
            config = FieldAnalysisStageConfig()
        super().__init__(config, "Stage 1B: Water Projects × Wetlands")

    def _load_input_data(self):
        """Load wetlands and water projects datasets."""
        # Load full wetlands dataset (original version with attributes)
        self.log.info("Loading complete wetlands dataset...")
        self._load_silver_dataset(CONFIG.wetlands_dataset, "wetlands_raw")

        # Load water projects (build side - small dataset that fits in memory)
        self._load_silver_dataset(CONFIG.water_projects_dataset, "water_projects_raw")

        # Proper SPATIAL_JOIN optimization: Water projects (build side) + ALL wetlands (probe side)
        # Water projects = smaller dataset (~2.4K) that fits in memory for spatial indexing
        # Wetlands = larger dataset (1.6M) as probe side for comprehensive coverage

        self.log.info("Decomposing water projects with ST_Dump for optimal spatial indexing...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE water_projects AS
            SELECT 
                project_id,
                project_type,  -- Include project_type for intersection results
                UNNEST(ST_Dump(geometry)).geom as geometry
            FROM water_projects_raw
        """)

        self.log.info("Decomposing ALL wetlands with ST_Dump for comprehensive analysis...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE wetlands AS
            SELECT 
                toerv_pct,  -- Keep wetland type for analysis
                UNNEST(ST_Dump(geometry)).geom as geometry
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

        # Get total wetland count for batching
        total_wetlands = self.conn.execute("SELECT COUNT(*) FROM wetlands_raw").fetchone()[0]
        batch_size = 100000  # Smaller batches for foundation data creation
        num_batches = (total_wetlands + batch_size - 1) // batch_size

        self.log.info(
            f"Creating wetland-water project foundation data from {total_wetlands:,} wetlands in {num_batches} batches"
        )

        # Create intersection results table
        self.conn.execute("""
            CREATE OR REPLACE TABLE wetland_water_intersections (
                toerv_pct VARCHAR,  -- Wetland type for analysis
                project_id VARCHAR,
                project_type VARCHAR,
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

            # Create wetlands batch with ST_Dump
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE wetlands_batch AS
                SELECT 
                    toerv_pct,  -- Keep wetland type for analysis
                    UNNEST(ST_Dump(geometry)).geom as geometry,
                    ST_Area_Spheroid(geometry) as wetland_area_m2
                FROM wetlands_raw
                LIMIT {batch_size} OFFSET {offset}
            """)

            batch_count = self.conn.execute("SELECT COUNT(*) FROM wetlands_batch").fetchone()[0]
            if batch_count == 0:
                break

            self.log.info(f"  Processing {batch_count:,} wetlands in batch {batch_num + 1}")

            # Create intersection records for this batch
            batch_query = """
            CREATE OR REPLACE TABLE batch_intersections AS
            SELECT 
                wb.toerv_pct,  -- Wetland type for analysis
                wp.project_id,
                wp.project_type,
                ST_Area_Spheroid(ST_Intersection(wb.geometry, wp.geometry)) as intersection_area_m2,
                wb.wetland_area_m2,
                ST_Area_Spheroid(wp.geometry) as project_area_m2
            FROM wetlands_batch wb
            JOIN water_projects_raw wp ON ST_Intersects(wb.geometry, wp.geometry)
            WHERE ST_Area_Spheroid(ST_Intersection(wb.geometry, wp.geometry)) > 1.0  -- Filter tiny intersections
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
                    CAST(NULL AS VARCHAR) as toerv_pct,
                    CAST(NULL AS VARCHAR) as project_id,
                    CAST(NULL AS VARCHAR) as project_type,
                    CAST(NULL AS DOUBLE) as intersection_area_m2,
                    CAST(NULL AS DOUBLE) as wetland_area_m2,
                    CAST(NULL AS DOUBLE) as project_area_m2
                WHERE FALSE
            """)
            self.log.info("✅ Created empty intersection table (no intersections found)")

        # Create summary statistics for reporting (but keep detailed intersections as foundation data)
        self.log.info("Creating summary statistics...")

        summary_query = """
        CREATE OR REPLACE TABLE wetland_water_coverage AS
        SELECT 
            'All Wetlands' as wetland_category,
            NULL as total_wetland_geom,  -- Don't store large geometries
            NULL as wetland_covered_by_water_projects_geom,
            
            -- Calculate totals from intersection records and remaining wetlands
            (SELECT SUM(DISTINCT wetland_area_m2) FROM wetland_water_intersections) +
            (SELECT SUM(ST_Area_Spheroid(geometry)) FROM wetlands_raw) as total_wetland_area_m2,
            
            SUM(intersection_area_m2) as wetland_covered_area_m2,
            
            -- Calculate coverage percentage
            CASE 
                WHEN (SELECT SUM(ST_Area_Spheroid(geometry)) FROM wetlands_raw) > 0
                THEN (SUM(intersection_area_m2) / (SELECT SUM(ST_Area_Spheroid(geometry)) FROM wetlands_raw)) * 100
                ELSE 0 
            END as coverage_percentage,
            
            -- Count statistics
            (SELECT COUNT(*) FROM wetlands_raw) as total_wetland_polygons,
            COUNT(DISTINCT project_id) as water_projects_with_wetlands
            
        FROM wetland_water_intersections
        GROUP BY 'All Wetlands'
        """

        self.conn.execute(summary_query)

        # Log final results
        stats = self.conn.execute("""
            SELECT 
                wetland_category,
                total_wetland_area_m2 / 1000000 as total_area_km2,
                COALESCE(wetland_covered_area_m2, 0) / 1000000 as covered_area_km2,
                COALESCE(coverage_percentage, 0) as coverage_pct,
                total_wetlands,
                COALESCE(wetlands_with_water_projects, 0) as wetlands_with_projects
            FROM wetland_water_coverage
        """).fetchall()

        intersection_count = self.conn.execute(
            "SELECT COUNT(*) FROM wetland_water_intersections"
        ).fetchone()[0]

        self.log.info("✅ Created wetland-water project foundation data:")
        for stat in stats:
            category, total_km2, covered_km2, coverage_pct, total_count, covered_count = stat
            self.log.info(
                f"   {category}: {total_km2:.1f} km² total, {covered_km2:.1f} km² covered ({coverage_pct:.1f}%)"
            )
            self.log.info(
                f"   Wetlands: {total_count:,} total, {covered_count:,} with water projects"
            )

        self.log.info(
            f"   Foundation data: {intersection_count:,} wetland-water project intersection records"
        )

        return {
            "wetland_categories": 1,
            "coverage_stats": stats,
            "intersection_records": intersection_count,
            "batches_processed": num_batches,
        }

    def _save_output_data(self, result: Dict[str, Any]):
        """Save both summary and detailed foundation data to GCS."""
        # Save summary for backward compatibility
        self._save_stage_output("wetland_water_coverage", "wetland_water_coverage")

        # Save detailed intersection records for Stage 3B to use
        self._save_stage_output(
            "wetland_water_intersections", "water_projects_wetlands_intersections"
        )
