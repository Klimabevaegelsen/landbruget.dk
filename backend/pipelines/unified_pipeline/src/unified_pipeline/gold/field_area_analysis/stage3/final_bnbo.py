"""Stage 3A: Final BNBO Analysis

Combine BNBO water coverage analysis with pre-filtered properties.
Creates the final BNBO analysis table ready for consolidation.

Optimized for DuckDB Spatial v1.2.2 with single spatial predicates.
"""

from typing import Any, Dict

from ..base import FieldAnalysisStageBase, FieldAnalysisStageConfig
from ..config import CONFIG


class FinalBNBOAnalysis(FieldAnalysisStageBase):
    """Combine BNBO analysis with pre-filtered properties."""

    def __init__(self, config: FieldAnalysisStageConfig = None):
        if config is None:
            config = FieldAnalysisStageConfig()
        super().__init__(config, "Stage 3A: Final BNBO Analysis")

    def _load_input_data(self):
        """Load BNBO analysis from Stage 2A and foundation data for 3-way spatial analysis."""
        # Load BNBO water coverage from Stage 2A
        stage2a_path = f"gs://{CONFIG.bucket}/gold/{CONFIG.stage_outputs['fields_bnbo_water']}/{CONFIG.stage_outputs['fields_bnbo_water']}.parquet"
        self.gcs_access.query_parquet_direct(stage2a_path, "SELECT *", "fields_bnbo_water")

        # Load pre-filtered field-property intersections from Stage 1C
        stage1c_path = f"gs://{CONFIG.bucket}/gold/{CONFIG.stage_outputs['field_property_intersections']}/{CONFIG.stage_outputs['field_property_intersections']}.parquet"
        self.gcs_access.query_parquet_direct(
            stage1c_path, "SELECT *", "field_property_intersections"
        )

        # Load water project × BNBO intersections from Stage 1A for 3-way spatial analysis
        stage1a_path = f"gs://{CONFIG.bucket}/gold/{CONFIG.stage_outputs['water_projects_bnbo_intersections']}/{CONFIG.stage_outputs['water_projects_bnbo_intersections']}.parquet"
        self.gcs_access.query_parquet_direct(
            stage1a_path, "SELECT *", "water_projects_bnbo_intersections"
        )

        # Load original BNBO data for spatial analysis (needed for property-BNBO intersections)
        self._load_silver_dataset(CONFIG.bnbo_dataset, "bnbo_for_fields")

    async def _execute_stage_processing(self) -> Dict[str, Any]:
        """
        Combine BNBO analysis with property ownership through spatial analysis.

        SPATIAL ANALYSIS APPROACH (PR #545 compliant):
        1. Load field BNBO data from Stage 2A
        2. Load property intersection geometries from Stage 1C
        3. Spatial join: Property intersections × BNBO features (single ST_Intersects)
        4. Aggregate by field to show which properties have BNBO coverage
        """

        self.log.info("🎯 SPATIAL ANALYSIS: Connecting BNBO features to property ownership")
        self.log.info("✅ Following DuckDB Spatial PR #545: Single spatial predicate joins")

        # Get total field count for batching
        total_fields = self.conn.execute("SELECT COUNT(*) FROM fields_bnbo_water").fetchone()[0]
        batch_size = CONFIG.stage3_batch_size
        num_batches = (total_fields + batch_size - 1) // batch_size

        self.log.info(
            f"Processing {total_fields:,} fields in {num_batches} batches of {batch_size:,}"
        )

        # Initialize result table
        self.conn.execute("""
            CREATE OR REPLACE TABLE final_bnbo_analysis AS
            SELECT 
                CAST(NULL AS VARCHAR) as field_id,
                CAST(NULL AS VARCHAR) as block_id,
                CAST(NULL AS VARCHAR) as cvr_number,
                CAST(NULL AS INTEGER) as year,
                CAST(NULL AS GEOMETRY) as geometry,
                CAST(NULL AS DOUBLE) as field_area_m2,
                
                -- BNBO analysis data
                CAST(NULL AS DOUBLE) as total_bnbo_area_m2,
                CAST(NULL AS DOUBLE) as bnbo_covered_by_water_projects_m2,
                CAST(NULL AS DOUBLE) as bnbo_covered_by_water_projects_pct,
                CAST(NULL AS DOUBLE) as bnbo_not_covered_by_water_projects_pct,
                CAST(NULL AS DOUBLE) as field_bnbo_coverage_pct,
                CAST(NULL AS VARCHAR) as dominant_bnbo_status,
                
                -- Property ownership analysis
                CAST(NULL AS INTEGER) as property_count,
                CAST(NULL AS DOUBLE) as total_property_intersection_area_m2,
                CAST(NULL AS DOUBLE) as avg_property_area_share_pct,
                CAST(NULL AS DOUBLE) as max_property_area_share_pct,
                CAST(NULL AS VARCHAR) as primary_bfe_number,
                
                -- Property-BNBO spatial analysis
                CAST(NULL AS DOUBLE) as property_bnbo_intersection_area_m2,
                CAST(NULL AS DOUBLE) as property_bnbo_coverage_pct,
                CAST(NULL AS INTEGER) as properties_with_bnbo_count,
                CAST(NULL AS VARCHAR) as bnbo_property_owners,
                
                -- Property-level BNBO water project coverage analysis
                CAST(NULL AS DOUBLE) as property_bnbo_covered_by_water_m2,
                CAST(NULL AS DOUBLE) as property_bnbo_not_covered_by_water_m2,
                CAST(NULL AS DOUBLE) as property_bnbo_water_coverage_pct,
                CAST(NULL AS INTEGER) as properties_with_covered_bnbo_count,
                CAST(NULL AS INTEGER) as properties_with_uncovered_bnbo_count,
                CAST(NULL AS VARCHAR) as covered_bnbo_property_owners,
                CAST(NULL AS VARCHAR) as uncovered_bnbo_property_owners
            WHERE FALSE
        """)

        # Process each batch
        for batch_num in range(num_batches):
            offset = batch_num * batch_size
            progress_pct = ((batch_num + 1) / num_batches) * 100
            self.log.info(f"📦 Batch {batch_num + 1}/{num_batches} - {progress_pct:.1f}% complete")

            # Create field batch
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE fields_batch AS
                SELECT * FROM fields_bnbo_water
                LIMIT {batch_size} OFFSET {offset}
            """)

            batch_count = self.conn.execute("SELECT COUNT(*) FROM fields_batch").fetchone()[0]
            if batch_count == 0:
                break

            # Get property intersections for this batch
            self.conn.execute("""
                CREATE OR REPLACE TABLE batch_property_intersections AS
                SELECT 
                    p.field_id,
                    p.block_id,
                    p.cvr_number,
                    p.year,
                    p.bfe_number,
                    p.intersection_area_m2,
                    p.field_area_share_pct,
                    p.property_area_share_pct,
                    p.intersection_geometry
                FROM field_property_intersections p
                WHERE EXISTS (
                    SELECT 1 FROM fields_batch b 
                    WHERE p.field_id = b.field_id 
                    AND p.block_id = b.block_id 
                    AND p.cvr_number = b.cvr_number
                )
            """)

            property_count = self.conn.execute(
                "SELECT COUNT(*) FROM batch_property_intersections"
            ).fetchone()[0]
            self.log.info(f"  Found {property_count:,} property intersections for batch")

            if property_count == 0:
                # Handle fields with no properties (just copy BNBO data)
                self.conn.execute("""
                    INSERT INTO final_bnbo_analysis
                    SELECT 
                        field_id, block_id, cvr_number, year, geometry, field_area_m2,
                        total_bnbo_area_m2, bnbo_covered_by_water_projects_m2, 
                        bnbo_covered_by_water_projects_pct, bnbo_not_covered_by_water_projects_pct,
                        field_bnbo_coverage_pct, dominant_bnbo_status,
                        0 as property_count, 0 as total_property_intersection_area_m2,
                        0 as avg_property_area_share_pct, 0 as max_property_area_share_pct,
                        NULL as primary_bfe_number,
                        0 as property_bnbo_intersection_area_m2, 0 as property_bnbo_coverage_pct,
                        0 as properties_with_bnbo_count, NULL as bnbo_property_owners,
                        0 as property_bnbo_covered_by_water_m2, 0 as property_bnbo_not_covered_by_water_m2,
                        0 as property_bnbo_water_coverage_pct, 0 as properties_with_covered_bnbo_count,
                        0 as properties_with_uncovered_bnbo_count, NULL as covered_bnbo_property_owners,
                        NULL as uncovered_bnbo_property_owners
                    FROM fields_batch
                """)
                continue

            # SPATIAL ANALYSIS: Property intersections × BNBO features
            # Load BNBO data for spatial analysis
            self.conn.execute("""
                CREATE OR REPLACE TABLE batch_bnbo_features AS
                SELECT 
                    b.field_id,
                    b.block_id,
                    b.cvr_number,
                    b.year,
                    bnbo.id as bnbo_id,
                    bnbo.status_category,
                    bnbo.geometry as bnbo_geometry
                FROM fields_batch b
                JOIN bnbo_for_fields bnbo ON ST_Intersects(b.geometry, bnbo.geometry)
                WHERE b.total_bnbo_area_m2 > 0
            """)

            bnbo_features_count = self.conn.execute(
                "SELECT COUNT(*) FROM batch_bnbo_features"
            ).fetchone()[0]
            self.log.info(f"  Found {bnbo_features_count:,} BNBO features for spatial analysis")

            if bnbo_features_count > 0:
                # STEP 1: Property × BNBO spatial intersections (PR #545 compliant)
                self.log.info("  Step 1: Property intersections × BNBO features")
                # Pure spatial join with SINGLE predicate (PR #545 compliant)
                self.conn.execute("""
                    CREATE OR REPLACE TABLE batch_property_bnbo_raw AS
                    SELECT 
                        p.field_id,
                        p.block_id,
                        p.cvr_number,
                        p.year,
                        p.bfe_number,
                        p.intersection_area_m2 as property_intersection_area_m2,
                        p.intersection_geometry,
                        b.bnbo_id,
                        b.status_category,
                        b.bnbo_geometry,
                        ST_Area_Spheroid(ST_Intersection(p.intersection_geometry, b.bnbo_geometry)) as property_bnbo_area_m2
                    FROM batch_property_intersections p
                    JOIN batch_bnbo_features b ON ST_Intersects(p.intersection_geometry, b.bnbo_geometry)
                    WHERE ST_Area_Spheroid(ST_Intersection(p.intersection_geometry, b.bnbo_geometry)) > 10
                """)

                # Filter to matching fields (equality constraints applied after spatial join)
                self.conn.execute("""
                    CREATE OR REPLACE TABLE batch_property_bnbo_spatial AS
                    SELECT 
                        field_id,
                        block_id,
                        cvr_number,
                        year,
                        bfe_number,
                        property_intersection_area_m2,
                        bnbo_id,
                        status_category,
                        property_bnbo_area_m2,
                        ST_Intersection(intersection_geometry, bnbo_geometry) as property_bnbo_geometry
                    FROM batch_property_bnbo_raw
                    WHERE (field_id, block_id, cvr_number, year) IN (
                        SELECT field_id, block_id, cvr_number, year 
                        FROM batch_bnbo_features
                    )
                """)

                spatial_intersections = self.conn.execute(
                    "SELECT COUNT(*) FROM batch_property_bnbo_spatial"
                ).fetchone()[0]
                self.log.info(
                    f"  Found {spatial_intersections:,} property-BNBO spatial intersections"
                )

                # STEP 2: 3-way analysis - Property × BNBO × Water Projects (PR #545 compliant)
                self.log.info("  Step 2: Property-BNBO × Water projects (3-way spatial analysis)")

                # Get water project intersections for this batch's BNBO features
                self.conn.execute("""
                    CREATE OR REPLACE TABLE batch_water_bnbo_intersections AS
                    SELECT 
                        w.project_id,
                        w.bnbo_id,
                        w.status_category,
                        w.intersection_area_m2 as water_bnbo_area_m2,
                        w.intersection_geometry as water_bnbo_geometry
                    FROM water_projects_bnbo_intersections w
                    WHERE w.bnbo_id IN (
                        SELECT DISTINCT bnbo_id FROM batch_property_bnbo_spatial
                    )
                """)

                water_bnbo_count = self.conn.execute(
                    "SELECT COUNT(*) FROM batch_water_bnbo_intersections"
                ).fetchone()[0]
                self.log.info(
                    f"  Found {water_bnbo_count:,} water-BNBO intersections for 3-way analysis"
                )

                if water_bnbo_count > 0:
                    # Pure spatial join with SINGLE predicate (PR #545 compliant)
                    self.conn.execute("""
                        CREATE OR REPLACE TABLE batch_3way_raw AS
                        SELECT 
                            pb.field_id,
                            pb.block_id,
                            pb.cvr_number,
                            pb.year,
                            pb.bfe_number,
                            pb.bnbo_id,
                            pb.status_category,
                            pb.property_bnbo_area_m2,
                            wb.project_id,
                            ST_Area_Spheroid(ST_Intersection(pb.property_bnbo_geometry, wb.water_bnbo_geometry)) as property_bnbo_water_area_m2
                        FROM batch_property_bnbo_spatial pb
                        JOIN batch_water_bnbo_intersections wb ON ST_Intersects(pb.property_bnbo_geometry, wb.water_bnbo_geometry)
                        WHERE ST_Area_Spheroid(ST_Intersection(pb.property_bnbo_geometry, wb.water_bnbo_geometry)) > 10
                    """)

                    # Filter to matching BNBO features (equality constraints applied after spatial join)
                    self.conn.execute("""
                        CREATE OR REPLACE TABLE batch_property_bnbo_water_analysis AS
                        SELECT 
                            field_id,
                            block_id,
                            cvr_number,
                            year,
                            bfe_number,
                            bnbo_id,
                            status_category,
                            property_bnbo_area_m2,
                            SUM(property_bnbo_water_area_m2) as property_bnbo_covered_by_water_m2,
                            property_bnbo_area_m2 - SUM(property_bnbo_water_area_m2) as property_bnbo_not_covered_by_water_m2
                        FROM batch_3way_raw
                        WHERE bnbo_id IN (SELECT DISTINCT bnbo_id FROM batch_property_bnbo_spatial)
                        GROUP BY field_id, block_id, cvr_number, year, bfe_number, bnbo_id, status_category, property_bnbo_area_m2
                    """)

                    three_way_count = self.conn.execute(
                        "SELECT COUNT(*) FROM batch_property_bnbo_water_analysis"
                    ).fetchone()[0]
                    self.log.info(
                        f"  Created {three_way_count:,} property-BNBO-water analysis records"
                    )

                    # Handle property-BNBO areas NOT covered by water projects
                    self.conn.execute("""
                        INSERT INTO batch_property_bnbo_water_analysis
                        SELECT 
                            field_id,
                            block_id,
                            cvr_number,
                            year,
                            bfe_number,
                            bnbo_id,
                            status_category,
                            property_bnbo_area_m2,
                            0 as property_bnbo_covered_by_water_m2,
                            property_bnbo_area_m2 as property_bnbo_not_covered_by_water_m2
                        FROM batch_property_bnbo_spatial
                        WHERE bnbo_id NOT IN (
                            SELECT DISTINCT bnbo_id FROM batch_property_bnbo_water_analysis
                        )
                    """)
                else:
                    # No water project intersections - all property BNBO is uncovered
                    self.conn.execute("""
                        CREATE OR REPLACE TABLE batch_property_bnbo_water_analysis AS
                        SELECT 
                            field_id,
                            block_id,
                            cvr_number,
                            year,
                            bfe_number,
                            bnbo_id,
                            status_category,
                            property_bnbo_area_m2,
                            0 as property_bnbo_covered_by_water_m2,
                            property_bnbo_area_m2 as property_bnbo_not_covered_by_water_m2
                        FROM batch_property_bnbo_spatial
                    """)
            else:
                # No BNBO features for spatial analysis, create empty tables for consistency
                self.conn.execute("""
                    CREATE OR REPLACE TABLE batch_property_bnbo_spatial AS
                    SELECT 
                        CAST(NULL AS VARCHAR) as field_id,
                        CAST(NULL AS VARCHAR) as block_id,
                        CAST(NULL AS VARCHAR) as cvr_number,
                        CAST(NULL AS INTEGER) as year,
                        CAST(NULL AS VARCHAR) as bfe_number,
                        CAST(NULL AS DOUBLE) as property_intersection_area_m2,
                        CAST(NULL AS VARCHAR) as bnbo_id,
                        CAST(NULL AS VARCHAR) as status_category,
                        CAST(NULL AS DOUBLE) as property_bnbo_area_m2
                    WHERE FALSE
                """)

                self.conn.execute("""
                    CREATE OR REPLACE TABLE batch_property_bnbo_water_analysis AS
                    SELECT 
                        CAST(NULL AS VARCHAR) as field_id,
                        CAST(NULL AS VARCHAR) as block_id,
                        CAST(NULL AS VARCHAR) as cvr_number,
                        CAST(NULL AS INTEGER) as year,
                        CAST(NULL AS VARCHAR) as bfe_number,
                        CAST(NULL AS VARCHAR) as bnbo_id,
                        CAST(NULL AS VARCHAR) as status_category,
                        CAST(NULL AS DOUBLE) as property_bnbo_area_m2,
                        CAST(NULL AS DOUBLE) as property_bnbo_covered_by_water_m2,
                        CAST(NULL AS DOUBLE) as property_bnbo_not_covered_by_water_m2
                    WHERE FALSE
                """)

            # Aggregate results per field
            self.log.info("  Aggregating property-BNBO analysis per field")
            self.conn.execute("""
                INSERT INTO final_bnbo_analysis
                SELECT 
                    b.field_id,
                    b.block_id,
                    b.cvr_number,
                    b.year,
                    b.geometry,
                    b.field_area_m2,
                    
                    -- BNBO analysis data
                    b.total_bnbo_area_m2,
                    b.bnbo_covered_by_water_projects_m2,
                    b.bnbo_covered_by_water_projects_pct,
                    b.bnbo_not_covered_by_water_projects_pct,
                    b.field_bnbo_coverage_pct,
                    b.dominant_bnbo_status,
                    
                    -- Property information (aggregated per field)
                    COALESCE(p.property_count, 0) as property_count,
                    COALESCE(p.total_property_intersection_area_m2, 0) as total_property_intersection_area_m2,
                    COALESCE(p.avg_property_area_share_pct, 0) as avg_property_area_share_pct,
                    COALESCE(p.max_property_area_share_pct, 0) as max_property_area_share_pct,
                    COALESCE(p.primary_bfe_number, NULL) as primary_bfe_number,
                    
                    -- Property-BNBO spatial analysis
                    COALESCE(ps.property_bnbo_intersection_area_m2, 0) as property_bnbo_intersection_area_m2,
                    CASE 
                        WHEN COALESCE(p.total_property_intersection_area_m2, 0) > 0 
                        THEN (COALESCE(ps.property_bnbo_intersection_area_m2, 0) / p.total_property_intersection_area_m2) * 100
                        ELSE 0 
                    END as property_bnbo_coverage_pct,
                    COALESCE(ps.properties_with_bnbo_count, 0) as properties_with_bnbo_count,
                    COALESCE(ps.bnbo_property_owners, NULL) as bnbo_property_owners,
                    
                    -- Property-level BNBO water project coverage analysis
                    COALESCE(pwa.property_bnbo_covered_by_water_m2, 0) as property_bnbo_covered_by_water_m2,
                    COALESCE(pwa.property_bnbo_not_covered_by_water_m2, 0) as property_bnbo_not_covered_by_water_m2,
                    CASE 
                        WHEN COALESCE(ps.property_bnbo_intersection_area_m2, 0) > 0 
                        THEN (COALESCE(pwa.property_bnbo_covered_by_water_m2, 0) / ps.property_bnbo_intersection_area_m2) * 100
                        ELSE 0 
                    END as property_bnbo_water_coverage_pct,
                    COALESCE(pwa.properties_with_covered_bnbo_count, 0) as properties_with_covered_bnbo_count,
                    COALESCE(pwa.properties_with_uncovered_bnbo_count, 0) as properties_with_uncovered_bnbo_count,
                    COALESCE(pwa.covered_bnbo_property_owners, NULL) as covered_bnbo_property_owners,
                    COALESCE(pwa.uncovered_bnbo_property_owners, NULL) as uncovered_bnbo_property_owners
                    
                FROM fields_batch b
                LEFT JOIN (
                    SELECT 
                        field_id, block_id, cvr_number, year,
                        COUNT(*) as property_count,
                        SUM(intersection_area_m2) as total_property_intersection_area_m2,
                        AVG(field_area_share_pct) as avg_property_area_share_pct,
                        MAX(field_area_share_pct) as max_property_area_share_pct,
                        (
                            SELECT bfe_number 
                            FROM batch_property_intersections bp2 
                            WHERE bp2.field_id = bp.field_id 
                            AND bp2.block_id = bp.block_id 
                            AND bp2.cvr_number = bp.cvr_number
                            ORDER BY bp2.intersection_area_m2 DESC 
                            LIMIT 1
                        ) as primary_bfe_number
                    FROM batch_property_intersections bp
                    GROUP BY field_id, block_id, cvr_number, year
                ) p ON b.field_id = p.field_id 
                    AND b.block_id = p.block_id 
                    AND b.cvr_number = p.cvr_number 
                    AND b.year = p.year
                LEFT JOIN (
                    SELECT 
                        field_id, block_id, cvr_number, year,
                        SUM(property_bnbo_area_m2) as property_bnbo_intersection_area_m2,
                        COUNT(DISTINCT bfe_number) as properties_with_bnbo_count,
                        STRING_AGG(DISTINCT bfe_number, ', ') as bnbo_property_owners
                    FROM batch_property_bnbo_spatial
                    GROUP BY field_id, block_id, cvr_number, year
                ) ps ON b.field_id = ps.field_id 
                    AND b.block_id = ps.block_id 
                    AND b.cvr_number = ps.cvr_number 
                    AND b.year = ps.year
                LEFT JOIN (
                    SELECT 
                        field_id, block_id, cvr_number, year,
                        SUM(property_bnbo_covered_by_water_m2) as property_bnbo_covered_by_water_m2,
                        SUM(property_bnbo_not_covered_by_water_m2) as property_bnbo_not_covered_by_water_m2,
                        COUNT(DISTINCT CASE WHEN property_bnbo_covered_by_water_m2 > 0 THEN bfe_number END) as properties_with_covered_bnbo_count,
                        COUNT(DISTINCT CASE WHEN property_bnbo_not_covered_by_water_m2 > 0 THEN bfe_number END) as properties_with_uncovered_bnbo_count,
                        STRING_AGG(DISTINCT CASE WHEN property_bnbo_covered_by_water_m2 > 0 THEN bfe_number END, ', ') as covered_bnbo_property_owners,
                        STRING_AGG(DISTINCT CASE WHEN property_bnbo_not_covered_by_water_m2 > 0 THEN bfe_number END, ', ') as uncovered_bnbo_property_owners
                    FROM batch_property_bnbo_water_analysis
                    GROUP BY field_id, block_id, cvr_number, year
                ) pwa ON b.field_id = pwa.field_id 
                    AND b.block_id = pwa.block_id 
                    AND b.cvr_number = pwa.cvr_number 
                    AND b.year = pwa.year
            """)

            # Clean up batch tables
            self.conn.execute("DROP TABLE IF EXISTS fields_batch")
            self.conn.execute("DROP TABLE IF EXISTS batch_property_intersections")
            self.conn.execute("DROP TABLE IF EXISTS batch_bnbo_features")
            self.conn.execute("DROP TABLE IF EXISTS batch_property_bnbo_raw")
            self.conn.execute("DROP TABLE IF EXISTS batch_property_bnbo_spatial")
            self.conn.execute("DROP TABLE IF EXISTS batch_water_bnbo_intersections")
            self.conn.execute("DROP TABLE IF EXISTS batch_3way_raw")
            self.conn.execute("DROP TABLE IF EXISTS batch_property_bnbo_water_analysis")

            # Memory cleanup
            if (batch_num + 1) % CONFIG.memory_cleanup_frequency == 0:
                import gc

                gc.collect()

        # Final count
        final_count = self.conn.execute("SELECT COUNT(*) FROM final_bnbo_analysis").fetchone()[0]
        self.log.info(f"✅ Processed {final_count:,} fields for final BNBO analysis")

        return {
            "total_fields": final_count,
        }

    def _save_output_data(self, result: Dict[str, Any]):
        """Save final BNBO analysis to GCS."""
        self._save_stage_output("final_bnbo_analysis", "final_bnbo")
