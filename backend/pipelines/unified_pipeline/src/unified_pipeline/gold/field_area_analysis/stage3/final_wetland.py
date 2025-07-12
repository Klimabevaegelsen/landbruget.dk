"""Stage 3B: Final Wetland Analysis

Combine wetland water coverage analysis with pre-filtered properties.
Creates comprehensive wetland analysis including property-level water project coverage.

KEY FEATURES:
- Property-wetland spatial analysis (which properties own wetlands)
- Property-level wetland water project coverage analysis
- Detailed metrics for covered vs uncovered wetland areas by property ownership
- Answers: "How much wetlands covered by water projects is owned by property X?"

Optimized for DuckDB Spatial v1.2.2 with single spatial predicates.
"""

from typing import Any, Dict

from ..base import FieldAnalysisStageBase, FieldAnalysisStageConfig
from ..config import CONFIG


class FinalWetlandAnalysis(FieldAnalysisStageBase):
    """Combine wetland analysis with pre-filtered properties."""

    def __init__(self, config: FieldAnalysisStageConfig = None):
        if config is None:
            config = FieldAnalysisStageConfig()
        super().__init__(config, "Stage 3B: Final Wetland Analysis")

    def _load_input_data(self):
        """Load wetland analysis from Stage 2B and water project intersections from Stage 1B."""
        # Load wetland water coverage from Stage 2B
        stage2b_path = f"gs://{CONFIG.bucket}/gold/{CONFIG.stage_outputs['fields_wetland_water']}/{CONFIG.stage_outputs['fields_wetland_water']}.parquet"
        self.gcs_access.query_parquet_direct(stage2b_path, "SELECT *", "fields_wetland_water")

        # Load pre-filtered field-property intersections from Stage 1C
        stage1c_path = f"gs://{CONFIG.bucket}/gold/{CONFIG.stage_outputs['field_property_intersections']}/{CONFIG.stage_outputs['field_property_intersections']}.parquet"
        self.gcs_access.query_parquet_direct(
            stage1c_path, "SELECT *", "field_property_intersections"
        )

        # Load water project × wetland intersections from Stage 1B (includes toerv_pct)
        stage1b_path = f"gs://{CONFIG.bucket}/gold/{CONFIG.stage_outputs['water_projects_wetlands_intersections']}/{CONFIG.stage_outputs['water_projects_wetlands_intersections']}.parquet"
        self.gcs_access.query_parquet_direct(
            stage1b_path, "SELECT *", "water_projects_wetlands_intersections"
        )

        # Load original wetlands data for spatial analysis (needed for property-wetland intersections)
        self._load_silver_dataset(CONFIG.wetlands_dataset, "wetlands_raw")

    async def _execute_stage_processing(self) -> Dict[str, Any]:
        """
        Combine wetland analysis with property ownership through enhanced spatial analysis.

        ENHANCED SPATIAL ANALYSIS APPROACH (PR #545 compliant):
        1. Load field wetland data from Stage 2B
        2. Load property intersection geometries from Stage 1C
        3. Load water project × wetland foundation data from Stage 1B
        4. Spatial join: Property intersections × Wetland features (single ST_Intersects)
        5. Enhanced analysis: Property × Wetland × Water Project Coverage
        6. Aggregate by field with comprehensive property-wetland-water metrics

        ANSWERS QUESTIONS LIKE:
        - Which properties own wetlands?
        - How much wetland area is owned by property X?
        - How much of property X's wetlands are covered by water projects?
        - How much of property X's wetlands are NOT covered by water projects?
        """

        self.log.info("🎯 SPATIAL ANALYSIS: Connecting wetland features to property ownership")
        self.log.info("✅ Following DuckDB Spatial PR #545: Single spatial predicate joins")

        # Get total field count for batching
        total_fields = self.conn.execute("SELECT COUNT(*) FROM fields_wetland_water").fetchone()[0]
        batch_size = CONFIG.stage3_batch_size
        num_batches = (total_fields + batch_size - 1) // batch_size

        self.log.info(
            f"Processing {total_fields:,} fields in {num_batches} batches of {batch_size:,}"
        )

        # Initialize result table
        self.conn.execute("""
            CREATE OR REPLACE TABLE final_wetland_analysis AS
            SELECT 
                CAST(NULL AS VARCHAR) as field_id,
                CAST(NULL AS VARCHAR) as block_id,
                CAST(NULL AS VARCHAR) as cvr_number,
                CAST(NULL AS INTEGER) as year,
                CAST(NULL AS GEOMETRY) as geometry,
                CAST(NULL AS DOUBLE) as field_area_m2,
                
                -- Wetland analysis data
                CAST(NULL AS DOUBLE) as total_wetland_area_m2,
                CAST(NULL AS DOUBLE) as wetland_covered_by_water_projects_m2,
                CAST(NULL AS DOUBLE) as wetland_covered_by_water_projects_pct,
                CAST(NULL AS DOUBLE) as wetland_not_covered_by_water_projects_pct,
                CAST(NULL AS DOUBLE) as field_wetland_coverage_pct,

                
                -- Property ownership analysis
                CAST(NULL AS INTEGER) as property_count,
                CAST(NULL AS DOUBLE) as total_property_intersection_area_m2,
                CAST(NULL AS DOUBLE) as avg_property_area_share_pct,
                CAST(NULL AS DOUBLE) as max_property_area_share_pct,
                CAST(NULL AS VARCHAR) as primary_bfe_number,
                
                -- Property-wetland spatial analysis
                CAST(NULL AS DOUBLE) as property_wetland_intersection_area_m2,
                CAST(NULL AS DOUBLE) as property_wetland_coverage_pct,
                CAST(NULL AS INTEGER) as properties_with_wetland_count,
                CAST(NULL AS VARCHAR) as wetland_property_owners,
                
                -- Property-level wetland water project coverage analysis
                CAST(NULL AS DOUBLE) as property_wetland_covered_by_water_m2,
                CAST(NULL AS DOUBLE) as property_wetland_not_covered_by_water_m2,
                CAST(NULL AS DOUBLE) as property_wetland_water_coverage_pct,
                CAST(NULL AS INTEGER) as properties_with_covered_wetland_count,
                CAST(NULL AS INTEGER) as properties_with_uncovered_wetland_count,
                CAST(NULL AS VARCHAR) as covered_wetland_property_owners,
                CAST(NULL AS VARCHAR) as uncovered_wetland_property_owners
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
                SELECT * FROM fields_wetland_water
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
                # Handle fields with no properties (just copy wetland data)
                self.conn.execute("""
                    INSERT INTO final_wetland_analysis
                    SELECT 
                        field_id, block_id, cvr_number, year, geometry, field_area_m2,
                        total_wetland_area_m2, wetland_covered_by_water_projects_m2, 
                        wetland_covered_by_water_projects_pct, wetland_not_covered_by_water_projects_pct,
                        field_wetland_coverage_pct,
                        0 as property_count, 0 as total_property_intersection_area_m2,
                        0 as avg_property_area_share_pct, 0 as max_property_area_share_pct,
                        NULL as primary_bfe_number,
                        0 as property_wetland_intersection_area_m2, 0 as property_wetland_coverage_pct,
                        0 as properties_with_wetland_count, NULL as wetland_property_owners,
                        0 as property_wetland_covered_by_water_m2, 0 as property_wetland_not_covered_by_water_m2,
                        0 as property_wetland_water_coverage_pct, 0 as properties_with_covered_wetland_count,
                        0 as properties_with_uncovered_wetland_count, NULL as covered_wetland_property_owners,
                        NULL as uncovered_wetland_property_owners
                    FROM fields_batch
                """)
                continue

            # SPATIAL ANALYSIS: Property intersections × Wetland features
            # Load wetland data for spatial analysis (with IDs from Stage 1B)
            self.conn.execute("""
                CREATE OR REPLACE TABLE batch_wetland_features AS
                SELECT 
                    b.field_id,
                    b.block_id,
                    b.cvr_number,
                    b.year,
                    ROW_NUMBER() OVER (ORDER BY w.toerv_pct, ST_X(ST_Centroid(w.geometry)), ST_Y(ST_Centroid(w.geometry))) as wetland_id,  -- Generate consistent IDs
                    w.toerv_pct,  -- Only keep meaningful wetland classification
                    w.geometry as wetland_geometry,
                    ST_Area_Spheroid(w.geometry) as wetland_area_m2
                FROM fields_batch b
                JOIN wetlands_raw w ON ST_Intersects(b.geometry, w.geometry)
                WHERE b.total_wetland_area_m2 > 0
            """)

            wetland_features_count = self.conn.execute(
                "SELECT COUNT(*) FROM batch_wetland_features"
            ).fetchone()[0]
            self.log.info(
                f"  Found {wetland_features_count:,} wetland features for spatial analysis"
            )

            if wetland_features_count > 0:
                # SINGLE SPATIAL JOIN: Property intersections × Wetland features (PR #545 compliant)
                self.log.info(
                    "  Performing spatial join: Property intersections × Wetland features"
                )
                # Step 1: Pure spatial join with SINGLE predicate (PR #545 compliant)
                self.conn.execute("""
                    CREATE OR REPLACE TABLE batch_spatial_raw AS
                    SELECT 
                        p.field_id,
                        p.block_id,
                        p.cvr_number,
                        p.year,
                        p.bfe_number,
                        p.intersection_area_m2 as property_intersection_area_m2,
                        p.intersection_geometry,
                        w.toerv_pct,
                        w.wetland_geometry,
                        ST_Area_Spheroid(ST_Intersection(p.intersection_geometry, w.wetland_geometry)) as property_wetland_area_m2
                    FROM batch_property_intersections p
                    JOIN batch_wetland_features w ON ST_Intersects(p.intersection_geometry, w.wetland_geometry)
                    WHERE ST_Area_Spheroid(ST_Intersection(p.intersection_geometry, w.wetland_geometry)) > 10
                """)

                # Step 2: Filter to matching fields (equality constraints applied after spatial join)
                self.conn.execute("""
                    CREATE OR REPLACE TABLE batch_property_wetland_spatial AS
                    SELECT 
                        field_id,
                        block_id,
                        cvr_number,
                        year,
                        bfe_number,
                        property_intersection_area_m2,
                        toerv_pct,
                        property_wetland_area_m2,
                        ST_Intersection(intersection_geometry, wetland_geometry) as property_wetland_geometry
                    FROM batch_spatial_raw
                    WHERE (field_id, block_id, cvr_number, year) IN (
                        SELECT field_id, block_id, cvr_number, year 
                        FROM batch_wetland_features
                    )
                """)

                spatial_intersections = self.conn.execute(
                    "SELECT COUNT(*) FROM batch_property_wetland_spatial"
                ).fetchone()[0]
                self.log.info(
                    f"  Found {spatial_intersections:,} property-wetland spatial intersections"
                )

                # STEP 2: Use existing wetland-water intersections from Stage 1B (efficient!)
                self.log.info("  Step 2: Using existing wetland-water intersections from Stage 1B")

                # Match property-wetland areas with existing water project coverage by wetland_id
                self.conn.execute("""
                    CREATE OR REPLACE TABLE batch_property_wetland_water_analysis AS
                    SELECT 
                        pw.field_id,
                        pw.block_id,
                        pw.cvr_number,
                        pw.year,
                        pw.bfe_number,
                        pw.toerv_pct,
                        pw.property_wetland_area_m2,
                        
                        -- Calculate covered area using existing intersection data
                        COALESCE(
                            pw.property_wetland_area_m2 * (
                                SELECT SUM(wwi.intersection_area_m2) / SUM(wwi.wetland_area_m2)
                                FROM water_projects_wetlands_intersections wwi 
                                WHERE wwi.wetland_id = bw.wetland_id
                            ), 0
                        ) as property_wetland_covered_by_water_m2,
                        
                        -- Calculate uncovered area
                        pw.property_wetland_area_m2 - COALESCE(
                            pw.property_wetland_area_m2 * (
                                SELECT SUM(wwi.intersection_area_m2) / SUM(wwi.wetland_area_m2)
                                FROM water_projects_wetlands_intersections wwi 
                                WHERE wwi.wetland_id = bw.wetland_id
                            ), 0
                        ) as property_wetland_not_covered_by_water_m2
                        
                    FROM batch_property_wetland_spatial pw
                    JOIN batch_wetland_features bw ON ST_Intersects(pw.property_wetland_geometry, bw.wetland_geometry)
                """)

                # Analysis already completed using existing foundation data
                water_analysis_count = self.conn.execute(
                    "SELECT COUNT(*) FROM batch_property_wetland_water_analysis"
                ).fetchone()[0]
                self.log.info(
                    f"  Created {water_analysis_count:,} property-wetland-water analysis records using existing foundation data"
                )
            else:
                # No wetland features for spatial analysis, create empty table for consistency
                self.conn.execute("""
                    CREATE OR REPLACE TABLE batch_property_wetland_water_analysis AS
                    SELECT 
                        CAST(NULL AS VARCHAR) as field_id,
                        CAST(NULL AS VARCHAR) as block_id,
                        CAST(NULL AS VARCHAR) as cvr_number,
                        CAST(NULL AS INTEGER) as year,
                        CAST(NULL AS VARCHAR) as bfe_number,
                        CAST(NULL AS DOUBLE) as property_intersection_area_m2,
                        CAST(NULL AS VARCHAR) as toerv_pct,
                        CAST(NULL AS DOUBLE) as property_wetland_area_m2,
                        CAST(NULL AS DOUBLE) as property_wetland_covered_by_water_m2,
                        CAST(NULL AS DOUBLE) as property_wetland_not_covered_by_water_m2
                    WHERE FALSE
                """)

            # Aggregate results per field
            self.log.info("  Aggregating property-wetland analysis per field")
            self.conn.execute("""
                INSERT INTO final_wetland_analysis
                SELECT 
                    b.field_id,
                    b.block_id,
                    b.cvr_number,
                    b.year,
                    b.geometry,
                    b.field_area_m2,
                    
                    -- Wetland analysis data
                    b.total_wetland_area_m2,
                    b.wetland_covered_by_water_projects_m2,
                    b.wetland_covered_by_water_projects_pct,
                    b.wetland_not_covered_by_water_projects_pct,
                    b.field_wetland_coverage_pct,
                    
                    -- Property information (aggregated per field)
                    COALESCE(p.property_count, 0) as property_count,
                    COALESCE(p.total_property_intersection_area_m2, 0) as total_property_intersection_area_m2,
                    COALESCE(p.avg_property_area_share_pct, 0) as avg_property_area_share_pct,
                    COALESCE(p.max_property_area_share_pct, 0) as max_property_area_share_pct,
                    COALESCE(p.primary_bfe_number, NULL) as primary_bfe_number,
                    
                    -- Property-wetland spatial analysis
                    COALESCE(ps.property_wetland_intersection_area_m2, 0) as property_wetland_intersection_area_m2,
                    CASE 
                        WHEN COALESCE(p.total_property_intersection_area_m2, 0) > 0 
                        THEN (COALESCE(ps.property_wetland_intersection_area_m2, 0) / p.total_property_intersection_area_m2) * 100
                        ELSE 0 
                    END as property_wetland_coverage_pct,
                    COALESCE(ps.properties_with_wetland_count, 0) as properties_with_wetland_count,
                    COALESCE(ps.wetland_property_owners, NULL) as wetland_property_owners,
                    
                    -- Property-level wetland water project coverage analysis
                    COALESCE(pwa.property_wetland_covered_by_water_m2, 0) as property_wetland_covered_by_water_m2,
                    COALESCE(pwa.property_wetland_not_covered_by_water_m2, 0) as property_wetland_not_covered_by_water_m2,
                    CASE 
                        WHEN COALESCE(ps.property_wetland_intersection_area_m2, 0) > 0 
                        THEN (COALESCE(pwa.property_wetland_covered_by_water_m2, 0) / ps.property_wetland_intersection_area_m2) * 100
                        ELSE 0 
                    END as property_wetland_water_coverage_pct,
                    COALESCE(pwa.properties_with_covered_wetland_count, 0) as properties_with_covered_wetland_count,
                    COALESCE(pwa.properties_with_uncovered_wetland_count, 0) as properties_with_uncovered_wetland_count,
                    COALESCE(pwa.covered_wetland_property_owners, NULL) as covered_wetland_property_owners,
                    COALESCE(pwa.uncovered_wetland_property_owners, NULL) as uncovered_wetland_property_owners
                    
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
                        SUM(property_wetland_area_m2) as property_wetland_intersection_area_m2,
                        COUNT(DISTINCT bfe_number) as properties_with_wetland_count,
                        STRING_AGG(DISTINCT bfe_number, ', ') as wetland_property_owners
                    FROM batch_property_wetland_spatial
                    GROUP BY field_id, block_id, cvr_number, year
                ) ps ON b.field_id = ps.field_id 
                    AND b.block_id = ps.block_id 
                    AND b.cvr_number = ps.cvr_number 
                    AND b.year = ps.year
                LEFT JOIN (
                    SELECT 
                        field_id, block_id, cvr_number, year,
                        SUM(property_wetland_covered_by_water_m2) as property_wetland_covered_by_water_m2,
                        SUM(property_wetland_not_covered_by_water_m2) as property_wetland_not_covered_by_water_m2,
                        COUNT(DISTINCT CASE WHEN property_wetland_covered_by_water_m2 > 0 THEN bfe_number END) as properties_with_covered_wetland_count,
                        COUNT(DISTINCT CASE WHEN property_wetland_not_covered_by_water_m2 > 0 THEN bfe_number END) as properties_with_uncovered_wetland_count,
                        STRING_AGG(DISTINCT CASE WHEN property_wetland_covered_by_water_m2 > 0 THEN bfe_number END, ', ') as covered_wetland_property_owners,
                        STRING_AGG(DISTINCT CASE WHEN property_wetland_not_covered_by_water_m2 > 0 THEN bfe_number END, ', ') as uncovered_wetland_property_owners
                    FROM batch_property_wetland_water_analysis
                    GROUP BY field_id, block_id, cvr_number, year
                ) pwa ON b.field_id = pwa.field_id 
                    AND b.block_id = pwa.block_id 
                    AND b.cvr_number = pwa.cvr_number 
                    AND b.year = pwa.year
            """)

            # Clean up batch tables
            self.conn.execute("DROP TABLE IF EXISTS fields_batch")
            self.conn.execute("DROP TABLE IF EXISTS batch_property_intersections")
            self.conn.execute("DROP TABLE IF EXISTS batch_wetland_features")
            self.conn.execute("DROP TABLE IF EXISTS batch_spatial_raw")
            self.conn.execute("DROP TABLE IF EXISTS batch_property_wetland_spatial")

            self.conn.execute("DROP TABLE IF EXISTS batch_property_wetland_water_analysis")

            # Memory cleanup
            if (batch_num + 1) % CONFIG.memory_cleanup_frequency == 0:
                import gc

                gc.collect()

        # Final count
        final_count = self.conn.execute("SELECT COUNT(*) FROM final_wetland_analysis").fetchone()[0]
        self.log.info(f"✅ Processed {final_count:,} fields for final wetland analysis")

        return {
            "total_fields": final_count,
        }

    def _save_output_data(self, result: Dict[str, Any]):
        """Save final wetland analysis to GCS."""
        self._save_stage_output("final_wetland_analysis", "final_wetland")
