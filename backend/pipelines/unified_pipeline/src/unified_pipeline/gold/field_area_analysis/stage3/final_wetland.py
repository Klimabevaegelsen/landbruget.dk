"""Stage 3B: Final Wetland Analysis - Optimized with Pre-computed Intersections

Combine wetland water coverage analysis with property ownership using pre-computed intersection geometries.
SPEED OPTIMIZATION: Uses Stage 2B field-wetland intersections instead of expensive spatial joins.

OPTIMIZED APPROACH:
- Use Stage 1C property/field intersections (with intersection_geometry)
- Use Stage 2B field-wetland intersections (pre-computed intersection geometries)
- Use Stage 2B field-level wetland coverage (aggregated results)
- Geometric intersection: Property intersections × Field-wetland intersections (NO SPATIAL JOIN!)
- Field-level join: Apply water coverage ratios from Stage 2B

ACHIEVES THE NESTED STRUCTURE:
- field A
  -- property 1
     --- wetland area
     --- wetland area covered by water projects
     --- wetland area not covered by water projects
  -- property 2
     --- wetland area
     --- wetland area covered by water projects
     --- wetland area not covered by water projects

Optimized for DuckDB Spatial v1.2.2 with pre-computed intersection geometries.
"""

from typing import Any, Dict

from ..base import FieldAnalysisStageBase, FieldAnalysisStageConfig
from ..config import CONFIG


class FinalWetlandAnalysis(FieldAnalysisStageBase):
    """Combine wetland analysis with property ownership using foundation data approach."""

    def __init__(self, config: FieldAnalysisStageConfig = None):
        if config is None:
            config = FieldAnalysisStageConfig()
        super().__init__(config, "Stage 3B: Final Wetland Analysis - Foundation Data")

    def _load_input_data(self):
        """Load foundation data from previous stages."""
        # Load field-level wetland coverage from Stage 2B
        stage2b_dataset = CONFIG.stage_outputs["fields_wetland_water"]
        stage2b_path = self._get_latest_gold_path(stage2b_dataset)
        self.gcs_access.query_parquet_direct(stage2b_path, "SELECT *", "fields_wetland_water")

        # Load property/field intersections from Stage 1C (includes intersection_geometry)
        stage1c_dataset = CONFIG.stage_outputs["field_property_intersections"]
        stage1c_path = self._get_latest_gold_path(stage1c_dataset)
        self.gcs_access.query_parquet_direct(
            stage1c_path, "SELECT *", "field_property_intersections"
        )

        # Load water project/wetland intersections from Stage 1B (foundation data with wetland_id)
        stage1b_dataset = CONFIG.stage_outputs["water_projects_wetlands_intersections"]
        stage1b_path = self._get_latest_gold_path(stage1b_dataset)
        self.gcs_access.query_parquet_direct(
            stage1b_path, "SELECT *", "water_projects_wetlands_intersections"
        )

        # Load field-wetland intersections from Stage 2B (SPEED OPTIMIZATION!)
        stage2b_intersections_dataset = CONFIG.stage_outputs["field_wetland_intersections"]
        stage2b_intersections_path = self._get_latest_gold_path(stage2b_intersections_dataset)
        self.gcs_access.query_parquet_direct(
            stage2b_intersections_path, "SELECT *", "field_wetland_intersections"
        )

        self.log.info(
            "✅ SPEED OPTIMIZATION: Using pre-computed field-wetland intersections from Stage 2B"
        )
        self.log.info("✅ No more expensive Property × Environmental spatial joins!")

    async def _execute_stage_processing(self) -> Dict[str, Any]:
        """
        Create property-level wetland analysis using foundation data approach.

        FOUNDATION DATA STRATEGY:
        1. Start with field-level wetland coverage (Stage 2B)
        2. Get property intersections for those fields (Stage 1C)
        3. Single spatial join: Property intersections × Wetland features
        4. Calculate property-level wetland areas per field
        5. Use existing water project coverage using wetland_id from Stage 1B
        6. Aggregate to achieve nested field→property→environmental structure

        DuckDB Spatial v1.2.2 COMPLIANCE:
        - Only single spatial predicate (ST_Intersects)
        - No complex 3-way spatial joins
        - Use foundation data and ID-based joins where possible
        """

        self.log.info("🎯 FOUNDATION DATA APPROACH: Property-level wetland analysis")
        self.log.info("✅ DuckDB Spatial v1.2.2: Single spatial predicates only")

        # Get total field count for batching
        total_fields = self.conn.execute("SELECT COUNT(*) FROM fields_wetland_water").fetchone()[0]
        batch_size = CONFIG.stage3_batch_size
        num_batches = (total_fields + batch_size - 1) // batch_size

        self.log.info(
            f"Processing {total_fields:,} fields in {num_batches} batches of {batch_size:,}"
        )

        # Initialize result table with nested structure
        self.conn.execute("""
            CREATE OR REPLACE TABLE final_wetland_analysis AS
            SELECT 
                CAST(NULL AS VARCHAR) as field_id,
                CAST(NULL AS VARCHAR) as block_id,
                CAST(NULL AS VARCHAR) as cvr_number,
                CAST(NULL AS INTEGER) as year,
                CAST(NULL AS GEOMETRY) as geometry,
                CAST(NULL AS DOUBLE) as field_area_m2,
                
                -- Field-level wetland data (from Stage 2B)
                CAST(NULL AS DOUBLE) as total_wetland_area_m2,
                CAST(NULL AS DOUBLE) as wetland_covered_by_water_projects_m2,
                CAST(NULL AS DOUBLE) as wetland_covered_by_water_projects_pct,
                CAST(NULL AS DOUBLE) as wetland_not_covered_by_water_projects_pct,
                CAST(NULL AS DOUBLE) as field_wetland_coverage_pct,
                
                -- Property ownership summary
                CAST(NULL AS INTEGER) as property_count,
                CAST(NULL AS DOUBLE) as total_property_intersection_area_m2,
                CAST(NULL AS VARCHAR) as primary_bfe_number,
                
                -- Property-level wetland breakdown (NESTED STRUCTURE)
                CAST(NULL AS VARCHAR) as property_wetland_breakdown,  -- JSON: {bfe_number: {wetland_area_m2, covered_m2, uncovered_m2}}
                CAST(NULL AS DOUBLE) as total_property_wetland_area_m2,
                CAST(NULL AS DOUBLE) as total_property_wetland_covered_m2,
                CAST(NULL AS DOUBLE) as total_property_wetland_uncovered_m2,
                CAST(NULL AS INTEGER) as properties_with_wetland_count,
                CAST(NULL AS VARCHAR) as wetland_property_owners
            WHERE FALSE
        """)

        # Process each batch
        for batch_num in range(num_batches):
            offset = batch_num * batch_size
            progress_pct = ((batch_num + 1) / num_batches) * 100
            self.log.info(f"📦 Batch {batch_num + 1}/{num_batches} - {progress_pct:.1f}% complete")

            # Create field batch (only fields with wetlands)
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE fields_batch AS
                SELECT * FROM fields_wetland_water
                WHERE total_wetland_area_m2 > 0
                LIMIT {batch_size} OFFSET {offset}
            """)

            batch_count = self.conn.execute("SELECT COUNT(*) FROM fields_batch").fetchone()[0]
            if batch_count == 0:
                break

            self.log.info(
                f"  Processing {batch_count:,} fields with wetlands in batch {batch_num + 1}"
            )

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

            if property_count > 0:
                # DuckDB Spatial PR #545 COMPLIANCE: Separate JOIN and spatial filtering
                self.log.info(
                    "  STEP 1: Property intersections × Field-wetland intersections (ID-based JOIN)"
                )
                self.conn.execute("""
                    CREATE OR REPLACE TABLE batch_property_wetland_raw AS
                    SELECT 
                        p.field_id,
                        p.block_id,
                        p.cvr_number,
                        p.year,
                        p.bfe_number,
                        p.intersection_area_m2 as property_intersection_area_m2,
                        p.intersection_geometry as property_geometry,
                        fw.toerv_pct,
                        fw.field_wetland_intersection_geometry as wetland_geometry
                    FROM batch_property_intersections p
                    JOIN field_wetland_intersections fw ON p.field_id = fw.field_id 
                        AND p.block_id = fw.block_id 
                        AND p.cvr_number = fw.cvr_number
                        AND p.year = fw.year
                """)

                # Post-JOIN spatial processing (NO SPATIAL WHERE CLAUSES)
                self.log.info("  STEP 2: Area calculation (no spatial filtering)")
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
                        ST_Area_Spheroid(ST_Intersection(property_geometry, wetland_geometry)) as property_wetland_area_m2
                    FROM batch_property_wetland_raw
                """)

                spatial_count = self.conn.execute(
                    "SELECT COUNT(*) FROM batch_property_wetland_spatial"
                ).fetchone()[0]
                self.log.info(f"  Found {spatial_count:,} property-wetland spatial intersections")

                # Calculate water project coverage using field-level coverage ratios from Stage 2B
                self.log.info(
                    "  Calculating water project coverage using field-level ratios from Stage 2B"
                )

                # Apply field-level coverage ratios to property-wetland areas
                self.conn.execute("""
                    CREATE OR REPLACE TABLE batch_property_wetland_water AS
                    SELECT 
                        pw.field_id,
                        pw.block_id,
                        pw.cvr_number,
                        pw.year,
                        pw.bfe_number,
                        pw.toerv_pct,
                        pw.property_wetland_area_m2,
                        -- Apply field-level water coverage ratio from Stage 2B
                        pw.property_wetland_area_m2 * (fw.wetland_covered_by_water_projects_pct / 100.0) as property_wetland_covered_m2,
                        pw.property_wetland_area_m2 * (1 - (fw.wetland_covered_by_water_projects_pct / 100.0)) as property_wetland_uncovered_m2
                    FROM batch_property_wetland_spatial pw
                    JOIN fields_wetland_water fw ON pw.field_id = fw.field_id 
                        AND pw.block_id = fw.block_id 
                        AND pw.cvr_number = fw.cvr_number 
                        AND pw.year = fw.year
                """)

                water_analysis_count = self.conn.execute(
                    "SELECT COUNT(*) FROM batch_property_wetland_water"
                ).fetchone()[0]
                self.log.info(
                    f"  Created {water_analysis_count:,} property-wetland-water analysis records using wetland_id"
                )

                # Aggregate to create nested property breakdown per field
                self.log.info("  Creating nested property-level wetland breakdown per field")
                self.conn.execute("""
                    CREATE OR REPLACE TABLE batch_property_breakdown AS
                    SELECT 
                        field_id,
                        block_id,
                        cvr_number,
                        year,
                        -- Create JSON breakdown: {bfe_number: {wetland_area_m2, covered_m2, uncovered_m2}}
                        '{' || STRING_AGG(
                            '"' || bfe_number || '": {' ||
                            '"wetland_area_m2": ' || ROUND(total_wetland_area, 2) || ', ' ||
                            '"covered_m2": ' || ROUND(total_covered, 2) || ', ' ||
                            '"uncovered_m2": ' || ROUND(total_uncovered, 2) ||
                            '}', ', '
                        ) || '}' as property_wetland_breakdown,
                        
                        -- Summary metrics
                        SUM(total_wetland_area) as total_property_wetland_area_m2,
                        SUM(total_covered) as total_property_wetland_covered_m2,
                        SUM(total_uncovered) as total_property_wetland_uncovered_m2,
                        COUNT(DISTINCT bfe_number) as properties_with_wetland_count,
                        STRING_AGG(DISTINCT bfe_number, ', ') as wetland_property_owners
                    FROM (
                        SELECT 
                            field_id, block_id, cvr_number, year, bfe_number,
                            SUM(property_wetland_area_m2) as total_wetland_area,
                            SUM(property_wetland_covered_m2) as total_covered,
                            SUM(property_wetland_uncovered_m2) as total_uncovered
                        FROM batch_property_wetland_water
                        GROUP BY field_id, block_id, cvr_number, year, bfe_number
                    ) property_totals
                    GROUP BY field_id, block_id, cvr_number, year
                """)

                breakdown_count = self.conn.execute(
                    "SELECT COUNT(*) FROM batch_property_breakdown"
                ).fetchone()[0]
                self.log.info(f"  Created {breakdown_count:,} field-level property breakdowns")
            else:
                # No properties for this batch - create empty breakdown table
                self.conn.execute("""
                    CREATE OR REPLACE TABLE batch_property_breakdown AS
                    SELECT 
                        field_id, block_id, cvr_number, year,
                        '{}' as property_wetland_breakdown,
                        0 as total_property_wetland_area_m2,
                        0 as total_property_wetland_covered_m2,
                        0 as total_property_wetland_uncovered_m2,
                        0 as properties_with_wetland_count,
                        NULL as wetland_property_owners
                    FROM fields_batch
                    WHERE FALSE
                """)

            # Combine field-level data with property breakdown
            self.log.info("  Combining field-level wetland data with property breakdown")
            self.conn.execute("""
                INSERT INTO final_wetland_analysis
                SELECT 
                    b.field_id,
                    b.block_id,
                    b.cvr_number,
                    b.year,
                    b.geometry,
                    b.field_area_m2,
                    
                    -- Field-level wetland data (from Stage 2B)
                    b.total_wetland_area_m2,
                    b.wetland_covered_by_water_projects_m2,
                    b.wetland_covered_by_water_projects_pct,
                    b.wetland_not_covered_by_water_projects_pct,
                    b.field_wetland_coverage_pct,
                    
                    -- Property ownership summary
                    COALESCE(ps.property_count, 0) as property_count,
                    COALESCE(ps.total_property_intersection_area_m2, 0) as total_property_intersection_area_m2,
                    COALESCE(ps.primary_bfe_number, NULL) as primary_bfe_number,
                    
                    -- Property-level wetland breakdown (NESTED STRUCTURE)
                    COALESCE(pb.property_wetland_breakdown, '{}') as property_wetland_breakdown,
                    COALESCE(pb.total_property_wetland_area_m2, 0) as total_property_wetland_area_m2,
                    COALESCE(pb.total_property_wetland_covered_m2, 0) as total_property_wetland_covered_m2,
                    COALESCE(pb.total_property_wetland_uncovered_m2, 0) as total_property_wetland_uncovered_m2,
                    COALESCE(pb.properties_with_wetland_count, 0) as properties_with_wetland_count,
                    COALESCE(pb.wetland_property_owners, NULL) as wetland_property_owners
                    
                FROM fields_batch b
                LEFT JOIN (
                    SELECT 
                        field_id, block_id, cvr_number, year,
                        COUNT(*) as property_count,
                        SUM(intersection_area_m2) as total_property_intersection_area_m2,
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
                ) ps ON b.field_id = ps.field_id 
                    AND b.block_id = ps.block_id 
                    AND b.cvr_number = ps.cvr_number 
                    AND b.year = ps.year
                LEFT JOIN batch_property_breakdown pb ON b.field_id = pb.field_id 
                    AND b.block_id = pb.block_id 
                    AND b.cvr_number = pb.cvr_number 
                    AND b.year = pb.year
            """)

            # Clean up batch tables
            self.conn.execute("DROP TABLE IF EXISTS fields_batch")
            self.conn.execute("DROP TABLE IF EXISTS batch_property_intersections")
            self.conn.execute("DROP TABLE IF EXISTS batch_property_wetland_raw")
            self.conn.execute("DROP TABLE IF EXISTS batch_property_wetland_spatial")
            self.conn.execute("DROP TABLE IF EXISTS batch_property_wetland_water")
            self.conn.execute("DROP TABLE IF EXISTS batch_property_breakdown")

            # Memory cleanup
            if (batch_num + 1) % CONFIG.stage2_memory_cleanup_frequency == 0:
                import gc

                gc.collect()
                self.log.info(f"  🧹 Memory cleanup after batch {batch_num + 1}")

        # Get final statistics
        final_count = self.conn.execute("SELECT COUNT(*) FROM final_wetland_analysis").fetchone()[0]

        # Sample the nested structure
        sample_breakdown = self.conn.execute("""
            SELECT property_wetland_breakdown 
            FROM final_wetland_analysis 
            WHERE property_wetland_breakdown != '{}' 
            LIMIT 1
        """).fetchone()

        if sample_breakdown:
            self.log.info(f"✅ Sample property breakdown: {sample_breakdown[0][:200]}...")

        self.log.info(
            f"✅ Created {final_count:,} final wetland analysis records with nested property structure"
        )
        self.log.info(
            "🎯 ACHIEVED: field → property → wetland (area/covered/uncovered) nested breakdown"
        )

        return {
            "final_records": final_count,
            "batches_processed": num_batches,
            "foundation_data_approach": True,
            "single_spatial_predicates": True,
        }

    def _save_output_data(self, result: Dict[str, Any]):
        """Save final wetland analysis with nested property structure."""
        self._save_stage_output("final_wetland_analysis", "final_wetland_analysis")
