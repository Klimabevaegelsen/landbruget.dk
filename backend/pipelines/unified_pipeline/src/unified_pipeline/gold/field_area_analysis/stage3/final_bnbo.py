"""Stage 3A: Final BNBO Analysis - Optimized with Pre-computed Intersections

Combine BNBO water coverage analysis with property ownership using pre-computed intersection geometries.
SPEED OPTIMIZATION: Uses Stage 2A field-BNBO intersections instead of expensive spatial joins.

OPTIMIZED APPROACH:
- Use Stage 1C property/field intersections (with intersection_geometry)
- Use Stage 2A field-BNBO intersections (pre-computed intersection geometries)
- Use Stage 2A field-level BNBO coverage (aggregated results)
- Geometric intersection: Property intersections × Field-BNBO intersections (NO SPATIAL JOIN!)
- Field-level join: Apply water coverage ratios from Stage 2A

ACHIEVES THE NESTED STRUCTURE:
- field A
  -- property 1
     --- bnbo area
     --- bnbo area covered by water projects
     --- bnbo area not covered by water projects
  -- property 2
     --- bnbo area
     --- bnbo area covered by water projects
     --- bnbo area not covered by water projects

Optimized for DuckDB Spatial v1.2.2 with pre-computed intersection geometries.
"""

from typing import Any, Dict

from ..base import FieldAnalysisStageBase, FieldAnalysisStageConfig
from ..config import CONFIG


class FinalBNBOAnalysis(FieldAnalysisStageBase):
    """Combine BNBO analysis with property ownership using foundation data approach."""

    def __init__(self, config: FieldAnalysisStageConfig = None):
        if config is None:
            config = FieldAnalysisStageConfig()
        super().__init__(config, "Stage 3A: Final BNBO Analysis - Foundation Data")

    def _load_input_data(self):
        """Load foundation data from previous stages."""
        # Get year-aware dataset names
        updated_outputs = CONFIG.update_outputs_for_year()
        
        # Load field-level BNBO coverage from Stage 2A
        stage2a_dataset = updated_outputs["fields_bnbo_water"]
        stage2a_path = self._get_latest_gold_path(stage2a_dataset)
        self.gcs_access.query_parquet_direct(stage2a_path, "SELECT *", "fields_bnbo_water")
        self.log.info(f"✅ Loaded fields_bnbo_water from {stage2a_dataset}")

        # Load property/field intersections from Stage 1C (includes intersection_geometry)
        stage1c_dataset = updated_outputs["field_property_intersections"]
        stage1c_path = self._get_latest_gold_path(stage1c_dataset)
        self.gcs_access.query_parquet_direct(
            stage1c_path, "SELECT *", "field_property_intersections"
        )
        self.log.info(f"✅ Loaded field_property_intersections from {stage1c_dataset}")

        # Load water project/BNBO intersections from Stage 1A (foundation data)
        stage1a_dataset = updated_outputs["water_projects_bnbo_intersections"]
        stage1a_path = self._get_latest_gold_path(stage1a_dataset)
        self.gcs_access.query_parquet_direct(
            stage1a_path, "SELECT *", "water_projects_bnbo_intersections"
        )
        self.log.info(f"✅ Loaded water_projects_bnbo_intersections from {stage1a_dataset}")

        # Load field-BNBO intersections from Stage 2A (SPEED OPTIMIZATION!)
        stage2a_intersections_dataset = updated_outputs["field_bnbo_intersections"]
        stage2a_intersections_path = self._get_latest_gold_path(stage2a_intersections_dataset)
        self.gcs_access.query_parquet_direct(
            stage2a_intersections_path, "SELECT *", "field_bnbo_intersections"
        )
        self.log.info(f"✅ Loaded field_bnbo_intersections from {stage2a_intersections_dataset}")

        self.log.info(
            "✅ SPEED OPTIMIZATION: Using pre-computed field-BNBO intersections from Stage 2A"
        )
        self.log.info("✅ No more expensive Property × Environmental spatial joins!")
        
        # Store input area reference for validation
        if self._should_validate_areas():
            fields_area_stats = self.conn.execute("""
                SELECT 
                    COUNT(DISTINCT field_uuid) as field_count,
                    SUM(field_area_m2) as total_area
                FROM (
                    SELECT DISTINCT field_uuid, field_area_m2
                    FROM fields_bnbo_water
                    WHERE field_area_m2 IS NOT NULL AND field_area_m2 > 0
                ) unique_fields
            """).fetchone()
            
            self._input_area_reference = {
                "total_area": fields_area_stats[1] or 0,
                "field_count": fields_area_stats[0] or 0
            }

    async def _execute_stage_processing(self) -> Dict[str, Any]:
        """
        Create property-level BNBO analysis using foundation data approach.

        FOUNDATION DATA STRATEGY:
        1. Start with field-level BNBO coverage (Stage 2A)
        2. Get property intersections for those fields (Stage 1C)
        3. Single spatial join: Property intersections × BNBO features
        4. Calculate property-level BNBO areas per field
        5. Use existing water project coverage ratios from Stage 1A
        6. Aggregate to achieve nested field→property→environmental structure

        DuckDB Spatial v1.2.2 COMPLIANCE:
        - Only single spatial predicate (ST_Intersects)
        - No complex 3-way spatial joins
        - Use foundation data and ID-based joins where possible
        """

        self.log.info("🎯 FOUNDATION DATA APPROACH: Property-level BNBO analysis")
        self.log.info("✅ DuckDB Spatial v1.2.2: Single spatial predicates only")

        # Get total field count for batching
        total_fields = self.conn.execute("SELECT COUNT(*) FROM fields_bnbo_water").fetchone()[0]
        batch_size = CONFIG.stage3_batch_size
        num_batches = (total_fields + batch_size - 1) // batch_size

        self.log.info(
            f"Processing {total_fields:,} fields in {num_batches} batches of {batch_size:,}"
        )

        # Initialize field-level aggregated result table 
        self.conn.execute("""
            CREATE OR REPLACE TABLE final_bnbo_analysis AS
            SELECT 
                CAST(NULL AS VARCHAR) as field_id,
                CAST(NULL AS VARCHAR) as block_id,
                CAST(NULL AS VARCHAR) as cvr_number,
                CAST(NULL AS INTEGER) as year,
                CAST(NULL AS VARCHAR) as field_uuid,
                CAST(NULL AS GEOMETRY) as geometry,
                CAST(NULL AS DOUBLE) as field_area_m2,
                
                -- Field-level BNBO data (from Stage 2A)
                CAST(NULL AS DOUBLE) as field_bnbo_total_m2,
                CAST(NULL AS DOUBLE) as field_bnbo_water_covered_m2,
                CAST(NULL AS DOUBLE) as field_bnbo_water_covered_pct,
                CAST(NULL AS DOUBLE) as field_bnbo_water_uncovered_pct,
                CAST(NULL AS DOUBLE) as field_bnbo_coverage_pct,
                
                -- BNBO status metrics - flattened by category (hectares)
                CAST(NULL AS DOUBLE) as bnbo_action_required_hectares,
                CAST(NULL AS DOUBLE) as bnbo_completed_hectares,
                CAST(NULL AS DOUBLE) as bnbo_action_required_overlap_hectares,
                CAST(NULL AS DOUBLE) as bnbo_completed_overlap_hectares,
                CAST(NULL AS DOUBLE) as bnbo_action_required_not_covered_by_water_hectares,
                CAST(NULL AS DOUBLE) as bnbo_completed_not_covered_by_water_hectares,
                CAST(NULL AS VARCHAR) as bnbo_status_categories,  -- Comma-separated list of categories present
                CAST(NULL AS INTEGER) as bnbo_status_count,
                
                -- Property ownership summary
                CAST(NULL AS INTEGER) as property_count,
                CAST(NULL AS DOUBLE) as total_property_intersection_area_m2,
                CAST(NULL AS VARCHAR) as primary_bfe_number,
                
                -- Property-level BNBO breakdown (NESTED STRUCTURE)
                CAST(NULL AS VARCHAR) as property_bnbo_breakdown,  -- JSON: {bfe_number: {bnbo_area_m2, covered_m2, uncovered_m2}}
                CAST(NULL AS DOUBLE) as property_bnbo_total_m2,
                CAST(NULL AS DOUBLE) as property_bnbo_water_covered_m2,
                CAST(NULL AS DOUBLE) as property_bnbo_water_uncovered_m2,
                CAST(NULL AS INTEGER) as property_bnbo_count,
                CAST(NULL AS VARCHAR) as property_bnbo_owners
            WHERE FALSE
        """)
        
        # Initialize property-level detailed intersection table (NEW OUTPUT)
        self.conn.execute("""
            CREATE OR REPLACE TABLE property_bnbo_intersections AS
            SELECT 
                CAST(NULL AS VARCHAR) as field_id,
                CAST(NULL AS VARCHAR) as block_id,
                CAST(NULL AS VARCHAR) as cvr_number,
                CAST(NULL AS INTEGER) as year,
                CAST(NULL AS VARCHAR) as field_uuid,
                CAST(NULL AS VARCHAR) as bfe_number,
                CAST(NULL AS VARCHAR) as status_category,
                CAST(NULL AS DOUBLE) as property_bnbo_area_m2,
                CAST(NULL AS DOUBLE) as property_bnbo_water_covered_m2,
                CAST(NULL AS DOUBLE) as property_bnbo_water_uncovered_m2
            WHERE FALSE
        """)

        # Process each batch
        for batch_num in range(num_batches):
            offset = batch_num * batch_size
            progress_pct = ((batch_num + 1) / num_batches) * 100
            self.log.info(f"📦 Batch {batch_num + 1}/{num_batches} - {progress_pct:.1f}% complete")

            # Create field batch (Stage 2 already filtered to fields with BNBO > 0)
            # Use deterministic ordering to ensure consistent batching
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE fields_batch AS
                SELECT * FROM fields_bnbo_water
                ORDER BY field_uuid  -- Deterministic ordering
                LIMIT {batch_size} OFFSET {offset}
            """)

            batch_count = self.conn.execute("SELECT COUNT(*) FROM fields_batch").fetchone()[0]
            if batch_count == 0:
                break

            self.log.info(f"  Processing {batch_count:,} fields with BNBO in batch {batch_num + 1}")

            # Get property intersections for this batch
            self.conn.execute("""
                CREATE OR REPLACE TABLE batch_property_intersections AS
                SELECT 
                    p.field_id,
                    p.block_id,
                    p.cvr_number,
                    p.year,
                    p.field_uuid,
                    p.bfe_number,
                    p.intersection_area_m2,
                    p.field_area_share_pct,
                    p.property_area_share_pct,
                    p.intersection_geometry
                FROM field_property_intersections p
                WHERE EXISTS (
                    SELECT 1 FROM fields_batch b 
                    WHERE p.field_uuid = b.field_uuid 
                    AND p.year = b.year
                )
            """)

            property_count = self.conn.execute(
                "SELECT COUNT(*) FROM batch_property_intersections"
            ).fetchone()[0]
            self.log.info(f"  Found {property_count:,} property intersections for batch")
            
            # DEBUG: Count unique fields in property intersections
            property_fields = self.conn.execute(
                "SELECT COUNT(DISTINCT field_uuid) FROM batch_property_intersections"
            ).fetchone()[0]
            self.log.info(f"  🔍 DEBUG: Property intersections cover {property_fields:,} unique fields")

            if property_count > 0:
                # PROPER SPATIAL JOIN: Only create records where geometries actually intersect
                self.log.info(
                    "  STEP 1: Property intersections × BNBO intersections (SPATIAL JOIN)"
                )
                self.conn.execute("""
                    CREATE OR REPLACE TABLE batch_property_bnbo_spatial AS
                    SELECT 
                        p.field_id,
                        p.block_id,
                        p.cvr_number,
                        p.year,
                        p.field_uuid,
                        p.bfe_number,
                        p.intersection_area_m2 as property_intersection_area_m2,
                        fb.status_category,
                        ST_Area_Spheroid(ST_Intersection(p.intersection_geometry, fb.field_bnbo_intersection_geometry)) as property_bnbo_area_m2
                    FROM batch_property_intersections p
                    JOIN field_bnbo_intersections fb ON p.field_uuid = fb.field_uuid 
                        AND p.year = fb.year
                        AND ST_Intersects(p.intersection_geometry, fb.field_bnbo_intersection_geometry)
                    WHERE p.intersection_geometry IS NOT NULL 
                        AND fb.field_bnbo_intersection_geometry IS NOT NULL
                """)

                spatial_count = self.conn.execute(
                    "SELECT COUNT(*) FROM batch_property_bnbo_spatial"
                ).fetchone()[0]
                self.log.info(f"  Found {spatial_count:,} property-BNBO spatial intersections")
                
                # DEBUG: Show how many property records were filtered out by spatial join
                pre_spatial_count = self.conn.execute("""
                    SELECT COUNT(*) FROM batch_property_intersections p
                    JOIN field_bnbo_intersections fb ON p.field_uuid = fb.field_uuid AND p.year = fb.year
                """).fetchone()[0]
                filtered_out = pre_spatial_count - spatial_count
                self.log.info(f"  🔍 DEBUG: {filtered_out:,} property records filtered out by spatial intersection")

                # Calculate water project coverage using field-level coverage ratios from Stage 2A
                self.log.info(
                    "  Calculating water project coverage using field-level ratios from Stage 2A"
                )

                # Apply field-level coverage ratios to property-BNBO areas
                self.conn.execute("""
                    CREATE OR REPLACE TABLE batch_property_bnbo_water AS
                    SELECT 
                        pb.field_id,
                        pb.block_id,
                        pb.cvr_number,
                        pb.year,
                        pb.field_uuid,
                        pb.bfe_number,
                        pb.status_category,
                        pb.property_bnbo_area_m2,
                        -- Apply field-level water coverage ratio from Stage 2A
                        pb.property_bnbo_area_m2 * (fb.field_bnbo_water_covered_pct / 100.0) as property_bnbo_covered_m2,
                        pb.property_bnbo_area_m2 * (1 - (fb.field_bnbo_water_covered_pct / 100.0)) as property_bnbo_uncovered_m2
                    FROM batch_property_bnbo_spatial pb
                    JOIN fields_bnbo_water fb ON pb.field_uuid = fb.field_uuid 
                        AND pb.year = fb.year
                """)

                water_analysis_count = self.conn.execute(
                    "SELECT COUNT(*) FROM batch_property_bnbo_water"
                ).fetchone()[0]
                self.log.info(
                    f"  Created {water_analysis_count:,} property-BNBO-water analysis records"
                )

                # Aggregate to create nested property breakdown per field
                self.log.info("  Creating nested property-level BNBO breakdown per field")
                self.conn.execute("""
                    CREATE OR REPLACE TABLE batch_property_breakdown AS
                    SELECT 
                        field_id,
                        block_id,
                        cvr_number,
                        year,
                        field_uuid,
                        -- Create JSON breakdown: {bfe_number: {bnbo_area_m2, covered_m2, uncovered_m2}}
                        '{' || STRING_AGG(
                            '"' || bfe_number || '": {' ||
                            '"bnbo_area_m2": ' || ROUND(total_bnbo_area, 2) || ', ' ||
                            '"covered_m2": ' || ROUND(total_covered, 2) || ', ' ||
                            '"uncovered_m2": ' || ROUND(total_uncovered, 2) ||
                            '}', ', '
                        ) || '}' as property_bnbo_breakdown,
                        
                        -- Summary metrics
                        SUM(total_bnbo_area) as property_bnbo_total_m2,
                        SUM(total_covered) as property_bnbo_water_covered_m2,
                        SUM(total_uncovered) as property_bnbo_water_uncovered_m2,
                        COUNT(DISTINCT bfe_number) as property_bnbo_count,
                        STRING_AGG(DISTINCT bfe_number, ', ') as property_bnbo_owners
                    FROM (
                        SELECT 
                            field_id, block_id, cvr_number, year, field_uuid, bfe_number,
                            SUM(property_bnbo_area_m2) as total_bnbo_area,
                            SUM(property_bnbo_covered_m2) as total_covered,
                            SUM(property_bnbo_uncovered_m2) as total_uncovered
                        FROM batch_property_bnbo_water
                        GROUP BY field_id, block_id, cvr_number, year, field_uuid, bfe_number
                    ) property_totals
                    GROUP BY field_id, block_id, cvr_number, year, field_uuid
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
                        field_id, block_id, cvr_number, year, field_uuid,
                        '{}' as property_bnbo_breakdown,
                        0 as property_bnbo_total_m2,
                        0 as property_bnbo_water_covered_m2,
                        0 as property_bnbo_water_uncovered_m2,
                        0 as property_bnbo_count,
                        NULL as property_bnbo_owners
                    FROM fields_batch
                    WHERE FALSE
                """)

            # Create BNBO status breakdown per field
            self.log.info("  Creating BNBO status breakdown per field")
            if property_count > 0:
                self.conn.execute("""
                    CREATE OR REPLACE TABLE batch_bnbo_status_breakdown AS
                    SELECT 
                        field_id,
                        block_id,
                        cvr_number,
                        year,
                        field_uuid,
                        -- Flattened BNBO metrics by status category (in hectares)
                        ROUND(
                            SUM(CASE WHEN status_category = 'action_required' THEN property_bnbo_area_m2 ELSE 0 END) / 10000, 4
                        ) as bnbo_action_required_hectares,
                        
                        ROUND(
                            SUM(CASE WHEN status_category = 'completed' THEN property_bnbo_area_m2 ELSE 0 END) / 10000, 4
                        ) as bnbo_completed_hectares,
                        
                        ROUND(
                            SUM(CASE WHEN status_category = 'action_required' THEN property_bnbo_covered_m2 ELSE 0 END) / 10000, 4
                        ) as bnbo_action_required_overlap_hectares,
                        
                        ROUND(
                            SUM(CASE WHEN status_category = 'completed' THEN property_bnbo_covered_m2 ELSE 0 END) / 10000, 4
                        ) as bnbo_completed_overlap_hectares,
                        
                        ROUND(
                            SUM(CASE WHEN status_category = 'action_required' THEN property_bnbo_uncovered_m2 ELSE 0 END) / 10000, 4
                        ) as bnbo_action_required_not_covered_by_water_hectares,
                        
                        ROUND(
                            SUM(CASE WHEN status_category = 'completed' THEN property_bnbo_uncovered_m2 ELSE 0 END) / 10000, 4
                        ) as bnbo_completed_not_covered_by_water_hectares,
                        
                        -- Summary fields
                        STRING_AGG(DISTINCT status_category, ', ' ORDER BY status_category) as bnbo_status_categories,
                        COUNT(DISTINCT status_category) as bnbo_status_count
                    FROM batch_property_bnbo_water pb
                    GROUP BY field_id, block_id, cvr_number, year, field_uuid
                """)
            else:
                # No properties for this batch - create empty status breakdown table
                self.conn.execute("""
                    CREATE OR REPLACE TABLE batch_bnbo_status_breakdown AS
                    SELECT 
                        field_id, block_id, cvr_number, year, field_uuid,
                        0.0 as bnbo_action_required_hectares,
                        0.0 as bnbo_completed_hectares,
                        0.0 as bnbo_action_required_overlap_hectares,
                        0.0 as bnbo_completed_overlap_hectares,
                        0.0 as bnbo_action_required_not_covered_by_water_hectares,
                        0.0 as bnbo_completed_not_covered_by_water_hectares,
                        NULL as bnbo_status_categories,
                        0 as bnbo_status_count
                    FROM fields_batch
                    WHERE FALSE
                """)

            # Combine field-level data with property breakdown
            self.log.info("  Aggregating BNBO fragments to field-level and combining with property and status breakdown")
            self.conn.execute("""
                INSERT INTO final_bnbo_analysis
                SELECT 
                    fab.field_id,
                    fab.block_id,
                    fab.cvr_number,
                    fab.year,
                    fab.field_uuid,
                    fab.geometry,
                    fab.field_area_m2,
                    
                    -- Field-level BNBO data (aggregated from Stage 2A fragments)
                    fab.field_bnbo_total_m2,
                    fab.field_bnbo_water_covered_m2,
                    fab.field_bnbo_water_covered_pct,
                    fab.field_bnbo_water_uncovered_pct,
                    fab.field_bnbo_coverage_pct,
                    
                    -- BNBO status metrics - flattened by category (from batch processing)
                    COALESCE(sb.bnbo_action_required_hectares, 0.0) as bnbo_action_required_hectares,
                    COALESCE(sb.bnbo_completed_hectares, 0.0) as bnbo_completed_hectares,
                    COALESCE(sb.bnbo_action_required_overlap_hectares, 0.0) as bnbo_action_required_overlap_hectares,
                    COALESCE(sb.bnbo_completed_overlap_hectares, 0.0) as bnbo_completed_overlap_hectares,
                    COALESCE(sb.bnbo_action_required_not_covered_by_water_hectares, 0.0) as bnbo_action_required_not_covered_by_water_hectares,
                    COALESCE(sb.bnbo_completed_not_covered_by_water_hectares, 0.0) as bnbo_completed_not_covered_by_water_hectares,
                    COALESCE(sb.bnbo_status_categories, NULL) as bnbo_status_categories,
                    COALESCE(sb.bnbo_status_count, 0) as bnbo_status_count,
                    
                    -- Property ownership summary
                    COALESCE(ps.property_count, 0) as property_count,
                    COALESCE(ps.total_property_intersection_area_m2, 0) as total_property_intersection_area_m2,
                    COALESCE(ps.primary_bfe_number, NULL) as primary_bfe_number,
                    
                    -- Property-level BNBO breakdown (NESTED STRUCTURE)
                    COALESCE(pb.property_bnbo_breakdown, '{}') as property_bnbo_breakdown,
                    COALESCE(pb.property_bnbo_total_m2, 0) as property_bnbo_total_m2,
                    COALESCE(pb.property_bnbo_water_covered_m2, 0) as property_bnbo_water_covered_m2,
                    COALESCE(pb.property_bnbo_water_uncovered_m2, 0) as property_bnbo_water_uncovered_m2,
                    COALESCE(pb.property_bnbo_count, 0) as property_bnbo_count,
                    COALESCE(pb.property_bnbo_owners, NULL) as property_bnbo_owners
                    
                FROM (
                    -- CRITICAL FIX: Aggregate multiple BNBO fragments per field to single field-level record
                    SELECT 
                        field_id,
                        block_id,
                        cvr_number,
                        year,
                        field_uuid,
                        FIRST(geometry) as geometry,  -- Take first geometry per field
                        FIRST(field_area_m2) as field_area_m2,  -- Field area should be same across fragments
                        
                        -- Aggregate BNBO areas from multiple fragments
                        SUM(field_bnbo_total_m2) as field_bnbo_total_m2,
                        SUM(field_bnbo_water_covered_m2) as field_bnbo_water_covered_m2,
                        
                        -- Recalculate percentages from aggregated totals
                        CASE 
                            WHEN SUM(field_bnbo_total_m2) > 0 
                            THEN (SUM(field_bnbo_water_covered_m2) / SUM(field_bnbo_total_m2)) * 100.0
                            ELSE 0 
                        END as field_bnbo_water_covered_pct,
                        
                        CASE 
                            WHEN SUM(field_bnbo_total_m2) > 0 
                            THEN ((SUM(field_bnbo_total_m2) - SUM(field_bnbo_water_covered_m2)) / SUM(field_bnbo_total_m2)) * 100.0
                            ELSE 0 
                        END as field_bnbo_water_uncovered_pct,
                        
                        CASE 
                            WHEN FIRST(field_area_m2) > 0 
                            THEN (SUM(field_bnbo_total_m2) / FIRST(field_area_m2)) * 100.0
                            ELSE 0 
                        END as field_bnbo_coverage_pct
                        
                    FROM fields_batch 
                    GROUP BY field_id, block_id, cvr_number, year, field_uuid
                ) fab
                LEFT JOIN (
                    SELECT 
                        field_uuid, year,
                        -- Keep composite keys for reference
                        field_id, block_id, cvr_number,
                        SUM(CASE WHEN bfe_number IS NOT NULL THEN 1 ELSE 0 END) as property_count,
                        COALESCE(SUM(intersection_area_m2), 0) as total_property_intersection_area_m2,
                        (
                            SELECT bfe_number 
                            FROM batch_property_intersections bp2 
                            WHERE bp2.field_uuid = bp.field_uuid 
                            AND bp2.year = bp.year
                            AND bp2.bfe_number IS NOT NULL
                            ORDER BY bp2.intersection_area_m2 DESC 
                            LIMIT 1
                        ) as primary_bfe_number
                    FROM batch_property_intersections bp
                    GROUP BY field_uuid, year, field_id, block_id, cvr_number
                ) ps ON fab.field_uuid = ps.field_uuid 
                    AND fab.year = ps.year
                LEFT JOIN batch_property_breakdown pb ON fab.field_uuid = pb.field_uuid 
                    AND fab.year = pb.year
                LEFT JOIN batch_bnbo_status_breakdown sb ON fab.field_uuid = sb.field_uuid 
                    AND fab.year = sb.year
            """)

            # Save property-level intersection data before cleanup (NEW OUTPUT TABLE)
            # Check if batch_property_bnbo_water has any data to save
            batch_property_data_count = self.conn.execute(
                "SELECT COUNT(*) FROM batch_property_bnbo_water"
            ).fetchone()[0]
            
            if batch_property_data_count > 0:
                self.log.info("  Aggregating and saving property-level BNBO totals...")
                self.conn.execute("""
                    INSERT INTO property_bnbo_intersections
                    SELECT 
                        field_id,
                        block_id,
                        cvr_number,
                        year,
                        field_uuid,
                        bfe_number,
                        FIRST(status_category) as status_category,  -- Take first value if multiple BNBO statuses
                        SUM(property_bnbo_area_m2) as property_bnbo_area_m2,
                        SUM(property_bnbo_covered_m2) as property_bnbo_water_covered_m2,
                        SUM(property_bnbo_uncovered_m2) as property_bnbo_water_uncovered_m2
                    FROM batch_property_bnbo_water
                    GROUP BY field_id, block_id, cvr_number, year, field_uuid, bfe_number
                """)
                
                property_intersections_saved = self.conn.execute(
                    "SELECT COUNT(*) FROM property_bnbo_intersections"
                ).fetchone()[0]
                self.log.info(f"  ✅ Saved {property_intersections_saved:,} property-level BNBO intersections so far")
                
                # DEBUG: Show unique fields in final output
                final_fields = self.conn.execute(
                    "SELECT COUNT(DISTINCT field_uuid) FROM property_bnbo_intersections"
                ).fetchone()[0]
                self.log.info(f"  🔍 DEBUG: Final output covers {final_fields:,} unique fields with property×BNBO data")

            # Clean up batch tables
            self.conn.execute("DROP TABLE IF EXISTS fields_batch")
            self.conn.execute("DROP TABLE IF EXISTS batch_property_intersections")
            self.conn.execute("DROP TABLE IF EXISTS batch_property_bnbo_raw")
            self.conn.execute("DROP TABLE IF EXISTS batch_property_bnbo_spatial")
            self.conn.execute("DROP TABLE IF EXISTS batch_property_bnbo_water")
            self.conn.execute("DROP TABLE IF EXISTS batch_property_breakdown")
            self.conn.execute("DROP TABLE IF EXISTS batch_bnbo_status_breakdown")

            # Memory cleanup
            if (batch_num + 1) % CONFIG.stage2_memory_cleanup_frequency == 0:
                import gc

                gc.collect()
                self.log.info(f"  🧹 Memory cleanup after batch {batch_num + 1}")

        # CRITICAL FIX: Final aggregation to handle fields split across batches
        self.log.info("🔧 Final aggregation: Consolidating any field fragments split across batches...")
        
        # Check for duplicates before final aggregation
        pre_agg_stats = self.conn.execute("""
            SELECT COUNT(*) as total_records, COUNT(DISTINCT field_uuid) as unique_fields
            FROM final_bnbo_analysis
        """).fetchone()
        
        duplicates_found = pre_agg_stats[0] - pre_agg_stats[1]
        if duplicates_found > 0:
            self.log.info(f"  🔍 Found {duplicates_found} duplicate field records to consolidate")
            
            # Create final aggregated table
            self.conn.execute("""
                CREATE OR REPLACE TABLE final_bnbo_analysis_consolidated AS
                SELECT 
                    field_id,
                    block_id,
                    cvr_number,
                    year,
                    field_uuid,
                    FIRST(geometry) as geometry,
                    FIRST(field_area_m2) as field_area_m2,
                    
                    -- Aggregate BNBO areas from fragments split across batches
                    SUM(field_bnbo_total_m2) as field_bnbo_total_m2,
                    SUM(field_bnbo_water_covered_m2) as field_bnbo_water_covered_m2,
                    
                    -- Recalculate percentages from final aggregated totals
                    CASE 
                        WHEN SUM(field_bnbo_total_m2) > 0 
                        THEN (SUM(field_bnbo_water_covered_m2) / SUM(field_bnbo_total_m2)) * 100.0
                        ELSE 0 
                    END as field_bnbo_water_covered_pct,
                    
                    CASE 
                        WHEN SUM(field_bnbo_total_m2) > 0 
                        THEN ((SUM(field_bnbo_total_m2) - SUM(field_bnbo_water_covered_m2)) / SUM(field_bnbo_total_m2)) * 100.0
                        ELSE 0 
                    END as field_bnbo_water_uncovered_pct,
                    
                    CASE 
                        WHEN FIRST(field_area_m2) > 0 
                        THEN (SUM(field_bnbo_total_m2) / FIRST(field_area_m2)) * 100.0
                        ELSE 0 
                    END as field_bnbo_coverage_pct,
                    
                    -- Aggregate property summary fields
                    MAX(property_count) as property_count,
                    SUM(total_property_intersection_area_m2) as total_property_intersection_area_m2,
                    FIRST(primary_bfe_number) as primary_bfe_number,
                    
                    -- For property breakdown, take the most complete one (longest JSON string)
                    (SELECT property_bnbo_breakdown 
                     FROM final_bnbo_analysis fba2 
                     WHERE fba2.field_uuid = fba.field_uuid 
                     ORDER BY LENGTH(property_bnbo_breakdown) DESC 
                     LIMIT 1) as property_bnbo_breakdown,
                    
                    SUM(property_bnbo_total_m2) as property_bnbo_total_m2,
                    SUM(property_bnbo_water_covered_m2) as property_bnbo_water_covered_m2,
                    SUM(property_bnbo_water_uncovered_m2) as property_bnbo_water_uncovered_m2,
                    MAX(property_bnbo_count) as property_bnbo_count,
                    FIRST(property_bnbo_owners) as property_bnbo_owners
                    
                FROM final_bnbo_analysis fba
                GROUP BY field_id, block_id, cvr_number, year, field_uuid
            """)
            
            # Replace the original table with the consolidated one
            self.conn.execute("DROP TABLE final_bnbo_analysis")
            self.conn.execute("ALTER TABLE final_bnbo_analysis_consolidated RENAME TO final_bnbo_analysis")
            
            # Verify the fix
            post_agg_stats = self.conn.execute("""
                SELECT COUNT(*) as total_records, COUNT(DISTINCT field_uuid) as unique_fields
                FROM final_bnbo_analysis
            """).fetchone()
            
            self.log.info(f"  ✅ Final aggregation complete: {post_agg_stats[0]:,} records ({post_agg_stats[1]:,} unique fields)")
            
            if post_agg_stats[0] == post_agg_stats[1]:
                self.log.info(f"  🎉 All field duplicates resolved!")
            else:
                self.log.warning(f"  ⚠️ Still have {post_agg_stats[0] - post_agg_stats[1]} duplicates - needs investigation")
        else:
            self.log.info(f"  ✅ No duplicate fields found - batching worked perfectly")

        # Get final statistics
        final_count = self.conn.execute("SELECT COUNT(*) FROM final_bnbo_analysis").fetchone()[0]

        # Sample the nested structure
        sample_breakdown = self.conn.execute("""
            SELECT property_bnbo_breakdown 
            FROM final_bnbo_analysis 
            WHERE property_bnbo_breakdown != '{}' 
            LIMIT 1
        """).fetchone()

        if sample_breakdown:
            self.log.info(f"✅ Sample property breakdown: {sample_breakdown[0][:200]}...")

        self.log.info(
            f"✅ Created {final_count:,} final BNBO analysis records with nested property structure"
        )
        self.log.info(
            "🎯 ACHIEVED: field → property → BNBO (area/covered/uncovered) nested breakdown"
        )

        # Get property-level intersection statistics
        property_intersections_count = self.conn.execute("SELECT COUNT(*) FROM property_bnbo_intersections").fetchone()[0]
        
        return {
            "final_records": final_count,
            "property_intersections": property_intersections_count,
            "batches_processed": num_batches,
            "foundation_data_approach": True,
            "single_spatial_predicates": True,
        }

    def _save_output_data(self, result: Dict[str, Any]):
        """Save both field-level and property-level BNBO analysis."""
        # Save field-level aggregated analysis (existing output)
        self._save_stage_output("final_bnbo_analysis", "final_bnbo")
        
        # Save property-level intersection analysis (new output)
        self._save_stage_output("property_bnbo_intersections", "property_bnbo_intersections")
        
        self.log.info(f"✅ Saved field-level analysis: {result['final_records']:,} records")
        self.log.info(f"✅ Saved property-level intersections: {result['property_intersections']:,} records")
    
    def _get_input_area_reference(self) -> Dict[str, Any]:
        """Get reference area statistics from input data for validation."""
        return getattr(self, '_input_area_reference', None)
    
    def _get_main_output_table(self) -> str:
        """Get the name of the main output table for area validation."""
        return "final_bnbo_analysis"
