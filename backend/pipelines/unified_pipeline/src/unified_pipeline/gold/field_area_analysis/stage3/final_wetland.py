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
        """Load wetland analysis from Stage 2B and pre-filtered properties from Stage 1C."""
        # Load wetland water coverage from Stage 2B
        stage2b_path = f"gs://{CONFIG.bucket}/gold/{CONFIG.stage_outputs['fields_wetland_water']}/{CONFIG.stage_outputs['fields_wetland_water']}.parquet"
        self.gcs_access.query_parquet_direct(stage2b_path, "SELECT *", "fields_wetland_water")

        # Load pre-filtered field-property intersections from Stage 1C
        stage1c_path = f"gs://{CONFIG.bucket}/gold/{CONFIG.stage_outputs['field_property_intersections']}/{CONFIG.stage_outputs['field_property_intersections']}.parquet"
        self.gcs_access.query_parquet_direct(
            stage1c_path, "SELECT *", "field_property_intersections"
        )

        # Load water project × wetland foundation data from Stage 1B for property-level analysis
        stage1b_path = f"gs://{CONFIG.bucket}/gold/{CONFIG.stage_outputs['water_projects_wetlands']}/{CONFIG.stage_outputs['water_projects_wetlands']}.parquet"
        self.gcs_access.query_parquet_direct(stage1b_path, "SELECT *", "water_projects_wetlands")

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
                CAST(NULL AS INTEGER) as dominant_wetland_gridcode,
                CAST(NULL AS INTEGER) as wetland_polygon_count,
                
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
                        field_wetland_coverage_pct, dominant_wetland_gridcode, wetland_polygon_count,
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
            # Load wetland data for spatial analysis
            self.conn.execute("""
                CREATE OR REPLACE TABLE batch_wetland_features AS
                SELECT 
                    b.field_id,
                    b.block_id,
                    b.cvr_number,
                    b.year,
                    w.wetland_id,
                    w.gridcode,
                    w.toerv_pct,
                    w.geometry as wetland_geometry
                FROM fields_batch b
                JOIN wetlands_for_fields w ON ST_Intersects(b.geometry, w.geometry)
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
                self.conn.execute("""
                    CREATE OR REPLACE TABLE batch_property_wetland_spatial AS
                    SELECT 
                        p.field_id,
                        p.block_id,
                        p.cvr_number,
                        p.year,
                        p.bfe_number,
                        p.intersection_area_m2 as property_intersection_area_m2,
                        w.wetland_id,
                        w.gridcode,
                        w.toerv_pct,
                        ST_Area_Spheroid(ST_Intersection(p.intersection_geometry, w.wetland_geometry)) as property_wetland_area_m2
                    FROM batch_property_intersections p
                    JOIN batch_wetland_features w ON p.field_id = w.field_id 
                        AND p.block_id = w.block_id 
                        AND p.cvr_number = w.cvr_number
                        AND ST_Intersects(p.intersection_geometry, w.wetland_geometry)
                    WHERE ST_Area_Spheroid(ST_Intersection(p.intersection_geometry, w.wetland_geometry)) > 10
                """)

                spatial_intersections = self.conn.execute(
                    "SELECT COUNT(*) FROM batch_property_wetland_spatial"
                ).fetchone()[0]
                self.log.info(
                    f"  Found {spatial_intersections:,} property-wetland spatial intersections"
                )

                # ENHANCED ANALYSIS: Property × Wetland × Water Project Coverage
                self.log.info("  Analyzing wetland water project coverage at property level")
                self.conn.execute("""
                    CREATE OR REPLACE TABLE batch_property_wetland_water_analysis AS
                    SELECT 
                        pw.field_id,
                        pw.block_id,
                        pw.cvr_number,
                        pw.year,
                        pw.bfe_number,
                        pw.property_intersection_area_m2,
                        pw.wetland_id,
                        pw.gridcode,
                        pw.toerv_pct,
                        pw.property_wetland_area_m2,
                        
                        -- Check if this wetland area is covered by water projects
                        CASE 
                            WHEN wpw.wetland_id IS NOT NULL THEN pw.property_wetland_area_m2
                            ELSE 0 
                        END as property_wetland_covered_by_water_m2,
                        
                        CASE 
                            WHEN wpw.wetland_id IS NULL THEN pw.property_wetland_area_m2
                            ELSE 0 
                        END as property_wetland_not_covered_by_water_m2,
                        
                        wpw.water_project_id,
                        wpw.water_project_coverage_area_m2
                        
                    FROM batch_property_wetland_spatial pw
                    LEFT JOIN water_projects_wetlands wpw ON pw.wetland_id = wpw.wetland_id
                """)

                water_analysis_count = self.conn.execute(
                    "SELECT COUNT(*) FROM batch_property_wetland_water_analysis"
                ).fetchone()[0]
                self.log.info(
                    f"  Created {water_analysis_count:,} property-wetland-water analysis records"
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
                        CAST(NULL AS VARCHAR) as wetland_id,
                        CAST(NULL AS INTEGER) as gridcode,
                        CAST(NULL AS DOUBLE) as toerv_pct,
                        CAST(NULL AS DOUBLE) as property_wetland_area_m2,
                        CAST(NULL AS DOUBLE) as property_wetland_covered_by_water_m2,
                        CAST(NULL AS DOUBLE) as property_wetland_not_covered_by_water_m2,
                        CAST(NULL AS VARCHAR) as water_project_id,
                        CAST(NULL AS DOUBLE) as water_project_coverage_area_m2
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
                    b.dominant_wetland_gridcode,
                    b.wetland_polygon_count,
                    
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
            self.conn.execute("DROP TABLE IF EXISTS batch_property_wetland_spatial")
            self.conn.execute("DROP TABLE IF EXISTS batch_property_wetland_water_analysis")

            # Memory cleanup
            if (batch_num + 1) % CONFIG.memory_cleanup_frequency == 0:
                import gc

                gc.collect()

        # Final statistics
        final_count = self.conn.execute("SELECT COUNT(*) FROM final_wetland_analysis").fetchone()[0]

        # Get comprehensive statistics including property-wetland spatial analysis
        stats = self.conn.execute("""
            SELECT 
                COUNT(*) as total_fields,
                COUNT(CASE WHEN total_wetland_area_m2 > 0 THEN 1 END) as fields_with_wetlands,
                COUNT(CASE WHEN property_count > 0 THEN 1 END) as fields_with_properties,
                COUNT(CASE WHEN properties_with_wetland_count > 0 THEN 1 END) as fields_with_wetland_properties,
                COUNT(CASE WHEN properties_with_covered_wetland_count > 0 THEN 1 END) as fields_with_covered_wetland_properties,
                COUNT(CASE WHEN properties_with_uncovered_wetland_count > 0 THEN 1 END) as fields_with_uncovered_wetland_properties,
                AVG(property_count) as avg_properties_per_field,
                AVG(CASE WHEN total_wetland_area_m2 > 0 THEN wetland_covered_by_water_projects_pct END) as avg_wetland_water_coverage,
                AVG(CASE WHEN total_wetland_area_m2 > 0 THEN field_wetland_coverage_pct END) as avg_field_wetland_coverage,
                AVG(CASE WHEN property_count > 0 THEN property_wetland_coverage_pct END) as avg_property_wetland_coverage,
                AVG(CASE WHEN properties_with_wetland_count > 0 THEN property_wetland_water_coverage_pct END) as avg_property_wetland_water_coverage,
                AVG(wetland_polygon_count) as avg_wetland_polygons_per_field,
                SUM(properties_with_wetland_count) as total_property_wetland_relationships,
                SUM(properties_with_covered_wetland_count) as total_properties_with_covered_wetlands,
                SUM(properties_with_uncovered_wetland_count) as total_properties_with_uncovered_wetlands,
                SUM(property_wetland_covered_by_water_m2) as total_property_wetland_covered_area,
                SUM(property_wetland_not_covered_by_water_m2) as total_property_wetland_uncovered_area
            FROM final_wetland_analysis
        """).fetchone()

        (
            total_fields,
            fields_with_wetlands,
            fields_with_props,
            fields_with_wetland_props,
            fields_with_covered_wetland_props,
            fields_with_uncovered_wetland_props,
            avg_props,
            avg_wetland_water,
            avg_field_wetland,
            avg_property_wetland,
            avg_property_wetland_water,
            avg_polygons,
            total_relationships,
            total_covered_props,
            total_uncovered_props,
            total_covered_area,
            total_uncovered_area,
        ) = stats

        self.log.info("✅ Wetland-Property spatial analysis completed:")
        self.log.info(f"   Total fields: {total_fields:,}")
        self.log.info(
            f"   Fields with wetlands: {fields_with_wetlands:,} ({(fields_with_wetlands / total_fields) * 100:.1f}%)"
        )
        self.log.info(
            f"   Fields with properties: {fields_with_props:,} ({(fields_with_props / total_fields) * 100:.1f}%)"
        )
        self.log.info(
            f"   Fields with wetland-property relationships: {fields_with_wetland_props:,}"
        )
        self.log.info(f"   Average properties per field: {avg_props:.1f}")
        self.log.info(f"   Average wetland water coverage: {avg_wetland_water:.1f}%")
        self.log.info(f"   Average property wetland coverage: {avg_property_wetland:.1f}%")
        self.log.info(
            f"   Average property wetland water coverage: {avg_property_wetland_water:.1f}%"
        )
        self.log.info(f"   Average wetland polygons per field: {avg_polygons:.1f}")
        self.log.info(f"   Total property-wetland spatial relationships: {total_relationships:,}")
        self.log.info("🌊 Property-level wetland water project coverage:")
        self.log.info(
            f"   Fields with properties owning covered wetlands: {fields_with_covered_wetland_props:,}"
        )
        self.log.info(
            f"   Fields with properties owning uncovered wetlands: {fields_with_uncovered_wetland_props:,}"
        )
        self.log.info(f"   Total properties with covered wetlands: {total_covered_props:,}")
        self.log.info(f"   Total properties with uncovered wetlands: {total_uncovered_props:,}")
        self.log.info(
            f"   Total property wetland area covered by water projects: {total_covered_area:,.0f} m²"
        )
        self.log.info(
            f"   Total property wetland area NOT covered by water projects: {total_uncovered_area:,.0f} m²"
        )

        return {
            "total_fields": total_fields,
            "fields_with_wetlands": fields_with_wetlands,
            "fields_with_properties": fields_with_props,
            "fields_with_wetland_properties": fields_with_wetland_props,
            "fields_with_covered_wetland_properties": fields_with_covered_wetland_props,
            "fields_with_uncovered_wetland_properties": fields_with_uncovered_wetland_props,
            "avg_properties_per_field": avg_props,
            "avg_wetland_water_coverage": avg_wetland_water,
            "avg_field_wetland_coverage": avg_field_wetland,
            "avg_property_wetland_coverage": avg_property_wetland,
            "avg_property_wetland_water_coverage": avg_property_wetland_water,
            "avg_polygons_per_field": avg_polygons,
            "total_property_wetland_relationships": total_relationships,
            "total_properties_with_covered_wetlands": total_covered_props,
            "total_properties_with_uncovered_wetlands": total_uncovered_props,
            "total_property_wetland_covered_area_m2": total_covered_area,
            "total_property_wetland_uncovered_area_m2": total_uncovered_area,
        }

    def _save_output_data(self, result: Dict[str, Any]):
        """Save final wetland analysis to GCS."""
        self._save_stage_output("final_wetland_analysis", "final_wetland")
