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
        """Load BNBO analysis from Stage 2A and pre-filtered properties from Stage 1C."""
        # Load BNBO water coverage from Stage 2A
        stage2a_path = f"gs://{CONFIG.bucket}/gold/{CONFIG.stage_outputs['fields_bnbo_water']}/{CONFIG.stage_outputs['fields_bnbo_water']}.parquet"
        self.gcs_access.query_parquet_direct(stage2a_path, "SELECT *", "fields_bnbo_water")

        # Load pre-filtered field-property intersections from Stage 1C
        stage1c_path = f"gs://{CONFIG.bucket}/gold/{CONFIG.stage_outputs['field_property_intersections']}/{CONFIG.stage_outputs['field_property_intersections']}.parquet"
        self.gcs_access.query_parquet_direct(
            stage1c_path, "SELECT *", "field_property_intersections"
        )

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
                CAST(NULL AS VARCHAR) as bnbo_property_owners
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
                        0 as properties_with_bnbo_count, NULL as bnbo_property_owners
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
                # SINGLE SPATIAL JOIN: Property intersections × BNBO features (PR #545 compliant)
                self.log.info("  Performing spatial join: Property intersections × BNBO features")
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
                        b.bnbo_id,
                        b.status_category,
                        b.bnbo_geometry,
                        ST_Area_Spheroid(ST_Intersection(p.intersection_geometry, b.bnbo_geometry)) as property_bnbo_area_m2
                    FROM batch_property_intersections p
                    JOIN batch_bnbo_features b ON ST_Intersects(p.intersection_geometry, b.bnbo_geometry)
                    WHERE ST_Area_Spheroid(ST_Intersection(p.intersection_geometry, b.bnbo_geometry)) > 10
                """)

                # Step 2: Filter to matching fields (equality constraints applied after spatial join)
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
                        property_bnbo_area_m2
                    FROM batch_spatial_raw
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
                    COALESCE(ps.bnbo_property_owners, NULL) as bnbo_property_owners
                    
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
            """)

            # Clean up batch tables
            self.conn.execute("DROP TABLE IF EXISTS fields_batch")
            self.conn.execute("DROP TABLE IF EXISTS batch_property_intersections")
            self.conn.execute("DROP TABLE IF EXISTS batch_bnbo_features")
            self.conn.execute("DROP TABLE IF EXISTS batch_spatial_raw")
            self.conn.execute("DROP TABLE IF EXISTS batch_property_bnbo_spatial")

            # Memory cleanup
            if (batch_num + 1) % CONFIG.memory_cleanup_frequency == 0:
                import gc

                gc.collect()

        # Final statistics
        final_count = self.conn.execute("SELECT COUNT(*) FROM final_bnbo_analysis").fetchone()[0]

        # Get comprehensive statistics including property-BNBO spatial analysis
        stats = self.conn.execute("""
            SELECT 
                COUNT(*) as total_fields,
                COUNT(CASE WHEN total_bnbo_area_m2 > 0 THEN 1 END) as fields_with_bnbo,
                COUNT(CASE WHEN property_count > 0 THEN 1 END) as fields_with_properties,
                COUNT(CASE WHEN properties_with_bnbo_count > 0 THEN 1 END) as fields_with_bnbo_properties,
                AVG(property_count) as avg_properties_per_field,
                AVG(CASE WHEN total_bnbo_area_m2 > 0 THEN bnbo_covered_by_water_projects_pct END) as avg_bnbo_water_coverage,
                AVG(CASE WHEN total_bnbo_area_m2 > 0 THEN field_bnbo_coverage_pct END) as avg_field_bnbo_coverage,
                AVG(CASE WHEN property_count > 0 THEN property_bnbo_coverage_pct END) as avg_property_bnbo_coverage,
                SUM(properties_with_bnbo_count) as total_property_bnbo_relationships
            FROM final_bnbo_analysis
        """).fetchone()

        (
            total_fields,
            fields_with_bnbo,
            fields_with_props,
            fields_with_bnbo_props,
            avg_props,
            avg_bnbo_water,
            avg_field_bnbo,
            avg_property_bnbo,
            total_relationships,
        ) = stats

        self.log.info("✅ BNBO-Property spatial analysis completed:")
        self.log.info(f"   Total fields: {total_fields:,}")
        self.log.info(
            f"   Fields with BNBO: {fields_with_bnbo:,} ({(fields_with_bnbo / total_fields) * 100:.1f}%)"
        )
        self.log.info(
            f"   Fields with properties: {fields_with_props:,} ({(fields_with_props / total_fields) * 100:.1f}%)"
        )
        self.log.info(f"   Fields with BNBO-property relationships: {fields_with_bnbo_props:,}")
        self.log.info(f"   Average properties per field: {avg_props:.1f}")
        self.log.info(f"   Average BNBO water coverage: {avg_bnbo_water:.1f}%")
        self.log.info(f"   Average property BNBO coverage: {avg_property_bnbo:.1f}%")
        self.log.info(f"   Total property-BNBO spatial relationships: {total_relationships:,}")

        return {
            "total_fields": total_fields,
            "fields_with_bnbo": fields_with_bnbo,
            "fields_with_properties": fields_with_props,
            "fields_with_bnbo_properties": fields_with_bnbo_props,
            "avg_properties_per_field": avg_props,
            "avg_bnbo_water_coverage": avg_bnbo_water,
            "avg_field_bnbo_coverage": avg_field_bnbo,
            "avg_property_bnbo_coverage": avg_property_bnbo,
            "total_property_bnbo_relationships": total_relationships,
        }

    def _save_output_data(self, result: Dict[str, Any]):
        """Save final BNBO analysis to GCS."""
        self._save_stage_output("final_bnbo_analysis", "final_bnbo")
