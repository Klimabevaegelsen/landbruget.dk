"""Stage 4A: Final BNBO Analysis

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
        super().__init__(config, "Stage 4A: Final BNBO Analysis")

    def _load_input_data(self):
        """Load BNBO analysis from Stage 3A and pre-filtered properties from Stage 1C."""
        # Load BNBO water coverage from Stage 3A
        stage3a_path = f"gs://{CONFIG.bucket}/gold/{CONFIG.stage_outputs['fields_bnbo_water']}/{CONFIG.stage_outputs['fields_bnbo_water']}.parquet"
        self.gcs_access.query_parquet_direct(stage3a_path, "SELECT *", "fields_bnbo_water")

        # Load pre-filtered field-property intersections from Stage 1C
        stage1c_path = f"gs://{CONFIG.bucket}/gold/{CONFIG.stage_outputs['field_property_intersections']}/{CONFIG.stage_outputs['field_property_intersections']}.parquet"
        self.gcs_access.query_parquet_direct(
            stage1c_path, "SELECT *", "field_property_intersections"
        )

    async def _execute_stage_processing(self) -> Dict[str, Any]:
        """
        Combine BNBO analysis with property information.

        Key optimizations:
        1. Use pre-filtered properties (massive size reduction already applied)
        2. Efficient LEFT JOIN to preserve all fields
        3. Aggregate property data per field for clean final structure
        """

        self.log.info("Combining BNBO analysis with property information...")

        # Create final BNBO analysis with property information
        final_query = """
        CREATE OR REPLACE TABLE final_bnbo_analysis AS
        SELECT 
            b.field_id,
            b.block_id,
            b.cvr_number,
            b.year,
            b.geometry,
            b.field_area_m2,
            b.dominant_soil_code,
            b.dominant_soil_description,
            b.dominant_soil_category,
            b.dominant_soil_share_pct,
            
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
            COALESCE(p.primary_bfe_number, NULL) as primary_bfe_number
            
        FROM fields_bnbo_water b
        LEFT JOIN (
            SELECT 
                field_id,
                block_id,
                cvr_number,
                year,
                COUNT(*) as property_count,
                SUM(intersection_area_m2) as total_property_intersection_area_m2,
                AVG(property_area_share_pct) as avg_property_area_share_pct,
                MAX(property_area_share_pct) as max_property_area_share_pct,
                -- Primary property (largest intersection)
                (
                    SELECT bfe_number 
                    FROM field_property_intersections fp2 
                    WHERE fp2.field_id = fp.field_id 
                      AND fp2.block_id = fp.block_id 
                      AND fp2.cvr_number = fp.cvr_number
                    ORDER BY fp2.intersection_area_m2 DESC 
                    LIMIT 1
                ) as primary_bfe_number
            FROM field_property_intersections fp
            GROUP BY field_id, block_id, cvr_number, year
        ) p ON b.field_id = p.field_id 
           AND b.block_id = p.block_id 
           AND b.cvr_number = p.cvr_number 
           AND b.year = p.year
        """

        self.conn.execute(final_query)

        # Log results
        result_count = self.conn.execute("SELECT COUNT(*) FROM final_bnbo_analysis").fetchone()[0]

        # Get final statistics
        stats = self.conn.execute("""
            SELECT 
                COUNT(*) as total_fields,
                COUNT(CASE WHEN total_bnbo_area_m2 > 0 THEN 1 END) as fields_with_bnbo,
                COUNT(CASE WHEN property_count > 0 THEN 1 END) as fields_with_properties,
                COUNT(CASE WHEN total_bnbo_area_m2 > 0 AND property_count > 0 THEN 1 END) as fields_with_both,
                AVG(property_count) as avg_properties_per_field,
                AVG(CASE WHEN total_bnbo_area_m2 > 0 THEN bnbo_covered_by_water_projects_pct END) as avg_bnbo_water_coverage,
                AVG(CASE WHEN total_bnbo_area_m2 > 0 THEN field_bnbo_coverage_pct END) as avg_field_bnbo_coverage
            FROM final_bnbo_analysis
        """).fetchone()

        (
            total_fields,
            fields_with_bnbo,
            fields_with_props,
            fields_with_both,
            avg_props,
            avg_bnbo_water,
            avg_field_bnbo,
        ) = stats

        # Get BNBO status breakdown with property information
        bnbo_breakdown = self.conn.execute("""
            SELECT 
                COALESCE(dominant_bnbo_status, 'No BNBO') as status,
                COUNT(*) as field_count,
                AVG(property_count) as avg_properties,
                AVG(CASE WHEN total_bnbo_area_m2 > 0 THEN bnbo_covered_by_water_projects_pct END) as avg_water_coverage
            FROM final_bnbo_analysis
            GROUP BY COALESCE(dominant_bnbo_status, 'No BNBO')
            ORDER BY field_count DESC
        """).fetchall()

        self.log.info("✅ Created final BNBO analysis:")
        self.log.info(f"   Total fields: {total_fields:,}")
        self.log.info(
            f"   Fields with BNBO: {fields_with_bnbo:,} ({(fields_with_bnbo / total_fields) * 100:.1f}%)"
        )
        self.log.info(
            f"   Fields with properties: {fields_with_props:,} ({(fields_with_props / total_fields) * 100:.1f}%)"
        )
        self.log.info(f"   Fields with both BNBO and properties: {fields_with_both:,}")
        self.log.info(f"   Average properties per field: {avg_props:.1f}")
        self.log.info(f"   Average BNBO water coverage: {avg_bnbo_water:.1f}%")
        self.log.info(f"   Average field BNBO coverage: {avg_field_bnbo:.1f}%")

        self.log.info("   BNBO status breakdown:")
        for status, count, avg_props_status, avg_water in bnbo_breakdown:
            self.log.info(
                f"     {status}: {count:,} fields, {avg_props_status:.1f} avg properties, {avg_water:.1f}% avg water coverage"
            )

        return {
            "total_fields": total_fields,
            "fields_with_bnbo": fields_with_bnbo,
            "fields_with_properties": fields_with_props,
            "fields_with_both": fields_with_both,
            "avg_properties_per_field": avg_props,
            "avg_bnbo_water_coverage": avg_bnbo_water,
            "avg_field_bnbo_coverage": avg_field_bnbo,
            "bnbo_breakdown": bnbo_breakdown,
        }

    def _save_output_data(self, result: Dict[str, Any]):
        """Save final BNBO analysis to GCS."""
        self._save_stage_output("final_bnbo_analysis", "final_bnbo")
