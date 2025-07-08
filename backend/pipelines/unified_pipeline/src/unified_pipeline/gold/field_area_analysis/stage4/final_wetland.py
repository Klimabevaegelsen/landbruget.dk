"""Stage 4B: Final Wetland Analysis

Combine wetland water coverage analysis with pre-filtered properties.
Creates the final wetland analysis table ready for consolidation.

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
        super().__init__(config, "Stage 4B: Final Wetland Analysis")

    def _load_input_data(self):
        """Load wetland analysis from Stage 3B and pre-filtered properties from Stage 1C."""
        # Load wetland water coverage from Stage 3B
        stage3b_path = f"gs://{CONFIG.bucket}/gold/{CONFIG.stage_outputs['fields_wetland_water']}/{CONFIG.stage_outputs['fields_wetland_water']}.parquet"
        self.gcs_access.query_parquet_direct(stage3b_path, "SELECT *", "fields_wetland_water")

        # Load pre-filtered field-property intersections from Stage 1C
        stage1c_path = f"gs://{CONFIG.bucket}/gold/{CONFIG.stage_outputs['field_property_intersections']}/{CONFIG.stage_outputs['field_property_intersections']}.parquet"
        self.gcs_access.query_parquet_direct(
            stage1c_path, "SELECT *", "field_property_intersections"
        )

    async def _execute_stage_processing(self) -> Dict[str, Any]:
        """
        Combine wetland analysis with property information.

        Key optimizations:
        1. Use pre-filtered properties (massive size reduction already applied)
        2. Efficient LEFT JOIN to preserve all fields
        3. Aggregate property data per field for clean final structure
        """

        self.log.info("Combining wetland analysis with property information...")

        # Create final wetland analysis with property information
        final_query = """
        CREATE OR REPLACE TABLE final_wetland_analysis AS
        SELECT 
            w.field_id,
            w.block_id,
            w.cvr_number,
            w.year,
            w.geometry,
            w.field_area_m2,
            w.dominant_soil_code,
            w.dominant_soil_description,
            w.dominant_soil_category,
            w.dominant_soil_share_pct,
            
            -- Wetland analysis data
            w.total_wetland_area_m2,
            w.wetland_covered_by_water_projects_m2,
            w.wetland_covered_by_water_projects_pct,
            w.wetland_not_covered_by_water_projects_pct,
            w.field_wetland_coverage_pct,
            w.dominant_wetland_type,
            w.wetland_polygon_count,
            
            -- Property information (aggregated per field)
            COALESCE(p.property_count, 0) as property_count,
            COALESCE(p.total_property_intersection_area_m2, 0) as total_property_intersection_area_m2,
            COALESCE(p.avg_property_area_share_pct, 0) as avg_property_area_share_pct,
            COALESCE(p.max_property_area_share_pct, 0) as max_property_area_share_pct,
            COALESCE(p.primary_bfe_number, NULL) as primary_bfe_number
            
        FROM fields_wetland_water w
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
        ) p ON w.field_id = p.field_id 
           AND w.block_id = p.block_id 
           AND w.cvr_number = p.cvr_number 
           AND w.year = p.year
        """

        self.conn.execute(final_query)

        # Log results
        result_count = self.conn.execute("SELECT COUNT(*) FROM final_wetland_analysis").fetchone()[
            0
        ]

        # Get final statistics
        stats = self.conn.execute("""
            SELECT 
                COUNT(*) as total_fields,
                COUNT(CASE WHEN total_wetland_area_m2 > 0 THEN 1 END) as fields_with_wetlands,
                COUNT(CASE WHEN property_count > 0 THEN 1 END) as fields_with_properties,
                COUNT(CASE WHEN total_wetland_area_m2 > 0 AND property_count > 0 THEN 1 END) as fields_with_both,
                AVG(property_count) as avg_properties_per_field,
                AVG(CASE WHEN total_wetland_area_m2 > 0 THEN wetland_covered_by_water_projects_pct END) as avg_wetland_water_coverage,
                AVG(CASE WHEN total_wetland_area_m2 > 0 THEN field_wetland_coverage_pct END) as avg_field_wetland_coverage,
                AVG(wetland_polygon_count) as avg_wetland_polygons_per_field
            FROM final_wetland_analysis
        """).fetchone()

        (
            total_fields,
            fields_with_wetlands,
            fields_with_props,
            fields_with_both,
            avg_props,
            avg_wetland_water,
            avg_field_wetland,
            avg_polygons,
        ) = stats

        # Get wetland type breakdown with property information
        wetland_breakdown = self.conn.execute("""
            SELECT 
                COALESCE(dominant_wetland_type, 'No Wetlands') as wetland_type,
                COUNT(*) as field_count,
                AVG(property_count) as avg_properties,
                AVG(CASE WHEN total_wetland_area_m2 > 0 THEN wetland_covered_by_water_projects_pct END) as avg_water_coverage,
                AVG(wetland_polygon_count) as avg_polygons
            FROM final_wetland_analysis
            GROUP BY COALESCE(dominant_wetland_type, 'No Wetlands')
            ORDER BY field_count DESC
        """).fetchall()

        self.log.info("✅ Created final wetland analysis:")
        self.log.info(f"   Total fields: {total_fields:,}")
        self.log.info(
            f"   Fields with wetlands: {fields_with_wetlands:,} ({(fields_with_wetlands / total_fields) * 100:.1f}%)"
        )
        self.log.info(
            f"   Fields with properties: {fields_with_props:,} ({(fields_with_props / total_fields) * 100:.1f}%)"
        )
        self.log.info(f"   Fields with both wetlands and properties: {fields_with_both:,}")
        self.log.info(f"   Average properties per field: {avg_props:.1f}")
        self.log.info(f"   Average wetland water coverage: {avg_wetland_water:.1f}%")
        self.log.info(f"   Average field wetland coverage: {avg_field_wetland:.1f}%")
        self.log.info(f"   Average wetland polygons per field: {avg_polygons:.1f}")

        self.log.info("   Wetland type breakdown:")
        for wetland_type, count, avg_props_type, avg_water, avg_polys in wetland_breakdown[
            :5
        ]:  # Top 5
            self.log.info(
                f"     {wetland_type}: {count:,} fields, {avg_props_type:.1f} avg properties, {avg_water:.1f}% avg water coverage"
            )

        return {
            "total_fields": total_fields,
            "fields_with_wetlands": fields_with_wetlands,
            "fields_with_properties": fields_with_props,
            "fields_with_both": fields_with_both,
            "avg_properties_per_field": avg_props,
            "avg_wetland_water_coverage": avg_wetland_water,
            "avg_field_wetland_coverage": avg_field_wetland,
            "avg_polygons_per_field": avg_polygons,
            "wetland_breakdown": wetland_breakdown,
        }

    def _save_output_data(self, result: Dict[str, Any]):
        """Save final wetland analysis to GCS."""
        self._save_stage_output("final_wetland_analysis", "final_wetland")
