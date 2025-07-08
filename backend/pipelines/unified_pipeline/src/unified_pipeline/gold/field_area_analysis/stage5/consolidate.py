"""Stage 5: Consolidation

Merge BNBO and wetland analyses into final comprehensive table.
Creates the final field_area_analysis_final dataset.

Optimized for DuckDB Spatial v1.2.2 with single spatial predicates.
"""

from typing import Any, Dict

from ..base import FieldAnalysisStageBase, FieldAnalysisStageConfig
from ..config import CONFIG


class ConsolidateResults(FieldAnalysisStageBase):
    """Consolidate BNBO and wetland analyses into final comprehensive table."""

    def __init__(self, config: FieldAnalysisStageConfig = None):
        if config is None:
            config = FieldAnalysisStageConfig()
        super().__init__(config, "Stage 5: Consolidation")

    def _load_input_data(self):
        """Load final BNBO and wetland analyses from Stage 4."""
        # Load final BNBO analysis from Stage 4A
        stage4a_path = f"gs://{CONFIG.bucket}/gold/{CONFIG.stage_outputs['final_bnbo']}/{CONFIG.stage_outputs['final_bnbo']}.parquet"
        self.gcs_access.query_parquet_direct(stage4a_path, "SELECT *", "final_bnbo_analysis")

        # Load final wetland analysis from Stage 4B
        stage4b_path = f"gs://{CONFIG.bucket}/gold/{CONFIG.stage_outputs['final_wetland']}/{CONFIG.stage_outputs['final_wetland']}.parquet"
        self.gcs_access.query_parquet_direct(stage4b_path, "SELECT *", "final_wetland_analysis")

    async def _execute_stage_processing(self) -> Dict[str, Any]:
        """
        Consolidate BNBO and wetland analyses into final comprehensive table.

        Key optimizations:
        1. Use FULL OUTER JOIN to ensure all fields are included
        2. Combine all environmental and property data
        3. Create final comprehensive schema
        """

        self.log.info("Consolidating BNBO and wetland analyses...")

        # Create final consolidated analysis
        consolidation_query = """
        CREATE OR REPLACE TABLE field_area_analysis_final AS
        SELECT 
            -- Field identification (use COALESCE to handle potential missing data)
            COALESCE(b.field_id, w.field_id) as field_id,
            COALESCE(b.block_id, w.block_id) as block_id,
            COALESCE(b.cvr_number, w.cvr_number) as cvr_number,
            COALESCE(b.year, w.year) as year,
            COALESCE(b.geometry, w.geometry) as geometry,
            COALESCE(b.field_area_m2, w.field_area_m2) as field_area_m2,
            
            -- Soil information
            COALESCE(b.dominant_soil_code, w.dominant_soil_code) as soil_code,
            COALESCE(b.dominant_soil_description, w.dominant_soil_description) as soil_description,
            COALESCE(b.dominant_soil_category, w.dominant_soil_category) as soil_category,
            COALESCE(b.dominant_soil_share_pct, w.dominant_soil_share_pct) as soil_share_pct,
            
            -- BNBO analysis data
            COALESCE(b.total_bnbo_area_m2, 0) as total_bnbo_area_m2,
            COALESCE(b.bnbo_covered_by_water_projects_m2, 0) as bnbo_covered_by_water_projects_m2,
            COALESCE(b.bnbo_covered_by_water_projects_pct, 0) as bnbo_covered_by_water_projects_pct,
            COALESCE(b.bnbo_not_covered_by_water_projects_pct, 0) as bnbo_not_covered_by_water_projects_pct,
            COALESCE(b.field_bnbo_coverage_pct, 0) as field_bnbo_coverage_pct,
            b.dominant_bnbo_status as bnbo_status,
            
            -- Wetland analysis data
            COALESCE(w.total_wetland_area_m2, 0) as total_wetland_area_m2,
            COALESCE(w.wetland_covered_by_water_projects_m2, 0) as wetland_covered_by_water_projects_m2,
            COALESCE(w.wetland_covered_by_water_projects_pct, 0) as wetland_covered_by_water_projects_pct,
            COALESCE(w.wetland_not_covered_by_water_projects_pct, 0) as wetland_not_covered_by_water_projects_pct,
            COALESCE(w.field_wetland_coverage_pct, 0) as field_wetland_coverage_pct,
            w.dominant_wetland_type as wetland_type,
            COALESCE(w.wetland_polygon_count, 0) as wetland_polygon_count,
            
            -- Property information (use BNBO as primary, fallback to wetland)
            COALESCE(b.property_count, w.property_count, 0) as property_count,
            COALESCE(b.total_property_intersection_area_m2, w.total_property_intersection_area_m2, 0) as total_property_intersection_area_m2,
            COALESCE(b.avg_property_area_share_pct, w.avg_property_area_share_pct, 0) as avg_property_area_share_pct,
            COALESCE(b.max_property_area_share_pct, w.max_property_area_share_pct, 0) as max_property_area_share_pct,
            COALESCE(b.primary_bfe_number, w.primary_bfe_number) as primary_bfe_number,
            
            -- Calculated field-level percentages
            CASE WHEN COALESCE(b.field_area_m2, w.field_area_m2) > 0 
            THEN (COALESCE(b.total_bnbo_area_m2, 0) / COALESCE(b.field_area_m2, w.field_area_m2)) * 100
            ELSE 0 END as field_bnbo_percentage,
            
            CASE WHEN COALESCE(b.field_area_m2, w.field_area_m2) > 0 
            THEN (COALESCE(w.total_wetland_area_m2, 0) / COALESCE(b.field_area_m2, w.field_area_m2)) * 100
            ELSE 0 END as field_wetland_percentage,
            
            CASE WHEN COALESCE(b.field_area_m2, w.field_area_m2) > 0 
            THEN (COALESCE(b.total_property_intersection_area_m2, w.total_property_intersection_area_m2, 0) / COALESCE(b.field_area_m2, w.field_area_m2)) * 100
            ELSE 0 END as field_property_coverage_percentage
            
        FROM final_bnbo_analysis b
        FULL OUTER JOIN final_wetland_analysis w 
            ON b.field_id = w.field_id 
            AND b.block_id = w.block_id 
            AND b.cvr_number = w.cvr_number 
            AND b.year = w.year
        """

        self.conn.execute(consolidation_query)

        # Log results
        result_count = self.conn.execute(
            "SELECT COUNT(*) FROM field_area_analysis_final"
        ).fetchone()[0]

        # Get comprehensive statistics
        stats = self.conn.execute("""
            SELECT 
                COUNT(*) as total_fields,
                COUNT(CASE WHEN total_bnbo_area_m2 > 0 THEN 1 END) as fields_with_bnbo,
                COUNT(CASE WHEN total_wetland_area_m2 > 0 THEN 1 END) as fields_with_wetlands,
                COUNT(CASE WHEN property_count > 0 THEN 1 END) as fields_with_properties,
                COUNT(CASE WHEN total_bnbo_area_m2 > 0 AND total_wetland_area_m2 > 0 THEN 1 END) as fields_with_both_env,
                COUNT(CASE WHEN total_bnbo_area_m2 > 0 AND property_count > 0 THEN 1 END) as fields_with_bnbo_and_props,
                COUNT(CASE WHEN total_wetland_area_m2 > 0 AND property_count > 0 THEN 1 END) as fields_with_wetland_and_props,
                COUNT(CASE WHEN total_bnbo_area_m2 > 0 AND total_wetland_area_m2 > 0 AND property_count > 0 THEN 1 END) as fields_with_all,
                
                -- Average coverages
                AVG(field_bnbo_percentage) as avg_field_bnbo_pct,
                AVG(field_wetland_percentage) as avg_field_wetland_pct,
                AVG(field_property_coverage_percentage) as avg_field_property_pct,
                
                -- Water project coverages
                AVG(CASE WHEN total_bnbo_area_m2 > 0 THEN bnbo_covered_by_water_projects_pct END) as avg_bnbo_water_coverage,
                AVG(CASE WHEN total_wetland_area_m2 > 0 THEN wetland_covered_by_water_projects_pct END) as avg_wetland_water_coverage,
                
                -- Total areas
                SUM(field_area_m2) / 1000000 as total_field_area_km2,
                SUM(total_bnbo_area_m2) / 1000000 as total_bnbo_area_km2,
                SUM(total_wetland_area_m2) / 1000000 as total_wetland_area_km2,
                SUM(bnbo_covered_by_water_projects_m2) / 1000000 as total_bnbo_covered_km2,
                SUM(wetland_covered_by_water_projects_m2) / 1000000 as total_wetland_covered_km2
            FROM field_area_analysis_final
        """).fetchone()

        (
            total_fields,
            fields_with_bnbo,
            fields_with_wetlands,
            fields_with_props,
            fields_with_both_env,
            fields_with_bnbo_props,
            fields_with_wetland_props,
            fields_with_all,
            avg_bnbo_pct,
            avg_wetland_pct,
            avg_prop_pct,
            avg_bnbo_water,
            avg_wetland_water,
            total_area_km2,
            bnbo_area_km2,
            wetland_area_km2,
            bnbo_covered_km2,
            wetland_covered_km2,
        ) = stats

        # Get environmental category breakdown
        env_breakdown = self.conn.execute("""
            SELECT 
                CASE 
                    WHEN total_bnbo_area_m2 > 0 AND total_wetland_area_m2 > 0 THEN 'Both BNBO and Wetlands'
                    WHEN total_bnbo_area_m2 > 0 THEN 'BNBO Only'
                    WHEN total_wetland_area_m2 > 0 THEN 'Wetlands Only'
                    ELSE 'No Environmental Features'
                END as environmental_category,
                COUNT(*) as field_count,
                AVG(property_count) as avg_properties,
                AVG(field_bnbo_percentage + field_wetland_percentage) as avg_total_env_coverage
            FROM field_area_analysis_final
            GROUP BY 
                CASE 
                    WHEN total_bnbo_area_m2 > 0 AND total_wetland_area_m2 > 0 THEN 'Both BNBO and Wetlands'
                    WHEN total_bnbo_area_m2 > 0 THEN 'BNBO Only'
                    WHEN total_wetland_area_m2 > 0 THEN 'Wetlands Only'
                    ELSE 'No Environmental Features'
                END
            ORDER BY field_count DESC
        """).fetchall()

        self.log.info("✅ Created final consolidated field area analysis:")
        self.log.info(f"   Total fields: {total_fields:,}")
        self.log.info(
            f"   Fields with BNBO: {fields_with_bnbo:,} ({(fields_with_bnbo / total_fields) * 100:.1f}%)"
        )
        self.log.info(
            f"   Fields with wetlands: {fields_with_wetlands:,} ({(fields_with_wetlands / total_fields) * 100:.1f}%)"
        )
        self.log.info(
            f"   Fields with properties: {fields_with_props:,} ({(fields_with_props / total_fields) * 100:.1f}%)"
        )
        self.log.info(f"   Fields with both environmental features: {fields_with_both_env:,}")
        self.log.info(f"   Fields with all data types: {fields_with_all:,}")

        self.log.info("   Average coverage percentages:")
        self.log.info(f"     BNBO: {avg_bnbo_pct:.2f}%")
        self.log.info(f"     Wetlands: {avg_wetland_pct:.2f}%")
        self.log.info(f"     Properties: {avg_prop_pct:.2f}%")

        self.log.info("   Water project coverage:")
        self.log.info(f"     BNBO areas: {avg_bnbo_water:.1f}%")
        self.log.info(f"     Wetland areas: {avg_wetland_water:.1f}%")

        self.log.info("   Total areas:")
        self.log.info(f"     Fields: {total_area_km2:.1f} km²")
        self.log.info(
            f"     BNBO: {bnbo_area_km2:.1f} km² ({(bnbo_area_km2 / total_area_km2) * 100:.1f}% of fields)"
        )
        self.log.info(
            f"     Wetlands: {wetland_area_km2:.1f} km² ({(wetland_area_km2 / total_area_km2) * 100:.1f}% of fields)"
        )
        self.log.info(f"     BNBO covered by water projects: {bnbo_covered_km2:.1f} km²")
        self.log.info(f"     Wetlands covered by water projects: {wetland_covered_km2:.1f} km²")

        self.log.info("   Environmental category breakdown:")
        for category, count, avg_props, avg_env in env_breakdown:
            self.log.info(
                f"     {category}: {count:,} fields ({(count / total_fields) * 100:.1f}%), {avg_props:.1f} avg properties, {avg_env:.1f}% avg env coverage"
            )

        return {
            "total_fields": total_fields,
            "fields_with_bnbo": fields_with_bnbo,
            "fields_with_wetlands": fields_with_wetlands,
            "fields_with_properties": fields_with_props,
            "fields_with_both_env": fields_with_both_env,
            "fields_with_all": fields_with_all,
            "avg_bnbo_pct": avg_bnbo_pct,
            "avg_wetland_pct": avg_wetland_pct,
            "avg_property_pct": avg_prop_pct,
            "avg_bnbo_water_coverage": avg_bnbo_water,
            "avg_wetland_water_coverage": avg_wetland_water,
            "total_area_km2": total_area_km2,
            "bnbo_area_km2": bnbo_area_km2,
            "wetland_area_km2": wetland_area_km2,
            "env_breakdown": env_breakdown,
        }

    def _save_output_data(self, result: Dict[str, Any]):
        """Save final consolidated analysis to GCS."""
        self._save_stage_output("field_area_analysis_final", "consolidated")
