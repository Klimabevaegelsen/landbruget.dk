"""Stage 4: Consolidation

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
        super().__init__(config, "Stage 4: Consolidation")

    def _load_input_data(self):
        """Load final BNBO and wetland analyses from Stage 3."""
        # Load final BNBO analysis from Stage 3A
        stage3a_dataset = CONFIG.stage_outputs["final_bnbo"]
        stage3a_path = self._get_latest_gold_path(stage3a_dataset)
        self.gcs_access.query_parquet_direct(stage3a_path, "SELECT *", "final_bnbo_analysis")

        # Load final wetland analysis from Stage 3B
        stage3b_dataset = CONFIG.stage_outputs["final_wetland"]
        stage3b_path = self._get_latest_gold_path(stage3b_dataset)
        self.gcs_access.query_parquet_direct(stage3b_path, "SELECT *", "final_wetland_analysis")

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
            
            -- BNBO analysis data
            COALESCE(b.total_bnbo_area_m2, 0) as total_bnbo_area_m2,
            COALESCE(b.bnbo_covered_by_water_projects_m2, 0) as bnbo_covered_by_water_projects_m2,
            COALESCE(b.bnbo_covered_by_water_projects_pct, 0) as bnbo_covered_by_water_projects_pct,
            COALESCE(b.bnbo_not_covered_by_water_projects_pct, 0) as bnbo_not_covered_by_water_projects_pct,
            COALESCE(b.field_bnbo_coverage_pct, 0) as field_bnbo_coverage_pct,
            COALESCE(b.dominant_bnbo_status, 'No BNBO') as dominant_bnbo_status,
            
            -- Wetland analysis data
            COALESCE(w.total_wetland_area_m2, 0) as total_wetland_area_m2,
            COALESCE(w.wetland_covered_by_water_projects_m2, 0) as wetland_covered_by_water_projects_m2,
            COALESCE(w.wetland_covered_by_water_projects_pct, 0) as wetland_covered_by_water_projects_pct,
            COALESCE(w.wetland_not_covered_by_water_projects_pct, 0) as wetland_not_covered_by_water_projects_pct,
            COALESCE(w.field_wetland_coverage_pct, 0) as field_wetland_coverage_pct,

            
            -- Property ownership analysis (use BNBO data as primary, fallback to wetland)
            COALESCE(b.property_count, w.property_count, 0) as property_count,
            COALESCE(b.total_property_intersection_area_m2, w.total_property_intersection_area_m2, 0) as total_property_intersection_area_m2,
            COALESCE(b.avg_property_area_share_pct, w.avg_property_area_share_pct, 0) as avg_property_area_share_pct,
            COALESCE(b.max_property_area_share_pct, w.max_property_area_share_pct, 0) as max_property_area_share_pct,
            COALESCE(b.primary_bfe_number, w.primary_bfe_number) as primary_bfe_number,
            
            -- Property-environmental spatial relationships
            COALESCE(b.property_bnbo_intersection_area_m2, 0) as property_bnbo_intersection_area_m2,
            COALESCE(b.property_bnbo_coverage_pct, 0) as property_bnbo_coverage_pct,
            COALESCE(b.properties_with_bnbo_count, 0) as properties_with_bnbo_count,
            COALESCE(b.bnbo_property_owners, NULL) as bnbo_property_owners,
            
            COALESCE(w.property_wetland_intersection_area_m2, 0) as property_wetland_intersection_area_m2,
            COALESCE(w.property_wetland_coverage_pct, 0) as property_wetland_coverage_pct,
            COALESCE(w.properties_with_wetland_count, 0) as properties_with_wetland_count,
            COALESCE(w.wetland_property_owners, NULL) as wetland_property_owners,
            
            -- Property-level wetland water project coverage
            COALESCE(w.property_wetland_covered_by_water_m2, 0) as property_wetland_covered_by_water_m2,
            COALESCE(w.property_wetland_not_covered_by_water_m2, 0) as property_wetland_not_covered_by_water_m2,
            COALESCE(w.property_wetland_water_coverage_pct, 0) as property_wetland_water_coverage_pct,
            COALESCE(w.properties_with_covered_wetland_count, 0) as properties_with_covered_wetland_count,
            COALESCE(w.properties_with_uncovered_wetland_count, 0) as properties_with_uncovered_wetland_count,
            COALESCE(w.covered_wetland_property_owners, NULL) as covered_wetland_property_owners,
            COALESCE(w.uncovered_wetland_property_owners, NULL) as uncovered_wetland_property_owners,
            
            -- Combined environmental-property metrics
            COALESCE(b.properties_with_bnbo_count, 0) + COALESCE(w.properties_with_wetland_count, 0) as total_properties_with_environmental_features,
            CASE 
                WHEN COALESCE(b.property_count, w.property_count, 0) > 0 
                THEN ((COALESCE(b.property_bnbo_intersection_area_m2, 0) + COALESCE(w.property_wetland_intersection_area_m2, 0)) / 
                      COALESCE(b.total_property_intersection_area_m2, w.total_property_intersection_area_m2, 1)) * 100
                ELSE 0 
            END as combined_property_environmental_coverage_pct,
            
            -- Environmental summary flags
            CASE 
                WHEN COALESCE(b.total_bnbo_area_m2, 0) > 0 OR COALESCE(w.total_wetland_area_m2, 0) > 0 
                THEN TRUE ELSE FALSE 
            END as has_environmental_features,
            
            CASE 
                WHEN COALESCE(b.properties_with_bnbo_count, 0) > 0 OR COALESCE(w.properties_with_wetland_count, 0) > 0 
                THEN TRUE ELSE FALSE 
            END as has_property_environmental_relationships
            
        FROM final_bnbo_analysis b
        FULL OUTER JOIN final_wetland_analysis w ON b.field_id = w.field_id 
            AND b.block_id = w.block_id 
            AND b.cvr_number = w.cvr_number 
            AND b.year = w.year
        """

        self.conn.execute(consolidation_query)

        # Log results
        result_count = self.conn.execute(
            "SELECT COUNT(*) FROM field_area_analysis_final"
        ).fetchone()[0]

        # Get comprehensive statistics including property-environmental relationships
        stats = self.conn.execute("""
            SELECT 
                COUNT(*) as total_fields,
                COUNT(CASE WHEN total_bnbo_area_m2 > 0 THEN 1 END) as fields_with_bnbo,
                COUNT(CASE WHEN total_wetland_area_m2 > 0 THEN 1 END) as fields_with_wetlands,
                COUNT(CASE WHEN property_count > 0 THEN 1 END) as fields_with_properties,
                COUNT(CASE WHEN has_environmental_features THEN 1 END) as fields_with_environmental_features,
                COUNT(CASE WHEN has_property_environmental_relationships THEN 1 END) as fields_with_property_env_relationships,
                COUNT(CASE WHEN properties_with_bnbo_count > 0 THEN 1 END) as fields_with_bnbo_property_relationships,
                COUNT(CASE WHEN properties_with_wetland_count > 0 THEN 1 END) as fields_with_wetland_property_relationships,
                
                -- Average coverages
                AVG(field_bnbo_coverage_pct) as avg_field_bnbo_pct,
                AVG(field_wetland_coverage_pct) as avg_field_wetland_pct,
                AVG(combined_property_environmental_coverage_pct) as avg_property_environmental_pct,
                AVG(property_count) as avg_properties_per_field,
                
                -- Water project coverages
                AVG(CASE WHEN total_bnbo_area_m2 > 0 THEN bnbo_covered_by_water_projects_pct END) as avg_bnbo_water_coverage,
                AVG(CASE WHEN total_wetland_area_m2 > 0 THEN wetland_covered_by_water_projects_pct END) as avg_wetland_water_coverage,
                AVG(CASE WHEN properties_with_wetland_count > 0 THEN property_wetland_water_coverage_pct END) as avg_property_wetland_water_coverage,
                
                -- Property-environmental spatial relationships
                SUM(properties_with_bnbo_count) as total_bnbo_property_relationships,
                SUM(properties_with_wetland_count) as total_wetland_property_relationships,
                SUM(total_properties_with_environmental_features) as total_environmental_property_relationships,
                
                -- Total areas
                SUM(field_area_m2) / 1000000 as total_field_area_km2,
                SUM(total_bnbo_area_m2) / 1000000 as total_bnbo_area_km2,
                SUM(total_wetland_area_m2) / 1000000 as total_wetland_area_km2,
                SUM(bnbo_covered_by_water_projects_m2) / 1000000 as total_bnbo_covered_km2,
                SUM(wetland_covered_by_water_projects_m2) / 1000000 as total_wetland_covered_km2,
                
                -- Property-level wetland water coverage totals
                SUM(property_wetland_covered_by_water_m2) / 1000000 as total_property_wetland_covered_km2,
                SUM(property_wetland_not_covered_by_water_m2) / 1000000 as total_property_wetland_uncovered_km2,
                SUM(properties_with_covered_wetland_count) as total_properties_with_covered_wetlands,
                SUM(properties_with_uncovered_wetland_count) as total_properties_with_uncovered_wetlands
            FROM field_area_analysis_final
        """).fetchone()

        (
            total_fields,
            fields_with_bnbo,
            fields_with_wetlands,
            fields_with_props,
            fields_with_env_features,
            fields_with_prop_env_relationships,
            fields_with_bnbo_prop_relationships,
            fields_with_wetland_prop_relationships,
            avg_bnbo_pct,
            avg_wetland_pct,
            avg_prop_env_pct,
            avg_props_per_field,
            avg_bnbo_water,
            avg_wetland_water,
            avg_property_wetland_water,
            total_bnbo_prop_relationships,
            total_wetland_prop_relationships,
            total_env_prop_relationships,
            total_area_km2,
            bnbo_area_km2,
            wetland_area_km2,
            bnbo_covered_km2,
            wetland_covered_km2,
            total_property_wetland_covered_km2,
            total_property_wetland_uncovered_km2,
            total_properties_with_covered_wetlands,
            total_properties_with_uncovered_wetlands,
        ) = stats

        # Get environmental category breakdown with property relationships
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
                AVG(field_bnbo_coverage_pct + field_wetland_coverage_pct) as avg_total_env_coverage,
                AVG(combined_property_environmental_coverage_pct) as avg_property_env_coverage,
                SUM(properties_with_bnbo_count + properties_with_wetland_count) as total_property_relationships
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

        self.log.info(
            "✅ Created final consolidated field area analysis with property-environmental relationships:"
        )
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
        self.log.info(f"   Fields with environmental features: {fields_with_env_features:,}")
        self.log.info(
            f"   Fields with property-environmental relationships: {fields_with_prop_env_relationships:,}"
        )
        self.log.info(
            f"   Fields with BNBO-property relationships: {fields_with_bnbo_prop_relationships:,}"
        )
        self.log.info(
            f"   Fields with wetland-property relationships: {fields_with_wetland_prop_relationships:,}"
        )

        self.log.info("   Average coverage percentages:")
        self.log.info(f"     BNBO: {avg_bnbo_pct:.2f}%")
        self.log.info(f"     Wetlands: {avg_wetland_pct:.2f}%")
        self.log.info(f"     Property-environmental: {avg_prop_env_pct:.2f}%")
        self.log.info(f"     Properties per field: {avg_props_per_field:.1f}")

        self.log.info("   Property-environmental spatial relationships:")
        self.log.info(f"     Total BNBO-property relationships: {total_bnbo_prop_relationships:,}")
        self.log.info(
            f"     Total wetland-property relationships: {total_wetland_prop_relationships:,}"
        )
        self.log.info(
            f"     Total environmental-property relationships: {total_env_prop_relationships:,}"
        )

        self.log.info("   Water project coverage:")
        self.log.info(f"     BNBO areas: {avg_bnbo_water:.1f}%")
        self.log.info(f"     Wetland areas: {avg_wetland_water:.1f}%")
        self.log.info(f"     Property wetland areas: {avg_property_wetland_water:.1f}%")

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
        self.log.info("🏠 Property-level wetland water project coverage:")
        self.log.info(
            f"     Property wetland area covered by water projects: {total_property_wetland_covered_km2:.1f} km²"
        )
        self.log.info(
            f"     Property wetland area NOT covered by water projects: {total_property_wetland_uncovered_km2:.1f} km²"
        )
        self.log.info(
            f"     Properties with covered wetlands: {total_properties_with_covered_wetlands:,}"
        )
        self.log.info(
            f"     Properties with uncovered wetlands: {total_properties_with_uncovered_wetlands:,}"
        )

        self.log.info("   Environmental category breakdown:")
        for category, count, avg_props, avg_env, avg_prop_env, total_relationships in env_breakdown:
            self.log.info(
                f"     {category}: {count:,} fields ({(count / total_fields) * 100:.1f}%), {avg_props:.1f} avg properties, {avg_env:.1f}% env coverage, {total_relationships:,} property relationships"
            )

        return {
            "total_fields": total_fields,
            "fields_with_bnbo": fields_with_bnbo,
            "fields_with_wetlands": fields_with_wetlands,
            "fields_with_properties": fields_with_props,
            "fields_with_environmental_features": fields_with_env_features,
            "fields_with_property_env_relationships": fields_with_prop_env_relationships,
            "avg_bnbo_pct": avg_bnbo_pct,
            "avg_wetland_pct": avg_wetland_pct,
            "avg_property_environmental_pct": avg_prop_env_pct,
            "avg_bnbo_water_coverage": avg_bnbo_water,
            "avg_wetland_water_coverage": avg_wetland_water,
            "avg_property_wetland_water_coverage": avg_property_wetland_water,
            "total_area_km2": total_area_km2,
            "bnbo_area_km2": bnbo_area_km2,
            "wetland_area_km2": wetland_area_km2,
            "total_bnbo_property_relationships": total_bnbo_prop_relationships,
            "total_wetland_property_relationships": total_wetland_prop_relationships,
            "total_environmental_property_relationships": total_env_prop_relationships,
            "total_property_wetland_covered_km2": total_property_wetland_covered_km2,
            "total_property_wetland_uncovered_km2": total_property_wetland_uncovered_km2,
            "total_properties_with_covered_wetlands": total_properties_with_covered_wetlands,
            "total_properties_with_uncovered_wetlands": total_properties_with_uncovered_wetlands,
            "env_breakdown": env_breakdown,
        }

    def _save_output_data(self, result: Dict[str, Any]):
        """Save final consolidated analysis to GCS."""
        self._save_stage_output("field_area_analysis_final", "consolidated")
