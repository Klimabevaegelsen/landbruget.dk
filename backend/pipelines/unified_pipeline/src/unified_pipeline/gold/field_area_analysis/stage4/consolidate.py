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
        """Load all field-property intersections, soil data, and environmental analyses from previous stages."""
        # Load ALL field-property intersections from Stage 1C (foundation for all fields with properties)
        stage1c_dataset = CONFIG.stage_outputs["field_property_intersections"]
        stage1c_path = self._get_latest_gold_path(stage1c_dataset)
        self.gcs_access.query_parquet_direct(
            stage1c_path, "SELECT *", "field_property_intersections"
        )

        # Load field-soil intersections from Stage 1B
        stage1b_dataset = CONFIG.stage_outputs["field_soil_intersections"]
        stage1b_path = self._get_latest_gold_path(stage1b_dataset)
        self.gcs_access.query_parquet_direct(stage1b_path, "SELECT *", "field_soil_areas")

        # Load final BNBO analysis from Stage 3A (only fields with BNBO)
        stage3a_dataset = CONFIG.stage_outputs["final_bnbo"]
        stage3a_path = self._get_latest_gold_path(stage3a_dataset)
        self.gcs_access.query_parquet_direct(stage3a_path, "SELECT *", "final_bnbo_analysis")

        # Load final wetland analysis from Stage 3B (only fields with wetlands)
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

        self.log.info("Consolidating ALL fields with property data and environmental analyses...")

        # First, create a comprehensive field summary with property data
        self.log.info("Step 1: Creating field-property summary from Stage 1C...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE all_fields_with_properties AS
            SELECT 
                field_id,
                block_id,
                cvr_number,
                year,
                field_uuid,
                field_geometry as geometry,
                field_area_m2,
                
                -- Property ownership summary
                COUNT(*) as property_count,
                SUM(intersection_area_m2) as total_property_intersection_area_m2,
                (
                    SELECT bfe_number 
                    FROM field_property_intersections fp2 
                    WHERE fp2.field_id = fp.field_id 
                    AND fp2.block_id = fp.block_id 
                    AND fp2.cvr_number = fp.cvr_number
                    AND fp2.year = fp.year
                    ORDER BY fp2.intersection_area_m2 DESC 
                    LIMIT 1
                ) as primary_bfe_number
                
            FROM field_property_intersections fp
            GROUP BY field_id, block_id, cvr_number, year, field_uuid, field_geometry, field_area_m2
        """)

        all_fields_count = self.conn.execute(
            "SELECT COUNT(*) FROM all_fields_with_properties"
        ).fetchone()[0]
        self.log.info(f"Found {all_fields_count:,} fields with property intersections")

        # Create soil type summary per field from Stage 1B data
        self.log.info("Step 2: Creating field-level soil type summary...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_soil_summary AS
            SELECT 
                field_id,
                block_id,
                cvr_number,
                year,
                
                -- Soil type diversity metrics
                COUNT(DISTINCT soil_type_category) as soil_type_count,
                COUNT(DISTINCT soil_code) as unique_soil_codes,
                
                -- Dominant soil type (largest area)
                (
                    SELECT soil_type_category 
                    FROM field_soil_areas fsa2 
                    WHERE fsa2.field_id = fsa.field_id 
                    AND fsa2.block_id = fsa.block_id 
                    AND fsa2.cvr_number = fsa.cvr_number 
                    AND fsa2.year = fsa.year
                    ORDER BY fsa2.soil_area_m2 DESC 
                    LIMIT 1
                ) as dominant_soil_type,
                
                -- Dominant soil coverage percentage
                (
                    SELECT soil_area_share_pct 
                    FROM field_soil_areas fsa2 
                    WHERE fsa2.field_id = fsa.field_id 
                    AND fsa2.block_id = fsa.block_id 
                    AND fsa2.cvr_number = fsa.cvr_number 
                    AND fsa2.year = fsa.year
                    ORDER BY fsa2.soil_area_m2 DESC 
                    LIMIT 1
                ) as dominant_soil_coverage_pct,
                
                -- Total soil coverage (should be close to 100% for most fields)
                SUM(soil_area_share_pct) as total_soil_coverage_pct,
                
                -- Soil breakdown as JSON: {soil_type: {area_m2, coverage_pct}}
                '{' || STRING_AGG(
                    '"' || soil_type_category || '": {' ||
                    '"area_m2": ' || ROUND(soil_area_m2, 2) || ', ' ||
                    '"coverage_pct": ' || ROUND(soil_area_share_pct, 2) ||
                    '}', ', '
                    ORDER BY soil_area_m2 DESC
                ) || '}' as soil_type_breakdown
                
            FROM field_soil_areas fsa
            GROUP BY field_id, block_id, cvr_number, year
        """)

        soil_fields_count = self.conn.execute("SELECT COUNT(*) FROM field_soil_summary").fetchone()[
            0
        ]
        self.log.info(f"Created soil summaries for {soil_fields_count:,} fields")

        # Create final consolidated analysis starting with ALL fields that have properties
        self.log.info(
            "Step 3: Creating final analysis with environmental and soil data as LEFT JOINs..."
        )
        consolidation_query = """
        CREATE OR REPLACE TABLE field_area_analysis_final AS
        SELECT 
            -- Field identification (from property intersections - guaranteed to exist)
            f.field_id,
            f.block_id,
            f.cvr_number,
            f.year,
            f.field_uuid,
            f.geometry,
            f.field_area_m2,
            
            -- Property ownership data (from Stage 1C - guaranteed to exist)
            f.property_count,
            f.total_property_intersection_area_m2,
            f.primary_bfe_number,
            
            -- Soil type data (from Stage 1B - may be NULL if no soil data)
            COALESCE(s.soil_type_count, 0) as soil_type_count,
            COALESCE(s.unique_soil_codes, 0) as unique_soil_codes,
            COALESCE(s.dominant_soil_type, NULL) as dominant_soil_type,
            COALESCE(s.dominant_soil_coverage_pct, 0) as dominant_soil_coverage_pct,
            COALESCE(s.total_soil_coverage_pct, 0) as total_soil_coverage_pct,
            COALESCE(s.soil_type_breakdown, '{}') as soil_type_breakdown,
            
            -- BNBO analysis data (from Stage 3A - may be NULL if no BNBO)
            COALESCE(b.field_bnbo_total_m2, 0) as field_bnbo_total_m2,
            COALESCE(b.field_bnbo_water_covered_m2, 0) as field_bnbo_water_covered_m2,
            COALESCE(b.field_bnbo_water_covered_pct, 0) as field_bnbo_water_covered_pct,
            COALESCE(b.field_bnbo_water_uncovered_pct, 0) as field_bnbo_water_uncovered_pct,
            COALESCE(b.field_bnbo_coverage_pct, 0) as field_bnbo_coverage_pct,
            
            -- Wetland analysis data (from Stage 3B - may be NULL if no wetlands)
            COALESCE(w.field_wetland_total_m2, 0) as field_wetland_total_m2,
            COALESCE(w.field_wetland_water_covered_m2, 0) as field_wetland_water_covered_m2,
            COALESCE(w.field_wetland_water_covered_pct, 0) as field_wetland_water_covered_pct,
            COALESCE(w.field_wetland_water_uncovered_pct, 0) as field_wetland_water_uncovered_pct,
            COALESCE(w.field_wetland_coverage_pct, 0) as field_wetland_coverage_pct,
            
            -- Property-environmental spatial relationships (using actual columns from stage 3)
            COALESCE(b.property_bnbo_total_m2, 0) as property_bnbo_total_m2,
            COALESCE(b.property_bnbo_count, 0) as property_bnbo_count,
            COALESCE(b.property_bnbo_owners, NULL) as property_bnbo_owners,
            COALESCE(b.property_bnbo_breakdown, '{}') as property_bnbo_breakdown,
            COALESCE(b.property_bnbo_water_covered_m2, 0) as property_bnbo_water_covered_m2,
            COALESCE(b.property_bnbo_water_uncovered_m2, 0) as property_bnbo_water_uncovered_m2,
            
            COALESCE(w.property_wetland_total_m2, 0) as property_wetland_total_m2,
            COALESCE(w.property_wetland_count, 0) as property_wetland_count,
            COALESCE(w.property_wetland_owners, NULL) as property_wetland_owners,
            COALESCE(w.property_wetland_breakdown, '{}') as property_wetland_breakdown,
            COALESCE(w.property_wetland_water_covered_m2, 0) as property_wetland_water_covered_m2,
            COALESCE(w.property_wetland_water_uncovered_m2, 0) as property_wetland_water_uncovered_m2,
            
            -- Combined environmental-property metrics
            COALESCE(b.property_bnbo_count, 0) + COALESCE(w.property_wetland_count, 0) as total_properties_with_environmental_features,
            CASE 
                WHEN f.property_count > 0 
                THEN ((COALESCE(b.property_bnbo_total_m2, 0) + COALESCE(w.property_wetland_total_m2, 0)) / 
                      f.total_property_intersection_area_m2) * 100
                ELSE 0 
            END as combined_property_environmental_coverage_pct,
            
            -- Environmental summary flags
            CASE 
                WHEN COALESCE(b.field_bnbo_total_m2, 0) > 0 OR COALESCE(w.field_wetland_total_m2, 0) > 0 
                THEN TRUE ELSE FALSE 
            END as has_environmental_features,
            
            CASE 
                WHEN COALESCE(b.property_bnbo_count, 0) > 0 OR COALESCE(w.property_wetland_count, 0) > 0 
                THEN TRUE ELSE FALSE 
            END as has_property_environmental_relationships
            
        FROM all_fields_with_properties f
        LEFT JOIN field_soil_summary s ON f.field_id = s.field_id 
            AND f.block_id = s.block_id 
            AND f.cvr_number = s.cvr_number 
            AND f.year = s.year
        LEFT JOIN final_bnbo_analysis b ON f.field_id = b.field_id 
            AND f.block_id = b.block_id 
            AND f.cvr_number = b.cvr_number 
            AND f.year = b.year
        LEFT JOIN final_wetland_analysis w ON f.field_id = w.field_id 
            AND f.block_id = w.block_id 
            AND f.cvr_number = w.cvr_number 
            AND f.year = w.year
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
                COUNT(CASE WHEN field_bnbo_total_m2 > 0 THEN 1 END) as fields_with_bnbo,
                COUNT(CASE WHEN field_wetland_total_m2 > 0 THEN 1 END) as fields_with_wetlands,
                COUNT(CASE WHEN property_count > 0 THEN 1 END) as fields_with_properties,
                COUNT(CASE WHEN soil_type_count > 0 THEN 1 END) as fields_with_soil_data,
                COUNT(CASE WHEN has_environmental_features THEN 1 END) as fields_with_environmental_features,
                COUNT(CASE WHEN has_property_environmental_relationships THEN 1 END) as fields_with_property_env_relationships,
                COUNT(CASE WHEN property_bnbo_count > 0 THEN 1 END) as fields_with_bnbo_property_relationships,
                COUNT(CASE WHEN property_wetland_count > 0 THEN 1 END) as fields_with_wetland_property_relationships,
                
                -- Average coverages
                AVG(field_bnbo_coverage_pct) as avg_field_bnbo_pct,
                AVG(field_wetland_coverage_pct) as avg_field_wetland_pct,
                AVG(combined_property_environmental_coverage_pct) as avg_property_environmental_pct,
                AVG(property_count) as avg_properties_per_field,
                AVG(CASE WHEN soil_type_count > 0 THEN dominant_soil_coverage_pct END) as avg_dominant_soil_coverage_pct,
                AVG(CASE WHEN soil_type_count > 0 THEN total_soil_coverage_pct END) as avg_total_soil_coverage_pct,
                AVG(CASE WHEN soil_type_count > 0 THEN soil_type_count END) as avg_soil_types_per_field,
                
                -- Water project coverages
                AVG(CASE WHEN field_bnbo_total_m2 > 0 THEN field_bnbo_water_covered_pct END) as avg_bnbo_water_coverage,
                AVG(CASE WHEN field_wetland_total_m2 > 0 THEN field_wetland_water_covered_pct END) as avg_wetland_water_coverage,
                AVG(CASE WHEN property_wetland_count > 0 THEN 
                    (property_wetland_water_covered_m2 / NULLIF(property_wetland_total_m2, 0)) * 100 
                END) as avg_property_wetland_water_coverage,
                
                -- Property-environmental spatial relationships
                SUM(property_bnbo_count) as total_bnbo_property_relationships,
                SUM(property_wetland_count) as total_wetland_property_relationships,
                SUM(total_properties_with_environmental_features) as total_environmental_property_relationships,
                
                -- Total areas
                SUM(field_area_m2) / 1000000 as total_field_area_km2,
                SUM(field_bnbo_total_m2) / 1000000 as total_bnbo_area_km2,
                SUM(field_wetland_total_m2) / 1000000 as total_wetland_area_km2,
                SUM(field_bnbo_water_covered_m2) / 1000000 as total_bnbo_covered_km2,
                SUM(field_wetland_water_covered_m2) / 1000000 as total_wetland_covered_km2,
                
                -- Property-level wetland water coverage totals
                SUM(property_wetland_water_covered_m2) / 1000000 as total_property_wetland_covered_km2,
                SUM(property_wetland_water_uncovered_m2) / 1000000 as total_property_wetland_uncovered_km2,
                SUM(CASE WHEN property_wetland_water_covered_m2 > 0 THEN property_wetland_count ELSE 0 END) as total_properties_with_covered_wetlands,
                SUM(CASE WHEN property_wetland_water_uncovered_m2 > 0 THEN property_wetland_count ELSE 0 END) as total_properties_with_uncovered_wetlands
            FROM field_area_analysis_final
        """).fetchone()

        (
            total_fields,
            fields_with_bnbo,
            fields_with_wetlands,
            fields_with_props,
            fields_with_soil_data,
            fields_with_env_features,
            fields_with_prop_env_relationships,
            fields_with_bnbo_prop_relationships,
            fields_with_wetland_prop_relationships,
            avg_bnbo_pct,
            avg_wetland_pct,
            avg_prop_env_pct,
            avg_props_per_field,
            avg_dominant_soil_pct,
            avg_total_soil_pct,
            avg_soil_types_per_field,
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
                    WHEN field_bnbo_total_m2 > 0 AND field_wetland_total_m2 > 0 THEN 'Both BNBO and Wetlands'
                    WHEN field_bnbo_total_m2 > 0 THEN 'BNBO Only'
                    WHEN field_wetland_total_m2 > 0 THEN 'Wetlands Only'
                    ELSE 'No Environmental Features'
                END as environmental_category,
                COUNT(*) as field_count,
                AVG(property_count) as avg_properties,
                AVG(field_bnbo_coverage_pct + field_wetland_coverage_pct) as avg_total_env_coverage,
                AVG(combined_property_environmental_coverage_pct) as avg_property_env_coverage,
                SUM(property_bnbo_count + property_wetland_count) as total_property_relationships
            FROM field_area_analysis_final
            GROUP BY 
                CASE 
                    WHEN field_bnbo_total_m2 > 0 AND field_wetland_total_m2 > 0 THEN 'Both BNBO and Wetlands'
                    WHEN field_bnbo_total_m2 > 0 THEN 'BNBO Only'
                    WHEN field_wetland_total_m2 > 0 THEN 'Wetlands Only'
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
        self.log.info(
            f"   Fields with soil data: {fields_with_soil_data:,} ({(fields_with_soil_data / total_fields) * 100:.1f}%)"
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
        self.log.info(f"     Dominant soil type coverage: {avg_dominant_soil_pct:.1f}%")
        self.log.info(f"     Total soil coverage: {avg_total_soil_pct:.1f}%")
        self.log.info(f"     Soil types per field: {avg_soil_types_per_field:.1f}")

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
            "fields_with_soil_data": fields_with_soil_data,
            "fields_with_environmental_features": fields_with_env_features,
            "fields_with_property_env_relationships": fields_with_prop_env_relationships,
            "avg_bnbo_pct": avg_bnbo_pct,
            "avg_wetland_pct": avg_wetland_pct,
            "avg_property_environmental_pct": avg_prop_env_pct,
            "avg_dominant_soil_coverage_pct": avg_dominant_soil_pct,
            "avg_total_soil_coverage_pct": avg_total_soil_pct,
            "avg_soil_types_per_field": avg_soil_types_per_field,
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
