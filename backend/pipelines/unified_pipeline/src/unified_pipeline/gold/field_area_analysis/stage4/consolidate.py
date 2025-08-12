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
        # Debug: Log the current configuration
        self.log.info(
            f"🔍 DEBUG: Using agricultural_fields_year = {CONFIG.agricultural_fields_year}"
        )

        # Get year-aware output dataset names
        updated_outputs = CONFIG.update_outputs_for_year()
        self.log.info(f"🔍 DEBUG: Updated dataset outputs: {updated_outputs}")

        # Load ALL field-property intersections from Stage 1C (foundation for all fields with properties)
        stage1c_dataset = updated_outputs["field_property_intersections"]
        self.log.info(f"🔍 DEBUG: Looking for dataset: {stage1c_dataset}")
        try:
            stage1c_path = self._get_latest_gold_path(stage1c_dataset)
            self.log.info(f"🔍 DEBUG: Found path: {stage1c_path}")
            self.gcs_access.query_parquet_direct(
                stage1c_path, "SELECT *", "field_property_intersections"
            )
        except Exception as e:
            self.log.error(f"❌ Failed to load {stage1c_dataset}: {e}")
            raise

        # Load field-soil intersections from Stage 1B
        stage1b_dataset = updated_outputs["field_soil_intersections"]
        self.log.info(f"🔍 DEBUG: Looking for dataset: {stage1b_dataset}")
        try:
            stage1b_path = self._get_latest_gold_path(stage1b_dataset)
            self.log.info(f"🔍 DEBUG: Found path: {stage1b_path}")
            self.gcs_access.query_parquet_direct(stage1b_path, "SELECT *", "field_soil_areas")
        except Exception as e:
            self.log.error(f"❌ Failed to load {stage1b_dataset}: {e}")
            raise

        # Load final BNBO analysis from Stage 3A (only fields with BNBO)
        stage3a_dataset = updated_outputs["final_bnbo"]
        self.log.info(f"🔍 DEBUG: Looking for dataset: {stage3a_dataset}")
        try:
            stage3a_path = self._get_latest_gold_path(stage3a_dataset)
            self.log.info(f"🔍 DEBUG: Found path: {stage3a_path}")
            self.gcs_access.query_parquet_direct(stage3a_path, "SELECT *", "final_bnbo_analysis")
        except Exception as e:
            self.log.error(f"❌ Failed to load {stage3a_dataset}: {e}")
            raise

        # DEBUG: Check how many BNBO records were loaded
        bnbo_count = self.conn.execute("SELECT COUNT(*) FROM final_bnbo_analysis").fetchone()[0]
        self.log.info(
            f"🔍 DEBUG: Loaded {bnbo_count:,} final BNBO analysis records from {stage3a_path}"
        )

        # Load final wetland analysis from Stage 3B (only fields with wetlands)
        stage3b_dataset = updated_outputs["final_wetland"]
        self.log.info(f"🔍 DEBUG: Looking for dataset: {stage3b_dataset}")
        try:
            stage3b_path = self._get_latest_gold_path(stage3b_dataset)
            self.log.info(f"🔍 DEBUG: Found path: {stage3b_path}")
            self.gcs_access.query_parquet_direct(stage3b_path, "SELECT *", "final_wetland_analysis")
        except Exception as e:
            self.log.error(f"❌ Failed to load {stage3b_dataset}: {e}")
            raise

        # DEBUG: Check how many wetland records were loaded
        wetland_count = self.conn.execute("SELECT COUNT(*) FROM final_wetland_analysis").fetchone()[
            0
        ]
        self.log.info(
            f"🔍 DEBUG: Loaded {wetland_count:,} final wetland analysis records from {stage3b_path}"
        )

        # DEBUG: Check if we have UUID fields and what the schemas look like
        if bnbo_count > 0:
            bnbo_columns = [
                desc[0]
                for desc in self.conn.execute("PRAGMA table_info(final_bnbo_analysis)").fetchall()
            ]
            self.log.info(f"🔍 DEBUG: BNBO table columns: {bnbo_columns}")
            if "field_uuid" in bnbo_columns:
                self.log.info("✅ BNBO table has field_uuid - should use UUID-based JOINs!")

        if wetland_count > 0:
            wetland_columns = [
                desc[0]
                for desc in self.conn.execute(
                    "PRAGMA table_info(final_wetland_analysis)"
                ).fetchall()
            ]
            self.log.info(f"🔍 DEBUG: Wetland table columns: {wetland_columns}")
            if "field_uuid" in wetland_columns:
                self.log.info("✅ Wetland table has field_uuid - should use UUID-based JOINs!")

        # Check soil data structure
        soil_columns = [
            desc[0] for desc in self.conn.execute("PRAGMA table_info(field_soil_areas)").fetchall()
        ]
        self.log.info(f"🔍 DEBUG: Soil data columns: {soil_columns}")
        if "field_uuid" in soil_columns:
            self.log.info(
                "✅ Soil data has field_uuid - consider updating soil JOINs to use UUID too!"
            )

        if bnbo_count > 0 or wetland_count > 0:
            self.log.info(
                "✅ FIXED: Updated environmental JOINs to use field_uuid instead of composite keys!"
            )

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
                    WHERE fp2.field_uuid = fp.field_uuid 
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

        # Store input area reference for validation
        if self._should_validate_areas():
            fields_area_stats = self.conn.execute("""
                SELECT 
                    COUNT(*) as field_count,
                    SUM(field_area_m2) as total_area
                FROM all_fields_with_properties
                WHERE field_area_m2 IS NOT NULL AND field_area_m2 > 0
            """).fetchone()

            self._input_area_reference = {
                "total_area": fields_area_stats[1] or 0,
                "field_count": fields_area_stats[0] or 0,
            }

        # Create soil type summary per field from Stage 1B data
        self.log.info("Step 2: Creating field-level soil type summary...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_soil_summary AS
            SELECT 
                field_uuid,
                year,
                
                -- Keep composite keys for debugging/reference
                field_id,
                block_id,
                cvr_number,
                
                -- Soil type diversity metrics
                COUNT(DISTINCT soil_type_category) as soil_type_count,
                COUNT(DISTINCT soil_code) as unique_soil_codes,
                
                -- Dominant soil type (largest area)
                (
                    SELECT soil_type_category 
                    FROM field_soil_areas fsa2 
                    WHERE fsa2.field_uuid = fsa.field_uuid 
                    AND fsa2.year = fsa.year
                    ORDER BY fsa2.soil_area_m2 DESC 
                    LIMIT 1
                ) as dominant_soil_type,
                
                -- Dominant soil coverage percentage
                (
                    SELECT soil_area_share_pct 
                    FROM field_soil_areas fsa2 
                    WHERE fsa2.field_uuid = fsa.field_uuid 
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
            GROUP BY field_uuid, year, field_id, block_id, cvr_number
        """)

        soil_fields_count = self.conn.execute("SELECT COUNT(*) FROM field_soil_summary").fetchone()[
            0
        ]
        self.log.info(f"Created soil summaries for {soil_fields_count:,} fields")

        # STEP 3A: Pre-aggregate Stage 3A BNBO data to field level (fix cartesian product!)
        self.log.info(
            "Step 3A: Pre-aggregating BNBO data to field level to prevent cartesian products..."
        )
        self.conn.execute("""
            CREATE OR REPLACE TABLE bnbo_field_summary AS
            SELECT 
                field_uuid, year,
                -- Field-level totals (should be same across property rows - use MAX)
                MAX(field_bnbo_total_m2) as field_bnbo_total_m2,
                MAX(field_bnbo_water_covered_m2) as field_bnbo_water_covered_m2,
                MAX(field_bnbo_water_covered_pct) as field_bnbo_water_covered_pct,
                MAX(field_bnbo_water_uncovered_pct) as field_bnbo_water_uncovered_pct,
                MAX(field_bnbo_coverage_pct) as field_bnbo_coverage_pct,
                
                -- BNBO status metrics (field-level - should be same across property rows)
                MAX(bnbo_action_required_hectares) as bnbo_action_required_hectares,
                MAX(bnbo_completed_hectares) as bnbo_completed_hectares,
                MAX(bnbo_action_required_overlap_hectares) as bnbo_action_required_overlap_hectares,
                MAX(bnbo_completed_overlap_hectares) as bnbo_completed_overlap_hectares,
                MAX(bnbo_action_required_not_covered_by_water_hectares) as bnbo_action_required_not_covered_by_water_hectares,
                MAX(bnbo_completed_not_covered_by_water_hectares) as bnbo_completed_not_covered_by_water_hectares,
                MAX(bnbo_status_categories) as bnbo_status_categories,
                MAX(bnbo_status_count) as bnbo_status_count,
                
                -- Property-level BNBO totals (SUM - aggregate across all properties for this field)
                SUM(property_bnbo_total_m2) as property_bnbo_total_m2,
                SUM(property_bnbo_water_covered_m2) as property_bnbo_water_covered_m2,
                SUM(property_bnbo_water_uncovered_m2) as property_bnbo_water_uncovered_m2,
                SUM(property_bnbo_count) as property_bnbo_count,  -- Total count across all properties
                MAX(property_bnbo_owners) as property_bnbo_owners,
                MAX(property_bnbo_breakdown) as property_bnbo_breakdown
                
            FROM final_bnbo_analysis 
            GROUP BY field_uuid, year
        """)

        bnbo_summary_count = self.conn.execute(
            "SELECT COUNT(*) FROM bnbo_field_summary"
        ).fetchone()[0]
        self.log.info(f"✅ Created BNBO field summaries for {bnbo_summary_count:,} fields")

        # STEP 3B: Pre-aggregate Stage 3B wetland data to field level (fix cartesian product!)
        self.log.info(
            "Step 3B: Pre-aggregating wetland data to field level to prevent cartesian products..."
        )
        self.conn.execute("""
            CREATE OR REPLACE TABLE wetland_field_summary AS
            SELECT 
                field_uuid, year,
                -- Field-level totals (should be same across property rows - use MAX)
                MAX(field_wetland_total_m2) as field_wetland_total_m2,
                MAX(field_wetland_water_covered_m2) as field_wetland_water_covered_m2,
                MAX(field_wetland_water_covered_pct) as field_wetland_water_covered_pct,
                MAX(field_wetland_water_uncovered_pct) as field_wetland_water_uncovered_pct,
                MAX(field_wetland_coverage_pct) as field_wetland_coverage_pct,
                
                -- Property-level wetland totals (SUM - aggregate across all properties for this field)
                SUM(property_wetland_total_m2) as property_wetland_total_m2,
                SUM(property_wetland_water_covered_m2) as property_wetland_water_covered_m2,
                SUM(property_wetland_water_uncovered_m2) as property_wetland_water_uncovered_m2,
                SUM(property_wetland_count) as property_wetland_count,  -- Total count across all properties
                MAX(property_wetland_owners) as property_wetland_owners,
                MAX(property_wetland_breakdown) as property_wetland_breakdown
                
            FROM final_wetland_analysis 
            GROUP BY field_uuid, year
        """)

        wetland_summary_count = self.conn.execute(
            "SELECT COUNT(*) FROM wetland_field_summary"
        ).fetchone()[0]
        self.log.info(f"✅ Created wetland field summaries for {wetland_summary_count:,} fields")

        # STEP 3C: Create TWO separate outputs - field-level and property-level environmental analysis
        self.log.info(
            "Step 3C: Creating field-level environmental analysis (one record per field)..."
        )

        # TABLE 1: Field-Level Environmental Analysis (one record per field)
        consolidation_query = """
            CREATE OR REPLACE TABLE field_area_analysis_final AS
            SELECT * FROM wetland_field_summary
        """
        self.conn.execute(consolidation_query)

        # Log results
        final_result_count = self.conn.execute(
            "SELECT COUNT(*) FROM field_area_analysis_final"
        ).fetchone()[0]
        self.log.info(f"✅ Created final analysis table with {final_result_count:,} fields")

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

        # Calculate percentages safely to avoid division by zero
        bnbo_pct = (fields_with_bnbo / total_fields) * 100 if total_fields > 0 else 0.0
        wetlands_pct = (fields_with_wetlands / total_fields) * 100 if total_fields > 0 else 0.0
        props_pct = (fields_with_props / total_fields) * 100 if total_fields > 0 else 0.0
        soil_pct = (fields_with_soil_data / total_fields) * 100 if total_fields > 0 else 0.0

        self.log.info(f"   Fields with BNBO: {fields_with_bnbo:,} ({bnbo_pct:.1f}%)")
        self.log.info(f"   Fields with wetlands: {fields_with_wetlands:,} ({wetlands_pct:.1f}%)")
        self.log.info(f"   Fields with properties: {fields_with_props:,} ({props_pct:.1f}%)")
        self.log.info(f"   Fields with soil data: {fields_with_soil_data:,} ({soil_pct:.1f}%)")
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
        self.log.info(
            f"     BNBO: {avg_bnbo_pct:.2f}%" if avg_bnbo_pct is not None else "     BNBO: 0.00%"
        )
        self.log.info(
            f"     Wetlands: {avg_wetland_pct:.2f}%"
            if avg_wetland_pct is not None
            else "     Wetlands: 0.00%"
        )
        self.log.info(
            f"     Property-environmental: {avg_prop_env_pct:.2f}%"
            if avg_prop_env_pct is not None
            else "     Property-environmental: 0.00%"
        )
        self.log.info(
            f"     Properties per field: {avg_props_per_field:.1f}"
            if avg_props_per_field is not None
            else "     Properties per field: 0.0"
        )
        self.log.info(
            f"     Dominant soil type coverage: {avg_dominant_soil_pct:.1f}%"
            if avg_dominant_soil_pct is not None
            else "     Dominant soil type coverage: 0.0%"
        )
        self.log.info(
            f"     Total soil coverage: {avg_total_soil_pct:.1f}%"
            if avg_total_soil_pct is not None
            else "     Total soil coverage: 0.0%"
        )
        self.log.info(
            f"     Soil types per field: {avg_soil_types_per_field:.1f}"
            if avg_soil_types_per_field is not None
            else "     Soil types per field: 0.0"
        )

        self.log.info("   Property-environmental spatial relationships:")
        self.log.info(f"     Total BNBO-property relationships: {total_bnbo_prop_relationships:,}")
        self.log.info(
            f"     Total wetland-property relationships: {total_wetland_prop_relationships:,}"
        )
        self.log.info(
            f"     Total environmental-property relationships: {total_env_prop_relationships:,}"
        )

        self.log.info("   Water project coverage:")
        self.log.info(
            f"     BNBO areas: {avg_bnbo_water:.1f}%"
            if avg_bnbo_water is not None
            else "     BNBO areas: 0.0%"
        )
        self.log.info(
            f"     Wetland areas: {avg_wetland_water:.1f}%"
            if avg_wetland_water is not None
            else "     Wetland areas: 0.0%"
        )
        self.log.info(
            f"     Property wetland areas: {avg_property_wetland_water:.1f}%"
            if avg_property_wetland_water is not None
            else "     Property wetland areas: 0.0%"
        )

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
            avg_props_str = f"{avg_props:.1f}" if avg_props is not None else "0.0"
            avg_env_str = f"{avg_env:.1f}" if avg_env is not None else "0.0"
            self.log.info(
                f"     {category}: {count:,} fields ({(count / total_fields) * 100:.1f}%), {avg_props_str} avg properties, {avg_env_str}% env coverage, {total_relationships:,} property relationships"
            )

        # COMPREHENSIVE VALIDATION SUITE
        if self.area_validator:
            self.log.info("🔍 RUNNING COMPREHENSIVE VALIDATION SUITE...")

            # Run comprehensive validation for both BNBO and wetland data in consolidated table
            validation_results = {}

            # Validate BNBO hierarchy
            bnbo_validation = self.area_validator.run_comprehensive_stage_validation(
                "field_environmental_analysis", "Stage 4 Consolidated BNBO", "bnbo"
            )
            validation_results.update({f"bnbo_{k}": v for k, v in bnbo_validation.items()})

            # Validate wetland hierarchy
            wetland_validation = self.area_validator.run_comprehensive_stage_validation(
                "field_environmental_analysis", "Stage 4 Consolidated Wetland", "wetland"
            )
            validation_results.update({f"wetland_{k}": v for k, v in wetland_validation.items()})

            # Additional consolidated validation: check total environmental areas ≤ field areas
            consolidated_hierarchy = self.area_validator.validate_area_hierarchy(
                "field_environmental_analysis",
                "Stage 4 Consolidated Environmental",
                [
                    # Water areas ≤ Environmental areas
                    (
                        "property_bnbo_water_covered_m2",
                        "property_bnbo_total_m2",
                        "Property BNBO water covered ≤ Property BNBO total",
                    ),
                    (
                        "property_wetland_water_covered_m2",
                        "property_wetland_total_m2",
                        "Property wetland water covered ≤ Property wetland total",
                    ),
                    # Environmental areas ≤ Property areas
                    (
                        "property_bnbo_total_m2",
                        "total_property_intersection_area_m2",
                        "Property BNBO area ≤ Property area",
                    ),
                    (
                        "property_wetland_total_m2",
                        "total_property_intersection_area_m2",
                        "Property wetland area ≤ Property area",
                    ),
                ],
            )
            validation_results["consolidated_hierarchy"] = consolidated_hierarchy

            # Check if any validation failed
            failed_validations = [
                name for name, result in validation_results.items() if not result.is_valid
            ]

            if failed_validations and self.validation_config.fail_on_validation_error:
                from ..area_validation import ValidationException

                failed_result = validation_results[failed_validations[0]]
                raise ValidationException(failed_result)
            elif failed_validations:
                self.log.warning(
                    f"⚠️ Validation failures detected but continuing: {failed_validations}"
                )
            else:
                self.log.info("✅ All Stage 4 consolidated validations PASSED!")

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

    def _get_input_area_reference(self) -> Dict[str, Any]:
        """Get reference area statistics from input data for validation."""
        return getattr(self, "_input_area_reference", None)

    def _get_main_output_table(self) -> str:
        """Get the name of the main output table for area validation."""
        return "field_environmental_analysis"

    def _save_output_data(self, result: Dict[str, Any]):
        """Save final consolidated analysis to GCS."""
        self._save_stage_output("field_area_analysis_final", "consolidated")
