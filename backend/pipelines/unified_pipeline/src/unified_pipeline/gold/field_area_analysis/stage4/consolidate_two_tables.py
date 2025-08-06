"""
Stage 4: Field Area Analysis Consolidation - Redesigned Architecture

Performs ALL area calculations from geometric data produced by redesigned Stages 2 & 3.
Creates two separate output tables following the architectural redesign:

1. Field Environmental Analysis (field_environmental_analysis_fields) - One record per field
2. Property Environmental Analysis - One record per field×property

Key Principles:
- ALL ST_Area_Spheroid() calculations happen here (removed from Stages 1-3)
- Uses clean geometric data from redesigned pipeline
- Single place for all area computations
- Two separate output tables eliminate record explosion issues
"""

from typing import Any, Dict

from ..base import FieldAnalysisStageBase, FieldAnalysisStageConfig
from ..config import CONFIG


class ConsolidateResultsTwoTables(FieldAnalysisStageBase):
    """
    Stage 4: Consolidate all area calculations from geometric pipeline data.
    
    Redesigned to:
    - Process geometric outputs from redesigned Stages 2A, 2B, 3A, 3B
    - Perform ALL ST_Area_Spheroid() calculations in single place
    - Create two separate output tables per architectural design
    """

    def __init__(self, config: FieldAnalysisStageConfig = None):
        if config is None:
            config = FieldAnalysisStageConfig()
        super().__init__(config, "Stage 4: Consolidate Results - Redesigned Architecture")

    def _load_input_data(self):
        """Load geometric data from redesigned Stages 2A, 2B, 3A, 3B."""
        updated_outputs = CONFIG.update_outputs_for_year()
        
        # Load agricultural fields (foundation data)
        self._load_silver_dataset(CONFIG.get_agricultural_fields_dataset(), "agricultural_fields")
        
        # Load Stage 1C field-property intersections (foundation data)
        stage1c_dataset = updated_outputs["field_property_intersections"]
        stage1c_path = self._get_latest_gold_path(stage1c_dataset)
        self.gcs_access.query_parquet_direct(
            stage1c_path, "SELECT *", "field_property_intersections"
        )
        self.log.info(f"✅ Loaded field_property_intersections from {stage1c_dataset}")

        # Load Stage 1D field-soil intersections for soil analysis
        stage1d_dataset = updated_outputs["field_soil_intersections"]
        stage1d_path = self._get_latest_gold_path(stage1d_dataset)
        self.gcs_access.query_parquet_direct(
            stage1d_path, "SELECT *", "field_soil_intersections"
        )
        self.log.info(f"✅ Loaded field_soil_intersections from {stage1d_dataset}")

        # Load Stage 2A outputs: field × BNBO geometries
        stage2a_bnbo_dataset = updated_outputs["field_bnbo_intersections"]
        stage2a_bnbo_path = self._get_latest_gold_path(stage2a_bnbo_dataset)
        self.gcs_access.query_parquet_direct(
            stage2a_bnbo_path, "SELECT *", "field_bnbo_intersections"
        )
        
        stage2a_water_dataset = updated_outputs["field_bnbo_water_intersections"]
        stage2a_water_path = self._get_latest_gold_path(stage2a_water_dataset)
        self.gcs_access.query_parquet_direct(
            stage2a_water_path, "SELECT *", "field_bnbo_water_intersections"
        )
        self.log.info("✅ Loaded Stage 2A BNBO geometric outputs")

        # Load Stage 2B outputs: field × wetland geometries
        stage2b_wetland_dataset = updated_outputs["field_wetland_intersections"]
        stage2b_wetland_path = self._get_latest_gold_path(stage2b_wetland_dataset)
        self.gcs_access.query_parquet_direct(
            stage2b_wetland_path, "SELECT *", "field_wetland_intersections"
        )
        
        stage2b_water_dataset = updated_outputs["field_wetland_water_intersections"]
        stage2b_water_path = self._get_latest_gold_path(stage2b_water_dataset)
        self.gcs_access.query_parquet_direct(
            stage2b_water_path, "SELECT *", "field_wetland_water_intersections"
        )
        self.log.info("✅ Loaded Stage 2B wetland geometric outputs")

        # Load Stage 3A outputs: property × BNBO geometries
        stage3a_bnbo_dataset = updated_outputs["property_bnbo_intersections"]
        stage3a_bnbo_path = self._get_latest_gold_path(stage3a_bnbo_dataset)
        self.gcs_access.query_parquet_direct(
            stage3a_bnbo_path, "SELECT *", "property_bnbo_intersections"
        )
        
        stage3a_water_dataset = updated_outputs["property_bnbo_water_intersections"]
        stage3a_water_path = self._get_latest_gold_path(stage3a_water_dataset)
        self.gcs_access.query_parquet_direct(
            stage3a_water_path, "SELECT *", "property_bnbo_water_intersections"
        )
        self.log.info("✅ Loaded Stage 3A property-BNBO geometric outputs")

        # Load Stage 3B outputs: property × wetland geometries
        stage3b_wetland_dataset = updated_outputs["property_wetland_intersections"]
        stage3b_wetland_path = self._get_latest_gold_path(stage3b_wetland_dataset)
        self.gcs_access.query_parquet_direct(
            stage3b_wetland_path, "SELECT *", "property_wetland_intersections"
        )
        
        stage3b_water_dataset = updated_outputs["property_wetland_water_intersections"]
        stage3b_water_path = self._get_latest_gold_path(stage3b_water_dataset)
        self.gcs_access.query_parquet_direct(
            stage3b_water_path, "SELECT *", "property_wetland_water_intersections"
        )
        self.log.info("✅ Loaded Stage 3B property-wetland geometric outputs")

        self.log.info(
            "🚀 REDESIGNED STAGE 4: All geometric data loaded for centralized calculations"
        )

    async def _execute_stage_processing(self) -> Dict[str, Any]:
        """Execute redesigned Stage 4 consolidation with centralized area calculations."""
        
        self.log.info(
            "🎯 REDESIGNED STAGE 4: Performing ALL area calculations from geometric data"
        )
        
        # Create the two output tables with all area calculations
        self._create_field_level_analysis()
        self._create_property_level_analysis()
        
        # Validate and return statistics
        return self._get_consolidation_statistics()

    def _create_field_level_analysis(self):
        """Create field-level environmental analysis with ALL area calculations."""
        
        self.log.info(
            "📦 Creating field_environmental_analysis_fields with centralized area calculations"
        )
        
        # FIXED: Pre-aggregate each intersection type separately to avoid Cartesian product
        self.log.info("🔧 PRE-AGGREGATING intersection data to prevent JOIN explosion...")
        
        # Pre-aggregate BNBO intersections by field
        self.conn.execute("""
            CREATE OR REPLACE TABLE bnbo_field_aggregates AS
            SELECT 
                field_uuid,
                SUM(ST_Area_Spheroid(field_bnbo_geometry)) as field_bnbo_total_m2,
                COUNT(DISTINCT status_category) as bnbo_status_count,
                STRING_AGG(DISTINCT status_category, ', ' ORDER BY status_category) 
                    as bnbo_status_categories,
                SUM(CASE WHEN status_category = 'Action Required' 
                    THEN ST_Area_Spheroid(field_bnbo_geometry) ELSE 0 END) / 10000.0 
                    as bnbo_action_required_hectares,
                SUM(CASE WHEN status_category = 'Completed' 
                    THEN ST_Area_Spheroid(field_bnbo_geometry) ELSE 0 END) / 10000.0 
                    as bnbo_completed_hectares
            FROM field_bnbo_intersections
            GROUP BY field_uuid
        """)
        
        # Pre-aggregate BNBO water intersections by field
        self.conn.execute("""
            CREATE OR REPLACE TABLE bnbo_water_field_aggregates AS
            SELECT 
                field_uuid,
                SUM(ST_Area_Spheroid(field_bnbo_water_geometry)) 
                    as field_bnbo_water_covered_m2
            FROM field_bnbo_water_intersections
            GROUP BY field_uuid
        """)
        
        # Pre-aggregate wetland intersections by field
        self.conn.execute("""
            CREATE OR REPLACE TABLE wetland_field_aggregates AS
            SELECT 
                field_uuid,
                SUM(ST_Area_Spheroid(field_wetland_geometry)) as field_wetland_total_m2
            FROM field_wetland_intersections
            GROUP BY field_uuid
        """)
        
        # Pre-aggregate wetland water intersections by field
        self.conn.execute("""
            CREATE OR REPLACE TABLE wetland_water_field_aggregates AS
            SELECT 
                field_uuid,
                SUM(ST_Area_Spheroid(field_wetland_water_geometry)) 
                    as field_wetland_water_covered_m2
            FROM field_wetland_water_intersections
            GROUP BY field_uuid
        """)
        
        self.log.info("✅ Pre-aggregation completed - no more Cartesian products!")
        
        # Create the final table using pre-aggregated data (NO CARTESIAN PRODUCT)
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_environmental_analysis_fields AS
            SELECT 
                f.field_uuid,
                f.field_id,
                f.block_id,
                f.cvr_number,
                f.year,
                ST_Area_Spheroid(f.geometry) as field_area_m2,
                
                -- BNBO Analysis (using pre-aggregated data)
                COALESCE(ba.field_bnbo_total_m2, 0) as field_bnbo_total_m2,
                COALESCE(bwa.field_bnbo_water_covered_m2, 0) as field_bnbo_water_covered_m2,
                CASE 
                    WHEN COALESCE(ba.field_bnbo_total_m2, 0) > 0 
                    THEN (ba.field_bnbo_total_m2 / ST_Area_Spheroid(f.geometry)) * 100.0
                    ELSE 0 
                END as field_bnbo_coverage_pct,
                CASE 
                    WHEN COALESCE(ba.field_bnbo_total_m2, 0) > 0 
                    THEN (COALESCE(bwa.field_bnbo_water_covered_m2, 0) / 
                          ba.field_bnbo_total_m2) * 100.0
                    ELSE 0 
                END as field_bnbo_water_coverage_pct,
                
                -- BNBO Status Analysis (from pre-aggregated data)
                COALESCE(ba.bnbo_status_count, 0) as bnbo_status_count,
                ba.bnbo_status_categories,
                COALESCE(ba.bnbo_action_required_hectares, 0) as bnbo_action_required_hectares,
                COALESCE(ba.bnbo_completed_hectares, 0) as bnbo_completed_hectares,
                
                -- Wetland Analysis (using pre-aggregated data)
                COALESCE(wa.field_wetland_total_m2, 0) as field_wetland_total_m2,
                COALESCE(wwa.field_wetland_water_covered_m2, 0) as field_wetland_water_covered_m2,
                CASE 
                    WHEN COALESCE(wa.field_wetland_total_m2, 0) > 0 
                    THEN (wa.field_wetland_total_m2 / ST_Area_Spheroid(f.geometry)) * 100.0
                    ELSE 0 
                END as field_wetland_coverage_pct,
                CASE 
                    WHEN COALESCE(wa.field_wetland_total_m2, 0) > 0 
                    THEN (COALESCE(wwa.field_wetland_water_covered_m2, 0) / 
                          wa.field_wetland_total_m2) * 100.0
                    ELSE 0 
                END as field_wetland_water_coverage_pct
                
                -- Soil Analysis (from Stage 1D field_soil_intersections)
                -- TODO: Add soil calculations from field_soil_intersections geometries
                
            FROM agricultural_fields f
            LEFT JOIN bnbo_field_aggregates ba ON f.field_uuid = ba.field_uuid
            LEFT JOIN bnbo_water_field_aggregates bwa ON f.field_uuid = bwa.field_uuid
            LEFT JOIN wetland_field_aggregates wa ON f.field_uuid = wa.field_uuid
            LEFT JOIN wetland_water_field_aggregates wwa ON f.field_uuid = wwa.field_uuid
        """)
        
        field_count = self.conn.execute(
            "SELECT COUNT(*) FROM field_environmental_analysis_fields"
        ).fetchone()[0]
        self.log.info(f"✅ Created field_environmental_analysis_fields: {field_count:,} records")

    def _create_property_level_analysis(self):
        """Create property-level environmental analysis with ALL area calculations."""
        
        self.log.info(
            "📦 Creating field_environmental_analysis_properties with centralized area calculations"
        )
        
        # Create table with field×property combinations and environmental calculations
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_environmental_analysis_properties AS
            SELECT 
                fp.field_uuid,
                fp.bfe_number,
                fp.field_id,
                fp.block_id,
                fp.cvr_number,
                fp.year,
                SUM(ST_Area_Spheroid(fp.intersection_geometry)) 
                    as property_intersection_area_m2,
                
                -- Environmental within this field×property intersection
                COALESCE(SUM(ST_Area_Spheroid(pbi.property_bnbo_geometry)), 0) 
                    as property_bnbo_total_m2,
                COALESCE(SUM(ST_Area_Spheroid(pbwi.property_bnbo_water_geometry)), 0) 
                    as property_bnbo_water_covered_m2,
                COALESCE(SUM(ST_Area_Spheroid(pwi.property_wetland_geometry)), 0) 
                    as property_wetland_total_m2,
                COALESCE(SUM(ST_Area_Spheroid(pwwi.property_wetland_water_geometry)), 0) 
                    as property_wetland_water_covered_m2,
                
                -- BNBO Status Analysis (aggregated at property level)
                COUNT(DISTINCT pbi.status_category) as property_bnbo_status_count,
                STRING_AGG(DISTINCT pbi.status_category, ', ' ORDER BY pbi.status_category) 
                    as property_bnbo_status_categories,
                COALESCE(SUM(CASE WHEN pbi.status_category = 'Action Required' 
                    THEN ST_Area_Spheroid(pbi.property_bnbo_geometry) ELSE 0 END), 0) / 10000.0 
                    as property_bnbo_action_required_hectares,
                COALESCE(SUM(CASE WHEN pbi.status_category = 'Completed' 
                    THEN ST_Area_Spheroid(pbi.property_bnbo_geometry) ELSE 0 END), 0) / 10000.0 
                    as property_bnbo_completed_hectares
                
            FROM field_property_intersections fp
            LEFT JOIN property_bnbo_intersections pbi 
                ON fp.field_uuid = pbi.field_uuid AND fp.bfe_number = pbi.bfe_number
            LEFT JOIN property_bnbo_water_intersections pbwi 
                ON fp.field_uuid = pbwi.field_uuid AND fp.bfe_number = pbwi.bfe_number
            LEFT JOIN property_wetland_intersections pwi 
                ON fp.field_uuid = pwi.field_uuid AND fp.bfe_number = pwi.bfe_number
            LEFT JOIN property_wetland_water_intersections pwwi 
                ON fp.field_uuid = pwwi.field_uuid AND fp.bfe_number = pwwi.bfe_number
            GROUP BY fp.field_uuid, fp.bfe_number, fp.field_id, fp.block_id, 
                     fp.cvr_number, fp.year
        """)
        
        property_count = self.conn.execute(
            "SELECT COUNT(*) FROM field_environmental_analysis_properties"
        ).fetchone()[0]
        self.log.info(
            f"✅ Created field_environmental_analysis_properties: {property_count:,} records"
        )

    def _get_consolidation_statistics(self) -> Dict[str, Any]:
        """Get statistics for both output tables."""
        
        # Field-level statistics
        field_stats = self.conn.execute("""
            SELECT 
                COUNT(*) as total_fields,
                COUNT(CASE WHEN field_bnbo_total_m2 > 0 THEN 1 END) as fields_with_bnbo,
                COUNT(CASE WHEN field_wetland_total_m2 > 0 THEN 1 END) as fields_with_wetlands,
                AVG(field_area_m2) as avg_field_area_m2,
                SUM(field_bnbo_total_m2) as total_bnbo_area_m2,
                SUM(field_wetland_total_m2) as total_wetland_area_m2
            FROM field_environmental_analysis_fields
        """).fetchone()
        
        # Property-level statistics
        property_stats = self.conn.execute("""
            SELECT 
                COUNT(*) as total_property_combinations,
                COUNT(DISTINCT field_uuid) as fields_with_properties,
                COUNT(CASE WHEN property_bnbo_total_m2 > 0 THEN 1 END) as properties_with_bnbo,
                COUNT(CASE WHEN property_wetland_total_m2 > 0 THEN 1 END) 
                    as properties_with_wetlands,
                AVG(property_intersection_area_m2) as avg_property_area_m2,
                SUM(property_bnbo_total_m2) as total_property_bnbo_area_m2,
                SUM(property_wetland_total_m2) as total_property_wetland_area_m2
            FROM field_environmental_analysis_properties
            """).fetchone()
        
        stats = {
            "field_level": {
                "total_fields": field_stats[0],
                "fields_with_bnbo": field_stats[1],
                "fields_with_wetlands": field_stats[2],
                "avg_field_area_m2": field_stats[3],
                "total_bnbo_area_m2": field_stats[4],
                "total_wetland_area_m2": field_stats[5]
            },
            "property_level": {
                "total_property_combinations": property_stats[0],
                "fields_with_properties": property_stats[1],
                "properties_with_bnbo": property_stats[2],
                "properties_with_wetlands": property_stats[3],
                "avg_property_area_m2": property_stats[4],
                "total_property_bnbo_area_m2": property_stats[5],
                "total_property_wetland_area_m2": property_stats[6]
            }
        }
        
        self.log.info("📊 REDESIGNED STAGE 4 STATISTICS:")
        self.log.info(f"   Field-level analysis: {stats['field_level']['total_fields']:,} fields")
        property_count = stats['property_level']['total_property_combinations']
        self.log.info(
            f"   Property-level analysis: {property_count:,} field-property combinations"
        )
        self.log.info(f"   Fields with BNBO: {stats['field_level']['fields_with_bnbo']:,}")
        self.log.info(f"   Fields with wetlands: {stats['field_level']['fields_with_wetlands']:,}")
        self.log.info("🎉 ALL AREA CALCULATIONS CENTRALIZED IN STAGE 4!")
        
        return stats

    def _save_output_data(self, result: Dict[str, Any]):
        """Save both redesigned output tables to GCS."""
        
        self.log.info("Saving redesigned output tables to GCS...")
        
        # Save field-level table
        self._save_stage_output(
            "field_environmental_analysis_fields", "field_environmental_analysis_fields"
        )
        
        # Save property-level table  
        self._save_stage_output(
            "field_environmental_analysis_properties", "field_environmental_analysis_properties"
        )
        
        self.log.info("✅ Both redesigned output tables saved to GCS")
        
        # Perform comprehensive validations after saving
        self._validate_comprehensive_output()

    def _validate_comprehensive_output(self):
        """Comprehensive validation for Stage 4 redesigned outputs."""
        
        self.log.info("🔍 REDESIGNED STAGE 4: Running comprehensive validations...")
        
        # 1. RECORD COUNT VALIDATIONS
        self._validate_output_record_counts()
        
        # 2. AREA SANITY CHECKS  
        self._validate_area_calculations()
        
        # 3. DATA CONSISTENCY VALIDATIONS
        self._validate_output_data_consistency()
        
        self.log.info("✅ REDESIGNED STAGE 4: All comprehensive validations completed")

    def _validate_output_record_counts(self):
        """Validate output record counts and coverage."""
        self.log.info("🔍 Validating output record counts...")
        
        # Get field-level output counts
        field_count = self.conn.execute(
            "SELECT COUNT(*) FROM field_environmental_analysis_fields"
        ).fetchone()[0]
        field_unique = self.conn.execute(
            "SELECT COUNT(DISTINCT field_uuid) FROM field_environmental_analysis_fields"
        ).fetchone()[0]
        
        # Get property-level output counts
        property_count = self.conn.execute(
            "SELECT COUNT(*) FROM field_environmental_analysis_properties"
        ).fetchone()[0]
        property_unique_fields = self.conn.execute(
            "SELECT COUNT(DISTINCT field_uuid) FROM field_environmental_analysis_properties"
        ).fetchone()[0]
        combo_query = (
            "SELECT COUNT(DISTINCT field_uuid || '_' || bfe_number) "
            "FROM field_environmental_analysis_properties"
        )
        property_unique_combos = self.conn.execute(combo_query).fetchone()[0]
        
        # Validate field-level table (should be 1:1 with fields)
        if field_count == field_unique:
            self.log.info(f"✅ Field-level table: {field_count:,} records (exactly 1 per field)")
        else:
            duplicate_count = field_count - field_unique
            self.log.error(
                f"❌ CRITICAL: Field-level table has {duplicate_count:,} duplicate field records!"
            )
        
        # Validate property-level table (should be 1:1 with field×property combinations)
        if property_count == property_unique_combos:
            self.log.info(
                f"✅ Property-level table: {property_count:,} records "
                f"(exactly 1 per field×property combination)"
            )
            self.log.info(
                f"   Covers {property_unique_fields:,} fields with property intersections"
            )
        else:
            duplicate_combos = property_count - property_unique_combos
            self.log.error(
                f"❌ ANALYZING: Property table has {duplicate_combos:,} records for "
                f"{property_unique_combos:,} unique field×property pairs"
            )
            
            # Analyze the nature of these "duplicates"
            metadata_analysis = self.conn.execute("""
                SELECT 
                    COALESCE(SUM(CASE WHEN years > 1 THEN combo_count - 1 ELSE 0 END), 0) 
                        as multi_year_extras,
                    COALESCE(SUM(CASE WHEN cvrs > 1 THEN combo_count - 1 ELSE 0 END), 0) 
                        as multi_cvr_extras,
                    COALESCE(SUM(CASE WHEN blocks > 1 THEN combo_count - 1 ELSE 0 END), 0) 
                        as multi_block_extras,
                    COALESCE(SUM(CASE WHEN fields > 1 THEN combo_count - 1 ELSE 0 END), 0) 
                        as multi_field_extras
                FROM (
                    SELECT field_uuid, bfe_number, COUNT(*) as combo_count,
                           COUNT(DISTINCT year) as years,
                           COUNT(DISTINCT cvr_number) as cvrs, 
                           COUNT(DISTINCT block_id) as blocks,
                           COUNT(DISTINCT field_id) as fields
                    FROM field_environmental_analysis_properties
                    GROUP BY field_uuid, bfe_number
                    HAVING COUNT(*) > 1
                )
            """).fetchone()
            
            self.log.info(f"🔍 METADATA ANALYSIS of {duplicate_combos:,} extra records:")
            self.log.info(f"   Multi-year combinations: {metadata_analysis[0]:,}")
            self.log.info(f"   Multi-CVR combinations: {metadata_analysis[1]:,}")
            self.log.info(f"   Multi-block combinations: {metadata_analysis[2]:,}")
            self.log.info(f"   Multi-field combinations: {metadata_analysis[3]:,}")
            
            # This tells us WHY we have "duplicates" - likely legitimate temporal/admin variation

    def _validate_area_calculations(self):
        """Validate area calculations are reasonable."""
        self.log.info("🔍 Validating area calculations...")
        
        # Check for negative areas (should never happen)
        negative_field_areas = self.conn.execute("""
            SELECT COUNT(*) FROM field_environmental_analysis_fields 
            WHERE field_area_m2 < 0 OR field_bnbo_total_m2 < 0 OR field_wetland_total_m2 < 0
        """).fetchone()[0]
        
        negative_property_areas = self.conn.execute("""
            SELECT COUNT(*) FROM field_environmental_analysis_properties 
            WHERE property_intersection_area_m2 < 0 OR property_bnbo_total_m2 < 0 
               OR property_wetland_total_m2 < 0
        """).fetchone()[0]
        
        if negative_field_areas > 0:
            self.log.error(
                f"❌ CRITICAL: {negative_field_areas:,} field records have negative areas!"
            )
        else:
            self.log.info("✅ All field areas are non-negative")
            
        if negative_property_areas > 0:
            self.log.error(
                f"❌ CRITICAL: {negative_property_areas:,} property records have negative areas!"
            )
        else:
            self.log.info("✅ All property areas are non-negative")
        
        # Check for unreasonably large environmental areas (larger than field)
        oversized_field_env = self.conn.execute("""
            SELECT COUNT(*) FROM field_environmental_analysis_fields 
            WHERE field_bnbo_total_m2 > field_area_m2 * 1.01 
               OR field_wetland_total_m2 > field_area_m2 * 1.01
        """).fetchone()[0]
        
        if oversized_field_env > 0:
            self.log.warning(
                f"⚠️ Found {oversized_field_env:,} fields where environmental area > field area"
                f" (tolerance: 1%)"
            )
        else:
            self.log.info(
                "✅ All environmental areas are within reasonable bounds of field sizes"
            )
        
        # Summary statistics
        field_stats = self.conn.execute("""
            SELECT 
                AVG(field_area_m2) as avg_field_area,
                AVG(field_bnbo_total_m2) as avg_bnbo_area,
                AVG(field_wetland_total_m2) as avg_wetland_area,
                MAX(field_area_m2) as max_field_area
            FROM field_environmental_analysis_fields
        """).fetchone()
        
        if field_stats:
            self.log.info(
                f"📊 Area statistics: Avg field: {field_stats[0]:,.0f}m², "
                f"Avg BNBO: {field_stats[1]:,.0f}m², Avg wetland: {field_stats[2]:,.0f}m²"
            )
            self.log.info(f"📊 Max field area: {field_stats[3]:,.0f}m²")

    def _validate_output_data_consistency(self):
        """Validate data consistency between output tables."""
        self.log.info("🔍 Validating data consistency...")
        
        # Validate that all property-level fields exist in field-level table
        missing_fields = self.conn.execute("""
            SELECT COUNT(DISTINCT p.field_uuid)
            FROM field_environmental_analysis_properties p
            LEFT JOIN field_environmental_analysis_fields f ON p.field_uuid = f.field_uuid
            WHERE f.field_uuid IS NULL
        """).fetchone()[0]
        
        if missing_fields > 0:
            self.log.error(
                f"❌ CRITICAL: {missing_fields:,} fields in property table "
                f"but missing from field table!"
            )
        else:
            self.log.info("✅ All fields in property table also exist in field table")
        
        # Validate environmental feature flags are consistent
        inconsistent_bnbo = self.conn.execute("""
            SELECT COUNT(*)
            FROM field_environmental_analysis_fields
            WHERE (field_bnbo_total_m2 > 0 AND field_bnbo_coverage_pct = 0) 
               OR (field_bnbo_total_m2 = 0 AND field_bnbo_coverage_pct > 0)
        """).fetchone()[0]
        
        inconsistent_wetland = self.conn.execute("""
            SELECT COUNT(*)
            FROM field_environmental_analysis_fields
            WHERE (field_wetland_total_m2 > 0 AND field_wetland_coverage_pct = 0) 
               OR (field_wetland_total_m2 = 0 AND field_wetland_coverage_pct > 0)
        """).fetchone()[0]
        
        if inconsistent_bnbo > 0:
            self.log.warning(
                f"⚠️ Found {inconsistent_bnbo:,} fields with inconsistent BNBO "
                f"area/coverage calculations"
            )
        if inconsistent_wetland > 0:
            self.log.warning(
                f"⚠️ Found {inconsistent_wetland:,} fields with inconsistent wetland "
                f"area/coverage calculations"
            )
        
        if inconsistent_bnbo == 0 and inconsistent_wetland == 0:
            self.log.info("✅ All environmental area and coverage calculations are consistent")

    def _get_main_output_table(self) -> str:
        """Get the main output table for area validation."""
        return "field_environmental_analysis_fields"