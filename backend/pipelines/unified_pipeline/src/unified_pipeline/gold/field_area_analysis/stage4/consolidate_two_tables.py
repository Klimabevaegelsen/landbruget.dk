"""
Stage 4: Field Area Analysis Consolidation - Two Table Architecture

Creates two separate output tables:
1. Field-Level Environmental Analysis (one record per field)
2. Property-Level Environmental Analysis (one record per field-property combination)

This eliminates the record explosion issue by properly separating field-level and property-level data.
"""

from typing import Dict, Any, Optional
from ..base import FieldAnalysisStageBase, FieldAnalysisStageConfig
from ..config import CONFIG


class ConsolidateResultsTwoTables(FieldAnalysisStageBase):
    """
    Stage 4: Consolidate field area analysis results into two separate tables.
    
    Creates:
    - field_environmental_analysis: One record per field with field-wide environmental totals
    - property_environmental_analysis: One record per field-property combination with property-specific data
    """

    def __init__(self, config: FieldAnalysisStageConfig = None):
        if config is None:
            config = FieldAnalysisStageConfig()
        super().__init__(config, "Stage 4: Consolidate Results - Two-Table Architecture")

    async def _execute_stage_processing(self) -> Dict[str, Any]:
        """Execute Stage 4 consolidation with two-table architecture."""
        
        # Create base tables for consolidation
        self._create_base_tables()
        
        # Create the two output tables
        self._create_field_level_table()
        self._create_property_level_table()
        
        # Return statistics
        return self._get_final_statistics()
        
    def _save_output_data(self, result: Dict[str, Any]):
        """Save both output tables to GCS."""
        
        self.log.info("Saving output tables to GCS...")
        
        # Save field-level table
        self._save_stage_output("field_environmental_analysis", "field_environmental_analysis")
        
        # Save property-level table  
        self._save_stage_output("property_environmental_analysis", "property_environmental_analysis")
        
        self.log.info("✅ Both output tables saved successfully")

    def _load_input_data(self):
        """Load all required input data from previous stages."""
        
        updated_outputs = CONFIG.update_outputs_for_year()
        
        self.log.info("Loading input data from previous stages...")
        
        # Load Stage 1 outputs
        stage1a_path = self._get_latest_gold_path(updated_outputs["field_property_intersections"])
        self.gcs_access.query_parquet_direct(stage1a_path, "SELECT *", "field_property_intersections")
        
        stage1b_path = self._get_latest_gold_path(updated_outputs["field_soil_intersections"])
        self.gcs_access.query_parquet_direct(stage1b_path, "SELECT *", "field_soil_areas")
        
        # Load Stage 3 outputs (both field-level and property-level consolidated data)
        stage3a_fields_path = self._get_latest_gold_path(updated_outputs["final_bnbo"])
        self.gcs_access.query_parquet_direct(stage3a_fields_path, "SELECT *", "final_bnbo_analysis")
        
        stage3b_fields_path = self._get_latest_gold_path(updated_outputs["final_wetland"])
        self.gcs_access.query_parquet_direct(stage3b_fields_path, "SELECT *", "final_wetland_analysis")
        
        stage3a_properties_path = self._get_latest_gold_path(updated_outputs["property_bnbo_intersections"])
        self.gcs_access.query_parquet_direct(stage3a_properties_path, "SELECT *", "property_bnbo_intersections")
        
        stage3b_properties_path = self._get_latest_gold_path(updated_outputs["property_wetland_intersections"])
        self.gcs_access.query_parquet_direct(stage3b_properties_path, "SELECT *", "property_wetland_intersections")
        
        self.log.info("✅ All input data loaded successfully")

    def _create_base_tables(self):
        """Validate input data counts."""
        
        self.log.info("Validating input data...")
        
        # Check Stage 3 field-level outputs  
        bnbo_fields = self.conn.execute("SELECT COUNT(*) FROM final_bnbo_analysis").fetchone()[0]
        wetland_fields = self.conn.execute("SELECT COUNT(*) FROM final_wetland_analysis").fetchone()[0]
        
        # Check Stage 3 property-level outputs
        bnbo_properties = self.conn.execute("SELECT COUNT(*) FROM property_bnbo_intersections").fetchone()[0]
        wetland_properties = self.conn.execute("SELECT COUNT(*) FROM property_wetland_intersections").fetchone()[0]
        
        # Check Stage 1 outputs (property intersections)
        property_intersections = self.conn.execute("SELECT COUNT(*) FROM field_property_intersections").fetchone()[0]
        
        self.log.info(f"✅ Stage 3 field-level: {bnbo_fields:,} BNBO fields, {wetland_fields:,} wetland fields") 
        self.log.info(f"✅ Stage 3 property-level: {bnbo_properties:,} BNBO property intersections, {wetland_properties:,} wetland property intersections")
        self.log.info(f"✅ Stage 1: {property_intersections:,} field-property intersections")

    def _create_field_level_table(self):
        """Create field-level environmental analysis table (one record per field)."""
        
        self.log.info("Creating field-level environmental analysis from Stage 3 consolidated field data...")
        
        # Use Stage 3 consolidated field-level analysis tables directly
        # These already have all the field-level environmental data properly consolidated
        
        # Create comprehensive field environmental analysis from ALL agricultural fields
        # Environmental data is enrichment (0 values where no environmental features)
        
        # First get all unique agricultural fields from property intersections
        self.conn.execute("""
            CREATE OR REPLACE TABLE all_agricultural_fields AS
            SELECT DISTINCT 
                field_id, block_id, cvr_number, year, field_uuid,
                FIRST(field_geometry) as geometry,
                FIRST(field_area_m2) as field_area_m2
            FROM field_property_intersections
            GROUP BY field_id, block_id, cvr_number, year, field_uuid
        """)
        
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_environmental_analysis AS
            SELECT 
                f.field_id, f.block_id, f.cvr_number, f.year, f.field_uuid, 
                f.geometry, f.field_area_m2,
                
                -- BNBO environmental data (0 if no BNBO features)
                COALESCE(b.field_bnbo_total_m2, 0) as field_bnbo_total_m2,
                COALESCE(b.field_bnbo_water_covered_m2, 0) as field_area_bnbo_covered_by_water,
                COALESCE(b.field_bnbo_total_m2, 0) - COALESCE(b.field_bnbo_water_covered_m2, 0) as field_area_bnbo_not_covered_by_water,
                
                -- Wetland environmental data (0 if no wetland features)
                COALESCE(w.field_wetland_total_m2, 0) as field_wetland_total_m2,
                COALESCE(w.field_wetland_water_covered_m2, 0) as field_area_wetlands_covered_by_water,
                COALESCE(w.field_wetland_total_m2, 0) - COALESCE(w.field_wetland_water_covered_m2, 0) as field_area_wetlands_not_covered_by_water,
                
                -- Environmental presence flags
                CASE WHEN b.field_uuid IS NOT NULL OR w.field_uuid IS NOT NULL THEN TRUE ELSE FALSE END as has_environmental_features
                
            FROM all_agricultural_fields f
            LEFT JOIN final_bnbo_analysis b ON f.field_uuid = b.field_uuid AND f.year = b.year
            LEFT JOIN final_wetland_analysis w ON f.field_uuid = w.field_uuid AND f.year = w.year
        """)
        
        field_count = self.conn.execute("SELECT COUNT(*) FROM field_environmental_analysis").fetchone()[0]
        field_unique = self.conn.execute("SELECT COUNT(DISTINCT field_uuid) FROM field_environmental_analysis").fetchone()[0]
        
        if field_count == field_unique:
            self.log.info(f"✅ Field-level table: {field_count:,} records (exactly 1 per field)")
        else:
            self.log.error(f"❌ Field-level table has {field_count - field_unique:,} duplicate records!")

    def _create_property_level_table(self):
        """Create property-level environmental analysis table (one record per field-property combination)."""
        
        self.log.info("Creating property-level environmental analysis from ALL field×property combinations...")
        
        # Create property-level analysis using ALL field×property intersections
        # Environmental data is enrichment (0 values where no environmental features)
        self.conn.execute("""
            CREATE OR REPLACE TABLE property_environmental_analysis AS
            SELECT 
                -- Field and property identification
                fp.field_id,
                fp.block_id,
                fp.cvr_number,
                fp.year,
                fp.field_uuid,
                fp.bfe_number,
                fp.field_area_m2,
                fp.intersection_area_m2 as property_area_within_field_m2,
                fp.field_area_share_pct as property_share_of_field_pct,
                
                -- Property-environmental intersection data from Stage 3 (0 if no environmental features)
                COALESCE(b.property_bnbo_area_m2, 0) as property_bnbo_total_m2,
                COALESCE(b.property_bnbo_covered_m2, 0) as property_area_bnbo_covered_by_water,
                COALESCE(b.property_bnbo_uncovered_m2, 0) as property_area_bnbo_not_covered_by_water,
                COALESCE(b.status_category, NULL) as bnbo_status_category,
                
                COALESCE(w.property_wetland_area_m2, 0) as property_wetland_total_m2,
                COALESCE(w.property_wetland_covered_m2, 0) as property_area_wetlands_covered_by_water,
                COALESCE(w.property_wetland_uncovered_m2, 0) as property_area_wetlands_not_covered_by_water,
                COALESCE(w.toerv_pct, NULL) as wetland_type,
                
                -- Property environmental summary flag
                CASE 
                    WHEN COALESCE(b.property_bnbo_area_m2, 0) > 0 OR COALESCE(w.property_wetland_area_m2, 0) > 0 
                    THEN TRUE ELSE FALSE 
                END as has_environmental_features
                
            FROM field_property_intersections fp
            LEFT JOIN property_bnbo_intersections b ON fp.field_uuid = b.field_uuid 
                AND fp.year = b.year AND fp.bfe_number = b.bfe_number
            LEFT JOIN property_wetland_intersections w ON fp.field_uuid = w.field_uuid 
                AND fp.year = w.year AND fp.bfe_number = w.bfe_number
            WHERE fp.bfe_number IS NOT NULL  -- Only properties, not NULL property fields
        """)
        
        property_count = self.conn.execute("SELECT COUNT(*) FROM property_environmental_analysis").fetchone()[0]
        property_fields = self.conn.execute("SELECT COUNT(DISTINCT field_uuid) FROM property_environmental_analysis").fetchone()[0]
        property_combinations = self.conn.execute("SELECT COUNT(DISTINCT field_uuid || '_' || bfe_number) FROM property_environmental_analysis").fetchone()[0]
        
        if property_count == property_combinations:
            self.log.info(f"✅ Property-level table: {property_count:,} records (exactly 1 per field-property combination)")
            self.log.info(f"   Covers {property_fields:,} fields with property intersections")
        else:
            self.log.error(f"❌ Property-level table has {property_count - property_combinations:,} duplicate combinations!")



    def _get_final_statistics(self) -> Dict[str, Any]:
        """Get final statistics for both tables."""
        
        field_stats = self.conn.execute("""
            SELECT 
                COUNT(*) as total_fields,
                COUNT(CASE WHEN has_environmental_features THEN 1 END) as fields_with_environmental_features,
                COUNT(CASE WHEN field_area_bnbo_covered_by_water > 0 THEN 1 END) as fields_with_bnbo,
                COUNT(CASE WHEN field_area_wetlands_covered_by_water > 0 THEN 1 END) as fields_with_wetlands,
                AVG(field_area_m2) as avg_field_area_m2,
                SUM(field_area_bnbo_covered_by_water + field_area_bnbo_not_covered_by_water) as total_bnbo_area_m2,
                SUM(field_area_wetlands_covered_by_water + field_area_wetlands_not_covered_by_water) as total_wetland_area_m2
            FROM field_environmental_analysis
        """).fetchone()
        
        property_stats = self.conn.execute("""
            SELECT 
                COUNT(*) as total_property_combinations,
                COUNT(DISTINCT field_uuid) as fields_with_properties,
                COUNT(DISTINCT bfe_number) as unique_properties,
                COUNT(CASE WHEN has_environmental_features THEN 1 END) as properties_with_environmental_features,
                AVG(property_area_within_field_m2) as avg_property_area_m2,
                SUM(property_area_bnbo_covered_by_water + property_area_bnbo_not_covered_by_water) as total_property_bnbo_area_m2,
                SUM(property_area_wetlands_covered_by_water + property_area_wetlands_not_covered_by_water) as total_property_wetland_area_m2
            FROM property_environmental_analysis
        """).fetchone()
        
        stats = {
            "field_level": {
                "total_fields": field_stats[0],
                "fields_without_properties": field_stats[1],
                "fields_with_environmental_features": field_stats[2],
                "fields_with_bnbo": field_stats[3],
                "fields_with_wetlands": field_stats[4],
                "avg_field_area_m2": field_stats[5],
                "total_bnbo_area_m2": field_stats[6],
                "total_wetland_area_m2": field_stats[7]
            },
            "property_level": {
                "total_property_combinations": property_stats[0],
                "fields_with_properties": property_stats[1],
                "unique_properties": property_stats[2],
                "properties_with_environmental_features": property_stats[3],
                "avg_property_area_m2": property_stats[4],
                "total_property_bnbo_area_m2": property_stats[5],
                "total_property_wetland_area_m2": property_stats[6]
            }
        }
        
        self.log.info("📊 FINAL STATISTICS:")
        self.log.info(f"   Field-level table: {stats['field_level']['total_fields']:,} fields")
        self.log.info(f"   Property-level table: {stats['property_level']['total_property_combinations']:,} field-property combinations")
        self.log.info(f"   Fields without properties: {stats['field_level']['fields_without_properties']:,}")
        self.log.info(f"   Fields with environmental features: {stats['field_level']['fields_with_environmental_features']:,}")
        
        return stats

    def _get_input_area_reference(self) -> Optional[Dict[str, Any]]:
        """
        Stage 4 validation: Validate against ORIGINAL agricultural fields.
        
        Stage 4 creates comprehensive datasets with ALL fields, so we should validate
        against the original field dataset (Stage 1 input), not filtered Stage 3 outputs.
        """
        if not self._should_validate_areas():
            return None
            
        try:
            # Get reference from original field-property intersections (which has all fields)
            # Handle potential fragments correctly - field_property_intersections may have multiple records per field
            fields_area_stats = self.conn.execute("""
                SELECT 
                    COUNT(DISTINCT field_uuid) as field_count,
                    SUM(field_area_m2) as total_area
                FROM (
                    SELECT DISTINCT field_uuid, field_area_m2
                    FROM field_property_intersections
                    WHERE field_area_m2 IS NOT NULL AND field_area_m2 > 0
                ) unique_fields
            """).fetchone()
            
            if fields_area_stats and fields_area_stats[0] and fields_area_stats[1]:
                return {
                    "total_area": fields_area_stats[1],
                    "field_count": fields_area_stats[0]
                }
            else:
                self.log.warning("⚠️ Could not get field reference stats for Stage 4 validation")
                return None
                
        except Exception as e:
            self.log.warning(f"⚠️ Failed to get Stage 4 input reference: {e}")
            return None

    def _get_main_output_table(self) -> Optional[str]:
        """Return the main output table for validation."""
        return "field_environmental_analysis"
    
    def _validate_stage_areas(self) -> None:
        """
        Custom validation for Stage 4: Comprehensive Field Environmental Analysis.
        
        Validates that ALL original agricultural fields are preserved with correct areas.
        """
        if not self._should_validate_areas() or not self.area_validator:
            return
            
        input_reference = self._get_input_area_reference()
        if not input_reference:
            self.log.info("⚠️ Stage 4 validation skipped: missing input reference")
            return
            
        try:
            # Validate field-level table: should have ALL fields with correct total area
            field_stats = self.conn.execute("""
                SELECT 
                    COUNT(DISTINCT field_uuid) as distinct_field_count,
                    (SELECT SUM(field_area_m2) 
                     FROM (SELECT DISTINCT field_uuid, field_area_m2 
                           FROM field_environmental_analysis 
                           WHERE field_area_m2 IS NOT NULL AND field_area_m2 > 0)
                    ) as total_distinct_field_area,
                    COUNT(*) as total_records
                FROM field_environmental_analysis
                WHERE field_area_m2 IS NOT NULL AND field_area_m2 > 0
            """).fetchone()
            
            distinct_field_count = field_stats[0] or 0
            total_distinct_field_area = field_stats[1] or 0
            total_records = field_stats[2] or 0
            
            # Validate property-level table stats (different expectations!)
            property_stats = self.conn.execute("""
                SELECT 
                    COUNT(DISTINCT field_uuid) as distinct_field_count,
                    COUNT(*) as total_property_records,
                    SUM(COALESCE(property_area_within_field_m2, 0)) as total_intersection_area
                FROM property_environmental_analysis
                WHERE field_area_m2 IS NOT NULL AND field_area_m2 > 0
            """).fetchone()
            
            property_field_count = property_stats[0] or 0
            property_total_records = property_stats[1] or 0
            property_total_intersection_area = property_stats[2] or 0
            
            # Validation 1: Field-level table should have ALL fields
            field_count_diff = distinct_field_count - input_reference["field_count"]
            field_count_valid = field_count_diff == 0
            
            # Validation 2: Property-level table should have FEWER fields (only those with properties)
            # This is expected and correct - not all fields have property intersections
            property_field_count_valid = property_field_count <= input_reference["field_count"]
            
            # Validation 3: Field-level area preservation (should match original exactly)
            field_area_difference = total_distinct_field_area - input_reference["total_area"]
            field_area_difference_pct = (field_area_difference / input_reference["total_area"]) * 100 if input_reference["total_area"] > 0 else 0
            field_area_valid = abs(field_area_difference_pct) <= self.area_validator.tolerance_pct
            
            # Validation 4: Property-level area should be LESS than original (incomplete coverage)
            property_area_difference = property_total_intersection_area - input_reference["total_area"]  
            property_area_difference_pct = (property_area_difference / input_reference["total_area"]) * 100 if input_reference["total_area"] > 0 else 0
            property_coverage_pct = (property_total_intersection_area / input_reference["total_area"]) * 100 if input_reference["total_area"] > 0 else 0
            
            # Property area should be ≤ original area (expect reduction due to incomplete coverage)
            property_area_valid = property_area_difference_pct <= 0 and abs(property_area_difference_pct) <= 50.0  # 50% tolerance for property coverage
            
            # Overall validation result
            validation_passed = field_count_valid and property_field_count_valid and field_area_valid and property_area_valid
            
            # Log results
            if validation_passed:
                self.log.info(f"✅ Stage 4 validation PASSED:")  
            else:
                self.log.error(f"❌ Stage 4 validation FAILED:")
                
            self.log.info(f"📊 Field-Level Table (ALL agricultural fields):")
            self.log.info(f"    Field Count - Input: {input_reference['field_count']:,}, Output: {distinct_field_count:,} ({field_count_diff:+,})")
            self.log.info(f"    Total Area - Input: {input_reference['total_area']:,.0f} m², Output: {total_distinct_field_area:,.0f} m² ({field_area_difference_pct:+.3f}%)")
            self.log.info(f"    Records: {total_records:,} (should be 1 per field)")
            
            self.log.info(f"📊 Property-Level Table (fields with properties only):")
            self.log.info(f"    Field Count - Fields with properties: {property_field_count:,} of {input_reference['field_count']:,} total fields")
            self.log.info(f"    Property Coverage - {property_total_intersection_area:,.0f} m² ({property_coverage_pct:.1f}% of total field area)")
            self.log.info(f"    Property Area vs Total - {property_area_difference_pct:+.3f}% (expected reduction due to incomplete coverage)")
            self.log.info(f"    Records: {property_total_records:,} field×property combinations")
            
            # Handle validation failure
            if not validation_passed:
                error_msg = f"Stage 4 validation failed - Field count: {field_count_valid}, Property field count: {property_field_count_valid}, Field area: {field_area_valid}, Property area: {property_area_valid}"
                if self.validation_config.fail_on_validation_error:
                    from ..area_validation import ValidationException
                    raise ValidationException(error_msg)
                else:
                    self.log.warning(f"⚠️ {error_msg} but continuing")
                    
        except Exception as e:
            error_msg = f"❌ Stage 4 validation error: {str(e)}"
            if self.validation_config.fail_on_validation_error:
                raise Exception(error_msg)
            else:
                self.log.warning(f"⚠️ {error_msg} but continuing")