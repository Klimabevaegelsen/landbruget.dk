"""
Stage 4: Field Area Analysis Consolidation - Two Table Architecture

Creates two separate output tables:
1. Field-Level Environmental Analysis (one record per field)
2. Property-Level Environmental Analysis (one record per field-property combination)

This eliminates the record explosion issue by properly separating field-level and property-level data.
"""

from typing import Dict, Any
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
        stage3a_fields_path = self._get_latest_gold_path(updated_outputs["final_bnbo_analysis"])
        self.gcs_access.query_parquet_direct(stage3a_fields_path, "SELECT *", "final_bnbo_analysis")
        
        stage3b_fields_path = self._get_latest_gold_path(updated_outputs["final_wetland_analysis"])
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
        
        # Combine Stage 3 field-level analysis tables to create comprehensive field environmental analysis
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_environmental_analysis AS
            -- BNBO fields with environmental data
            SELECT 
                field_id, block_id, cvr_number, year, field_uuid, geometry, field_area_m2,
                
                -- BNBO environmental data  
                field_bnbo_total_m2,
                field_bnbo_water_covered_m2 as field_area_bnbo_covered_by_water,
                field_bnbo_total_m2 - field_bnbo_water_covered_m2 as field_area_bnbo_not_covered_by_water,
                
                -- No wetland data for BNBO-only fields
                0 as field_wetland_total_m2,
                0 as field_area_wetlands_covered_by_water, 
                0 as field_area_wetlands_not_covered_by_water,
                
                TRUE as has_environmental_features
            FROM final_bnbo_analysis
            
            UNION ALL
            
            -- Wetland fields with environmental data  
            SELECT 
                field_id, block_id, cvr_number, year, field_uuid, geometry, field_area_m2,
                
                -- No BNBO data for wetland-only fields
                0 as field_bnbo_total_m2,
                0 as field_area_bnbo_covered_by_water,
                0 as field_area_bnbo_not_covered_by_water,
                
                -- Wetland environmental data
                field_wetland_total_m2,
                field_wetland_water_covered_m2 as field_area_wetlands_covered_by_water,
                field_wetland_total_m2 - field_wetland_water_covered_m2 as field_area_wetlands_not_covered_by_water,
                
                TRUE as has_environmental_features
            FROM final_wetland_analysis
            WHERE field_uuid NOT IN (SELECT field_uuid FROM final_bnbo_analysis)  -- Avoid duplicates
        """)
        
        field_count = self.conn.execute("SELECT COUNT(*) FROM field_environmental_analysis").fetchone()[0]
        field_unique = self.conn.execute("SELECT COUNT(DISTINCT field_uuid) FROM field_environmental_analysis").fetchone()[0]
        
        if field_count == field_unique:
            self.log.info(f"✅ Field-level table: {field_count:,} records (exactly 1 per field)")
        else:
            self.log.error(f"❌ Field-level table has {field_count - field_unique:,} duplicate records!")

    def _create_property_level_table(self):
        """Create property-level environmental analysis table (one record per field-property combination)."""
        
        self.log.info("Creating property-level environmental analysis table...")
        
        # Create property-level environmental analysis using ACTUAL property-environment intersections from Stage 3
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
                
                -- ACTUAL property-environmental intersection data from Stage 3
                COALESCE(b.property_bnbo_area_m2, 0) as property_bnbo_total_m2,
                COALESCE(b.property_bnbo_water_covered_m2, 0) as property_area_bnbo_covered_by_water,
                COALESCE(b.property_bnbo_water_uncovered_m2, 0) as property_area_bnbo_not_covered_by_water,
                COALESCE(b.status_category, NULL) as bnbo_status_category,
                
                COALESCE(w.property_wetland_area_m2, 0) as property_wetland_total_m2,
                COALESCE(w.property_wetland_water_covered_m2, 0) as property_area_wetlands_covered_by_water,
                COALESCE(w.property_wetland_water_uncovered_m2, 0) as property_area_wetlands_not_covered_by_water,
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