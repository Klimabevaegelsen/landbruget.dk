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
        
        # Load Stage 2 outputs (field-level environmental data - direct field-environment intersections)
        stage2a_path = self._get_latest_gold_path(updated_outputs["fields_bnbo_water"])
        self.gcs_access.query_parquet_direct(stage2a_path, "SELECT *", "fields_bnbo_water")
        
        stage2b_path = self._get_latest_gold_path(updated_outputs["fields_wetland_water"])
        self.gcs_access.query_parquet_direct(stage2b_path, "SELECT *", "fields_wetland_water")
        
        # Load Stage 3 property-level intersection outputs (NEW!)
        stage3a_properties_path = self._get_latest_gold_path(updated_outputs["property_bnbo_intersections"])
        self.gcs_access.query_parquet_direct(stage3a_properties_path, "SELECT *", "property_bnbo_intersections")
        
        stage3b_properties_path = self._get_latest_gold_path(updated_outputs["property_wetland_intersections"])
        self.gcs_access.query_parquet_direct(stage3b_properties_path, "SELECT *", "property_wetland_intersections")
        
        self.log.info("✅ All input data loaded successfully")

    def _create_base_tables(self):
        """Validate input data counts."""
        
        self.log.info("Validating input data...")
        
        # Check Stage 2 outputs (field-level environmental data)
        bnbo_fields = self.conn.execute("SELECT COUNT(*) FROM fields_bnbo_water").fetchone()[0]
        wetland_fields = self.conn.execute("SELECT COUNT(*) FROM fields_wetland_water").fetchone()[0]
        
        # Check Stage 1 outputs (property intersections)
        property_intersections = self.conn.execute("SELECT COUNT(*) FROM field_property_intersections").fetchone()[0]
        
        self.log.info(f"✅ Stage 2: {bnbo_fields:,} fields with BNBO, {wetland_fields:,} fields with wetlands") 
        self.log.info(f"✅ Stage 1: {property_intersections:,} field-property intersections")

    def _create_field_level_table(self):
        """Create field-level environmental analysis table (one record per field)."""
        
        self.log.info("Creating field-level environmental analysis from Stage 2 field-environment intersections...")
        
        # Start from ALL fields and LEFT JOIN environmental data
        # This ensures we get all fields with environmental data as 0 for fields without features
        self.conn.execute("""
            CREATE OR REPLACE TABLE all_fields_base AS
            SELECT DISTINCT 
                field_id, block_id, cvr_number, year, field_uuid, 
                field_geometry as geometry, field_area_m2
            FROM field_property_intersections
        """)
        
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_environmental_analysis AS
            SELECT 
                -- Field identification (from complete field set)
                f.field_id,
                f.block_id,
                f.cvr_number,
                f.year,
                f.field_uuid,
                f.geometry,
                f.field_area_m2,
                
                -- Soil data (from Stage 1 - field-soil intersections)
                COALESCE(s.soil_type_count, 0) as soil_type_count,
                COALESCE(s.unique_soil_codes, 0) as unique_soil_codes,
                COALESCE(s.dominant_soil_type, NULL) as dominant_soil_type,
                COALESCE(s.dominant_soil_coverage_pct, 0) as dominant_soil_coverage_pct,
                COALESCE(s.total_soil_coverage_pct, 0) as total_soil_coverage_pct,
                COALESCE(s.soil_type_breakdown, '{}') as soil_type_breakdown,
                
                -- Field-wide environmental data (from Stage 2 - direct field-environment intersections)
                COALESCE(b.field_bnbo_total_m2, 0) as field_bnbo_total_m2,
                COALESCE(b.field_bnbo_water_covered_m2, 0) as field_area_bnbo_covered_by_water,
                COALESCE(b.field_bnbo_total_m2, 0) - COALESCE(b.field_bnbo_water_covered_m2, 0) as field_area_bnbo_not_covered_by_water,
                
                COALESCE(w.field_wetland_total_m2, 0) as field_wetland_total_m2,
                COALESCE(w.field_wetland_water_covered_m2, 0) as field_area_wetlands_covered_by_water,
                COALESCE(w.field_wetland_total_m2, 0) - COALESCE(w.field_wetland_water_covered_m2, 0) as field_area_wetlands_not_covered_by_water,
                
                -- Environmental summary flag
                CASE 
                    WHEN COALESCE(b.field_bnbo_total_m2, 0) > 0 OR COALESCE(w.field_wetland_total_m2, 0) > 0 
                    THEN TRUE ELSE FALSE 
                END as has_environmental_features
                
            FROM all_fields_base f
            LEFT JOIN fields_bnbo_water b ON f.field_uuid = b.field_uuid AND f.year = b.year
            LEFT JOIN fields_wetland_water w ON f.field_uuid = w.field_uuid AND f.year = w.year
            LEFT JOIN (
                SELECT 
                    field_uuid, year,
                    COUNT(DISTINCT soil_type_category) as soil_type_count,
                    COUNT(DISTINCT soil_code) as unique_soil_codes,
                    (SELECT soil_type_category FROM field_soil_areas fsa2 WHERE fsa2.field_uuid = fsa.field_uuid AND fsa2.year = fsa.year ORDER BY fsa2.soil_area_m2 DESC LIMIT 1) as dominant_soil_type,
                    (SELECT soil_area_share_pct FROM field_soil_areas fsa2 WHERE fsa2.field_uuid = fsa.field_uuid AND fsa2.year = fsa.year ORDER BY fsa2.soil_area_m2 DESC LIMIT 1) as dominant_soil_coverage_pct,
                    SUM(soil_area_share_pct) as total_soil_coverage_pct,
                    '{' || STRING_AGG('"' || soil_type_category || '": {' || '"area_m2": ' || ROUND(soil_area_m2, 2) || ', "coverage_pct": ' || ROUND(soil_area_share_pct, 2) || '}', ', ' ORDER BY soil_area_m2 DESC) || '}' as soil_type_breakdown
                FROM field_soil_areas fsa
                GROUP BY field_uuid, year
            ) s ON f.field_uuid = s.field_uuid AND f.year = s.year
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
            WHERE fp.bfe_number IS NOT NULL  -- Only properties, not NULL property fields
            LEFT JOIN property_bnbo_intersections b ON fp.field_uuid = b.field_uuid 
                AND fp.year = b.year AND fp.bfe_number = b.bfe_number
            LEFT JOIN property_wetland_intersections w ON fp.field_uuid = w.field_uuid 
                AND fp.year = w.year AND fp.bfe_number = w.bfe_number
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
                COUNT(CASE WHEN primary_bfe_number IS NULL THEN 1 END) as fields_without_properties,
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