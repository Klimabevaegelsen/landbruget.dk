"""Stage 2B: Fields × Wetlands - Redesigned Architecture

Creates BOTH total and water-covered wetland geometries following the architectural redesign:
1. field_wetland_intersections: 2-way total wetlands in fields  
2. field_wetland_water_intersections: 3-way water-covered wetlands in fields

Key Principles:
- GEOMETRY ONLY: No ST_Area_Spheroid() calculations (moved to Stage 4)
- SPATIAL_JOIN Compliance: Single spatial predicates in JOIN ON clauses
- Leverage Stage 1 Foundation Data: Reuse existing intersection geometries

Architectural Benefits:
- Eliminates fragment duplication issues
- Ensures DuckDB Spatial PR #545 compliance
- Creates clean geometric pipeline for Stage 3/4 consumption
"""

from typing import Any, Dict

from ..base import FieldAnalysisStageBase, FieldAnalysisStageConfig
from ..config import CONFIG


class FieldsWetlandWaterCoverage(FieldAnalysisStageBase):
    """Create both total and water-covered wetland geometries following redesigned architecture."""

    def __init__(self, config: FieldAnalysisStageConfig = None):
        if config is None:
            config = FieldAnalysisStageConfig()
        super().__init__(config, "Stage 2B: Fields × Wetland Water Coverage")

    def _load_input_data(self):
        """Load foundation data from Stage 1 for geometric intersections."""
        updated_outputs = CONFIG.update_outputs_for_year()
        
        # Load agricultural fields (BUILD side for spatial joins)
        self._load_silver_dataset(CONFIG.get_agricultural_fields_dataset(), "agricultural_fields")

        # Load Stage 0 pre-filtered wetlands data
        self.log.info("Loading Stage 0 pre-filtered wetlands dataset...")
        stage0_wetlands_dataset = updated_outputs["wetlands_prefiltered"]
        stage0_wetlands_path = self._get_latest_gold_path(stage0_wetlands_dataset)
        self.gcs_access.query_parquet_direct(
            stage0_wetlands_path,
            "SELECT wetland_id, CAST(toerv_pct AS VARCHAR) as toerv_pct, geometry, wetland_area_m2",
            "wetlands_prefiltered",
        )

        # Load water project × wetland intersections from Stage 1B (foundation data)
        self.log.info("Loading Stage 1B water projects × wetlands intersections...")
        stage1b_dataset = updated_outputs["water_projects_wetlands_intersections"]
        stage1b_path = self._get_latest_gold_path(stage1b_dataset)
        self.gcs_access.query_parquet_direct(
            stage1b_path,
            "SELECT wetland_id, CAST(toerv_pct AS VARCHAR) as toerv_pct, project_id, intersection_geometry, intersection_area_m2, wetland_area_m2, project_area_m2",
            "water_projects_wetlands_intersections",
        )

        self.log.info(f"✅ Loaded foundation data for redesigned Stage 2B")

        # Input validation
        field_count = self.conn.execute("SELECT COUNT(*) FROM agricultural_fields").fetchone()[0]
        wetland_count = self.conn.execute("SELECT COUNT(*) FROM wetlands_prefiltered").fetchone()[0]
        water_wetland_count = self.conn.execute(
            "SELECT COUNT(*) FROM water_projects_wetlands_intersections"
        ).fetchone()[0]

        self.log.info(f"📊 Foundation data loaded:")
        self.log.info(f"  Fields: {field_count:,}")
        self.log.info(f"  Wetlands (pre-filtered): {wetland_count:,}")
        self.log.info(f"  Water-covered wetland intersections: {water_wetland_count:,}")

        if field_count == 0:
            raise ValueError("No agricultural fields found")
        if wetland_count == 0:
            self.log.warning("No wetlands found - pipeline will create empty result tables")
        if water_wetland_count == 0:
            self.log.warning("No water-covered wetlands found - all water coverage will be 0")

        return {
            "fields_count": field_count,
            "wetland_count": wetland_count,
            "water_wetland_count": water_wetland_count,
        }

    async def _execute_stage_processing(self) -> Dict[str, Any]:
        """
        Redesigned Stage 2B: Create both total and water-covered wetland geometries.
        
        Creates two output tables following architectural redesign:
        1. field_wetland_intersections: 2-way total wetlands in fields  
        2. field_wetland_water_intersections: 3-way water-covered wetlands in fields
        
        Key Principles:
        - GEOMETRY ONLY: No ST_Area_Spheroid() calculations (moved to Stage 4)
        - SPATIAL_JOIN Compliance: Single spatial predicates in JOIN ON clauses
        - Leverage Stage 1 Foundation Data: Reuse existing intersection geometries
        """

        self.log.info("🎯 REDESIGNED STAGE 2B: WETLAND GEOMETRIC INTERSECTIONS")
        self.log.info("✅ GEOMETRY ONLY - All area calculations moved to Stage 4")
        self.log.info("🔧 SPATIAL_JOIN Compliance - Single spatial predicates in JOIN ON")

        # Step 1: Create 2-way field × wetland intersections (Total wetlands in fields)
        self.log.info("📦 Step 1: Creating field_wetland_intersections (2-way total wetlands)")
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_wetland_intersections AS
            SELECT 
                f.field_uuid,
                f.field_id,
                f.block_id,
                f.cvr_number,
                f.year,
                w.wetland_id,
                w.toerv_pct,
                ST_Intersection(f.geometry, w.geometry) as field_wetland_geometry
            FROM agricultural_fields f
            JOIN wetlands_prefiltered w ON ST_Intersects(f.geometry, w.geometry)
        """)

        # Step 2: Create 3-way field × wetland × water intersections (Water-covered wetlands in fields)
        # SPATIAL_JOIN COMPLIANT: Uses existing geometries from Step 1 and Stage 1B foundation
        self.log.info("📦 Step 2: Creating field_wetland_water_intersections (3-way water-covered)")
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_wetland_water_intersections AS
            SELECT 
                fwi.field_uuid,
                fwi.field_id,
                fwi.block_id,
                fwi.cvr_number,
                fwi.year,
                fwi.wetland_id,
                fwi.toerv_pct,
                wpwi.project_id,
                ST_Intersection(fwi.field_wetland_geometry, wpwi.intersection_geometry) as field_wetland_water_geometry
            FROM field_wetland_intersections fwi
            JOIN water_projects_wetlands_intersections wpwi ON fwi.wetland_id = wpwi.wetland_id
                AND ST_Intersects(fwi.field_wetland_geometry, wpwi.intersection_geometry)
        """)

        # Get result statistics
        total_wetland_intersections = self.conn.execute(
            "SELECT COUNT(*) FROM field_wetland_intersections"
        ).fetchone()[0]
        
        water_covered_intersections = self.conn.execute(
            "SELECT COUNT(*) FROM field_wetland_water_intersections"
        ).fetchone()[0]

        unique_fields_with_wetlands = self.conn.execute(
            "SELECT COUNT(DISTINCT field_uuid) FROM field_wetland_intersections"
        ).fetchone()[0]

        # Validation: Geometry validity checks
        self._validate_geometric_output()

        self.log.info(f"✅ REDESIGNED STAGE 2B COMPLETE:")
        self.log.info(f"📊 Total field-wetland intersections: {total_wetland_intersections:,}")
        self.log.info(f"📊 Water-covered intersections: {water_covered_intersections:,}")
        self.log.info(f"📊 Unique fields with wetlands: {unique_fields_with_wetlands:,}")
        self.log.info("🚀 GEOMETRIC PIPELINE: Ready for Stage 3 consumption")

        return {
            "total_wetland_intersections": total_wetland_intersections,
            "water_covered_intersections": water_covered_intersections,
            "unique_fields_with_wetlands": unique_fields_with_wetlands,
        }

    def _save_output_data(self, result: Dict[str, Any]):
        """Save both geometric intersection tables to GCS."""
        # Save 2-way field × wetland intersections
        self._save_stage_output("field_wetland_intersections", "field_wetland_intersections")

        # Save 3-way field × wetland × water intersections  
        self._save_stage_output("field_wetland_water_intersections", "field_wetland_water_intersections")
    
    def _validate_geometric_output(self):
        """Comprehensive validation including geometry, record counts, and data consistency."""
        
        self.log.info("🔍 REDESIGNED STAGE 2B: Running comprehensive validations...")
        
        # 1. GEOMETRY VALIDATIONS
        self._validate_geometry_quality()
        
        # 2. RECORD COUNT VALIDATIONS  
        self._validate_record_counts()
        
        # 3. DATA CONSISTENCY VALIDATIONS
        self._validate_data_consistency()
        
        self.log.info("✅ REDESIGNED STAGE 2B: All comprehensive validations completed")

    def _validate_geometry_quality(self):
        """Validate geometry quality and integrity."""
        self.log.info("🔍 Validating geometry quality...")
        
        # Check field_wetland_intersections geometry validity
        invalid_wetland = self.conn.execute("""
            SELECT COUNT(*) 
            FROM field_wetland_intersections 
            WHERE field_wetland_geometry IS NULL OR NOT ST_IsValid(field_wetland_geometry)
        """).fetchone()[0]
        
        # Check field_wetland_water_intersections geometry validity  
        invalid_water = self.conn.execute("""
            SELECT COUNT(*) 
            FROM field_wetland_water_intersections 
            WHERE field_wetland_water_geometry IS NULL OR NOT ST_IsValid(field_wetland_water_geometry)
        """).fetchone()[0]
        
        # Check for empty geometries
        empty_wetland = self.conn.execute("""
            SELECT COUNT(*) 
            FROM field_wetland_intersections 
            WHERE field_wetland_geometry IS NOT NULL AND ST_IsEmpty(field_wetland_geometry)
        """).fetchone()[0]
        
        empty_water = self.conn.execute("""
            SELECT COUNT(*) 
            FROM field_wetland_water_intersections 
            WHERE field_wetland_water_geometry IS NOT NULL AND ST_IsEmpty(field_wetland_water_geometry)
        """).fetchone()[0]
        
        # Report geometry validation results
        total_wetland = self.conn.execute("SELECT COUNT(*) FROM field_wetland_intersections").fetchone()[0]
        total_water = self.conn.execute("SELECT COUNT(*) FROM field_wetland_water_intersections").fetchone()[0]
        
        if invalid_wetland > 0:
            self.log.error(f"❌ Found {invalid_wetland:,}/{total_wetland:,} invalid/NULL geometries in field_wetland_intersections")
        else:
            self.log.info(f"✅ All {total_wetland:,} field_wetland_intersections geometries are valid")
            
        if invalid_water > 0:
            self.log.error(f"❌ Found {invalid_water:,}/{total_water:,} invalid/NULL geometries in field_wetland_water_intersections") 
        else:
            self.log.info(f"✅ All {total_water:,} field_wetland_water_intersections geometries are valid")
            
        if empty_wetland > 0:
            self.log.warning(f"⚠️ Found {empty_wetland:,} empty geometries in field_wetland_intersections")
        if empty_water > 0:
            self.log.warning(f"⚠️ Found {empty_water:,} empty geometries in field_wetland_water_intersections")

    def _validate_record_counts(self):
        """Validate record counts and data preservation."""
        self.log.info("🔍 Validating record counts and data preservation...")
        
        # Get input counts
        fields_count = self.conn.execute("SELECT COUNT(*) FROM agricultural_fields").fetchone()[0]
        wetland_count = self.conn.execute("SELECT COUNT(*) FROM wetlands_prefiltered").fetchone()[0]
        water_wetland_count = self.conn.execute("SELECT COUNT(*) FROM water_projects_wetlands_intersections").fetchone()[0]
        
        # Get output counts
        field_wetland_count = self.conn.execute("SELECT COUNT(*) FROM field_wetland_intersections").fetchone()[0]
        field_wetland_water_count = self.conn.execute("SELECT COUNT(*) FROM field_wetland_water_intersections").fetchone()[0]
        
        # Validate field coverage
        unique_fields_with_wetland = self.conn.execute("""
            SELECT COUNT(DISTINCT field_uuid) FROM field_wetland_intersections
        """).fetchone()[0]
        
        unique_fields_with_water = self.conn.execute("""
            SELECT COUNT(DISTINCT field_uuid) FROM field_wetland_water_intersections
        """).fetchone()[0]
        
        # Log validation results
        self.log.info(f"📊 Input data: {fields_count:,} fields, {wetland_count:,} wetland features, {water_wetland_count:,} water×wetland intersections")
        self.log.info(f"📊 Output data: {field_wetland_count:,} field×wetland intersections, {field_wetland_water_count:,} field×wetland×water intersections")
        self.log.info(f"📊 Field coverage: {unique_fields_with_wetland:,} fields with wetlands, {unique_fields_with_water:,} fields with water-covered wetlands")
        
        # Sanity checks
        if field_wetland_count == 0:
            self.log.error("❌ CRITICAL: No field×wetland intersections produced!")
        elif field_wetland_count > fields_count * wetland_count:
            self.log.warning(f"⚠️ Very high intersection count: {field_wetland_count:,} (check for data explosion)")
        
        if unique_fields_with_wetland > fields_count:
            self.log.error(f"❌ CRITICAL: More unique fields with wetlands ({unique_fields_with_wetland:,}) than total fields ({fields_count:,})")

    def _validate_data_consistency(self):
        """Validate data consistency and logical relationships."""
        self.log.info("🔍 Validating data consistency...")
        
        # Validate that water-covered wetlands is subset of total wetlands
        water_only_fields = self.conn.execute("""
            SELECT COUNT(DISTINCT fww.field_uuid)
            FROM field_wetland_water_intersections fww
            LEFT JOIN field_wetland_intersections fw ON fww.field_uuid = fw.field_uuid
            WHERE fw.field_uuid IS NULL
        """).fetchone()[0]
        
        if water_only_fields > 0:
            self.log.error(f"❌ CRITICAL: {water_only_fields:,} fields have water-covered wetlands but no total wetland (data inconsistency)")
        else:
            self.log.info("✅ Data consistency: All water-covered wetland fields also have total wetland records")
        
        # Validate field UUIDs consistency
        invalid_field_uuids = self.conn.execute("""
            SELECT COUNT(*)
            FROM field_wetland_intersections fw
            LEFT JOIN agricultural_fields f ON fw.field_uuid = f.field_uuid
            WHERE f.field_uuid IS NULL
        """).fetchone()[0]
        
        if invalid_field_uuids > 0:
            self.log.error(f"❌ CRITICAL: {invalid_field_uuids:,} intersection records have invalid field_uuid references")
        else:
            self.log.info("✅ All intersection records have valid field_uuid references")
        
        # Validate no duplicate field×wetland combinations
        duplicates = self.conn.execute("""
            SELECT COUNT(*) - COUNT(DISTINCT field_uuid, wetland_id) as duplicate_count
            FROM field_wetland_intersections
        """).fetchone()[0]
        
        if duplicates > 0:
            self.log.error(f"❌ CRITICAL: {duplicates:,} duplicate field×wetland combinations found")
        else:
            self.log.info("✅ No duplicate field×wetland combinations found")
    
    def _get_main_output_table(self) -> str:
        """Get the name of the main output table for area validation."""
        return "field_wetland_intersections"
