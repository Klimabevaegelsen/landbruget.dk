"""Stage 2A: Fields × BNBO - Redesigned Architecture

Creates BOTH total and water-covered BNBO geometries following the architectural redesign:
1. field_bnbo_intersections: 2-way total BNBO in fields  
2. field_bnbo_water_intersections: 3-way water-covered BNBO in fields

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


class FieldsBNBOWaterCoverage(FieldAnalysisStageBase):
    """Create both total and water-covered BNBO geometries following redesigned architecture."""

    def __init__(self, config: FieldAnalysisStageConfig = None):
        if config is None:
            config = FieldAnalysisStageConfig()
        super().__init__(config, "Stage 2A: Fields × BNBO Water Coverage")

    def _load_input_data(self):
        """Load foundation data from Stage 1 for geometric intersections."""
        updated_outputs = CONFIG.update_outputs_for_year()
        
        # Load agricultural fields (BUILD side for spatial joins)
        self._load_silver_dataset(CONFIG.get_agricultural_fields_dataset(), "agricultural_fields")

        # Load Stage 0 pre-filtered BNBO data
        self.log.info("Loading Stage 0 pre-filtered BNBO dataset...")
        stage0_bnbo_dataset = updated_outputs["bnbo_prefiltered"]
        stage0_bnbo_path = self._get_latest_gold_path(stage0_bnbo_dataset)
        self.gcs_access.query_parquet_direct(
            stage0_bnbo_path,
            "SELECT *",
            "bnbo_prefiltered",
        )

        # Load water project × BNBO intersections from Stage 1A (foundation data)
        self.log.info("Loading Stage 1A water projects × BNBO intersections...")
        stage1a_dataset = updated_outputs["water_projects_bnbo_intersections"]
        stage1a_path = self._get_latest_gold_path(stage1a_dataset)
        # Load all columns - filtering can be done in SQL if needed
        self.gcs_access.query_parquet_direct(
            stage1a_path,
            "SELECT *",
            "water_projects_bnbo_intersections",
        )

        self.log.info("✅ Loaded foundation data for redesigned Stage 2A")

        # Input validation
        field_count = self.conn.execute("SELECT COUNT(*) FROM agricultural_fields").fetchone()[0]
        bnbo_count = self.conn.execute("SELECT COUNT(*) FROM bnbo_prefiltered").fetchone()[0]
        water_bnbo_count = self.conn.execute(
            "SELECT COUNT(*) FROM water_projects_bnbo_intersections"
        ).fetchone()[0]

        self.log.info("📊 Foundation data loaded:")
        self.log.info(f"  Fields: {field_count:,}")
        self.log.info(f"  BNBO areas (pre-filtered): {bnbo_count:,}")
        self.log.info(f"  Water-covered BNBO intersections: {water_bnbo_count:,}")

        if field_count == 0:
            raise ValueError("No agricultural fields found")
        if bnbo_count == 0:
            self.log.warning("No BNBO areas found - pipeline will create empty result tables")
        if water_bnbo_count == 0:
            self.log.warning("No water-covered BNBO found - all water coverage will be 0")

        return {
            "fields_count": field_count,
            "bnbo_count": bnbo_count,
            "water_bnbo_count": water_bnbo_count,
        }

    async def _execute_stage_processing(self) -> Dict[str, Any]:
        """
        Redesigned Stage 2A: Create both total and water-covered BNBO geometries.
        
        Creates two output tables following architectural redesign:
        1. field_bnbo_intersections: 2-way total BNBO in fields  
        2. field_bnbo_water_intersections: 3-way water-covered BNBO in fields
        
        Key Principles:
        - GEOMETRY ONLY: No ST_Area_Spheroid() calculations (moved to Stage 4)
        - SPATIAL_JOIN Compliance: Single spatial predicates in JOIN ON clauses
        - Leverage Stage 1 Foundation Data: Reuse existing intersection geometries
        """

        self.log.info("🎯 REDESIGNED STAGE 2A: BNBO GEOMETRIC INTERSECTIONS")
        self.log.info("✅ GEOMETRY ONLY - All area calculations moved to Stage 4")
        self.log.info("🔧 SPATIAL_JOIN Compliance - Single spatial predicates in JOIN ON")

        # Step 1: Create 2-way field × BNBO intersections (Total BNBO in fields)
        self.log.info("📦 Step 1: Creating field_bnbo_intersections (2-way total BNBO)")
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_bnbo_intersections AS
            SELECT 
                f.field_uuid,
                f.field_id,
                f.block_id,
                f.cvr_number,
                f.year,
                b.bnbo_id,
                b.status_category,
                ST_Intersection(f.geometry, b.geometry) as field_bnbo_geometry
            FROM agricultural_fields f
            JOIN bnbo_prefiltered b ON ST_Intersects(f.geometry, b.geometry)
        """)

        # Step 2: Create 3-way field × BNBO × water intersections (Water-covered BNBO in fields)
        # SPATIAL_JOIN COMPLIANT: Uses existing geometries from Step 1 and Stage 1A foundation
        self.log.info("📦 Step 2: Creating field_bnbo_water_intersections (3-way water-covered)")
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_bnbo_water_intersections AS
            SELECT 
                fbi.field_uuid,
                fbi.field_id,
                fbi.block_id,
                fbi.cvr_number,
                fbi.year,
                fbi.bnbo_id,
                fbi.status_category,
                wpbi.project_id,
                ST_Intersection(fbi.field_bnbo_geometry, wpbi.intersection_geometry) as field_bnbo_water_geometry
            FROM field_bnbo_intersections fbi
            JOIN water_projects_bnbo_intersections wpbi 
                ON fbi.bnbo_id = wpbi.bnbo_id  -- FIX: Ensure same BNBO to prevent cross-contamination
                AND ST_Intersects(fbi.field_bnbo_geometry, wpbi.intersection_geometry)
        """)

        # Get result statistics
        total_bnbo_intersections = self.conn.execute(
            "SELECT COUNT(*) FROM field_bnbo_intersections"
        ).fetchone()[0]
        
        water_covered_intersections = self.conn.execute(
            "SELECT COUNT(*) FROM field_bnbo_water_intersections"
        ).fetchone()[0]

        unique_fields_with_bnbo = self.conn.execute(
            "SELECT COUNT(DISTINCT field_uuid) FROM field_bnbo_intersections"
        ).fetchone()[0]

        # Validation: Geometry validity checks
        self._validate_geometric_output()

        self.log.info("✅ REDESIGNED STAGE 2A COMPLETE:")
        self.log.info(f"📊 Total field-BNBO intersections: {total_bnbo_intersections:,}")
        self.log.info(f"📊 Water-covered intersections: {water_covered_intersections:,}")
        self.log.info(f"📊 Unique fields with BNBO: {unique_fields_with_bnbo:,}")
        self.log.info("🚀 GEOMETRIC PIPELINE: Ready for Stage 3 consumption")

        return {
            "total_bnbo_intersections": total_bnbo_intersections,
            "water_covered_intersections": water_covered_intersections,
            "unique_fields_with_bnbo": unique_fields_with_bnbo,
        }

    def _save_output_data(self, result: Dict[str, Any]):
        """Save both geometric intersection tables to GCS."""
        # Save 2-way field × BNBO intersections
        self._save_stage_output("field_bnbo_intersections", "field_bnbo_intersections")

        # Save 3-way field × BNBO × water intersections  
        self._save_stage_output("field_bnbo_water_intersections", "field_bnbo_water_intersections")
    
    def _validate_geometric_output(self):
        """Comprehensive validation including geometry, record counts, and data consistency."""
        
        self.log.info("🔍 REDESIGNED STAGE 2A: Running comprehensive validations...")
        
        # 1. GEOMETRY VALIDATIONS
        self._validate_geometry_quality()
        
        # 2. RECORD COUNT VALIDATIONS  
        self._validate_record_counts()
        
        # 3. DATA CONSISTENCY VALIDATIONS
        self._validate_data_consistency()
        
        self.log.info("✅ REDESIGNED STAGE 2A: All comprehensive validations completed")

    def _validate_geometry_quality(self):
        """Validate geometry quality and integrity."""
        self.log.info("🔍 Validating geometry quality...")
        
        # Check field_bnbo_intersections geometry validity
        invalid_bnbo = self.conn.execute("""
            SELECT COUNT(*) 
            FROM field_bnbo_intersections 
            WHERE field_bnbo_geometry IS NULL OR NOT ST_IsValid(field_bnbo_geometry)
        """).fetchone()[0]
        
        # Check field_bnbo_water_intersections geometry validity  
        invalid_water = self.conn.execute("""
            SELECT COUNT(*) 
            FROM field_bnbo_water_intersections 
            WHERE field_bnbo_water_geometry IS NULL OR NOT ST_IsValid(field_bnbo_water_geometry)
        """).fetchone()[0]
        
        # Check for empty geometries
        empty_bnbo = self.conn.execute("""
            SELECT COUNT(*) 
            FROM field_bnbo_intersections 
            WHERE field_bnbo_geometry IS NOT NULL AND ST_IsEmpty(field_bnbo_geometry)
        """).fetchone()[0]
        
        empty_water = self.conn.execute("""
            SELECT COUNT(*) 
            FROM field_bnbo_water_intersections 
            WHERE field_bnbo_water_geometry IS NOT NULL AND ST_IsEmpty(field_bnbo_water_geometry)
        """).fetchone()[0]
        
        # Report geometry validation results
        total_bnbo = self.conn.execute("SELECT COUNT(*) FROM field_bnbo_intersections").fetchone()[0]
        total_water = self.conn.execute("SELECT COUNT(*) FROM field_bnbo_water_intersections").fetchone()[0]
        
        if invalid_bnbo > 0:
            self.log.error(f"❌ Found {invalid_bnbo:,}/{total_bnbo:,} invalid/NULL geometries in field_bnbo_intersections")
        else:
            self.log.info(f"✅ All {total_bnbo:,} field_bnbo_intersections geometries are valid")
            
        if invalid_water > 0:
            self.log.error(f"❌ Found {invalid_water:,}/{total_water:,} invalid/NULL geometries in field_bnbo_water_intersections") 
        else:
            self.log.info(f"✅ All {total_water:,} field_bnbo_water_intersections geometries are valid")
            
        if empty_bnbo > 0:
            self.log.warning(f"⚠️ Found {empty_bnbo:,} empty geometries in field_bnbo_intersections")
        if empty_water > 0:
            self.log.warning(f"⚠️ Found {empty_water:,} empty geometries in field_bnbo_water_intersections")

    def _validate_record_counts(self):
        """Validate record counts and data preservation."""
        self.log.info("🔍 Validating record counts and data preservation...")
        
        # Get input counts
        fields_count = self.conn.execute("SELECT COUNT(*) FROM agricultural_fields").fetchone()[0]
        bnbo_count = self.conn.execute("SELECT COUNT(*) FROM bnbo_prefiltered").fetchone()[0]
        water_bnbo_count = self.conn.execute("SELECT COUNT(*) FROM water_projects_bnbo_intersections").fetchone()[0]
        
        # Get output counts
        field_bnbo_count = self.conn.execute("SELECT COUNT(*) FROM field_bnbo_intersections").fetchone()[0]
        field_bnbo_water_count = self.conn.execute("SELECT COUNT(*) FROM field_bnbo_water_intersections").fetchone()[0]
        
        # Validate field coverage
        unique_fields_with_bnbo = self.conn.execute("""
            SELECT COUNT(DISTINCT field_uuid) FROM field_bnbo_intersections
        """).fetchone()[0]
        
        unique_fields_with_water = self.conn.execute("""
            SELECT COUNT(DISTINCT field_uuid) FROM field_bnbo_water_intersections
        """).fetchone()[0]
        
        # Log validation results
        self.log.info(f"📊 Input data: {fields_count:,} fields, {bnbo_count:,} BNBO features, {water_bnbo_count:,} water×BNBO intersections")
        self.log.info(f"📊 Output data: {field_bnbo_count:,} field×BNBO intersections, {field_bnbo_water_count:,} field×BNBO×water intersections")
        self.log.info(f"📊 Field coverage: {unique_fields_with_bnbo:,} fields with BNBO, {unique_fields_with_water:,} fields with water-covered BNBO")
        
        # Sanity checks
        if field_bnbo_count == 0:
            self.log.error("❌ CRITICAL: No field×BNBO intersections produced!")
        elif field_bnbo_count > fields_count * bnbo_count:
            self.log.warning(f"⚠️ Very high intersection count: {field_bnbo_count:,} (check for data explosion)")
        
        if unique_fields_with_bnbo > fields_count:
            self.log.error(f"❌ CRITICAL: More unique fields with BNBO ({unique_fields_with_bnbo:,}) than total fields ({fields_count:,})")

    def _validate_data_consistency(self):
        """Validate data consistency and logical relationships."""
        self.log.info("🔍 Validating data consistency...")
        
        # Validate that water-covered BNBO is subset of total BNBO
        water_only_fields = self.conn.execute("""
            SELECT COUNT(DISTINCT fbw.field_uuid)
            FROM field_bnbo_water_intersections fbw
            LEFT JOIN field_bnbo_intersections fb ON fbw.field_uuid = fb.field_uuid
            WHERE fb.field_uuid IS NULL
        """).fetchone()[0]
        
        if water_only_fields > 0:
            self.log.error(f"❌ CRITICAL: {water_only_fields:,} fields have water-covered BNBO but no total BNBO (data inconsistency)")
        else:
            self.log.info("✅ Data consistency: All water-covered BNBO fields also have total BNBO records")
        
        # Validate field UUIDs consistency
        invalid_field_uuids = self.conn.execute("""
            SELECT COUNT(*)
            FROM field_bnbo_intersections fb
            LEFT JOIN agricultural_fields f ON fb.field_uuid = f.field_uuid
            WHERE f.field_uuid IS NULL
        """).fetchone()[0]
        
        if invalid_field_uuids > 0:
            self.log.error(f"❌ CRITICAL: {invalid_field_uuids:,} intersection records have invalid field_uuid references")
        else:
            self.log.info("✅ All intersection records have valid field_uuid references")
        
        # Validate no duplicate field×BNBO combinations
        total_records = self.conn.execute("SELECT COUNT(*) FROM field_bnbo_intersections").fetchone()[0]
        unique_combinations = self.conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT DISTINCT field_uuid, bnbo_id FROM field_bnbo_intersections
            )
        """).fetchone()[0]
        duplicates = total_records - unique_combinations
        
        if duplicates > 0:
            self.log.error(f"❌ CRITICAL: {duplicates:,} duplicate field×BNBO combinations found")
        else:
            self.log.info("✅ No duplicate field×BNBO combinations found")
    
    def _get_main_output_table(self) -> str:
        """Get the name of the main output table for area validation."""
        return "field_bnbo_intersections"
