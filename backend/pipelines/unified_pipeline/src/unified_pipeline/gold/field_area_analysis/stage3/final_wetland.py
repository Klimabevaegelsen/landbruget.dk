"""Stage 3B: Properties × Wetlands-Water - Simplified Architecture

Creates property-level environmental intersections using existing geometries from Stage 2B.
MASSIVELY SIMPLIFIED per architectural redesign - no complex spatial recalculations needed.

New Simplified Approach:
- Use field_property_intersections from Stage 1C (foundation data)
- Use field_wetland_intersections and field_wetland_water_intersections from Stage 2B
- Simple geometric intersections with existing geometries
- GEOMETRY ONLY: No ST_Area_Spheroid() calculations (moved to Stage 4)
- SPATIAL_JOIN Compliance: Single spatial predicates in JOIN ON clauses

Architectural Benefits:
- Eliminates complex spatial recalculations
- Leverages Stage 2B geometric foundation
- Creates clean input for Stage 4 calculations
"""

from typing import Any, Dict

from ..base import FieldAnalysisStageBase, FieldAnalysisStageConfig
from ..config import CONFIG


class FinalWetlandAnalysis(FieldAnalysisStageBase):
    """Create property-level wetland geometries using Stage 2B foundation data."""

    def __init__(self, config: FieldAnalysisStageConfig = None):
        if config is None:
            config = FieldAnalysisStageConfig()
        super().__init__(config, "Stage 3B: Properties × Wetlands - Simplified")

    def _load_input_data(self):
        """Load geometric foundation data from previous stages."""
        updated_outputs = CONFIG.update_outputs_for_year()

        # Load field-property intersections from Stage 1C (foundation data)
        stage1c_dataset = updated_outputs["field_property_intersections"]
        stage1c_path = self._get_latest_gold_path(stage1c_dataset)
        self.gcs_access.query_parquet_direct(
            stage1c_path,
            "SELECT field_uuid, bfe_number, intersection_geometry as property_geometry",
            "field_property_intersections",
        )
        self.log.info(f"✅ Loaded field_property_intersections from {stage1c_dataset}")

        # Load field-wetland intersections from Stage 2B
        stage2b_wetland_dataset = updated_outputs["field_wetland_intersections"]
        stage2b_wetland_path = self._get_latest_gold_path(stage2b_wetland_dataset)
        self.gcs_access.query_parquet_direct(
            stage2b_wetland_path, "SELECT *", "field_wetland_intersections"
        )
        self.log.info(f"✅ Loaded field_wetland_intersections from {stage2b_wetland_dataset}")

        # Load field-wetland-water intersections from Stage 2B
        stage2b_water_dataset = updated_outputs["field_wetland_water_intersections"]
        stage2b_water_path = self._get_latest_gold_path(stage2b_water_dataset)
        self.gcs_access.query_parquet_direct(
            stage2b_water_path, "SELECT *", "stage3b_field_wetland_water_intersections"
        )
        self.log.info(f"✅ Loaded field_wetland_water_intersections from {stage2b_water_dataset}")

        self.log.info("✅ SIMPLIFIED STAGE 3B: Using existing geometries from Stage 2B")

        # Input validation
        property_count = self.conn.execute(
            "SELECT COUNT(*) FROM field_property_intersections"
        ).fetchone()[0]
        wetland_count = self.conn.execute(
            "SELECT COUNT(*) FROM field_wetland_intersections"
        ).fetchone()[0]
        water_wetland_count = self.conn.execute(
            "SELECT COUNT(*) FROM stage3b_field_wetland_water_intersections"
        ).fetchone()[0]

        self.log.info("📊 Input data loaded:")
        self.log.info(f"  Field-property intersections: {property_count:,}")
        self.log.info(f"  Field-wetland intersections: {wetland_count:,}")
        self.log.info(f"  Field-wetland-water intersections: {water_wetland_count:,}")

        return {
            "property_count": property_count,
            "wetland_count": wetland_count,
            "water_wetland_count": water_wetland_count,
        }

    async def _execute_stage_processing(self) -> Dict[str, Any]:
        """
        Simplified Stage 3B: Create property-level wetland geometries using existing Stage 2B data.

        Creates property-level environmental intersections with existing geometries:
        1. property_wetland_intersections: Properties × Wetland total geometries
        2. property_wetland_water_intersections: Properties × Wetland water-covered geometries

        Key Principles:
        - GEOMETRY ONLY: No ST_Area_Spheroid() calculations (moved to Stage 4)
        - SPATIAL_JOIN Compliance: Single spatial predicates in JOIN ON clauses
        - Use existing geometries from Stage 2B: No complex spatial recalculations
        """

        self.log.info("🎯 SIMPLIFIED STAGE 3B: PROPERTY-LEVEL WETLAND GEOMETRIES")
        self.log.info("✅ GEOMETRY ONLY - All area calculations moved to Stage 4")
        self.log.info("🔧 Using existing geometries from Stage 2B - No complex recalculations")

        # Step 1: Create property × wetland intersections (total wetlands within properties)
        self.log.info("📦 Step 1: Creating property_wetland_intersections")
        self.conn.execute("""
            CREATE OR REPLACE TABLE property_wetland_intersections AS
            SELECT 
                p.field_uuid,
                p.bfe_number,
                fwi.field_id,
                fwi.block_id,
                fwi.cvr_number,
                fwi.year,
                fwi.wetland_id,
                fwi.toerv_pct,
                ST_Intersection(p.property_geometry, fwi.field_wetland_geometry) as property_wetland_geometry
            FROM field_property_intersections p
            JOIN field_wetland_intersections fwi ON p.field_uuid = fwi.field_uuid
                AND ST_Intersects(p.property_geometry, fwi.field_wetland_geometry)
        """)

        # Step 2: Create property × wetland × water intersections (water-covered wetlands within properties)
        self.log.info("📦 Step 2: Creating property_wetland_water_intersections")
        self.conn.execute("""
            CREATE OR REPLACE TABLE property_wetland_water_intersections AS
            SELECT 
                p.field_uuid,
                p.bfe_number,
                fwwi.field_id,
                fwwi.block_id,
                fwwi.cvr_number,
                fwwi.year,
                fwwi.wetland_id,
                fwwi.toerv_pct,
                fwwi.project_id,
                ST_Intersection(p.property_geometry, fwwi.field_wetland_water_geometry) as property_wetland_water_geometry
            FROM field_property_intersections p
            JOIN stage3b_field_wetland_water_intersections fwwi ON p.field_uuid = fwwi.field_uuid
                AND ST_Intersects(p.property_geometry, fwwi.field_wetland_water_geometry)
        """)

        # Get result statistics
        property_wetland_count = self.conn.execute(
            "SELECT COUNT(*) FROM property_wetland_intersections"
        ).fetchone()[0]

        property_wetland_water_count = self.conn.execute(
            "SELECT COUNT(*) FROM property_wetland_water_intersections"
        ).fetchone()[0]

        unique_properties = self.conn.execute(
            "SELECT COUNT(DISTINCT bfe_number) FROM property_wetland_intersections"
        ).fetchone()[0]

        # Validation: Geometry validity checks
        self._validate_geometric_output()

        self.log.info("✅ SIMPLIFIED STAGE 3B COMPLETE:")
        self.log.info(f"📊 Property-wetland intersections: {property_wetland_count:,}")
        self.log.info(f"📊 Property-wetland-water intersections: {property_wetland_water_count:,}")
        self.log.info(f"📊 Unique properties with wetlands: {unique_properties:,}")
        self.log.info("🚀 GEOMETRIC PIPELINE: Ready for Stage 4 consumption")

        return {
            "property_wetland_intersections": property_wetland_count,
            "property_wetland_water_intersections": property_wetland_water_count,
            "unique_properties": unique_properties,
        }

    def _save_output_data(self, result: Dict[str, Any]):
        """Save both geometric intersection tables to GCS."""
        # Save property × wetland intersections
        self._save_stage_output("property_wetland_intersections", "property_wetland_intersections")

        # Save property × wetland × water intersections
        self._save_stage_output(
            "property_wetland_water_intersections", "property_wetland_water_intersections"
        )

    def _validate_geometric_output(self):
        """Comprehensive validation including geometry, record counts, and data consistency."""

        self.log.info("🔍 REDESIGNED STAGE 3B: Running comprehensive validations...")

        # 1. GEOMETRY VALIDATIONS
        self._validate_geometry_quality()

        # 2. RECORD COUNT VALIDATIONS
        self._validate_record_counts()

        # 3. DATA CONSISTENCY VALIDATIONS
        self._validate_data_consistency()

        self.log.info("✅ REDESIGNED STAGE 3B: All comprehensive validations completed")

    def _validate_geometry_quality(self):
        """Validate geometry quality and integrity."""
        self.log.info("🔍 Validating geometry quality...")

        # Check property_wetland_intersections geometry validity
        invalid_wetland = self.conn.execute("""
            SELECT COUNT(*) 
            FROM property_wetland_intersections 
            WHERE property_wetland_geometry IS NULL OR NOT ST_IsValid(property_wetland_geometry)
        """).fetchone()[0]

        # Check property_wetland_water_intersections geometry validity
        invalid_water = self.conn.execute("""
            SELECT COUNT(*) 
            FROM property_wetland_water_intersections 
            WHERE property_wetland_water_geometry IS NULL OR NOT ST_IsValid(property_wetland_water_geometry)
        """).fetchone()[0]

        # Check for empty geometries
        empty_wetland = self.conn.execute("""
            SELECT COUNT(*) 
            FROM property_wetland_intersections 
            WHERE property_wetland_geometry IS NOT NULL AND ST_IsEmpty(property_wetland_geometry)
        """).fetchone()[0]

        empty_water = self.conn.execute("""
            SELECT COUNT(*) 
            FROM property_wetland_water_intersections 
            WHERE property_wetland_water_geometry IS NOT NULL AND ST_IsEmpty(property_wetland_water_geometry)
        """).fetchone()[0]

        # Report geometry validation results
        total_wetland = self.conn.execute(
            "SELECT COUNT(*) FROM property_wetland_intersections"
        ).fetchone()[0]
        total_water = self.conn.execute(
            "SELECT COUNT(*) FROM property_wetland_water_intersections"
        ).fetchone()[0]

        if invalid_wetland > 0:
            self.log.error(
                f"❌ Found {invalid_wetland:,}/{total_wetland:,} invalid/NULL geometries in property_wetland_intersections"
            )
        else:
            self.log.info(
                f"✅ All {total_wetland:,} property_wetland_intersections geometries are valid"
            )

        if invalid_water > 0:
            self.log.error(
                f"❌ Found {invalid_water:,}/{total_water:,} invalid/NULL geometries in property_wetland_water_intersections"
            )
        else:
            self.log.info(
                f"✅ All {total_water:,} property_wetland_water_intersections geometries are valid"
            )

        if empty_wetland > 0:
            self.log.warning(
                f"⚠️ Found {empty_wetland:,} empty geometries in property_wetland_intersections"
            )
        if empty_water > 0:
            self.log.warning(
                f"⚠️ Found {empty_water:,} empty geometries in property_wetland_water_intersections"
            )

    def _validate_record_counts(self):
        """Validate record counts and data preservation."""
        self.log.info("🔍 Validating record counts and data preservation...")

        # Get input counts
        field_property_count = self.conn.execute(
            "SELECT COUNT(*) FROM field_property_intersections"
        ).fetchone()[0]
        field_wetland_count = self.conn.execute(
            "SELECT COUNT(*) FROM field_wetland_intersections"
        ).fetchone()[0]
        field_wetland_water_count = self.conn.execute(
            "SELECT COUNT(*) FROM stage3b_field_wetland_water_intersections"
        ).fetchone()[0]

        # Get output counts
        property_wetland_count = self.conn.execute(
            "SELECT COUNT(*) FROM property_wetland_intersections"
        ).fetchone()[0]
        property_wetland_water_count = self.conn.execute(
            "SELECT COUNT(*) FROM property_wetland_water_intersections"
        ).fetchone()[0]

        # Validate property coverage
        unique_properties_with_wetland = self.conn.execute("""
            SELECT COUNT(DISTINCT bfe_number) FROM property_wetland_intersections
        """).fetchone()[0]

        unique_properties_with_water = self.conn.execute("""
            SELECT COUNT(DISTINCT bfe_number) FROM property_wetland_water_intersections
        """).fetchone()[0]

        # Log validation results
        self.log.info(
            f"📊 Input data: {field_property_count:,} field×property intersections, {field_wetland_count:,} field×wetland, {field_wetland_water_count:,} field×wetland×water"
        )
        self.log.info(
            f"📊 Output data: {property_wetland_count:,} property×wetland intersections, {property_wetland_water_count:,} property×wetland×water intersections"
        )
        self.log.info(
            f"📊 Property coverage: {unique_properties_with_wetland:,} properties with wetlands, {unique_properties_with_water:,} properties with water-covered wetlands"
        )

        # Sanity checks
        if property_wetland_count == 0:
            self.log.warning(
                "⚠️ No property×wetland intersections produced (may be expected if no field×property×wetland overlap)"
            )

        if property_wetland_count > field_property_count * field_wetland_count:
            self.log.warning(
                f"⚠️ Very high intersection count: {property_wetland_count:,} (check for data explosion)"
            )

    def _validate_data_consistency(self):
        """Validate data consistency and logical relationships."""
        self.log.info("🔍 Validating data consistency...")

        # Validate that water-covered wetland properties are subset of total wetlands
        water_only_properties = self.conn.execute("""
            SELECT COUNT(DISTINCT pww.bfe_number)
            FROM property_wetland_water_intersections pww
            LEFT JOIN property_wetland_intersections pw ON pww.bfe_number = pw.bfe_number AND pww.field_uuid = pw.field_uuid
            WHERE pw.bfe_number IS NULL
        """).fetchone()[0]

        if water_only_properties > 0:
            self.log.error(
                f"❌ CRITICAL: {water_only_properties:,} properties have water-covered wetlands but no total wetland (data inconsistency)"
            )
        else:
            self.log.info(
                "✅ Data consistency: All water-covered wetland properties also have total wetland records"
            )

        # Validate property IDs consistency
        invalid_bfe_numbers = self.conn.execute("""
            SELECT COUNT(*)
            FROM property_wetland_intersections pw
            LEFT JOIN field_property_intersections fp ON pw.field_uuid = fp.field_uuid AND pw.bfe_number = fp.bfe_number
            WHERE fp.bfe_number IS NULL
        """).fetchone()[0]

        if invalid_bfe_numbers > 0:
            self.log.error(
                f"❌ CRITICAL: {invalid_bfe_numbers:,} intersection records have invalid bfe_number references"
            )
        else:
            self.log.info("✅ All intersection records have valid bfe_number references")

        # Validate no duplicate property×wetland combinations within fields
        total_records = self.conn.execute(
            "SELECT COUNT(*) FROM property_wetland_intersections"
        ).fetchone()[0]
        unique_combinations = self.conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT DISTINCT field_uuid, bfe_number, wetland_id FROM property_wetland_intersections
            )
        """).fetchone()[0]
        duplicates = total_records - unique_combinations

        if duplicates > 0:
            self.log.error(
                f"❌ CRITICAL: {duplicates:,} duplicate field×property×wetland combinations found"
            )
        else:
            self.log.info("✅ No duplicate field×property×wetland combinations found")

    def _get_main_output_table(self) -> str:
        """Get the name of the main output table for area validation."""
        return "property_wetland_intersections"
