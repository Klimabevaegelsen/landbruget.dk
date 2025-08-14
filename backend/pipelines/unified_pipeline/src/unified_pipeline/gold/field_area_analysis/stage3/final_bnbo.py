"""Stage 3A: Properties × BNBO-Water - Simplified Architecture

Creates property-level environmental intersections using existing geometries from Stage 2A.
MASSIVELY SIMPLIFIED per architectural redesign - no complex spatial recalculations needed.

New Simplified Approach:
- Use field_property_intersections from Stage 1C (foundation data)
- Use field_bnbo_intersections and field_bnbo_water_intersections from Stage 2A
- Simple geometric intersections with existing geometries
- GEOMETRY ONLY: No ST_Area_Spheroid() calculations (moved to Stage 4)
- SPATIAL_JOIN Compliance: Single spatial predicates in JOIN ON clauses

Architectural Benefits:
- Eliminates complex spatial recalculations
- Leverages Stage 2A geometric foundation
- Creates clean input for Stage 4 calculations
"""

from typing import Any, Dict

from ..base import FieldAnalysisStageBase, FieldAnalysisStageConfig
from ..config import CONFIG


class FinalBNBOAnalysis(FieldAnalysisStageBase):
    """Create property-level BNBO geometries using Stage 2A foundation data."""

    def __init__(self, config: FieldAnalysisStageConfig = None):
        if config is None:
            config = FieldAnalysisStageConfig()
        super().__init__(config, "Stage 3A: Properties × BNBO - Simplified")

    def _load_input_data(self):
        """Load geometric foundation data from previous stages."""
        updated_outputs = CONFIG.update_outputs_for_year()

        # Load field-property intersections from Stage 1C (foundation data)
        stage1c_dataset = updated_outputs["field_property_intersections"]
        stage1c_path = self._get_latest_gold_path(stage1c_dataset)
        self.gcs_access.query_parquet_direct(
            stage1c_path,
            (
                "SELECT field_uuid, bfe_number, "
                "intersection_geometry as property_geometry"
            ),
            "field_property_intersections",
        )
        self.log.info(
            f"✅ Loaded field_property_intersections from {stage1c_dataset}"
        )

        # Load field-BNBO intersections from Stage 2A
        stage2a_bnbo_dataset = updated_outputs["field_bnbo_intersections"]
        stage2a_bnbo_path = self._get_latest_gold_path(stage2a_bnbo_dataset)
        self.gcs_access.query_parquet_direct(
            stage2a_bnbo_path, "SELECT *", "field_bnbo_intersections"
        )
        self.log.info(
            f"✅ Loaded field_bnbo_intersections from {stage2a_bnbo_dataset}"
        )

        # Load field-BNBO-water intersections from Stage 2A
        stage2a_water_dataset = updated_outputs["field_bnbo_water_intersections"]
        stage2a_water_path = self._get_latest_gold_path(stage2a_water_dataset)
        self.gcs_access.query_parquet_direct(
            stage2a_water_path, "SELECT *", "stage3a_field_bnbo_water_intersections"
        )
        self.log.info(
            f"✅ Loaded field_bnbo_water_intersections from {stage2a_water_dataset}"
        )

        self.log.info(
            "✅ SIMPLIFIED STAGE 3A: Using existing geometries from Stage 2A"
        )

        # Input validation
        property_count = self.conn.execute(
            "SELECT COUNT(*) FROM field_property_intersections"
        ).fetchone()[0]
        bnbo_count = self.conn.execute(
            "SELECT COUNT(*) FROM field_bnbo_intersections"
        ).fetchone()[0]
        water_bnbo_count = self.conn.execute(
            "SELECT COUNT(*) FROM stage3a_field_bnbo_water_intersections"
        ).fetchone()[0]

        self.log.info("📊 Input data loaded:")
        self.log.info(f"  Field-property intersections: {property_count:,}")
        self.log.info(f"  Field-BNBO intersections: {bnbo_count:,}")
        self.log.info(f"  Field-BNBO-water intersections: {water_bnbo_count:,}")

        return {
            "property_count": property_count,
            "bnbo_count": bnbo_count,
            "water_bnbo_count": water_bnbo_count,
        }

    async def _execute_stage_processing(self) -> Dict[str, Any]:
        """
        Simplified Stage 3A: Create property-level BNBO geometries using existing Stage 2A data.

        Creates property-level environmental intersections with existing
        geometries:
        1. property_bnbo_intersections: Properties × BNBO total
           geometries
        2. property_bnbo_water_intersections: Properties × BNBO
           water-covered geometries

        Key Principles:
        - GEOMETRY ONLY: No ST_Area_Spheroid() calculations (moved to Stage 4)
        - SPATIAL_JOIN Compliance: Single spatial predicates in JOIN ON
          clauses
        - Use existing geometries from Stage 2A: No complex spatial
          recalculations
        """

        self.log.info(
            "🎯 SIMPLIFIED STAGE 3A: PROPERTY-LEVEL BNBO GEOMETRIES"
        )
        self.log.info(
            "✅ GEOMETRY ONLY - All area calculations moved to Stage 4"
        )
        self.log.info(
            "🔧 Using existing geometries from Stage 2A - No complex recalculations"
        )

        # Step 1: Create property × BNBO intersections (total BNBO within
        # properties)
        self.log.info("📦 Step 1: Creating property_bnbo_intersections")
        self.conn.execute("""
            CREATE OR REPLACE TABLE property_bnbo_intersections AS
            SELECT
                p.field_uuid,
                p.bfe_number,
                fbi.field_id,
                fbi.block_id,
                fbi.cvr_number,
                fbi.year,
                fbi.bnbo_id,
                fbi.status_category,
                ST_Intersection(
                    p.property_geometry, fbi.field_bnbo_geometry
                ) as property_bnbo_geometry
            FROM field_property_intersections p
            JOIN field_bnbo_intersections fbi ON p.field_uuid = fbi.field_uuid
                AND ST_Intersects(
                    p.property_geometry, fbi.field_bnbo_geometry
                )
        """)

        # Step 2: Create property × BNBO × water intersections
        # (water-covered BNBO within properties)
        self.log.info("📦 Step 2: Creating property_bnbo_water_intersections")
        self.conn.execute("""
            CREATE OR REPLACE TABLE property_bnbo_water_intersections AS
            SELECT
                p.field_uuid,
                p.bfe_number,
                fbwi.field_id,
                fbwi.block_id,
                fbwi.cvr_number,
                fbwi.year,
                fbwi.bnbo_id,
                fbwi.status_category,
                fbwi.project_id,
                ST_Intersection(
                    p.property_geometry, fbwi.field_bnbo_water_geometry
                ) as property_bnbo_water_geometry
            FROM field_property_intersections p
            JOIN stage3a_field_bnbo_water_intersections fbwi
                ON p.field_uuid = fbwi.field_uuid
                AND ST_Intersects(
                    p.property_geometry, fbwi.field_bnbo_water_geometry
                )
        """)

        # Get result statistics
        property_bnbo_count = self.conn.execute(
            "SELECT COUNT(*) FROM property_bnbo_intersections"
        ).fetchone()[0]

        property_bnbo_water_count = self.conn.execute(
            "SELECT COUNT(*) FROM property_bnbo_water_intersections"
        ).fetchone()[0]

        unique_properties = self.conn.execute(
            "SELECT COUNT(DISTINCT bfe_number) FROM property_bnbo_intersections"
        ).fetchone()[0]

        # Validation: Geometry validity checks
        self._validate_geometric_output()

        self.log.info("✅ SIMPLIFIED STAGE 3A COMPLETE:")
        self.log.info(
            f"📊 Property-BNBO intersections: {property_bnbo_count:,}"
        )
        self.log.info(
            f"📊 Property-BNBO-water intersections: {property_bnbo_water_count:,}"
        )
        self.log.info(
            f"📊 Unique properties with BNBO: {unique_properties:,}"
        )
        self.log.info(
            "🚀 GEOMETRIC PIPELINE: Ready for Stage 4 consumption"
        )

        return {
            "property_bnbo_intersections": property_bnbo_count,
            "property_bnbo_water_intersections": property_bnbo_water_count,
            "unique_properties": unique_properties,
        }

    def _save_output_data(self, result: Dict[str, Any]):
        """Save both geometric intersection tables to GCS."""
        # Save property × BNBO intersections
        self._save_stage_output(
            "property_bnbo_intersections", "property_bnbo_intersections"
        )

        # Save property × BNBO × water intersections
        self._save_stage_output(
            "property_bnbo_water_intersections",
            "property_bnbo_water_intersections"
        )

    def _validate_geometric_output(self):
        """Comprehensive validation including geometry, record counts, and data
        consistency."""

        self.log.info(
            "🔍 REDESIGNED STAGE 3A: Running comprehensive validations..."
        )

        # 1. GEOMETRY VALIDATIONS
        self._validate_geometry_quality()

        # 2. RECORD COUNT VALIDATIONS
        self._validate_record_counts()

        # 3. DATA CONSISTENCY VALIDATIONS
        self._validate_data_consistency()

        self.log.info(
            "✅ REDESIGNED STAGE 3A: All comprehensive validations completed"
        )

    def _validate_geometry_quality(self):
        """Validate geometry quality and integrity."""
        self.log.info("🔍 Validating geometry quality...")

        # Check property_bnbo_intersections geometry validity
        invalid_bnbo = self.conn.execute("""
            SELECT COUNT(*)
            FROM property_bnbo_intersections
            WHERE property_bnbo_geometry IS NULL
               OR NOT ST_IsValid(property_bnbo_geometry)
        """).fetchone()[0]

        # Check property_bnbo_water_intersections geometry validity
        invalid_water = self.conn.execute("""
            SELECT COUNT(*)
            FROM property_bnbo_water_intersections
            WHERE property_bnbo_water_geometry IS NULL
               OR NOT ST_IsValid(property_bnbo_water_geometry)
        """).fetchone()[0]

        # Check for empty geometries
        empty_bnbo = self.conn.execute("""
            SELECT COUNT(*)
            FROM property_bnbo_intersections
            WHERE property_bnbo_geometry IS NOT NULL
              AND ST_IsEmpty(property_bnbo_geometry)
        """).fetchone()[0]

        empty_water = self.conn.execute("""
            SELECT COUNT(*)
            FROM property_bnbo_water_intersections
            WHERE property_bnbo_water_geometry IS NOT NULL
              AND ST_IsEmpty(property_bnbo_water_geometry)
        """).fetchone()[0]

        # Report geometry validation results
        total_bnbo = self.conn.execute(
            "SELECT COUNT(*) FROM property_bnbo_intersections"
        ).fetchone()[0]
        total_water = self.conn.execute(
            "SELECT COUNT(*) FROM property_bnbo_water_intersections"
        ).fetchone()[0]

        if invalid_bnbo > 0:
            self.log.error(
                f"❌ Found {invalid_bnbo:,}/{total_bnbo:,} invalid/NULL "
                f"geometries in property_bnbo_intersections"
            )
        else:
            self.log.info(
                f"✅ All {total_bnbo:,} property_bnbo_intersections "
                f"geometries are valid"
            )

        if invalid_water > 0:
            self.log.error(
                f"❌ Found {invalid_water:,}/{total_water:,} invalid/NULL "
                f"geometries in property_bnbo_water_intersections"
            )
        else:
            self.log.info(
                f"✅ All {total_water:,} property_bnbo_water_intersections "
                f"geometries are valid"
            )

        if empty_bnbo > 0:
            self.log.warning(
                f"⚠️ Found {empty_bnbo:,} empty geometries in "
                f"property_bnbo_intersections"
            )
        if empty_water > 0:
            self.log.warning(
                f"⚠️ Found {empty_water:,} empty geometries in "
                f"property_bnbo_water_intersections"
            )

    def _validate_record_counts(self):
        """Validate record counts and data preservation."""
        self.log.info("🔍 Validating record counts and data preservation...")

        # Get input counts
        field_property_count = self.conn.execute(
            "SELECT COUNT(*) FROM field_property_intersections"
        ).fetchone()[0]
        field_bnbo_count = self.conn.execute(
            "SELECT COUNT(*) FROM field_bnbo_intersections"
        ).fetchone()[0]
        field_bnbo_water_count = self.conn.execute(
            "SELECT COUNT(*) FROM stage3a_field_bnbo_water_intersections"
        ).fetchone()[0]

        # Get output counts
        property_bnbo_count = self.conn.execute(
            "SELECT COUNT(*) FROM property_bnbo_intersections"
        ).fetchone()[0]
        property_bnbo_water_count = self.conn.execute(
            "SELECT COUNT(*) FROM property_bnbo_water_intersections"
        ).fetchone()[0]

        # Validate property coverage
        unique_properties_with_bnbo = self.conn.execute("""
            SELECT COUNT(DISTINCT bfe_number) FROM property_bnbo_intersections
        """).fetchone()[0]

        unique_properties_with_water = self.conn.execute("""
            SELECT COUNT(DISTINCT bfe_number) FROM property_bnbo_water_intersections
        """).fetchone()[0]

        # Log validation results
        self.log.info(
            f"📊 Input data: {field_property_count:,} field×property intersections, "
            f"{field_bnbo_count:,} field×BNBO, "
            f"{field_bnbo_water_count:,} field×BNBO×water"
        )
        self.log.info(
            f"📊 Output data: {property_bnbo_count:,} property×BNBO "
            f"intersections, {property_bnbo_water_count:,} "
            f"property×BNBO×water intersections"
        )
        self.log.info(
            f"📊 Property coverage: {unique_properties_with_bnbo:,} properties "
            f"with BNBO, {unique_properties_with_water:,} properties with "
            f"water-covered BNBO"
        )

        # Sanity checks
        if property_bnbo_count == 0:
            self.log.warning(
                "⚠️ No property×BNBO intersections produced (may be expected "
                "if no field×property×BNBO overlap)"
            )

        if property_bnbo_count > field_property_count * field_bnbo_count:
            self.log.warning(
                f"⚠️ Very high intersection count: {property_bnbo_count:,} "
                f"(check for data explosion)"
            )

    def _validate_data_consistency(self):
        """Validate data consistency and logical relationships."""
        self.log.info("🔍 Validating data consistency...")

        # Validate that water-covered BNBO properties are subset of total
        # BNBO
        water_only_properties = self.conn.execute("""
            SELECT COUNT(DISTINCT pbw.bfe_number)
            FROM property_bnbo_water_intersections pbw
            LEFT JOIN property_bnbo_intersections pb
                ON pbw.bfe_number = pb.bfe_number
                AND pbw.field_uuid = pb.field_uuid
            WHERE pb.bfe_number IS NULL
        """).fetchone()[0]

        if water_only_properties > 0:
            self.log.error(
                f"❌ CRITICAL: {water_only_properties:,} properties have "
                f"water-covered BNBO but no total BNBO (data inconsistency)"
            )
        else:
            self.log.info(
                "✅ Data consistency: All water-covered BNBO properties also "
                "have total BNBO records"
            )

        # Validate bfe_number consistency
        invalid_bfe_numbers = self.conn.execute("""
            SELECT COUNT(*)
            FROM property_bnbo_intersections pb
            LEFT JOIN field_property_intersections fp
                ON pb.field_uuid = fp.field_uuid
                AND pb.bfe_number = fp.bfe_number
            WHERE fp.bfe_number IS NULL
        """).fetchone()[0]

        if invalid_bfe_numbers > 0:
            self.log.error(
                f"❌ CRITICAL: {invalid_bfe_numbers:,} intersection records have "
                f"invalid bfe_number references"
            )
        else:
            self.log.info(
                "✅ All intersection records have valid bfe_number references"
            )

        # Validate no duplicate property×BNBO combinations within fields
        total_records = self.conn.execute(
            "SELECT COUNT(*) FROM property_bnbo_intersections"
        ).fetchone()[0]
        unique_combinations = self.conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT DISTINCT field_uuid, bfe_number, bnbo_id
                FROM property_bnbo_intersections
            )
        """).fetchone()[0]
        duplicates = total_records - unique_combinations

        if duplicates > 0:
            self.log.error(
                f"❌ CRITICAL: {duplicates:,} duplicate field×property×BNBO "
                f"combinations found"
            )
        else:
            self.log.info(
                "✅ No duplicate field×property×BNBO combinations found"
            )

    def _get_main_output_table(self) -> str:
        """Get the name of the main output table for area validation."""
        return "property_bnbo_intersections"
