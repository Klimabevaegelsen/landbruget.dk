"""Stage 3C: Properties × Grukos - Groundwater Action Areas Analysis

Creates property-level grukos (indsatsområder) intersection geometries using existing
geometries from Stage 2C.

Grukos areas represent groundwater protection zones for nitrate-sensitive (NFI) and
pesticide-sensitive (SFI) groundwater intake areas.

Key Principles:
- Use field_grukos_intersections from Stage 2C
- Use field_property_intersections from Stage 1C (foundation data)
- Simple geometric intersections with existing geometries
- GEOMETRY ONLY: No ST_Area_Spheroid() calculations (moved to Stage 4)
- SPATIAL_JOIN Compliance: Single spatial predicates in JOIN ON clauses

Architectural Benefits:
- Eliminates complex spatial recalculations
- Leverages Stage 2C geometric foundation
- Creates clean input for Stage 4 calculations
"""

from typing import Any

from ..base import FieldAnalysisStageBase, FieldAnalysisStageConfig
from ..config import CONFIG


class FinalGrukosAnalysis(FieldAnalysisStageBase):
    """Create property-level grukos geometries using Stage 2C foundation data."""

    def __init__(self, config: FieldAnalysisStageConfig = None):
        if config is None:
            config = FieldAnalysisStageConfig()
        super().__init__(config, "Stage 3C: Properties × Grukos - Groundwater Action Areas")

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
        self.log.info(f"Loaded field_property_intersections from {stage1c_dataset}")

        # Load field-grukos intersections from Stage 2C
        stage2c_grukos_dataset = updated_outputs["field_grukos_intersections"]
        stage2c_grukos_path = self._get_latest_gold_path(stage2c_grukos_dataset)
        self.gcs_access.query_parquet_direct(
            stage2c_grukos_path, "SELECT *", "field_grukos_intersections"
        )
        self.log.info(f"Loaded field_grukos_intersections from {stage2c_grukos_dataset}")

        self.log.info("STAGE 3C: Using existing geometries from Stage 2C")

        # Input validation
        property_count = self.conn.execute(
            "SELECT COUNT(*) FROM field_property_intersections"
        ).fetchone()[0]
        grukos_count = self.conn.execute(
            "SELECT COUNT(*) FROM field_grukos_intersections"
        ).fetchone()[0]

        self.log.info("Input data loaded:")
        self.log.info(f"  Field-property intersections: {property_count:,}")
        self.log.info(f"  Field-grukos intersections: {grukos_count:,}")

        if property_count == 0:
            raise ValueError("No field-property intersections found")
        if grukos_count == 0:
            self.log.warning(
                "No grukos intersections found - pipeline will create empty result tables"
            )

        return {
            "property_count": property_count,
            "grukos_count": grukos_count,
        }

    async def _execute_stage_processing(self) -> dict[str, Any]:
        """
        Stage 3C: Create property-level grukos geometries using existing Stage 2C data.

        Creates property-level environmental intersections with existing geometries:
        - property_grukos_intersections: Properties × Grukos geometries

        Key Principles:
        - GEOMETRY ONLY: No ST_Area_Spheroid() calculations (moved to Stage 4)
        - SPATIAL_JOIN Compliance: Single spatial predicates in JOIN ON clauses
        - Use existing geometries from Stage 2C: No complex spatial recalculations
        """

        self.log.info("STAGE 3C: PROPERTY-LEVEL GRUKOS GEOMETRIES")
        self.log.info("GEOMETRY ONLY - All area calculations moved to Stage 4")
        self.log.info("Using existing geometries from Stage 2C - No complex recalculations")

        # Create property × grukos intersections
        self.log.info("Creating property_grukos_intersections")
        self.conn.execute("""
            CREATE OR REPLACE TABLE property_grukos_intersections AS
            SELECT
                p.field_uuid,
                p.bfe_number,
                fgi.field_id,
                fgi.block_id,
                fgi.cvr_number,
                fgi.year,
                fgi.grukos_id,
                ST_Intersection(p.property_geometry, fgi.field_grukos_geometry) as
                    property_grukos_geometry
            FROM field_property_intersections p
            JOIN field_grukos_intersections fgi ON p.field_uuid = fgi.field_uuid
                AND ST_Intersects(p.property_geometry, fgi.field_grukos_geometry)
        """)

        # Get result statistics
        property_grukos_count = self.conn.execute(
            "SELECT COUNT(*) FROM property_grukos_intersections"
        ).fetchone()[0]

        unique_properties = self.conn.execute(
            "SELECT COUNT(DISTINCT bfe_number) FROM property_grukos_intersections"
        ).fetchone()[0]

        # Validation: Geometry validity checks
        self._validate_geometric_output()

        self.log.info("STAGE 3C COMPLETE:")
        self.log.info(f"Property-grukos intersections: {property_grukos_count:,}")
        self.log.info(f"Unique properties in grukos zones: {unique_properties:,}")
        self.log.info("GEOMETRIC PIPELINE: Ready for Stage 4 consumption")

        return {
            "property_grukos_intersections": property_grukos_count,
            "unique_properties": unique_properties,
        }

    def _save_output_data(self, result: dict[str, Any]):
        """Save property × grukos intersection table to GCS."""
        self._save_stage_output("property_grukos_intersections", "property_grukos_intersections")

    def _validate_geometric_output(self):
        """Comprehensive validation including geometry, record counts, and data consistency."""

        self.log.info("STAGE 3C: Running comprehensive validations...")

        # 1. GEOMETRY VALIDATIONS
        self._validate_geometry_quality()

        # 2. RECORD COUNT VALIDATIONS
        self._validate_record_counts()

        # 3. DATA CONSISTENCY VALIDATIONS
        self._validate_data_consistency()

        self.log.info("STAGE 3C: All comprehensive validations completed")

    def _validate_geometry_quality(self):
        """Validate geometry quality and integrity."""
        self.log.info("Validating geometry quality...")

        # Check property_grukos_intersections geometry validity
        invalid_grukos = self.conn.execute("""
            SELECT COUNT(*)
            FROM property_grukos_intersections
            WHERE property_grukos_geometry IS NULL OR NOT ST_IsValid(property_grukos_geometry)
        """).fetchone()[0]

        # Check for empty geometries
        empty_grukos = self.conn.execute("""
            SELECT COUNT(*)
            FROM property_grukos_intersections
            WHERE property_grukos_geometry IS NOT NULL AND ST_IsEmpty(property_grukos_geometry)
        """).fetchone()[0]

        # Report geometry validation results
        total_grukos = self.conn.execute(
            "SELECT COUNT(*) FROM property_grukos_intersections"
        ).fetchone()[0]

        if invalid_grukos > 0:
            self.log.error(
                f"Found {invalid_grukos:,}/{total_grukos:,} invalid/NULL geometries in "
                f"property_grukos_intersections"
            )
        else:
            self.log.info(
                f"All {total_grukos:,} property_grukos_intersections geometries are valid"
            )

        if empty_grukos > 0:
            self.log.warning(
                f"Found {empty_grukos:,} empty geometries in property_grukos_intersections"
            )

    def _validate_record_counts(self):
        """Validate record counts and data preservation."""
        self.log.info("Validating record counts and data preservation...")

        # Get input counts
        field_property_count = self.conn.execute(
            "SELECT COUNT(*) FROM field_property_intersections"
        ).fetchone()[0]
        field_grukos_count = self.conn.execute(
            "SELECT COUNT(*) FROM field_grukos_intersections"
        ).fetchone()[0]

        # Get output counts
        property_grukos_count = self.conn.execute(
            "SELECT COUNT(*) FROM property_grukos_intersections"
        ).fetchone()[0]

        # Validate property coverage
        unique_properties_with_grukos = self.conn.execute("""
            SELECT COUNT(DISTINCT bfe_number) FROM property_grukos_intersections
        """).fetchone()[0]

        # Log validation results
        self.log.info(
            f"Input data: {field_property_count:,} field×property intersections, "
            f"{field_grukos_count:,} field×grukos"
        )
        self.log.info(f"Output data: {property_grukos_count:,} property×grukos intersections")
        self.log.info(
            f"Property coverage: {unique_properties_with_grukos:,} properties in grukos zones"
        )

        # Sanity checks
        if property_grukos_count == 0 and field_grukos_count > 0:
            self.log.warning(
                "No property×grukos intersections produced "
                "(may be expected if no field×property×grukos overlap)"
            )

        if property_grukos_count > field_property_count * field_grukos_count:
            self.log.warning(
                f"Very high intersection count: {property_grukos_count:,} "
                f"(check for data explosion)"
            )

    def _validate_data_consistency(self):
        """Validate data consistency and logical relationships."""
        self.log.info("Validating data consistency...")

        # Validate bfe_number consistency
        invalid_bfe_numbers = self.conn.execute("""
            SELECT COUNT(*)
            FROM property_grukos_intersections pg
            LEFT JOIN field_property_intersections fp ON
                pg.field_uuid = fp.field_uuid AND pg.bfe_number = fp.bfe_number
            WHERE fp.bfe_number IS NULL
        """).fetchone()[0]

        if invalid_bfe_numbers > 0:
            self.log.error(
                f"CRITICAL: {invalid_bfe_numbers:,} intersection records have "
                f"invalid bfe_number references"
            )
        else:
            self.log.info("All intersection records have valid bfe_number references")

        # Validate no duplicate property×grukos combinations within fields
        total_records = self.conn.execute(
            "SELECT COUNT(*) FROM property_grukos_intersections"
        ).fetchone()[0]
        unique_combinations = self.conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT DISTINCT field_uuid, bfe_number, grukos_id
                FROM property_grukos_intersections
            )
        """).fetchone()[0]
        duplicates = total_records - unique_combinations

        if duplicates > 0:
            self.log.error(
                f"CRITICAL: {duplicates:,} duplicate field×property×grukos combinations found"
            )
        else:
            self.log.info("No duplicate field×property×grukos combinations found")

    def _get_main_output_table(self) -> str:
        """Get the name of the main output table for area validation."""
        return "property_grukos_intersections"
