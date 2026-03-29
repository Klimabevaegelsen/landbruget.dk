"""Stage 2C: Fields × Grukos - Groundwater Action Areas Analysis

Creates field-level grukos (indsatsområder) intersection geometries.
Grukos areas represent groundwater protection zones for nitrate-sensitive (NFI)
and pesticide-sensitive (SFI) groundwater intake areas.

Key Principles:
- GEOMETRY ONLY: No ST_Area_Spheroid() calculations (moved to Stage 4)
- SPATIAL_JOIN Compliance: Single spatial predicates in JOIN ON clauses
- Uses Stage 0 pre-filtered Grukos data for performance

Architectural Benefits:
- Identifies fields within groundwater protection zones
- Enables regulatory compliance analysis for pesticide/nitrate restrictions
- Creates clean geometric pipeline for Stage 3/4 consumption
"""

from typing import Any

from unified_pipeline.common.uuid_utils import LandbrugsdataUUID

from ..base import FieldAnalysisStageBase, FieldAnalysisStageConfig
from ..config import CONFIG


class FieldsGrukosCoverage(FieldAnalysisStageBase):
    """Create field × grukos intersection geometries for groundwater protection analysis."""

    def __init__(self, config: FieldAnalysisStageConfig = None):
        if config is None:
            config = FieldAnalysisStageConfig()
        super().__init__(config, "Stage 2C: Fields × Grukos Coverage")

    def _load_input_data(self):
        """Load foundation data from Stage 0 for geometric intersections."""
        # Set up UUID generation functions in DuckDB
        self.log.info("Setting up UUID generation functions...")
        LandbrugsdataUUID.setup_duckdb_functions(self.conn)

        updated_outputs = CONFIG.update_outputs_for_year()

        # Load agricultural fields (BUILD side for spatial joins)
        self._load_silver_dataset(
            CONFIG.get_agricultural_fields_dataset(), "agricultural_fields_raw"
        )

        # Generate field_uuid from geometry (exclude existing NULL column to avoid duplicate)
        self.log.info("Generating field_uuid from geometry...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE agricultural_fields AS
            SELECT
                * EXCLUDE (field_uuid),
                field_uuid(geometry) as field_uuid
            FROM agricultural_fields_raw
        """)

        # Load Stage 0 pre-filtered Grukos data
        self.log.info("Loading Stage 0 pre-filtered Grukos dataset...")
        stage0_grukos_dataset = updated_outputs["grukos_prefiltered"]
        stage0_grukos_path = self._get_latest_gold_path(stage0_grukos_dataset)
        self.storage.query_parquet_direct(
            stage0_grukos_path,
            "SELECT *",
            "grukos_prefiltered",
        )

        self.log.info("Loaded foundation data for Stage 2C")

        # Input validation
        field_count = self.conn.execute("SELECT COUNT(*) FROM agricultural_fields").fetchone()[0]
        grukos_count = self.conn.execute("SELECT COUNT(*) FROM grukos_prefiltered").fetchone()[0]

        self.log.info("Foundation data loaded:")
        self.log.info(f"  Fields: {field_count:,}")
        self.log.info(f"  Grukos areas (pre-filtered): {grukos_count:,}")

        if field_count == 0:
            raise ValueError("No agricultural fields found")
        if grukos_count == 0:
            self.log.warning("No Grukos areas found - pipeline will create empty result tables")

        return {
            "fields_count": field_count,
            "grukos_count": grukos_count,
        }

    async def _execute_stage_processing(self) -> dict[str, Any]:
        """
        Stage 2C: Create field × grukos intersection geometries.

        Creates output table:
        - field_grukos_intersections: Fields intersecting with groundwater protection zones

        Key Principles:
        - GEOMETRY ONLY: No ST_Area_Spheroid() calculations (moved to Stage 4)
        - SPATIAL_JOIN Compliance: Single spatial predicates in JOIN ON clauses
        """

        self.log.info("STAGE 2C: GRUKOS GEOMETRIC INTERSECTIONS")
        self.log.info("GEOMETRY ONLY - All area calculations moved to Stage 4")
        self.log.info("SPATIAL_JOIN Compliance - Single spatial predicates in JOIN ON")

        # Create field × grukos intersections
        self.log.info("Creating field_grukos_intersections...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_grukos_intersections AS
            SELECT
                f.field_uuid,
                f.field_id,
                f.block_id,
                f.cvr_number,
                f.year,
                g.grukos_id,
                ST_Intersection(f.geometry, g.geometry) as field_grukos_geometry
            FROM agricultural_fields f
            JOIN grukos_prefiltered g ON ST_Intersects(f.geometry, g.geometry)
        """)

        # Get result statistics
        total_grukos_intersections = self.conn.execute(
            "SELECT COUNT(*) FROM field_grukos_intersections"
        ).fetchone()[0]

        unique_fields_with_grukos = self.conn.execute(
            "SELECT COUNT(DISTINCT field_uuid) FROM field_grukos_intersections"
        ).fetchone()[0]

        # Validation: Geometry validity checks
        self._validate_geometric_output()

        self.log.info("STAGE 2C COMPLETE:")
        self.log.info(f"Total field-grukos intersections: {total_grukos_intersections:,}")
        self.log.info(f"Unique fields in grukos zones: {unique_fields_with_grukos:,}")
        self.log.info("GEOMETRIC PIPELINE: Ready for Stage 3/4 consumption")

        return {
            "total_grukos_intersections": total_grukos_intersections,
            "unique_fields_with_grukos": unique_fields_with_grukos,
        }

    def _save_output_data(self, result: dict[str, Any]):
        """Save field × grukos intersection table to cloud storage."""
        self._save_stage_output("field_grukos_intersections", "field_grukos_intersections")

    def _validate_geometric_output(self):
        """Comprehensive validation including geometry, record counts, and data consistency."""

        self.log.info("STAGE 2C: Running comprehensive validations...")

        # 1. GEOMETRY VALIDATIONS
        self._validate_geometry_quality()

        # 2. RECORD COUNT VALIDATIONS
        self._validate_record_counts()

        # 3. DATA CONSISTENCY VALIDATIONS
        self._validate_data_consistency()

        self.log.info("STAGE 2C: All comprehensive validations completed")

    def _validate_geometry_quality(self):
        """Validate geometry quality and integrity."""
        self.log.info("Validating geometry quality...")

        # Check field_grukos_intersections geometry validity
        invalid_grukos = self.conn.execute("""
            SELECT COUNT(*)
            FROM field_grukos_intersections
            WHERE field_grukos_geometry IS NULL OR NOT ST_IsValid(field_grukos_geometry)
        """).fetchone()[0]

        # Check for empty geometries
        empty_grukos = self.conn.execute("""
            SELECT COUNT(*)
            FROM field_grukos_intersections
            WHERE field_grukos_geometry IS NOT NULL AND ST_IsEmpty(field_grukos_geometry)
        """).fetchone()[0]

        # Report geometry validation results
        total_grukos = self.conn.execute(
            "SELECT COUNT(*) FROM field_grukos_intersections"
        ).fetchone()[0]

        if invalid_grukos > 0:
            self.log.error(
                f"Found {invalid_grukos:,}/{total_grukos:,} invalid/NULL geometries in "
                f"field_grukos_intersections"
            )
        else:
            self.log.info(f"All {total_grukos:,} field_grukos_intersections geometries are valid")

        if empty_grukos > 0:
            self.log.warning(
                f"Found {empty_grukos:,} empty geometries in field_grukos_intersections"
            )

    def _validate_record_counts(self):
        """Validate record counts and data preservation."""
        self.log.info("Validating record counts and data preservation...")

        # Get input counts
        fields_count = self.conn.execute("SELECT COUNT(*) FROM agricultural_fields").fetchone()[0]
        grukos_count = self.conn.execute("SELECT COUNT(*) FROM grukos_prefiltered").fetchone()[0]

        # Get output counts
        field_grukos_count = self.conn.execute(
            "SELECT COUNT(*) FROM field_grukos_intersections"
        ).fetchone()[0]

        # Validate field coverage
        unique_fields_with_grukos = self.conn.execute("""
            SELECT COUNT(DISTINCT field_uuid) FROM field_grukos_intersections
        """).fetchone()[0]

        # Log validation results
        self.log.info(f"Input data: {fields_count:,} fields, {grukos_count:,} grukos features")
        self.log.info(f"Output data: {field_grukos_count:,} field×grukos intersections")
        self.log.info(
            f"Field coverage: {unique_fields_with_grukos:,} fields in groundwater protection zones"
        )

        # Sanity checks
        if field_grukos_count == 0 and grukos_count > 0:
            self.log.warning("No field×grukos intersections produced despite grukos data existing!")

        if unique_fields_with_grukos > fields_count:
            self.log.error(
                f"CRITICAL: More unique fields with grukos ({unique_fields_with_grukos:,}) "
                f"than total fields ({fields_count:,})"
            )

    def _validate_data_consistency(self):
        """Validate data consistency and logical relationships."""
        self.log.info("Validating data consistency...")

        # Validate field UUIDs consistency
        invalid_field_uuids = self.conn.execute("""
            SELECT COUNT(*)
            FROM field_grukos_intersections fg
            LEFT JOIN agricultural_fields f ON fg.field_uuid = f.field_uuid
            WHERE f.field_uuid IS NULL
        """).fetchone()[0]

        if invalid_field_uuids > 0:
            self.log.error(
                f"CRITICAL: {invalid_field_uuids:,} intersection records have "
                f"invalid field_uuid references"
            )
        else:
            self.log.info("All intersection records have valid field_uuid references")

        # Validate no duplicate field×grukos combinations
        total_records = self.conn.execute(
            "SELECT COUNT(*) FROM field_grukos_intersections"
        ).fetchone()[0]
        unique_combinations = self.conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT DISTINCT field_uuid, grukos_id FROM field_grukos_intersections
            )
        """).fetchone()[0]
        duplicates = total_records - unique_combinations

        if duplicates > 0:
            self.log.error(f"CRITICAL: {duplicates:,} duplicate field×grukos combinations found")
        else:
            self.log.info("No duplicate field×grukos combinations found")

    def _get_main_output_table(self) -> str:
        """Get the name of the main output table for area validation."""
        return "field_grukos_intersections"
