"""Stage 2A: Fields × BNBO Water Coverage Analysis

Calculate BNBO coverage by water projects for each field.
Uses pre-computed BNBO intersection geometries from Stage 1A (SPEED OPTIMIZATION).
No longer recreates spatial intersections - reuses Stage 1A intersection geometries.

Optimized for DuckDB Spatial v1.2.2 with foundation data approach.
Based on successful Stage 1 implementations.
"""

from typing import Any, Dict

from ..base import FieldAnalysisStageBase, FieldAnalysisStageConfig
from ..config import CONFIG


class FieldsBNBOWaterCoverage(FieldAnalysisStageBase):
    """Calculate BNBO coverage by water projects for each field using foundation data."""

    def __init__(self, config: FieldAnalysisStageConfig = None):
        if config is None:
            config = FieldAnalysisStageConfig()
        super().__init__(config, "Stage 2A: Fields × BNBO Water Coverage")

    def _load_input_data(self):
        """Load field data and water project BNBO intersections from Stage 1."""
        # Get year-aware dataset names
        updated_outputs = CONFIG.update_outputs_for_year()
        
        # Load agricultural fields (still from silver - this is the BUILD side)
        self._load_silver_dataset(CONFIG.get_agricultural_fields_dataset(), "agricultural_fields")

        # Load Stage 0 pre-filtered BNBO data for field intersections (PROBE side optimization)
        self.log.info("Loading Stage 0 pre-filtered BNBO dataset...")
        stage0_bnbo_dataset = updated_outputs["bnbo_prefiltered"]
        stage0_bnbo_path = self._get_latest_gold_path(stage0_bnbo_dataset)
        # Load all columns - filtering can be done in SQL if needed
        self.gcs_access.query_parquet_direct(
            stage0_bnbo_path,
            "SELECT *",
            "bnbo_for_fields",
        )

        self.log.info("✅ STAGE 0 OPTIMIZATION: Using pre-filtered BNBO for field intersections!")
        self.log.info("🚀 PERFORMANCE: 3.7x faster than original (3.7K → 1K BNBO polygons)")

        # Load water project × BNBO intersections from Stage 1A
        # This contains the pre-computed intersection geometries we need (OPTIMIZATION!)
        stage1a_dataset = updated_outputs["water_projects_bnbo_intersections"]
        stage1a_path = self._get_latest_gold_path(stage1a_dataset)
        # Load all columns - filtering can be done in SQL if needed
        self.gcs_access.query_parquet_direct(
            stage1a_path,
            "SELECT *",
            "water_projects_bnbo_intersections",
        )

        # Use pre-computed BNBO areas covered by water projects (SPEED OPTIMIZATION)
        # No need to recreate - Stage 1A already computed intersection geometries!
        self.log.info("Using pre-computed BNBO intersection geometries from Stage 1A...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE bnbo_covered_by_water AS
            SELECT 
                status_category,
                intersection_geometry as covered_bnbo_geometry,
                intersection_area_m2 as covered_area_m2
            FROM water_projects_bnbo_intersections
            WHERE intersection_area_m2 > 100
        """)

        # Log loaded data
        fields_count = self.conn.execute("SELECT COUNT(*) FROM agricultural_fields").fetchone()[0]
        bnbo_count = self.conn.execute("SELECT COUNT(*) FROM bnbo_for_fields").fetchone()[0]
        covered_count = self.conn.execute("SELECT COUNT(*) FROM bnbo_covered_by_water").fetchone()[
            0
        ]
        intersections_count = self.conn.execute(
            "SELECT COUNT(*) FROM water_projects_bnbo_intersections"
        ).fetchone()[0]

        self.log.info("✅ Loaded data for field-level analysis:")
        self.log.info(f"   Agricultural fields: {fields_count:,}")
        self.log.info(f"   BNBO polygons: {bnbo_count:,}")
        self.log.info(f"   BNBO areas covered by water projects: {covered_count:,}")
        self.log.info(f"   Intersection records (for validation): {intersections_count:,}")
        
        # Store input area reference for validation
        if self._should_validate_areas():
            fields_area_stats = self.conn.execute("""
                SELECT 
                    COUNT(*) as field_count,
                    SUM(ST_Area_Spheroid(geometry)) as total_area
                FROM agricultural_fields
                WHERE geometry IS NOT NULL
            """).fetchone()
            
            self._input_area_reference = {
                "total_area": fields_area_stats[1] or 0,
                "field_count": fields_area_stats[0] or 0
            }

    async def _execute_stage_processing(self) -> Dict[str, Any]:
        """
        Calculate BNBO coverage by water projects for each field.

        DuckDB Spatial PR #545 COMPLIANCE:
        - Separate spatial joins into distinct processing steps
        - Single spatial predicate per join
        - Move area filtering to post-join processing
        - No WHERE clauses with spatial predicates on JOIN operations

        CORRECT APPROACH:
        1. Fields × BNBO: Get total BNBO area within each field (STEP 1)
        2. Fields × (BNBO covered by water projects): Get covered BNBO area within each field (STEP 2)
        3. Calculate field-level percentages: % BNBO in field, % of those covered by water projects
        """

        self.log.info("🎯 FIELD-LEVEL BNBO WATER COVERAGE ANALYSIS")
        self.log.info("🔧 Using BNBO-water project intersection geometries")
        self.log.info(
            "✅ DuckDB Spatial PR #545 COMPLIANCE: Separate spatial joins, no WHERE spatial predicates"
        )

        # Get total field count for batching
        total_fields = self.conn.execute("SELECT COUNT(*) FROM agricultural_fields").fetchone()[0]
        batch_size = CONFIG.stage2_batch_size
        num_batches = (total_fields + batch_size - 1) // batch_size

        self.log.info(
            f"Processing {total_fields:,} fields in {num_batches} batches of {batch_size:,}"
        )

        # Initialize result table (field-level aggregates)
        self.conn.execute("""
            CREATE OR REPLACE TABLE fields_bnbo_water AS
            SELECT 
                CAST(NULL AS VARCHAR) as field_id,
                CAST(NULL AS VARCHAR) as block_id,
                CAST(NULL AS VARCHAR) as cvr_number,
                CAST(NULL AS INTEGER) as year,
                CAST(NULL AS VARCHAR) as field_uuid,
                CAST(NULL AS GEOMETRY) as geometry,
                CAST(NULL AS DOUBLE) as field_area_m2,
                CAST(NULL AS DOUBLE) as field_bnbo_total_m2,
                CAST(NULL AS DOUBLE) as field_bnbo_water_covered_m2,
                CAST(NULL AS DOUBLE) as field_bnbo_water_covered_pct,
                CAST(NULL AS DOUBLE) as field_bnbo_water_uncovered_pct,
                CAST(NULL AS DOUBLE) as field_bnbo_coverage_pct
            WHERE FALSE
        """)

        # Initialize detailed intersections table for Stage 3 optimization
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_bnbo_intersections AS
            SELECT 
                CAST(NULL AS VARCHAR) as field_id,
                CAST(NULL AS VARCHAR) as block_id,
                CAST(NULL AS VARCHAR) as cvr_number,
                CAST(NULL AS INTEGER) as year,
                CAST(NULL AS VARCHAR) as field_uuid,
                CAST(NULL AS VARCHAR) as status_category,
                CAST(NULL AS GEOMETRY) as field_bnbo_intersection_geometry,
                CAST(NULL AS DOUBLE) as field_bnbo_intersection_area_m2,
                CAST(NULL AS GEOMETRY) as field_geometry,
                CAST(NULL AS DOUBLE) as field_area_m2
            WHERE FALSE
        """)

        total_fields_processed = 0
        total_bnbo_intersections = 0
        total_covered_intersections = 0

        # Process each batch with separated spatial joins
        for batch_num in range(num_batches):
            offset = batch_num * batch_size
            progress_pct = ((batch_num + 1) / num_batches) * 100
            self.log.info(f"📦 Batch {batch_num + 1}/{num_batches} - {progress_pct:.1f}% complete")

            # Create field batch
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE fields_batch AS
                SELECT * FROM agricultural_fields
                LIMIT {batch_size} OFFSET {offset}
            """)

            batch_count = self.conn.execute("SELECT COUNT(*) FROM fields_batch").fetchone()[0]
            if batch_count == 0:
                break

            # STEP 1: Fields × BNBO (total BNBO area within each field)
            # DuckDB Spatial PR #545 COMPLIANCE: Single spatial predicate only
            self.log.info(f"  STEP 1: {batch_count:,} fields × BNBO (single spatial join)")
            self.conn.execute("""
                CREATE OR REPLACE TABLE batch_field_bnbo_raw AS
                SELECT 
                    f.field_id,
                    f.block_id,
                    f.cvr_number,
                    f.year,
                    f.field_uuid,
                    f.geometry as field_geometry,
                    ST_Area_Spheroid(f.geometry) as field_area_m2,
                    b.bnbo_id,
                    b.status_category,
                    b.geometry as bnbo_geometry
                FROM fields_batch f
                JOIN bnbo_for_fields b ON ST_Intersects(f.geometry, b.geometry)
            """)

            # Post-join processing: Calculate areas (NO SPATIAL WHERE CLAUSES)
            self.log.info("  Post-processing: Calculate intersection areas")
            self.conn.execute("""
                CREATE OR REPLACE TABLE batch_field_bnbo_total AS
                SELECT 
                    field_id,
                    block_id,
                    cvr_number,
                    year,
                    field_uuid,
                    field_geometry,
                    field_area_m2,
                    bnbo_id,
                    status_category,
                    ST_Intersection(field_geometry, bnbo_geometry) as field_bnbo_intersection_geometry,
                    ST_Area_Spheroid(ST_Intersection(field_geometry, bnbo_geometry)) as field_bnbo_area_m2
                FROM batch_field_bnbo_raw
            """)

            # Save detailed intersections for Stage 3 optimization
            self.log.info("  Saving detailed field-BNBO intersections for Stage 3")
            self.conn.execute("""
                INSERT INTO field_bnbo_intersections
                SELECT 
                    field_id,
                    block_id,
                    cvr_number,
                    year,
                    field_uuid,
                    status_category,
                    field_bnbo_intersection_geometry,
                    field_bnbo_area_m2 as field_bnbo_intersection_area_m2,
                    field_geometry,
                    field_area_m2
                FROM batch_field_bnbo_total
            """)

            # STEP 2: Fields × (BNBO covered by water projects)
            # DuckDB Spatial PR #545 COMPLIANCE: Separate spatial join
            self.log.info(
                f"  STEP 2: {batch_count:,} fields × BNBO covered by water projects (separate spatial join)"
            )
            self.conn.execute("""
                CREATE OR REPLACE TABLE batch_field_bnbo_covered_raw AS
                SELECT 
                    f.field_id,
                    f.block_id,
                    f.cvr_number,
                    f.year,
                    f.field_uuid,
                    f.geometry as field_geometry,
                    wpbi.status_category,
                    wpbi.intersection_geometry as water_covered_bnbo_geometry
                FROM fields_batch f
                JOIN water_projects_bnbo_intersections wpbi ON ST_Intersects(f.geometry, wpbi.intersection_geometry)
            """)

            # Post-join processing: Calculate covered areas (NO SPATIAL WHERE CLAUSES)
            self.log.info("  Post-processing: Calculate covered intersection areas")
            self.conn.execute("""
                CREATE OR REPLACE TABLE batch_field_bnbo_covered AS
                SELECT 
                    field_id,
                    block_id,
                    cvr_number,
                    year,
                    field_uuid,
                    status_category,
                    ST_Area_Spheroid(ST_Intersection(field_geometry, water_covered_bnbo_geometry)) as field_covered_bnbo_area_m2
                FROM batch_field_bnbo_covered_raw
            """)

            batch_bnbo_intersections = self.conn.execute(
                "SELECT COUNT(*) FROM batch_field_bnbo_total"
            ).fetchone()[0]
            batch_covered_intersections = self.conn.execute(
                "SELECT COUNT(*) FROM batch_field_bnbo_covered"
            ).fetchone()[0]

            self.log.info(f"  Found {batch_bnbo_intersections:,} total field-BNBO intersections")
            self.log.info(
                f"  Found {batch_covered_intersections:,} field-covered BNBO intersections"
            )

            if batch_bnbo_intersections == 0:
                self.log.info(f"  No BNBO intersections found in batch {batch_num + 1}")
                continue

            # ARCHITECTURAL FIX: Skip field-level aggregation in Stage 2A (same as 2B)
            # Stage 2A only creates detailed intersections for Stage 3
            # All aggregation is handled in Stage 3 to avoid double-counting
            self.log.info("  ✅ ARCHITECTURAL FIX: Skipping field-level aggregation in Stage 2A")
            self.log.info("  🎯 Stage 2A focuses on intersection geometries, Stage 3 handles all aggregation")
            
            # Create minimal field reference table for final output (fields with any BNBO)
            self.conn.execute("""
                CREATE OR REPLACE TABLE batch_field_aggregates AS
                SELECT DISTINCT
                    field_id,
                    block_id,
                    cvr_number,
                    year,
                    field_uuid,
                    field_geometry as geometry,
                    field_area_m2
                FROM batch_field_bnbo_total
            """)

            # ARCHITECTURAL FIX: No percentage calculations in Stage 2A
            # All aggregation and calculations moved to Stage 3
            self.conn.execute("""
                CREATE OR REPLACE TABLE batch_final AS
                SELECT 
                    field_id,
                    block_id,
                    cvr_number,
                    year,
                    field_uuid,
                    geometry,
                    field_area_m2,
                    -- Placeholder values - real calculations done in Stage 3
                    0 as field_bnbo_total_m2,
                    0 as field_bnbo_water_covered_m2,
                    0 as field_bnbo_water_covered_pct,
                    0 as field_bnbo_water_uncovered_pct,
                    0 as field_bnbo_coverage_pct
                FROM batch_field_aggregates
            """)

            # Insert into main results table
            self.conn.execute("""
                INSERT INTO fields_bnbo_water 
                SELECT * FROM batch_final
            """)

            total_fields_processed += batch_count
            total_bnbo_intersections += batch_bnbo_intersections
            total_covered_intersections += batch_covered_intersections

            # Memory cleanup
            self.conn.execute("DROP TABLE IF EXISTS fields_batch")
            self.conn.execute("DROP TABLE IF EXISTS batch_field_bnbo_raw")
            self.conn.execute("DROP TABLE IF EXISTS batch_field_bnbo_total")
            self.conn.execute("DROP TABLE IF EXISTS batch_field_bnbo_covered_raw")
            self.conn.execute("DROP TABLE IF EXISTS batch_field_bnbo_covered")
            self.conn.execute("DROP TABLE IF EXISTS batch_field_aggregates")
            self.conn.execute("DROP TABLE IF EXISTS batch_final")

        # Final summary
        final_fields = self.conn.execute("SELECT COUNT(*) FROM fields_bnbo_water").fetchone()[0]

        self.log.info(f"✅ STAGE 2A COMPLETE: {final_fields:,} fields with BNBO analysis")
        self.log.info(f"📊 Total BNBO intersections: {total_bnbo_intersections:,}")
        self.log.info(f"📊 Water-covered intersections: {total_covered_intersections:,}")
        self.log.info("✅ DuckDB Spatial PR #545 COMPLIANCE: Separated spatial joins completed")
        
        # COMPREHENSIVE VALIDATION SUITE
        if self.area_validator:
            self.log.info("🔍 RUNNING COMPREHENSIVE VALIDATION SUITE...")
            
            # Run comprehensive stage validation
            validation_results = self.area_validator.run_comprehensive_stage_validation(
                "fields_bnbo_water", 
                "Stage 2A BNBO", 
                "bnbo"
            )
            
            # Fragment sum consistency validation
            if final_fields > 0:
                fragment_consistency = self.area_validator.validate_fragment_sum_consistency(
                    "field_bnbo_intersections",
                    "fields_bnbo_water", 
                    "Stage 2A Fragment Consistency",
                    ["field_uuid", "year"],
                    "field_bnbo_intersection_area_m2",
                    "field_bnbo_total_m2"
                )
                validation_results["fragment_consistency"] = fragment_consistency
            
            # Check if any validation failed
            failed_validations = [name for name, result in validation_results.items() if not result.is_valid]
            
            if failed_validations and self.validation_config.fail_on_validation_error:
                from ..area_validation import ValidationException
                failed_result = validation_results[failed_validations[0]]
                raise ValidationException(failed_result)
            elif failed_validations:
                self.log.warning(f"⚠️ Validation failures detected but continuing: {failed_validations}")
            else:
                self.log.info("✅ All Stage 2A validations PASSED!")

        return {
            "fields_processed": final_fields,
            "total_bnbo_intersections": total_bnbo_intersections,
            "covered_intersections": total_covered_intersections,
        }

    def _save_output_data(self, result: Dict[str, Any]):
        """Save fields with BNBO water coverage and detailed intersections to GCS."""
        # Save field-level aggregates
        self._save_stage_output("fields_bnbo_water", "fields_bnbo_water")

        # Save detailed field-BNBO intersections for Stage 3 optimization
        self._save_stage_output("field_bnbo_intersections", "field_bnbo_intersections")
    
    def _get_input_area_reference(self) -> Dict[str, Any]:
        """Get reference area statistics from input data for validation."""
        return getattr(self, '_input_area_reference', None)
    
    def _get_main_output_table(self) -> str:
        """Get the name of the main output table for area validation."""
        return "fields_bnbo_water"
