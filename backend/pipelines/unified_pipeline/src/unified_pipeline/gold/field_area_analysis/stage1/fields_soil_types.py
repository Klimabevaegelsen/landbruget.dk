"""Stage 1D: Fields × Soil Types Base Intersection

Basic field analysis with soil type intersections.
Creates foundation dataset for environmental coverage calculations.

Optimized for DuckDB Spatial v1.2.2 with SPATIAL_JOIN operator.
Based on learnings from Stage 1C (Fields × Properties) success.
"""

from typing import Any, Dict

from ..base import FieldAnalysisStageBase, FieldAnalysisStageConfig
from ..config import CONFIG


class FieldsSoilTypesIntersection(FieldAnalysisStageBase):
    """Analyze agricultural fields with soil type intersections using SPATIAL_JOIN optimization."""

    def __init__(self, config: FieldAnalysisStageConfig = None):
        if config is None:
            config = FieldAnalysisStageConfig()
        super().__init__(config, "Stage 1D: Fields × Soil Types")

    def _load_input_data(self):
        """Load agricultural fields and soil types datasets."""
        # Load agricultural fields (600K fields)
        self._load_silver_dataset(CONFIG.agricultural_fields_dataset, "agricultural_fields")

        # Load soil types (13K polygons)
        self._load_silver_dataset(CONFIG.soil_types_dataset, "soil_types")

    async def _execute_stage_processing(self) -> Dict[str, Any]:
        """
        Create base field-soil intersections using batched processing for memory efficiency.

        Key optimizations:
        1. Process fields in batches to manage memory (600K fields is too large for single join)
        2. Single spatial predicate (ST_Intersects only) for SPATIAL_JOIN operator per batch
        3. Soil types (13K, smaller) as BUILD side, field batches as PROBE side
        4. Stream results to avoid memory accumulation
        """

        # Get total field count for batching
        total_fields = self.conn.execute("SELECT COUNT(*) FROM agricultural_fields").fetchone()[0]
        batch_size = (
            5000  # Further reduced for better temp file management (5K × 13K = 65M combinations)
        )
        num_batches = (total_fields + batch_size - 1) // batch_size

        self.log.info(
            f"Processing {total_fields:,} fields × 13K soil types in {num_batches} batches of {batch_size:,}"
        )

        # Additional memory optimizations for large spatial joins
        self.conn.execute("SET preserve_insertion_order=false")  # Reduce memory overhead
        self.conn.execute("SET threads=2")  # Reduce parallelism to save memory

        # Initialize result table
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_soil_detailed AS
            SELECT 
                CAST(NULL AS VARCHAR) as field_id,
                CAST(NULL AS VARCHAR) as block_id,
                CAST(NULL AS VARCHAR) as cvr_number,
                CAST(NULL AS INTEGER) as year,
                CAST(NULL AS GEOMETRY) as field_geometry,
                CAST(NULL AS DOUBLE) as field_area_m2,
                CAST(NULL AS VARCHAR) as soil_code,
                CAST(NULL AS VARCHAR) as soil_description,
                CAST(NULL AS VARCHAR) as soil_type_category,
                CAST(NULL AS DOUBLE) as soil_intersection_area_m2,
                CAST(NULL AS DOUBLE) as soil_area_share_pct
            WHERE FALSE
        """)

        total_intersections = 0
        total_meaningful = 0

        # Process each batch
        for batch_num in range(num_batches):
            offset = batch_num * batch_size
            self.log.info(f"Processing batch {batch_num + 1}/{num_batches} (offset: {offset:,})")

            # Create field batch
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE fields_batch AS
                SELECT 
                    field_id,
                    block_id,
                    cvr_number,
                    year,
                    geometry,
                    ST_Area_Spheroid(geometry) as field_area_m2
                FROM agricultural_fields
                LIMIT {batch_size} OFFSET {offset}
            """)

            batch_count = self.conn.execute("SELECT COUNT(*) FROM fields_batch").fetchone()[0]
            if batch_count == 0:
                break

            # Step 1: SPATIAL_JOIN for this batch - CORRECT ORDER: soil types (BUILD) × fields (PROBE)
            self.conn.execute("""
                CREATE OR REPLACE TABLE batch_spatial AS
                SELECT 
                    f.field_id,
                    f.block_id, 
                    f.cvr_number,
                    f.year,
                    f.geometry as field_geometry,
                    f.field_area_m2,
                    s.soil_code,
                    s.soil_description,
                    COALESCE(s.theme_name, 'Unknown') as soil_type_category,
                    s.geometry as soil_geometry
                FROM soil_types s  -- BUILD side (smaller dataset - gets spatial indexed)
                JOIN fields_batch f ON ST_Intersects(s.geometry, f.geometry)  -- PROBE side (larger dataset)
            """)

            batch_spatial_count = self.conn.execute(
                "SELECT COUNT(*) FROM batch_spatial"
            ).fetchone()[0]

            # Step 2: Calculate areas and apply filtering for this batch
            if batch_spatial_count > 0:
                self.conn.execute("""
                    CREATE OR REPLACE TABLE batch_detailed AS
                    SELECT 
                        field_id,
                        block_id,
                        cvr_number,
                        year,
                        field_geometry,
                        field_area_m2,
                        soil_code,
                        soil_description,
                        soil_type_category,
                        
                        -- Calculate intersection area
                        ST_Area_Spheroid(ST_Intersection(field_geometry, soil_geometry)) as soil_intersection_area_m2,
                        (ST_Area_Spheroid(ST_Intersection(field_geometry, soil_geometry)) / field_area_m2) * 100 as soil_area_share_pct
                        
                    FROM batch_spatial
                    WHERE 
                        -- Area filtering to remove noise (learned from Stage 1C)
                        ST_Area_Spheroid(ST_Intersection(field_geometry, soil_geometry)) > 100
                        AND (ST_Area_Spheroid(ST_Intersection(field_geometry, soil_geometry)) / field_area_m2) > 0.01
                """)

                batch_meaningful_count = self.conn.execute(
                    "SELECT COUNT(*) FROM batch_detailed"
                ).fetchone()[0]

                # Append to main result table
                if batch_meaningful_count > 0:
                    self.conn.execute("""
                        INSERT INTO field_soil_detailed 
                        SELECT * FROM batch_detailed
                    """)

                total_meaningful += batch_meaningful_count
            else:
                batch_meaningful_count = 0

            total_intersections += batch_spatial_count

            self.log.info(
                f"  ✅ Batch {batch_num + 1}: {batch_count:,} fields → {batch_spatial_count:,} intersections → {batch_meaningful_count:,} meaningful"
            )

            # Clean up batch tables
            self.conn.execute("DROP TABLE IF EXISTS fields_batch")
            self.conn.execute("DROP TABLE IF EXISTS batch_spatial")
            self.conn.execute("DROP TABLE IF EXISTS batch_detailed")

            # Memory cleanup every 5 batches
            if (batch_num + 1) % 5 == 0:
                import gc

                gc.collect()
                self.log.info(f"  🧹 Memory cleanup after batch {batch_num + 1}")

        spatial_count = total_intersections
        detailed_count = total_meaningful
        filtered_out = spatial_count - detailed_count

        self.log.info("✅ Batched processing completed:")
        self.log.info(f"   Total spatial intersections: {spatial_count:,}")
        self.log.info(f"   Meaningful intersections: {detailed_count:,}")
        self.log.info(f"   Noise filtered: {filtered_out:,}")

        self.log.info("Step 2: Creating simplified soil areas per field...")

        # Create simplified table with just area per soil type per field
        final_query = """
        CREATE OR REPLACE TABLE field_soil_areas AS
        SELECT 
            field_id,
            block_id,
            cvr_number,
            year,
            soil_code,
            soil_description,
            soil_type_category,
            soil_intersection_area_m2 as soil_area_m2,
            soil_area_share_pct,
            field_area_m2
        FROM field_soil_detailed
        ORDER BY field_id, block_id, cvr_number, soil_intersection_area_m2 DESC
        """

        self.conn.execute(final_query)

        # Get comprehensive statistics
        final_count = self.conn.execute("SELECT COUNT(*) FROM field_soil_areas").fetchone()[0]

        # Get soil type statistics
        soil_stats = self.conn.execute("""
            SELECT 
                soil_type_category,
                COUNT(*) as intersection_count,
                COUNT(DISTINCT field_id || '-' || block_id || '-' || cvr_number) as field_count,
                SUM(soil_area_m2) / 1000000 as total_area_km2,
                AVG(soil_area_share_pct) as avg_coverage_pct
            FROM field_soil_areas
            GROUP BY soil_type_category
            ORDER BY field_count DESC
        """).fetchall()

        # Get coverage statistics
        coverage_stats = self.conn.execute("""
            SELECT 
                COUNT(*) as total_intersections,
                COUNT(DISTINCT field_id || '-' || block_id || '-' || cvr_number) as fields_with_soil,
                AVG(soil_area_share_pct) as avg_soil_coverage,
                COUNT(DISTINCT soil_code) as unique_soil_types
            FROM field_soil_areas
        """).fetchone()

        total_intersections, fields_with_soil, avg_coverage, unique_soil_types = coverage_stats

        self.log.info("✅ Created simplified field soil areas:")
        self.log.info(f"   Total field-soil intersections: {total_intersections:,}")
        self.log.info(f"   Fields with soil data: {fields_with_soil:,}")
        self.log.info(f"   Average soil coverage: {avg_coverage:.1f}%")
        self.log.info(f"   Unique soil types: {unique_soil_types}")

        self.log.info("   Top soil categories by field count:")
        for soil_category, intersection_count, field_count, area_km2, avg_pct in soil_stats[:5]:
            self.log.info(
                f"     {soil_category}: {field_count:,} fields, {intersection_count:,} intersections, {area_km2:.1f} km², {avg_pct:.1f}% avg coverage"
            )

        # Clean up intermediate table to save memory
        self.conn.execute("DROP TABLE IF EXISTS field_soil_detailed")

        return {
            "total_intersections": total_intersections,
            "fields_with_soil": fields_with_soil,
            "avg_soil_coverage": avg_coverage,
            "unique_soil_types": unique_soil_types,
            "soil_category_stats": soil_stats,
            "spatial_intersections": spatial_count,
            "meaningful_intersections": detailed_count,
            "noise_filtered": filtered_out,
        }

    def _save_output_data(self, result: Dict[str, Any]):
        """Save field soil areas directly to GCS as parquet file."""
        import os
        import tempfile
        from datetime import datetime

        try:
            # Create timestamp and GCS path following the standard pattern
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            dataset_name = "field_soil_areas"
            filename = f"{dataset_name}.parquet"
            gcs_path = f"gold/{dataset_name}/{timestamp}/{filename}"

            # Create temporary file for export
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
                temp_path = tmp_file.name

            # Export table to temporary file using DuckDB COPY
            self.conn.execute(f"""
                COPY field_soil_areas TO '{temp_path}' 
                (FORMAT PARQUET, COMPRESSION zstd, ROW_GROUP_SIZE 100000)
            """)

            # Upload to GCS using gcs_access
            full_gcs_path = f"gs://{CONFIG.bucket}/{gcs_path}"

            # Use gcs_access fs to upload
            with open(temp_path, "rb") as src:
                with self.gcs_access.fs.open(full_gcs_path, "wb") as dst:
                    import shutil

                    shutil.copyfileobj(src, dst)

            # Clean up temporary file
            if os.path.exists(temp_path):
                os.unlink(temp_path)

            self.log.info(f"✅ Saved field soil areas to {full_gcs_path}")

        except Exception as e:
            self.log.error(f"❌ Failed to save field soil areas: {e}")
            # Clean up temp file on error
            if "temp_path" in locals() and os.path.exists(temp_path):
                os.unlink(temp_path)
            raise
