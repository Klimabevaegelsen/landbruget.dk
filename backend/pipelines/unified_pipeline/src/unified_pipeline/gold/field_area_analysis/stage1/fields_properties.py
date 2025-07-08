"""Stage 1C: Fields × Properties Pre-filtering

Pre-filter properties that intersect with any agricultural field and stream intersection geometries.
This provides massive size reduction (6.5M → ~2M properties) for later stages.
Streams intersection geometries for downstream spatial analysis of water projects/wetlands within properties.

Optimized for DuckDB Spatial v1.2.2 SPATIAL_JOIN operator with single spatial predicates.
"""

import time
from typing import Any, Dict

from ..base import FieldAnalysisStageBase, FieldAnalysisStageConfig
from ..config import CONFIG


class FieldsPropertiesIntersection(FieldAnalysisStageBase):
    """Pre-filter properties that intersect with agricultural fields and stream intersection geometries."""

    def __init__(self, config: FieldAnalysisStageConfig = None):
        if config is None:
            config = FieldAnalysisStageConfig()
        super().__init__(config, "Stage 1C: Fields × Properties Intersection & Streaming")

    def _load_input_data(self):
        """Load agricultural fields and properties datasets."""
        # Load agricultural fields (600K fields) - this will be our BUILD side
        self.log.info("Loading agricultural fields dataset...")
        self._load_silver_dataset(CONFIG.agricultural_fields_dataset, "agricultural_fields_full")

        # Load properties (6.5M properties - this is the large dataset) - PROBE side
        self.log.info("Loading properties dataset...")
        self._load_silver_dataset(CONFIG.properties_dataset, "properties_full")

        # SPATIAL_JOIN optimization: Fields (smaller, 617K) as BUILD side, Properties (larger, 6.5M) as PROBE side
        # This creates a spatial index on fields and probes with properties

        # Configure DuckDB for chunked spatial processing (optimized for memory efficiency)
        self.log.info("Configuring DuckDB for chunked spatial processing...")
        self.conn.execute(
            "SET max_temp_directory_size='25GB'"
        )  # Increased for chunked spatial operations
        self.conn.execute("SET preserve_insertion_order=false")  # Disable for better performance
        self.conn.execute(
            "SET threads=2"
        )  # Reduce threads to save memory during chunked processing

        self.log.info("Preparing fields as BUILD side (spatial index will be created)...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE agricultural_fields AS
            SELECT 
                field_id,
                block_id,
                cvr_number,
                year,
                geometry,
                ST_Area_Spheroid(geometry) as field_area_m2
            FROM agricultural_fields_full
        """)

        self.log.info("Preparing properties as PROBE side...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE properties AS
            SELECT 
                bestemtFastEjendomBFENr,
                geometry,
                ST_Area_Spheroid(geometry) as property_area_m2
            FROM properties_full
        """)

        # Log final table sizes
        fields_count = self.conn.execute("SELECT COUNT(*) FROM agricultural_fields").fetchone()[0]
        properties_count = self.conn.execute("SELECT COUNT(*) FROM properties").fetchone()[0]
        self.log.info(
            f"✅ Loaded {fields_count:,} fields (BUILD) and {properties_count:,} properties (PROBE)"
        )
        self.log.info("🚀 Ready for SPATIAL_JOIN operator with spatial indexing")

    async def _execute_stage_processing(self) -> Dict[str, Any]:
        """
        Pre-filter properties intersecting with fields using chunked processing to avoid memory overflow.

        Key optimizations for DuckDB Spatial v1.2.2:
        1. Process properties in chunks to manage memory (6.5M properties is too large for single join)
        2. Fields (smaller, 617K) as BUILD side - gets spatial indexed per chunk
        3. Properties chunks as PROBE side - processed incrementally
        4. Stream results to avoid memory accumulation
        5. STREAM intersection geometries for later water project/wetland spatial analysis
        """

        # Get total property count for chunking
        total_properties = self.conn.execute("SELECT COUNT(*) FROM properties").fetchone()[0]
        chunk_size = 500000  # Process 500K properties at a time to manage memory
        num_chunks = (total_properties + chunk_size - 1) // chunk_size

        self.log.info(
            f"Processing {total_properties:,} properties in {num_chunks} chunks of {chunk_size:,}"
        )

        # Initialize result table for streaming
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_property_intersections AS
            SELECT 
                CAST(NULL AS VARCHAR) as field_id,
                CAST(NULL AS VARCHAR) as block_id,
                CAST(NULL AS VARCHAR) as cvr_number,
                CAST(NULL AS INTEGER) as year,
                CAST(NULL AS DOUBLE) as field_area_m2,
                CAST(NULL AS VARCHAR) as bfe_number,
                CAST(NULL AS DOUBLE) as property_area_m2,
                CAST(NULL AS DOUBLE) as intersection_area_m2,
                CAST(NULL AS DOUBLE) as field_area_share_pct,
                CAST(NULL AS DOUBLE) as property_area_share_pct,
                CAST(NULL AS GEOMETRY) as field_geometry,
                CAST(NULL AS GEOMETRY) as property_geometry,
                CAST(NULL AS GEOMETRY) as intersection_geometry
            WHERE FALSE
        """)

        total_intersections = 0
        total_time = 0

        # Process each chunk
        for chunk_num in range(num_chunks):
            offset = chunk_num * chunk_size
            progress_pct = ((chunk_num + 1) / num_chunks) * 100
            self.log.info(
                f"Processing chunk {chunk_num + 1}/{num_chunks} (offset: {offset:,}) - {progress_pct:.1f}% complete"
            )

            chunk_start = time.time()

            # Create property chunk
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE properties_chunk AS
                SELECT 
                    bestemtFastEjendomBFENr,
                    geometry,
                    ST_Area_Spheroid(geometry) as property_area_m2
                FROM properties
                LIMIT {chunk_size} OFFSET {offset}
            """)

            chunk_count = self.conn.execute("SELECT COUNT(*) FROM properties_chunk").fetchone()[0]
            if chunk_count == 0:
                break

            self.log.info(f"  Processing {chunk_count:,} properties in chunk {chunk_num + 1}")

            # Step 1: SPATIAL_JOIN for this chunk - Fields (BUILD) × Properties chunk (PROBE)
            self.log.info(f"  Chunk {chunk_num + 1}: SPATIAL_JOIN with ST_Intersects...")
            self.conn.execute("""
                CREATE OR REPLACE TABLE chunk_raw_intersections AS
                SELECT 
                    f.field_id,
                    f.block_id,
                    f.cvr_number,
                    f.year,
                    f.field_area_m2,
                    p.bestemtFastEjendomBFENr as bfe_number,
                    p.property_area_m2,
                    f.geometry as field_geometry,
                    p.geometry as property_geometry
                FROM agricultural_fields f
                JOIN properties_chunk p ON ST_Intersects(f.geometry, p.geometry)
            """)

            chunk_raw_count = self.conn.execute(
                "SELECT COUNT(*) FROM chunk_raw_intersections"
            ).fetchone()[0]
            self.log.info(f"  Chunk {chunk_num + 1}: Found {chunk_raw_count:,} raw intersections")

            # Step 2: Calculate areas, apply filters, and add geometries in one step
            if chunk_raw_count > 0:
                self.log.info(
                    f"  Chunk {chunk_num + 1}: Calculating areas and streaming geometries..."
                )
                self.conn.execute("""
                    CREATE OR REPLACE TABLE chunk_final_intersections AS
                    SELECT 
                        field_id,
                        block_id,
                        cvr_number,
                        year,
                        field_area_m2,
                        bfe_number,
                        property_area_m2,
                        
                        -- Calculate intersection area and percentages
                        ST_Area_Spheroid(ST_Intersection(field_geometry, property_geometry)) as intersection_area_m2,
                        (ST_Area_Spheroid(ST_Intersection(field_geometry, property_geometry)) / field_area_m2) * 100 as field_area_share_pct,
                        (ST_Area_Spheroid(ST_Intersection(field_geometry, property_geometry)) / property_area_m2) * 100 as property_area_share_pct,
                        
                        -- STREAM intersection geometries for downstream spatial analysis
                        field_geometry,
                        property_geometry,
                        ST_Intersection(field_geometry, property_geometry) as intersection_geometry
                        
                    FROM chunk_raw_intersections
                    WHERE 
                        -- Filter out tiny intersections (< 1% of field area or < 100 m²)
                        ST_Area_Spheroid(ST_Intersection(field_geometry, property_geometry)) > 100
                        AND (ST_Area_Spheroid(ST_Intersection(field_geometry, property_geometry)) / field_area_m2) > 0.01
                """)

                chunk_final_count = self.conn.execute(
                    "SELECT COUNT(*) FROM chunk_final_intersections"
                ).fetchone()[0]
                self.log.info(
                    f"  Chunk {chunk_num + 1}: {chunk_final_count:,} valid intersections after filtering"
                )

                # Stream this chunk to the final result table
                if chunk_final_count > 0:
                    self.conn.execute("""
                        INSERT INTO field_property_intersections 
                        SELECT * FROM chunk_final_intersections
                    """)

                total_intersections += chunk_final_count
            else:
                chunk_final_count = 0

            # Clean up chunk tables to save memory
            self.conn.execute("DROP TABLE IF EXISTS properties_chunk")
            self.conn.execute("DROP TABLE IF EXISTS chunk_raw_intersections")
            self.conn.execute("DROP TABLE IF EXISTS chunk_final_intersections")

            chunk_time = time.time() - chunk_start
            total_time += chunk_time

            # Check memory usage
            try:
                memory_info = self.conn.execute("PRAGMA memory_usage").fetchone()
                memory_str = f", memory: {memory_info[0]}" if memory_info else ""
            except:
                memory_str = ""

            self.log.info(
                f"  Chunk {chunk_num + 1}: Completed in {chunk_time:.1f}s, total intersections: {total_intersections:,}{memory_str}"
            )

        # Get final statistics
        self.log.info("Calculating final statistics...")

        final_count = self.conn.execute(
            "SELECT COUNT(*) FROM field_property_intersections"
        ).fetchone()[0]

        if final_count > 0:
            stats = self.conn.execute("""
                SELECT 
                    COUNT(*) as total_intersections,
                    COUNT(DISTINCT bfe_number) as unique_properties,
                    COUNT(DISTINCT field_id || '_' || block_id || '_' || cvr_number) as unique_fields,
                    AVG(field_area_share_pct) as avg_field_share_pct,
                    AVG(property_area_share_pct) as avg_property_share_pct,
                    AVG(intersection_area_m2) as avg_intersection_area_m2
                FROM field_property_intersections
            """).fetchone()

            (
                total_intersections,
                unique_props,
                unique_flds,
                avg_field_share,
                avg_prop_share,
                avg_intersection,
            ) = stats

            # Calculate size reduction
            original_properties = self.conn.execute("SELECT COUNT(*) FROM properties").fetchone()[0]
            reduction_pct = ((original_properties - unique_props) / original_properties) * 100

            self.log.info("✅ Chunked processing optimization results:")
            self.log.info(f"   Total processing time: {total_time:.1f}s")
            self.log.info(f"   Total intersections: {total_intersections:,}")
            self.log.info(f"   Unique properties: {unique_props:,}")
            self.log.info(f"   Unique fields: {unique_flds:,}")
            self.log.info(
                f"   Size reduction: {reduction_pct:.1f}% ({original_properties:,} → {unique_props:,} properties)"
            )
            self.log.info(f"   Average field area share: {avg_field_share:.1f}%")
            self.log.info(f"   Average property area share: {avg_prop_share:.1f}%")
            self.log.info(f"   Average intersection area: {avg_intersection:.0f} m²")
            self.log.info("🚀 Chunked spatial processing successfully completed!")
            self.log.info(
                "📤 Streaming intersection geometries for downstream water project/wetland analysis"
            )

            return {
                "total_intersections": total_intersections,
                "unique_properties": unique_props,
                "unique_fields": unique_flds,
                "size_reduction_pct": reduction_pct,
                "avg_field_share_pct": avg_field_share,
                "avg_property_share_pct": avg_prop_share,
                "total_time_s": total_time,
                "chunks_processed": num_chunks,
            }
        else:
            self.log.warning("No intersections found after processing all chunks")
            return {
                "total_intersections": 0,
                "unique_properties": 0,
                "unique_fields": 0,
                "size_reduction_pct": 0,
                "avg_field_share_pct": 0,
                "avg_property_share_pct": 0,
                "total_time_s": total_time,
                "chunks_processed": num_chunks,
            }

    def _save_output_data(self, result: Dict[str, Any]):
        """Save pre-filtered field-property intersections with streaming geometries to GCS."""
        self._save_stage_output("field_property_intersections", "field_property_intersections")
