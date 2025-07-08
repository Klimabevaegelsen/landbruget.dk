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

        # Configure DuckDB for streaming geometries (requires more temp space)
        self.log.info("Configuring DuckDB for streaming geometry processing...")
        self.conn.execute("SET max_temp_directory_size='20GB'")  # Increased for geometries
        self.conn.execute("SET preserve_insertion_order=false")  # Disable for better performance
        self.conn.execute("SET threads=3")  # Reduce threads to save memory

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
        Pre-filter properties intersecting with fields and stream intersection geometries for downstream analysis.

        Key optimizations for DuckDB Spatial v1.2.2:
        1. SINGLE spatial predicate only (ST_Intersects) - required for SPATIAL_JOIN
        2. Fields (smaller, 617K) as BUILD side - gets spatial indexed
        3. Properties (larger, 6.5M) as PROBE side - gets filtered by spatial index
        4. Post-filter by area thresholds AFTER the spatial join
        5. STREAM intersection geometries for later water project/wetland spatial analysis
        """

        self.log.info("🚀 Executing SPATIAL_JOIN: Fields (BUILD) × Properties (PROBE)")
        self.log.info("   This will create a spatial index on fields and filter properties...")

        join_start = time.time()

        # Step 1: SPATIAL_JOIN with SINGLE spatial predicate (required for optimization)
        self.log.info("   Step 1: SPATIAL_JOIN with ST_Intersects...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE raw_intersections AS
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
            JOIN properties p ON ST_Intersects(f.geometry, p.geometry)
        """)

        join_time = time.time() - join_start
        raw_count = self.conn.execute("SELECT COUNT(*) FROM raw_intersections").fetchone()[0]
        self.log.info(
            f"   ✅ SPATIAL_JOIN completed in {join_time:.1f}s: {raw_count:,} intersections"
        )

        # Step 2: Calculate intersection areas and apply filters
        self.log.info("   Step 2: Calculating intersection areas and applying filters...")
        filter_start = time.time()

        # First, calculate areas and apply filters (memory efficient)
        self.conn.execute("""
            CREATE OR REPLACE TABLE filtered_intersections AS
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
                (ST_Area_Spheroid(ST_Intersection(field_geometry, property_geometry)) / property_area_m2) * 100 as property_area_share_pct
                
            FROM raw_intersections
            WHERE 
                -- Filter out tiny intersections (< 1% of field area or < 100 m²)
                ST_Area_Spheroid(ST_Intersection(field_geometry, property_geometry)) > 100
                AND (ST_Area_Spheroid(ST_Intersection(field_geometry, property_geometry)) / field_area_m2) > 0.01
        """)

        filter_time = time.time() - filter_start
        filtered_count = self.conn.execute(
            "SELECT COUNT(*) FROM filtered_intersections"
        ).fetchone()[0]
        self.log.info(
            f"   ✅ Area filtering completed in {filter_time:.1f}s: {filtered_count:,} valid intersections"
        )

        # Step 3: Add geometries to filtered results (memory efficient - smaller dataset)
        self.log.info("   Step 3: Adding intersection geometries to filtered results...")
        geometry_start = time.time()

        self.conn.execute("""
            CREATE OR REPLACE TABLE field_property_intersections AS
            SELECT 
                fi.*,
                -- STREAM intersection geometries for downstream spatial analysis
                ri.field_geometry,
                ri.property_geometry,
                ST_Intersection(ri.field_geometry, ri.property_geometry) as intersection_geometry
                
            FROM filtered_intersections fi
            JOIN raw_intersections ri ON 
                fi.field_id = ri.field_id 
                AND fi.block_id = ri.block_id 
                AND fi.cvr_number = ri.cvr_number 
                AND fi.bfe_number = ri.bfe_number
        """)

        geometry_time = time.time() - geometry_start
        self.log.info(f"   ✅ Geometry streaming completed in {geometry_time:.1f}s")

        # Clean up intermediate tables immediately
        self.conn.execute("DROP TABLE raw_intersections")
        self.conn.execute("DROP TABLE filtered_intersections")
        self.log.info("   🧹 Cleaned up intermediate tables")

        # Get statistics from final table
        final_count = self.conn.execute(
            "SELECT COUNT(*) FROM field_property_intersections"
        ).fetchone()[0]

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

        total_time = join_time + filter_time + geometry_time

        self.log.info("✅ SPATIAL_JOIN optimization results:")
        self.log.info(
            f"   Total processing time: {total_time:.1f}s (join: {join_time:.1f}s, filter: {filter_time:.1f}s, geometry: {geometry_time:.1f}s)"
        )
        self.log.info(f"   Total intersections: {total_intersections:,}")
        self.log.info(f"   Unique properties: {unique_props:,}")
        self.log.info(f"   Unique fields: {unique_flds:,}")
        self.log.info(
            f"   Size reduction: {reduction_pct:.1f}% ({original_properties:,} → {unique_props:,} properties)"
        )
        self.log.info(f"   Average field area share: {avg_field_share:.1f}%")
        self.log.info(f"   Average property area share: {avg_prop_share:.1f}%")
        self.log.info(f"   Average intersection area: {avg_intersection:.0f} m²")
        self.log.info("🚀 SPATIAL_JOIN operator successfully utilized!")
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
            "spatial_join_time_s": join_time,
            "filter_time_s": filter_time,
            "geometry_time_s": geometry_time,
            "total_time_s": total_time,
        }

    def _save_output_data(self, result: Dict[str, Any]):
        """Save pre-filtered field-property intersections with streaming geometries to GCS."""
        self._save_stage_output("field_property_intersections", "field_property_intersections")
