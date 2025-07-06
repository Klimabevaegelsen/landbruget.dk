"""
Spatial joining utilities for H3 PFAS exposure analysis.
"""

import gc
import math
import time

import duckdb
from loguru import logger
from tqdm import tqdm

from ..config import H3SpatialConfig


class SpatialJoiner:
    """Handles the 5-stage optimized spatial join pipeline."""

    def __init__(self, conn: duckdb.DuckDBPyConnection, config: H3SpatialConfig):
        self.conn = conn
        self.config = config
        self.log = logger.bind(component="SpatialJoiner")

    def perform_chunked_spatial_join(
        self, h3_table: str, fields_table: str, pesticide_table: str, year: int
    ) -> str:
        """Perform chunked spatial join with 5-stage pipeline."""

        # Get data counts
        h3_count = self.conn.execute(f"SELECT COUNT(*) FROM {h3_table}").fetchone()[0]
        self.conn.execute(f"SELECT COUNT(*) FROM {fields_table}").fetchone()[0]

        self.log.info(f"🎯 Starting chunked spatial join for {h3_count:,} H3 cells")

        # Calculate chunking strategy
        total_chunks = math.ceil(h3_count / self.config.chunk_size)
        self.log.info(
            f"📦 Processing in {total_chunks} chunks of {self.config.chunk_size:,} cells each"
        )

        # Create result table
        result_table = f"h3_spatial_results_{year}"
        self.conn.execute(f"DROP TABLE IF EXISTS {result_table}")

        # Process chunks with progress bar
        start_time = time.time()

        # Create progress bar
        progress_bar = tqdm(
            total=total_chunks,
            desc=f"Processing H3 chunks (year {year})",
            unit="chunk",
            disable=not self.config.enable_progress_tracking,
        )

        try:
            for chunk_idx in range(total_chunks):
                chunk_start_time = time.time()
                offset = chunk_idx * self.config.chunk_size

                # Only log every 10th chunk to reduce noise, unless chunk details are enabled
                if self.config.log_chunk_details or (chunk_idx + 1) % 10 == 0:
                    self.log.info(f"📦 Processing chunk {chunk_idx + 1}/{total_chunks}")

                # Create chunk table
                chunk_results = self._process_single_chunk(
                    h3_table, fields_table, pesticide_table, offset, chunk_idx, total_chunks, year
                )

                # Append to results
                if chunk_idx == 0:
                    self.conn.execute(
                        f"CREATE TABLE {result_table} AS SELECT * FROM {chunk_results}"
                    )
                else:
                    self.conn.execute(f"INSERT INTO {result_table} SELECT * FROM {chunk_results}")

                # Clean up chunk table
                self.conn.execute(f"DROP TABLE IF EXISTS {chunk_results}")

                # Force garbage collection and checkpoint every few chunks
                if chunk_idx % 5 == 0:
                    gc.collect()
                    try:
                        self.conn.execute("CHECKPOINT")
                    except Exception:
                        pass

                chunk_time = time.time() - chunk_start_time
                progress_pct = (chunk_idx + 1) / total_chunks * 100

                # Update progress bar with detailed metrics
                progress_bar.update(1)
                progress_bar.set_postfix(
                    {
                        "chunk_time": f"{chunk_time:.2f}s",
                        "progress": f"{progress_pct:.1f}%",
                        "avg_time": f"{(time.time() - start_time) / (chunk_idx + 1):.2f}s",
                    }
                )

                # Log every 10th chunk or if chunk details are enabled
                if self.config.log_chunk_details or (chunk_idx + 1) % 10 == 0:
                    self.log.info(
                        f"   ✅ Chunk {chunk_idx + 1}/{total_chunks} completed in {chunk_time:.2f}s ({progress_pct:.1f}%)"
                    )

        finally:
            progress_bar.close()

        total_time = time.time() - start_time
        self.log.info(f"🎉 Chunked spatial join completed in {total_time:.2f}s")

        return result_table

    def _process_single_chunk(
        self,
        h3_table: str,
        fields_table: str,
        pesticide_table: str,
        offset: int,
        chunk_idx: int,
        total_chunks: int,
        year: int,
    ) -> str:
        """Process a single chunk using the 5-stage pipeline."""

        # Get chunk of H3 cells
        chunk_h3_table = f"h3_chunk_{chunk_idx}"
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE {chunk_h3_table} AS
            SELECT * FROM {h3_table}
            LIMIT {self.config.chunk_size} OFFSET {offset}
        """)

        # Stage 1: Fast intersection detection
        if self.config.stage_1_fast_intersection:
            intersections_table = self._stage_1_fast_intersection(
                chunk_h3_table, fields_table, chunk_idx
            )

        # Stage 2: Area calculations
        if self.config.stage_2_area_calculation:
            areas_table = self._stage_2_area_calculation(intersections_table, chunk_idx)

        # Stage 3: H3-field aggregation
        if self.config.stage_3_h3_field_aggregation:
            aggregated_table = self._stage_3_h3_field_aggregation(areas_table, chunk_idx)

        # Stage 4: Pesticide join
        if self.config.stage_4_pesticide_join:
            pesticide_joined_table = self._stage_4_pesticide_join(
                aggregated_table, pesticide_table, chunk_idx
            )

        # Stage 5: Geometric union
        if self.config.stage_5_geometric_union:
            final_table = self._stage_5_geometric_union(
                pesticide_joined_table, intersections_table, chunk_idx
            )

        # Clean up intermediate tables
        for table in [
            chunk_h3_table,
            intersections_table,
            areas_table,
            aggregated_table,
            pesticide_joined_table,
        ]:
            self.conn.execute(f"DROP TABLE IF EXISTS {table}")

        return final_table

    def _stage_1_fast_intersection(self, h3_table: str, fields_table: str, chunk_idx: int) -> str:
        """Stage 1: Fast spatial intersection detection."""

        if self.config.log_stage_timings:
            stage_start = time.time()

        result_table = f"stage1_intersections_{chunk_idx}"

        query = f"""
        CREATE OR REPLACE TABLE {result_table} AS
        SELECT
            h.h3_cell,
            h.center_lat,
            h.center_lon,
            h.h3_geometry,
            f.field_id,
            f.cvr_number,
            f.block_id,
            f.area_ha as field_area_ha,
            f.crop_code,
            f.crop_name,
            f.original_geometry,
            f.flipped_geometry
        FROM {h3_table} h
        INNER JOIN {fields_table} f ON ST_Intersects(h.h3_geometry, f.flipped_geometry)
        """

        self.conn.execute(query)

        if self.config.log_stage_timings:
            stage_time = time.time() - stage_start
            count = self.conn.execute(f"SELECT COUNT(*) FROM {result_table}").fetchone()[0]
            self.log.debug(f"   🚀 Stage 1: {count:,} intersections in {stage_time:.3f}s")

        return result_table

    def _stage_2_area_calculation(self, intersections_table: str, chunk_idx: int) -> str:
        """Stage 2: Calculate intersection areas with coordinate flipping."""

        if self.config.log_stage_timings:
            stage_start = time.time()

        result_table = f"stage2_areas_{chunk_idx}"

        query = f"""
        CREATE OR REPLACE TABLE {result_table} AS
        SELECT
            *,
            -- Calculate intersection area using original coordinates
            ST_Area_Spheroid(ST_Intersection(
                original_geometry,
                ST_FlipCoordinates(h3_geometry)
            )) / 10000.0 as intersection_area_ha,
            -- Calculate coverage ratio
            CASE
                WHEN ST_Area_Spheroid(ST_FlipCoordinates(h3_geometry)) > 0 THEN
                    LEAST(1.0, ST_Area_Spheroid(ST_Intersection(
                        original_geometry,
                        ST_FlipCoordinates(h3_geometry)
                    )) / ST_Area_Spheroid(ST_FlipCoordinates(h3_geometry)))
                ELSE 0.0
            END as coverage_ratio
        FROM {intersections_table}
        WHERE ST_Area_Spheroid(ST_Intersection(
            original_geometry,
            ST_FlipCoordinates(h3_geometry)
        )) > 0
        """

        self.conn.execute(query)

        if self.config.log_stage_timings:
            stage_time = time.time() - stage_start
            count = self.conn.execute(f"SELECT COUNT(*) FROM {result_table}").fetchone()[0]
            self.log.debug(f"   📐 Stage 2: {count:,} valid areas in {stage_time:.3f}s")

        return result_table

    def _stage_3_h3_field_aggregation(self, areas_table: str, chunk_idx: int) -> str:
        """Stage 3: Aggregate by H3 cell and field to prevent double-counting."""

        if self.config.log_stage_timings:
            stage_start = time.time()

        result_table = f"stage3_aggregated_{chunk_idx}"

        query = f"""
        CREATE OR REPLACE TABLE {result_table} AS
        SELECT
            h3_cell,
            center_lat,
            center_lon,
            field_id,
            cvr_number,
            block_id,
            field_area_ha,
            crop_code,
            crop_name,
            h3_geometry,
            original_geometry,
            -- Keep only ONE intersection area per H3-field combination
            MAX(intersection_area_ha) as intersection_area_ha,
            MAX(coverage_ratio) as coverage_ratio
        FROM {areas_table}
        WHERE h3_cell IS NOT NULL
        GROUP BY h3_cell, center_lat, center_lon, field_id, cvr_number, block_id,
                 field_area_ha, crop_code, crop_name, h3_geometry, original_geometry
        """

        self.conn.execute(query)

        if self.config.log_stage_timings:
            stage_time = time.time() - stage_start
            count = self.conn.execute(f"SELECT COUNT(*) FROM {result_table}").fetchone()[0]
            self.log.debug(
                f"   📊 Stage 3: {count:,} unique H3-field combinations in {stage_time:.3f}s"
            )

        return result_table

    def _stage_4_pesticide_join(
        self, aggregated_table: str, pesticide_table: str, chunk_idx: int
    ) -> str:
        """Stage 4: Join with pesticide data."""

        if self.config.log_stage_timings:
            stage_start = time.time()

        result_table = f"stage4_pesticide_{chunk_idx}"

        query = f"""
        CREATE OR REPLACE TABLE {result_table} AS
        SELECT
            i.*,
            p.PesticideRegistrationNumber,
            p.DosageQuantity,
            p.DosageUnit,
            p.contains_pfas,
            p.contains_diquat,
            p.contains_glyphosate,
            p.pfas_containing_active_ingredient_grams,
            p.diquat_containing_active_ingredient_grams,
            p.glyphosate_containing_active_ingredient_grams,
            p.pesticide_belastning_applied,
            p.pfas_containing_pesticide_belastning_applied,
            p.diquat_containing_pesticide_belastning_applied,
            p.glyphosate_containing_pesticide_belastning_applied,
            -- Calculate weighted PFAS-containing active ingredient exposure based on coverage
            CASE
                WHEN p.contains_pfas = true AND p.pfas_containing_active_ingredient_grams IS NOT NULL THEN
                    p.pfas_containing_active_ingredient_grams * i.coverage_ratio
                ELSE 0
            END as weighted_pfas_containing_active_ingredient_grams,
            -- Calculate weighted diquat-containing active ingredient exposure based on coverage
            CASE
                WHEN p.contains_diquat = true AND p.diquat_containing_active_ingredient_grams IS NOT NULL THEN
                    p.diquat_containing_active_ingredient_grams * i.coverage_ratio
                ELSE 0
            END as weighted_diquat_containing_active_ingredient_grams,
            -- Calculate weighted glyphosate-containing active ingredient exposure based on coverage
            CASE
                WHEN p.contains_glyphosate = true AND p.glyphosate_containing_active_ingredient_grams IS NOT NULL THEN
                    p.glyphosate_containing_active_ingredient_grams * i.coverage_ratio
                ELSE 0
            END as weighted_glyphosate_containing_active_ingredient_grams,
            -- Calculate weighted pesticide load
            CASE
                WHEN p.pesticide_belastning_applied IS NOT NULL THEN
                    p.pesticide_belastning_applied * i.coverage_ratio
                ELSE 0
            END as weighted_pesticide_belastning,
            -- Calculate weighted PFAS pesticide load
            CASE
                WHEN p.pfas_containing_pesticide_belastning_applied IS NOT NULL THEN
                    p.pfas_containing_pesticide_belastning_applied * i.coverage_ratio
                ELSE 0
            END as weighted_pfas_pesticide_belastning,
            -- Calculate weighted diquat pesticide load
            CASE
                WHEN p.diquat_containing_pesticide_belastning_applied IS NOT NULL THEN
                    p.diquat_containing_pesticide_belastning_applied * i.coverage_ratio
                ELSE 0
            END as weighted_diquat_pesticide_belastning,
            -- Calculate weighted glyphosate pesticide load
            CASE
                WHEN p.glyphosate_containing_pesticide_belastning_applied IS NOT NULL THEN
                    p.glyphosate_containing_pesticide_belastning_applied * i.coverage_ratio
                ELSE 0
            END as weighted_glyphosate_pesticide_belastning
        FROM {aggregated_table} i
        LEFT JOIN {pesticide_table} p ON (
            i.cvr_number = p.cvr
            AND i.field_id = p.extracted_field_id
            AND i.block_id = p.extracted_block_id
        )
        """

        self.conn.execute(query)

        if self.config.log_stage_timings:
            stage_time = time.time() - stage_start
            count = self.conn.execute(f"SELECT COUNT(*) FROM {result_table}").fetchone()[0]
            self.log.debug(
                f"   🧪 Stage 4: {count:,} records with pesticide data in {stage_time:.3f}s"
            )

        return result_table

    def _stage_5_geometric_union(
        self, pesticide_table: str, raw_intersections_table: str, chunk_idx: int
    ) -> str:
        """Stage 5: Calculate geometric union and final H3 aggregation."""

        if self.config.log_stage_timings:
            stage_start = time.time()

        result_table = f"stage5_final_{chunk_idx}"

        query = f"""
        CREATE OR REPLACE TABLE {result_table} AS
        WITH geometric_union AS (
            SELECT
                r.h3_cell,
                r.center_lat,
                r.center_lon,
                -- Calculate union of all field geometries intersecting this H3 cell
                ST_Area_Spheroid(
                    ST_Intersection(
                        ST_Union_Agg(r.original_geometry),
                        ST_FlipCoordinates(r.h3_geometry)
                    )
                ) / 10000.0 as actual_intersection_area_ha,
                -- Calculate H3 cell area for validation
                ST_Area_Spheroid(ST_FlipCoordinates(r.h3_geometry)) / 10000.0 as h3_cell_area_ha
            FROM {raw_intersections_table} r
            WHERE r.h3_cell IS NOT NULL
            GROUP BY r.h3_cell, r.center_lat, r.center_lon, r.h3_geometry
        ),
        field_stats AS (
            SELECT
                h3_cell,
                COUNT(DISTINCT CONCAT(cvr_number, '_', block_id, '_', field_id)) as unique_field_count,
                SUM(COALESCE(weighted_pfas_containing_active_ingredient_grams, 0)) as total_pfas_containing_active_ingredient_grams,
                SUM(COALESCE(weighted_diquat_containing_active_ingredient_grams, 0)) as total_diquat_containing_active_ingredient_grams,
                SUM(COALESCE(weighted_glyphosate_containing_active_ingredient_grams, 0)) as total_glyphosate_containing_active_ingredient_grams,
                SUM(COALESCE(weighted_pesticide_belastning, 0)) as total_pesticide_belastning,
                SUM(COALESCE(weighted_pfas_pesticide_belastning, 0)) as total_pfas_pesticide_belastning,
                SUM(COALESCE(weighted_diquat_pesticide_belastning, 0)) as total_diquat_pesticide_belastning,
                SUM(COALESCE(weighted_glyphosate_pesticide_belastning, 0)) as total_glyphosate_pesticide_belastning,
                COUNT(CASE WHEN PesticideRegistrationNumber IS NOT NULL THEN 1 END) as total_pesticide_applications,
                COUNT(CASE WHEN contains_pfas = true THEN 1 END) as pfas_containing_applications,
                COUNT(CASE WHEN contains_diquat = true THEN 1 END) as diquat_containing_applications,
                COUNT(CASE WHEN contains_glyphosate = true THEN 1 END) as glyphosate_containing_applications,
                STRING_AGG(DISTINCT crop_name, '; ') as crop_types,
                COUNT(DISTINCT crop_code) as crop_diversity
            FROM {pesticide_table}
            WHERE h3_cell IS NOT NULL
            GROUP BY h3_cell
        )
        SELECT
            g.h3_cell,
            g.center_lat,
            g.center_lon,
            g.h3_cell_area_ha,
            g.actual_intersection_area_ha as total_intersection_area_ha,
            CASE
                WHEN g.h3_cell_area_ha > 0 THEN
                    LEAST(1.0, g.actual_intersection_area_ha / g.h3_cell_area_ha)
                ELSE 0.0
            END as actual_coverage_ratio,
            COALESCE(f.unique_field_count, 0) as unique_field_count,
            -- Active ingredient totals
            COALESCE(f.total_pfas_containing_active_ingredient_grams, 0) as total_pfas_containing_active_ingredient_grams,
            COALESCE(f.total_diquat_containing_active_ingredient_grams, 0) as total_diquat_containing_active_ingredient_grams,
            COALESCE(f.total_glyphosate_containing_active_ingredient_grams, 0) as total_glyphosate_containing_active_ingredient_grams,
            -- Pesticide load totals
            COALESCE(f.total_pesticide_belastning, 0) as total_pesticide_belastning,
            COALESCE(f.total_pfas_pesticide_belastning, 0) as total_pfas_pesticide_belastning,
            COALESCE(f.total_diquat_pesticide_belastning, 0) as total_diquat_pesticide_belastning,
            COALESCE(f.total_glyphosate_pesticide_belastning, 0) as total_glyphosate_pesticide_belastning,
            -- Application counts
            COALESCE(f.total_pesticide_applications, 0) as total_pesticide_applications,
            COALESCE(f.pfas_containing_applications, 0) as pfas_containing_applications,
            COALESCE(f.diquat_containing_applications, 0) as diquat_containing_applications,
            COALESCE(f.glyphosate_containing_applications, 0) as glyphosate_containing_applications,
            -- Crop information
            COALESCE(f.crop_types, '') as crop_types,
            COALESCE(f.crop_diversity, 0) as crop_diversity,
            -- Intensity metrics (grams per hectare)
            CASE
                WHEN g.actual_intersection_area_ha > 0 THEN
                    COALESCE(f.total_pfas_containing_active_ingredient_grams, 0) / g.actual_intersection_area_ha
                ELSE 0
            END as pfas_containing_active_ingredient_intensity_grams_per_ha,
            CASE
                WHEN g.actual_intersection_area_ha > 0 THEN
                    COALESCE(f.total_diquat_containing_active_ingredient_grams, 0) / g.actual_intersection_area_ha
                ELSE 0
            END as diquat_containing_active_ingredient_intensity_grams_per_ha,
            CASE
                WHEN g.actual_intersection_area_ha > 0 THEN
                    COALESCE(f.total_glyphosate_containing_active_ingredient_grams, 0) / g.actual_intersection_area_ha
                ELSE 0
            END as glyphosate_containing_active_ingredient_intensity_grams_per_ha,
            -- Total pesticide load intensity (grams per hectare)
            CASE
                WHEN g.actual_intersection_area_ha > 0 THEN
                    COALESCE(f.total_pesticide_belastning, 0) / g.actual_intersection_area_ha
                ELSE 0
            END as pesticide_belastning_per_ha,
            CURRENT_TIMESTAMP as created_at
        FROM geometric_union g
        LEFT JOIN field_stats f ON g.h3_cell = f.h3_cell
        WHERE g.actual_intersection_area_ha > 0
        """

        self.conn.execute(query)

        if self.config.log_stage_timings:
            stage_time = time.time() - stage_start
            count = self.conn.execute(f"SELECT COUNT(*) FROM {result_table}").fetchone()[0]
            self.log.debug(f"   📊 Stage 5: {count:,} final H3 cells in {stage_time:.3f}s")

        return result_table
