#!/usr/bin/env python3
"""
H3 PFAS Processor - Refactored Version with GCS Integration
==========================================================

This implementation incorporates the successful patterns from the chunked spatial join processor
AND the GCS data loading capabilities from the original processor:
- 5-stage optimized spatial join pipeline
- Geometric union for overlap handling
- ST_FlipCoordinates for accurate area calculations
- Proper H3-centric aggregation
- GCS data loading for multiple years (2015+)
- Comprehensive validation and monitoring
"""

import asyncio
import math
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
from loguru import logger


@dataclass
class H3SpatialConfig:
    """Centralized configuration for H3 spatial processing."""

    # H3 Configuration
    h3_resolution: int = 10  # Resolution 10 = ~1.5 ha cells
    denmark_bounds: dict[str, float] = field(
        default_factory=lambda: {"min_lat": 54.5, "max_lat": 57.8, "min_lon": 8.0, "max_lon": 15.2}
    )

    # Chunked Processing Configuration
    chunk_size: int = 25000  # H3 cells per chunk
    min_intersection_area_sqm: float = 0.0  # Include all intersections, no size limits
    memory_limit: str = "12GB"
    thread_count: int = 4

    # 5-Stage Spatial Join Configuration
    stage_1_fast_intersection: bool = True
    stage_2_area_calculation: bool = True
    stage_3_h3_field_aggregation: bool = True
    stage_4_pesticide_join: bool = True
    stage_5_geometric_union: bool = True

    # Coordinate System Configuration
    use_coordinate_flipping: bool = True
    coordinate_system: str = "EPSG:4326"

    # Validation Thresholds
    min_h3_area_ha: float = 0.91
    max_h3_area_ha: float = 1.82
    theoretical_avg_area_ha: float = 1.5048
    max_area_deviation_pct: float = 20.0

    # Performance Monitoring
    enable_progress_tracking: bool = True
    log_chunk_details: bool = True
    log_stage_timings: bool = True

    # GCS Configuration
    bucket: str = "landbrugsdata-raw-data"
    available_years: list[int] = field(
        default_factory=lambda: [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023]
    )


@dataclass
class ValidationResult:
    """Result of a validation check."""

    name: str
    passed: bool
    metrics: dict[str, Any] = field(default_factory=dict)
    message: str = ""


class CoordinateTransformer:
    """Handles coordinate system transformations and area calculations."""

    def __init__(self, conn: duckdb.DuckDBPyConnection, config: H3SpatialConfig):
        self.conn = conn
        self.config = config
        self.log = logger.bind(component="CoordinateTransformer")

    def prepare_geometries(self, table_name: str, geometry_column: str = "geometry_wkt") -> str:
        """Prepare geometries with coordinate flipping for spatial operations."""

        prepared_table = f"{table_name}_prepared"

        query = f"""
        CREATE OR REPLACE TABLE {prepared_table} AS
        SELECT *,
            -- Original geometry for area calculations (LAT/LON)
            ST_GeomFromText({geometry_column}) as original_geometry,
            -- Flipped geometry for spatial operations (LON/LAT)
            ST_FlipCoordinates(ST_GeomFromText({geometry_column})) as flipped_geometry
        FROM {table_name}
        WHERE {geometry_column} IS NOT NULL
        AND ST_IsValid(ST_GeomFromText({geometry_column}))
        """

        self.conn.execute(query)
        self.log.debug(f"✅ Prepared geometries for {table_name}")
        return prepared_table


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

        # Process chunks
        start_time = time.time()

        for chunk_idx in range(total_chunks):
            chunk_start_time = time.time()
            offset = chunk_idx * self.config.chunk_size

            self.log.info(f"📦 Processing chunk {chunk_idx + 1}/{total_chunks}")

            # Create chunk table
            chunk_results = self._process_single_chunk(
                h3_table, fields_table, pesticide_table, offset, chunk_idx, total_chunks, year
            )

            # Append to results
            if chunk_idx == 0:
                self.conn.execute(f"CREATE TABLE {result_table} AS SELECT * FROM {chunk_results}")
            else:
                self.conn.execute(f"INSERT INTO {result_table} SELECT * FROM {chunk_results}")

            # Clean up chunk table
            self.conn.execute(f"DROP TABLE IF EXISTS {chunk_results}")

            chunk_time = time.time() - chunk_start_time
            progress_pct = (chunk_idx + 1) / total_chunks * 100

            if self.config.log_chunk_details:
                self.log.info(
                    f"   ✅ Chunk {chunk_idx + 1} completed in {chunk_time:.2f}s ({progress_pct:.1f}%)"
                )

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
            p.pfas_containing_active_ingredient_grams,
            p.pesticide_belastning_applied,
            p.pfas_containing_pesticide_belastning_applied,
            -- Calculate weighted PFAS-containing active ingredient exposure based on coverage
            CASE
                WHEN p.contains_pfas = true AND p.pfas_containing_active_ingredient_grams IS NOT NULL THEN
                    p.pfas_containing_active_ingredient_grams * i.coverage_ratio
                ELSE 0
            END as weighted_pfas_containing_active_ingredient_grams,
            -- Calculate weighted pesticide load
            CASE
                WHEN p.pesticide_belastning_applied IS NOT NULL THEN
                    p.pesticide_belastning_applied * i.coverage_ratio
                ELSE 0
            END as weighted_pesticide_belastning
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
                SUM(COALESCE(weighted_pesticide_belastning, 0)) as total_pesticide_belastning,
                COUNT(CASE WHEN PesticideRegistrationNumber IS NOT NULL THEN 1 END) as total_pesticide_applications,
                COUNT(CASE WHEN contains_pfas = true THEN 1 END) as pfas_containing_applications,
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
            COALESCE(f.total_pfas_containing_active_ingredient_grams, 0) as total_pfas_containing_active_ingredient_grams,
            COALESCE(f.total_pesticide_belastning, 0) as total_pesticide_belastning,
            COALESCE(f.total_pesticide_applications, 0) as total_pesticide_applications,
            COALESCE(f.pfas_containing_applications, 0) as pfas_containing_applications,
            COALESCE(f.crop_types, '') as crop_types,
            COALESCE(f.crop_diversity, 0) as crop_diversity,
            -- Intensity metrics
            CASE
                WHEN g.actual_intersection_area_ha > 0 THEN
                    COALESCE(f.total_pfas_containing_active_ingredient_grams, 0) / g.actual_intersection_area_ha
                ELSE 0
            END as pfas_containing_active_ingredient_intensity_grams_per_ha,
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


class AreaValidator:
    """Validates area calculations and geometric operations."""

    def __init__(self, conn: duckdb.DuckDBPyConnection, config: H3SpatialConfig):
        self.conn = conn
        self.config = config
        self.log = logger.bind(component="AreaValidator")

    def validate_h3_cell_areas(self, results_table: str) -> ValidationResult:
        """Validate H3 cell areas are within expected bounds."""

        area_stats = self.conn.execute(f"""
            SELECT
                MIN(h3_cell_area_ha) as min_area,
                MAX(h3_cell_area_ha) as max_area,
                AVG(h3_cell_area_ha) as avg_area,
                COUNT(*) as total_cells
            FROM {results_table}
        """).fetchone()

        within_bounds = (
            self.config.min_h3_area_ha <= area_stats[0]
            and area_stats[1] <= self.config.max_h3_area_ha
        )
        avg_deviation = (
            abs(area_stats[2] - self.config.theoretical_avg_area_ha)
            / self.config.theoretical_avg_area_ha
            * 100
        )

        return ValidationResult(
            name="H3 Cell Areas",
            passed=within_bounds and avg_deviation < self.config.max_area_deviation_pct,
            metrics={
                "min_area_ha": area_stats[0],
                "max_area_ha": area_stats[1],
                "avg_area_ha": area_stats[2],
                "total_cells": area_stats[3],
                "avg_deviation_pct": avg_deviation,
            },
            message=f"H3 areas: {area_stats[0]:.3f}-{area_stats[1]:.3f} ha (expected: {self.config.min_h3_area_ha}-{self.config.max_h3_area_ha} ha)",
        )

    def validate_intersection_areas(self, results_table: str) -> ValidationResult:
        """Validate intersection area calculations."""

        validation_stats = self.conn.execute(f"""
            SELECT
                COUNT(*) as total_cells,
                COUNT(CASE WHEN total_intersection_area_ha > h3_cell_area_ha THEN 1 END) as impossible_intersections,
                COUNT(CASE WHEN actual_coverage_ratio < 0 OR actual_coverage_ratio > 1 THEN 1 END) as invalid_coverage,
                MAX(total_intersection_area_ha) as max_intersection,
                MAX(h3_cell_area_ha) as max_h3_area
            FROM {results_table}
        """).fetchone()

        impossible_pct = (
            (validation_stats[1] / validation_stats[0]) * 100 if validation_stats[0] > 0 else 0
        )
        invalid_coverage_pct = (
            (validation_stats[2] / validation_stats[0]) * 100 if validation_stats[0] > 0 else 0
        )

        return ValidationResult(
            name="Intersection Areas",
            passed=validation_stats[1] == 0 and validation_stats[2] == 0,
            metrics={
                "total_cells": validation_stats[0],
                "impossible_intersections": validation_stats[1],
                "invalid_coverage_ratios": validation_stats[2],
                "impossible_intersection_pct": impossible_pct,
                "invalid_coverage_pct": invalid_coverage_pct,
            },
            message=f"Impossible intersections: {impossible_pct:.2f}%, Invalid coverage: {invalid_coverage_pct:.2f}%",
        )


class H3PFASProcessorRefactored:
    """Refactored H3 PFAS processor with modular architecture and GCS data loading."""

    def __init__(self, config: H3SpatialConfig, local_data_dir: Path | None = None):
        self.config = config
        self.local_data_dir = local_data_dir
        self.log = logger.bind(processor="H3PFASRefactored")
        self.conn = None

        # Initialize components
        self.coordinate_transformer = None
        self.spatial_joiner = None
        self.area_validator = None
        self.gcs_access = None

    def setup_duckdb(self):
        """Setup DuckDB with required extensions."""
        self.log.info("🔧 Setting up DuckDB with H3 and spatial extensions")

        self.conn = duckdb.connect(":memory:")
        self.conn.execute(f"SET memory_limit='{self.config.memory_limit}'")
        self.conn.execute(f"SET threads={self.config.thread_count}")

        # Install extensions
        extensions = [
            ("h3", "FROM community"),  # Use community repository for H3 extension
            ("spatial", ""),
            ("httpfs", ""),
        ]
        for ext_name, ext_source in extensions:
            try:
                install_cmd = f"INSTALL {ext_name} {ext_source}".strip()
                self.conn.execute(install_cmd)
                self.conn.execute(f"LOAD {ext_name}")
                self.log.debug(f"✅ Loaded DuckDB extension: {ext_name}")
            except Exception as e:
                self.log.error(f"❌ Failed to load extension {ext_name}: {e}")
                raise

        # Initialize GCS access for cloud data loading
        if not self.local_data_dir:
            try:
                # Try multiple import paths for different environments
                try:
                    # First try: Direct import from backend path (GitHub Actions)
                    from backend.pipelines.unified_pipeline.src.unified_pipeline.util.gcs_access import (
                        GCSDataAccess,
                    )
                except ImportError:
                    try:
                        # Second try: Add workspace root to Python path and import
                        import os
                        import sys

                        # Find project root by looking for pyproject.toml
                        current_dir = os.path.dirname(os.path.abspath(__file__))
                        project_root = current_dir

                        # Walk up the directory tree to find project root
                        while project_root != os.path.dirname(project_root):
                            if os.path.exists(os.path.join(project_root, "pyproject.toml")):
                                break
                            project_root = os.path.dirname(project_root)

                        # Add project root to Python path for backend imports
                        if project_root not in sys.path:
                            sys.path.insert(0, project_root)

                        # Now try the backend import
                        from backend.pipelines.unified_pipeline.src.unified_pipeline.util.gcs_access import (
                            GCSDataAccess,
                        )
                    except ImportError:
                        # Third try: Relative import from current location
                        import os
                        import sys

                        # Get the absolute path to the unified pipeline
                        current_dir = os.path.dirname(os.path.abspath(__file__))
                        unified_path = os.path.join(
                            current_dir, "..", "..", "..", "..", "unified_pipeline", "src"
                        )
                        unified_path = os.path.abspath(unified_path)

                        if os.path.exists(unified_path):
                            sys.path.insert(0, unified_path)
                            from unified_pipeline.util.gcs_access import GCSDataAccess
                        else:
                            raise ImportError(f"Unified pipeline not found at {unified_path}")

                self.gcs_access = GCSDataAccess(connection=self.conn)
                self.log.info("✅ GCS access initialized for cloud data loading")
            except ImportError as e:
                self.log.warning(f"⚠️ GCS access not available: {e}")
                self.gcs_access = None

        # Initialize components
        self.coordinate_transformer = CoordinateTransformer(self.conn, self.config)
        self.spatial_joiner = SpatialJoiner(self.conn, self.config)
        self.area_validator = AreaValidator(self.conn, self.config)

        self.log.info("✅ DuckDB setup complete")

    def generate_h3_grid(self) -> str:
        """Generate H3 grid covering Denmark."""
        self.log.info(f"🗺️ Generating Denmark H3 grid at resolution {self.config.h3_resolution}")

        bounds = self.config.denmark_bounds

        query = f"""
        CREATE OR REPLACE TABLE denmark_h3_grid AS
        WITH denmark_bbox AS (
            SELECT ST_MakeEnvelope(
                {bounds["min_lon"]}, {bounds["min_lat"]},
                {bounds["max_lon"]}, {bounds["max_lat"]}
            ) as bbox_geom
        ),
        h3_cells AS (
            SELECT h3_polygon_wkt_to_cells(ST_AsText(bbox_geom), {self.config.h3_resolution}) as h3_cells
            FROM denmark_bbox
        ),
        h3_exploded AS (
            SELECT UNNEST(h3_cells) as h3_cell
            FROM h3_cells
        )
        SELECT
            h3_cell,
            ST_GeomFromText(h3_cell_to_boundary_wkt(h3_cell)) as h3_geometry,
            h3_cell_to_lat(h3_cell) as center_lat,
            h3_cell_to_lng(h3_cell) as center_lon
        FROM h3_exploded
        WHERE h3_cell IS NOT NULL
        """

        self.conn.execute(query)

        count = self.conn.execute("SELECT COUNT(*) FROM denmark_h3_grid").fetchone()[0]
        self.log.info(f"✅ Generated {count:,} H3 cells covering Denmark")

        return "denmark_h3_grid"

    async def run_analysis_multi_year(self, years: list[int] | None = None) -> bool:
        """Run H3 PFAS analysis for multiple years from GCS data."""
        self.log.info("🚀 Starting multi-year H3 PFAS analysis from GCS")

        # Use provided years or default from config
        years_to_process = years or self.config.available_years
        self.log.info(f"📅 Processing years: {years_to_process}")

        # Setup DuckDB
        self.setup_duckdb()

        if not self.gcs_access:
            self.log.error("❌ GCS access not available - cannot load cloud data")
            return False

        # Load BMD data once for all years (PFAS detection data)
        self.log.info("🧪 Loading BMD pesticide data with PFAS indicators...")
        bmd_table = self._load_bmd_data_from_gcs()
        self.log.info(f"✅ BMD data loaded: {bmd_table}")

        successful_years = 0
        failed_years = 0

        # Process each year
        for year in years_to_process:
            self.log.info("=" * 80)
            self.log.info(f"🔄 Processing year {year}")
            self.log.info("=" * 80)

            try:
                # Check data availability for this year
                if not self._check_year_data_availability(year):
                    self.log.warning(f"⚠️ Skipping year {year}: missing required data")
                    failed_years += 1
                    continue

                # Process single year
                result_count = await self._process_single_year_from_gcs(year, bmd_table)

                if result_count > 0:
                    self.log.info(
                        f"✅ Year {year}: Successfully processed {result_count:,} H3 hexagons with PFAS-containing active ingredient data"
                    )
                    successful_years += 1
                else:
                    self.log.warning(f"⚠️ Year {year}: No results generated")
                    failed_years += 1

            except Exception as e:
                self.log.error(f"❌ Failed to process year {year}: {e}")
                import traceback

                self.log.error(f"📋 Traceback: {traceback.format_exc()}")
                failed_years += 1

        # Summary
        self.log.info("=" * 80)
        self.log.info("📊 Multi-Year H3 PFAS-containing Active Ingredient Analysis Summary")
        self.log.info("=" * 80)
        self.log.info(f"✅ Successfully processed: {successful_years} years")
        self.log.info(f"❌ Failed to process: {failed_years} years")
        total_years = successful_years + failed_years
        if total_years > 0:
            self.log.info(f"📈 Success rate: {successful_years / total_years * 100:.1f}%")
        self.log.info("🎉 Multi-year H3 PFAS-containing active ingredient analysis completed")

        return successful_years > 0

    def _load_bmd_data_from_gcs(self) -> str:
        """Load BMD pesticide data with PFAS indicators from GCS."""
        # Use the latest BMD data from silver layer
        bmd_path = self._get_latest_silver_path("bmd")

        if not bmd_path:
            raise Exception("BMD data not found in silver layer")

        self.log.info(f"📄 Loading BMD data from: {bmd_path}")

        # Load BMD data directly from GCS
        self._load_table_from_gcs(bmd_path, "temp_bmd_raw")

        # Check available columns and handle both old and new standardized names
        bmd_columns = self.conn.execute("PRAGMA table_info(temp_bmd_raw)").fetchall()
        bmd_column_names = [col[1] for col in bmd_columns]

        # Handle product name column
        if "produktnavn" in bmd_column_names:
            product_name_column = "produktnavn"
        elif "product_name" in bmd_column_names:
            product_name_column = "product_name"
        else:
            product_name_column = "produktnavn"  # fallback

        # Handle registration number column
        if "registrerings_nr" in bmd_column_names:
            registration_nr_column = "registrerings_nr"
        elif "registration_number" in bmd_column_names:
            registration_nr_column = "registration_number"
        else:
            registration_nr_column = "registrerings_nr"  # fallback

        # Handle active ingredient column
        if "aktivstofnavn_e" in bmd_column_names:
            active_ingredient_column = "aktivstofnavn_e"
        elif "active_ingredient_name" in bmd_column_names:
            active_ingredient_column = "active_ingredient_name"
        else:
            active_ingredient_column = "aktivstofnavn_e"  # fallback

        # Handle concentration column
        if "koncentration_er" in bmd_column_names:
            concentration_column = "koncentration_er"
        elif "concentration" in bmd_column_names:
            concentration_column = "concentration"
        else:
            concentration_column = "koncentration_er"  # fallback

        # Handle unit column
        if "enhed_er" in bmd_column_names:
            unit_column = "enhed_er"
        elif "unit" in bmd_column_names:
            unit_column = "unit"
        else:
            unit_column = "enhed_er"  # fallback

        # Handle total load column
        if "samlet_belastning" in bmd_column_names:
            total_load_column = "samlet_belastning"
        elif "total_load" in bmd_column_names:
            total_load_column = "total_load"
        else:
            total_load_column = "samlet_belastning"  # fallback

        self.log.info("🔍 BMD column mappings:")
        self.log.info(f"   Product name: {product_name_column}")
        self.log.info(f"   Registration number: {registration_nr_column}")
        self.log.info(f"   Active ingredient: {active_ingredient_column}")
        self.log.info(f"   Concentration: {concentration_column}")
        self.log.info(f"   Unit: {unit_column}")
        self.log.info(f"   Total load: {total_load_column}")

        # Process BMD data with standardized column names
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE bmd_data AS
            SELECT
                {product_name_column} as produktnavn,
                {registration_nr_column} as registrerings_nr,
                {active_ingredient_column} as active_ingredient,
                {concentration_column} as koncentration_er,
                {unit_column} as enhed_er,
                {total_load_column} as total_load_per_unit,
                COALESCE(belastning_miljøeffekt, environmental_effect) as environmental_effect_per_unit,
                COALESCE(belastning_miljøadfærd, environmental_behavior) as environmental_behavior_per_unit,
                COALESCE(belastning_sundhed, health_effect) as health_effect_per_unit,
                contains_pfas,
                -- Clean concentration values (handle Danish decimal comma)
                TRY_CAST(REPLACE(REPLACE({concentration_column}, ',', '.'), ' ', '') AS DOUBLE) as concentration_numeric
            FROM temp_bmd_raw
            WHERE {registration_nr_column} IS NOT NULL
        """)

        # Get statistics for logging
        total_count = self.conn.execute("SELECT COUNT(*) FROM bmd_data").fetchone()[0]
        pfas_count = self.conn.execute(
            "SELECT COUNT(*) FROM bmd_data WHERE contains_pfas = true"
        ).fetchone()[0]

        self.log.info(
            f"✅ BMD data loaded: {total_count:,} products, {pfas_count:,} containing PFAS-based active ingredients ({pfas_count / total_count * 100:.1f}%)"
        )

        return "bmd_data"

    def _check_year_data_availability(self, year: int) -> bool:
        """Check if required data is available for a given year."""
        # Check pesticide disaggregation data for year Y
        pesticide_path = f"gs://{self.config.bucket}/gold/pesticide_disaggregation_{year}/"
        pesticide_available = self._check_gcs_path_exists(pesticide_path)

        # Check FVM marker data for year Y+1 (Y+1 pattern)
        field_year = year + 1
        field_path = f"gs://{self.config.bucket}/silver/fvm_marker_{field_year}/"
        field_available = self._check_gcs_path_exists(field_path)

        self.log.info(f"📊 Year {year} data availability (Y+1 pattern):")
        self.log.info(
            f"   Pesticide disaggregation ({year}): {'✅' if pesticide_available else '❌'}"
        )
        self.log.info(f"   FVM marker ({field_year}): {'✅' if field_available else '❌'}")

        return pesticide_available and field_available

    def _check_gcs_path_exists(self, path: str) -> bool:
        """Check if a GCS path exists and has data."""
        try:
            return self.gcs_access.file_exists(path)
        except Exception as e:
            self.log.debug(f"Error checking GCS path {path}: {e}")
            return False

    async def _process_single_year_from_gcs(self, year: int, bmd_table: str) -> int:
        """Process a single year using GCS data with the refactored spatial methodology."""
        self.log.info(
            f"⚙️ Processing H3 PFAS-containing active ingredient exposure for year {year} (GCS data, refactored methodology)"
        )

        # Step 1: Load and prepare field data (Y+1 pattern)
        field_year = year + 1
        fields_table = self._load_and_prepare_fields_from_gcs(field_year, year)

        # Step 2: Load pesticide disaggregation for year Y
        pesticide_table = self._load_pesticide_disaggregation_from_gcs(year)

        # Step 3: Join pesticide data with BMD for PFAS detection
        pesticide_pfas_table = self._join_pesticide_with_bmd_pfas(pesticide_table, bmd_table, year)

        # Step 4: Generate H3 grid
        h3_grid_table = self.generate_h3_grid()

        # Step 5: Perform chunked spatial join using the refactored methodology
        results_table = self.spatial_joiner.perform_chunked_spatial_join(
            h3_grid_table, fields_table, pesticide_pfas_table, year
        )

        # Step 6: Validate results
        self._validate_results(results_table)

        # Step 7: Save results to GCS
        result_count = self._save_year_results_kepler_compatible(results_table, year)

        # Step 8: Clean up intermediate tables
        self._cleanup_year_tables(year)

        return result_count

    def _load_and_prepare_fields_from_gcs(self, field_year: int, pesticide_year: int) -> str:
        """Load FVM field data from GCS and prepare for spatial intersection."""
        self.log.info(f"📄 Loading FVM field data for year {field_year} from GCS")

        # Get FVM data path
        silver_path = self._get_latest_silver_path(f"fvm_marker_{field_year}")
        if not silver_path:
            raise FileNotFoundError(f"No FVM marker data found for year {field_year}")

        # Load into temporary table
        temp_table = f"temp_fvm_{field_year}"
        self._load_table_from_gcs(silver_path, temp_table)

        # Get pesticide field lookup for filtering
        pesticide_path = self._get_latest_gold_path("pesticide_disaggregation", pesticide_year)
        if pesticide_path:
            self.log.info(
                f"🔗 Filtering FVM fields to only those with pesticide data from {pesticide_year}"
            )
            temp_pesticide_table = f"temp_pesticide_lookup_{pesticide_year}"
            self._load_table_from_gcs(pesticide_path, temp_pesticide_table)

            # Create lookup table
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE pesticide_field_lookup AS
                SELECT DISTINCT
                    CompanyRegistrationNumber as cvr,
                    REGEXP_EXTRACT(MatchedFieldID, 'marker_(.+)', 1) as field_id,
                    REGEXP_EXTRACT(MatchedBlockID, 'block_(.+)', 1) as block_id
                FROM {temp_pesticide_table}
                WHERE MatchedFieldID IS NOT NULL
                AND MatchedBlockID IS NOT NULL
                AND CompanyRegistrationNumber IS NOT NULL
            """)
            self.conn.execute(f"DROP TABLE IF EXISTS {temp_pesticide_table}")
        else:
            # Create empty lookup
            self.conn.execute("""
                CREATE OR REPLACE TABLE pesticide_field_lookup AS
                SELECT 'dummy' as cvr, 'dummy' as field_id, 'dummy' as block_id
                WHERE FALSE
            """)

        # Check available columns (handle older years and new standardized names)
        columns = self.conn.execute(f"PRAGMA table_info({temp_table})").fetchall()
        column_names = [col[1] for col in columns]

        # Handle CVR column - prioritize new standardized name
        if "cvr_number" in column_names:
            cvr_select = "cvr_number"
        elif "company_registration_number" in column_names:
            cvr_select = "company_registration_number as cvr_number"
        else:
            cvr_select = "NULL as cvr_number"

        # Handle block ID column - prioritize new standardized name
        if "block_id" in column_names:
            block_select = "block_id"
        elif "block_number" in column_names:
            block_select = "block_number as block_id"
        else:
            block_select = "NULL as block_id"

        # Handle area column - prioritize new standardized name with proper casting
        if "area_ha" in column_names:
            area_select = "CAST(area_ha AS DOUBLE) as area_ha"
        elif "field_area_ha" in column_names:
            area_select = "CAST(field_area_ha AS DOUBLE) as area_ha"
        else:
            area_select = "NULL as area_ha"

        self.log.info(f"🔍 Available columns: {column_names}")
        self.log.info(f"🔍 CVR column handling: {cvr_select}")
        self.log.info(f"🔍 Block ID column handling: {block_select}")
        self.log.info(f"🔍 Area column handling: {area_select}")

        # Process fields with geometry preparation
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE prepared_fields AS
            SELECT
                f.field_id,
                {area_select.replace("area_ha", "f.area_ha")},
                {cvr_select.replace("cvr_number", "f.cvr_number").replace("company_registration_number", "f.company_registration_number")},
                {block_select.replace("block_id", "f.block_id").replace("block_number", "f.block_number")},
                f.crop_code,
                f.crop_name,
                f.geometry_wkt
            FROM {temp_table} f
            INNER JOIN pesticide_field_lookup p ON (
                COALESCE(f.cvr_number, f.company_registration_number) = p.cvr
                AND f.field_id = p.field_id
                AND COALESCE(f.block_id, f.block_number) = p.block_id
            )
            WHERE f.geometry_wkt IS NOT NULL
            AND ST_IsValid(ST_GeomFromText(f.geometry_wkt))
            AND COALESCE(CAST(f.area_ha AS DOUBLE), CAST(f.field_area_ha AS DOUBLE)) > 0
            AND COALESCE(f.cvr_number, f.company_registration_number) IS NOT NULL
            AND COALESCE(f.block_id, f.block_number) IS NOT NULL
        """)

        # Use coordinate transformer to prepare geometries
        prepared_table = self.coordinate_transformer.prepare_geometries("prepared_fields")
        self.conn.execute("DROP TABLE prepared_fields")
        self.conn.execute(f"ALTER TABLE {prepared_table} RENAME TO prepared_fields")

        # Clean up temporary table
        self.conn.execute(f"DROP TABLE IF EXISTS {temp_table}")

        count = self.conn.execute("SELECT COUNT(*) FROM prepared_fields").fetchone()[0]
        self.log.info(f"✅ Field data processed: {count:,} fields with geometries")

        return "prepared_fields"

    def _load_pesticide_disaggregation_from_gcs(self, year: int) -> str:
        """Load pesticide disaggregation data from GCS for a specific year."""
        table_name = f"pesticides_{year}"

        self.log.info(f"🧪 Loading pesticide disaggregation for year {year} from GCS")

        # Get the latest pesticide disaggregation file for the year
        full_path = self._get_latest_gold_path("pesticide_disaggregation", year)

        if not full_path:
            raise Exception(f"No pesticide disaggregation data found for year {year}")

        self.log.info(f"📄 Loading pesticides from: {full_path}")

        # Load pesticide data directly from GCS
        self._load_table_from_gcs(full_path, "temp_pesticides_raw")

        # Check available columns and handle both old and new standardized names
        pest_columns = self.conn.execute("PRAGMA table_info(temp_pesticides_raw)").fetchall()
        pest_column_names = [col[1] for col in pest_columns]

        # Handle CVR column mapping
        if "CompanyRegistrationNumber" in pest_column_names:
            cvr_column = "CompanyRegistrationNumber"
        elif "cvr_number" in pest_column_names:
            cvr_column = "cvr_number"
        elif "company_registration_number" in pest_column_names:
            cvr_column = "company_registration_number"
        else:
            raise Exception("No CVR column found in pesticide disaggregation data")

        # Handle pesticide name column
        if "PesticideName" in pest_column_names:
            pesticide_name_column = "PesticideName"
        elif "pesticide_name" in pest_column_names:
            pesticide_name_column = "pesticide_name"
        else:
            pesticide_name_column = "PesticideName"  # fallback

        # Handle pesticide registration number column
        if "PesticideRegistrationNumber" in pest_column_names:
            pesticide_reg_column = "PesticideRegistrationNumber"
        elif "pesticide_registration_number" in pest_column_names:
            pesticide_reg_column = "pesticide_registration_number"
        else:
            pesticide_reg_column = "PesticideRegistrationNumber"  # fallback

        # Handle dosage quantity column
        if "DosageQuantity" in pest_column_names:
            dosage_quantity_column = "DosageQuantity"
        elif "dosage_quantity" in pest_column_names:
            dosage_quantity_column = "dosage_quantity"
        else:
            dosage_quantity_column = "DosageQuantity"  # fallback

        # Handle dosage unit column
        if "DosageUnit" in pest_column_names:
            dosage_unit_column = "DosageUnit"
        elif "dosage_unit" in pest_column_names:
            dosage_unit_column = "dosage_unit"
        else:
            dosage_unit_column = "DosageUnit"  # fallback

        self.log.info("🔍 Pesticide column mappings:")
        self.log.info(f"   CVR: {cvr_column}")
        self.log.info(f"   Pesticide name: {pesticide_name_column}")
        self.log.info(f"   Registration number: {pesticide_reg_column}")
        self.log.info(f"   Dosage quantity: {dosage_quantity_column}")
        self.log.info(f"   Dosage unit: {dosage_unit_column}")

        # Process pesticide data with correct field names and CVR + block_id + field_id extraction
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT
                DisaggregatedID,
                MatchedFieldID,
                MatchedBlockID,
                {cvr_column} as cvr,
                {pesticide_name_column} as PesticideName,
                {pesticide_reg_column} as PesticideRegistrationNumber,
                {dosage_quantity_column} as DosageQuantity,
                {dosage_unit_column} as DosageUnit,
                COALESCE(AllocatedArea, allocated_area) as AllocatedArea,
                COALESCE(AllocationMethod, allocation_method) as AllocationMethod,
                COALESCE(MatchConfidence, match_confidence) as MatchConfidence,
                -- Extract field_id and block_id for matching
                REGEXP_EXTRACT(MatchedFieldID, 'marker_(.+)', 1) as extracted_field_id,
                REGEXP_EXTRACT(MatchedBlockID, 'block_(.+)', 1) as extracted_block_id
            FROM temp_pesticides_raw
            WHERE MatchedFieldID IS NOT NULL
            AND MatchedBlockID IS NOT NULL
            AND {cvr_column} IS NOT NULL
            AND {pesticide_reg_column} IS NOT NULL
        """)

        # Get count for logging
        count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        self.log.info(f"✅ Loaded {count:,} pesticide disaggregation records for year {year}")

        return table_name

    def _join_pesticide_with_bmd_pfas(self, pesticide_table: str, bmd_table: str, year: int) -> str:
        """Join pesticide disaggregation with BMD data for PFAS detection."""
        pesticide_pfas_table = f"pesticide_pfas_{year}"

        self.log.info(f"🧪 Joining pesticide data with BMD PFAS indicators for year {year}")

        self.conn.execute(f"""
            CREATE OR REPLACE TABLE {pesticide_pfas_table} AS
            SELECT
                p.DisaggregatedID,
                p.MatchedFieldID,
                p.MatchedBlockID,
                p.cvr,
                p.extracted_field_id,
                p.extracted_block_id,
                p.PesticideName,
                p.PesticideRegistrationNumber,
                p.DosageQuantity,
                p.DosageUnit,
                p.AllocatedArea,
                p.AllocationMethod,
                p.MatchConfidence,

                -- BMD PFAS data
                b.active_ingredient,
                b.total_load_per_unit,
                COALESCE(b.contains_pfas, false) as contains_pfas,

                -- Calculate actual PFAS-containing active ingredient amount applied (grams) with proper unit conversion
                CASE
                    WHEN b.contains_pfas = true AND b.concentration_numeric IS NOT NULL THEN
                        CASE
                            -- Liter dosage (Unit 4) with g/l concentration: L × g/l = g
                            WHEN p.DosageUnit = 4 AND b.enhed_er LIKE '%g/l%' THEN
                                p.DosageQuantity * b.concentration_numeric / 1000.0

                            -- Kg dosage (Unit 2) with g/kg concentration: kg × g/kg = g
                            WHEN p.DosageUnit = 2 AND b.enhed_er LIKE '%g/kg%' THEN
                                p.DosageQuantity * b.concentration_numeric / 1000.0

                            ELSE 0
                        END
                    ELSE 0
                END as pfas_containing_active_ingredient_grams,

                -- Pesticide load applied
                CASE
                    WHEN b.total_load_per_unit IS NOT NULL THEN
                        p.DosageQuantity * b.total_load_per_unit
                    ELSE 0
                END as pesticide_belastning_applied,

                -- PFAS-containing pesticide load
                CASE
                    WHEN b.contains_pfas = true AND b.total_load_per_unit IS NOT NULL THEN
                        p.DosageQuantity * b.total_load_per_unit
                    ELSE 0
                END as pfas_containing_pesticide_belastning_applied

            FROM {pesticide_table} p
            LEFT JOIN {bmd_table} b ON (
                p.PesticideRegistrationNumber = b.registrerings_nr
                OR LOWER(p.PesticideName) = LOWER(b.produktnavn)
            )
        """)

        # Get statistics for logging
        total_count = self.conn.execute(f"SELECT COUNT(*) FROM {pesticide_pfas_table}").fetchone()[
            0
        ]
        pfas_count = self.conn.execute(
            f"SELECT COUNT(*) FROM {pesticide_pfas_table} WHERE contains_pfas = true"
        ).fetchone()[0]

        self.log.info(
            f"✅ Pesticide-BMD join completed: {total_count:,} records, {pfas_count:,} with PFAS-containing active ingredients ({pfas_count / total_count * 100:.1f}%)"
        )

        return pesticide_pfas_table

    def _save_year_results_kepler_compatible(self, results_table: str, year: int) -> int:
        """Save results to GCS with Kepler.gl compatibility fixes."""
        self.log.info(
            f"💾 Saving Kepler.gl-compatible H3 pesticide exposure results for year {year} (resolution {self.config.h3_resolution}) to GCS"
        )

        # Create Kepler.gl compatible version by converting BigInt columns to regular numbers
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE final_results_kepler_{year} AS
            SELECT
                -- Convert H3 cell to string format with correct column name for Kepler.gl H3 layer auto-detection
                CAST(h3_cell AS VARCHAR) as h3_id,
                CAST(center_lat AS DOUBLE) as center_lat,
                CAST(center_lon AS DOUBLE) as center_lon,
                CAST(h3_cell_area_ha AS DOUBLE) as h3_cell_area_ha,
                CAST(total_intersection_area_ha AS DOUBLE) as total_intersection_area_ha,
                CAST(actual_coverage_ratio AS DOUBLE) as actual_coverage_ratio,

                -- Convert BigInt counts to regular integers (32-bit max: 2.1 billion)
                CAST(unique_field_count AS INTEGER) as unique_field_count,
                CAST(total_pesticide_applications AS INTEGER) as total_pesticide_applications,
                CAST(pfas_containing_applications AS INTEGER) as pfas_containing_applications,
                CAST(crop_diversity AS INTEGER) as crop_diversity,

                -- PFAS-containing active ingredient exposure metrics as doubles
                CAST(total_pfas_containing_active_ingredient_grams AS DOUBLE) as total_pfas_containing_active_ingredient_grams,
                CAST(total_pesticide_belastning AS DOUBLE) as total_pesticide_belastning,
                CAST(pfas_containing_active_ingredient_intensity_grams_per_ha AS DOUBLE) as pfas_containing_active_ingredient_intensity_grams_per_ha,

                -- String fields (no conversion needed)
                crop_types,

                -- Timestamp as string for better compatibility
                CAST(created_at AS VARCHAR) as created_at
            FROM {results_table}
            ORDER BY h3_cell
        """)

        # Create output path for Kepler-compatible version with resolution in filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path_kepler = f"gs://{self.config.bucket}/gold/h3_pesticide_{year}_res{self.config.h3_resolution}/{timestamp}/h3_pesticide_{year}_res{self.config.h3_resolution}_kepler.parquet"

        # Upload Kepler-compatible table
        self.gcs_access.upload_from_duckdb_table(f"final_results_kepler_{year}", output_path_kepler)

        # Also save the original version with BigInt columns
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE final_results_{year} AS
            SELECT
                *,
                CURRENT_TIMESTAMP as created_at
            FROM {results_table}
            ORDER BY h3_cell
        """)

        output_path_original = f"gs://{self.config.bucket}/gold/h3_pesticide_{year}_res{self.config.h3_resolution}/{timestamp}/h3_pesticide_{year}_res{self.config.h3_resolution}.parquet"
        self.gcs_access.upload_from_duckdb_table(f"final_results_{year}", output_path_original)

        # Get count for return
        count = self.conn.execute(f"SELECT COUNT(*) FROM final_results_{year}").fetchone()[0]

        self.log.info(
            f"✅ Saved {count:,} H3 pesticide exposure records for year {year} (resolution {self.config.h3_resolution})"
        )
        self.log.info(f"   📊 Original format: {output_path_original}")
        self.log.info(f"   🗺️  Kepler.gl compatible: {output_path_kepler}")

        return count

    def _cleanup_year_tables(self, year: int):
        """Clean up intermediate tables for a specific year to free memory."""
        tables_to_drop = [
            f"pesticides_{year}",
            f"pesticide_pfas_{year}",
            f"final_results_{year}",
            "prepared_fields",
            "pesticide_field_lookup",
        ]

        for table in tables_to_drop:
            try:
                self.conn.execute(f"DROP TABLE IF EXISTS {table}")
            except Exception:
                pass  # Ignore errors if table doesn't exist

    def _load_table_from_gcs(self, gcs_path: str, table_name: str):
        """Load data from GCS into a DuckDB table using optimized GCS access."""
        try:
            # Use the optimized download approach with our DuckDB connection
            with self.gcs_access._temp_download(gcs_path) as temp_file:
                self.conn.execute(f"""
                    CREATE OR REPLACE TABLE {table_name} AS
                    SELECT * FROM read_parquet('{temp_file}')
                """)
            self.log.debug(f"✅ Loaded {table_name} from {gcs_path}")
        except Exception as e:
            self.log.error(f"❌ Failed to load {table_name} from {gcs_path}: {e}")
            raise

    def _get_latest_silver_path(self, dataset: str) -> str | None:
        """Get the latest silver data path for a dataset."""
        try:
            bucket_name = self.config.bucket
            prefix = f"gs://{bucket_name}/silver/{dataset}/"

            # Get all files in the dataset directory (recursive)
            files = self.gcs_access.list_files(f"{prefix}**")

            # For BMD data, look for timestamped directories
            if dataset == "bmd":
                # Find all timestamped directories
                timestamp_dirs = set()
                for file_path in files:
                    path_parts = file_path.replace(prefix, "").split("/")
                    if len(path_parts) >= 1 and path_parts[0]:
                        timestamp_dirs.add(path_parts[0])

                if not timestamp_dirs:
                    return None

                # Get the latest timestamp directory
                latest_timestamp = sorted(timestamp_dirs)[-1]

                # Look for pesticide_products.parquet in the latest directory
                latest_files = self.gcs_access.list_files(f"{prefix}{latest_timestamp}/*")

                for file_path in latest_files:
                    if file_path.endswith("pesticide_products.parquet"):
                        return file_path

                return None

            else:
                # For other datasets, use the original logic
                parquet_files = [
                    file_path
                    for file_path in files
                    if file_path.endswith(".parquet") or file_path.endswith("data.parquet")
                ]

                if not parquet_files:
                    return None

                # Sort by timestamp and get latest
                latest_file = sorted(parquet_files)[-1]
                return latest_file

        except Exception as e:
            self.log.debug(f"Error getting latest silver path for {dataset}: {e}")
            return None

    def _get_latest_gold_path(self, dataset: str, year: int) -> str | None:
        """Get the latest gold data path for a dataset and year."""
        try:
            bucket_name = self.config.bucket
            prefix = f"gs://{bucket_name}/gold/{dataset}_{year}/"

            # Get all files in the dataset directory (recursive)
            files = self.gcs_access.list_files(f"{prefix}**")

            # Find all timestamped directories
            timestamp_dirs = set()
            for file_path in files:
                path_parts = file_path.replace(prefix, "").split("/")
                if len(path_parts) >= 1 and path_parts[0]:
                    timestamp_dirs.add(path_parts[0])

            if not timestamp_dirs:
                return None

            # Get the latest timestamp directory
            latest_timestamp = sorted(timestamp_dirs)[-1]

            # Look for the parquet file in the latest directory
            latest_files = self.gcs_access.list_files(f"{prefix}{latest_timestamp}/*")

            for file_path in latest_files:
                if file_path.endswith(f"{dataset}_{year}.parquet"):
                    return file_path

            return None

        except Exception as e:
            self.log.debug(f"Error getting latest gold path for {dataset}_{year}: {e}")
            return None

    def _validate_results(self, results_table: str):
        """Validate the analysis results."""
        self.log.info("🔍 Validating analysis results...")

        # Validate H3 cell areas
        area_validation = self.area_validator.validate_h3_cell_areas(results_table)
        if area_validation.passed:
            self.log.info(f"✅ {area_validation.name}: {area_validation.message}")
        else:
            self.log.warning(f"⚠️ {area_validation.name}: {area_validation.message}")

        # Validate intersection areas
        intersection_validation = self.area_validator.validate_intersection_areas(results_table)
        if intersection_validation.passed:
            self.log.info(f"✅ {intersection_validation.name}: {intersection_validation.message}")
        else:
            self.log.warning(f"⚠️ {intersection_validation.name}: {intersection_validation.message}")

        # Get summary statistics
        stats = self.conn.execute(f"""
            SELECT
                COUNT(*) as total_h3_cells,
                SUM(unique_field_count) as total_field_intersections,
                SUM(pfas_containing_applications) as total_pfas_containing_applications,
                SUM(total_pfas_containing_active_ingredient_grams) as total_pfas_containing_active_ingredient_grams,
                SUM(total_intersection_area_ha) as total_area_ha,
                AVG(actual_coverage_ratio) as avg_coverage_ratio
            FROM {results_table}
        """).fetchone()

        self.log.info("📊 Analysis Summary:")
        self.log.info(f"   🗺️  H3 cells with agriculture: {stats[0]:,}")
        self.log.info(f"   🔗 Field intersections: {stats[1]:,}")
        self.log.info(f"   🧪 PFAS-containing applications: {stats[2]:,}")
        self.log.info(f"   ⚗️  Total PFAS-containing active ingredients: {stats[3]:,.2f} grams")
        self.log.info(f"   📐 Total agricultural area: {stats[4]:,.2f} hectares")
        self.log.info(f"   📊 Average coverage ratio: {stats[5]:.3f}")

    def _load_dagi_kommuner_data(self) -> str:
        """Load DAGI kommuner (municipality) data from GCS silver layer."""
        self.log.info("🏛️ Loading DAGI kommuner data from GCS silver layer")

        # Get the latest DAGI kommuner data from silver layer
        kommuner_path = self._get_latest_silver_path("dagi_kommuner")
        if not kommuner_path:
            raise Exception("DAGI kommuner data not found in silver layer")

        self.log.info(f"📄 Loading DAGI kommuner data from: {kommuner_path}")

        # Load kommuner data directly from GCS
        self._load_table_from_gcs(kommuner_path, "temp_dagi_kommuner_raw")

        # Process kommuner data with geometry preparation
        self.conn.execute("""
            CREATE OR REPLACE TABLE dagi_kommuner AS
            SELECT
                code as kommune_code,
                name as kommune_name,
                region_code,
                geometry_wkt,
                -- Prepare geometries for spatial operations
                ST_GeomFromText(geometry_wkt) as original_geometry,
                ST_FlipCoordinates(ST_GeomFromText(geometry_wkt)) as flipped_geometry,
                area_m2 / 10000.0 as kommune_area_ha,
                centroid_x,
                centroid_y,
                is_valid_geometry
            FROM temp_dagi_kommuner_raw
            WHERE geometry_wkt IS NOT NULL
            AND is_valid_geometry = true
            AND ST_IsValid(ST_GeomFromText(geometry_wkt))
        """)

        # Clean up temporary table
        self.conn.execute("DROP TABLE IF EXISTS temp_dagi_kommuner_raw")

        # Get statistics for logging
        total_count = self.conn.execute("SELECT COUNT(*) FROM dagi_kommuner").fetchone()[0]

        self.log.info(f"✅ DAGI kommuner data loaded: {total_count:,} municipalities")

        return "dagi_kommuner"

    async def _process_single_year_kommune_analysis(self, year: int, bmd_table: str) -> int:
        """Process a single year for kommune-level analysis with area weighting."""
        self.log.info(f"🏛️ Processing kommune-level PFAS analysis for year {year}")

        # Step 1: Load DAGI kommuner data
        kommuner_table = self._load_dagi_kommuner_data()

        # Step 2: Load and prepare field data (Y+1 pattern)
        field_year = year + 1
        fields_table = self._load_and_prepare_fields_from_gcs(field_year, year)

        # Step 3: Load pesticide disaggregation for year Y
        pesticide_table = self._load_pesticide_disaggregation_from_gcs(year)

        # Step 4: Join pesticide data with BMD for PFAS detection
        pesticide_pfas_table = self._join_pesticide_with_bmd_pfas(pesticide_table, bmd_table, year)

        # Step 5: Perform kommune-field spatial join with area weighting
        kommune_results_table = self._perform_kommune_spatial_join(
            kommuner_table, fields_table, pesticide_pfas_table, year
        )

        # Step 6: Save kommune results
        result_count = self._save_kommune_results(kommune_results_table, year)

        # Step 7: Clean up intermediate tables
        self._cleanup_kommune_tables(year)

        return result_count

    def _perform_kommune_spatial_join(
        self, kommuner_table: str, fields_table: str, pesticide_pfas_table: str, year: int
    ) -> str:
        """Perform spatial join between kommuner and fields with proper area weighting."""
        self.log.info(
            f"🔗 Performing kommune-field spatial join with area weighting for year {year}"
        )

        # Debug: Check data availability
        kommuner_count = self.conn.execute(f"SELECT COUNT(*) FROM {kommuner_table}").fetchone()[0]
        fields_count = self.conn.execute(f"SELECT COUNT(*) FROM {fields_table}").fetchone()[0]
        pesticide_count = self.conn.execute(
            f"SELECT COUNT(*) FROM {pesticide_pfas_table}"
        ).fetchone()[0]

        self.log.info(
            f"🔍 Debug - Data counts: {kommuner_count} kommuner, {fields_count} fields, {pesticide_count} pesticide records"
        )

        # Debug: Check geometry validity
        invalid_kommuner = self.conn.execute(f"""
            SELECT COUNT(*) FROM {kommuner_table} 
            WHERE original_geometry IS NULL OR NOT ST_IsValid(original_geometry)
        """).fetchone()[0]

        invalid_fields = self.conn.execute(f"""
            SELECT COUNT(*) FROM {fields_table} 
            WHERE original_geometry IS NULL OR NOT ST_IsValid(original_geometry)
        """).fetchone()[0]

        self.log.info(
            f"🔍 Debug - Invalid geometries: {invalid_kommuner} kommuner, {invalid_fields} fields"
        )

        # Debug: Test basic spatial intersection without area filters
        basic_intersections = self.conn.execute(f"""
            SELECT COUNT(*) FROM {kommuner_table} k
            INNER JOIN {fields_table} f ON ST_Intersects(k.original_geometry, f.flipped_geometry)
        """).fetchone()[0]

        self.log.info(
            f"🔍 Debug - Basic spatial intersections found (corrected): {basic_intersections}"
        )

        # Debug: Check coordinate bounds
        kommune_bounds = self.conn.execute(f"""
            SELECT 
                MIN(ST_X(ST_Centroid(original_geometry))) as min_x,
                MAX(ST_X(ST_Centroid(original_geometry))) as max_x,
                MIN(ST_Y(ST_Centroid(original_geometry))) as min_y,
                MAX(ST_Y(ST_Centroid(original_geometry))) as max_y
            FROM {kommuner_table}
        """).fetchone()

        field_bounds = self.conn.execute(f"""
            SELECT 
                MIN(ST_X(ST_Centroid(original_geometry))) as min_x,
                MAX(ST_X(ST_Centroid(original_geometry))) as max_x,
                MIN(ST_Y(ST_Centroid(original_geometry))) as min_y,
                MAX(ST_Y(ST_Centroid(original_geometry))) as max_y
            FROM {fields_table}
        """).fetchone()

        self.log.info(
            f"🔍 Debug - Kommune bounds: X({kommune_bounds[0]:.2f}, {kommune_bounds[1]:.2f}), Y({kommune_bounds[2]:.2f}, {kommune_bounds[3]:.2f})"
        )
        self.log.info(
            f"🔍 Debug - Field bounds: X({field_bounds[0]:.2f}, {field_bounds[1]:.2f}), Y({field_bounds[2]:.2f}, {field_bounds[3]:.2f})"
        )

        # Debug: Check geometry types and sample geometries
        sample_kommune_geom = self.conn.execute(f"""
            SELECT ST_AsText(ST_Centroid(original_geometry)) as centroid_wkt
            FROM {kommuner_table} 
            LIMIT 1
        """).fetchone()[0]

        sample_field_geom = self.conn.execute(f"""
            SELECT ST_AsText(ST_Centroid(original_geometry)) as centroid_wkt
            FROM {fields_table} 
            LIMIT 1
        """).fetchone()[0]

        self.log.info(f"🔍 Debug - Sample kommune centroid: {sample_kommune_geom}")
        self.log.info(f"🔍 Debug - Sample field centroid: {sample_field_geom}")

        # Debug: Try intersection with original geometries instead of flipped
        original_intersections = self.conn.execute(f"""
            SELECT COUNT(*) FROM {kommuner_table} k
            INNER JOIN {fields_table} f ON ST_Intersects(k.original_geometry, f.original_geometry)
        """).fetchone()[0]

        self.log.info(f"🔍 Debug - Original geometry intersections: {original_intersections}")

        # Debug: Check if flipped geometries exist
        kommune_flipped_count = self.conn.execute(f"""
            SELECT COUNT(*) FROM {kommuner_table} 
            WHERE flipped_geometry IS NOT NULL
        """).fetchone()[0]

        field_flipped_count = self.conn.execute(f"""
            SELECT COUNT(*) FROM {fields_table} 
            WHERE flipped_geometry IS NOT NULL
        """).fetchone()[0]

        self.log.info(
            f"🔍 Debug - Flipped geometries: {kommune_flipped_count} kommuner, {field_flipped_count} fields"
        )

        result_table = f"kommune_pfas_results_{year}"

        # Complex spatial join query with area weighting
        query = f"""
        CREATE OR REPLACE TABLE {result_table} AS
        WITH kommune_field_intersections AS (
            -- Find all field-kommune intersections with area calculations
            SELECT
                k.kommune_code,
                k.kommune_name,
                k.region_code,
                k.kommune_area_ha,
                k.centroid_x as kommune_centroid_x,
                k.centroid_y as kommune_centroid_y,
                f.field_id,
                f.cvr_number,
                f.block_id,
                f.area_ha as field_area_ha,
                f.crop_code,
                f.crop_name,

                -- Calculate intersection area between field and kommune
                ST_Area_Spheroid(ST_Intersection(
                    f.flipped_geometry,
                    k.original_geometry
                )) / 10000.0 as intersection_area_ha,

                -- Calculate field area for coverage ratio
                ST_Area_Spheroid(f.flipped_geometry) / 10000.0 as calculated_field_area_ha,

                -- Calculate coverage ratio: what fraction of the field is in this kommune
                CASE
                    WHEN ST_Area_Spheroid(f.flipped_geometry) > 0 THEN
                        LEAST(1.0, ST_Area_Spheroid(ST_Intersection(
                            f.flipped_geometry,
                            k.original_geometry
                        )) / ST_Area_Spheroid(f.flipped_geometry))
                    ELSE 0.0
                END as field_coverage_ratio

            FROM {kommuner_table} k
            INNER JOIN {fields_table} f ON ST_Intersects(k.original_geometry, f.flipped_geometry)
            WHERE ST_Area_Spheroid(ST_Intersection(
                f.flipped_geometry,
                k.original_geometry
            )) > 0  -- Only include actual intersections
        ),
        kommune_field_pesticide AS (
            -- Join field intersections with pesticide data
            SELECT
                kfi.*,
                p.PesticideRegistrationNumber,
                p.DosageQuantity,
                p.DosageUnit,
                p.contains_pfas,
                p.pfas_containing_active_ingredient_grams,
                p.pesticide_belastning_applied,
                p.pfas_containing_pesticide_belastning_applied,

                -- Calculate area-weighted PFAS exposure (weight by field coverage in this kommune)
                CASE
                    WHEN p.contains_pfas = true AND p.pfas_containing_active_ingredient_grams IS NOT NULL THEN
                        p.pfas_containing_active_ingredient_grams * kfi.field_coverage_ratio
                    ELSE 0
                END as weighted_pfas_containing_active_ingredient_grams,

                -- Calculate area-weighted pesticide load
                CASE
                    WHEN p.pesticide_belastning_applied IS NOT NULL THEN
                        p.pesticide_belastning_applied * kfi.field_coverage_ratio
                    ELSE 0
                END as weighted_pesticide_belastning,

                -- Calculate area-weighted PFAS pesticide load
                CASE
                    WHEN p.contains_pfas = true AND p.pfas_containing_pesticide_belastning_applied IS NOT NULL THEN
                        p.pfas_containing_pesticide_belastning_applied * kfi.field_coverage_ratio
                    ELSE 0
                END as weighted_pfas_pesticide_belastning

            FROM kommune_field_intersections kfi
            LEFT JOIN {pesticide_pfas_table} p ON (
                kfi.cvr_number = p.cvr
                AND kfi.field_id = p.extracted_field_id
                AND kfi.block_id = p.extracted_block_id
            )
        )
        -- Final kommune-level aggregation
        SELECT
            kommune_code,
            kommune_name,
            region_code,
            kommune_area_ha,
            kommune_centroid_x,
            kommune_centroid_y,

            -- Agricultural area and field statistics
            SUM(intersection_area_ha) as total_agricultural_area_ha,
            COUNT(DISTINCT CONCAT(cvr_number, '_', block_id, '_', field_id)) as unique_field_count,
            COUNT(DISTINCT cvr_number) as unique_company_count,

            -- Coverage statistics
            AVG(field_coverage_ratio) as avg_field_coverage_ratio,
            MAX(field_coverage_ratio) as max_field_coverage_ratio,
            MIN(field_coverage_ratio) as min_field_coverage_ratio,

            -- Crop diversity
            COUNT(DISTINCT crop_code) as crop_diversity,
            STRING_AGG(DISTINCT crop_name, '; ') as crop_types,

            -- PFAS exposure metrics (area-weighted)
            SUM(COALESCE(weighted_pfas_containing_active_ingredient_grams, 0)) as total_pfas_containing_active_ingredient_grams,
            SUM(COALESCE(weighted_pesticide_belastning, 0)) as total_pesticide_belastning,
            SUM(COALESCE(weighted_pfas_pesticide_belastning, 0)) as total_pfas_pesticide_belastning,

            -- Application counts
            COUNT(CASE WHEN PesticideRegistrationNumber IS NOT NULL THEN 1 END) as total_pesticide_applications,
            COUNT(CASE WHEN contains_pfas = true THEN 1 END) as pfas_containing_applications,
            COUNT(DISTINCT CASE WHEN contains_pfas = true THEN PesticideRegistrationNumber END) as unique_pfas_products,
            COUNT(DISTINCT PesticideRegistrationNumber) as unique_pesticide_products,

            -- Intensity metrics per hectare
            CASE
                WHEN SUM(intersection_area_ha) > 0 THEN
                    SUM(COALESCE(weighted_pfas_containing_active_ingredient_grams, 0)) / SUM(intersection_area_ha)
                ELSE 0
            END as pfas_containing_active_ingredient_intensity_grams_per_ha,

            CASE
                WHEN SUM(intersection_area_ha) > 0 THEN
                    SUM(COALESCE(weighted_pesticide_belastning, 0)) / SUM(intersection_area_ha)
                ELSE 0
            END as pesticide_belastning_per_ha,

            CASE
                WHEN SUM(intersection_area_ha) > 0 THEN
                    SUM(COALESCE(weighted_pfas_pesticide_belastning, 0)) / SUM(intersection_area_ha)
                ELSE 0
            END as pfas_pesticide_belastning_per_ha,

            -- Agricultural coverage of kommune
            CASE
                WHEN kommune_area_ha > 0 THEN
                    SUM(intersection_area_ha) / kommune_area_ha * 100
                ELSE 0
            END as agricultural_coverage_pct,

            -- Timestamp
            CURRENT_TIMESTAMP as created_at

        FROM kommune_field_pesticide
        GROUP BY kommune_code, kommune_name, region_code, kommune_area_ha,
                 kommune_centroid_x, kommune_centroid_y
        HAVING SUM(intersection_area_ha) > 0  -- Only kommuner with agricultural activity
        ORDER BY total_pfas_containing_active_ingredient_grams DESC
        """

        self.conn.execute(query)

        # Log results
        count = self.conn.execute(f"SELECT COUNT(*) FROM {result_table}").fetchone()[0]
        count = count or 0  # Handle None case

        # Get summary statistics
        stats = self.conn.execute(f"""
            SELECT
                COUNT(*) as total_kommuner,
                COALESCE(SUM(unique_field_count), 0) as total_field_intersections,
                COALESCE(SUM(pfas_containing_applications), 0) as total_pfas_applications,
                COALESCE(SUM(total_pfas_containing_active_ingredient_grams), 0) as total_pfas_grams,
                COALESCE(SUM(total_agricultural_area_ha), 0) as total_agricultural_area,
                COALESCE(AVG(agricultural_coverage_pct), 0) as avg_agricultural_coverage
            FROM {result_table}
        """).fetchone()

        self.log.info(
            f"✅ Kommune spatial join completed: {count:,} municipalities with agricultural activity"
        )
        self.log.info(f"   🔗 Field intersections: {stats[1]:,}")
        self.log.info(f"   🧪 PFAS-containing applications: {stats[2]:,}")
        self.log.info(f"   ⚗️  Total PFAS-containing active ingredients: {stats[3]:,.2f} grams")
        self.log.info(f"   📐 Total agricultural area: {stats[4]:,.2f} hectares")
        self.log.info(f"   🌾 Average agricultural coverage: {stats[5]:.1f}%")

        return result_table

    def _save_kommune_results(self, results_table: str, year: int) -> int:
        """Save kommune-level results to GCS."""
        self.log.info(f"💾 Saving kommune-level pesticide exposure results for year {year} to GCS")

        # Create final results table
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE final_kommune_results_{year} AS
            SELECT
                kommune_code,
                kommune_name,
                region_code,
                CAST(kommune_area_ha AS DOUBLE) as kommune_area_ha,
                CAST(kommune_centroid_x AS DOUBLE) as kommune_centroid_x,
                CAST(kommune_centroid_y AS DOUBLE) as kommune_centroid_y,
                CAST(total_agricultural_area_ha AS DOUBLE) as total_agricultural_area_ha,
                CAST(unique_field_count AS INTEGER) as unique_field_count,
                CAST(unique_company_count AS INTEGER) as unique_company_count,
                CAST(avg_field_coverage_ratio AS DOUBLE) as avg_field_coverage_ratio,
                CAST(max_field_coverage_ratio AS DOUBLE) as max_field_coverage_ratio,
                CAST(min_field_coverage_ratio AS DOUBLE) as min_field_coverage_ratio,
                CAST(crop_diversity AS INTEGER) as crop_diversity,
                crop_types,
                CAST(total_pfas_containing_active_ingredient_grams AS DOUBLE) as total_pfas_containing_active_ingredient_grams,
                CAST(total_pesticide_belastning AS DOUBLE) as total_pesticide_belastning,
                CAST(total_pfas_pesticide_belastning AS DOUBLE) as total_pfas_pesticide_belastning,
                CAST(total_pesticide_applications AS INTEGER) as total_pesticide_applications,
                CAST(pfas_containing_applications AS INTEGER) as pfas_containing_applications,
                CAST(unique_pfas_products AS INTEGER) as unique_pfas_products,
                CAST(unique_pesticide_products AS INTEGER) as unique_pesticide_products,
                CAST(pfas_containing_active_ingredient_intensity_grams_per_ha AS DOUBLE) as pfas_containing_active_ingredient_intensity_grams_per_ha,
                CAST(pesticide_belastning_per_ha AS DOUBLE) as pesticide_belastning_per_ha,
                CAST(pfas_pesticide_belastning_per_ha AS DOUBLE) as pfas_pesticide_belastning_per_ha,
                CAST(agricultural_coverage_pct AS DOUBLE) as agricultural_coverage_pct,
                CAST(created_at AS VARCHAR) as created_at
            FROM {results_table}
            ORDER BY total_pfas_containing_active_ingredient_grams DESC
        """)

        # Create output paths
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path_parquet = f"gs://{self.config.bucket}/gold/kommune_pesticide_{year}/{timestamp}/kommune_pesticide_{year}.parquet"
        output_path_csv = f"gs://{self.config.bucket}/gold/kommune_pesticide_{year}/{timestamp}/kommune_pesticide_{year}.csv"

        # Upload to GCS in both formats
        self.gcs_access.upload_from_duckdb_table(
            f"final_kommune_results_{year}", output_path_parquet
        )
        self.gcs_access.upload_from_duckdb_table(f"final_kommune_results_{year}", output_path_csv)

        # Get count for return
        count = self.conn.execute(f"SELECT COUNT(*) FROM final_kommune_results_{year}").fetchone()[
            0
        ]

        self.log.info(f"✅ Saved {count:,} kommune pesticide exposure records for year {year}")
        self.log.info(f"   📊 Parquet format: {output_path_parquet}")
        self.log.info(f"   📄 CSV format: {output_path_csv}")

        return count

    def _cleanup_kommune_tables(self, year: int):
        """Clean up intermediate tables for kommune analysis."""
        tables_to_drop = [
            "dagi_kommuner",
            f"kommune_pfas_results_{year}",
            f"final_kommune_results_{year}",
        ]

        for table in tables_to_drop:
            try:
                self.conn.execute(f"DROP TABLE IF EXISTS {table}")
            except Exception:
                pass  # Ignore errors if table doesn't exist

    async def run_kommune_analysis_multi_year(self, years: list[int] | None = None) -> bool:
        """Run kommune-level PFAS analysis for multiple years from GCS data."""
        self.log.info("🏛️ Starting multi-year kommune-level PFAS analysis from GCS")

        # Use provided years or default from config
        years_to_process = years or self.config.available_years
        self.log.info(f"📅 Processing years: {years_to_process}")

        # Setup DuckDB
        self.setup_duckdb()

        if not self.gcs_access:
            self.log.error("❌ GCS access not available - cannot load cloud data")
            return False

        # Load BMD data once for all years (PFAS detection data)
        self.log.info("🧪 Loading BMD pesticide data with PFAS indicators...")
        bmd_table = self._load_bmd_data_from_gcs()
        self.log.info(f"✅ BMD data loaded: {bmd_table}")

        successful_years = 0
        failed_years = 0

        # Process each year
        for year in years_to_process:
            self.log.info("=" * 80)
            self.log.info(f"🏛️ Processing kommune analysis for year {year}")
            self.log.info("=" * 80)

            try:
                # Check data availability for this year
                if not self._check_year_data_availability(year):
                    self.log.warning(f"⚠️ Skipping year {year}: missing required data")
                    failed_years += 1
                    continue

                # Process single year for kommune analysis
                result_count = await self._process_single_year_kommune_analysis(year, bmd_table)

                if result_count > 0:
                    self.log.info(
                        f"✅ Year {year}: Successfully processed {result_count:,} municipalities"
                    )
                    successful_years += 1
                else:
                    self.log.warning(f"⚠️ Year {year}: No results generated")
                    failed_years += 1

            except Exception as e:
                self.log.error(f"❌ Failed to process year {year}: {e}")
                import traceback

                self.log.error(f"📋 Traceback: {traceback.format_exc()}")
                failed_years += 1

        # Summary
        self.log.info("=" * 80)
        self.log.info("📊 Multi-Year Kommune PFAS Analysis Summary")
        self.log.info("=" * 80)
        self.log.info(f"✅ Successfully processed: {successful_years} years")
        self.log.info(f"❌ Failed to process: {failed_years} years")
        total_years = successful_years + failed_years
        if total_years > 0:
            self.log.info(f"📈 Success rate: {successful_years / total_years * 100:.1f}%")
        self.log.info("🎉 Multi-year kommune PFAS analysis completed")

        return successful_years > 0

    def load_test_data(self):
        """Load test data from local files."""
        if not self.local_data_dir:
            raise ValueError("Local data directory not specified")

        # Load BMD data
        bmd_path = self.local_data_dir / "bmd_pesticide_products.parquet"
        if not bmd_path.exists():
            raise FileNotFoundError(f"BMD data not found: {bmd_path}")

        self.conn.execute(f"CREATE OR REPLACE TABLE temp_bmd_raw AS SELECT * FROM '{bmd_path}'")
        self._process_bmd_data()

        # Load pesticide data FIRST (needed for field filtering)
        pesticide_path = self.local_data_dir / "pesticide_disaggregation_2022.parquet"
        if not pesticide_path.exists():
            raise FileNotFoundError(f"Pesticide data not found: {pesticide_path}")

        self.conn.execute(
            f"CREATE OR REPLACE TABLE temp_pesticide_raw AS SELECT * FROM '{pesticide_path}'"
        )

        # Load FVM marker data (depends on pesticide data for filtering)
        fvm_path = self.local_data_dir / "fvm_marker_2023.parquet"
        if not fvm_path.exists():
            raise FileNotFoundError(f"FVM data not found: {fvm_path}")

        self.conn.execute(f"CREATE OR REPLACE TABLE temp_fvm_raw AS SELECT * FROM '{fvm_path}'")
        self._process_field_data()

        # Process pesticide data (after field processing)
        self._process_pesticide_data()

        self.log.info("✅ Test data loaded successfully")

    def _process_bmd_data(self):
        """Process BMD data with PFAS indicators."""
        self.conn.execute("""
            CREATE OR REPLACE TABLE bmd_data AS
            SELECT
                produktnavn,
                registrerings_nr,
                aktivstofnavn_e as active_ingredient,
                koncentration_er,
                enhed_er,
                samlet_belastning as total_load_per_unit,
                belastning_miljøeffekt as environmental_effect_per_unit,
                belastning_miljøadfærd as environmental_behavior_per_unit,
                belastning_sundhed as health_effect_per_unit,
                contains_pfas,
                TRY_CAST(REPLACE(REPLACE(koncentration_er, ',', '.'), ' ', '') AS DOUBLE) as concentration_numeric
            FROM temp_bmd_raw
            WHERE registrerings_nr IS NOT NULL
        """)

        total_count = self.conn.execute("SELECT COUNT(*) FROM bmd_data").fetchone()[0]
        pfas_count = self.conn.execute(
            "SELECT COUNT(*) FROM bmd_data WHERE contains_pfas = true"
        ).fetchone()[0]

        self.log.info(
            f"✅ BMD data processed: {total_count:,} products, {pfas_count:,} containing PFAS-based active ingredients"
        )

    def _process_field_data(self):
        """Process field data with geometry preparation."""
        # Get pesticide field lookup
        self.conn.execute("""
            CREATE OR REPLACE TABLE pesticide_field_lookup AS
            SELECT DISTINCT
                CompanyRegistrationNumber as cvr,
                REGEXP_EXTRACT(MatchedFieldID, 'marker_(.+)', 1) as field_id,
                REGEXP_EXTRACT(MatchedBlockID, 'block_(.+)', 1) as block_id
            FROM temp_pesticide_raw
            WHERE MatchedFieldID IS NOT NULL
            AND MatchedBlockID IS NOT NULL
            AND CompanyRegistrationNumber IS NOT NULL
        """)

        # Process fields with geometry preparation
        self.conn.execute("""
            CREATE OR REPLACE TABLE prepared_fields AS
            SELECT
                f.field_id,
                CAST(f.area_ha AS DOUBLE) as area_ha,
                f.cvr_number,
                f.block_id,
                f.crop_code,
                f.crop_name,
                f.geometry_wkt
            FROM temp_fvm_raw f
            INNER JOIN pesticide_field_lookup p ON (
                f.cvr_number = p.cvr
                AND f.field_id = p.field_id
                AND f.block_id = p.block_id
            )
            WHERE f.geometry_wkt IS NOT NULL
            AND ST_IsValid(ST_GeomFromText(f.geometry_wkt))
            AND CAST(f.area_ha AS DOUBLE) > 0
            AND f.cvr_number IS NOT NULL
            AND f.block_id IS NOT NULL
        """)

        # Use coordinate transformer to prepare geometries
        prepared_table = self.coordinate_transformer.prepare_geometries("prepared_fields")
        self.conn.execute("DROP TABLE prepared_fields")
        self.conn.execute(f"ALTER TABLE {prepared_table} RENAME TO prepared_fields")

        count = self.conn.execute("SELECT COUNT(*) FROM prepared_fields").fetchone()[0]
        self.log.info(f"✅ Field data processed: {count:,} fields with geometries")

    def _process_pesticide_data(self):
        """Process pesticide data and join with BMD."""
        # Process pesticide disaggregation
        self.conn.execute("""
            CREATE OR REPLACE TABLE pesticide_processed AS
            SELECT
                DisaggregatedID,
                MatchedFieldID,
                MatchedBlockID,
                CompanyRegistrationNumber as cvr,
                PesticideName,
                PesticideRegistrationNumber,
                DosageQuantity,
                DosageUnit,
                AllocatedArea,
                AllocationMethod,
                MatchConfidence,
                REGEXP_EXTRACT(MatchedFieldID, 'marker_(.+)', 1) as extracted_field_id,
                REGEXP_EXTRACT(MatchedBlockID, 'block_(.+)', 1) as extracted_block_id
            FROM temp_pesticide_raw
            WHERE MatchedFieldID IS NOT NULL
            AND MatchedBlockID IS NOT NULL
            AND CompanyRegistrationNumber IS NOT NULL
            AND PesticideRegistrationNumber IS NOT NULL
        """)

        # Join with BMD for PFAS detection
        self.conn.execute("""
            CREATE OR REPLACE TABLE pesticide_pfas AS
            SELECT
                p.*,
                b.active_ingredient,
                b.total_load_per_unit,
                COALESCE(b.contains_pfas, false) as contains_pfas,

                -- Calculate PFAS-containing active ingredient amount
                CASE
                    WHEN b.contains_pfas = true AND b.concentration_numeric IS NOT NULL THEN
                        CASE
                            WHEN p.DosageUnit = 4 AND b.enhed_er LIKE '%g/l%' THEN
                                p.DosageQuantity * b.concentration_numeric / 1000.0
                            WHEN p.DosageUnit = 2 AND b.enhed_er LIKE '%g/kg%' THEN
                                p.DosageQuantity * b.concentration_numeric / 1000.0
                            ELSE 0
                        END
                    ELSE 0
                END as pfas_containing_active_ingredient_grams,

                -- Pesticide load
                CASE
                    WHEN b.total_load_per_unit IS NOT NULL THEN
                        p.DosageQuantity * b.total_load_per_unit
                    ELSE 0
                END as pesticide_belastning_applied,

                -- PFAS-containing pesticide load
                CASE
                    WHEN b.contains_pfas = true AND b.total_load_per_unit IS NOT NULL THEN
                        p.DosageQuantity * b.total_load_per_unit
                    ELSE 0
                END as pfas_containing_pesticide_belastning_applied
            FROM pesticide_processed p
            LEFT JOIN bmd_data b ON p.PesticideRegistrationNumber = b.registrerings_nr
        """)

        total_count = self.conn.execute("SELECT COUNT(*) FROM pesticide_pfas").fetchone()[0]
        pfas_count = self.conn.execute(
            "SELECT COUNT(*) FROM pesticide_pfas WHERE contains_pfas = true"
        ).fetchone()[0]

        self.log.info(
            f"✅ Pesticide data processed: {total_count:,} records, {pfas_count:,} applications with PFAS-containing active ingredients"
        )

    async def run_analysis(self, year: int = 2022) -> str:
        """Run the complete H3 PFAS analysis with local test data."""
        self.log.info(f"🚀 Starting H3 PFAS analysis for year {year} (local test data)")

        # Setup DuckDB
        self.setup_duckdb()

        # Load test data
        self.load_test_data()

        # Generate H3 grid
        h3_grid_table = self.generate_h3_grid()

        # Perform chunked spatial join
        results_table = self.spatial_joiner.perform_chunked_spatial_join(
            h3_grid_table, "prepared_fields", "pesticide_pfas", year
        )

        # Validate results
        self._validate_results(results_table)

        # Save results
        self._save_results(results_table, year)

        self.log.info("🎉 H3 PFAS analysis completed successfully!")
        return results_table

    def _save_results(self, results_table: str, year: int):
        """Save results to local file."""
        if not self.local_data_dir:
            return

        output_dir = self.local_data_dir / "results"
        output_dir.mkdir(exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Save Kepler.gl compatible version
        output_path_kepler = (
            output_dir
            / f"h3_pesticide_{year}_res{self.config.h3_resolution}_{timestamp}_kepler.parquet"
        )

        # Create Kepler.gl compatible version
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE {results_table}_kepler AS
            SELECT
                -- Convert H3 cell to string format with correct column name for Kepler.gl H3 layer auto-detection
                CAST(h3_cell AS VARCHAR) as h3_id,
                CAST(center_lat AS DOUBLE) as center_lat,
                CAST(center_lon AS DOUBLE) as center_lon,
                CAST(h3_cell_area_ha AS DOUBLE) as h3_cell_area_ha,
                CAST(total_intersection_area_ha AS DOUBLE) as total_intersection_area_ha,
                CAST(actual_coverage_ratio AS DOUBLE) as actual_coverage_ratio,

                -- Convert BigInt counts to regular integers for Kepler.gl compatibility
                CAST(unique_field_count AS INTEGER) as unique_field_count,
                CAST(total_pesticide_applications AS INTEGER) as total_pesticide_applications,
                CAST(pfas_containing_applications AS INTEGER) as pfas_containing_applications,
                CAST(crop_diversity AS INTEGER) as crop_diversity,

                -- PFAS-containing active ingredient exposure metrics as doubles
                CAST(total_pfas_containing_active_ingredient_grams AS DOUBLE) as total_pfas_containing_active_ingredient_grams,
                CAST(total_pesticide_belastning AS DOUBLE) as total_pesticide_belastning,
                CAST(pfas_containing_active_ingredient_intensity_grams_per_ha AS DOUBLE) as pfas_containing_active_ingredient_intensity_grams_per_ha,

                -- String fields
                crop_types,

                -- Timestamp as string
                CAST(created_at AS VARCHAR) as created_at
            FROM {results_table}
            ORDER BY h3_cell
        """)

        self.log.info(f"💾 Saving Kepler.gl-compatible results to {output_path_kepler}")
        self.conn.execute(f"COPY {results_table}_kepler TO '{output_path_kepler}' (FORMAT PARQUET)")

        # Also save original version
        output_path_original = (
            output_dir / f"h3_pesticide_{year}_res{self.config.h3_resolution}_{timestamp}.parquet"
        )
        self.log.info(f"💾 Saving original results to {output_path_original}")
        self.conn.execute(f"COPY {results_table} TO '{output_path_original}' (FORMAT PARQUET)")

        if output_path_kepler.exists() and output_path_original.exists():
            kepler_size = output_path_kepler.stat().st_size / (1024 * 1024)
            original_size = output_path_original.stat().st_size / (1024 * 1024)
            row_count = self.conn.execute(f"SELECT COUNT(*) FROM {results_table}").fetchone()[0]

            self.log.info(f"✅ Saved {row_count:,} rows of pesticide exposure data")
            self.log.info(f"   📊 Original format: {original_size:.1f} MB")
            self.log.info(f"   🗺️  Kepler.gl compatible: {kepler_size:.1f} MB")
            self.log.info("   🎯 Use the *_kepler.parquet file for Kepler.gl visualization")


# Multi-year H3 analysis function
async def run_multi_year_analysis(years: list[int] | None = None) -> bool:
    """Run multi-year H3 PFAS analysis from GCS data."""
    logger.info("🚀 Starting multi-year H3 PFAS-containing active ingredient analysis from GCS")

    # Create configuration for GCS processing
    config = H3SpatialConfig(
        h3_resolution=10,
        chunk_size=25000,
        memory_limit="12GB",
        thread_count=4,
        enable_progress_tracking=True,
        log_chunk_details=True,
        log_stage_timings=True,
        bucket="landbrugsdata-raw-data",
        available_years=years or [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023],
    )

    # Create processor for GCS data (no local_data_dir)
    processor = H3PFASProcessorRefactored(config, local_data_dir=None)

    try:
        # Run multi-year analysis
        success = await processor.run_analysis_multi_year(years)

        if success:
            logger.info(
                "🎉 Multi-year H3 PFAS-containing active ingredient analysis completed successfully!"
            )
        else:
            logger.error("❌ Multi-year H3 PFAS-containing active ingredient analysis failed!")

        return success

    except Exception as e:
        logger.error(f"❌ Multi-year H3 analysis failed: {e}")
        import traceback

        logger.error(f"Traceback: {traceback.format_exc()}")
        return False


# Multi-year kommune analysis function
async def run_multi_year_kommune_analysis(years: list[int] | None = None) -> bool:
    """Run multi-year kommune-level PFAS analysis from GCS data."""
    logger.info("🏛️ Starting multi-year kommune-level PFAS analysis from GCS")

    # Create configuration for GCS processing
    config = H3SpatialConfig(
        h3_resolution=10,
        chunk_size=25000,
        memory_limit="12GB",
        thread_count=4,
        enable_progress_tracking=True,
        log_chunk_details=True,
        log_stage_timings=True,
        bucket="landbrugsdata-raw-data",
        available_years=years or [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023],
    )

    # Create processor for GCS data (no local_data_dir)
    processor = H3PFASProcessorRefactored(config, local_data_dir=None)

    try:
        # Run multi-year kommune analysis
        success = await processor.run_kommune_analysis_multi_year(years)

        if success:
            logger.info("🎉 Multi-year kommune PFAS analysis completed successfully!")
        else:
            logger.error("❌ Multi-year kommune PFAS analysis failed!")

        return success

    except Exception as e:
        logger.error(f"❌ Multi-year kommune analysis failed: {e}")
        import traceback

        logger.error(f"Traceback: {traceback.format_exc()}")
        return False


# Test function
async def test_refactored_processor():
    """Test the refactored processor with local data."""
    logger.info("🧪 Testing refactored H3 PFAS-containing active ingredient processor (local data)")

    # Check local data
    local_data_dir = Path("data/h3_pfas_test")
    if not local_data_dir.exists():
        logger.error("❌ Local data directory not found!")
        logger.info("📥 Run: python scripts/testing/download_h3_pfas_datasets.py")
        return False

    # Create configuration
    config = H3SpatialConfig(
        h3_resolution=10,
        chunk_size=25000,
        memory_limit="8GB",
        thread_count=4,
        enable_progress_tracking=True,
        log_chunk_details=True,
        log_stage_timings=True,
    )

    # Create processor
    processor = H3PFASProcessorRefactored(config, local_data_dir)

    try:
        # Run analysis
        await processor.run_analysis(year=2022)
        logger.info("🎉 Refactored processor test completed successfully!")
        return True

    except Exception as e:
        logger.error(f"❌ Refactored processor test failed: {e}")
        import traceback

        logger.error(f"Traceback: {traceback.format_exc()}")
        return False


if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="H3 PFAS Processor - Refactored Version")
    parser.add_argument(
        "--mode",
        choices=["test", "gcs", "years", "kommune"],
        default="test",
        help="Mode: 'test' for local test data, 'gcs' for all years from GCS, 'years' for specific years, 'kommune' for kommune-level analysis",
    )
    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        help="Specific years to process (e.g., --years 2020 2021 2022)",
    )
    parser.add_argument(
        "--memory-limit", default="12GB", help="DuckDB memory limit (default: 12GB)"
    )
    parser.add_argument(
        "--thread-count", type=int, default=4, help="DuckDB thread count (default: 4)"
    )
    parser.add_argument(
        "--h3-resolution", type=int, default=10, help="H3 resolution level (default: 10)"
    )

    args = parser.parse_args()

    async def main():
        if args.mode == "test":
            logger.info("🧪 Running local test mode")
            success = await test_refactored_processor()
        elif args.mode == "gcs":
            logger.info("☁️ Running H3 GCS mode for all available years")
            success = await run_multi_year_analysis()
        elif args.mode == "years":
            if not args.years:
                logger.error("❌ --years must be specified when using 'years' mode")
                sys.exit(1)
            logger.info(f"☁️ Running H3 GCS mode for years: {args.years}")
            success = await run_multi_year_analysis(args.years)
        elif args.mode == "kommune":
            if args.years:
                logger.info(f"🏛️ Running kommune analysis for years: {args.years}")
                success = await run_multi_year_kommune_analysis(args.years)
            else:
                logger.info("🏛️ Running kommune analysis for all available years")
                success = await run_multi_year_kommune_analysis()
        else:
            logger.error(f"❌ Unknown mode: {args.mode}")
            sys.exit(1)

        if success:
            logger.info("✅ Analysis completed successfully!")
            sys.exit(0)
        else:
            logger.error("❌ Analysis failed!")
            sys.exit(1)

    # Run the main function
    asyncio.run(main())
