"""
Field Area Analysis Gold Layer - Optimized Implementation

This module implements the gold layer processor for field area analysis with performance optimizations
from the redesigned implementation. It performs comprehensive spatial analysis of agricultural fields
against multiple datasets including properties, soil types, BNBO status, wetlands, and water projects.

Key optimizations applied:
1. Optimal spatial join ordering (smallest to largest build side)
2. Multipolygon splitting using ST_Dump for better spatial indexing
3. Single spatial predicate per join to enable SPATIAL_JOIN operator
4. Chunked processing only for properties dataset (6.5M rows)
5. Direct spatial joins for smaller datasets with automatic spatial indexing
6. Performance tracking and detailed timing metrics

Migrated from the standalone field_area_analysis_pipeline to the unified pipeline architecture.
"""

import os
import time
from typing import Any, Dict, List, Optional

import psutil
from pydantic import ConfigDict

from unified_pipeline.common.base import BaseJobConfig, BaseSource, GoldJobInterface
from unified_pipeline.util.log_util import Logger


class FieldAreaAnalysisGoldConfig(BaseJobConfig):
    """Configuration for Field Area Analysis gold layer."""

    name: str = "Field Area Analysis Gold"
    dataset: str = "field_area_analysis"
    type: str = "gold"
    description: str = "Comprehensive spatial analysis of agricultural fields"
    frequency: str = "weekly"
    bucket: str = os.getenv("GCS_BUCKET")

    # Input silver datasets
    agricultural_fields_dataset: str = "fvm_marker"
    properties_dataset: str = "property_cadastral_merged"
    soil_types_dataset: str = "soil_types"
    bnbo_status_dataset: str = "bnbo_status_dissolved"
    wetlands_dataset: str = "wetlands_dissolved"
    water_projects_dataset: str = "water_projects_dissolved"

    # Processing configuration - optimized for direct spatial joins
    batch_size: int = 500000  # Increased for properties chunking only
    memory_limit: str = "8GB"
    thread_count: int = 1  # Single thread for memory-intensive spatial operations
    max_temp_directory_size: str = "6GB"

    # Quality thresholds
    min_area_threshold: float = 0.01  # Minimum area share to include (1%)

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class FieldAreaAnalysisGold(BaseSource[FieldAreaAnalysisGoldConfig], GoldJobInterface):
    """
    Gold layer processor for field area analysis with performance optimizations.

    Performs comprehensive spatial analysis of agricultural fields against multiple datasets
    to create analytics-ready spatial intersection results using optimal spatial join ordering.
    """

    def __init__(self, config: FieldAreaAnalysisGoldConfig):
        super().__init__(config)
        self.log = Logger.get_logger()

        # Performance tracking
        self.start_time = None
        self.phase_times = {}

        # Use the base class connection - don't create a new one
        self._configure_duckdb_additional()

    def _configure_duckdb_additional(self):
        """Configure additional DuckDB settings for optimized field area analysis."""
        try:
            # Update memory settings to use config values
            self.conn.execute(f"SET memory_limit='{self.config.memory_limit}'")
            self.conn.execute(f"SET threads={self.config.thread_count}")
            self.conn.execute(f"SET max_memory='{self.config.memory_limit}'")

            # Set temp directory size limit to prevent overflow
            self.conn.execute(
                f"SET max_temp_directory_size='{self.config.max_temp_directory_size}'"
            )
            self.conn.execute("SET temp_directory='/tmp/duckdb_field_analysis'")

            # Optimize for spatial operations
            self.conn.execute("SET threads=1")
            self.conn.execute("SET preserve_insertion_order=false")

            # Enable spatial optimizations
            self.conn.execute("SET enable_spatial_index=true")
            self.conn.execute("SET enable_progress_bar=false")

        except Exception as e:
            self.log.warning(f"Could not apply some DuckDB optimizations: {e}")

        # Create temp directory
        temp_dir = "/tmp/duckdb_field_analysis"
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir, exist_ok=True)

        # Clean up any existing temp files
        self._cleanup_temp_files()

        # Spatial extension is already loaded in base class
        self.log.info("✅ DuckDB Spatial configured - Field Area Analysis Gold Layer (Optimized)")
        self.log.info(
            f"   Memory: {self.config.memory_limit}, Threads: 1, Properties chunk size: {self.config.batch_size:,}"
        )

    def _prepare_geometries_optimized(self, fields_table_name: str, year: int):
        """
        Phase 1: Load and prepare all geometries for optimal spatial joins.

        Key optimizations:
        - Convert agricultural fields WKT to geometry (probe side)
        - Split multipolygons using ST_Dump for better spatial indexing
        - Validate geometry quality
        """
        phase_start = time.time()
        self.log.info("📥 Phase 1: Geometry preprocessing and validation (optimized)...")

        # Prepare fields table for spatial analysis
        self._prepare_fields_table_for_analysis(fields_table_name, year)

        field_count = self.conn.execute("SELECT COUNT(*) FROM current_fields").fetchone()[0]
        self.log.info(f"✅ Processed {field_count:,} valid agricultural fields")

        # Load and prepare build side datasets with optimal spatial indexing
        self._prepare_build_side_datasets_optimized()

        self.phase_times["geometry_preprocessing"] = time.time() - phase_start
        self.log.info(
            f"✅ Phase 1 completed in {self.phase_times['geometry_preprocessing']:.1f} seconds"
        )

    def _prepare_build_side_datasets_optimized(self):
        """Prepare all build side datasets with optimal spatial indexing and multipolygon splitting."""

        # Load reference datasets in optimal order for spatial joins
        dataset_paths = self._load_silver_data_streaming(None)

        # 1. Soil types (smallest build side) - load directly
        self._load_soil_types_optimized(dataset_paths)

        # 2. BNBO status - split multipolygons using ST_Dump
        self._load_bnbo_status_optimized(dataset_paths)

        # 3. Water projects - split multipolygons using ST_Dump
        self._load_water_projects_optimized(dataset_paths)

        # 4. Wetlands - large but fits in memory
        self._load_wetlands_optimized(dataset_paths)

        # 5. Properties - will be processed separately with chunking

    def _load_soil_types_optimized(self, dataset_paths: Dict[str, Any]):
        """Load soil types dataset (smallest build side for optimal spatial indexing) with geometry validation."""
        self.log.info("🔄 Loading soil types (optimized)...")

        soil_data = dataset_paths.get(self.config.soil_types_dataset)
        if soil_data is None:
            self.log.warning("No soil data available - creating empty table")
            self.conn.execute(
                "CREATE TABLE soil_types (soil_description VARCHAR, soil_code VARCHAR, geom GEOMETRY)"
            )
            return

        if isinstance(soil_data, str) and soil_data.startswith("gs://"):
            # Load from GCS
            self.gcs_access.query_parquet_direct(
                soil_data,
                """SELECT 
                    soil_description,
                    soil_code,
                    geometry as geom""",
                "soil_types",
            )
        else:
            # Load from memory
            self.conn.register("soil_df", soil_data)
            self.conn.execute("""
                CREATE TABLE soil_types AS
                SELECT 
                    soil_description,
                    soil_code,
                    geometry as geom
                FROM soil_df
            """)

        # Check for invalid geometries and apply centralized validator if needed
        soil_count = self.conn.execute("SELECT COUNT(*) FROM soil_types").fetchone()[0]
        invalid_soil = self.conn.execute("""
            SELECT COUNT(*) FROM soil_types 
            WHERE NOT ST_IsValid(geom)
        """).fetchone()[0]

        if invalid_soil > 0:
            self.log.warning(
                f"⚠️ Found {invalid_soil:,} invalid soil geometries - applying centralized geometry validator as fallback"
            )

            # Use the centralized geometry validator to fix the issue
            validate_and_transform_geometries_duckdb(
                self.conn, "soil_types", "soil_types_validation", geometry_column="geom"
            )

            soil_count = self.conn.execute("SELECT COUNT(*) FROM soil_types").fetchone()[0]

        self.log.info(f"✅ Loaded {soil_count:,} valid soil type polygons")

    def _load_bnbo_status_optimized(self, dataset_paths: Dict[str, Any]):
        """Load BNBO status dataset and split multipolygons using ST_Dump for optimal spatial indexing."""
        self.log.info("🔄 Loading and splitting BNBO multipolygons (optimized)...")

        bnbo_data = dataset_paths.get(self.config.bnbo_status_dataset)
        if bnbo_data is None:
            self.log.warning("No BNBO data available - creating empty table")
            self.conn.execute("CREATE TABLE bnbo_polygons (status_category VARCHAR, geom GEOMETRY)")
            return

        if isinstance(bnbo_data, str) and bnbo_data.startswith("gs://"):
            # Load from GCS
            self.gcs_access.create_table_from_gcs("bnbo_raw", bnbo_data)
        else:
            # Load from memory
            self.conn.register("bnbo_df", bnbo_data)
            self.conn.execute("CREATE TABLE bnbo_raw AS SELECT * FROM bnbo_df")

        # Split multipolygons into individual polygons for optimal spatial indexing
        self.conn.execute("""
            CREATE TABLE bnbo_polygons AS
            SELECT 
                status_category,
                (unnest(ST_Dump(geometry))).geom as geom,
                ROW_NUMBER() OVER () as polygon_id
            FROM bnbo_raw
        """)

        # Clean up raw table
        self.conn.execute("DROP TABLE bnbo_raw")

        bnbo_count = self.conn.execute("SELECT COUNT(*) FROM bnbo_polygons").fetchone()[0]
        self.log.info(f"✅ Split BNBO into {bnbo_count:,} individual polygons")

    def _load_water_projects_optimized(self, dataset_paths: Dict[str, Any]):
        """Load water projects dataset and split multipolygons using ST_Dump for optimal spatial indexing."""
        self.log.info("🔄 Loading and splitting water project multipolygons (optimized)...")

        water_data = dataset_paths.get(self.config.water_projects_dataset)
        if water_data is None:
            self.log.warning("No water projects data available - creating empty table")
            self.conn.execute(
                "CREATE TABLE water_project_polygons (project_id VARCHAR, geom GEOMETRY)"
            )
            return

        if isinstance(water_data, str) and water_data.startswith("gs://"):
            # Load from GCS
            self.gcs_access.create_table_from_gcs("water_raw", water_data)
        else:
            # Load from memory
            self.conn.register("water_df", water_data)
            self.conn.execute("CREATE TABLE water_raw AS SELECT * FROM water_df")

        # Split multipolygons into individual polygons for optimal spatial indexing
        self.conn.execute("""
            CREATE TABLE water_project_polygons AS
            SELECT
                project_id,
                (unnest(ST_Dump(geometry))).geom as geom,
                ROW_NUMBER() OVER () as polygon_id
            FROM water_raw
        """)

        # Clean up raw table
        self.conn.execute("DROP TABLE water_raw")

        water_count = self.conn.execute("SELECT COUNT(*) FROM water_project_polygons").fetchone()[0]
        self.log.info(f"✅ Split water projects into {water_count:,} individual polygons")

    def _load_wetlands_optimized(self, dataset_paths: Dict[str, Any]):
        """Load wetlands dataset (large but fits in memory with spatial indexing)."""
        self.log.info("🔄 Loading wetlands (optimized)...")

        wetlands_data = dataset_paths.get(self.config.wetlands_dataset)
        if wetlands_data is None:
            self.log.warning("No wetlands data available - creating empty table")
            self.conn.execute("CREATE TABLE wetlands (wetland_id VARCHAR, geom GEOMETRY)")
            return

        if isinstance(wetlands_data, str) and wetlands_data.startswith("gs://"):
            # Load from GCS
            self.gcs_access.query_parquet_direct(
                wetlands_data,
                """SELECT 
                    wetland_id,
                    geometry as geom""",
                "wetlands",
            )
        else:
            # Load from memory
            self.conn.register("wetlands_df", wetlands_data)
            self.conn.execute("""
                CREATE TABLE wetlands AS
                SELECT 
                    wetland_id,
                    geometry as geom
                FROM wetlands_df
            """)

        # Create spatial index on wetlands for faster intersection queries
        try:
            self.conn.execute("CREATE INDEX idx_wetlands_geom ON wetlands USING GIST (geom)")
            self.log.info("    🚀 Created spatial index on wetlands")
        except Exception as e:
            # Spatial indexing may not be available in all DuckDB versions
            self.log.info(f"    ℹ️ Spatial indexing not available: {e}")

        wetlands_count = self.conn.execute("SELECT COUNT(*) FROM wetlands").fetchone()[0]
        self.log.info(f"✅ Loaded {wetlands_count:,} wetland polygons")

    def _execute_optimal_spatial_joins(self):
        """
        Phase 2: Execute spatial joins in optimal order using DuckDB Spatial v1.2.2.

        Join order: smallest to largest build side for optimal spatial indexing.
        Each join uses single spatial predicate to enable SPATIAL_JOIN operator.
        """
        phase_start = time.time()
        self.log.info("⚡ Phase 2: Executing optimal spatial joins...")

        # 1. Soil types (smallest build side) - fastest spatial index creation
        self.log.info("🔄 Joining with soil types...")
        join_start = time.time()

        self.conn.execute("""
            CREATE TABLE fields_with_soil AS
            SELECT 
                f.*,
                s.soil_code,
                s.soil_description,
                ST_Area(ST_Intersection(f.geom, s.geom)) / ST_Area(f.geom) * 100 as soil_area_share
            FROM current_fields f
            LEFT JOIN soil_types s ON ST_Intersects(f.geom, s.geom)
        """)

        soil_time = time.time() - join_start
        self.log.info(f"✅ Soil types join completed in {soil_time:.1f} seconds")

        # 2. BNBO polygons (individual polygons) - optimized with spatial indexing
        self.log.info("🔄 Joining with BNBO polygons...")
        join_start = time.time()

        self.conn.execute("""
            CREATE TABLE fields_with_bnbo AS
            SELECT 
                f.*,
                b.status_category,
                CASE 
                    WHEN b.status_category IS NOT NULL THEN 
                        ST_Area(ST_Intersection(f.geom, b.geom)) / ST_Area(f.geom) * 100
                    ELSE NULL
                END as bnbo_area_share
            FROM fields_with_soil f
            LEFT JOIN bnbo_polygons b ON ST_Intersects(f.geom, b.geom)
        """)

        bnbo_time = time.time() - join_start
        self.log.info(f"✅ BNBO polygons join completed in {bnbo_time:.1f} seconds")

        # 3. Water project polygons - optimized spatial indexing
        self.log.info("🔄 Joining with water project polygons...")
        join_start = time.time()

        self.conn.execute("""
            CREATE TABLE fields_with_water AS
            SELECT 
                f.*,
                wp.project_id,
                CASE 
                    WHEN wp.project_id IS NOT NULL THEN 
                        ST_Area(ST_Intersection(f.geom, wp.geom)) / ST_Area(f.geom) * 100
                    ELSE 0
                END as water_projects_area_share
            FROM fields_with_bnbo f
            LEFT JOIN water_project_polygons wp ON ST_Intersects(f.geom, wp.geom)
        """)

        water_time = time.time() - join_start
        self.log.info(f"✅ Water project polygons join completed in {water_time:.1f} seconds")

        # 4. Wetlands (large build side but benefits from spatial indexing)
        self.log.info("🔄 Joining with wetlands...")
        join_start = time.time()

        # Simplified wetland join using single spatial predicate for SPATIAL_JOIN operator
        self.conn.execute("""
            CREATE TABLE fields_with_wetlands AS
            WITH wetland_intersections AS (
                SELECT 
                    f.field_id,
                    f.block_id,
                    f.cvr_number,
                    f.year,
                    f.geom,
                    f.field_area_m2,
                    f.soil_code,
                    f.soil_description,
                    f.soil_area_share,
                    f.status_category,
                    f.bnbo_area_share,
                    f.project_id,
                    f.water_projects_area_share,
                    COALESCE(ST_Area(ST_Union_Agg(ST_Intersection(f.geom, w.geom))) / ST_Area(f.geom) * 100, 0) as wetland_area_share
                FROM fields_with_water f
                LEFT JOIN wetlands w ON ST_Intersects(f.geom, w.geom)
                GROUP BY f.field_id, f.block_id, f.cvr_number, f.year, f.geom, f.field_area_m2, 
                         f.soil_code, f.soil_description, f.soil_area_share, 
                         f.status_category, f.bnbo_area_share, f.project_id, f.water_projects_area_share
            )
            SELECT * FROM wetland_intersections
        """)

        wetlands_time = time.time() - join_start
        self.log.info(f"✅ Wetlands join completed in {wetlands_time:.1f} seconds")

        # 5. Properties (large dataset) - requires chunked processing
        properties_time = self._process_properties_chunked()

        self.phase_times["spatial_joins"] = {
            "soil_types": soil_time,
            "bnbo_polygons": bnbo_time,
            "water_projects": water_time,
            "wetlands": wetlands_time,
            "properties": properties_time,
            "total": time.time() - phase_start,
        }

        self.log.info(
            f"✅ Phase 2 completed in {self.phase_times['spatial_joins']['total']:.1f} seconds"
        )

    def _process_properties_chunked(self) -> float:
        """
        Process properties dataset using spatial chunking approach.
        Properties dataset is too large to be build side, so we chunk it.
        """
        self.log.info("🔄 Processing properties with spatial chunking approach...")
        chunk_start = time.time()

        # Get properties dataset path
        dataset_paths = self._load_silver_data_streaming(None)
        properties_data = dataset_paths.get(self.config.properties_dataset)

        if properties_data is None:
            self.log.warning("No property_cadastral_merged data found, skipping...")
            # Create empty results table
            self.conn.execute("""
                CREATE TABLE field_property_results (
                    field_id VARCHAR,
                    block_id VARCHAR,
                    cvr_number VARCHAR,
                    bfe_number VARCHAR,
                    area_share DOUBLE
                )
            """)
            return 0.0

        if isinstance(properties_data, str) and properties_data.startswith("gs://"):
            # Stream from GCS
            properties_path = properties_data

            # Get total properties count WITHOUT loading the entire dataset
            self.log.info("🔍 Getting properties count without loading full dataset...")
            total_properties = self.conn.execute(f"""
                SELECT COUNT(*) FROM read_parquet('{properties_path}')
            """).fetchone()[0]

            chunk_size = self.config.batch_size  # Use config batch size for properties

            # Create results table
            self.conn.execute("""
                CREATE TABLE field_property_results (
                    field_id VARCHAR,
                    block_id VARCHAR,
                    cvr_number VARCHAR,
                    bfe_number VARCHAR,
                    area_share DOUBLE
                )
            """)

            # Process properties in chunks
            processed_properties = 0
            for offset in range(0, total_properties, chunk_size):
                chunk_properties = min(chunk_size, total_properties - offset)

                self.log.info(
                    f"   Processing properties chunk {offset // chunk_size + 1}: {chunk_properties:,} properties"
                )

                # Create current properties chunk (build side)
                self.conn.execute(f"""
                    CREATE OR REPLACE TABLE properties_chunk AS
                    SELECT 
                        bestemtFastEjendomBFENr as bfe_number,
                        geometry as geom
                    FROM read_parquet('{properties_path}')
                    WHERE bestemtFastEjendomBFENr IS NOT NULL
                    LIMIT {chunk_size} OFFSET {offset}
                """)

                # Spatial join with area calculation
                self.conn.execute("""
                    INSERT INTO field_property_results
                    SELECT 
                        f.field_id,
                        f.block_id,
                        f.cvr_number,
                        p.bfe_number,
                        ST_Area(ST_Intersection(f.geom, p.geom)) / ST_Area(f.geom) * 100 as area_share
                    FROM fields_with_wetlands f
                    JOIN properties_chunk p ON ST_Intersects(f.geom, p.geom)
                """)

                processed_properties += chunk_properties
                self.log.info(
                    f"   Processed {processed_properties:,}/{total_properties:,} properties ({processed_properties / total_properties * 100:.1f}%)"
                )

        else:
            # Handle in-memory properties data
            self.log.info("🔄 Processing properties from memory...")
            self.conn.register("properties_df", properties_data)

            # Check columns
            available_columns = [
                row[0] for row in self.conn.execute("DESCRIBE properties_df").fetchall()
            ]

            if "bfe_number" not in available_columns:
                if "bestemtFastEjendomBFENr" in available_columns:
                    bfe_column = "bestemtFastEjendomBFENr"
                else:
                    raise ValueError("Required BFE column not found in property data")
            else:
                bfe_column = "bfe_number"

            # Create properties table
            self.conn.execute(f"""
                CREATE TABLE properties AS
                SELECT 
                    {bfe_column} as bfe_number,
                    geometry as geom
                FROM properties_df
                WHERE {bfe_column} IS NOT NULL
            """)

            # Create results table and process
            self.conn.execute("""
                CREATE TABLE field_property_results AS
                SELECT 
                    f.field_id,
                    f.block_id,
                    f.cvr_number,
                    p.bfe_number,
                    ST_Area(ST_Intersection(f.geom, p.geom)) / ST_Area(f.geom) * 100 as area_share
                FROM fields_with_wetlands f
                JOIN properties p ON ST_Intersects(f.geom, p.geom)
            """)

        properties_time = time.time() - chunk_start
        self.log.info(f"✅ Properties processing completed in {properties_time:.1f} seconds")

        return properties_time

    def _generate_final_results_optimized(self, year: int) -> Dict[str, any]:
        """
        Phase 3: Generate final consolidated results with JSON aggregations.
        """
        phase_start = time.time()
        self.log.info("📊 Phase 3: Generating final results with JSON aggregations...")

        # Create JSON aggregations for properties
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_property_json AS
            SELECT 
                field_id,
                block_id,
                cvr_number,
                COALESCE('{' || string_agg('"' || bfe_number || '":' || area_share, ',') || '}', '{}') as property_area_shares
            FROM field_property_results
            GROUP BY field_id, block_id, cvr_number
        """)

        # Create JSON aggregations for soil types
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_soil_json AS
            SELECT 
                field_id,
                block_id,
                cvr_number,
                CASE 
                    WHEN soil_code IS NOT NULL THEN 
                        '{' || '"' || soil_code || '":' || soil_area_share || '}'
                    ELSE '{}'
                END as soil_area_shares
            FROM fields_with_wetlands
            WHERE soil_code IS NOT NULL
        """)

        # Create JSON aggregations for BNBO status
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_bnbo_json AS
            SELECT 
                field_id,
                block_id,
                cvr_number,
                CASE 
                    WHEN status_category IS NOT NULL THEN 
                        '{' || '"' || status_category || '":' || bnbo_area_share || '}'
                    ELSE '{}'
                END as bnbo_area_shares
            FROM fields_with_wetlands
            WHERE status_category IS NOT NULL
        """)

        # Create final consolidated table
        year_results_table = f"field_area_analysis_results_{year}"
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE {year_results_table} AS
            SELECT 
                f.field_id,
                f.block_id,
                f.cvr_number,
                f.year,
                f.wetland_area_share,
                0 as wetland_water_projects_share,  -- Complex overlap simplified
                f.water_projects_area_share,
                COALESCE(fpj.property_area_shares, '{{}}') as property_area_shares,
                COALESCE(fsj.soil_area_shares, '{{}}') as soil_area_shares,
                COALESCE(fbj.bnbo_area_shares, '{{}}') as bnbo_area_shares,
                '{{}}' as bnbo_water_projects_shares  -- Complex overlap simplified
            FROM fields_with_wetlands f
            LEFT JOIN field_property_json fpj ON f.field_id = fpj.field_id AND f.block_id = fpj.block_id
            LEFT JOIN field_soil_json fsj ON f.field_id = fsj.field_id AND f.block_id = fsj.block_id
            LEFT JOIN field_bnbo_json fbj ON f.field_id = fbj.field_id AND f.block_id = fbj.block_id
        """)

        # Generate summary statistics
        total_fields = self.conn.execute(f"SELECT COUNT(*) FROM {year_results_table}").fetchone()[0]

        soil_coverage = self.conn.execute(f"""
            SELECT COUNT(*) FROM {year_results_table} WHERE soil_area_shares != '{{}}'
        """).fetchone()[0]

        bnbo_coverage = self.conn.execute(f"""
            SELECT COUNT(*) FROM {year_results_table} WHERE bnbo_area_shares != '{{}}'
        """).fetchone()[0]

        wetland_coverage = self.conn.execute(f"""
            SELECT COUNT(*) FROM {year_results_table} WHERE wetland_area_share > 0
        """).fetchone()[0]

        property_coverage = self.conn.execute(f"""
            SELECT COUNT(*) FROM {year_results_table} WHERE property_area_shares != '{{}}'
        """).fetchone()[0]

        results = {
            "total_fields": total_fields,
            "coverage": {
                "soil_types": {
                    "count": soil_coverage,
                    "percentage": soil_coverage / total_fields * 100 if total_fields > 0 else 0,
                },
                "bnbo_status": {
                    "count": bnbo_coverage,
                    "percentage": bnbo_coverage / total_fields * 100 if total_fields > 0 else 0,
                },
                "wetlands": {
                    "count": wetland_coverage,
                    "percentage": wetland_coverage / total_fields * 100 if total_fields > 0 else 0,
                },
                "properties": {
                    "count": property_coverage,
                    "percentage": property_coverage / total_fields * 100 if total_fields > 0 else 0,
                },
            },
        }

        self.phase_times["final_results"] = time.time() - phase_start
        self.log.info(f"✅ Phase 3 completed in {self.phase_times['final_results']:.1f} seconds")

        return results, year_results_table

    def _log_performance_summary(self, total_time: float):
        """Log comprehensive performance summary."""
        self.log.info("\n" + "=" * 80)
        self.log.info("📈 FIELD AREA ANALYSIS PERFORMANCE SUMMARY")
        self.log.info("=" * 80)
        self.log.info(
            f"Total execution time: {total_time:.1f} seconds ({total_time / 60:.1f} minutes)"
        )
        self.log.info(f"Geometry preprocessing: {self.phase_times['geometry_preprocessing']:.1f}s")

        if "spatial_joins" in self.phase_times:
            joins = self.phase_times["spatial_joins"]
            self.log.info(f"Spatial joins total: {joins['total']:.1f}s")
            self.log.info(f"  - Soil types: {joins['soil_types']:.1f}s")
            self.log.info(f"  - BNBO polygons: {joins['bnbo_polygons']:.1f}s")
            self.log.info(f"  - Water projects: {joins['water_projects']:.1f}s")
            self.log.info(f"  - Wetlands: {joins['wetlands']:.1f}s")
            self.log.info(f"  - Properties (chunked): {joins['properties']:.1f}s")

        self.log.info(f"Final results generation: {self.phase_times['final_results']:.1f}s")
        self.log.info("🚀 OPTIMIZATIONS APPLIED:")
        self.log.info("  - Optimal spatial join ordering (smallest to largest build side)")
        self.log.info("  - Multipolygon splitting with ST_Dump for better indexing")
        self.log.info("  - Single spatial predicate per join for SPATIAL_JOIN operator")
        self.log.info("  - Chunked processing only for properties dataset")
        self.log.info("=" * 80)

    def _load_agricultural_fields_for_years_optimized(
        self, years: List[int], silver_data: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        ✅ MIGRATED: Load agricultural fields data for all available years using optimized patterns.

        Returns:
            str: Name of the DuckDB table containing combined fields data
        """
        all_table_names = []

        for year in years:
            dataset_name = f"fvm_marker_{year}"
            table_name = f"fields_{year}"

            # Check if data is available in memory
            if silver_data and dataset_name in silver_data:
                self.log.info(f"Using in-memory data for {dataset_name}")
                year_data = silver_data[dataset_name]

                # Register with DuckDB and create table
                self.conn.register(f"temp_{table_name}", year_data)
                self.conn.execute(f"""
                    CREATE TABLE {table_name} AS
                    SELECT *, {year} as year
                    FROM temp_{table_name}
                """)

            else:
                # ✅ OPTIMIZED: Load from GCS using new access layer
                self.log.info(f"Loading {dataset_name} from GCS storage (optimized)")
                gcs_path = self._get_latest_silver_path_for_dataset(dataset_name)

                if gcs_path:
                    # Direct table creation with year column
                    self.gcs_access.query_parquet_direct(
                        gcs_path, f"SELECT *, {year} as year", table_name
                    )
                else:
                    self.log.warning(f"No data found for {dataset_name}")
                    continue

            # Verify table was created
            try:
                count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                if count > 0:
                    all_table_names.append(table_name)
                    self.log.info(f"✅ Loaded {count:,} fields for year {year} (optimized)")
                else:
                    self.log.warning(f"No data in table {table_name}")
            except Exception as e:
                self.log.warning(f"Failed to verify table {table_name}: {e}")

        if all_table_names:
            # ✅ OPTIMIZED: Combine multiple years using DuckDB directly
            combined_table_name = "combined_fields"

            # Create combined table with UNION ALL
            union_queries = [f"SELECT * FROM {table}" for table in all_table_names]
            combined_query = " UNION ALL ".join(union_queries)

            self.conn.execute(f"""
                CREATE OR REPLACE TABLE {combined_table_name} AS
                {combined_query}
            """)

            # Get total count and cleanup individual year tables
            total_count = self.conn.execute(
                f"SELECT COUNT(*) FROM {combined_table_name}"
            ).fetchone()[0]

            # Cleanup individual year tables to save memory
            for table_name in all_table_names:
                self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")

            self.log.info(
                f"✅ Combined total: {total_count:,} fields across {len(all_table_names)} years using DuckDB (optimized)"
            )
            return combined_table_name
        else:
            self.log.error("No agricultural fields data found for any year")
            return None

    def _get_latest_silver_path_for_dataset(self, dataset_name: str) -> Optional[str]:
        """Get the latest silver data path for a specific dataset."""
        try:
            # ✅ MIGRATION: Use unified pattern for all datasets
            pattern = f"gs://{self.config.bucket}/silver/{dataset_name}/*/*.parquet"
            files = self.gcs_access.list_files(pattern)
            if files:
                latest_path = sorted(files, reverse=True)[0]
                self.log.info(f"Found latest data for {dataset_name} at {latest_path}")
                return latest_path
            else:
                self.log.warning(f"No data found for {dataset_name} with pattern {pattern}")
                return None
        except Exception as e:
            self.log.error(f"Error finding latest data for {dataset_name}: {e}")
            return None

    def _load_silver_data_streaming(self, silver_data: Optional[Dict[str, Any]]) -> Dict[str, str]:
        """Load silver data paths for streaming processing, avoiding memory loading of large datasets."""

        dataset_paths = {}
        required_datasets = [
            # Note: agricultural_fields_dataset is loaded separately using year discovery
            self.config.properties_dataset,
            self.config.soil_types_dataset,
            self.config.bnbo_status_dataset,
            self.config.wetlands_dataset,
            self.config.water_projects_dataset,
        ]

        # Large datasets that should be streamed directly to DuckDB
        large_datasets = {self.config.properties_dataset}

        for dataset_name in required_datasets:
            if silver_data and dataset_name in silver_data and dataset_name not in large_datasets:
                # Use in-memory data for smaller datasets only
                self.log.info(f"Using in-memory silver data for {dataset_name}")
                dataset_paths[dataset_name] = silver_data[dataset_name]
            else:
                # Get file paths for streaming processing
                self.log.info(f"Setting up streaming for {dataset_name}")
                try:
                    path = self._get_latest_silver_path_for_dataset(dataset_name)
                    if path:
                        dataset_paths[dataset_name] = path
                    else:
                        self.log.warning(f"No data found for {dataset_name}")
                        dataset_paths[dataset_name] = None
                except Exception as e:
                    self.log.error(f"Error finding {dataset_name}: {e}")
                    dataset_paths[dataset_name] = None

        return dataset_paths

    # Legacy reference data loading methods removed - now handled in optimized build side datasets preparation

    # Legacy field batch processing removed - now using optimized direct spatial joins

    async def run(self, silver_data: Optional[Dict[str, Any]] = None) -> None:
        """
        Run the optimized field area analysis gold processing.

        This implementation uses performance optimizations from the redesigned version:
        - Optimal spatial join ordering (smallest to largest build side)
        - Multipolygon splitting with ST_Dump for better spatial indexing
        - Single spatial predicate per join to enable SPATIAL_JOIN operator
        - Chunked processing only for properties dataset
        - Performance tracking and detailed timing metrics

        Args:
            silver_data: Optional dictionary of silver datasets for in-memory processing
        """
        self.start_time = time.time()
        self.log.info("🚀 Starting Field Area Analysis Gold processing (optimized)")

        try:
            # Get available years and process the latest one
            available_years = self._get_available_fvm_marker_years()
            if not available_years:
                self.log.error("No fvm_marker years found - cannot proceed with analysis")
                return

            # Get the latest year only
            latest_year = max(available_years)
            self.log.info(f"Found fvm_marker data for years: {available_years}")
            self.log.info(f"🚀 Processing only the latest year: {latest_year}")

            # Load fields for the latest year
            fields_table_name = self._load_agricultural_fields_for_years_optimized(
                [latest_year], silver_data
            )
            if not fields_table_name:
                self.log.error(f"No fields found for latest year {latest_year}")
                return

            # Phase 1: Geometry preprocessing and validation
            self._prepare_geometries_optimized(fields_table_name, latest_year)

            # Phase 2: Execute spatial joins in optimal order
            self._execute_optimal_spatial_joins()

            # Phase 3: Generate final results with JSON aggregations
            results, year_results_table = self._generate_final_results_optimized(latest_year)

            # Get field count for this year
            fields_count = self.conn.execute(
                f"SELECT COUNT(*) FROM {year_results_table}"
            ).fetchone()[0]

            self.log.info(f"✅ Year {latest_year} completed: {fields_count:,} fields processed")

            # Save results for this year
            self.save_data_direct(
                year_results_table,
                f"{self.config.dataset}_{latest_year}",
                self.config.bucket,
                "gold",
            )

            # Performance summary
            total_time = time.time() - self.start_time
            self._log_performance_summary(total_time)

            self.log.info("✅ Field Area Analysis completed successfully")
            self.log.info(f"   Fields processed: {fields_count:,}")
            self.log.info(f"   Year processed: {latest_year}")
            self.log.info(f"   Total time: {total_time / 60:.1f} minutes")
            self.log.info(f"   Results saved to: {self.config.dataset}_{latest_year}")

        except Exception as e:
            self.log.error(f"Field Area Analysis Gold processing failed: {e}")
            raise

        finally:
            self._safe_close_connection()

    # Legacy chunked processing methods removed - now using optimized direct spatial joins

    # Legacy chunk size calculation removed - now using direct spatial joins

    def _log_memory_usage(self, context: str):
        """Log memory and disk usage like H3 PFAS pipeline does."""
        try:
            import shutil

            import psutil

            # Memory usage
            memory = psutil.virtual_memory()
            memory_used_gb = (memory.total - memory.available) / (1024**3)
            memory_total_gb = memory.total / (1024**3)
            memory_percent = memory.percent

            # Disk usage for temp directory
            temp_usage = shutil.disk_usage("/tmp")
            temp_used_gb = (temp_usage.total - temp_usage.free) / (1024**3)
            temp_total_gb = temp_usage.total / (1024**3)

            self.log.info(
                f"💾 {context}: Memory {memory_used_gb:.1f}GB/{memory_total_gb:.1f}GB ({memory_percent:.1f}%), Temp disk {temp_used_gb:.1f}GB/{temp_total_gb:.1f}GB"
            )

            # Warning if approaching limits
            if memory_percent > 85:
                self.log.warning(f"⚠️ High memory usage: {memory_percent:.1f}%")
            if temp_used_gb > 10:  # More than 10GB temp usage
                self.log.warning(f"⚠️ High temp disk usage: {temp_used_gb:.1f}GB")

        except ImportError:
            # psutil not available, skip monitoring
            pass
        except Exception as e:
            self.log.warning(f"Could not monitor memory usage: {e}")

    # Legacy result table creation removed - now using direct spatial joins

    # Legacy chunked processing methods removed - now using optimized direct spatial joins

    def _prepare_fields_table_for_analysis(self, fields_table_name: str, year: int):
        """Prepare fields table for spatial analysis."""
        # First check what columns are available in the agricultural fields data
        fields_columns = [
            row[0] for row in self.conn.execute(f"DESCRIBE {fields_table_name}").fetchall()
        ]
        self.log.info(f"Available columns in agricultural fields data: {fields_columns}")

        # Check for geometry column
        if "geometry" in fields_columns:
            geometry_column = "geometry"
        elif "geometry_wkt" in fields_columns:
            self.log.info(
                "Using 'geometry_wkt' column instead of 'geometry' for agricultural fields"
            )
            geometry_column = "geometry_wkt"
        else:
            self.log.error(
                f"No geometry column found in agricultural fields data. Available columns: {fields_columns}"
            )
            raise ValueError("Required geometry column not found in agricultural fields data")

        # Convert geometry_wkt to geometry if needed and calculate field_area_m2
        if geometry_column == "geometry_wkt":
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE current_fields AS
                SELECT 
                    field_id,
                    block_id,
                    cvr_number,
                    year,
                    ST_GeomFromText({geometry_column}) as geom,
                    ST_Area(ST_GeomFromText({geometry_column})) as field_area_m2
                FROM {fields_table_name}
                WHERE {geometry_column} IS NOT NULL
            """)
        else:
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE current_fields AS
                SELECT 
                    field_id,
                    block_id,
                    cvr_number,
                    year,
                    {geometry_column} as geom,
                    ST_Area({geometry_column}) as field_area_m2
                FROM {fields_table_name}
                WHERE {geometry_column} IS NOT NULL
            """)

            # Debug: Check for invalid geometries that might have slipped through silver layer validation
        invalid_count = self.conn.execute("""
            SELECT COUNT(*) FROM current_fields 
            WHERE NOT ST_IsValid(geom)
        """).fetchone()[0]

        if invalid_count > 0:
            self.log.warning(
                f"⚠️ Found {invalid_count:,} invalid geometries in silver data - applying centralized geometry validator as fallback"
            )

            # Use the centralized geometry validator to fix the issue
            validate_and_transform_geometries_duckdb(
                self.conn, "current_fields", "field_area_analysis_fields", geometry_column="geom"
            )

        final_count = self.conn.execute("SELECT COUNT(*) FROM current_fields").fetchone()[0]
        self.log.info(f"✅ Prepared {final_count:,} agricultural fields for analysis")

    # Legacy year results table creation removed - now handled in optimized final results generation

    def _check_disk_space(self):
        """Check available disk space and log warning if low."""
        import shutil

        # Check main disk space
        total, used, free = shutil.disk_usage("/")
        total_gb = total / (1024**3)
        used_gb = used / (1024**3)
        free_gb = free / (1024**3)

        self.log.info(
            f"💾 Disk space: {free_gb:.1f}GB free / {total_gb:.1f}GB total ({used_gb:.1f}GB used)"
        )

        if free_gb < 5:
            self.log.warning(f"⚠️ Low disk space: only {free_gb:.1f}GB free")

        # Check temp directory space
        temp_dir = "/tmp/duckdb_field_analysis"
        if os.path.exists(temp_dir):
            temp_size = sum(
                os.path.getsize(os.path.join(dirpath, filename))
                for dirpath, dirnames, filenames in os.walk(temp_dir)
                for filename in filenames
            )
            temp_gb = temp_size / (1024**3)
            self.log.info(f"🗂️ Temp directory: {temp_gb:.1f}GB used")

    def _check_memory_usage(self):
        """Check current memory usage and log warning if high."""
        try:
            process = psutil.Process(os.getpid())
            memory_info = process.memory_info()
            memory_gb = memory_info.rss / (1024**3)

            # Get system memory info
            system_memory = psutil.virtual_memory()
            total_memory_gb = system_memory.total / (1024**3)
            available_memory_gb = system_memory.available / (1024**3)

            self.log.info(
                f"💾 Memory usage: {memory_gb:.1f}GB process / {available_memory_gb:.1f}GB available / {total_memory_gb:.1f}GB total"
            )

            if memory_gb > 10:  # Warning if process uses more than 10GB
                self.log.warning(f"⚠️ High memory usage: {memory_gb:.1f}GB")

            if available_memory_gb < 2:  # Warning if system has less than 2GB available
                self.log.warning(f"⚠️ Low system memory: only {available_memory_gb:.1f}GB available")

        except Exception as e:
            self.log.debug(f"Could not check memory usage: {e}")

    def _cleanup_temp_files(self):
        """Clean up temporary files to prevent disk space issues."""
        import glob
        import os
        import shutil

        # Check disk space before cleanup
        self._check_disk_space()

        temp_dir = "/tmp/duckdb_field_analysis"
        try:
            if os.path.exists(temp_dir):
                # ✅ FIXED: Only clean up files that are safe to remove
                # Don't remove active DuckDB temp files (they start with 'duckdb_temp_storage_')
                for file_path in glob.glob(os.path.join(temp_dir, "*")):
                    try:
                        file_name = os.path.basename(file_path)
                        # Skip active DuckDB temporary files to avoid race conditions
                        if file_name.startswith("duckdb_temp_storage_"):
                            self.log.debug(f"Skipping active DuckDB temp file: {file_name}")
                            continue

                        if os.path.isfile(file_path):
                            os.remove(file_path)
                            self.log.debug(f"Removed temp file: {file_name}")
                        elif os.path.isdir(file_path):
                            shutil.rmtree(file_path)
                            self.log.debug(f"Removed temp directory: {file_name}")
                    except Exception as e:
                        # Don't fail on individual file cleanup errors
                        self.log.debug(f"Could not remove temp file {file_path}: {e}")

                self.log.info(f"🧹 Cleaned up temporary files in {temp_dir}")
        except Exception as e:
            self.log.debug(f"Error cleaning temp files: {e}")

        # Check disk space after cleanup
        self._check_disk_space()

    def _force_duckdb_checkpoint(self):
        """Force DuckDB to checkpoint and free memory."""
        try:
            self.conn.execute("CHECKPOINT")
            # Note: PRAGMA force_checkpoint may not be available in all DuckDB versions
            # self.conn.execute("PRAGMA force_checkpoint")
            self.log.debug("✅ Forced DuckDB checkpoint")
        except Exception as e:
            self.log.debug(f"Could not force checkpoint: {e}")

    # Legacy year processing cleanup removed - now handled in optimized final cleanup

    def _get_available_fvm_marker_years(self) -> List[int]:
        """Get list of available FVM marker years from GCS."""
        try:
            # Look for fvm_marker datasets in silver layer
            pattern = f"gs://{self.config.bucket}/silver/fvm_marker_*/*/*.parquet"
            files = self.gcs_access.list_files(pattern)

            # Extract years from file paths
            years = set()
            for file_path in files:
                # Extract year from path like: gs://bucket/silver/fvm_marker_2023/...
                parts = file_path.split("/")
                for part in parts:
                    if part.startswith("fvm_marker_"):
                        year_str = part.replace("fvm_marker_", "")
                        try:
                            year = int(year_str)
                            years.add(year)
                        except ValueError:
                            continue

            available_years = sorted(list(years))
            self.log.info(f"Found FVM marker data for years: {available_years}")
            return available_years

        except Exception as e:
            self.log.error(f"Error finding available FVM marker years: {e}")
            return []

    def _safe_close_connection(self):
        """Safely close the DuckDB connection with proper cleanup."""
        try:
            if hasattr(self, "conn") and self.conn:
                # ✅ FIXED: Force final checkpoint before closing
                self.log.debug("🔄 Final checkpoint before closing connection...")
                self.conn.execute("CHECKPOINT")

                # Close the connection
                self.log.debug("🔒 Closing DuckDB connection...")
                self.conn.close()

                # ✅ FIXED: Clean up temp files after connection is closed
                # This avoids race conditions with DuckDB's own cleanup
                self.log.debug("🧹 Final cleanup of temp files...")
                self._final_cleanup_temp_files()

        except Exception as e:
            self.log.debug(f"Error during connection cleanup: {e}")
            # Try to close connection even if checkpoint failed
            try:
                if hasattr(self, "conn") and self.conn:
                    self.conn.close()
            except Exception as close_error:
                self.log.debug(f"Error closing connection: {close_error}")

    def _final_cleanup_temp_files(self):
        """Final cleanup of all temporary files after DuckDB connection is closed."""
        import glob
        import os
        import shutil

        temp_dir = "/tmp/duckdb_field_analysis"
        try:
            if os.path.exists(temp_dir):
                # Now we can safely remove all files since DuckDB is closed
                for file_path in glob.glob(os.path.join(temp_dir, "*")):
                    try:
                        if os.path.isfile(file_path):
                            os.remove(file_path)
                        elif os.path.isdir(file_path):
                            shutil.rmtree(file_path)
                    except Exception as e:
                        self.log.debug(f"Could not remove temp file {file_path}: {e}")

                # Try to remove the temp directory itself if it's empty
                try:
                    os.rmdir(temp_dir)
                    self.log.debug(f"Removed empty temp directory: {temp_dir}")
                except OSError:
                    # Directory not empty or other error - that's OK
                    pass

                self.log.info(f"🧹 Final cleanup completed for {temp_dir}")
        except Exception as e:
            self.log.debug(f"Error in final temp file cleanup: {e}")
