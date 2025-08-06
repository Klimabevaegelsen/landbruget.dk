"""
Field Area Analysis Gold Layer - Clean Implementation

This module implements field area analysis with environmental coverage by water projects.

For each agricultural field, calculates:
- Basic intersections: soil types, BNBO status, wetlands, water projects, properties
- Environmental coverage: what percentage of BNBO and wetland areas are covered by water projects

Uses DuckDB Spatial v1.2.2 SPATIAL_JOIN operator for optimal performance.
"""

import os
import time
from typing import Any, Dict, List, Optional

from pydantic import ConfigDict

from unified_pipeline.common.base import BaseJobConfig, BaseSource, GoldJobInterface
from unified_pipeline.util.log_util import Logger


class FieldAreaAnalysisGoldConfig(BaseJobConfig):
    """Configuration for Field Area Analysis gold layer."""

    name: str = "Field Area Analysis Gold"
    dataset: str = "field_area_analysis"
    type: str = "gold"
    description: str = (
        "Comprehensive spatial analysis of agricultural fields with environmental coverage"
    )
    frequency: str = "weekly"
    bucket: str = os.getenv("GCS_BUCKET")

    # Input silver datasets
    agricultural_fields_dataset: str = "fvm_marker"
    properties_dataset: str = "property_cadastral_merged"
    soil_types_dataset: str = "soil_types"
    bnbo_status_dataset: str = "bnbo_status_dissolved"
    wetlands_dataset: str = "wetlands_dissolved"
    water_projects_dataset: str = "water_projects_dissolved"

    # Processing configuration
    batch_size: int = 250000

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class FieldAreaAnalysisGold(BaseSource[FieldAreaAnalysisGoldConfig], GoldJobInterface):
    """Field Area Analysis Gold processor with environmental coverage analysis."""

    def __init__(self, config: FieldAreaAnalysisGoldConfig):
        super().__init__(config)
        self.log = Logger.get_logger()
        self.phase_times = {}
        self.properties_path = None

    async def run(self, silver_data: Optional[Dict[str, Any]] = None) -> None:
        """Execute the Field Area Analysis Gold processing pipeline."""
        start_time = time.time()
        self.log.info("🚀 Starting Field Area Analysis Gold processing")

        try:
            # Phase 1: Load data
            self.log.info("📥 Phase 1: Loading data...")
            phase_start = time.time()
            self._load_agricultural_fields()
            self._load_reference_datasets()
            self.phase_times["loading"] = time.time() - phase_start
            self.log.info(f"✅ Phase 1 completed in {self.phase_times['loading']:.1f} seconds")

            # Phase 2: Basic spatial joins
            self.log.info("⚡ Phase 2: Basic spatial joins...")
            phase_start = time.time()
            self._execute_basic_spatial_joins()
            self.phase_times["basic_joins"] = time.time() - phase_start
            self.log.info(f"✅ Phase 2 completed in {self.phase_times['basic_joins']:.1f} seconds")

            # Phase 3: Environmental coverage analysis
            self.log.info("🌿 Phase 3: Environmental coverage by water projects...")
            phase_start = time.time()
            self._calculate_environmental_coverage()
            self.phase_times["environmental_coverage"] = time.time() - phase_start
            self.log.info(
                f"✅ Phase 3 completed in {self.phase_times['environmental_coverage']:.1f} seconds"
            )

            # Phase 4: Properties processing
            self.log.info("🏠 Phase 4: Properties processing...")
            phase_start = time.time()
            self._process_properties()
            self.phase_times["properties"] = time.time() - phase_start
            self.log.info(f"✅ Phase 4 completed in {self.phase_times['properties']:.1f} seconds")

            # Phase 5: Final results
            self.log.info("📊 Phase 5: Generating final results...")
            phase_start = time.time()
            results = self._generate_final_results()
            self.phase_times["final_results"] = time.time() - phase_start
            self.log.info(
                f"✅ Phase 5 completed in {self.phase_times['final_results']:.1f} seconds"
            )

            # Save results
            self.save_data_direct(
                "field_area_analysis_final",
                self.config.dataset,
                self.config.bucket,
                "gold",
            )

            # Performance summary
            total_time = time.time() - start_time
            self._log_performance_summary(total_time, results)

        except Exception as e:
            self.log.error(f"Field Area Analysis Gold processing failed: {e}")
            raise

    def _load_agricultural_fields(self):
        """Load agricultural fields from the latest available year."""
        available_years = self._get_available_fvm_marker_years()
        if not available_years:
            raise ValueError("No fvm_marker years found")

        latest_year = max(available_years)
        self.log.info(f"Processing latest year: {latest_year}")

        dataset_name = f"fvm_marker_{latest_year}"
        gcs_path = self._get_latest_silver_path(dataset_name)

        self.gcs_access.query_parquet_direct(
            gcs_path,
            f"""SELECT 
                field_id,
                block_id,
                cvr_number,
                field_uuid,
                COALESCE(
                    field_uuid, 
                    'legacy_' || CAST(cvr_number AS VARCHAR) || '_' || 
                    CAST(block_id AS VARCHAR) || '_' || CAST(field_id AS VARCHAR)
                ) as primary_field_id,
                {latest_year} as year,
                CASE 
                    WHEN geometry IS NOT NULL THEN geometry
                    WHEN geometry_wkt IS NOT NULL THEN ST_GeomFromText(geometry_wkt)
                    ELSE NULL
                END as geom
            WHERE crop_code IS NOT NULL
            """,
            "fields_raw",
        )

        self.conn.execute("""
            CREATE OR REPLACE TABLE agricultural_fields AS
                SELECT
                field_id,
                block_id,
                cvr_number,
                year,
                geom,
                field_uuid,
                primary_field_id,
                ST_Area_Spheroid(geom) as field_area_m2
            FROM fields_raw
            WHERE geom IS NOT NULL AND ST_IsValid(geom)
        """)

        field_count = self.conn.execute("SELECT COUNT(*) FROM agricultural_fields").fetchone()[0]
        self.log.info(f"✅ Loaded {field_count:,} valid agricultural fields for {latest_year}")

    def _load_reference_datasets(self):
        """Load all reference datasets."""
        # Soil types
        self._load_dataset(
            "soil_types",
            self.config.soil_types_dataset,
            "SELECT soil_code, soil_description, geometry as geom",
        )

        # BNBO status - split multipolygons
        self._load_dataset_with_split(
            "bnbo_status",
            self.config.bnbo_status_dataset,
            "SELECT status_category, (unnest(ST_Dump(geometry))).geom as geom",
        )

        # Water projects - split multipolygons
        self._load_dataset_with_split(
            "water_projects",
            self.config.water_projects_dataset,
            "SELECT project_id, (unnest(ST_Dump(geometry))).geom as geom",
        )

        # Wetlands
        self._load_dataset(
            "wetlands", self.config.wetlands_dataset, "SELECT wetland_id, geometry as geom"
        )

        # Properties (for later chunked processing)
        try:
            self.properties_path = self._get_latest_silver_path(self.config.properties_dataset)
            self.log.info("✅ Properties dataset path ready for chunked processing")
        except FileNotFoundError:
            self.log.warning("No properties dataset found")
            self.properties_path = None

    def _load_dataset(self, table_name: str, dataset_name: str, select_query: str):
        """Load a dataset with the given select query."""
        try:
            path = self._get_latest_silver_path(dataset_name)
            self.gcs_access.query_parquet_direct(path, select_query, table_name)
            count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            self.log.info(f"✅ Loaded {count:,} {table_name} records")
        except FileNotFoundError:
            self.log.warning(f"No data found for {dataset_name}")
            self.conn.execute(f"CREATE OR REPLACE TABLE {table_name} (geom GEOMETRY)")

    def _load_dataset_with_split(self, table_name: str, dataset_name: str, select_query: str):
        """Load a dataset and split multipolygons."""
        try:
            path = self._get_latest_silver_path(dataset_name)
            self.gcs_access.create_table_from_gcs(f"{table_name}_raw", path)

            self.conn.execute(f"""
                CREATE OR REPLACE TABLE {table_name} AS
                {select_query}
                FROM {table_name}_raw
            """)

            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}_raw")
            count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            self.log.info(f"✅ Loaded and split {count:,} {table_name} polygons")
        except FileNotFoundError:
            self.log.warning(f"No data found for {dataset_name}")
            self.conn.execute(f"CREATE OR REPLACE TABLE {table_name} (geom GEOMETRY)")

    def _execute_basic_spatial_joins(self):
        """Execute basic spatial joins using SPATIAL_JOIN operator."""

        # Start with agricultural fields
        self.conn.execute(
            "CREATE OR REPLACE TABLE current_fields AS SELECT * FROM agricultural_fields"
        )

        # 1. Soil types
        self.conn.execute("""
            CREATE OR REPLACE TABLE fields_with_soil AS
            SELECT
                f.*,
                s.soil_code,
                s.soil_description
            FROM current_fields f
            LEFT JOIN soil_types s ON ST_Intersects(f.geom, s.geom)
        """)

        # 2. BNBO status
        self.conn.execute("""
            CREATE OR REPLACE TABLE fields_with_bnbo AS
            SELECT
                f.*,
                b.status_category
            FROM fields_with_soil f
            LEFT JOIN bnbo_status b ON ST_Intersects(f.geom, b.geom)
        """)

        # 3. Water projects
        self.conn.execute("""
            CREATE OR REPLACE TABLE fields_with_water AS
            SELECT
                f.*,
                wp.project_id
            FROM fields_with_bnbo f
            LEFT JOIN water_projects wp ON ST_Intersects(f.geom, wp.geom)
        """)

        # 4. Wetlands
        self.conn.execute("""
            CREATE OR REPLACE TABLE fields_with_wetlands AS
            SELECT
                f.*,
                w.wetland_id
            FROM fields_with_water f
            LEFT JOIN wetlands w ON ST_Intersects(f.geom, w.geom)
        """)

    def _calculate_environmental_coverage(self):
        """Calculate environmental area coverage by water projects for each field."""

        self.log.info("🔄 Calculating BNBO and wetland coverage by water projects...")

        # Calculate overlapping areas between environmental datasets and water projects
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_environmental_coverage AS
            SELECT
                f.field_id,
                f.block_id,
                f.cvr_number,
                f.field_uuid,
                f.primary_field_id,
                f.field_area_m2,
                
                -- BNBO area calculations
                COALESCE(
                    SUM(CASE 
                        WHEN b.status_category IS NOT NULL 
                        THEN ST_Area_Spheroid(ST_Intersection(f.geom, b.geom))
                        ELSE 0 
                    END), 0
                ) as total_bnbo_area_m2,
                
                COALESCE(
                    SUM(CASE 
                        WHEN b.status_category IS NOT NULL AND wp.project_id IS NOT NULL
                        THEN ST_Area_Spheroid(ST_Intersection(ST_Intersection(f.geom, b.geom), wp.geom))
                        ELSE 0 
                    END), 0
                ) as bnbo_covered_by_water_projects_m2,
                
                -- Wetland area calculations  
                COALESCE(
                    SUM(CASE 
                        WHEN w.wetland_id IS NOT NULL 
                        THEN ST_Area_Spheroid(ST_Intersection(f.geom, w.geom))
                        ELSE 0 
                    END), 0
                ) as total_wetland_area_m2,
                
                COALESCE(
                    SUM(CASE 
                        WHEN w.wetland_id IS NOT NULL AND wp.project_id IS NOT NULL
                        THEN ST_Area_Spheroid(ST_Intersection(ST_Intersection(f.geom, w.geom), wp.geom))
                        ELSE 0 
                    END), 0
                ) as wetland_covered_by_water_projects_m2
                
            FROM current_fields f
            LEFT JOIN bnbo_status b ON ST_Intersects(f.geom, b.geom)
            LEFT JOIN wetlands w ON ST_Intersects(f.geom, w.geom)
            LEFT JOIN water_projects wp ON ST_Intersects(f.geom, wp.geom)
            GROUP BY (
                f.field_id, f.block_id, f.cvr_number, f.field_area_m2, 
                f.field_uuid, f.primary_field_id
            )
        """)

        # Calculate coverage percentages
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_coverage_percentages AS
            SELECT 
                *,
                -- BNBO coverage percentages
                CASE 
                    WHEN total_bnbo_area_m2 > 0 
                    THEN (bnbo_covered_by_water_projects_m2 / total_bnbo_area_m2) * 100
                    ELSE 0 
                END as bnbo_covered_by_water_projects_pct,
                
                CASE 
                    WHEN total_bnbo_area_m2 > 0 
                    THEN ((total_bnbo_area_m2 - bnbo_covered_by_water_projects_m2) / total_bnbo_area_m2) * 100
                    ELSE 0 
                END as bnbo_not_covered_by_water_projects_pct,
                
                -- Wetland coverage percentages
                CASE 
                    WHEN total_wetland_area_m2 > 0 
                    THEN (wetland_covered_by_water_projects_m2 / total_wetland_area_m2) * 100
                    ELSE 0 
                END as wetland_covered_by_water_projects_pct,
                
                CASE 
                    WHEN total_wetland_area_m2 > 0 
                    THEN ((total_wetland_area_m2 - wetland_covered_by_water_projects_m2) / total_wetland_area_m2) * 100
                    ELSE 0 
                END as wetland_not_covered_by_water_projects_pct,
                
                -- Field-level percentages
                (total_bnbo_area_m2 / field_area_m2) * 100 as field_bnbo_coverage_pct,
                (total_wetland_area_m2 / field_area_m2) * 100 as field_wetland_coverage_pct
                
            FROM field_environmental_coverage
        """)

        # Log summary
        summary = self.conn.execute("""
                SELECT
                COUNT(*) as total_fields,
                COUNT(CASE WHEN total_bnbo_area_m2 > 0 THEN 1 END) as fields_with_bnbo,
                COUNT(CASE WHEN total_wetland_area_m2 > 0 THEN 1 END) as fields_with_wetlands,
                AVG(CASE WHEN total_bnbo_area_m2 > 0 THEN bnbo_covered_by_water_projects_pct ELSE NULL END) as avg_bnbo_coverage,
                AVG(CASE WHEN total_wetland_area_m2 > 0 THEN wetland_covered_by_water_projects_pct ELSE NULL END) as avg_wetland_coverage
            FROM field_coverage_percentages
        """).fetchone()

        self.log.info("✅ Environmental coverage analysis completed")
        self.log.info(f"   Fields with BNBO areas: {summary[1]:,}")
        self.log.info(f"   Fields with wetland areas: {summary[2]:,}")
        if summary[3] is not None:
            self.log.info(f"   Average BNBO coverage by water projects: {summary[3]:.1f}%")
        if summary[4] is not None:
            self.log.info(f"   Average wetland coverage by water projects: {summary[4]:.1f}%")

    def _process_properties(self):
        """Process properties dataset with chunking if needed."""
        if not self.properties_path:
            self.log.warning("No properties data available, skipping...")
            return

        self.log.info("🔄 Processing properties...")

        # Get total count
        with self.gcs_access._temp_download(self.properties_path) as temp_file:
            total_properties = self.conn.execute(f"""
                SELECT COUNT(*) FROM read_parquet('{temp_file}')
            """).fetchone()[0]

        self.log.info(f"Processing {total_properties:,} properties...")

        # Create results table
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_property_results (
                field_id VARCHAR,
                block_id VARCHAR,
                cvr_number VARCHAR,
                bfe_number VARCHAR,
                intersection_area_m2 DOUBLE,
                field_uuid VARCHAR,
                primary_field_id VARCHAR
            )
        """)

        # Process in chunks
        chunk_size = self.config.batch_size
        processed = 0

        for offset in range(0, total_properties, chunk_size):
            chunk_size_actual = min(chunk_size, total_properties - offset)

            self.log.info(
                f"   Processing chunk: {processed:,} to {processed + chunk_size_actual:,}"
            )

            with self.gcs_access._temp_download(self.properties_path) as temp_file:
                self.conn.execute(f"""
                    CREATE OR REPLACE TABLE properties_chunk AS
                    SELECT * FROM read_parquet('{temp_file}')
                    LIMIT {chunk_size_actual} OFFSET {offset}
                """)

                self.conn.execute("""
                INSERT INTO field_property_results
                    SELECT
                        f.field_id,
                        f.block_id,
                        f.cvr_number,
                        p.bfe_number,
                        ST_Area_Spheroid(ST_Intersection(f.geom, p.geom)) / f.field_area_m2 * 100 as area_share,
                        f.field_uuid,
                        f.primary_field_id
                    FROM fields_with_wetlands f
                    JOIN properties_chunk p ON ST_Intersects(f.geom, p.geom)
                WHERE ST_Area_Spheroid(ST_Intersection(f.geom, p.geom)) / f.field_area_m2 > 0.01
            """)

            processed += chunk_size_actual

        self.log.info("✅ Properties processing completed")

    def _generate_final_results(self) -> Dict[str, any]:
        """Generate final consolidated results."""

        # Check what data is available
        has_properties = False
        try:
            self.conn.execute("SELECT COUNT(*) FROM field_property_results").fetchone()
            has_properties = True
        except Exception:
            pass

        has_environmental_coverage = False
        try:
            self.conn.execute("SELECT COUNT(*) FROM field_coverage_percentages").fetchone()
            has_environmental_coverage = True
        except Exception:
            pass

        # Create final results table
        if has_properties and has_environmental_coverage:
            self.conn.execute("""
                CREATE OR REPLACE TABLE field_area_analysis_final AS
                SELECT
                    f.field_id,
                    f.block_id,
                    f.cvr_number,
                    f.field_area_m2,
                    f.soil_code,
                    f.soil_description,
                    f.status_category as bnbo_status,
                    f.project_id as water_project_id,
                    f.wetland_id,
                    p.bfe_number,
                    p.area_share as property_area_share,
                    -- Environmental coverage by water projects
                    ec.total_bnbo_area_m2,
                    ec.bnbo_covered_by_water_projects_m2,
                    ec.bnbo_covered_by_water_projects_pct,
                    ec.bnbo_not_covered_by_water_projects_pct,
                    ec.total_wetland_area_m2,
                    ec.wetland_covered_by_water_projects_m2,
                    ec.wetland_covered_by_water_projects_pct,
                    ec.wetland_not_covered_by_water_projects_pct,
                    ec.field_bnbo_coverage_pct,
                    ec.field_wetland_coverage_pct
                FROM fields_with_wetlands f
                LEFT JOIN field_property_results p 
                    ON COALESCE(f.field_uuid, f.primary_field_id) = 
                       COALESCE(p.field_uuid, p.primary_field_id)
                LEFT JOIN field_coverage_percentages ec
                    ON COALESCE(f.field_uuid, f.primary_field_id) = 
                       COALESCE(ec.field_uuid, ec.primary_field_id)
            """)
        elif has_environmental_coverage:
            self.conn.execute("""
                CREATE OR REPLACE TABLE field_area_analysis_final AS
                SELECT
                    f.field_id,
                    f.block_id,
                    f.cvr_number,
                    f.field_area_m2,
                    f.soil_code,
                    f.soil_description,
                    f.status_category as bnbo_status,
                    f.project_id as water_project_id,
                    f.wetland_id,
                    CAST(NULL AS VARCHAR) as bfe_number,
                    CAST(NULL AS DOUBLE) as property_area_share,
                    -- Environmental coverage by water projects
                    ec.total_bnbo_area_m2,
                    ec.bnbo_covered_by_water_projects_m2,
                    ec.bnbo_covered_by_water_projects_pct,
                    ec.bnbo_not_covered_by_water_projects_pct,
                    ec.total_wetland_area_m2,
                    ec.wetland_covered_by_water_projects_m2,
                    ec.wetland_covered_by_water_projects_pct,
                    ec.wetland_not_covered_by_water_projects_pct,
                    ec.field_bnbo_coverage_pct,
                    ec.field_wetland_coverage_pct
                    FROM fields_with_wetlands f
                LEFT JOIN field_coverage_percentages ec
                    ON COALESCE(f.field_uuid, f.primary_field_id) = 
                       COALESCE(ec.field_uuid, ec.primary_field_id)
                """)
        else:
            # Fallback without environmental coverage
            self.conn.execute("""
                CREATE OR REPLACE TABLE field_area_analysis_final AS
                SELECT
                    f.field_id,
                    f.block_id,
                    f.cvr_number,
                    f.field_area_m2,
                    f.soil_code,
                    f.soil_description,
                    f.status_category as bnbo_status,
                    f.project_id as water_project_id,
                    f.wetland_id,
                    CAST(NULL AS VARCHAR) as bfe_number,
                    CAST(NULL AS DOUBLE) as property_area_share
                FROM fields_with_wetlands f
            """)

        # Generate summary statistics
        total_fields = self.conn.execute(
            "SELECT COUNT(*) FROM field_area_analysis_final"
        ).fetchone()[0]

        soil_coverage = self.conn.execute("""
            SELECT COUNT(*) FROM field_area_analysis_final WHERE soil_code IS NOT NULL
            """).fetchone()[0]

        bnbo_coverage = self.conn.execute("""
            SELECT COUNT(*) FROM field_area_analysis_final WHERE bnbo_status IS NOT NULL
        """).fetchone()[0]

        wetland_coverage = self.conn.execute("""
            SELECT COUNT(*) FROM field_area_analysis_final WHERE wetland_id IS NOT NULL
        """).fetchone()[0]

        water_project_coverage = self.conn.execute("""
            SELECT COUNT(*) FROM field_area_analysis_final WHERE water_project_id IS NOT NULL
        """).fetchone()[0]

        property_coverage = self.conn.execute("""
            SELECT COUNT(*) FROM field_area_analysis_final WHERE bfe_number IS NOT NULL
        """).fetchone()[0]

        # Environmental coverage statistics
        environmental_stats = {}
        if has_environmental_coverage:
            env_summary = self.conn.execute("""
                SELECT 
                    COUNT(CASE WHEN total_bnbo_area_m2 > 0 THEN 1 END) as fields_with_bnbo,
                    COUNT(CASE WHEN total_wetland_area_m2 > 0 THEN 1 END) as fields_with_wetlands,
                    AVG(CASE WHEN total_bnbo_area_m2 > 0 THEN bnbo_covered_by_water_projects_pct ELSE NULL END) as avg_bnbo_coverage,
                    AVG(CASE WHEN total_wetland_area_m2 > 0 THEN wetland_covered_by_water_projects_pct ELSE NULL END) as avg_wetland_coverage
                FROM field_area_analysis_final
                WHERE total_bnbo_area_m2 IS NOT NULL OR total_wetland_area_m2 IS NOT NULL
            """).fetchone()

            environmental_stats = {
                "fields_with_bnbo": env_summary[0] or 0,
                "fields_with_wetlands": env_summary[1] or 0,
                "avg_bnbo_coverage_by_water_projects": env_summary[2] or 0,
                "avg_wetland_coverage_by_water_projects": env_summary[3] or 0,
            }

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
                "water_projects": {
                    "count": water_project_coverage,
                    "percentage": water_project_coverage / total_fields * 100
                    if total_fields > 0
                    else 0,
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
            "environmental_coverage_by_water_projects": environmental_stats,
        }

        return results

    def _log_performance_summary(self, total_time: float, results: Dict[str, any]):
        """Log performance summary."""
        self.log.info("\n" + "=" * 80)
        self.log.info("📈 FIELD AREA ANALYSIS PERFORMANCE SUMMARY")
        self.log.info("=" * 80)
        self.log.info(
            f"Total execution time: {total_time:.1f} seconds ({total_time / 60:.1f} minutes)"
        )
        self.log.info(f"Data loading: {self.phase_times['loading']:.1f}s")
        self.log.info(f"Basic spatial joins: {self.phase_times['basic_joins']:.1f}s")
        self.log.info(f"Environmental coverage: {self.phase_times['environmental_coverage']:.1f}s")
        self.log.info(f"Properties processing: {self.phase_times['properties']:.1f}s")
        self.log.info(f"Final results: {self.phase_times['final_results']:.1f}s")
        self.log.info(f"Fields processed: {results['total_fields']:,}")

        env_stats = results.get("environmental_coverage_by_water_projects", {})
        if env_stats:
            self.log.info("🌿 Environmental Coverage Analysis:")
            self.log.info(f"   Fields with BNBO: {env_stats.get('fields_with_bnbo', 0):,}")
            self.log.info(f"   Fields with wetlands: {env_stats.get('fields_with_wetlands', 0):,}")
            self.log.info(
                f"   Avg BNBO coverage by water projects: "
                f"{env_stats.get('avg_bnbo_coverage_by_water_projects', 0):.1f}%"
            )
            self.log.info(
                f"   Avg wetland coverage by water projects: "
                f"{env_stats.get('avg_wetland_coverage_by_water_projects', 0):.1f}%"
            )

        self.log.info("🚀 DUCKDB SPATIAL v1.2.2 SPATIAL_JOIN OPERATOR USED")
        self.log.info("=" * 80)

    def _get_available_fvm_marker_years(self) -> List[int]:
        """Get list of available FVM marker years from GCS."""
        return super()._get_available_fvm_marker_years()

    def _get_latest_silver_path_for_dataset(self, dataset_name: str) -> Optional[str]:
        """Get the latest silver data path for a specific dataset."""
        try:
            return self._get_latest_silver_path(dataset_name)
        except FileNotFoundError:
            return None
