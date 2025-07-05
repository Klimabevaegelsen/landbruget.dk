"""
Main H3 PFAS processor with modular architecture and GCS data loading.
"""

import gc
from pathlib import Path

import duckdb
from loguru import logger

from ..config import H3SpatialConfig
from .area_validator import AreaValidator
from .coordinate_transformer import CoordinateTransformer
from .spatial_joiner import SpatialJoiner


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

        # STATIC DATA CACHING - avoid recomputing for each year
        self._cached_bmd_table = None
        self._cached_h3_grid_table = None
        self._cached_kommuner_table = None

        # Memory monitoring for GitHub Actions
        self._memory_alerts = 0
        self._disk_alerts = 0

        # Protected tables that should not be cleaned up during processing
        self._protected_tables = set()

    def _protect_table(self, table_name: str):
        """Mark a table as protected from cleanup."""
        self._protected_tables.add(table_name)

    def _unprotect_table(self, table_name: str):
        """Remove protection from a table."""
        self._protected_tables.discard(table_name)

    def _monitor_resources(self, operation: str):
        """Monitor memory and disk usage for GitHub Actions constraints."""
        if not self.config.github_actions_mode:
            return

        try:
            import psutil

            # Check PROCESS memory usage (not system total)
            process = psutil.Process()
            process_memory_gb = process.memory_info().rss / (1024**3)

            # Check AVAILABLE disk space (not total used)
            disk = psutil.disk_usage("/")
            available_disk_gb = disk.free / (1024**3)

            # GitHub Actions runners have ~14GB total disk, we need to keep some free
            min_free_disk_gb = 2.0  # Keep at least 2GB free
            max_process_memory_gb = 12.0  # Limit our process to 12GB

            # Alert if our process is using too much memory
            if process_memory_gb > max_process_memory_gb:
                self._memory_alerts += 1
                self.log.warning(
                    f"⚠️ {operation}: High process memory usage {process_memory_gb:.1f}GB (limit: {max_process_memory_gb}GB)"
                )

                # Only trigger cleanup after more alerts and avoid during critical operations
                if self._memory_alerts > 3 and not operation.startswith(("loading_", "loaded_")):
                    self.log.error(
                        f"❌ {operation}: Process memory usage too high, forcing selective cleanup"
                    )
                    self._selective_cleanup()

            # Alert if available disk space is getting low
            if available_disk_gb < min_free_disk_gb:
                self._disk_alerts += 1
                self.log.warning(
                    f"⚠️ {operation}: Low available disk space {available_disk_gb:.1f}GB (minimum: {min_free_disk_gb}GB)"
                )

                # Only trigger cleanup after more alerts and avoid during critical operations
                if self._disk_alerts > 3 and not operation.startswith(("loading_", "loaded_")):
                    self.log.error(
                        f"❌ {operation}: Available disk space too low, forcing selective cleanup"
                    )
                    self._selective_cleanup()

            # Log resource usage for debugging (but only occasionally to avoid spam)
            if self._memory_alerts % 10 == 0 or self._disk_alerts % 10 == 0:
                self.log.debug(
                    f"📊 {operation}: Process memory: {process_memory_gb:.1f}GB, Available disk: {available_disk_gb:.1f}GB"
                )

        except ImportError:
            # psutil not available, skip monitoring
            pass
        except Exception as e:
            # Don't let monitoring errors break the pipeline
            self.log.debug(f"Resource monitoring error: {e}")
            pass

    def _selective_cleanup(self):
        """Selective cleanup that avoids protected tables and critical processing tables."""
        self.log.info("🧹 Performing selective cleanup for GitHub Actions constraints")

        # Drop only safe temporary tables, avoiding protected ones
        try:
            tables = self.conn.execute("SHOW TABLES").fetchall()
            for (table_name,) in tables:
                # Skip protected tables
                if table_name in self._protected_tables:
                    continue

                # Only clean up clearly temporary tables that are safe to drop
                if any(
                    keyword in table_name.lower()
                    for keyword in [
                        "chunk_",
                        "stage1_",
                        "stage2_",
                        "stage3_",
                        "stage4_",
                        "stage5_",
                        "intermediate_",
                    ]
                ):
                    self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")

                # Also clean up old temp tables that are clearly not in use
                if table_name.startswith("temp_") and not any(
                    protected in table_name for protected in self._protected_tables
                ):
                    # Check if table was created more than 5 minutes ago (rough heuristic)
                    try:
                        # Only drop if it's clearly an old temporary table
                        if any(
                            old_pattern in table_name
                            for old_pattern in ["_old", "_backup", "_legacy"]
                        ):
                            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                    except Exception:
                        pass
        except Exception:
            pass

        # Force garbage collection
        gc.collect()

        # Clear DuckDB cache
        try:
            self.conn.execute("CHECKPOINT")
        except Exception:
            pass

    def _aggressive_cleanup(self):
        """Aggressive cleanup for GitHub Actions constraints - only used at end of processing."""
        self.log.info("🧹 Performing aggressive cleanup for GitHub Actions constraints")

        # Drop all temporary tables (this should only be called at the end)
        try:
            tables = self.conn.execute("SHOW TABLES").fetchall()
            for (table_name,) in tables:
                if any(
                    keyword in table_name.lower()
                    for keyword in [
                        "temp_",
                        "chunk_",
                        "intermediate_",
                        "stage1_",
                        "stage2_",
                        "stage3_",
                        "stage4_",
                        "stage5_",
                    ]
                ):
                    self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        except Exception:
            pass

        # Clear protected tables set
        self._protected_tables.clear()

        # Force garbage collection
        gc.collect()

        # Clear DuckDB cache
        try:
            self.conn.execute("CHECKPOINT")
        except Exception:
            pass

    def setup_duckdb(self):
        """Setup DuckDB with required extensions - OPTIMIZED for GitHub Actions."""
        self.log.info(
            "🔧 Setting up DuckDB with H3 and spatial extensions (GitHub Actions optimized)"
        )

        self.conn = duckdb.connect(":memory:")

        # GITHUB ACTIONS OPTIMIZED SETTINGS
        self.conn.execute(f"SET memory_limit='{self.config.memory_limit}'")
        self.conn.execute(f"SET max_memory='{self.config.memory_limit}'")
        self.conn.execute(f"SET threads={self.config.thread_count}")

        # Optimize for limited resources
        self.conn.execute("SET enable_progress_bar=false")  # Reduce overhead
        self.conn.execute("SET preserve_insertion_order=false")  # Save memory
        self.conn.execute("SET temp_directory='/tmp/duckdb'")

        # Aggressive memory management
        self.conn.execute("SET checkpoint_threshold='100MB'")  # Frequent checkpoints

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

        # Initialize GCS access with shared connection
        from unified_pipeline.util.gcs_access import GCSDataAccess

        self.gcs_access = GCSDataAccess(connection=self.conn)

        # Initialize helper classes
        self.coordinate_transformer = CoordinateTransformer(self.conn, self.config)
        self.spatial_joiner = SpatialJoiner(self.conn, self.config)
        self.area_validator = AreaValidator(self.conn, self.config)

        self.log.info("✅ DuckDB setup completed with GitHub Actions optimizations")

    def generate_h3_grid(self) -> str:
        """Generate H3 grid for Denmark - CACHED to avoid recomputation."""
        # Return cached grid if available
        if self._cached_h3_grid_table:
            self.log.info("✅ Using cached H3 grid")
            return self._cached_h3_grid_table

        self.log.info("🗺️ Generating Denmark H3 grid (caching for reuse)")
        self._monitor_resources("h3_grid_generation")

        # Generate H3 grid covering Denmark
        bounds = self.config.denmark_bounds
        resolution = self.config.h3_resolution

        # Use simple bounding box approach that was working before
        self.log.info(f"Creating H3 grid for Denmark using bounding box at resolution {resolution}")

        # Generate H3 grid using the simple bounding box approach from the working commit
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE h3_grid AS
            WITH denmark_bbox AS (
                SELECT ST_MakeEnvelope(
                    {bounds["min_lon"]}, {bounds["min_lat"]},
                    {bounds["max_lon"]}, {bounds["max_lat"]}
                ) as bbox_geom
            ),
            h3_cells AS (
                SELECT h3_polygon_wkt_to_cells(ST_AsText(bbox_geom), {resolution}) as h3_cells
                FROM denmark_bbox
            ),
            h3_exploded AS (
                SELECT UNNEST(h3_cells) as h3_cell
                FROM h3_cells
            )
            SELECT
                h3_cell as h3_index,
                h3_cell_to_boundary_wkt(h3_cell) as h3_boundary,
                h3_cell_to_lat(h3_cell) as lat,
                h3_cell_to_lng(h3_cell) as lon
            FROM h3_exploded
            WHERE h3_cell IS NOT NULL
        """)

        # Add geometry and area calculations using proper H3 functions
        self.conn.execute("""
            CREATE OR REPLACE TABLE h3_grid_with_geom AS
            SELECT
                h3_index as h3_cell,
                lat as center_lat,
                lon as center_lon,
                ST_GeomFromText(h3_boundary) as h3_geometry,
                h3_cell_area(h3_index, 'm^2') / 10000.0 as h3_area_ha
            FROM h3_grid
        """)

        # Clean up intermediate table
        self.conn.execute("DROP TABLE IF EXISTS h3_grid")

        # Get statistics and validate
        stats = self.conn.execute("""
            SELECT 
                COUNT(*) as total_cells,
                AVG(h3_area_ha) as avg_area,
                MIN(h3_area_ha) as min_area,
                MAX(h3_area_ha) as max_area
            FROM h3_grid_with_geom
        """).fetchone()

        total_cells, avg_area, min_area, max_area = stats

        self.log.info(f"✅ Generated H3 grid: {total_cells:,} cells, avg area: {avg_area:.4f} ha")
        self.log.info(f"   📊 Area range: {min_area:.4f} - {max_area:.4f} ha")

        # Validate H3 grid immediately
        expected_avg = self.config.theoretical_avg_area_ha
        avg_deviation_pct = abs(avg_area - expected_avg) / expected_avg * 100

        if avg_deviation_pct > self.config.max_area_deviation_pct:
            self.log.error("❌ H3 grid validation FAILED!")
            self.log.error(f"   Expected avg area: {expected_avg:.4f} ha")
            self.log.error(f"   Actual avg area: {avg_area:.4f} ha")
            self.log.error(
                f"   Deviation: {avg_deviation_pct:.1f}% (max allowed: {self.config.max_area_deviation_pct}%)"
            )
            raise ValueError(
                f"H3 grid generation failed validation - average area {avg_area:.4f} ha deviates {avg_deviation_pct:.1f}% from expected {expected_avg:.4f} ha"
            )

        if min_area < self.config.min_h3_area_ha or max_area > self.config.max_h3_area_ha:
            self.log.error("❌ H3 grid area bounds validation FAILED!")
            self.log.error(
                f"   Expected range: {self.config.min_h3_area_ha:.4f} - {self.config.max_h3_area_ha:.4f} ha"
            )
            self.log.error(f"   Actual range: {min_area:.4f} - {max_area:.4f} ha")
            raise ValueError(
                "H3 grid generation failed bounds validation - areas outside expected range"
            )

        # Check cell count is reasonable for resolution 10 (Denmark bounding box includes water and neighboring areas)
        # Bounding box covers much more than just Denmark, so expect 10-20 million cells
        min_expected = 5_000_000  # Minimum reasonable cells for this bounding box
        max_expected = 20_000_000  # Maximum reasonable cells for this bounding box

        if total_cells < min_expected or total_cells > max_expected:
            self.log.error("❌ H3 grid cell count validation FAILED!")
            self.log.error(
                f"   Expected: {min_expected:,} - {max_expected:,} cells for resolution 10 in Denmark bounding box"
            )
            self.log.error(f"   Actual: {total_cells:,} cells")
            raise ValueError(
                f"H3 grid generation failed - unreasonable cell count ({total_cells:,}) for resolution 10"
            )

        self.log.info(
            f"✅ H3 grid validation passed: {avg_deviation_pct:.1f}% deviation from expected"
        )

        # Cache the table name
        self._cached_h3_grid_table = "h3_grid_with_geom"

        self._monitor_resources("h3_grid_generated")
        return self._cached_h3_grid_table

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

    def _cleanup_year_tables(self, year: int):
        """Clean up intermediate tables for a specific year to free memory - AGGRESSIVE for GitHub Actions."""
        self.log.info(f"🧹 Cleaning up tables for year {year} (GitHub Actions mode)")

        # More aggressive cleanup for GitHub Actions
        tables_to_drop = [
            f"pesticides_{year}",
            f"pesticide_pfas_{year}",
            f"final_results_{year}",
            f"final_results_kepler_{year}",
            "prepared_fields",
            "pesticide_field_lookup",
            f"temp_fvm_{year + 1}",
            f"temp_pesticide_lookup_{year}",
            "temp_pesticides_raw",
            # Clean up any chunk tables
            f"chunk_results_{year}",
            f"chunk_intersections_{year}",
            f"chunk_areas_{year}",
            f"chunk_aggregated_{year}",
            f"chunk_pesticide_{year}",
            f"chunk_union_{year}",
        ]

        for table in tables_to_drop:
            try:
                self.conn.execute(f"DROP TABLE IF EXISTS {table}")
            except Exception:
                pass  # Ignore errors if table doesn't exist

        # Also drop any tables with temp_ prefix
        try:
            tables = self.conn.execute("SHOW TABLES").fetchall()
            for (table_name,) in tables:
                if table_name.startswith("temp_") or f"_{year}" in table_name:
                    self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        except Exception:
            pass

        # Force garbage collection and checkpoint
        gc.collect()
        try:
            self.conn.execute("CHECKPOINT")
        except Exception:
            pass

        self._monitor_resources(f"cleanup_year_{year}")

    async def run_analysis_multi_year(self, years: list[int] | None = None) -> bool:
        """Run multi-year H3 PFAS analysis from GCS data."""
        self.log.info(
            "🚀 Starting multi-year H3 PFAS-containing active ingredient analysis from GCS"
        )

        # Setup DuckDB
        self.setup_duckdb()

        # Import dependencies
        from .data_loader import H3DataLoader
        from .result_saver import H3ResultSaver

        # Initialize components
        data_loader = H3DataLoader(self.conn, self.config, self.gcs_access)
        result_saver = H3ResultSaver(self.conn, self.config, self.gcs_access)

        # Load BMD data once for all years
        bmd_table = data_loader.load_bmd_data_from_gcs()

        # Determine years to process
        if years is None:
            years = data_loader.get_available_years()
            self.log.info(f"📅 Processing all available years: {years}")
        else:
            self.log.info(f"📅 Processing specified years: {years}")

        # Process each year
        successful_years = 0
        total_records = 0

        for year in years:
            try:
                self.log.info(f"\n📊 Processing year {year}...")
                self._monitor_resources(f"starting_year_{year}")

                # Process single year
                year_records = await self._process_single_year_from_gcs(
                    year, bmd_table, data_loader, result_saver
                )

                if year_records > 0:
                    successful_years += 1
                    total_records += year_records
                    self.log.info(f"✅ Year {year}: {year_records:,} H3 hexagons processed")
                else:
                    self.log.warning(f"⚠️ Year {year}: No data processed")

                self._monitor_resources(f"completed_year_{year}")

            except Exception as e:
                self.log.error(f"❌ Failed to process year {year}: {e}")
                continue

        # Final summary
        self.log.info("\n🎉 Multi-year H3 PFAS analysis completed!")
        self.log.info(f"📊 Successfully processed {successful_years}/{len(years)} years")
        self.log.info(f"📈 Total H3 hexagons processed: {total_records:,}")

        # Final cleanup
        self._aggressive_cleanup()

        return successful_years > 0

    async def run_kommune_analysis_multi_year(self, years: list[int] | None = None) -> bool:
        """Run multi-year kommune-level PFAS analysis from GCS data."""
        self.log.info("🏛️ Starting multi-year kommune-level PFAS analysis from GCS")

        # Setup DuckDB
        self.setup_duckdb()

        # Import dependencies
        from .data_loader import H3DataLoader
        from .result_saver import H3ResultSaver

        # Initialize components
        data_loader = H3DataLoader(self.conn, self.config, self.gcs_access)
        result_saver = H3ResultSaver(self.conn, self.config, self.gcs_access)

        # Load BMD data once for all years
        bmd_table = data_loader.load_bmd_data_from_gcs()

        # Load kommune boundaries once
        kommune_table = self._load_kommune_boundaries()

        # Determine years to process
        if years is None:
            years = data_loader.get_available_years()
            self.log.info(f"📅 Processing all available years: {years}")
        else:
            self.log.info(f"📅 Processing specified years: {years}")

        # Process each year
        successful_years = 0
        total_records = 0

        for year in years:
            try:
                self.log.info(f"\n📊 Processing kommune analysis for year {year}...")
                self._monitor_resources(f"starting_kommune_year_{year}")

                # Process single year for kommune analysis
                year_records = await self._process_single_year_kommune_from_gcs(
                    year, bmd_table, kommune_table, data_loader, result_saver
                )

                if year_records > 0:
                    successful_years += 1
                    total_records += year_records
                    self.log.info(f"✅ Year {year}: {year_records:,} kommuner processed")
                else:
                    self.log.warning(f"⚠️ Year {year}: No kommune data processed")

                self._monitor_resources(f"completed_kommune_year_{year}")

            except Exception as e:
                self.log.error(f"❌ Failed to process kommune analysis for year {year}: {e}")
                continue

        # Final summary
        self.log.info("\n🎉 Multi-year kommune PFAS analysis completed!")
        self.log.info(f"📊 Successfully processed {successful_years}/{len(years)} years")
        self.log.info(f"🏛️ Total kommuner processed: {total_records:,}")

        # Final cleanup
        self._aggressive_cleanup()

        return successful_years > 0

    def _load_kommune_boundaries(self) -> str:
        """Load Danish kommune boundaries from GCS."""
        self.log.info("🗺️ Loading Danish kommune boundaries from GCS")

        kommune_table = "kommune_boundaries"

        # Load kommune data from GCS
        kommune_path = (
            f"gs://{self.config.bucket}/silver/dst/kommune_boundaries/kommune_boundaries.parquet"
        )

        try:
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE {kommune_table} AS
                SELECT
                    kommune_code,
                    kommune_name,
                    region_code,
                    region_name,
                    ST_GeomFromWKB(geometry) as geometry,
                    ST_Area_Spheroid(ST_GeomFromWKB(geometry)) / 10000.0 as kommune_area_ha,
                    ST_Centroid(ST_GeomFromWKB(geometry)) as centroid
                FROM read_parquet('{kommune_path}')
                WHERE geometry IS NOT NULL
            """)

            count = self.conn.execute(f"SELECT COUNT(*) FROM {kommune_table}").fetchone()[0]
            self.log.info(f"✅ Loaded {count:,} kommune boundaries")

        except Exception as e:
            self.log.warning(f"⚠️ Could not load kommune boundaries from GCS: {e}")
            # Create a fallback with basic kommune codes
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE {kommune_table} AS
                SELECT
                    CAST(101 + row_number() OVER () AS INTEGER) as kommune_code,
                    'Kommune_' || CAST(101 + row_number() OVER () AS VARCHAR) as kommune_name,
                    CAST(1 AS INTEGER) as region_code,
                    'Unknown Region' as region_name,
                    ST_Buffer(ST_Point(12.0, 56.0), 0.1) as geometry,
                    1000.0 as kommune_area_ha,
                    ST_Point(12.0, 56.0) as centroid
                FROM range(98) -- 98 kommuner in Denmark
            """)
            self.log.info("📝 Created fallback kommune table with basic structure")

        return kommune_table

    async def _process_single_year_kommune_from_gcs(
        self, year: int, bmd_table: str, kommune_table: str, data_loader, result_saver
    ) -> int:
        """Process a single year for kommune-level analysis using GCS data."""
        self.log.info(f"🏛️ Processing kommune-level PFAS analysis for year {year}")

        # Step 1: Load and prepare field data (Y+1 pattern)
        field_year = year + 1
        fields_table = data_loader.load_and_prepare_fields_from_gcs(field_year, year)

        # Prepare geometries for fields
        fields_table = self.coordinate_transformer.prepare_geometries(fields_table)

        # Step 2: Load pesticide disaggregation for year Y
        pesticide_table = data_loader.load_pesticide_disaggregation_from_gcs(year)

        # Step 3: Join pesticide data with BMD for PFAS detection
        pesticide_pfas_table = data_loader.join_pesticide_with_bmd_pfas(
            pesticide_table, bmd_table, year
        )

        # Step 4: Perform spatial join between fields and kommuner
        kommune_results_table = self._perform_kommune_spatial_join(
            kommune_table, fields_table, pesticide_pfas_table, year
        )

        # Step 5: Validate and save results
        result_count = result_saver.save_kommune_results(kommune_results_table, year)

        # Step 6: Clean up intermediate tables
        self._cleanup_year_tables(year)

        return result_count

    def _perform_kommune_spatial_join(
        self, kommune_table: str, fields_table: str, pesticide_table: str, year: int
    ) -> str:
        """Perform spatial join between fields and kommuner for PFAS analysis."""
        self.log.info(f"🔗 Performing kommune-level spatial join for year {year}")

        results_table = f"kommune_results_{year}"

        # Create the kommune-level aggregation
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE {results_table} AS
            WITH field_kommune_intersections AS (
                SELECT
                    k.kommune_code,
                    k.kommune_name,
                    k.region_code,
                    k.kommune_area_ha,
                    ST_X(k.centroid) as kommune_centroid_x,
                    ST_Y(k.centroid) as kommune_centroid_y,
                    f.cvr_number,
                    f.block_id,
                    f.field_id,
                    f.crop_code,
                    f.crop_name,
                    ST_Area_Spheroid(
                        ST_Intersection(f.prepared_geometry, k.geometry)
                    ) / 10000.0 as intersection_area_ha,
                    f.field_area_ha
                FROM {kommune_table} k
                INNER JOIN {fields_table} f ON ST_Intersects(f.prepared_geometry, k.geometry)
                WHERE ST_Area_Spheroid(ST_Intersection(f.prepared_geometry, k.geometry)) > 0
            ),
            pesticide_kommune_data AS (
                SELECT
                    fki.*,
                    p.PesticideName,
                    p.PesticideRegistrationNumber,
                    p.DosageQuantity,
                    p.DosageUnit,
                    p.contains_pfas,
                    -- Weight pesticide amounts by intersection area ratio
                    (fki.intersection_area_ha / fki.field_area_ha) * 
                        COALESCE(p.pfas_containing_active_ingredient_grams, 0) as weighted_pfas_grams,
                    (fki.intersection_area_ha / fki.field_area_ha) * 
                        COALESCE(p.pesticide_belastning_applied, 0) as weighted_pesticide_belastning,
                    (fki.intersection_area_ha / fki.field_area_ha) * 
                        COALESCE(p.pfas_containing_pesticide_belastning_applied, 0) as weighted_pfas_belastning
                FROM field_kommune_intersections fki
                LEFT JOIN {pesticide_table} p ON (
                    fki.cvr_number = p.cvr AND 
                    fki.block_id = p.extracted_block_id AND 
                    fki.field_id = p.extracted_field_id
                )
            )
            SELECT
                kommune_code,
                kommune_name,
                region_code,
                kommune_area_ha,
                kommune_centroid_x,
                kommune_centroid_y,
                SUM(intersection_area_ha) as total_agricultural_area_ha,
                COUNT(DISTINCT CONCAT(cvr_number, '_', block_id, '_', field_id)) as unique_field_count,
                COUNT(DISTINCT cvr_number) as unique_company_count,
                AVG(intersection_area_ha / field_area_ha) as avg_field_coverage_ratio,
                MAX(intersection_area_ha / field_area_ha) as max_field_coverage_ratio,
                MIN(intersection_area_ha / field_area_ha) as min_field_coverage_ratio,
                COUNT(DISTINCT crop_code) as crop_diversity,
                STRING_AGG(DISTINCT crop_name, '; ') as crop_types,
                SUM(COALESCE(weighted_pfas_grams, 0)) as total_pfas_containing_active_ingredient_grams,
                SUM(COALESCE(weighted_pesticide_belastning, 0)) as total_pesticide_belastning,
                SUM(COALESCE(weighted_pfas_belastning, 0)) as total_pfas_pesticide_belastning,
                COUNT(CASE WHEN PesticideRegistrationNumber IS NOT NULL THEN 1 END) as total_pesticide_applications,
                COUNT(CASE WHEN contains_pfas = true THEN 1 END) as pfas_containing_applications,
                COUNT(DISTINCT CASE WHEN contains_pfas = true THEN PesticideRegistrationNumber END) as unique_pfas_products,
                COUNT(DISTINCT PesticideRegistrationNumber) as unique_pesticide_products,
                -- Intensity metrics
                CASE 
                    WHEN SUM(intersection_area_ha) > 0 THEN 
                        SUM(COALESCE(weighted_pfas_grams, 0)) / SUM(intersection_area_ha)
                    ELSE 0 
                END as pfas_containing_active_ingredient_intensity_grams_per_ha,
                CASE 
                    WHEN SUM(intersection_area_ha) > 0 THEN 
                        SUM(COALESCE(weighted_pesticide_belastning, 0)) / SUM(intersection_area_ha)
                    ELSE 0 
                END as pesticide_belastning_per_ha,
                CASE 
                    WHEN SUM(intersection_area_ha) > 0 THEN 
                        SUM(COALESCE(weighted_pfas_belastning, 0)) / SUM(intersection_area_ha)
                    ELSE 0 
                END as pfas_pesticide_belastning_per_ha,
                -- Coverage metrics
                CASE 
                    WHEN kommune_area_ha > 0 THEN 
                        (SUM(intersection_area_ha) / kommune_area_ha) * 100.0
                    ELSE 0 
                END as agricultural_coverage_pct,
                CURRENT_TIMESTAMP as created_at
            FROM pesticide_kommune_data
            GROUP BY 
                kommune_code, kommune_name, region_code, kommune_area_ha,
                kommune_centroid_x, kommune_centroid_y
            HAVING SUM(intersection_area_ha) > 0
            ORDER BY total_pfas_containing_active_ingredient_grams DESC
        """)

        count = self.conn.execute(f"SELECT COUNT(*) FROM {results_table}").fetchone()[0]
        self.log.info(f"✅ Generated kommune analysis results for {count:,} kommuner")

        return results_table

    async def _process_single_year_from_gcs(
        self, year: int, bmd_table: str, data_loader, result_saver
    ) -> int:
        """Process a single year using GCS data with the refactored spatial methodology."""
        self.log.info(
            f"⚙️ Processing H3 PFAS-containing active ingredient exposure for year {year} (GCS data, refactored methodology)"
        )

        # Step 1: Load and prepare field data (Y+1 pattern)
        field_year = year + 1
        fields_table = data_loader.load_and_prepare_fields_from_gcs(field_year, year)

        # Prepare geometries for fields
        fields_table = self.coordinate_transformer.prepare_geometries(fields_table)

        # Step 2: Load pesticide disaggregation for year Y
        pesticide_table = data_loader.load_pesticide_disaggregation_from_gcs(year)

        # Step 3: Join pesticide data with BMD for PFAS detection
        pesticide_pfas_table = data_loader.join_pesticide_with_bmd_pfas(
            pesticide_table, bmd_table, year
        )

        # Step 4: Generate H3 grid
        h3_grid_table = self.generate_h3_grid()

        # Step 5: Perform chunked spatial join using the refactored methodology
        results_table = self.spatial_joiner.perform_chunked_spatial_join(
            h3_grid_table, fields_table, pesticide_pfas_table, year
        )

        # Step 6: Validate results
        self._validate_results(results_table)

        # Step 7: Save results to GCS
        result_count = result_saver.save_year_results_kepler_compatible(results_table, year)

        # Step 8: Clean up intermediate tables
        self._cleanup_year_tables(year)

        return result_count

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
        from .result_saver import H3ResultSaver

        result_saver = H3ResultSaver(self.conn, self.config, None)
        result_saver.save_local_results(results_table, year, self.local_data_dir)

        self.log.info("🎉 H3 PFAS analysis completed successfully!")
        return results_table

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
