"""
H3 PFAS Processor - The Heart of the Analysis Pipeline

=== WHAT THIS FILE DOES (FOR NON-TECHNICAL READERS) ===

This file contains the main "processor" class that orchestrates the entire PFAS analysis.
Think of it as the conductor of an orchestra - it coordinates all the different parts
of the analysis to work together.

The processor:
1. Sets up the database connections and tools needed for analysis
2. Loads data from various sources (pesticide records, field boundaries, etc.)
3. Creates the hexagonal grid over Denmark
4. Performs the spatial analysis to calculate PFAS exposure
5. Saves the results for visualization

=== KEY CONCEPTS ===

- H3 Hexagons: Equal-sized hexagonal cells that cover Denmark for consistent mapping
- Spatial Join: Finding which farm fields overlap with each hexagon
- PFAS Detection: Identifying which pesticides contain PFAS chemicals
- Exposure Calculation: Computing how much PFAS is applied per hectare
- Chunked Processing: Breaking large datasets into smaller pieces for efficient processing

=== TECHNICAL DETAILS ===

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
    """
    The main processor class that orchestrates PFAS exposure analysis.

    This class is like the "brain" of the operation. It:
    - Manages database connections and memory usage
    - Coordinates all the different analysis steps
    - Handles data loading from Google Cloud Storage
    - Performs the spatial analysis calculations
    - Saves results in formats suitable for mapping

    The "Refactored" name indicates this is an improved version that's more
    efficient and better organized than previous versions.
    """

    def __init__(self, config: H3SpatialConfig, local_data_dir: Path | None = None):
        """
        Initialize the processor with configuration settings.

        Args:
            config: Settings that control how the analysis runs (memory limits,
                   resolution levels, processing parameters, etc.)
            local_data_dir: Optional path to local data files (mainly for testing)
        """
        self.config = config
        self.local_data_dir = local_data_dir
        self.log = logger.bind(processor="H3PFASRefactored")
        self.conn = None  # Database connection (set up later)

        # Initialize specialized components (like different tools in a toolbox)
        self.coordinate_transformer = None  # Handles geographic coordinate systems
        self.spatial_joiner = None  # Performs spatial overlap calculations
        self.area_validator = None  # Checks that area calculations are correct
        self.gcs_access = None  # Handles Google Cloud Storage access

        # CACHING SYSTEM - avoid recomputing expensive operations
        # Think of this like keeping frequently-used items on your desk
        # instead of going to the filing cabinet every time
        self._cached_bmd_table = None  # Pesticide registration data
        self._cached_h3_grids = {}  # Hexagonal grids for different resolutions
        self._cached_kommuner_table = None  # Municipality boundaries

        # Resource monitoring for cloud computing constraints
        # GitHub Actions has limited memory and disk space, so we track usage
        self._memory_alerts = 0
        self._disk_alerts = 0

        # Protected tables that shouldn't be deleted during cleanup
        # Like marking important documents as "do not shred"
        self._protected_tables = set()

    def _protect_table(self, table_name: str):
        """
        Mark a database table as protected from automatic cleanup.

        This is like putting a "do not delete" sticky note on important files.
        We protect tables that contain expensive-to-compute data that we might
        need again during the analysis.
        """
        self._protected_tables.add(table_name)

    def _unprotect_table(self, table_name: str):
        """Remove protection from a table, allowing it to be cleaned up."""
        self._protected_tables.discard(table_name)

    def _monitor_resources(self, operation: str):
        """
        Monitor memory and disk usage to prevent system overload.

        This is like checking your computer's performance monitor to make sure
        you're not using too much RAM or disk space. Cloud computing platforms
        like GitHub Actions have strict limits, so we need to be careful.

        Args:
            operation: Description of what we're currently doing (for logging)
        """
        if not self.config.github_actions_mode:
            return

        try:
            import psutil

            # Check how much memory our process is using
            process = psutil.Process()
            process_memory_gb = process.memory_info().rss / (1024**3)

            # Check how much disk space is available
            disk = psutil.disk_usage("/")
            available_disk_gb = disk.free / (1024**3)

            # Set conservative limits to avoid system crashes
            min_free_disk_gb = 2.0  # Keep at least 2GB free
            max_process_memory_gb = 12.0  # Limit our process to 12GB

            # Alert if we're using too much memory
            if process_memory_gb > max_process_memory_gb:
                self._memory_alerts += 1
                self.log.warning(
                    f"⚠️ {operation}: High process memory usage {process_memory_gb:.1f}GB (limit: {max_process_memory_gb}GB)"
                )

                # Force cleanup if we've had too many warnings
                if self._memory_alerts > 3 and not operation.startswith(("loading_", "loaded_")):
                    self.log.error(
                        f"❌ {operation}: Process memory usage too high, forcing selective cleanup"
                    )
                    self._selective_cleanup()

            # Alert if disk space is getting low
            if available_disk_gb < min_free_disk_gb:
                self._disk_alerts += 1
                self.log.warning(
                    f"⚠️ {operation}: Low available disk space {available_disk_gb:.1f}GB (minimum: {min_free_disk_gb}GB)"
                )

                # Force cleanup if we've had too many warnings
                if self._disk_alerts > 3 and not operation.startswith(("loading_", "loaded_")):
                    self.log.error(
                        f"❌ {operation}: Available disk space too low, forcing selective cleanup"
                    )
                    self._selective_cleanup()

            # Occasionally log resource usage for debugging
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
        """
        Clean up temporary database tables to free memory and disk space.

        This is like cleaning up your desk by throwing away scratch paper
        and temporary notes, but keeping the important documents.
        We only delete tables that are clearly temporary and safe to remove.
        """
        self.log.info("🧹 Performing selective cleanup for GitHub Actions constraints")

        # Drop only safe temporary tables, avoiding protected ones
        try:
            tables = self.conn.execute("SHOW TABLES").fetchall()
            for (table_name,) in tables:
                # Skip protected tables (the important ones)
                if table_name in self._protected_tables:
                    continue

                # Only clean up clearly temporary tables that are safe to drop
                if any(
                    keyword in table_name.lower()
                    for keyword in [
                        "chunk_",  # Processing chunks
                        "stage1_",  # Processing stages
                        "stage2_",
                        "stage3_",
                        "stage4_",
                        "stage5_",
                        "intermediate_",  # Intermediate results
                    ]
                ):
                    self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")

                # Also clean up old temporary tables
                if table_name.startswith("temp_") and not any(
                    protected in table_name for protected in self._protected_tables
                ):
                    # Check if it's clearly an old temporary table
                    try:
                        if any(
                            old_pattern in table_name
                            for old_pattern in ["_old", "_backup", "_legacy"]
                        ):
                            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                    except Exception:
                        pass
        except Exception:
            pass

        # Force Python's garbage collection to free memory
        gc.collect()

        # Clear DuckDB's internal cache
        try:
            self.conn.execute("CHECKPOINT")
        except Exception:
            pass

    def _aggressive_cleanup(self):
        """
        Aggressive cleanup for end-of-processing cleanup.

        This is like doing a thorough spring cleaning - we remove all
        temporary files and data that we no longer need. This should
        only be called at the very end of processing.
        """
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
        """
        Set up the DuckDB database connection and configure it for spatial analysis.

        DuckDB is like a powerful calculator that can work with geographic data.
        This method:
        1. Creates a connection to the database
        2. Installs extensions for spatial analysis (working with maps and coordinates)
        3. Configures memory and performance settings
        4. Sets up our specialized tools (coordinate transformer, spatial joiner, etc.)

        Think of this as setting up your workspace with all the tools you need
        before starting a complex project.
        """
        self.log.info("🔧 Setting up DuckDB with spatial extensions")

        # Create database connection with optimized settings
        self.conn = duckdb.connect(":memory:")  # Use in-memory database for speed

        # Configure DuckDB for our specific needs
        memory_limit = self.config.duckdb_memory_limit
        threads = self.config.duckdb_threads

        self.log.info(f"💾 DuckDB memory limit: {memory_limit}")
        self.log.info(f"🔄 DuckDB threads: {threads}")

        # Set DuckDB configuration for optimal performance
        self.conn.execute(f"SET memory_limit = '{memory_limit}'")
        self.conn.execute(f"SET threads = {threads}")

        # Install and load spatial extensions for geographic analysis
        self.conn.execute("INSTALL spatial")
        self.conn.execute("LOAD spatial")

        # Enable progress bars for long-running operations
        self.conn.execute("SET enable_progress_bar = true")

        # Optimize for analytical workloads
        self.conn.execute("SET default_order = 'ASC'")

        # Initialize our specialized tools

        # Import GCS access utility
        try:
            from backend.common.storage_interface import GCSDataAccess

            self.gcs_access = GCSDataAccess()
            self.log.info("✅ GCS access initialized successfully")
        except ImportError as e:
            self.log.error(f"❌ Failed to import GCSDataAccess: {e}")
            raise

        # Set up our specialized components
        self.coordinate_transformer = CoordinateTransformer(self.conn, self.config)
        self.spatial_joiner = SpatialJoiner(self.conn, self.config)
        self.area_validator = AreaValidator(self.conn, self.config)

        self.log.info("✅ DuckDB setup complete with spatial extensions")

    def generate_h3_grid(self) -> str:
        """
        Generate a hexagonal grid (H3) covering Denmark at the specified resolution.

        This creates the "map overlay" of hexagons that we'll use for analysis.
        Think of it like laying a honeycomb pattern over a map of Denmark.

        Each hexagon:
        - Has a unique identifier (H3 cell ID)
        - Covers a specific area (e.g., ~1.5 hectares at resolution 10)
        - Has a center point with latitude/longitude coordinates
        - Can be used to aggregate data from overlapping farm fields

        Returns:
            str: Name of the database table containing the H3 grid
        """
        resolution = self.config.h3_resolution

        # Check if we already have this grid cached
        if resolution in self._cached_h3_grids:
            self.log.info(f"🔷 Using cached H3 grid for resolution {resolution}")
            return self._cached_h3_grids[resolution]

        self.log.info(f"🔷 Generating H3 grid at resolution {resolution} for Denmark")

        # Get Denmark's geographic boundaries
        bounds = self.config.denmark_bounds
        min_lat, max_lat = bounds["min_lat"], bounds["max_lat"]
        min_lon, max_lon = bounds["min_lon"], bounds["max_lon"]

        self.log.info(
            f"📍 Denmark bounds: {min_lat:.2f}°N to {max_lat:.2f}°N, {min_lon:.2f}°E to {max_lon:.2f}°E"
        )

        # Create the H3 grid table
        grid_table = f"h3_grid_res{resolution}"

        # Generate H3 cells that cover Denmark
        # This uses the H3 spatial indexing system to create hexagonal cells
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE {grid_table} AS
            WITH denmark_bounds AS (
                SELECT ST_MakeEnvelope({min_lon}, {min_lat}, {max_lon}, {max_lat}) as bounds
            ),
            h3_cells AS (
                SELECT 
                    h3_cell_to_boundary_wkt(h3_cell) as h3_boundary_wkt,
                    h3_cell,
                    h3_cell_to_lat(h3_cell) as center_lat,
                    h3_cell_to_lng(h3_cell) as center_lon
                FROM (
                    SELECT h3_polygon_wkt_to_cells(ST_AsText(bounds), {resolution}) as h3_cell
                    FROM denmark_bounds
                ) cells
            )
            SELECT 
                h3_cell,
                center_lat,
                center_lon,
                ST_GeomFromText(h3_boundary_wkt) as geometry,
                -- Calculate area in hectares (1 hectare = 10,000 square meters)
                ST_Area(ST_Transform(ST_GeomFromText(h3_boundary_wkt), 3857)) / 10000.0 as h3_area_ha
            FROM h3_cells
            WHERE h3_cell IS NOT NULL
        """)

        # Get statistics about the generated grid
        stats = self.conn.execute(f"""
            SELECT 
                COUNT(*) as total_cells,
                MIN(h3_area_ha) as min_area_ha,
                MAX(h3_area_ha) as max_area_ha,
                AVG(h3_area_ha) as avg_area_ha
            FROM {grid_table}
        """).fetchone()

        total_cells, min_area, max_area, avg_area = stats

        self.log.info(f"✅ Generated H3 grid with {total_cells:,} cells")
        self.log.info(
            f"📏 Cell areas: {min_area:.3f} to {max_area:.3f} hectares (avg: {avg_area:.3f} ha)"
        )

        # Validate that the grid looks reasonable
        expected_area = self.config.theoretical_avg_area_ha
        if abs(avg_area - expected_area) > (expected_area * 0.1):  # Within 10%
            self.log.warning(
                f"⚠️ Average cell area {avg_area:.3f} ha differs from expected {expected_area:.3f} ha"
            )

        # Cache the grid for future use
        self._cached_h3_grids[resolution] = grid_table
        self._protect_table(grid_table)  # Don't delete this expensive-to-compute table

        return grid_table

    def _validate_results(self, results_table: str):
        """
        Validate that the analysis results look reasonable.

        This performs sanity checks on the results to catch obvious errors:
        - Are hexagon areas within expected ranges?
        - Are PFAS exposure values reasonable?
        - Are there any data quality issues?

        Args:
            results_table: Name of the database table containing results
        """
        self.log.info("🔍 Validating analysis results")

        # Use our area validator to check hexagon areas
        if self.area_validator:
            self.area_validator.validate_h3_areas(results_table)

        # Check for basic data quality issues
        stats = self.conn.execute(f"""
            SELECT 
                COUNT(*) as total_cells,
                COUNT(CASE WHEN total_pfas_containing_active_ingredient_grams > 0 THEN 1 END) as pfas_cells,
                MAX(total_pfas_containing_active_ingredient_grams) as max_pfas_grams,
                AVG(total_pfas_containing_active_ingredient_grams) as avg_pfas_grams
            FROM {results_table}
        """).fetchone()

        total_cells, pfas_cells, max_pfas, avg_pfas = stats

        self.log.info("📊 Results validation:")
        self.log.info(f"   Total cells: {total_cells:,}")
        self.log.info(
            f"   Cells with PFAS exposure: {pfas_cells:,} ({pfas_cells / total_cells * 100:.1f}%)"
        )
        self.log.info(f"   Max PFAS exposure: {max_pfas:.2f} grams")
        self.log.info(f"   Average PFAS exposure: {avg_pfas:.2f} grams")

        # Flag potential issues
        if pfas_cells == 0:
            self.log.warning("⚠️ No cells with PFAS exposure detected - check data quality")

        if max_pfas > 10000:  # Arbitrary threshold for very high exposure
            self.log.warning(f"⚠️ Very high PFAS exposure detected: {max_pfas:.2f} grams")

    def _cleanup_year_tables(self, year: int):
        """
        Clean up database tables specific to a year to free memory.

        After processing each year, we remove temporary tables to prevent
        memory usage from growing too large.

        Args:
            year: The year whose tables should be cleaned up
        """
        self.log.debug(f"🧹 Cleaning up tables for year {year}")

        # List of table patterns to clean up
        cleanup_patterns = [
            f"*_{year}",
            f"*{year}*",
            "temp_*",
            "intermediate_*",
        ]

        try:
            tables = self.conn.execute("SHOW TABLES").fetchall()
            for (table_name,) in tables:
                # Skip protected tables
                if table_name in self._protected_tables:
                    continue

                # Check if table matches cleanup patterns
                for pattern in cleanup_patterns:
                    if pattern.replace("*", "") in table_name:
                        try:
                            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                            self.log.debug(f"   Dropped table: {table_name}")
                        except Exception:
                            pass
                        break
        except Exception as e:
            self.log.debug(f"Error during table cleanup: {e}")

        # Force garbage collection
        gc.collect()

    async def run_analysis_multi_year(self, years: list[int] | None = None) -> bool:
        """
        Run PFAS exposure analysis for multiple years.

        This is the main orchestration method that:
        1. Sets up the database and tools
        2. Determines which years to analyze
        3. Loads shared data (pesticide registration info, etc.)
        4. Processes each year's data
        5. Saves results for visualization

        Args:
            years: List of years to analyze (e.g., [2022, 2023])
                  If None, analyzes all available years

        Returns:
            bool: True if analysis completed successfully, False otherwise
        """
        self.log.info("🚀 Starting multi-year H3 PFAS exposure analysis")

        try:
            # Set up database and tools
            self.setup_duckdb()

            # Import our data handling components
            from .data_loader import H3DataLoader
            from .result_saver import H3ResultSaver

            # Initialize data loader and result saver
            data_loader = H3DataLoader(self.conn, self.config, self.gcs_access)
            result_saver = H3ResultSaver(self.conn, self.config, self.gcs_access)

            # Determine which years to analyze
            if years is None:
                years = data_loader.get_available_years()
                self.log.info(f"📅 Auto-detected available years: {years}")
            else:
                self.log.info(f"📅 Analyzing specified years: {years}")

            if not years:
                self.log.error("❌ No years available for analysis")
                return False

            # Load shared data that doesn't change between years
            self.log.info("📊 Loading shared reference data")

            # Load pesticide registration data (which products contain PFAS)
            bmd_table = data_loader.load_bmd_data_from_gcs()
            self._protect_table(bmd_table)

            # Generate H3 grid for the specified resolution
            h3_grid_table = self.generate_h3_grid()

            # Process each year
            total_processed = 0
            for year in years:
                self.log.info(f"📅 Processing year {year}")

                try:
                    # Process this year's data
                    processed_cells = await self._process_single_year_from_gcs(
                        year, bmd_table, data_loader, result_saver
                    )

                    if processed_cells > 0:
                        total_processed += processed_cells
                        self.log.info(f"✅ Year {year}: Processed {processed_cells:,} H3 cells")
                    else:
                        self.log.warning(f"⚠️ Year {year}: No data processed")

                except Exception as e:
                    self.log.error(f"❌ Year {year}: Processing failed - {e}")
                    continue

            if total_processed > 0:
                self.log.info("✅ Multi-year analysis completed successfully")
                self.log.info(f"📊 Total H3 cells processed: {total_processed:,}")
                return True
            else:
                self.log.error("❌ No data was processed for any year")
                return False

        except Exception as e:
            self.log.error(f"❌ Multi-year analysis failed: {e}")
            import traceback

            self.log.error(f"Error details: {traceback.format_exc()}")
            return False

        finally:
            # Clean up resources
            self._aggressive_cleanup()

    async def _process_single_year_from_gcs(
        self, year: int, bmd_table: str, data_loader, result_saver
    ) -> int:
        """
        Process PFAS exposure data for a single year.

        This method handles the core analysis for one year:
        1. Loads field boundaries and pesticide application data
        2. Identifies PFAS-containing pesticides
        3. Performs spatial analysis to calculate exposure per hexagon
        4. Saves results to Google Cloud Storage

        Args:
            year: The year to process (e.g., 2022)
            bmd_table: Database table with pesticide registration data
            data_loader: Tool for loading data from cloud storage
            result_saver: Tool for saving results to cloud storage

        Returns:
            int: Number of H3 cells that were processed
        """
        self.log.info(f"🔄 Processing single year: {year}")

        # Monitor memory usage
        self._monitor_resources(f"processing_year_{year}")

        try:
            # Load field data (farm boundaries and crop information)
            # Note: Field data is typically from year+1 due to reporting cycles
            field_year = year + 1
            self.log.info(f"📍 Loading field data for year {field_year}")
            fields_table = data_loader.load_and_prepare_fields_from_gcs(field_year, year)

            # Transform coordinates to ensure proper spatial analysis
            fields_table = self.coordinate_transformer.prepare_geometries(fields_table)

            # Load pesticide application data
            self.log.info(f"🧪 Loading pesticide application data for year {year}")
            pesticide_table = data_loader.load_pesticide_disaggregation_from_gcs(year)

            # Join pesticide data with PFAS detection information
            pesticide_pfas_table = data_loader.join_pesticide_with_bmd_pfas(
                pesticide_table, bmd_table, year
            )

            # Get the H3 grid (should be cached from earlier)
            h3_grid_table = self.generate_h3_grid()

            # Perform the spatial analysis
            self.log.info(f"🗺️ Performing spatial analysis for year {year}")
            results_table = self.spatial_joiner.perform_chunked_spatial_join(
                h3_grid_table, fields_table, pesticide_pfas_table, year
            )

            # Validate results
            self._validate_results(results_table)

            # Save results to cloud storage
            self.log.info(f"💾 Saving results for year {year}")
            await result_saver.save_h3_results_to_gcs(results_table, year)

            # Get count of processed cells
            cell_count = self.conn.execute(f"SELECT COUNT(*) FROM {results_table}").fetchone()[0]

            # Clean up year-specific tables to free memory
            self._cleanup_year_tables(year)

            return cell_count

        except Exception as e:
            self.log.error(f"❌ Failed to process year {year}: {e}")
            raise
