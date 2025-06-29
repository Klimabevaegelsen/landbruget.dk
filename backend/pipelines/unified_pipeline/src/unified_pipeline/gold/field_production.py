"""
Field Production Gold Layer

This module implements the gold layer processor for field production estimates.
It combines agricultural fields data with DST (Danish Statistics) yield data to create
comprehensive production estimates for analytics and downstream consumption.

Migrated from pandas/geopandas to pure DuckDB approach for optimal performance.
"""

import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import ConfigDict

from unified_pipeline.common.base import BaseJobConfig, BaseSource, GoldJobInterface
from unified_pipeline.util.gcs_access import GCSDataAccess
from unified_pipeline.util.gcs_util import GCSUtil
from unified_pipeline.util.log_util import Logger

# Import the DST mapping table from the DST pipeline
sys.path.append(str(Path(__file__).parent.parent.parent.parent.parent / "dst_pipeline"))


class FieldProductionGoldConfig(BaseJobConfig):
    """Configuration for Field Production gold layer."""

    name: str = "Field Production Gold"
    dataset: str = "field_production"
    type: str = "gold"
    description: str = "Comprehensive field production estimates using DST yield data"
    frequency: str = "weekly"
    bucket: str = os.getenv("GCS_BUCKET")

    # Input silver datasets
    agricultural_fields_dataset: str = "fvm_marker"
    dst_zone_mapping_dataset: str = "dst_zone_mapping"
    dst_yield_datasets: List[str] = [
        "hst77_processed",
        "gartn1_processed",
        "fro_processed",
        "halm1_processed",
    ]

    # Processing configuration
    batch_size: int = 5000  # Optimized for SPATIAL_JOIN performance
    max_year_lag: int = 3  # Maximum years between field and DST data

    # Quality thresholds
    min_yield_coverage: float = 0.3  # Minimum acceptable yield coverage rate

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class FieldProductionGold(BaseSource[FieldProductionGoldConfig], GoldJobInterface):
    """
    Gold layer processor for field production estimates using pure DuckDB.

    Combines agricultural fields and DST yield data to create
    comprehensive production estimates for analytics and downstream consumption.
    """

    def __init__(self, config: FieldProductionGoldConfig, gcs_util: GCSUtil):
        super().__init__(config, gcs_util)
        self.log = Logger.get_logger()

        # Initialize optimized GCS access
        self.gcs_access = GCSDataAccess()

        # Use the optimized DuckDB connection from GCS access
        self.conn = self.gcs_access.duckdb_conn
        self._configure_duckdb()

    def _configure_duckdb(self):
        """Configure DuckDB for optimal spatial operations."""
        self.conn.execute("SET memory_limit = '12GB'")  # Use 75% of available 16GB RAM
        self.conn.execute("SET threads = 4")  # Use all available CPU cores
        self.conn.execute("SET enable_progress_bar = true")
        self.conn.execute("SET preserve_insertion_order = false")

        # Spatial extensions already loaded by GCSDataAccess
        # Verify SPATIAL_JOIN operator availability
        try:
            version_result = self.conn.execute(
                "SELECT extension_name, extension_version FROM duckdb_extensions() WHERE extension_name = 'spatial'"
            ).fetchone()
            if version_result:
                self.log.info(f"DuckDB Spatial version: {version_result[1]}")
                if version_result[1] >= "1.2.2":
                    self.log.info("✅ SPATIAL_JOIN operator available")
                else:
                    self.log.warning(
                        f"⚠️  SPATIAL_JOIN operator may not be available in version {version_result[1]}"
                    )
        except Exception as e:
            self.log.warning(f"Could not verify spatial extension version: {e}")

    def _load_agricultural_fields_for_years(
        self, years: List[int], silver_data: Optional[Dict[str, Any]]
    ) -> str:
        """DEPRECATED: Use _process_single_year_with_dst_yields instead for memory efficiency."""
        raise NotImplementedError("This method has been replaced by year-by-year processing")

    def _get_latest_silver_path(self, dataset: str) -> str:
        """Override base method to handle both data.parquet and {dataset}.parquet naming patterns."""
        try:
            # Try the new pattern first: {dataset}.parquet
            pattern = f"gs://{self.config.bucket}/silver/{dataset}/*/{dataset}.parquet"
            files = self.gcs_access.list_files(pattern)
            if files:
                return sorted(files)[-1]  # Latest by timestamp

            # Fall back to old pattern: data.parquet
            pattern = f"gs://{self.config.bucket}/silver/{dataset}/*/data.parquet"
            files = self.gcs_access.list_files(pattern)
            if files:
                return sorted(files)[-1]  # Latest by timestamp

            raise FileNotFoundError(f"No silver data found for {dataset}")
        except Exception as e:
            raise FileNotFoundError(f"No silver data found for {dataset}: {e}")

    def _load_silver_data_to_table(
        self, dataset: str, table_name: str, silver_data: Optional[Dict[str, Any]]
    ) -> bool:
        """Load silver data into DuckDB table."""
        if silver_data and dataset in silver_data:
            self.log.info(f"Using in-memory silver data for {dataset}")
            self.conn.register(table_name, silver_data[dataset])
            return True

        # Load from GCS using direct download and load into our connection
        try:
            gcs_path = self._get_latest_silver_path(dataset)

            # Download to temp file and load into our connection
            with self.gcs_access._temp_download(gcs_path) as temp_file:
                self.conn.execute(f"""
                    CREATE OR REPLACE TABLE {table_name} AS
                    SELECT * FROM read_parquet('{temp_file}')
                """)

            count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            self.log.info(f"Loaded {dataset} from GCS into table {table_name} ({count:,} rows)")
            return True
        except FileNotFoundError:
            self.log.error(f"No silver data found for {dataset}")
            return False
        except Exception as e:
            self.log.error(f"Failed to load {dataset}: {e}")
            return False

    async def run(self, silver_data: Optional[Dict[str, Any]] = None) -> None:
        """Run field production estimation gold processing using pure DuckDB."""

        self.log.info("Starting field production gold layer processing with DuckDB")

        # Get all available years
        available_years = self._get_available_fvm_marker_years()
        if not available_years:
            self.log.error("No fvm_marker years found")
            return

        self.log.info(
            f"Found fvm_marker data for years: {available_years} ({len(available_years)} years)"
        )

        # Load DST zone mapping into DuckDB table (small dataset, can stay in memory)
        if not self._load_silver_data_to_table(
            self.config.dst_zone_mapping_dataset, "dst_zones_raw", silver_data
        ):
            self.log.error("DST zone mapping is required for production estimation")
            return

        # Setup spatial processing with DST zones
        self._setup_spatial_processing_with_dst_zones()

        # Load DST yield data into DuckDB tables (relatively small, can stay in memory)
        dst_tables_loaded = self._load_dst_yield_data(silver_data)
        if not dst_tables_loaded:
            self.log.warning("No DST yield data available - production estimates will be limited")

        # Create final results table
        self.conn.execute("DROP TABLE IF EXISTS final_production_estimates")
        self.conn.execute("""
            CREATE TABLE final_production_estimates AS
            SELECT * FROM (VALUES 
                ('dummy', 'dummy', 'dummy', 0, 0.0, 'dummy', false, 'dummy', 'dummy', 'dummy', 
                 0.0, 'dummy', 0.0, 'dummy', 'dummy', current_timestamp)
            ) AS t(field_id, block_id, cvr_number, year, area_ha, crop_type, organic_farming, 
                   landsdel_code, landsdel_name, dst_regions, yield_estimate_hkg_ha, 
                   yield_estimation_method, production_estimate_hkg, production_unit, geometry_wkt, created_at)
            WHERE false
        """)

        # Process each year individually to control memory usage
        total_fields_processed = 0
        for year in available_years:
            self.log.info(f"Processing year {year}...")

            try:
                # Load and process single year
                year_count = self._process_single_year_with_dst_yields(year, silver_data)
                if year_count > 0:
                    total_fields_processed += year_count
                    self.log.info(f"✅ Completed year {year}: {year_count:,} fields processed")
                else:
                    self.log.warning(f"No fields processed for year {year}")

                # Memory cleanup after each year
                self.conn.execute("DROP TABLE IF EXISTS current_year_fields")
                self.conn.execute("DROP TABLE IF EXISTS year_fields_with_zones")
                self.conn.execute("DROP TABLE IF EXISTS year_production_estimates")

            except Exception as e:
                self.log.error(f"Error processing year {year}: {e}")
                continue

        if total_fields_processed == 0:
            self.log.error("No fields were processed across all years")
            return

        self.log.info(
            f"Processed {total_fields_processed:,} fields across {len(available_years)} years"
        )

        # Generate summary statistics using DuckDB
        self._generate_summary_statistics()

        # Save to gold layer using optimized export
        self._save_results_to_gold()

        self.log.info("Field production gold layer processing completed")

    def _process_single_year_with_dst_yields(
        self, year: int, silver_data: Optional[Dict[str, Any]]
    ) -> int:
        """Process a single year of fields with DST yields to control memory usage."""
        try:
            dataset_name = f"fvm_marker_{year}"

            # Load single year of agricultural fields
            if silver_data and dataset_name in silver_data:
                self.log.info(f"Using in-memory data for {dataset_name}")
                # Register the  and create table
                self.conn.register(f"temp_{dataset_name}", silver_data[dataset_name])
                # Check if block_id column exists (added after 2007, available from 2008)
                if year >= 2008:
                    self.conn.execute(f"""
                        CREATE OR REPLACE TABLE current_year_fields AS
                        SELECT 
                            field_id,
                            block_id,
                            cvr_number,
                            area_ha,
                            crop_type,
                            organic_farming,
                            {year} as year,
                            ST_GeomFromWKB(geometry) as geometry
                        FROM temp_{dataset_name}
                        WHERE geometry IS NOT NULL
                    """)
                else:
                    # For years before 2008, block_id doesn't exist
                    self.conn.execute(f"""
                        CREATE OR REPLACE TABLE current_year_fields AS
                        SELECT 
                            field_id,
                            NULL as block_id,
                            cvr_number,
                            area_ha,
                            crop_type,
                            organic_farming,
                            {year} as year,
                            ST_GeomFromWKB(geometry) as geometry
                        FROM temp_{dataset_name}
                        WHERE geometry IS NOT NULL
                    """)
            else:
                # Load from GCS using direct download and load into our connection
                try:
                    gcs_path = self._get_latest_silver_path(dataset_name)

                    # Download to temp file and load into our connection
                    with self.gcs_access._temp_download(gcs_path) as temp_file:
                        # Check if block_id column exists (added after 2007, available from 2008)
                        if year >= 2008:
                            self.conn.execute(f"""
                                CREATE OR REPLACE TABLE current_year_fields AS
                                SELECT 
                                    field_id,
                                    block_id,
                                    cvr_number,
                                    area_ha,
                                    crop_type,
                                    organic_farming,
                                    {year} as year,
                                    ST_GeomFromWKB(geometry) as geometry
                                FROM read_parquet('{temp_file}')
                                WHERE geometry IS NOT NULL
                            """)
                        else:
                            # For years before 2008, block_id doesn't exist
                            self.conn.execute(f"""
                                CREATE OR REPLACE TABLE current_year_fields AS
                                SELECT 
                                    field_id,
                                    NULL as block_id,
                                    cvr_number,
                                    area_ha,
                                    crop_type,
                                    organic_farming,
                                    {year} as year,
                                    ST_GeomFromWKB(geometry) as geometry
                                FROM read_parquet('{temp_file}')
                                WHERE geometry IS NOT NULL
                            """)

                    # Geometry filtering is already done in the CREATE TABLE query above

                except Exception as e:
                    self.log.warning(f"Could not load {dataset_name} from GCS: {e}")
                    return 0

            # Check if any data was loaded
            year_count = self.conn.execute("SELECT COUNT(*) FROM current_year_fields").fetchone()[0]
            if year_count == 0:
                self.log.warning(f"No data loaded for {dataset_name}")
                return 0

            self.log.info(f"  Loaded {year_count:,} fields for year {year}")

            # Spatial join with DST zones using SPATIAL_JOIN operator
            self.conn.execute("""
                CREATE OR REPLACE TABLE year_fields_with_zones AS
                SELECT 
                    f.field_id,
                    f.block_id,
                    f.cvr_number,
                    f.area_ha,
                    f.crop_type,
                    f.organic_farming,
                    f.year,
                    z.landsdel_code,
                    z.landsdel_name,
                    z.dst_regions,
                    ST_AsText(f.geometry) as geometry_wkt
                FROM current_year_fields f
                LEFT JOIN dst_zones z ON ST_Within(f.geometry, z.geometry)
            """)

            # Create production estimates for this year
            self.conn.execute("""
                CREATE OR REPLACE TABLE year_production_estimates AS
                WITH yield_data AS (
                    SELECT 
                        f.*,
                        CASE 
                            WHEN f.dst_regions IS NOT NULL THEN 
                                COALESCE(
                                    -- Try exact DST region match first
                                    (SELECT d.value FROM dst_hst77_processed d 
                                     WHERE d.region = f.dst_regions AND d.year = f.year 
                                     AND d.measurement_unit ILIKE '%yield%' LIMIT 1),
                                    (SELECT d.value FROM dst_gartn1_processed d 
                                     WHERE d.region = f.dst_regions AND d.year = f.year 
                                     AND d.measurement_unit ILIKE '%yield%' LIMIT 1),
                                    (SELECT d.value FROM dst_fro_processed d 
                                     WHERE d.region = f.dst_regions AND d.year = f.year 
                                     AND d.measurement_unit ILIKE '%yield%' LIMIT 1),
                                    (SELECT d.value FROM dst_halm1_processed d 
                                     WHERE d.region = f.dst_regions AND d.year = f.year 
                                     AND d.measurement_unit ILIKE '%yield%' LIMIT 1),
                                    -- Fallback to national average
                                    (SELECT d.value FROM dst_hst77_processed d 
                                     WHERE d.region ILIKE '%Hele landet%' AND d.year = f.year 
                                     AND d.measurement_unit ILIKE '%yield%' LIMIT 1)
                                )
                            ELSE NULL
                        END as yield_estimate_hkg_ha
                    FROM year_fields_with_zones f
                )
                SELECT 
                    -- JOIN KEYS
                    field_id,
                    block_id,
                    cvr_number,
                    year,
                    -- FIELD DATA
                    area_ha,
                    crop_type,
                    organic_farming,
                    -- DST ZONE INFO
                    landsdel_code,
                    landsdel_name,
                    dst_regions,
                    -- YIELD DATA
                    yield_estimate_hkg_ha,
                    CASE 
                        WHEN yield_estimate_hkg_ha IS NOT NULL THEN 'dst_region_match'
                        ELSE 'no_yield_data'
                    END as yield_estimation_method,
                    -- PRODUCTION ESTIMATE
                    CASE 
                        WHEN yield_estimate_hkg_ha IS NOT NULL THEN area_ha * yield_estimate_hkg_ha
                        ELSE NULL
                    END as production_estimate_hkg,
                    CASE 
                        WHEN yield_estimate_hkg_ha IS NOT NULL THEN 'hkg'
                        ELSE NULL
                    END as production_unit,
                    -- SPATIAL INFO
                    geometry_wkt,
                    -- METADATA
                    current_timestamp as created_at
                FROM yield_data
            """)

            # Insert year results into final table
            self.conn.execute("""
                INSERT INTO final_production_estimates
                SELECT * FROM year_production_estimates
            """)

            # Get count of processed fields for this year
            processed_count = self.conn.execute(
                "SELECT COUNT(*) FROM year_production_estimates"
            ).fetchone()[0]

            return processed_count

        except Exception as e:
            self.log.error(f"Error processing year {year}: {e}")
            return 0

    def _process_all_fields_with_dst_yields(self, fields_table: str, years: List[int]) -> None:
        """DEPRECATED: Use _process_single_year_with_dst_yields instead for memory efficiency."""
        raise NotImplementedError("This method has been replaced by year-by-year processing")

    def _setup_spatial_processing_with_dst_zones(self) -> None:
        """Setup spatial processing with DST zones in DuckDB."""
        try:
            self.log.info("Setting up spatial processing with DST zones")

            # Debug: Check the structure and sample data of dst_zones_raw table
            try:
                columns = self.conn.execute("DESCRIBE dst_zones_raw").fetchall()
                self.log.info(f"dst_zones_raw table structure: {columns}")

                sample_data = self.conn.execute("SELECT * FROM dst_zones_raw LIMIT 3").fetchall()
                self.log.info(f"Sample dst_zones_raw data: {sample_data}")

                # Check for NULL geometries
                null_count = self.conn.execute(
                    "SELECT COUNT(*) FROM dst_zones_raw WHERE geometry IS NULL"
                ).fetchone()[0]
                empty_count = self.conn.execute(
                    "SELECT COUNT(*) FROM dst_zones_raw WHERE geometry = ''"
                ).fetchone()[0]
                total_count = self.conn.execute("SELECT COUNT(*) FROM dst_zones_raw").fetchone()[0]
                self.log.info(
                    f"Geometry data: {total_count} total, {null_count} NULL, {empty_count} empty"
                )

            except Exception as debug_e:
                self.log.warning(f"Debug info failed: {debug_e}")

                # Create optimized DST zones table with spatial geometry
            # Convert WKT geometry strings to GEOMETRY type using ST_GeomFromText for spatial indexing
            self.conn.execute("""
                CREATE OR REPLACE TABLE dst_zones AS
                SELECT 
                    landsdel_code,
                    landsdel_name,
                    dst_regions,
                    ST_GeomFromText(geometry) as geometry
                FROM dst_zones_raw
                WHERE geometry IS NOT NULL 
                AND geometry != ''
                AND geometry != 'NULL'
            """)

            # Verify we have valid zones
            zone_count = self.conn.execute("SELECT COUNT(*) FROM dst_zones").fetchone()[0]
            if zone_count == 0:
                self.log.error("No valid DST zones found after filtering")
                raise ValueError("No valid DST zones available for spatial processing")

            # Create spatial index
            self.conn.execute("CREATE INDEX idx_dst_zones_geom ON dst_zones USING RTREE (geometry)")

            self.log.info(
                f"✅ Created DST zones table with {zone_count} valid zones and spatial index"
            )

        except Exception as e:
            self.log.error(f"Failed to setup spatial processing with DST zones: {e}")
            raise

    def _load_dst_yield_data(self, silver_data: Optional[Dict[str, Any]] = None) -> List[str]:
        """Load DST yield data into DuckDB tables."""
        loaded_tables = []

        for dataset in self.config.dst_yield_datasets:
            table_name = f"dst_{dataset}"
            try:
                if self._load_silver_data_to_table(dataset, table_name, silver_data):
                    # Get record count
                    count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                    self.log.info(f"Loaded {count} records from {dataset} into {table_name}")
                    loaded_tables.append(table_name)
                else:
                    self.log.warning(f"No data found for {dataset}")
            except Exception as e:
                self.log.error(f"Error loading {dataset}: {e}")
                continue

        return loaded_tables

    def _generate_summary_statistics(self) -> None:
        """Generate summary statistics using DuckDB."""
        try:
            # Get summary statistics
            summary = self.conn.execute("""
                SELECT 
                    COUNT(*) as total_fields,
                    COUNT(DISTINCT year) as years_covered,
                    COUNT(DISTINCT crop_type) as crops_covered,
                    COUNT(yield_estimate_hkg_ha) as fields_with_yields,
                    COUNT(production_estimate_hkg) as fields_with_production,
                    MIN(year) as min_year,
                    MAX(year) as max_year
                FROM final_production_estimates
            """).fetchone()

            (
                total_fields,
                years_covered,
                crops_covered,
                fields_with_yields,
                fields_with_production,
                min_year,
                max_year,
            ) = summary

            yield_coverage = fields_with_yields / total_fields if total_fields > 0 else 0
            production_coverage = fields_with_production / total_fields if total_fields > 0 else 0

            self.log.info("Field production summary:")
            self.log.info(f"  Total production estimates: {total_fields:,}")
            self.log.info(f"  Years covered: {years_covered} years ({min_year}-{max_year})")
            self.log.info(f"  Unique crop types: {crops_covered}")
            self.log.info(
                f"  Fields with yield estimates: {fields_with_yields:,} ({yield_coverage:.1%})"
            )
            self.log.info(
                f"  Fields with production estimates: {fields_with_production:,} ({production_coverage:.1%})"
            )

            # Summary by year
            year_summary = self.conn.execute("""
                SELECT 
                    year,
                    COUNT(*) as year_count,
                    COUNT(production_estimate_hkg) as year_with_production,
                    COUNT(production_estimate_hkg) * 100.0 / COUNT(*) as year_coverage
                FROM final_production_estimates
                GROUP BY year
                ORDER BY year
            """).fetchall()

            for year, year_count, year_with_production, year_coverage in year_summary:
                self.log.info(
                    f"    Year {year}: {year_count:,} fields, {year_with_production:,} with production ({year_coverage:.1f}%)"
                )

            # Check quality thresholds
            if yield_coverage < self.config.min_yield_coverage:
                self.log.warning(
                    f"Yield coverage {yield_coverage:.1%} below minimum threshold {self.config.min_yield_coverage:.1%}"
                )

        except Exception as e:
            self.log.error(f"Error generating summary statistics: {e}")

    def _save_results_to_gold(self) -> None:
        """Save results to gold layer using optimized DuckDB export."""
        try:
            # Use optimized save method from base class
            output_path = (
                f"gs://{self.config.bucket}/gold/{self.config.dataset}/latest/data.parquet"
            )

            # Export directly from DuckDB table to GCS
            self.gcs_access.upload_from_duckdb_table(
                self.conn,
                "final_production_estimates",
                output_path,
                compression="zstd",
                row_group_size=100000,
            )

            self.log.info(f"Saved field production estimates to {output_path}")

        except Exception as e:
            self.log.error(f"Error saving results: {e}")
            raise
