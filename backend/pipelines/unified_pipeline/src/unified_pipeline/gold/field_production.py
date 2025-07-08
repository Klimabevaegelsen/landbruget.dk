"""
Field Production Gold Layer

This module implements the gold layer processor for field production estimates.
It combines agricultural fields data with DST (Danish Statistics) yield data to create
comprehensive production estimates for analytics and downstream consumption.

Migrated from pandas/geopandas to pure DuckDB approach for optimal performance.
"""

import os
from typing import Any, Dict, List, Optional

from pydantic import ConfigDict

from unified_pipeline.common.base import BaseJobConfig, BaseSource, GoldJobInterface
from unified_pipeline.util.log_util import Logger

# DST functionality is now integrated into the unified pipeline


class FieldProductionGoldConfig(BaseJobConfig):
    """Configuration for Field Production gold layer."""

    name: str = "Field Production Gold"
    dataset: str = "field_production"
    type: str = "gold"
    description: str = "Comprehensive field production estimates using DST yield data"
    frequency: str = "weekly"
    bucket: str = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")

    # Input silver datasets
    agricultural_fields_dataset: str = "fvm_marker"
    dst_zone_mapping_dataset: str = "dst_zone_mapping"
    dst_yield_datasets: List[str] = [
        "dst_hst77",
        "dst_gartn1",
        "dst_fro",
        "dst_halm1",
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

    def __init__(self, config: FieldProductionGoldConfig):
        super().__init__(config)
        self.log = Logger.get_logger()

        # Connection and spatial extension are already set up by BaseSource
        # Just verify spatial extension is working (it should be loaded by BaseSource)
        try:
            self.conn.execute("SELECT ST_Point(0, 0)")
            self.log.info("✅ Spatial extension is working (loaded by BaseSource)")
        except Exception as e:
            self.log.error(f"❌ Spatial extension not available from BaseSource: {e}")
            raise RuntimeError("Spatial extension is required but not available from BaseSource")

    def _load_agricultural_fields_for_years(
        self, years: List[int], silver_data: Optional[Dict[str, Any]]
    ) -> str:
        """DEPRECATED: Use _process_single_year_with_dst_yields instead for memory efficiency."""
        raise NotImplementedError("This method has been replaced by year-by-year processing")

    def _get_latest_silver_path(self, dataset: str) -> str:
        """Override base method to handle both data.parquet and {dataset}.parquet naming patterns."""
        try:
            all_files = []

            # Try the new pattern first: {dataset}.parquet
            pattern1 = f"gs://{self.config.bucket}/silver/{dataset}/*/{dataset}.parquet"
            files1 = self.gcs_access.list_files(pattern1)
            all_files.extend(files1)

            # Also try the old pattern: data.parquet
            pattern2 = f"gs://{self.config.bucket}/silver/{dataset}/*/data.parquet"
            files2 = self.gcs_access.list_files(pattern2)
            all_files.extend(files2)

            if not all_files:
                raise FileNotFoundError(f"No silver data found for {dataset}")

            # Return the latest file across all patterns by timestamp
            latest_file = sorted(all_files)[-1]
            self.log.info(f"Found latest silver data for {dataset}: {latest_file}")
            return latest_file

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
                # Also check for geometry column format (geometry_wkt vs geometry)
                columns_info = self.conn.execute(f"DESCRIBE temp_{dataset_name}").fetchall()
                column_names = [col[0] for col in columns_info]

                # Use standardized geometry column and check its type
                if "geometry" in column_names:
                    # Check geometry column type to handle both WKT and binary geometry
                    geom_type_query = f"SELECT typeof(geometry) FROM temp_{dataset_name} WHERE geometry IS NOT NULL LIMIT 1"
                    geom_type_result = self.conn.execute(geom_type_query).fetchone()

                    if geom_type_result and geom_type_result[0] == "BLOB":
                        # Binary geometry - spatial extension already loaded by GCSDataAccess
                        geometry_select = "geometry"
                        geometry_where = "geometry IS NOT NULL"
                        self.log.info(f"Using binary geometry format for {dataset_name}")
                    else:
                        # Text/WKT geometry
                        geometry_select = "geometry"
                        geometry_where = "geometry IS NOT NULL"
                        self.log.info(f"Using text geometry format for {dataset_name}")
                else:
                    self.log.warning(f"No geometry column found in {dataset_name}")
                    return 0

                # Determine crop/layer type column
                if "crop_type" in column_names:
                    crop_type_select = "crop_type"
                elif "layer_type" in column_names:
                    crop_type_select = "layer_type as crop_type"
                else:
                    crop_type_select = "'unknown' as crop_type"
                    self.log.warning(
                        f"No crop_type or layer_type column found in {dataset_name}, using 'unknown'"
                    )

                # Check for other optional columns
                field_id_select = "field_id" if "field_id" in column_names else "NULL as field_id"
                # Note: FVM marker data does NOT contain organic farming information
                # Organic farming data is in a separate fvm_organic_areas dataset
                organic_farming_select = "false as organic_farming"

                self.log.info(
                    f"Column mapping for {dataset_name}: field_id={field_id_select}, crop_type={crop_type_select}, organic_farming={organic_farming_select}"
                )

                if year >= 2008:
                    self.conn.execute(f"""
                        CREATE OR REPLACE TABLE current_year_fields AS
                        SELECT 
                            {field_id_select},
                            block_id,
                            cvr_number,
                            area_ha,
                            {crop_type_select},
                            {organic_farming_select},
                            {year} as year,
                            {geometry_select}
                        FROM temp_{dataset_name}
                        WHERE {geometry_where}
                    """)
                else:
                    # For years before 2008, block_id doesn't exist
                    self.conn.execute(f"""
                        CREATE OR REPLACE TABLE current_year_fields AS
                        SELECT 
                            {field_id_select},
                            NULL as block_id,
                            cvr_number,
                            area_ha,
                            {crop_type_select},
                            {organic_farming_select},
                            {year} as year,
                            {geometry_select}
                        FROM temp_{dataset_name}
                        WHERE {geometry_where}
                    """)
            else:
                # Load from GCS using direct download and load into our connection
                try:
                    gcs_path = self._get_latest_silver_path(dataset_name)

                    # Download to temp file and load into our connection
                    with self.gcs_access._temp_download(gcs_path) as temp_file:
                        # Check column structure first
                        columns_info = self.conn.execute(
                            f"DESCRIBE (SELECT * FROM read_parquet('{temp_file}') LIMIT 1)"
                        ).fetchall()
                        column_names = [col[0] for col in columns_info]

                        # Use standardized geometry column and check its type
                        if "geometry" in column_names:
                            # Check geometry column type to handle both WKT and binary geometry
                            geom_type_query = f"SELECT typeof(geometry) FROM read_parquet('{temp_file}') WHERE geometry IS NOT NULL LIMIT 1"
                            geom_type_result = self.conn.execute(geom_type_query).fetchone()

                            if geom_type_result and geom_type_result[0] == "BLOB":
                                # Binary geometry - spatial extension already loaded by GCSDataAccess
                                geometry_select = "geometry"
                                geometry_where = "geometry IS NOT NULL"
                                self.log.info(f"Using binary geometry format for {dataset_name}")
                            else:
                                # Text/WKT geometry
                                geometry_select = "geometry"
                                geometry_where = "geometry IS NOT NULL"
                                self.log.info(f"Using text geometry format for {dataset_name}")
                        else:
                            self.log.warning(f"No geometry column found in {dataset_name}")
                            return 0

                        # Determine crop/layer type column
                        if "crop_type" in column_names:
                            crop_type_select = "crop_type"
                        elif "layer_type" in column_names:
                            crop_type_select = "layer_type as crop_type"
                        else:
                            crop_type_select = "'unknown' as crop_type"
                            self.log.warning(
                                f"No crop_type or layer_type column found in {dataset_name}, using 'unknown'"
                            )

                        # Check for other optional columns
                        field_id_select = (
                            "field_id" if "field_id" in column_names else "NULL as field_id"
                        )
                        # Note: FVM marker data does NOT contain organic farming information
                        # Organic farming data is in a separate fvm_organic_areas dataset
                        organic_farming_select = "false as organic_farming"

                        self.log.info(
                            f"Column mapping for {dataset_name}: field_id={field_id_select}, crop_type={crop_type_select}, organic_farming={organic_farming_select}"
                        )

                        # Check if block_id column exists (added after 2007, available from 2008)
                        if year >= 2008:
                            self.conn.execute(f"""
                                CREATE OR REPLACE TABLE current_year_fields AS
                                SELECT 
                                    {field_id_select},
                                    block_id,
                                    cvr_number,
                                    area_ha,
                                    {crop_type_select},
                                    {organic_farming_select},
                                    {year} as year,
                                    {geometry_select}
                                FROM read_parquet('{temp_file}')
                                WHERE {geometry_where}
                            """)
                        else:
                            # For years before 2008, block_id doesn't exist
                            self.conn.execute(f"""
                                CREATE OR REPLACE TABLE current_year_fields AS
                                SELECT 
                                    {field_id_select},
                                    NULL as block_id,
                                    cvr_number,
                                    area_ha,
                                    {crop_type_select},
                                    {organic_farming_select},
                                    {year} as year,
                                    {geometry_select}
                                FROM read_parquet('{temp_file}')
                                WHERE {geometry_where}
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

            # Debug: Check what tables are available and their schemas
            try:
                tables = self.conn.execute("SHOW TABLES").fetchall()
                table_names = [table[0] for table in tables]
                self.log.info(f"Available tables in connection: {table_names}")

                # Check specifically for DST tables and their schemas
                dst_tables = [t for t in table_names if t.startswith("dst_")]
                self.log.info(f"DST tables found: {dst_tables}")

                # Debug: Check the schema of DST tables
                for dst_table in dst_tables:
                    if dst_table.startswith("dst_dst_"):  # Only check the yield tables
                        try:
                            schema = self.conn.execute(f"DESCRIBE {dst_table}").fetchall()
                            self.log.info(f"Schema for {dst_table}: {[col[0] for col in schema]}")

                            # Also show a sample row
                            sample = self.conn.execute(
                                f"SELECT * FROM {dst_table} LIMIT 1"
                            ).fetchall()
                            if sample:
                                self.log.info(f"Sample row from {dst_table}: {sample[0]}")
                        except Exception as schema_e:
                            self.log.warning(f"Could not get schema for {dst_table}: {schema_e}")
            except Exception as debug_e:
                self.log.warning(f"Could not debug tables: {debug_e}")

            # Create production estimates for this year using LEFT JOINs instead of subqueries
            self.conn.execute("""
                CREATE OR REPLACE TABLE year_production_estimates AS
                WITH yield_data AS (
                    SELECT 
                        f.*,
                        CASE 
                            WHEN f.dst_regions IS NOT NULL THEN 
                                COALESCE(
                                    -- Try exact DST region match first
                                    hst77.harvest_value,
                                    gartn1.horticulture_value,
                                    fro.seed_value,
                                    halm1.straw_value,
                                    -- Fallback to national average
                                    hst77_national.harvest_value
                                )
                            ELSE NULL
                        END as yield_estimate_hkg_ha
                    FROM year_fields_with_zones f
                    LEFT JOIN dst_dst_hst77 hst77 ON hst77.area_name = f.dst_regions AND hst77.time_period = CAST(f.year AS VARCHAR) AND hst77.measure_name ILIKE '%udbytte%'
                    LEFT JOIN dst_dst_gartn1 gartn1 ON gartn1.area_name = f.dst_regions AND gartn1.time_period = CAST(f.year AS VARCHAR) AND gartn1.measure_name ILIKE '%udbytte%'
                    LEFT JOIN dst_dst_fro fro ON fro.time_period = CAST(f.year AS VARCHAR) AND fro.measure_name ILIKE '%udbytte%'
                    LEFT JOIN dst_dst_halm1 halm1 ON halm1.area_name = f.dst_regions AND halm1.time_period = CAST(f.year AS VARCHAR) AND halm1.unit_name ILIKE '%udbytte%'
                    LEFT JOIN dst_dst_hst77 hst77_national ON hst77_national.area_name ILIKE '%Hele landet%' AND hst77_national.time_period = CAST(f.year AS VARCHAR) AND hst77_national.measure_name ILIKE '%udbytte%'
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

            # Spatial extension should already be loaded by BaseSource
            # Just verify it's working
            try:
                self.conn.execute("SELECT ST_Point(0, 0)")
                self.log.info("✅ Spatial extension is loaded and working")
            except Exception as spatial_e:
                self.log.error(f"❌ Spatial extension not available: {spatial_e}")
                raise RuntimeError(
                    "Spatial extension is required but not available from BaseSource"
                )

            # Debug: Check the structure and sample data of dst_zones_raw table
            try:
                columns = self.conn.execute("DESCRIBE dst_zones_raw").fetchall()
                self.log.info(f"dst_zones_raw table structure: {columns}")

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
