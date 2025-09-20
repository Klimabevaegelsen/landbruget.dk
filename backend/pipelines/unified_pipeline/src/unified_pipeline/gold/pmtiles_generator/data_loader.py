"""Data loading utilities for PMTiles generation."""

import asyncio
import logging
import re
from typing import Dict, Optional

import duckdb

from unified_pipeline.util.gcs_access import GCSDataAccess

from .config import PMTilesGeneratorConfig

logger = logging.getLogger(__name__)


class PMTilesDataLoader:
    """Loads and integrates data from various sources for PMTiles generation."""

    def __init__(
        self,
        config: PMTilesGeneratorConfig,
        gcs_access: GCSDataAccess,
        duckdb_conn: duckdb.DuckDBPyConnection,
    ):
        """Initialize the data loader.

        Args:
            config: PMTiles generator configuration
            gcs_access: GCS data access instance
            duckdb_conn: DuckDB connection for data processing
        """
        self.config = config
        self.gcs = gcs_access
        self.conn = duckdb_conn

    async def load_and_integrate_field_data(self, year: int) -> Optional[str]:
        """Load and integrate all field-related data for a given year.

        Args:
            year: Target year

        Returns:
            DuckDB table name with integrated data, or None if failed
        """
        logger.info(f"Loading and integrating field data for year {year}")

        try:
            # Load base field data (required)
            field_table = await self._load_fvm_marker_data(year)
            if not field_table:
                logger.error(f"Failed to load FVM marker data for year {year}")
                return None

            # Load optional data sources
            env_analysis_table = None
            production_table = None
            pesticide_table = None
            nles5_table = None

            if self.config.include_environmental_analysis:
                env_analysis_table = await self._load_field_environmental_analysis(year)

            if self.config.include_production_data:
                production_table = await self._load_field_production(year)

            if self.config.include_pesticide_proximity:
                pesticide_table = await self._load_pesticide_proximity(year)

            if self.config.include_nles5_data and year >= 2021:
                nles5_table = await self._load_nles5_estimates(year)

            # Integrate all data
            integrated_table = await self._integrate_field_data(
                field_table,
                env_analysis_table,
                production_table,
                pesticide_table,
                nles5_table,
                year,
            )

            return integrated_table

        except Exception as e:
            logger.error(f"Error loading and integrating field data for year {year}: {e}")
            return None

    async def _load_fvm_marker_data(self, year: int) -> Optional[str]:
        """Load FVM marker data for a specific year.

        Args:
            year: Target year

        Returns:
            DuckDB table name or None if failed
        """
        try:
            path = self.config.fvm_marker_path.format(year=year)
            gcs_path = f"gs://{self.config.gcs_bucket}/{path}"

            logger.info(f"Loading FVM marker data from {gcs_path}")

            # Check if data exists
            if not await asyncio.to_thread(self.gcs.file_exists, gcs_path):
                logger.warning(f"FVM marker data not found: {gcs_path}")
                return None

            # Create table name
            table_name = f"fvm_marker_{year}"

            # Load data into DuckDB
            query = f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT *
            FROM read_parquet('{gcs_path}/*.parquet')
            WHERE year = {year}
            """

            await asyncio.to_thread(self.conn.execute, query)

            # Verify data loaded
            count_result = await asyncio.to_thread(
                self.conn.execute, f"SELECT COUNT(*) FROM {table_name}"
            )
            count = count_result.fetchone()[0]

            if count == 0:
                logger.warning(f"No FVM marker data found for year {year}")
                return None

            logger.info(f"Loaded {count:,} FVM marker records for year {year}")
            return table_name

        except Exception as e:
            logger.error(f"Error loading FVM marker data for year {year}: {e}")
            return None

    async def _load_field_environmental_analysis(self, year: int) -> Optional[str]:
        """Load field environmental analysis data for a specific year.

        Args:
            year: Target year

        Returns:
            DuckDB table name or None if failed
        """
        try:
            path = self.config.field_environmental_path.format(year=year)
            gcs_path = f"gs://{self.config.gcs_bucket}/{path}"

            logger.info(f"Loading field environmental analysis from {gcs_path}")

            if not await asyncio.to_thread(self.gcs.file_exists, gcs_path):
                logger.warning(f"Field environmental analysis data not found: {gcs_path}")
                return None

            table_name = f"field_environmental_{year}"

            query = f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT *
            FROM read_parquet('{gcs_path}/*.parquet')
            WHERE year = {year}
            """

            await asyncio.to_thread(self.conn.execute, query)

            count_result = await asyncio.to_thread(
                self.conn.execute, f"SELECT COUNT(*) FROM {table_name}"
            )
            count = count_result.fetchone()[0]

            logger.info(f"Loaded {count:,} field environmental analysis records for year {year}")
            return table_name

        except Exception as e:
            logger.error(f"Error loading field environmental analysis for year {year}: {e}")
            return None

    async def _load_field_production(self, year: int) -> Optional[str]:
        """Load field production estimates for a specific year.

        Args:
            year: Target year

        Returns:
            DuckDB table name or None if failed
        """
        try:
            path = self.config.field_production_path.format(year=year)
            gcs_path = f"gs://{self.config.gcs_bucket}/{path}"

            logger.info(f"Loading field production data from {gcs_path}")

            if not await asyncio.to_thread(self.gcs.file_exists, gcs_path):
                logger.warning(f"Field production data not found: {gcs_path}")
                return None

            table_name = f"field_production_{year}"

            query = f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT *
            FROM read_parquet('{gcs_path}/*.parquet')
            WHERE year = {year}
            """

            await asyncio.to_thread(self.conn.execute, query)

            count_result = await asyncio.to_thread(
                self.conn.execute, f"SELECT COUNT(*) FROM {table_name}"
            )
            count = count_result.fetchone()[0]

            logger.info(f"Loaded {count:,} field production records for year {year}")
            return table_name

        except Exception as e:
            logger.error(f"Error loading field production data for year {year}: {e}")
            return None

    async def _load_pesticide_proximity(self, year: int) -> Optional[str]:
        """Load pesticide proximity data for a specific year.

        Args:
            year: Target year (base year, will look for year_year+1 pattern)

        Returns:
            DuckDB table name or None if failed
        """
        try:
            next_year = year + 1
            path = self.config.pesticide_proximity_path.format(year=year, next_year=next_year)
            gcs_path = f"gs://{self.config.gcs_bucket}/{path}"

            logger.info(f"Loading pesticide proximity data from {gcs_path}")

            if not await asyncio.to_thread(self.gcs.file_exists, gcs_path):
                logger.warning(f"Pesticide proximity data not found: {gcs_path}")
                return None

            table_name = f"pesticide_proximity_{year}"

            query = f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT *
            FROM read_parquet('{gcs_path}/*.parquet')
            """

            await asyncio.to_thread(self.conn.execute, query)

            count_result = await asyncio.to_thread(
                self.conn.execute, f"SELECT COUNT(*) FROM {table_name}"
            )
            count = count_result.fetchone()[0]

            logger.info(f"Loaded {count:,} pesticide proximity records for year {year}")
            return table_name

        except Exception as e:
            logger.error(f"Error loading pesticide proximity data for year {year}: {e}")
            return None

    async def _load_nles5_estimates(self, year: int) -> Optional[str]:
        """Load NLES5 nitrogen estimation data for a specific year.

        Args:
            year: Target year

        Returns:
            DuckDB table name or None if failed
        """
        try:
            path = self.config.nles5_estimation_path
            gcs_path = f"gs://{self.config.gcs_bucket}/{path}"

            logger.info(f"Loading NLES5 data from {gcs_path}")

            if not await asyncio.to_thread(self.gcs.file_exists, gcs_path):
                logger.warning(f"NLES5 data not found: {gcs_path}")
                return None

            table_name = f"nles5_estimates_{year}"

            query = f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT *
            FROM read_parquet('{gcs_path}/*.parquet')
            WHERE year = {year}
            """

            await asyncio.to_thread(self.conn.execute, query)

            count_result = await asyncio.to_thread(
                self.conn.execute, f"SELECT COUNT(*) FROM {table_name}"
            )
            count = count_result.fetchone()[0]

            logger.info(f"Loaded {count:,} NLES5 estimation records for year {year}")
            return table_name if count > 0 else None

        except Exception as e:
            logger.error(f"Error loading NLES5 data for year {year}: {e}")
            return None

    async def _integrate_field_data(
        self,
        field_table: str,
        env_analysis_table: Optional[str],
        production_table: Optional[str],
        pesticide_table: Optional[str],
        nles5_table: Optional[str],
        year: int,
    ) -> str:
        """Integrate all field data sources into a single table.

        Args:
            field_table: FVM marker table name (required)
            env_analysis_table: Environmental analysis table name (optional)
            production_table: Production estimates table name (optional)
            pesticide_table: Pesticide proximity table name (optional)
            nles5_table: NLES5 estimates table name (optional)
            year: Target year

        Returns:
            Integrated table name
        """
        try:
            integrated_table = f"integrated_fields_{year}"

            # Start with base field data
            query_parts = [
                f"""
            CREATE OR REPLACE TABLE {integrated_table} AS
            SELECT
                f.field_uuid,
                f.field_id,
                f.block_id,
                f.cvr_number,
                f.year,
                f.geometry,
                f.area_ha,
                f.crop_name,
                f.crop_code,
                f.is_organic,
                f.municipality
            """
            ]

            # Add environmental analysis fields
            if env_analysis_table:
                query_parts.append("""
                ,e.field_bnbo_coverage_pct,
                e.field_bnbo_water_coverage_pct,
                e.bnbo_status_categories,
                e.bnbo_action_required_hectares,
                e.bnbo_completed_hectares,
                e.field_wetland_coverage_pct,
                e.field_wetland_water_coverage_pct,
                e.field_soil_coverage_pct,
                e.soil_coverage_details
                """)

            # Add production fields
            if production_table:
                query_parts.append("""
                ,p.yield_estimate_hkg_ha,
                p.production_estimate_hkg,
                p.dst_regions
                """)

            # Add NLES5 fields
            if nles5_table:
                query_parts.append("""
                ,n.nitrogen_washout_kg_ha,
                n.total_nitrogen_washout_kg,
                n.soil_type as nles5_soil_type,
                n.m_code as nles5_crop_code,
                n.data_quality as nles5_data_quality
                """)

            # Add FROM clause
            query_parts.append(f"""
            FROM {field_table} f
            """)

            # Add JOINs for optional tables
            if env_analysis_table:
                query_parts.append(f"""
                LEFT JOIN {env_analysis_table} e ON f.field_uuid = e.field_uuid
                """)

            if production_table:
                query_parts.append(f"""
                LEFT JOIN {production_table} p ON f.field_uuid = p.field_uuid
                """)

            if nles5_table:
                query_parts.append(f"""
                LEFT JOIN {nles5_table} n ON f.field_id = n.field_id
                """)

            # Execute integration query
            integration_query = "".join(query_parts)
            await asyncio.to_thread(self.conn.execute, integration_query)

            # If pesticide data is available, create a summary and add it
            if pesticide_table:
                await self._add_pesticide_summary(integrated_table, pesticide_table)

            # Verify integration
            count_result = await asyncio.to_thread(
                self.conn.execute, f"SELECT COUNT(*) FROM {integrated_table}"
            )
            count = count_result.fetchone()[0]

            logger.info(f"Integrated {count:,} field records for year {year}")
            return integrated_table

        except Exception as e:
            logger.error(f"Error integrating field data for year {year}: {e}")
            raise

    async def _add_pesticide_summary(self, integrated_table: str, pesticide_table: str):
        """Add pesticide summary data to the integrated table.

        Args:
            integrated_table: Name of the integrated table to update
            pesticide_table: Name of the pesticide proximity table
        """
        try:
            # Create pesticide summary
            summary_query = f"""
            CREATE OR REPLACE TABLE temp_pesticide_summary AS
            SELECT
                field_uuid,
                COUNT(*) as pesticide_applications,
                STRING_AGG(DISTINCT PesticideName, ', ') as pesticides_used,
                residential_buildings_formatted,
                educational_facilities_formatted,
                water_distance_formatted,
                AVG(MatchConfidence) as avg_match_confidence
            FROM {pesticide_table}
            GROUP BY field_uuid, residential_buildings_formatted,
                     educational_facilities_formatted, water_distance_formatted
            """

            await asyncio.to_thread(self.conn.execute, summary_query)

            # Add pesticide columns to integrated table
            update_query = f"""
            CREATE OR REPLACE TABLE {integrated_table}_updated AS
            SELECT
                i.*,
                ps.pesticide_applications,
                ps.pesticides_used,
                ps.residential_buildings_formatted,
                ps.educational_facilities_formatted,
                ps.water_distance_formatted,
                ps.avg_match_confidence
            FROM {integrated_table} i
            LEFT JOIN temp_pesticide_summary ps ON i.field_uuid = ps.field_uuid
            """

            await asyncio.to_thread(self.conn.execute, update_query)

            # Replace original table
            await asyncio.to_thread(self.conn.execute, f"DROP TABLE {integrated_table}")
            await asyncio.to_thread(
                self.conn.execute,
                f"ALTER TABLE {integrated_table}_updated RENAME TO {integrated_table}",
            )
            await asyncio.to_thread(self.conn.execute, "DROP TABLE temp_pesticide_summary")

        except Exception as e:
            logger.error(f"Error adding pesticide summary: {e}")
            raise

    async def load_environmental_layers(self) -> Dict[str, str]:
        """Load environmental layer data (year-independent).

        Returns:
            Dictionary mapping layer names to DuckDB table names
        """
        logger.info("Loading environmental layers")

        layers = {}

        # Load BNBO status
        bnbo_table = await self._load_bnbo_status()
        if bnbo_table:
            layers["bnbo_status"] = bnbo_table

        # Load wetlands
        wetlands_table = await self._load_wetlands()
        if wetlands_table:
            layers["wetlands"] = wetlands_table

        # Load water projects
        water_projects_table = await self._load_water_projects()
        if water_projects_table:
            layers["water_projects"] = water_projects_table

        # Load BBR buildings
        buildings_table = await self._load_bbr_buildings()
        if buildings_table:
            layers["bbr_buildings"] = buildings_table

        return layers

    async def _find_latest_timestamped_path(self, base_path: str) -> Optional[str]:
        """Find the latest timestamped directory in a base path.

        Args:
            base_path: Base GCS path like gs://bucket/silver/dataset/

        Returns:
            Full path to latest timestamped directory, or None if not found
        """
        try:
            # List directories in the base path
            directories = await asyncio.to_thread(self.gcs.list_files, f"{base_path}*")
            if not directories:
                return None

            # Filter for timestamped directories (YYYYMMDD_HHMMSS pattern)
            timestamped_dirs = []
            for dir_path in directories:
                # Extract directory name from path
                dir_name = dir_path.rstrip("/").split("/")[-1]
                # Check if it matches timestamp pattern
                if re.match(r"^\d{8}_\d{6}$", dir_name):
                    # Ensure path ends with / for directory access
                    if not dir_path.endswith("/"):
                        dir_path += "/"
                    timestamped_dirs.append((dir_name, dir_path))

            if not timestamped_dirs:
                return None

            # Sort by timestamp (latest first) and return the path
            timestamped_dirs.sort(key=lambda x: x[0], reverse=True)
            latest_path = timestamped_dirs[0][1]

            logger.info(f"Found latest timestamped directory: {latest_path}")
            return latest_path

        except Exception as e:
            logger.error(f"Error finding latest timestamped path for {base_path}: {e}")
            return None

    async def _load_bnbo_status(self) -> Optional[str]:
        """Load BNBO status dissolved data."""
        try:
            gcs_path = f"gs://{self.config.gcs_bucket}/{self.config.bnbo_status_path}"

            if not await asyncio.to_thread(self.gcs.file_exists, gcs_path):
                logger.warning(f"BNBO status data not found: {gcs_path}")
                return None

            table_name = "bnbo_status_dissolved"

            query = f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT *
            FROM read_parquet('{gcs_path}/*.parquet')
            """

            await asyncio.to_thread(self.conn.execute, query)

            count_result = await asyncio.to_thread(
                self.conn.execute, f"SELECT COUNT(*) FROM {table_name}"
            )
            count = count_result.fetchone()[0]

            logger.info(f"Loaded {count:,} BNBO status records")
            return table_name

        except Exception as e:
            logger.error(f"Error loading BNBO status data: {e}")
            return None

    async def _load_wetlands(self) -> Optional[str]:
        """Load wetlands dissolved data."""
        try:
            gcs_path = f"gs://{self.config.gcs_bucket}/{self.config.wetlands_path}"

            if not await asyncio.to_thread(self.gcs.file_exists, gcs_path):
                logger.warning(f"Wetlands data not found: {gcs_path}")
                return None

            table_name = "wetlands_dissolved"

            query = f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT *
            FROM read_parquet('{gcs_path}/*.parquet')
            """

            await asyncio.to_thread(self.conn.execute, query)

            count_result = await asyncio.to_thread(
                self.conn.execute, f"SELECT COUNT(*) FROM {table_name}"
            )
            count = count_result.fetchone()[0]

            logger.info(f"Loaded {count:,} wetlands records")
            return table_name

        except Exception as e:
            logger.error(f"Error loading wetlands data: {e}")
            return None

    async def _load_water_projects(self) -> Optional[str]:
        """Load water projects dissolved data."""
        try:
            gcs_path = f"gs://{self.config.gcs_bucket}/{self.config.water_projects_path}"

            if not await asyncio.to_thread(self.gcs.file_exists, gcs_path):
                logger.warning(f"Water projects data not found: {gcs_path}")
                return None

            table_name = "water_projects_dissolved"

            query = f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT *
            FROM read_parquet('{gcs_path}/*.parquet')
            """

            await asyncio.to_thread(self.conn.execute, query)

            count_result = await asyncio.to_thread(
                self.conn.execute, f"SELECT COUNT(*) FROM {table_name}"
            )
            count = count_result.fetchone()[0]

            logger.info(f"Loaded {count:,} water projects records")
            return table_name

        except Exception as e:
            logger.error(f"Error loading water projects data: {e}")
            return None

    async def _load_bbr_buildings(self) -> Optional[str]:
        """Load BBR buildings data."""
        try:
            base_path = f"gs://{self.config.gcs_bucket}/{self.config.bbr_buildings_path}"

            # Find the latest timestamped directory
            gcs_path = await self._find_latest_timestamped_path(base_path)
            if not gcs_path:
                logger.warning(f"BBR buildings data not found in: {base_path}")
                return None

            table_name = "bbr_buildings"

            query = f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT *
            FROM read_parquet('{gcs_path}joined_buildings.parquet')
            WHERE category_group IN ('residential', 'publicServices', 'agricultural')
                AND geo_building_centroid IS NOT NULL
            """

            await asyncio.to_thread(self.conn.execute, query)

            count_result = await asyncio.to_thread(
                self.conn.execute, f"SELECT COUNT(*) FROM {table_name}"
            )
            count = count_result.fetchone()[0]

            logger.info(f"Loaded {count:,} BBR buildings records")
            return table_name

        except Exception as e:
            logger.error(f"Error loading BBR buildings data: {e}")
            return None
