"""Data loading utilities for PMTiles generation."""

import asyncio

import duckdb
from common.logging_utils import get_pipeline_logger
from common.storage import StorageAccess

from .config import PMTilesGeneratorConfig

logger = get_pipeline_logger(__name__)


class PMTilesDataLoader:
    """Loads and integrates data from various sources for PMTiles generation."""

    def __init__(
        self,
        config: PMTilesGeneratorConfig,
        storage_access: StorageAccess,
        duckdb_conn: duckdb.DuckDBPyConnection,
    ):
        """Initialize the data loader.

        Args:
            config: PMTiles generator configuration
            storage_access: cloud storage data access instance
            duckdb_conn: DuckDB connection for data processing
        """
        self.config = config
        self.storage = storage_access
        self.conn = duckdb_conn

    async def load_and_integrate_field_data(self, year: int) -> str | None:
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

            # Ensure schema compatibility for older years
            await self._ensure_fvm_schema_compatibility(field_table, year)

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
            return await self._integrate_field_data(
                field_table,
                env_analysis_table,
                production_table,
                pesticide_table,
                nles5_table,
                year,
            )

        except Exception as e:
            logger.error(f"Error loading and integrating field data for year {year}: {e}")
            return None

    async def _load_fvm_marker_data(self, year: int) -> str | None:
        """Load FVM marker data for a specific year.

        For 2023 pesticide data, we need 2024 field boundaries (Y+1 pattern).
        Falls back to older timestamps if the latest is unreadable.

        Args:
            year: Target pesticide year

        Returns:
            DuckDB table name or None if failed
        """
        boundary_year = year + 1
        logger.info(
            f"Using {boundary_year} field boundaries for {year} pesticide data (Y+1 pattern)"
        )

        base_path = f"r2://{self.config.storage_bucket}/silver/fvm_marker_{boundary_year}"
        storage_paths = await self._find_timestamped_paths_ranked(base_path)
        if not storage_paths:
            logger.warning(f"FVM marker data not found in: {base_path}")
            return None

        table_name = f"fvm_marker_{year}"

        for i, storage_path in enumerate(storage_paths):
            try:
                logger.info(f"Loading FVM marker data from {storage_path}")

                query = f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT *
                FROM read_parquet('{storage_path}data.parquet')
                WHERE year = {boundary_year}
                """
                await asyncio.to_thread(self.conn.execute, query)

                count_result = await asyncio.to_thread(
                    self.conn.execute, f"SELECT COUNT(*) FROM {table_name}"
                )
                count = count_result.fetchone()[0]

                if count == 0:
                    logger.warning(f"No FVM marker data in {storage_path} for year {year}")
                    continue

                logger.info(f"Loaded {count:,} FVM marker records for year {year}")
                return table_name

            except Exception as e:
                if i < len(storage_paths) - 1:
                    logger.warning(
                        f"Failed to load FVM marker from {storage_path}, trying older timestamp: {e}"
                    )
                else:
                    logger.error(f"Error loading FVM marker data for year {year}: {e}")

        return None

    async def _ensure_fvm_schema_compatibility(self, table_name: str, year: int) -> None:
        """Ensure FVM table has required columns for all years.

        Older years may be missing columns that newer years have.
        """
        try:
            # Get current schema
            schema_result = await asyncio.to_thread(self.conn.execute, f"DESCRIBE {table_name}")
            existing_columns = {row[0] for row in schema_result.fetchall()}

            # Required columns that might be missing in older years
            required_columns = {
                "crop_name": "VARCHAR DEFAULT NULL",
                "crop_code": "VARCHAR DEFAULT NULL",
                "is_organic": "BOOLEAN DEFAULT false",
                "organic_conversion_date": "TIMESTAMP DEFAULT NULL",
                "organic_deregistration_date": "TIMESTAMP DEFAULT NULL",
                "organic_conversion_status": "VARCHAR DEFAULT NULL",
            }

            # Add missing columns
            for col_name, col_type in required_columns.items():
                if col_name not in existing_columns:
                    logger.info(f"Adding missing column {col_name} to {table_name} for year {year}")
                    await asyncio.to_thread(
                        self.conn.execute,
                        f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}",
                    )

        except Exception as e:
            logger.warning(f"Error ensuring schema compatibility for {table_name}: {e}")
            # Don't fail the pipeline for schema issues

    async def _load_field_environmental_analysis(self, year: int) -> str | None:
        """Load field environmental analysis data for a specific year.

        Falls back to older timestamps if the latest is unreadable.

        Args:
            year: Target year (for pesticide data)

        Returns:
            DuckDB table name or None if failed
        """
        env_year = year + 1
        logger.info(f"Using {env_year} environmental data for {year} pesticide data (Y+1 pattern)")

        base_path = (
            f"r2://{self.config.storage_bucket}/gold/field_environmental_analysis_fields_{env_year}"
        )
        storage_paths = await self._find_timestamped_paths_ranked(base_path)
        if not storage_paths:
            logger.warning(f"Environmental analysis data not found in: {base_path}")
            return None

        table_name = f"field_environmental_{year}"

        for i, storage_path in enumerate(storage_paths):
            try:
                logger.info(f"Loading field environmental analysis from {storage_path}")

                query = f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT *
                FROM read_parquet('{storage_path}/data.parquet')
                """
                await asyncio.to_thread(self.conn.execute, query)

                count_result = await asyncio.to_thread(
                    self.conn.execute, f"SELECT COUNT(*) FROM {table_name}"
                )
                count = count_result.fetchone()[0]

                logger.info(
                    f"Loaded {count:,} environmental records from {env_year} "
                    f"data for pesticide year {year}"
                )
                return table_name

            except Exception as e:
                if i < len(storage_paths) - 1:
                    logger.warning(
                        f"Failed to load environmental analysis from {storage_path}, "
                        f"trying older timestamp: {e}"
                    )
                else:
                    logger.error(f"Error loading field environmental analysis for year {year}: {e}")

        return None

    async def _load_field_production(self, year: int) -> str | None:
        """Load field production estimates for a specific year.

        Production data uses Y+1 pattern to match field boundaries.

        Args:
            year: Target pesticide year

        Returns:
            DuckDB table name or None if failed
        """
        production_year = year + 1
        base_path = f"r2://{self.config.storage_bucket}/gold/field_production_{production_year}"
        storage_paths = await self._find_timestamped_paths_ranked(base_path)
        if not storage_paths:
            logger.warning(f"Field production data not found in: {base_path}")
            return None

        table_name = f"field_production_{year}"

        for i, storage_path in enumerate(storage_paths):
            try:
                logger.info(f"Loading field production data from {storage_path}")

                query = f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT *
                FROM read_parquet('{storage_path}data.parquet')
                WHERE year = {production_year}
                """
                await asyncio.to_thread(self.conn.execute, query)

                count_result = await asyncio.to_thread(
                    self.conn.execute, f"SELECT COUNT(*) FROM {table_name}"
                )
                count = count_result.fetchone()[0]

                logger.info(
                    f"Loaded {count:,} field production records for year {production_year} "
                    f"(for pesticide year {year})"
                )
                return table_name

            except Exception as e:
                if i < len(storage_paths) - 1:
                    logger.warning(
                        f"Failed to load field production from {storage_path}, "
                        f"trying older timestamp: {e}"
                    )
                else:
                    logger.error(f"Error loading field production data for year {year}: {e}")

        return None

    async def _load_pesticide_proximity(self, year: int) -> str | None:
        """Load pesticide proximity data for a specific year.

        Args:
            year: Target year (base year, will look for year_year+1 pattern)

        Returns:
            DuckDB table name or None if failed
        """
        next_year = year + 1
        base_path = f"r2://{self.config.storage_bucket}/gold/pesticide_proximity_{year}_{next_year}"
        storage_paths = await self._find_timestamped_paths_ranked(base_path)
        if not storage_paths:
            logger.warning(f"Pesticide proximity data not found in: {base_path}")
            return None

        table_name = f"pesticide_proximity_{year}"

        for i, storage_path in enumerate(storage_paths):
            try:
                logger.info(f"Loading pesticide proximity data from {storage_path}")

                query = f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT *
                FROM read_parquet('{storage_path}pesticide_proximity_{year}_{next_year}.parquet')
                """
                await asyncio.to_thread(self.conn.execute, query)

                count_result = await asyncio.to_thread(
                    self.conn.execute, f"SELECT COUNT(*) FROM {table_name}"
                )
                count = count_result.fetchone()[0]

                logger.info(f"Loaded {count:,} pesticide proximity records for year {year}")
                return table_name

            except Exception as e:
                if i < len(storage_paths) - 1:
                    logger.warning(
                        f"Failed to load pesticide proximity from {storage_path}, "
                        f"trying older timestamp: {e}"
                    )
                else:
                    logger.error(f"Error loading pesticide proximity data for year {year}: {e}")

        return None

    async def _load_nles5_estimates(self, year: int) -> str | None:
        """Load NLES5 nitrogen estimation data for a specific year.

        Args:
            year: Target year

        Returns:
            DuckDB table name or None if failed
        """
        try:
            path = self.config.nles5_estimation_path
            storage_path = f"r2://{self.config.storage_bucket}/{path}"

            logger.info(f"Loading NLES5 data from {storage_path}")

            if not await asyncio.to_thread(self.storage.file_exists, storage_path):
                logger.warning(f"NLES5 data not found: {storage_path}")
                return None

            table_name = f"nles5_estimates_{year}"

            query = f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT *
            FROM read_parquet('{storage_path}/*.parquet')
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
        env_analysis_table: str | None,
        production_table: str | None,
        pesticide_table: str | None,
        nles5_table: str | None,
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
                LEFT JOIN {nles5_table} n ON f.field_uuid = n.field_uuid
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
            # First, check if BMD data is available and enhance pesticide data
            enhanced_pesticide_table = await self._enhance_pesticide_with_bmd(pesticide_table)

            # Check if enhancement worked by testing for BMD columns
            has_bmd_data = False
            try:
                await asyncio.to_thread(
                    self.conn.execute,
                    f"SELECT COUNT(*) FROM {enhanced_pesticide_table} "
                    "WHERE contains_pfas IS NOT NULL LIMIT 1",
                )
                has_bmd_data = True
                logger.info("BMD enhancement successful - using categorized pesticide aggregation")
            except Exception:
                logger.warning("BMD enhancement failed - falling back to basic aggregation")
                has_bmd_data = False

            if has_bmd_data:
                # Enhanced aggregation with BMD classifications
                summary_query = f"""
                CREATE OR REPLACE TABLE temp_pesticide_summary AS
                SELECT
                    field_uuid,
                    COUNT(*) as pesticide_applications,
                    STRING_AGG(DISTINCT PesticideName, ', ') as pesticides_used,

                    -- Enhanced categorized product details with risk + product group + burden
                    -- Format: Name:Dosage:Unit:HealthRisk:EnvRisk:SignalWord:ProductGroup:Burden
                    STRING_AGG(
                        CASE WHEN COALESCE(contains_pfas, false) = true
                        THEN PesticideName || ':' || ROUND(COALESCE(DosageQuantity, 0), 2) || ':' ||
                             COALESCE(DosageUnit, 'ukendt') || ':' ||
                             COALESCE(health_risk, '') || ':' ||
                             COALESCE(environmental_risk, '') || ':' ||
                             COALESCE(signal_word, '') || ':' ||
                             COALESCE(product_group, '') || ':' ||
                             ROUND(COALESCE(samlet_belastning, 0), 2)
                        END, ';'
                    ) FILTER (WHERE COALESCE(contains_pfas, false) = true) as pfas_products_detail,
                    COUNT(
                        CASE WHEN COALESCE(contains_pfas, false) = true THEN 1 END
                    ) as pfas_applications,

                    STRING_AGG(
                        CASE WHEN COALESCE(contains_diquat, false) = true
                        THEN PesticideName || ':' || ROUND(COALESCE(DosageQuantity, 0), 2) || ':' ||
                             COALESCE(DosageUnit, 'ukendt') || ':' ||
                             COALESCE(health_risk, '') || ':' ||
                             COALESCE(environmental_risk, '') || ':' ||
                             COALESCE(signal_word, '') || ':' ||
                             COALESCE(product_group, '') || ':' ||
                             ROUND(COALESCE(samlet_belastning, 0), 2)
                        END, ';'
                    ) FILTER (
                        WHERE COALESCE(contains_diquat, false) = true
                    ) as diquat_products_detail,
                    COUNT(
                        CASE WHEN COALESCE(contains_diquat, false) = true THEN 1 END
                    ) as diquat_applications,

                    STRING_AGG(
                        CASE WHEN COALESCE(contains_glyphosate, false) = true
                        THEN PesticideName || ':' || ROUND(COALESCE(DosageQuantity, 0), 2) || ':' ||
                             COALESCE(DosageUnit, 'ukendt') || ':' ||
                             COALESCE(health_risk, '') || ':' ||
                             COALESCE(environmental_risk, '') || ':' ||
                             COALESCE(signal_word, '') || ':' ||
                             COALESCE(product_group, '') || ':' ||
                             ROUND(COALESCE(samlet_belastning, 0), 2)
                        END, ';'
                    ) FILTER (
                        WHERE COALESCE(contains_glyphosate, false) = true
                    ) as glyphosate_products_detail,
                    COUNT(
                        CASE WHEN COALESCE(contains_glyphosate, false) = true THEN 1 END
                    ) as glyphosate_applications,

                    STRING_AGG(
                        CASE WHEN COALESCE(contains_pfas, false) = false
                                  AND COALESCE(contains_diquat, false) = false
                                  AND COALESCE(contains_glyphosate, false) = false
                        THEN PesticideName || ':' || ROUND(COALESCE(DosageQuantity, 0), 2) || ':' ||
                             COALESCE(DosageUnit, 'ukendt') || ':' ||
                             COALESCE(health_risk, '') || ':' ||
                             COALESCE(environmental_risk, '') || ':' ||
                             COALESCE(signal_word, '') || ':' ||
                             COALESCE(product_group, '') || ':' ||
                             ROUND(COALESCE(samlet_belastning, 0), 2)
                        END, ';'
                    ) FILTER (WHERE COALESCE(contains_pfas, false) = false
                                    AND COALESCE(contains_diquat, false) = false
                                    AND COALESCE(contains_glyphosate, false) = false
                    ) as other_products_detail,
                    COUNT(CASE WHEN COALESCE(contains_pfas, false) = false
                                  AND COALESCE(contains_diquat, false) = false
                                  AND COALESCE(contains_glyphosate, false) = false
                                  THEN 1 END) as other_applications,

                    -- Legacy unit-based aggregations for backward compatibility
                    STRING_AGG(
                        CASE WHEN DosageUnit IN ('2', 'kg')
                        THEN PesticideName || ':' || ROUND(COALESCE(DosageQuantity, 0), 2)
                        END, ';'
                    ) FILTER (WHERE DosageUnit IN ('2', 'kg')) as pesticides_kg_detail,

                    STRING_AGG(
                        CASE WHEN DosageUnit IN ('4', 'L', 'liter')
                        THEN PesticideName || ':' || ROUND(COALESCE(DosageQuantity, 0), 2)
                        END, ';'
                    ) FILTER (WHERE DosageUnit IN ('4', 'L', 'liter')) as pesticides_liters_detail,

                    STRING_AGG(
                        CASE WHEN DosageUnit IN ('1', 'g', 'gram')
                        THEN PesticideName || ':' || ROUND(COALESCE(DosageQuantity, 0), 2)
                        END, ';'
                    ) FILTER (WHERE DosageUnit IN ('1', 'g', 'gram')) as pesticides_grams_detail,

                    STRING_AGG(
                        CASE WHEN DosageUnit IN ('5', 'ml', 'milliliter')
                        THEN PesticideName || ':' || ROUND(COALESCE(DosageQuantity, 0), 2)
                        END, ';'
                    ) FILTER (
                        WHERE DosageUnit IN ('5', 'ml', 'milliliter')
                    ) as pesticides_ml_detail,

                    STRING_AGG(
                        CASE WHEN DosageUnit IN ('3', 'tablet', 'tabletter')
                        THEN PesticideName || ':' || ROUND(COALESCE(DosageQuantity, 0), 2)
                        END, ';'
                    ) FILTER (
                        WHERE DosageUnit IN ('3', 'tablet', 'tabletter')
                    ) as pesticides_tablets_detail,

                    -- BMD burden calculations for visualization (B/ha)
                    -- Formula matches api_export: SUM(DosageQuantity * samlet_belastning) / AllocatedArea
                    CASE WHEN MAX(AllocatedArea) > 0
                        THEN SUM(COALESCE(DosageQuantity * samlet_belastning, 0)) / MAX(AllocatedArea)
                        ELSE 0 END as total_pesticide_belastning,
                    CASE WHEN MAX(AllocatedArea) > 0
                        THEN SUM(CASE WHEN COALESCE(contains_pfas, false) = true
                            THEN COALESCE(DosageQuantity * samlet_belastning, 0) ELSE 0 END) / MAX(AllocatedArea)
                        ELSE 0 END as total_pfas_belastning,
                    CASE WHEN MAX(AllocatedArea) > 0
                        THEN SUM(CASE WHEN COALESCE(contains_diquat, false) = true
                            THEN COALESCE(DosageQuantity * samlet_belastning, 0) ELSE 0 END) / MAX(AllocatedArea)
                        ELSE 0 END as total_diquat_belastning,
                    CASE WHEN MAX(AllocatedArea) > 0
                        THEN SUM(CASE WHEN COALESCE(contains_glyphosate, false) = true
                            THEN COALESCE(DosageQuantity * samlet_belastning, 0) ELSE 0 END) / MAX(AllocatedArea)
                        ELSE 0 END as total_glyphosate_belastning,

                    -- Active ingredient calculations
                    SUM(CASE WHEN COALESCE(contains_pfas, false) = true
                        THEN COALESCE(DosageQuantity, 0)
                        ELSE 0 END) as total_pfas_active_ingredient_kg,
                    SUM(CASE WHEN COALESCE(contains_glyphosate, false) = true
                        THEN COALESCE(DosageQuantity, 0)
                        ELSE 0 END) as total_glyphosate_active_ingredient_kg,

                    -- Product count
                    COUNT(DISTINCT PesticideName) as unique_pesticide_products,

                    -- Total dosage by unit (frontend displays as total per field)
                    SUM(CASE WHEN DosageUnit IN ('2', 'kg')
                        THEN COALESCE(DosageQuantity, 0) ELSE 0 END) as total_dosage_kg,
                    SUM(CASE WHEN DosageUnit IN ('4', 'L', 'liter')
                        THEN COALESCE(DosageQuantity, 0) ELSE 0 END) as total_dosage_liters,
                    SUM(CASE WHEN DosageUnit IN ('1', 'g', 'gram')
                        THEN COALESCE(DosageQuantity, 0) ELSE 0 END) as total_dosage_grams,
                    SUM(CASE WHEN DosageUnit IN ('5', 'ml', 'milliliter')
                        THEN COALESCE(DosageQuantity, 0) ELSE 0 END) as total_dosage_ml,
                    SUM(CASE WHEN DosageUnit IN ('3', 'tablet', 'tabletter')
                        THEN COALESCE(DosageQuantity, 0) ELSE 0 END) as total_dosage_tablets,


                    -- Proximity data (ANY_VALUE since proximity is per field_uuid which may vary)
                    ANY_VALUE(residential_buildings_formatted) as residential_buildings_formatted,
                    ANY_VALUE(educational_facilities_formatted) as educational_facilities_formatted,
                    ANY_VALUE(water_distance_formatted) as water_distance_formatted,
                    AVG(MatchConfidence) as avg_match_confidence
                FROM {enhanced_pesticide_table}
                GROUP BY field_uuid
                """
            else:
                # Fallback to basic aggregation without BMD classifications
                summary_query = f"""
                CREATE OR REPLACE TABLE temp_pesticide_summary AS
                SELECT
                    field_uuid,
                    COUNT(*) as pesticide_applications,
                    STRING_AGG(DISTINCT PesticideName, ', ') as pesticides_used,

                    -- Basic product details without classifications (no BMD data available)
                    NULL as pfas_products_detail,
                    0 as pfas_applications,
                    NULL as diquat_products_detail,
                    0 as diquat_applications,
                    NULL as glyphosate_products_detail,
                    0 as glyphosate_applications,
                    STRING_AGG(
                        PesticideName || ':' || ROUND(COALESCE(DosageQuantity, 0), 2) || ':' ||
                        COALESCE(DosageUnit, 'ukendt'), ';'
                    ) as other_products_detail,
                    COUNT(*) as other_applications,

                    -- Legacy unit-based aggregations
                    STRING_AGG(
                        CASE WHEN DosageUnit IN ('2', 'kg')
                        THEN PesticideName || ':' || ROUND(COALESCE(DosageQuantity, 0), 2)
                        END, ';'
                    ) FILTER (WHERE DosageUnit IN ('2', 'kg')) as pesticides_kg_detail,

                    STRING_AGG(
                        CASE WHEN DosageUnit IN ('4', 'L', 'liter')
                        THEN PesticideName || ':' || ROUND(COALESCE(DosageQuantity, 0), 2)
                        END, ';'
                    ) FILTER (WHERE DosageUnit IN ('4', 'L', 'liter')) as pesticides_liters_detail,

                    STRING_AGG(
                        CASE WHEN DosageUnit IN ('1', 'g', 'gram')
                        THEN PesticideName || ':' || ROUND(COALESCE(DosageQuantity, 0), 2)
                        END, ';'
                    ) FILTER (WHERE DosageUnit IN ('1', 'g', 'gram')) as pesticides_grams_detail,

                    STRING_AGG(
                        CASE WHEN DosageUnit IN ('5', 'ml', 'milliliter')
                        THEN PesticideName || ':' || ROUND(COALESCE(DosageQuantity, 0), 2)
                        END, ';'
                    ) FILTER (
                        WHERE DosageUnit IN ('5', 'ml', 'milliliter')
                    ) as pesticides_ml_detail,

                    STRING_AGG(
                        CASE WHEN DosageUnit IN ('3', 'tablet', 'tabletter')
                        THEN PesticideName || ':' || ROUND(COALESCE(DosageQuantity, 0), 2)
                        END, ';'
                    ) FILTER (
                        WHERE DosageUnit IN ('3', 'tablet', 'tabletter')
                    ) as pesticides_tablets_detail,

                    -- BMD burden calculations (fallback - no BMD data available)
                    0 as total_pesticide_belastning,
                    0 as total_pfas_belastning,
                    0 as total_diquat_belastning,
                    0 as total_glyphosate_belastning,

                    -- Active ingredient calculations (fallback)
                    0 as total_pfas_active_ingredient_kg,
                    0 as total_glyphosate_active_ingredient_kg,

                    -- Product count
                    COUNT(DISTINCT PesticideName) as unique_pesticide_products,

                    -- Total dosage by unit (frontend displays as total per field) - fallback version
                    SUM(CASE WHEN DosageUnit IN ('2', 'kg')
                        THEN COALESCE(DosageQuantity, 0) ELSE 0 END) as total_dosage_kg,
                    SUM(CASE WHEN DosageUnit IN ('4', 'L', 'liter')
                        THEN COALESCE(DosageQuantity, 0) ELSE 0 END) as total_dosage_liters,
                    SUM(CASE WHEN DosageUnit IN ('1', 'g', 'gram')
                        THEN COALESCE(DosageQuantity, 0) ELSE 0 END) as total_dosage_grams,
                    SUM(CASE WHEN DosageUnit IN ('5', 'ml', 'milliliter')
                        THEN COALESCE(DosageQuantity, 0) ELSE 0 END) as total_dosage_ml,
                    SUM(CASE WHEN DosageUnit IN ('3', 'tablet', 'tabletter')
                        THEN COALESCE(DosageQuantity, 0) ELSE 0 END) as total_dosage_tablets,

                    -- Proximity data (ANY_VALUE since proximity is per field_uuid which may vary)
                    ANY_VALUE(residential_buildings_formatted) as residential_buildings_formatted,
                    ANY_VALUE(educational_facilities_formatted) as educational_facilities_formatted,
                    ANY_VALUE(water_distance_formatted) as water_distance_formatted,
                    AVG(MatchConfidence) as avg_match_confidence
                FROM {pesticide_table}
                GROUP BY field_uuid
                """

            await asyncio.to_thread(self.conn.execute, summary_query)

            # DEBUG: Check what was created in the pesticide summary
            summary_count = await asyncio.to_thread(
                self.conn.execute, "SELECT COUNT(*) FROM temp_pesticide_summary"
            )
            summary_count_result = summary_count.fetchone()[0]
            logger.info(f"DEBUG: Created pesticide summary with {summary_count_result:,} records")

            # DEBUG: Check if key fields exist in summary
            try:
                sample_result = await asyncio.to_thread(
                    self.conn.execute,
                    "SELECT unique_pesticide_products, total_pesticide_belastning "
                    "FROM temp_pesticide_summary LIMIT 1",
                )
                sample = sample_result.fetchone()
                logger.info(f"DEBUG: Summary - products: {sample[0]}, belastning: {sample[1]}")
            except Exception as e:
                logger.error(f"DEBUG: Failed to read summary fields: {e}")

            # Get columns from integrated table to avoid wildcard collision
            integrated_columns = await asyncio.to_thread(
                self.conn.execute, f"DESCRIBE {integrated_table}"
            )
            integrated_col_names = [row[0] for row in integrated_columns.fetchall()]

            logger.info(f"DEBUG: Integrated table has {len(integrated_col_names)} columns")
            logger.info(f"DEBUG: Column names: {integrated_col_names}")

            # Exclude any pesticide columns that might already exist (shouldn't, but defensive)
            pesticide_col_patterns = ["pesticide", "belastning", "pfas", "diquat", "glyphosate"]
            excluded_cols = [
                col
                for col in integrated_col_names
                if any(pattern in col.lower() for pattern in pesticide_col_patterns)
            ]
            if excluded_cols:
                logger.warning(
                    f"Excluding {len(excluded_cols)} pesticide columns "
                    f"from integrated table: {excluded_cols}"
                )

            integrated_col_list = ", ".join(
                [
                    f"i.{col}"
                    for col in integrated_col_names
                    if not any(pattern in col.lower() for pattern in pesticide_col_patterns)
                ]
            )

            # Add pesticide columns to integrated table
            update_query = f"""
            CREATE OR REPLACE TABLE {integrated_table}_updated AS
            SELECT
                {integrated_col_list},
                ps.pesticide_applications,
                ps.pesticides_used,

                -- New categorized product details
                ps.pfas_products_detail,
                ps.pfas_applications,
                ps.diquat_products_detail,
                ps.diquat_applications,
                ps.glyphosate_products_detail,
                ps.glyphosate_applications,
                ps.other_products_detail,
                ps.other_applications,

                -- Legacy unit-based details
                ps.pesticides_kg_detail,
                ps.pesticides_liters_detail,
                ps.pesticides_grams_detail,
                ps.pesticides_ml_detail,
                ps.pesticides_tablets_detail,

                -- BMD burden calculations (MISSING FIELDS!)
                ps.total_pesticide_belastning,
                ps.total_pfas_belastning,
                ps.total_diquat_belastning,
                ps.total_glyphosate_belastning,

                -- Active ingredient calculations
                ps.total_pfas_active_ingredient_kg,
                ps.total_glyphosate_active_ingredient_kg,

                -- Product count (MISSING FIELD!)
                ps.unique_pesticide_products,

                -- Total dosage by unit (frontend expects these)
                ps.total_dosage_kg,
                ps.total_dosage_liters,
                ps.total_dosage_grams,
                ps.total_dosage_ml,
                ps.total_dosage_tablets,


                -- Proximity data
                ps.residential_buildings_formatted,
                ps.educational_facilities_formatted,
                ps.water_distance_formatted,
                ps.avg_match_confidence
            FROM {integrated_table} i
            LEFT JOIN temp_pesticide_summary ps ON i.field_uuid = ps.field_uuid
            """

            await asyncio.to_thread(self.conn.execute, update_query)

            # DEBUG: Check if integration worked
            try:
                integration_check = await asyncio.to_thread(
                    self.conn.execute,
                    f"SELECT COUNT(*) FROM {integrated_table}_updated "
                    f"WHERE unique_pesticide_products IS NOT NULL",
                )
                integrated_count = integration_check.fetchone()[0]
                logger.info(f"DEBUG: Integration - {integrated_count:,} records have products")

                # Check a sample
                sample_integration = await asyncio.to_thread(
                    self.conn.execute,
                    f"SELECT field_uuid, unique_pesticide_products, total_pesticide_belastning "
                    f"FROM {integrated_table}_updated WHERE unique_pesticide_products > 0 LIMIT 1",
                )
                sample = sample_integration.fetchone()
                if sample:
                    logger.info(f"DEBUG: Sample data - field: {sample[0]}, products: {sample[1]}")
                else:
                    logger.warning("DEBUG: No records found with pesticide data after integration!")
            except Exception as e:
                logger.error(f"DEBUG: Failed to check integration: {e}")

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

    async def _enhance_pesticide_with_bmd(self, pesticide_table: str) -> str:
        """Enhance pesticide data with BMD classifications for PFAS/Diquat/Glyphosate detection.

        Args:
            pesticide_table: Name of the pesticide proximity table

        Returns:
            Name of the enhanced table with BMD data (always succeeds with fallback)
        """
        enhanced_table = f"{pesticide_table}_enhanced"

        try:
            # Try to load BMD data
            bmd_table = await self._load_bmd_data()

            # Always create enhanced table, with or without BMD data
            if bmd_table:
                logger.info("Enhancing pesticide data with BMD classifications and risk data")
                # Join with BMD data for classifications and risk information
                # Get pesticide table columns to avoid wildcard issues
                pest_columns = await asyncio.to_thread(
                    self.conn.execute, f"DESCRIBE {pesticide_table}"
                )
                pest_col_names = [row[0] for row in pest_columns.fetchall()]

                # Build explicit column list (exclude samlet_belastning column)
                pest_col_list = ", ".join(
                    [f"p.{col}" for col in pest_col_names if col != "samlet_belastning"]
                )

                await asyncio.to_thread(
                    self.conn.execute,
                    f"""
                    CREATE OR REPLACE TABLE {enhanced_table} AS
                    SELECT
                        {pest_col_list},
                        COALESCE(b.contains_pfas, false) as contains_pfas,
                        COALESCE(b.contains_diquat, false) as contains_diquat,
                        COALESCE(b.contains_glyphosate, false) as contains_glyphosate,
                        b.farebetegnelse_sundhed as health_risk,
                        b.farebetegnelse_miljø as environmental_risk,
                        b.signalord as signal_word,
                        b.produktgruppe_pesticid as product_group,
                        -- BMD burden calculation fields
                        COALESCE(b.samlet_belastning, 0.0) as samlet_belastning,
                        COALESCE(b.belastning_sundhed, 0.0) as belastning_sundhed,
                        COALESCE(b.belastning_miljøeffekt, 0.0) as belastning_miljøeffekt,
                        COALESCE(b.belastning_miljøadfærd, 0.0) as belastning_miljøadfærd
                    FROM {pesticide_table} p
                    LEFT JOIN {bmd_table} b ON p.PesticideRegistrationNumber = b.registrerings_nr
                    """,
                )

                # Log enhancement statistics (simplified)
                total_count = await asyncio.to_thread(
                    self.conn.execute, f"SELECT COUNT(*) FROM {enhanced_table}"
                )
                logger.info(
                    f"Enhanced pesticide data: {total_count.fetchone()[0]:,} total records "
                    "with BMD classifications"
                )
            else:
                logger.warning(
                    "BMD data not available, creating enhanced table without classifications"
                )
                # Create enhanced table without BMD data (all classifications will be false)
                await asyncio.to_thread(
                    self.conn.execute,
                    f"""
                    CREATE OR REPLACE TABLE {enhanced_table} AS
                    SELECT *,
                        false as contains_pfas,
                        false as contains_diquat,
                        false as contains_glyphosate,
                        NULL as health_risk,
                        NULL as environmental_risk,
                        NULL as signal_word,
                        NULL as product_group,
                        -- BMD burden calculation fields (fallback)
                        0 as samlet_belastning,
                        0 as belastning_sundhed,
                        0 as belastning_miljøeffekt,
                        0 as belastning_miljøadfærd
                    FROM {pesticide_table}
                    """,
                )

                total_count = await asyncio.to_thread(
                    self.conn.execute, f"SELECT COUNT(*) FROM {enhanced_table}"
                )
                logger.info(
                    f"Enhanced pesticide data: {total_count.fetchone()[0]:,} total records "
                    "without BMD classifications"
                )

            return enhanced_table

        except Exception as e:
            logger.error(f"Error enhancing pesticide data with BMD: {e}")
            # Create a simple enhanced table as fallback
            try:
                await asyncio.to_thread(
                    self.conn.execute,
                    f"""
                    CREATE OR REPLACE TABLE {enhanced_table} AS
                    SELECT *,
                        false as contains_pfas,
                        false as contains_diquat,
                        false as contains_glyphosate,
                        NULL as health_risk,
                        NULL as environmental_risk,
                        NULL as signal_word,
                        NULL as product_group,
                        -- BMD burden calculation fields (fallback)
                        0 as samlet_belastning,
                        0 as belastning_sundhed,
                        0 as belastning_miljøeffekt,
                        0 as belastning_miljøadfærd
                    FROM {pesticide_table}
                    """,
                )
                logger.info("Created fallback enhanced table without BMD data")
                return enhanced_table
            except Exception as fallback_error:
                logger.error(f"Fallback enhancement also failed: {fallback_error}")
                # Ultimate fallback - return original table
                return pesticide_table

    async def _load_bmd_data(self) -> str | None:
        """Load BMD pesticide product database.

        Falls back to older timestamps if the latest is unreadable.

        Returns:
            DuckDB table name or None if not available
        """
        base_path = f"r2://{self.config.storage_bucket}/silver/bmd"
        storage_paths = await self._find_timestamped_paths_ranked(base_path)
        if not storage_paths:
            logger.warning(f"BMD data not found in: {base_path}")
            return None

        table_name = "bmd_data"

        for i, storage_path in enumerate(storage_paths):
            try:
                query = f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT *
                FROM read_parquet('{storage_path}pesticide_products.parquet')
                WHERE registrerings_nr IS NOT NULL
                """
                await asyncio.to_thread(self.conn.execute, query)

                count_result = await asyncio.to_thread(
                    self.conn.execute, f"SELECT COUNT(*) FROM {table_name}"
                )
                count = count_result.fetchone()[0]

                logger.info(f"Loaded {count:,} BMD pesticide product records")
                return table_name

            except Exception as e:
                if i < len(storage_paths) - 1:
                    logger.warning(
                        f"Failed to load BMD data from {storage_path}, trying older timestamp: {e}"
                    )
                else:
                    logger.warning(f"Could not load BMD data: {e}")

        return None

    async def load_environmental_layers(self) -> dict[str, str]:
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

    async def _find_timestamped_paths_ranked(
        self, base_path: str, max_results: int = 3
    ) -> list[str]:
        """Find timestamped directories ranked by recency (newest first).

        Args:
            base_path: Base storage path like r2://bucket/silver/dataset/
            max_results: Maximum number of paths to return

        Returns:
            List of directory paths sorted newest-first, or empty list
        """
        try:
            # Ensure base_path ends with / for proper pattern matching
            if not base_path.endswith("/"):
                base_path += "/"

            # Try different file patterns based on the dataset type
            patterns_to_try = [
                f"{base_path}*/data.parquet",  # Standard pattern (FVM marker, etc.)
                f"{base_path}*/joined_buildings.parquet",  # BBR buildings pattern
                f"{base_path}*/*.parquet",  # Any parquet file pattern
            ]

            files_with_timestamps = []

            for pattern in patterns_to_try:
                try:
                    # Get files with timestamps using the standard cloud storage utility
                    pattern_files = await asyncio.to_thread(
                        self.storage.list_files_with_timestamps, pattern
                    )
                    files_with_timestamps.extend(pattern_files)
                except Exception:
                    continue  # Try next pattern

            if not files_with_timestamps:
                return []

            # Sort by timestamp (most recent first)
            files_with_timestamps.sort(key=lambda x: x[1], reverse=True)

            # Extract unique directory paths, preserving order
            # Add r2:// prefix since DuckDB needs r2:// for R2 storage access
            seen = set()
            dir_paths = []
            for file_path, _ in files_with_timestamps:
                dir_path = "/".join(file_path.split("/")[:-1]) + "/"
                if not dir_path.startswith(("r2://", "s3://")):
                    dir_path = "r2://" + dir_path
                if dir_path not in seen:
                    seen.add(dir_path)
                    dir_paths.append(dir_path)
                    if len(dir_paths) >= max_results:
                        break

            if dir_paths:
                logger.info(f"Found {len(dir_paths)} timestamped directory(ies) for {base_path}")
            return dir_paths

        except Exception as e:
            logger.error(f"Error finding timestamped paths for {base_path}: {e}")
            return []

    async def _find_latest_timestamped_path(self, base_path: str) -> str | None:
        """Find the latest timestamped directory in a base path.

        Args:
            base_path: Base storage path like r2://bucket/silver/dataset/

        Returns:
            Full path to latest timestamped directory, or None if not found
        """
        paths = await self._find_timestamped_paths_ranked(base_path, max_results=1)
        return paths[0] if paths else None

    async def _load_bnbo_status(self) -> str | None:
        """Load BNBO status dissolved data."""
        base_path = f"r2://{self.config.storage_bucket}/{self.config.bnbo_status_path}"
        storage_paths = await self._find_timestamped_paths_ranked(base_path)
        if not storage_paths:
            logger.warning(f"BNBO status data not found in: {base_path}")
            return None

        table_name = "bnbo_status_dissolved"

        for i, storage_path in enumerate(storage_paths):
            try:
                logger.info(f"Loading BNBO status data from {storage_path}")
                query = f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT *
                FROM read_parquet('{storage_path}*.parquet')
                """
                await asyncio.to_thread(self.conn.execute, query)

                count_result = await asyncio.to_thread(
                    self.conn.execute, f"SELECT COUNT(*) FROM {table_name}"
                )
                count = count_result.fetchone()[0]

                logger.info(f"Loaded {count:,} BNBO status records")
                return table_name

            except Exception as e:
                if i < len(storage_paths) - 1:
                    logger.warning(
                        f"Failed to load BNBO status from {storage_path}, trying older timestamp: {e}"
                    )
                else:
                    logger.error(f"Error loading BNBO status data: {e}")

        return None

    async def _load_wetlands(self) -> str | None:
        """Load wetlands dissolved data."""
        base_path = f"r2://{self.config.storage_bucket}/{self.config.wetlands_path}"
        storage_paths = await self._find_timestamped_paths_ranked(base_path)
        if not storage_paths:
            logger.warning(f"Wetlands data not found in: {base_path}")
            return None

        table_name = "wetlands_dissolved"

        for i, storage_path in enumerate(storage_paths):
            try:
                logger.info(f"Loading wetlands data from {storage_path}")
                query = f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT *
                FROM read_parquet('{storage_path}*.parquet')
                """
                await asyncio.to_thread(self.conn.execute, query)

                count_result = await asyncio.to_thread(
                    self.conn.execute, f"SELECT COUNT(*) FROM {table_name}"
                )
                count = count_result.fetchone()[0]

                logger.info(f"Loaded {count:,} wetlands records")
                return table_name

            except Exception as e:
                if i < len(storage_paths) - 1:
                    logger.warning(
                        f"Failed to load wetlands from {storage_path}, trying older timestamp: {e}"
                    )
                else:
                    logger.error(f"Error loading wetlands data: {e}")

        return None

    async def _load_water_projects(self) -> str | None:
        """Load water projects dissolved data."""
        base_path = f"r2://{self.config.storage_bucket}/{self.config.water_projects_path}"
        storage_paths = await self._find_timestamped_paths_ranked(base_path)
        if not storage_paths:
            logger.warning(f"Water projects data not found in: {base_path}")
            return None

        table_name = "water_projects_dissolved"

        for i, storage_path in enumerate(storage_paths):
            try:
                logger.info(f"Loading water projects data from {storage_path}")
                query = f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT *
                FROM read_parquet('{storage_path}*.parquet')
                """
                await asyncio.to_thread(self.conn.execute, query)

                count_result = await asyncio.to_thread(
                    self.conn.execute, f"SELECT COUNT(*) FROM {table_name}"
                )
                count = count_result.fetchone()[0]

                logger.info(f"Loaded {count:,} water projects records")
                return table_name

            except Exception as e:
                if i < len(storage_paths) - 1:
                    logger.warning(
                        f"Failed to load water projects from {storage_path}, "
                        f"trying older timestamp: {e}"
                    )
                else:
                    logger.error(f"Error loading water projects data: {e}")

        return None

    async def _load_bbr_buildings(self) -> str | None:
        """Load BBR buildings data."""
        base_path = f"r2://{self.config.storage_bucket}/{self.config.bbr_buildings_path}"
        storage_paths = await self._find_timestamped_paths_ranked(base_path)
        if not storage_paths:
            logger.warning(f"BBR buildings data not found in: {base_path}")
            return None

        table_name = "bbr_buildings"

        for i, storage_path in enumerate(storage_paths):
            try:
                logger.info(f"Loading BBR buildings data from {storage_path}")
                query = f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT *
                FROM read_parquet('{storage_path}joined_buildings.parquet')
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
                if i < len(storage_paths) - 1:
                    logger.warning(
                        f"Failed to load BBR buildings from {storage_path}, trying older timestamp: {e}"
                    )
                else:
                    logger.error(f"Error loading BBR buildings data: {e}")

        return None
