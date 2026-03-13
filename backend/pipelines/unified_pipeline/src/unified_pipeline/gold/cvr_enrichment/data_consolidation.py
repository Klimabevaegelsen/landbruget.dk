"""
Data Consolidation Step - Bridge between Silver and Gold layers

This step reads the structured data from Silver layer (companies, persons, employment)
and consolidates it into the format expected by downstream Gold layer steps.
This ensures backward compatibility while enabling the new Bronze→Silver→Gold architecture.

Key Features:
- Reads structured data from Silver layer
- Consolidates into Gold layer format with company_data_json
- Maintains compatibility with existing downstream steps
- Applies final business logic and validation
"""

import os
from typing import Any, ClassVar

from pydantic import Field

from unified_pipeline.common.base import BaseJobConfig, BaseSource, GoldJobInterface
from unified_pipeline.util.timing import timed

from .shared.config import CVREnrichmentSharedConfig


class DataConsolidationConfig(BaseJobConfig):
    """Configuration for data consolidation step."""

    name: str = "CVR Data Consolidation"
    dataset: str = "cvr_enrichment"
    type: str = "data_consolidation"
    description: str = "Consolidate Silver layer data into Gold layer format"
    frequency: str = "monthly"
    bucket: str = "landbruget-data"

    # Shared configuration
    shared_config: CVREnrichmentSharedConfig = Field(
        default_factory=CVREnrichmentSharedConfig,
        description="Shared configuration for CVR enrichment pipeline",
    )

    # Processing configuration
    consolidation_batch_size: int = Field(
        default=1000,
        description="Number of companies to process per consolidation batch",
    )

    # Output configuration
    include_raw_json: bool = Field(
        default=True,
        description="Whether to include original raw JSON data in consolidated output",
    )

    model_config: ClassVar[dict[str, bool]] = {"frozen": True}

    def apply_cli_filters(self, cli_config):
        """Apply CLI configuration filters to this config."""
        if cli_config.test_limit is not None:
            object.__setattr__(
                self,
                "shared_config",
                self.shared_config.model_copy(update={"test_limit": cli_config.test_limit}),
            )


class DataConsolidation(BaseSource[DataConsolidationConfig], GoldJobInterface):
    """
    Data consolidation step implementation.

    This step:
    1. Loads structured data from Silver layer (companies, persons, employment)
    2. Consolidates into Gold layer format with company_data_json
    3. Applies final validation and business logic
    4. Saves consolidated data for downstream steps
    """

    def __init__(self, config: DataConsolidationConfig):
        """
        Initialize data consolidation step.

        Args:
            config: Configuration for data consolidation
        """
        super().__init__(config)

        self.log.info("Data consolidation step initialized")
        self.log.info("📋 Configuration:")
        self.log.info(f"   • Consolidation batch size: {self.config.consolidation_batch_size}")
        self.log.info(f"   • Include raw JSON: {self.config.include_raw_json}")
        if self.config.shared_config.test_limit:
            self.log.info(f"   • Test limit: {self.config.shared_config.test_limit} companies")

    @timed(name="Data consolidation processing")
    async def run(self, silver_data: dict[str, Any] | None = None) -> str:
        """
        Run the data consolidation process.

        Args:
            silver_data: Optional silver data (structured tables)

        Returns:
            Table name containing consolidated data
        """
        self.log.info("Starting data consolidation step")

        try:
            # Step 1: Load Silver layer structured data
            self.log.info("📋 Step 1/4: Loading Silver layer structured data")
            if silver_data:
                self.log.info("Using silver data passed from previous step")
                silver_tables = silver_data
            else:
                self.log.info("Loading silver data from GCS")
                silver_tables = self._load_silver_data()

            # Step 2: Consolidate into Gold layer format
            self.log.info("🔄 Step 2/4: Consolidating data into Gold layer format")
            consolidated_table = self._consolidate_data(silver_tables)

            # Step 3: Apply final validation and business logic
            self.log.info("✅ Step 3/4: Applying final validation and business logic")
            validated_table = self._apply_final_validation(consolidated_table)

            # Step 4: Save consolidated data
            self.log.info("💾 Step 4/4: Saving consolidated data")
            final_table_name = self._save_consolidated_data(validated_table)

            # Success summary
            self.log.info("=" * 60)
            self.log.info("✅ DATA CONSOLIDATION COMPLETED SUCCESSFULLY")
            self.log.info("=" * 60)

            return final_table_name

        except Exception as e:
            self.log.error("=" * 60)
            self.log.error("❌ DATA CONSOLIDATION FAILED")
            self.log.error("=" * 60)
            self.log.error(f"Error: {e!s}")
            self.log.error("=" * 60)
            raise

    def _find_latest_silver_file(self, table_type: str) -> str:
        """Find the latest Silver layer file for a given table type."""
        # Try current date pattern first
        current_path = (
            f"r2://{self.config.bucket}/silver/{table_type}/{self.date_pattern}/data.parquet"
        )

        try:
            # Quick check if current path exists by attempting to read metadata
            self.conn.execute(f"SELECT COUNT(*) FROM read_parquet('{current_path}') LIMIT 1")
            return current_path
        except Exception:
            pass

        # Fallback: try to find any recent file
        # This is a simplified approach - in production, you might want more sophisticated logic
        self.log.warning(f"Could not find Silver layer data at {current_path}, using fallback path")
        return current_path

    @timed(name="Loading Silver layer data")
    def _load_silver_data(self) -> dict[str, str]:
        """
        Load structured data from Silver layer.

        Returns:
            Dictionary containing table names for companies, persons, employment
        """
        silver_tables = {}

        # Load companies table - try to find latest Silver layer data
        companies_path = self._find_latest_silver_file("cvr_companies")
        companies_table = "silver_companies"
        try:
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE {companies_table} AS
                SELECT * FROM read_parquet('{companies_path}')
            """)
            count = self.conn.execute(f"SELECT COUNT(*) FROM {companies_table}").fetchone()[0]
            self.log.info(f"✅ Loaded {count:,} companies from Silver layer")
            silver_tables["companies"] = companies_table
        except Exception as e:
            self.log.warning(f"Failed to load companies from Silver layer: {e}")
            silver_tables["companies"] = None

        # Load persons table
        persons_path = self._find_latest_silver_file("cvr_persons")
        persons_table = "silver_persons"
        try:
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE {persons_table} AS
                SELECT * FROM read_parquet('{persons_path}')
            """)
            count = self.conn.execute(f"SELECT COUNT(*) FROM {persons_table}").fetchone()[0]
            self.log.info(f"✅ Loaded {count:,} persons from Silver layer")
            silver_tables["persons"] = persons_table
        except Exception as e:
            self.log.warning(f"Failed to load persons from Silver layer: {e}")
            silver_tables["persons"] = None

        # Load employment table
        employment_path = self._find_latest_silver_file("cvr_employment")
        employment_table = "silver_employment"
        try:
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE {employment_table} AS
                SELECT * FROM read_parquet('{employment_path}')
            """)
            count = self.conn.execute(f"SELECT COUNT(*) FROM {employment_table}").fetchone()[0]
            self.log.info(f"✅ Loaded {count:,} employment records from Silver layer")
            silver_tables["employment"] = employment_table
        except Exception as e:
            self.log.warning(f"Failed to load employment from Silver layer: {e}")
            silver_tables["employment"] = None

        return silver_tables

    @timed(name="Consolidating data")
    def _consolidate_data(self, silver_tables: dict[str, str]) -> str:
        """
        Consolidate Silver layer data into Gold layer format.

        Args:
            silver_tables: Dictionary of Silver layer table names

        Returns:
            Name of consolidated table
        """
        consolidated_table = "cvr_companies_consolidated"
        companies_table = silver_tables.get("companies")

        if not companies_table:
            raise ValueError("Companies table is required for consolidation")

        self.log.info("Consolidating Silver layer data into Gold layer format...")

        # Create consolidated table with company_data_json for backward compatibility
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE {consolidated_table} AS
            SELECT
                c.cvr_number,
                c.company_uuid,
                c.company_name,
                c.company_type_description,
                c.status,
                c.founded_date,
                c.dissolution_date,
                c.advertisement_protection,
                c.pnumber_count,
                -- Address fields (to be populated by Address Geocoding step)
                c.current_full_address,
                c.current_street_name,
                c.current_house_number,
                c.current_floor,
                c.current_door,
                c.current_postal_code,
                c.current_city,
                c.current_municipality_code,
                c.current_municipality_name,
                c.current_address_type,
                c.latitude,
                c.longitude,
                c.coordinate_quality,
                c.coordinate_source,
                c.dawa_enriched,
                -- Industry information
                c.primary_industry_code,
                c.primary_industry_description,
                c.is_agricultural_company,
                -- Consolidated JSON data for backward compatibility
                CASE
                    WHEN c.company_data_json IS NOT NULL THEN c.company_data_json
                    ELSE json_object(
                        'cvr_number', c.cvr_number,
                        'company_name', c.company_name,
                        'company_type_description', c.company_type_description,
                        'status', c.status,
                        'founded_date', c.founded_date,
                        'dissolution_date', c.dissolution_date,
                        'advertisement_protection', c.advertisement_protection,
                        'pnumber_count', c.pnumber_count,
                        'primary_industry_code', c.primary_industry_code,
                        'primary_industry_description', c.primary_industry_description,
                        'is_agricultural_company', c.is_agricultural_company,
                        'processing_timestamp', c.processing_timestamp
                    )
                END as company_data_json,
                c.processing_timestamp
            FROM {companies_table} c
        """)

        # Get count and log result
        count = self.conn.execute(f"SELECT COUNT(*) FROM {consolidated_table}").fetchone()[0]
        agricultural_count = self.conn.execute(
            f"SELECT COUNT(*) FROM {consolidated_table} WHERE is_agricultural_company = true"
        ).fetchone()[0]

        self.log.info(f"✅ Consolidated {count:,} companies ({agricultural_count:,} agricultural)")

        return consolidated_table

    @timed(name="Applying final validation")
    def _apply_final_validation(self, consolidated_table: str) -> str:
        """
        Apply final validation and business logic.

        Args:
            consolidated_table: Name of consolidated table

        Returns:
            Name of validated table
        """
        validated_table = "cvr_companies_validated"

        self.log.info("Applying final validation and business logic...")

        # Apply test limit if configured
        limit_clause = ""
        if self.config.shared_config.test_limit:
            limit_clause = f"LIMIT {self.config.shared_config.test_limit}"
            self.log.info(
                f"🧪 Applying test limit: {self.config.shared_config.test_limit} companies"
            )

        # Create validated table with final business rules
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE {validated_table} AS
            SELECT
                *,
                -- Add validation flags
                CASE
                    WHEN cvr_number IS NULL OR LENGTH(TRIM(CAST(cvr_number AS VARCHAR))) != 8
                         THEN false
                    WHEN NOT REGEXP_MATCHES(TRIM(CAST(cvr_number AS VARCHAR)), '^[1-9][0-9]{{7}}$')
                         THEN false
                    ELSE true
                END as cvr_number_valid,
                CASE
                    WHEN company_name IS NULL OR TRIM(company_name) = '' THEN false
                    ELSE true
                END as company_name_valid,
                CASE
                    WHEN company_uuid IS NULL THEN false
                    ELSE true
                END as uuid_valid
            FROM {consolidated_table}
            WHERE cvr_number IS NOT NULL
            AND company_name IS NOT NULL
            AND TRIM(company_name) != ''
            ORDER BY cvr_number
            {limit_clause}
        """)

        # Get validation statistics
        total_count = self.conn.execute(f"SELECT COUNT(*) FROM {validated_table}").fetchone()[0]
        valid_cvr_count = self.conn.execute(
            f"SELECT COUNT(*) FROM {validated_table} WHERE cvr_number_valid = true"
        ).fetchone()[0]
        valid_name_count = self.conn.execute(
            f"SELECT COUNT(*) FROM {validated_table} WHERE company_name_valid = true"
        ).fetchone()[0]
        valid_uuid_count = self.conn.execute(
            f"SELECT COUNT(*) FROM {validated_table} WHERE uuid_valid = true"
        ).fetchone()[0]

        self.log.info(f"✅ Validation complete: {total_count:,} companies")
        self.log.info(
            f"   • Valid CVR numbers: {valid_cvr_count:,} ({valid_cvr_count / total_count * 100:.1f}%)"
        )
        self.log.info(
            f"   • Valid company names: {valid_name_count:,} "
            f"({valid_name_count / total_count * 100:.1f}%)"
        )
        self.log.info(
            f"   • Valid UUIDs: {valid_uuid_count:,} ({valid_uuid_count / total_count * 100:.1f}%)"
        )

        return validated_table

    @timed(name="Saving consolidated data")
    def _save_consolidated_data(self, validated_table: str) -> str:
        """
        Save consolidated data to Gold layer.

        Args:
            validated_table: Name of validated table

        Returns:
            Final table name
        """
        timestamp = self.date_pattern

        # Save to Gold layer for downstream steps
        companies_path = (
            f"gs://{self.config.bucket}/gold/cvr_enrichment/{timestamp}/data_parsing.parquet"
        )
        self.gcs_access.upload_from_duckdb_table(
            validated_table,
            companies_path,
            compression="zstd",
            row_group_size=100000,
        )
        self.log.info(f"✅ Saved consolidated data to: {companies_path}")

        # Also save to legacy location for compatibility
        legacy_companies_path = (
            f"gs://{self.config.bucket}/gold/cvr_enrichment_companies/{timestamp}/data.parquet"
        )
        self.gcs_access.upload_from_duckdb_table(
            validated_table,
            legacy_companies_path,
            compression="zstd",
            row_group_size=100000,
        )
        self.log.info(f"✅ Saved legacy format to: {legacy_companies_path}")

        # Save locally for GitHub Actions artifact sharing
        if os.getenv("GITHUB_ACTIONS") == "true":
            local_companies_path = "/tmp/cvr_company_data.parquet"
            self.conn.execute(
                f"COPY {validated_table} TO '{local_companies_path}' "
                f"(FORMAT 'parquet', COMPRESSION 'zstd')"
            )
            self.log.info(f"💾 Saved companies to artifact: {local_companies_path}")

        self.log.info("✅ All consolidated data saved successfully")
        return validated_table
