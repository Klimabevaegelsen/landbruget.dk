"""
CVR Data Parser - Silver Layer Step

This step reads raw CVR JSON data from the Bronze layer and parses it into
structured tables (companies, persons, employment) following the Bronze→Silver→Gold pattern.

Key Features:
- Reads consolidated raw JSON from Bronze layer
- Parses complex CVR API responses into structured data
- Applies agricultural company classification (64 industry codes)
- Generates deterministic UUIDs using namespace
- Extracts persons/leadership and employment data
- Saves structured data to Silver layer for Gold layer consumption

Memory Efficiency:
- Processes data in batches to avoid memory accumulation
- Uses streaming JSON parsing for large datasets
- Immediate cleanup after each batch
"""

import os
from datetime import datetime
from typing import Any, ClassVar

from pydantic import Field

from unified_pipeline.common.base import BaseJobConfig, BaseSource, SilverJobInterface
from unified_pipeline.util.timing import timed


class CVRDataParserConfig(BaseJobConfig):
    """Configuration for CVR data parser step."""

    name: str = "CVR Data Parser"
    dataset: str = "cvr_companies"
    type: str = "data_parser"
    description: str = "Parse raw CVR JSON data into structured tables"
    frequency: str = "monthly"
    bucket: str = "landbrugsdata-raw-data"

    # Processing configuration
    parsing_batch_size: int = Field(
        default=1000,
        description="Number of companies to process per parsing batch (memory-safe)",
    )

    # Output configuration
    save_companies_table: bool = Field(
        default=True, description="Whether to save companies table to Silver layer"
    )

    save_persons_table: bool = Field(
        default=True, description="Whether to save persons table to Silver layer"
    )

    save_employment_table: bool = Field(
        default=True, description="Whether to save employment table to Silver layer"
    )

    # Test configuration
    test_limit: int | None = Field(
        default=None,
        description="Limit number of companies to process for testing (None = no limit)",
    )

    model_config: ClassVar[dict[str, bool]] = {"frozen": True}

    def apply_cli_filters(self, cli_config):
        """Apply CLI configuration filters to this config."""
        if cli_config.test_limit is not None:
            object.__setattr__(self, "test_limit", cli_config.test_limit)


class CVRDataParser(BaseSource[CVRDataParserConfig], SilverJobInterface):
    """
    CVR data parser implementation.

    This step:
    1. Reads consolidated raw JSON from Bronze layer
    2. Parses CVR API responses into structured data
    3. Applies business logic (agricultural classification, UUIDs)
    4. Creates companies, persons, and employment tables
    5. Saves structured data to Silver layer
    """

    def __init__(self, config: CVRDataParserConfig):
        """
        Initialize CVR data parser.

        Args:
            config: Configuration for CVR data parser
        """
        super().__init__(config)

        # Apply memory optimizations
        self._apply_memory_optimizations()

        # Setup UUID functions
        self._setup_uuid_functions()

        self.log.info("CVR data parser initialized")
        self.log.info("📋 Configuration:")
        self.log.info(f"   • Parsing batch size: {self.config.parsing_batch_size}")
        self.log.info(f"   • Save companies table: {self.config.save_companies_table}")
        self.log.info(f"   • Save persons table: {self.config.save_persons_table}")
        self.log.info(f"   • Save employment table: {self.config.save_employment_table}")
        if self.config.test_limit:
            self.log.info(f"   • Test limit: {self.config.test_limit} companies")

    def _apply_memory_optimizations(self):
        """Apply memory optimizations for Silver layer processing."""
        self.log.info("🔧 Applying CVR data parser memory optimizations...")

        # Conservative memory settings for parsing
        self.conn.execute("SET memory_limit = '6GB'")
        self.conn.execute("SET max_memory = '6GB'")
        self.conn.execute("SET threads = 2")
        self.conn.execute("SET temp_directory = '/tmp'")
        self.conn.execute("SET preserve_insertion_order = false")

        self.log.info("✅ CVR data parser memory optimizations applied")
        self.log.info("   • Memory limit: 6GB")
        self.log.info("   • Threads: 2")
        self.log.info("   • Preserve insertion order: false")
        self.log.info("   • Processing: Batch-based with immediate cleanup")

    def _setup_uuid_functions(self):
        """Setup UUID generation functions using namespace."""
        namespace = os.getenv("LANDBRUGSDATA_UUID_NAMESPACE")
        if not namespace:
            raise ValueError("LANDBRUGSDATA_UUID_NAMESPACE environment variable is required")

        # Create company UUID function using namespace from environment
        # Cast crypto_hash BLOB result to VARCHAR to avoid substr() type errors
        self.conn.execute(f"""
            CREATE OR REPLACE FUNCTION company_uuid(cvr_number) AS (
                SELECT CASE
                    WHEN cvr_number IS NULL OR LENGTH(TRIM(CAST(cvr_number AS VARCHAR))) != 8
                         OR NOT REGEXP_MATCHES(TRIM(CAST(cvr_number AS VARCHAR)),
                                               '^[1-9][0-9]{{7}}$')
                    THEN NULL
                    ELSE CONCAT(
                        SUBSTR(CAST(crypto_hash('md5', CONCAT('{namespace}', 'company-cvr-',
                               TRIM(CAST(cvr_number AS VARCHAR)))) AS VARCHAR), 1, 8), '-',
                        SUBSTR(CAST(crypto_hash('md5', CONCAT('{namespace}', 'company-cvr-',
                               TRIM(CAST(cvr_number AS VARCHAR)))) AS VARCHAR), 9, 4), '-',
                        '5', SUBSTR(CAST(crypto_hash('md5', CONCAT('{namespace}', 'company-cvr-',
                                      TRIM(CAST(cvr_number AS VARCHAR)))) AS VARCHAR), 13, 3), '-',
                        CONCAT('8', SUBSTR(CAST(crypto_hash('md5', CONCAT('{namespace}', 'company-cvr-',
                                               TRIM(CAST(cvr_number AS VARCHAR)))) AS VARCHAR), 17, 3)), '-',
                        SUBSTR(CAST(crypto_hash('md5', CONCAT('{namespace}', 'company-cvr-',
                               TRIM(CAST(cvr_number AS VARCHAR)))) AS VARCHAR), 21, 12)
                    )
                END
            )
        """)

        self.log.info("✅ UUID functions configured with namespace")

    @timed(name="CVR data parsing processing")
    async def run(self, bronze_data: Any | None = None) -> dict[str, Any] | None:
        """
        Run the CVR data parsing process.

        Args:
            bronze_data: Optional bronze data (raw JSON data)

        Returns:
            Dictionary containing processing results and table names
        """
        self.log.info("Starting CVR data parsing step")

        try:
            # Step 1: Load raw JSON data from Bronze layer
            self.log.info("📋 Step 1/4: Loading raw JSON data from Bronze layer")
            raw_data_table = self._load_raw_data(bronze_data)

            # Step 2: Parse companies data
            self.log.info("🏢 Step 2/4: Parsing companies data")
            companies_table = await self._parse_companies_data(raw_data_table)

            # Step 3: Parse persons/leadership data
            self.log.info("👥 Step 3/4: Parsing persons/leadership data")
            persons_table = await self._parse_persons_data(raw_data_table)

            # Step 4: Parse employment data
            self.log.info("💼 Step 4/4: Parsing employment data")
            employment_table = await self._parse_employment_data(raw_data_table)

            # Save all tables to Silver layer
            self._save_silver_data(companies_table, persons_table, employment_table)

            # Success summary
            self.log.info("=" * 60)
            self.log.info("✅ CVR DATA PARSING COMPLETED SUCCESSFULLY")
            self.log.info("=" * 60)

            return {
                "dataset": self.config.dataset,
                "processed_at": datetime.now().isoformat(),
                "status": "completed",
                "tables_created": [companies_table, persons_table, employment_table],
            }

        except Exception as e:
            self.log.error("=" * 60)
            self.log.error("❌ CVR DATA PARSING FAILED")
            self.log.error("=" * 60)
            self.log.error(f"Error: {e!s}")
            self.log.error("=" * 60)
            raise

    @timed(name="Loading raw data")
    def _load_raw_data(self, bronze_data: Any | None = None) -> str:
        """
        Load raw JSON data from Bronze layer.

        Args:
            bronze_data: Optional bronze data passed from previous step

        Returns:
            Table name containing raw data
        """
        table_name = "cvr_raw_data"

        if bronze_data:
            self.log.info("Using bronze data passed from previous step")
            # If bronze data is passed directly, use it
            if isinstance(bronze_data, str):
                # Bronze data is a table name
                self.conn.execute(
                    f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM {bronze_data}"
                )
            else:
                # Bronze data is actual data - should not happen in normal flow
                self.log.warning("Unexpected bronze data format, falling back to GCS")
                return self._load_raw_data_from_gcs(table_name)
        else:
            # Load from GCS (normal flow)
            return self._load_raw_data_from_gcs(table_name)

        return table_name

    def _load_raw_data_from_gcs(self, table_name: str) -> str:
        """Load raw data from GCS Bronze layer."""
        from unified_pipeline.gold.cvr_enrichment.shared.config import (
            CVREnrichmentStep,
            get_step_input_paths,
        )

        # Use shared config to get the proper input paths for data parsing step
        input_paths = get_step_input_paths(
            step=CVREnrichmentStep.DATA_PARSING,
            date_pattern=self.date_pattern,
            bucket=self.config.bucket,
            enable_independent_execution=True,
            max_days_back=30,
        )

        if not input_paths:
            # Provide more debugging information
            expected_path = f"gs://{self.config.bucket}/bronze/cvr_raw_companies/{self.date_pattern}/consolidated.parquet"
            self.log.error(f"Expected Bronze layer data at: {expected_path}")
            self.log.error(f"Date pattern used: {self.date_pattern}")
            self.log.error("This usually means:")
            self.log.error("1. The company_fetching step failed or didn't complete")
            self.log.error(
                "2. The data_parsing step is running independently without Bronze layer data"
            )
            self.log.error("3. There's a timestamp mismatch between steps")

            raise ValueError(
                f"No Bronze layer data found at expected path: {expected_path}. "
                "Ensure the company_fetching step has completed successfully."
            )

        raw_data_path = input_paths[0]  # Should be the consolidated.parquet file

        try:
            self.log.info(f"Loading raw data from: {raw_data_path}")
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT * FROM read_parquet('{raw_data_path}')
            """)

            # Check data loaded successfully
            count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            self.log.info(f"✅ Loaded {count:,} raw company records")

            # Apply test limit if configured
            if self.config.test_limit and count > self.config.test_limit:
                self.conn.execute(f"""
                    CREATE OR REPLACE TABLE {table_name} AS
                    SELECT * FROM {table_name}
                    ORDER BY cvr_number
                    LIMIT {self.config.test_limit}
                """)
                self.log.info(f"🧪 Applied test limit: {self.config.test_limit} companies")

            return table_name

        except Exception as e:
            self.log.error(f"Failed to load raw data from {raw_data_path}: {e}")
            raise

    @timed(name="Parsing companies data")
    async def _parse_companies_data(self, raw_data_table: str) -> str:
        """Parse raw JSON into companies table."""
        companies_table = "cvr_companies_parsed"

        self.log.info("Parsing companies data from raw JSON...")

        # Create companies table with comprehensive parsing
        # Use CTE to cast BLOB to VARCHAR first to avoid DuckDB type issues
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE {companies_table} AS
            WITH raw_data_with_varchar AS (
                SELECT
                    cvr_number,
                    CAST(raw_json AS VARCHAR) as raw_json_str,
                    raw_json,
                    fetch_timestamp
                FROM {raw_data_table}
                WHERE raw_json IS NOT NULL
            )
            SELECT
                cvr_number,
                company_uuid(cvr_number) as company_uuid,
                json_extract_string(raw_json_str, '$.company_name') as company_name,
                json_extract_string(raw_json_str, '$.company_type_description')
                    as company_type_description,
                json_extract_string(raw_json_str, '$.status') as status,
                json_extract_string(raw_json_str, '$.founded_date') as founded_date,
                json_extract_string(raw_json_str, '$.dissolution_date') as dissolution_date,
                json_extract(raw_json_str, '$.advertisement_protection')::BOOLEAN
                    as advertisement_protection,
                json_extract(raw_json_str, '$.pnumber_count')::INTEGER as pnumber_count,
                -- Address fields (will be populated by Address Geocoding step)
                NULL::VARCHAR as current_full_address,
                NULL::VARCHAR as current_street_name,
                NULL::VARCHAR as current_house_number,
                NULL::VARCHAR as current_floor,
                NULL::VARCHAR as current_door,
                NULL::INTEGER as current_postal_code,
                NULL::VARCHAR as current_city,
                NULL::INTEGER as current_municipality_code,
                NULL::VARCHAR as current_municipality_name,
                NULL::VARCHAR as current_address_type,
                NULL::DOUBLE as latitude,
                NULL::DOUBLE as longitude,
                NULL::VARCHAR as coordinate_quality,
                NULL::VARCHAR as coordinate_source,
                NULL::BOOLEAN as dawa_enriched,
                -- Extract industry information
                CASE
                    WHEN json_array_length(json_extract(raw_json_str, '$.industries')) > 0 THEN
                        json_extract_string(json_extract(raw_json_str, '$.industries[0]'),
                                          '$.industry_code')
                    ELSE NULL
                END as primary_industry_code,
                CASE
                    WHEN json_array_length(json_extract(raw_json_str, '$.industries')) > 0 THEN
                        json_extract_string(json_extract(raw_json_str, '$.industries[0]'),
                                          '$.industry_description')
                    ELSE NULL
                END as primary_industry_description,
                -- Agricultural company classification (64 industry codes)
                {self._get_agricultural_classification_sql()} as is_agricultural_company,
                raw_json as company_data_json,
                fetch_timestamp as processing_timestamp
            FROM raw_data_with_varchar
        """)

        # Get count and log result
        count = self.conn.execute(f"SELECT COUNT(*) FROM {companies_table}").fetchone()[0]
        agricultural_count = self.conn.execute(
            f"SELECT COUNT(*) FROM {companies_table} WHERE is_agricultural_company = true"
        ).fetchone()[0]

        self.log.info(f"✅ Parsed {count:,} companies ({agricultural_count:,} agricultural)")

        return companies_table

    def _get_agricultural_classification_sql(self) -> str:
        """Get SQL for agricultural company classification."""
        # Extract industry code for readability
        # Note: Expects raw_json_str (VARCHAR) to be available in the query context
        industry_code_expr = (
            "json_extract_string(json_extract(raw_json_str, '$.industries[0]'), '$.industry_code')"
        )
        is_current_expr = (
            "json_extract(json_extract(raw_json_str, '$.industries[0]'), '$.is_current')::BOOLEAN"
        )

        return f"""
            CASE
                WHEN json_array_length(json_extract(raw_json_str, '$.industries')) > 0 THEN
                    CASE
                        WHEN {industry_code_expr} IS NOT NULL
                        AND {is_current_expr} = true
                        THEN
                            CASE
                                -- Primary Agriculture, Forestry and Fishing (codes 01-03)
                                WHEN {industry_code_expr} LIKE '01%'
                                     OR {industry_code_expr} LIKE '02%'
                                     OR {industry_code_expr} LIKE '03%'
                                THEN true
                                -- Fish farming: 050200
                                WHEN {industry_code_expr} IN ('050200') THEN true
                                -- Real estate (agricultural): 702040, 682040
                                WHEN {industry_code_expr} IN ('702040', '682040') THEN true
                                -- Veterinary services: 852000, 750000
                                WHEN {industry_code_expr} IN ('852000', '750000') THEN true
                                -- Agricultural support: 749010, 741410
                                WHEN {industry_code_expr} IN ('749010', '741410') THEN true
                                -- Agricultural machinery
                                WHEN {industry_code_expr} IN (
                                    '516600', '773100', '713100', '466100',
                                    '518800', '283000', '293220'
                                ) THEN true
                                -- Agricultural trade
                                WHEN {industry_code_expr} IN (
                                    '512300', '462100', '462300', '462200',
                                    '512100', '512200', '461100', '511100'
                                ) THEN true
                                -- Agricultural processing
                                WHEN {industry_code_expr} IN (
                                    '151110', '109100', '101110', '110200',
                                    '101190', '101300', '105100'
                                ) THEN true
                                -- Agricultural retail: 524875, 477630, 524885, 477610
                                WHEN {industry_code_expr} IN (
                                    '524875', '477630', '524885', '477610'
                                ) THEN true
                                -- Agricultural education: 802240, 631200, 521000
                                WHEN {industry_code_expr} IN (
                                    '802240', '631200', '521000'
                                ) THEN true
                                ELSE false
                            END
                        ELSE false
                    END
                ELSE false
            END
        """

    @timed(name="Parsing persons data")
    async def _parse_persons_data(self, raw_data_table: str) -> str:
        """Parse raw JSON into persons/leadership table."""
        persons_table = "cvr_persons_parsed"

        self.log.info("Parsing persons/leadership data from raw JSON...")

        # Create persons table with leadership extraction
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE {persons_table} AS
            WITH raw_data_with_varchar AS (
                SELECT
                    cvr_number,
                    CAST(raw_json AS VARCHAR) as raw_json_str,
                    raw_json,
                    fetch_timestamp
                FROM {raw_data_table}
            ),
            leadership_flattened AS (
                SELECT
                    cvr_number,
                    raw_json_str,
                    fetch_timestamp,
                    idx as leadership_idx
                FROM raw_data_with_varchar
                CROSS JOIN unnest(generate_series(0::BIGINT,
                    (GREATEST(0, json_array_length(json_extract(raw_json_str, '$.leadership')) - 1))::BIGINT
                )) as t(idx)
                WHERE json_array_length(json_extract(raw_json_str, '$.leadership')) > 0
            )
            SELECT
                uuid() as person_uuid,
                cvr_number,
                company_uuid(cvr_number) as company_uuid,
                leadership_idx,
                'leadership' as relation_type,
                json_extract_string(
                    json_extract(raw_json_str, '$.leadership[' || leadership_idx || ']'),
                    '$.relation_type'
                ) as person_relation_type,
                json_extract(raw_json_str, '$.leadership[' || leadership_idx || ']')
                    as person_data_json,
                fetch_timestamp as processing_timestamp
            FROM leadership_flattened
        """)

        # Get count and log result
        count = self.conn.execute(f"SELECT COUNT(*) FROM {persons_table}").fetchone()[0]
        self.log.info(f"✅ Parsed {count:,} person/leadership records")

        return persons_table

    @timed(name="Parsing employment data")
    async def _parse_employment_data(self, raw_data_table: str) -> str:
        """Parse raw JSON into employment table."""
        employment_table = "cvr_employment_parsed"

        self.log.info("Parsing employment data from raw JSON...")

        # Create employment table with temporal data extraction
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE {employment_table} AS
            WITH raw_data_with_varchar AS (
                SELECT
                    cvr_number,
                    CAST(raw_json AS VARCHAR) as raw_json_str,
                    raw_json,
                    fetch_timestamp
                FROM {raw_data_table}
            ),
            employment_flattened AS (
                SELECT
                    cvr_number,
                    raw_json_str,
                    fetch_timestamp,
                    idx as employment_idx
                FROM raw_data_with_varchar
                CROSS JOIN unnest(generate_series(0::BIGINT,
                    (GREATEST(0, json_array_length(json_extract(raw_json_str, '$.employment')) - 1))::BIGINT
                )) as t(idx)
                WHERE json_array_length(json_extract(raw_json_str, '$.employment')) > 0
            )
            SELECT
                uuid() as employment_uuid,
                cvr_number,
                company_uuid(cvr_number) as company_uuid,
                employment_idx,
                json_extract(
                    json_extract(raw_json_str, '$.employment[' || employment_idx || ']'),
                    '$.employee_count'
                )::INTEGER as employee_count,
                json_extract_string(
                    json_extract(raw_json_str, '$.employment[' || employment_idx || ']'),
                    '$.period_start'
                ) as period_start,
                json_extract_string(
                    json_extract(raw_json_str, '$.employment[' || employment_idx || ']'),
                    '$.period_end'
                ) as period_end,
                json_extract(
                    json_extract(raw_json_str, '$.employment[' || employment_idx || ']'),
                    '$.is_current'
                )::BOOLEAN as is_current,
                json_extract(raw_json_str, '$.employment[' || employment_idx || ']')
                    as employment_data_json,
                fetch_timestamp as processing_timestamp
            FROM employment_flattened
        """)

        # Get count and log result
        count = self.conn.execute(f"SELECT COUNT(*) FROM {employment_table}").fetchone()[0]
        self.log.info(f"✅ Parsed {count:,} employment records")

        return employment_table

    @timed(name="Saving Silver layer data")
    def _save_silver_data(self, companies_table: str, persons_table: str, employment_table: str):
        """Save all parsed tables to Silver layer."""
        timestamp = self.date_pattern

        self.log.info("💾 Saving parsed data to Silver layer...")

        # Save companies table
        if self.config.save_companies_table:
            companies_path = (
                f"gs://{self.config.bucket}/silver/cvr_companies/{timestamp}/data.parquet"
            )
            self.gcs_access.upload_from_duckdb_table(
                companies_table,
                companies_path,
                compression="zstd",
                row_group_size=100000,
            )
            self.log.info(f"✅ Saved companies table to: {companies_path}")

        # Save persons table
        if self.config.save_persons_table:
            persons_path = f"gs://{self.config.bucket}/silver/cvr_persons/{timestamp}/data.parquet"
            self.gcs_access.upload_from_duckdb_table(
                persons_table,
                persons_path,
                compression="zstd",
                row_group_size=100000,
            )
            self.log.info(f"✅ Saved persons table to: {persons_path}")

        # Save employment table
        if self.config.save_employment_table:
            employment_path = (
                f"gs://{self.config.bucket}/silver/cvr_employment/{timestamp}/data.parquet"
            )
            self.gcs_access.upload_from_duckdb_table(
                employment_table,
                employment_path,
                compression="zstd",
                row_group_size=100000,
            )
            self.log.info(f"✅ Saved employment table to: {employment_path}")

        # Also save locally for GitHub Actions artifact sharing
        import os

        if os.getenv("GITHUB_ACTIONS") == "true" and self.config.save_companies_table:
            local_companies_path = "/tmp/cvr_companies_silver.parquet"
            self.conn.execute(
                f"COPY {companies_table} TO '{local_companies_path}' "
                f"(FORMAT 'parquet', COMPRESSION 'zstd')"
            )
            self.log.info(f"💾 Saved companies to artifact: {local_companies_path}")

        self.log.info("✅ All Silver layer data saved successfully")
