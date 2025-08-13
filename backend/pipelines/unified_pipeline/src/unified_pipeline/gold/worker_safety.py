"""
Worker Safety Gold Layer

This module implements the gold layer processor for worker safety/injury data.
It cleans and structures the silver layer worker safety data into a proper
normalized format with CVR, year, and injury type.

The processor handles:
- Unpivoting yearly data columns into proper temporal records
- Handling privacy-protected values (e.g., '<5')
- Clean structure: CVR, year, injury_type, injury_count
"""

import os
from typing import Any, Dict, Optional

from pydantic import ConfigDict, Field

from unified_pipeline.common.base import BaseJobConfig, BaseSource, GoldJobInterface
from unified_pipeline.util.timing import timed


class WorkerSafetyGoldConfig(BaseJobConfig):
    """Configuration for Worker Safety gold layer."""

    name: str = "Worker Safety Gold"
    dataset: str = "worker_safety"
    type: str = "gold"
    description: str = "Clean worker safety data by CVR, year, and injury type"
    frequency: str = "weekly"
    bucket: str = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")

    # Input silver datasets
    worker_safety_dataset: str = "worker safety"  # Note: space in dataset name as per GCS path

    # Processing configuration
    start_year: int = Field(default=2020, description="Start year for analysis")
    end_year: int = Field(default=2024, description="End year for analysis")

    # Handle privacy protection values - keep as range string
    privacy_value_replacement: str = Field(
        default="1-5",
        description="String to use for '<5' privacy-protected counts (preserves range info)",
    )

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class WorkerSafetyGold(BaseSource[WorkerSafetyGoldConfig], GoldJobInterface):
    """
    Gold layer processor for worker safety data.

    Cleans and structures worker safety silver data into normalized format:
    CVR, year, injury_type, injury_count
    """

    def __init__(self, config: WorkerSafetyGoldConfig):
        super().__init__(config)

        # Configure DuckDB for data processing
        self.conn.execute("SET memory_limit = '8GB'")
        self.conn.execute("SET threads = 4")
        self.conn.execute("SET temp_directory = '/tmp'")

    @timed
    async def run(self, silver_data: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, str]]:
        """Execute worker safety gold layer processing."""

        self.log.info("🚀 Starting Worker Safety Gold processing")

        try:
            # Load silver data
            await self._load_silver_data(silver_data)

            # Process and aggregate data
            await self._process_injury_data()

            # Save gold layer outputs
            output_paths = await self._save_gold_data()

            self.log.info("✅ Worker Safety Gold processing completed successfully")
            return output_paths

        except Exception as e:
            self.log.error(f"❌ Worker Safety Gold processing failed: {e}")
            raise

    async def _load_silver_data(self, silver_data: Optional[Dict[str, Any]]) -> None:
        """Load worker safety silver data from GCS."""

        if silver_data:
            self.log.warning("In-memory data passing not implemented for worker safety - using GCS")

        # Get latest silver data path
        silver_path = self._get_latest_silver_path(self.config.worker_safety_dataset)
        if not silver_path:
            raise ValueError(f"No silver data found for {self.config.worker_safety_dataset}")

        self.log.info(f"Loading worker safety data from: {silver_path}")

        # Load both parquet files using temp download pattern (like field_production.py)
        mv_path = f"{silver_path}/worker_safety_2020-2024_mv.parquet"
        skadeart_path = f"{silver_path}/worker_safety_2020-2024_skadeart.parquet"

        # Load main injury data (total by CVR and year)
        with self.gcs_access._temp_download(mv_path) as temp_mv_file:
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE worker_safety_raw AS 
                SELECT * FROM read_parquet('{temp_mv_file}')
            """)

        # Load injury type data (detailed by CVR, year, and injury type)
        with self.gcs_access._temp_download(skadeart_path) as temp_skadeart_file:
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE worker_safety_injury_types_raw AS 
                SELECT * FROM read_parquet('{temp_skadeart_file}')
            """)

        self.log.info("✅ Silver data loaded successfully")

    async def _process_injury_data(self) -> None:
        """Process and transform worker safety data into clean structured format."""

        self.log.info("📊 Processing and cleaning worker safety data...")

        # Create clean injury data table combining both data sources
        self.log.info("Creating clean worker safety data from both main and injury type data...")
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE worker_safety_clean AS
            WITH 
            -- Detailed injury type data (34 companies)
            injury_type_data AS (
                SELECT 
                    CAST(\n                        anmeldte_ulykker_i_easy_med_mere_end_en_dags_fravaer_i_branchegruppen_landbrug_jagt_skovbrug_og_fiskeri_2020_2024_fordelt_paa_cvr_nummer_og_skadeart \n                        AS BIGINT\n                    ) as cvr_number,
                    column_1 as injury_type,
                    column_2 as year_2019,
                    column_3 as year_2020,
                    column_4 as year_2021, 
                    column_5 as year_2022,
                    column_6 as year_2023,
                    column_7 as year_2024
                FROM worker_safety_injury_types_raw
                WHERE anmeldte_ulykker_i_easy_med_mere_end_en_dags_fravaer_i_branchegruppen_landbrug_jagt_skovbrug_og_fiskeri_2020_2024_fordelt_paa_cvr_nummer_og_skadeart 
                      ~ '^[0-9]+$'
                AND column_1 IS NOT NULL 
                AND column_1 != ''
                AND column_1 != 'Skadeart'
            ),
            -- Main total data (190 companies)
            main_data AS (
                SELECT 
                    CAST(
                        anmeldte_ulykker_i_easy_med_mere_end_en_dags_fravaer_i_branchegruppen_landbrug_jagt_skovbrug_og_fiskeri_2020_2024_fordelt_paa_cvr_nummer 
                        AS BIGINT
                    ) as cvr_number,
                    column_1 as year_2019,
                    column_2 as year_2020, 
                    column_3 as year_2021,
                    column_4 as year_2022,
                    column_5 as year_2023,
                    column_6 as year_2024
                FROM worker_safety_raw
                WHERE anmeldte_ulykker_i_easy_med_mere_end_en_dags_fravaer_i_branchegruppen_landbrug_jagt_skovbrug_og_fiskeri_2020_2024_fordelt_paa_cvr_nummer 
                      ~ '^[0-9]+$'
                AND anmeldte_ulykker_i_easy_med_mere_end_en_dags_fravaer_i_branchegruppen_landbrug_jagt_skovbrug_og_fiskeri_2020_2024_fordelt_paa_cvr_nummer 
                    != 'CVR-nr.'
            ),
            -- Get CVRs that have detailed injury type data
            detailed_cvrs AS (
                SELECT DISTINCT cvr_number FROM injury_type_data
            ),
            -- Unpivot detailed injury type data
            detailed_unpivoted AS (
                SELECT cvr_number, injury_type, 2019 as year, year_2019 as injury_count_raw FROM injury_type_data
                UNION ALL SELECT cvr_number, injury_type, 2020 as year, year_2020 as injury_count_raw FROM injury_type_data
                UNION ALL SELECT cvr_number, injury_type, 2021 as year, year_2021 as injury_count_raw FROM injury_type_data
                UNION ALL SELECT cvr_number, injury_type, 2022 as year, year_2022 as injury_count_raw FROM injury_type_data
                UNION ALL SELECT cvr_number, injury_type, 2023 as year, year_2023 as injury_count_raw FROM injury_type_data
                UNION ALL SELECT cvr_number, injury_type, 2024 as year, year_2024 as injury_count_raw FROM injury_type_data
            ),
            -- Unpivot main data for companies WITHOUT detailed injury type data
            main_unpivoted AS (
                SELECT 
                    m.cvr_number, 
                    'TOTAL' as injury_type, 
                    2019 as year, 
                    m.year_2019 as injury_count_raw 
                FROM main_data m
                LEFT JOIN detailed_cvrs d ON m.cvr_number = d.cvr_number
                WHERE d.cvr_number IS NULL  -- Only companies without detailed data
                
                UNION ALL SELECT m.cvr_number, 'TOTAL' as injury_type, 2020 as year, m.year_2020 as injury_count_raw 
                FROM main_data m LEFT JOIN detailed_cvrs d ON m.cvr_number = d.cvr_number WHERE d.cvr_number IS NULL
                
                UNION ALL SELECT m.cvr_number, 'TOTAL' as injury_type, 2021 as year, m.year_2021 as injury_count_raw 
                FROM main_data m LEFT JOIN detailed_cvrs d ON m.cvr_number = d.cvr_number WHERE d.cvr_number IS NULL
                
                UNION ALL SELECT m.cvr_number, 'TOTAL' as injury_type, 2022 as year, m.year_2022 as injury_count_raw 
                FROM main_data m LEFT JOIN detailed_cvrs d ON m.cvr_number = d.cvr_number WHERE d.cvr_number IS NULL
                
                UNION ALL SELECT m.cvr_number, 'TOTAL' as injury_type, 2023 as year, m.year_2023 as injury_count_raw 
                FROM main_data m LEFT JOIN detailed_cvrs d ON m.cvr_number = d.cvr_number WHERE d.cvr_number IS NULL
                
                UNION ALL SELECT m.cvr_number, 'TOTAL' as injury_type, 2024 as year, m.year_2024 as injury_count_raw 
                FROM main_data m LEFT JOIN detailed_cvrs d ON m.cvr_number = d.cvr_number WHERE d.cvr_number IS NULL
            ),
            -- Combine both datasets
            combined AS (
                SELECT * FROM detailed_unpivoted
                UNION ALL
                SELECT * FROM main_unpivoted
            )
            SELECT 
                cvr_number,
                year,
                injury_type,
                CASE 
                    WHEN injury_count_raw = '<5' THEN '{self.config.privacy_value_replacement}'
                    WHEN injury_count_raw = '' OR injury_count_raw IS NULL THEN '0'
                    ELSE injury_count_raw
                END as injury_count
            FROM combined
            WHERE injury_count_raw IS NOT NULL 
                AND injury_count_raw != ''
                AND year >= {self.config.start_year}
                AND year <= {self.config.end_year}
                AND injury_count_raw != '0'  -- Only include records with actual injuries
            ORDER BY cvr_number, year, injury_type
        """)

        # Log processing results
        total_records = self.conn.execute("SELECT COUNT(*) FROM worker_safety_clean").fetchone()[0]
        unique_cvrs = self.conn.execute(
            "SELECT COUNT(DISTINCT cvr_number) FROM worker_safety_clean"
        ).fetchone()[0]
        unique_injury_types = self.conn.execute(
            "SELECT COUNT(DISTINCT injury_type) FROM worker_safety_clean"
        ).fetchone()[0]
        detailed_cvrs = self.conn.execute(
            "SELECT COUNT(DISTINCT cvr_number) FROM worker_safety_clean WHERE injury_type != 'TOTAL'"
        ).fetchone()[0]
        total_only_cvrs = self.conn.execute(
            "SELECT COUNT(DISTINCT cvr_number) FROM worker_safety_clean WHERE injury_type = 'TOTAL'"
        ).fetchone()[0]
        privacy_records = self.conn.execute(
            f"SELECT COUNT(*) FROM worker_safety_clean WHERE injury_count = '{self.config.privacy_value_replacement}'"
        ).fetchone()[0]

        self.log.info("✅ Clean data created:")
        self.log.info(f"   • Total records: {total_records}")
        self.log.info(f"   • Unique CVR numbers: {unique_cvrs}")
        self.log.info(f"   • CVRs with detailed injury types: {detailed_cvrs}")
        self.log.info(f"   • CVRs with total only: {total_only_cvrs}")
        self.log.info(f"   • Unique injury types: {unique_injury_types}")
        self.log.info(
            f"   • Records with privacy values ('{self.config.privacy_value_replacement}'): {privacy_records}"
        )

    async def _save_gold_data(self) -> Dict[str, str]:
        """Save clean worker safety data to gold layer."""

        self.log.info("💾 Saving clean worker safety data...")

        # Create output directory
        output_dir = f"gold/{self.config.dataset}/{self.date_pattern}"

        # Save clean structured data
        clean_data_path = f"{output_dir}/worker_safety_clean.parquet"
        await self._save_table_to_gcs("worker_safety_clean", clean_data_path)

        self.log.info("✅ Clean worker safety data saved successfully")

        return {"worker_safety_clean": clean_data_path}

    async def _save_table_to_gcs(self, table_name: str, gcs_path: str) -> None:
        """Save a DuckDB table to GCS as parquet."""

        # Upload directly from DuckDB table to GCS (more efficient)
        full_gcs_path = f"gs://{self.config.bucket}/{gcs_path}"
        self.gcs_access.upload_from_duckdb_table(table_name, full_gcs_path)

        self.log.info(f"✅ Saved {table_name} to {full_gcs_path}")

    def _get_latest_silver_path(self, dataset: str) -> Optional[str]:
        """Get the latest silver data directory path for a dataset."""
        try:
            # Look for the specific worker safety files
            pattern = (
                f"gs://{self.config.bucket}/silver/{dataset}/*/worker_safety_2020-2024_mv.parquet"
            )
            files = self.gcs_access.list_files(pattern)

            if not files:
                self.log.warning(f"No worker safety files found for dataset: {dataset}")
                return None

            # Get the latest file and extract its directory
            latest_file = sorted(files)[-1]
            # Extract directory from file path
            # Example: gs://bucket/silver/worker safety/20240802_224643/worker_safety_2020-2024_mv.parquet
            # Should return: gs://bucket/silver/worker safety/20240802_224643
            directory_path = "/".join(latest_file.split("/")[:-1])

            self.log.info(f"Found latest worker safety silver data directory: {directory_path}")
            return directory_path

        except Exception as e:
            self.log.error(f"Error finding latest silver path for {dataset}: {e}")
            return None
