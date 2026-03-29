"""
Gold layer processor for National subsidies (De Minimis + Slagtepræmie).

Consumes silver deminimis and slagtepraemie data and produces:
- Combined national aid table
- CVR-level aggregations

Output table: landbrugstoette_national
"""

import os
from typing import Any

from pydantic import ConfigDict, Field

from unified_pipeline.common.base import BaseJobConfig, BaseSource, GoldJobInterface
from unified_pipeline.util.log_util import Logger
from unified_pipeline.util.timing import timed


class LandbrugstoetteNationalGoldConfig(BaseJobConfig):
    """Configuration for national subsidies gold layer."""

    name: str = "Landbrugsstøtte National Gold"
    dataset: str = "landbrugstoette_national"
    type: str = "gold"
    description: str = "National subsidies - De Minimis and Slaughter Premium"
    frequency: str = "yearly"
    bucket: str = (
        os.getenv("STORAGE_BUCKET")
        or os.getenv("R2_BUCKET")
        or os.getenv("GCS_BUCKET", "landbruget-data")
    )

    # Silver input datasets
    deminimis_dataset: str = Field(default="deminimis", description="De minimis silver dataset")
    slagtepraemie_dataset: str = Field(
        default="slagtepraemie", description="Slaughter premium silver dataset"
    )

    # Validation thresholds (ratios)
    min_net_gross_ratio: float = Field(
        default=0.60,
        description="Minimum expected net/gross ratio for de minimis",
    )
    max_net_gross_ratio: float = Field(
        default=0.95,
        description="Maximum expected net/gross ratio for de minimis",
    )

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class LandbrugstoetteNationalGold(BaseSource[LandbrugstoetteNationalGoldConfig], GoldJobInterface):
    """
    Gold layer processor for national subsidies.

    Combines:
    - De minimis aid (ledger-aggregated national support)
    - Slaughter premium (voluntary coupled support for cattle)
    """

    def __init__(self, config: LandbrugstoetteNationalGoldConfig) -> None:
        super().__init__(config)
        self.log = Logger.get_logger()

        self.conn.execute("SET memory_limit = '4GB'")
        self.conn.execute("SET threads = 2")
        self.conn.execute("SET temp_directory = '/tmp'")
        self.conn.execute("SET s3_region = 'europe-west1'")

    @timed
    async def run(self, silver_data: dict[str, Any] | None = None) -> dict[str, str] | None:
        """Execute national subsidies gold processing."""
        self.log.info("Starting Landbrugsstøtte National Gold processing")

        try:
            await self._load_silver_data(silver_data)
            await self._create_deminimis_gold()
            await self._create_slagtepraemie_gold()
            await self._create_combined_national()
            validation = await self._validate_data()
            output_paths = await self._save_gold_data()

            self.log.info("Landbrugsstøtte National Gold processing completed")

            return {
                "dataset": self.config.dataset,
                "output_paths": output_paths,
                "validation": validation,
            }

        except Exception as e:
            self.log.error(f"Error in Landbrugsstøtte National Gold: {e}")
            raise

    async def _load_silver_data(self, silver_data: dict[str, Any] | None = None) -> None:
        """Load silver de minimis and slaughter premium data."""
        self.log.info("Loading silver national subsidies data")

        # Load de minimis
        await self._load_dataset("deminimis_silver", self.config.deminimis_dataset, silver_data)

        # Load slaughter premium
        await self._load_dataset(
            "slagtepraemie_silver", self.config.slagtepraemie_dataset, silver_data
        )

    async def _load_dataset(
        self, table_name: str, dataset: str, silver_data: dict[str, Any] | None
    ) -> None:
        """Load a single silver dataset."""
        if silver_data and dataset in silver_data:
            self.log.info(f"Using in-memory silver data for {dataset}")
            self.conn.register(table_name, silver_data[dataset])
        else:
            pattern = f"{self.config.bucket}/silver/{dataset}/**/*.parquet"
            self.log.info(f"Loading {dataset} from: {pattern}")

            files = self.storage.list_files(pattern)
            if not files:
                self.log.warning(f"No files found for {dataset}, creating empty table")
                self.conn.execute(
                    f"CREATE OR REPLACE TABLE {table_name} AS SELECT NULL AS cvr WHERE FALSE"
                )
                return

            latest_file = sorted(files)[-1]
            with self.storage._temp_download(latest_file) as temp_file:
                self.conn.execute(f"""
                    CREATE OR REPLACE TABLE {table_name} AS
                    SELECT * FROM read_parquet('{temp_file}')
                """)

        count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        self.log.info(f"Loaded {count:,} rows for {dataset}")

    async def _create_deminimis_gold(self) -> None:
        """Create de minimis gold table."""
        self.log.info("Creating de minimis gold table")

        # Check if we have data
        count = self.conn.execute("SELECT COUNT(*) FROM deminimis_silver").fetchone()[0]
        if count == 0:
            self.log.warning("No de minimis data available")
            self.conn.execute("""
                CREATE OR REPLACE TABLE landbrugstoette_deminimis AS
                SELECT NULL::VARCHAR AS cvr WHERE FALSE
            """)
            return

        self.conn.execute("""
        CREATE OR REPLACE TABLE landbrugstoette_deminimis AS
        SELECT
            cvr,
            ordning_kode,
            ordning_dansk,
            netto_dkk,
            brutto_kredit_dkk,
            brutto_debit_dkk,
            antal_transaktioner,
            'DEMINIMIS' AS stoette_type,
            CURRENT_TIMESTAMP AS gold_processed_at
        FROM deminimis_silver
        WHERE cvr IS NOT NULL
        AND cvr ~ '^\\d{8}$'
        """)

        count = self.conn.execute("SELECT COUNT(*) FROM landbrugstoette_deminimis").fetchone()[0]
        self.log.info(f"Created de minimis gold: {count:,} rows")

    async def _create_slagtepraemie_gold(self) -> None:
        """Create slaughter premium gold table."""
        self.log.info("Creating slaughter premium gold table")

        count = self.conn.execute("SELECT COUNT(*) FROM slagtepraemie_silver").fetchone()[0]
        if count == 0:
            self.log.warning("No slaughter premium data available")
            self.conn.execute("""
                CREATE OR REPLACE TABLE landbrugstoette_slagtepraemie AS
                SELECT NULL::VARCHAR AS cvr WHERE FALSE
            """)
            return

        self.conn.execute("""
        CREATE OR REPLACE TABLE landbrugstoette_slagtepraemie AS
        SELECT
            cvr,
            regnskabsaar,
            ordning_kode,
            ordning_dansk,
            antal_dyr,
            praemie_dkk,
            praemie_per_dyr_dkk,
            'SLAGTEPRAEMIE' AS stoette_type,
            CURRENT_TIMESTAMP AS gold_processed_at
        FROM slagtepraemie_silver
        WHERE cvr IS NOT NULL
        AND cvr ~ '^\\d{8}$'
        """)

        count = self.conn.execute("SELECT COUNT(*) FROM landbrugstoette_slagtepraemie").fetchone()[
            0
        ]
        self.log.info(f"Created slaughter premium gold: {count:,} rows")

    async def _create_combined_national(self) -> None:
        """Create combined national subsidies summary by CVR."""
        self.log.info("Creating combined national subsidies summary")

        self.conn.execute("""
        CREATE OR REPLACE TABLE landbrugstoette_national_cvr_summary AS
        WITH deminimis_agg AS (
            SELECT
                cvr,
                SUM(netto_dkk) AS deminimis_netto_dkk,
                SUM(brutto_kredit_dkk) AS deminimis_brutto_dkk,
                COUNT(DISTINCT ordning_kode) AS deminimis_ordninger
            FROM landbrugstoette_deminimis
            GROUP BY cvr
        ),
        slagtepraemie_agg AS (
            SELECT
                cvr,
                SUM(praemie_dkk) AS slagtepraemie_dkk,
                SUM(antal_dyr) AS total_dyr,
                MIN(regnskabsaar) AS min_aar,
                MAX(regnskabsaar) AS max_aar
            FROM landbrugstoette_slagtepraemie
            GROUP BY cvr
        )
        SELECT
            COALESCE(d.cvr, s.cvr) AS cvr,
            COALESCE(d.deminimis_netto_dkk, 0) AS deminimis_dkk,
            COALESCE(d.deminimis_brutto_dkk, 0) AS deminimis_brutto_dkk,
            COALESCE(d.deminimis_ordninger, 0) AS deminimis_ordninger,
            COALESCE(s.slagtepraemie_dkk, 0) AS slagtepraemie_dkk,
            COALESCE(s.total_dyr, 0) AS slagtepraemie_dyr,
            s.min_aar AS slagtepraemie_fra_aar,
            s.max_aar AS slagtepraemie_til_aar,
            COALESCE(d.deminimis_netto_dkk, 0) + COALESCE(s.slagtepraemie_dkk, 0) AS national_total_dkk,
            CURRENT_TIMESTAMP AS gold_processed_at
        FROM deminimis_agg d
        FULL OUTER JOIN slagtepraemie_agg s ON d.cvr = s.cvr
        """)

        count = self.conn.execute(
            "SELECT COUNT(*) FROM landbrugstoette_national_cvr_summary"
        ).fetchone()[0]
        self.log.info(f"Created combined national summary: {count:,} unique CVRs")

    async def _validate_data(self) -> dict[str, Any]:
        """Validate national subsidies data."""
        self.log.info("Validating national subsidies data")

        validation = {}

        # De minimis validation
        dm_stats = self.conn.execute("""
        SELECT
            COUNT(DISTINCT cvr) AS unique_cvrs,
            SUM(netto_dkk) AS total_netto,
            SUM(brutto_kredit_dkk) AS total_brutto
        FROM landbrugstoette_deminimis
        """).fetchone()

        validation["deminimis"] = {
            "unique_cvrs": dm_stats[0] or 0,
            "total_netto_dkk": round(dm_stats[1] or 0, 2),
            "total_brutto_dkk": round(dm_stats[2] or 0, 2),
        }

        if dm_stats[2] and dm_stats[2] > 0:
            ratio = (dm_stats[1] or 0) / dm_stats[2]
            validation["deminimis"]["net_gross_ratio"] = round(ratio, 2)
            if self.config.min_net_gross_ratio <= ratio <= self.config.max_net_gross_ratio:
                validation["deminimis"]["ratio_check"] = "PASS"
            else:
                validation["deminimis"]["ratio_check"] = "WARN"

        # Slaughter premium validation
        slp_stats = self.conn.execute("""
        SELECT
            COUNT(DISTINCT cvr) AS unique_cvrs,
            SUM(praemie_dkk) AS total_praemie,
            SUM(antal_dyr) AS total_dyr,
            AVG(praemie_per_dyr_dkk) AS avg_rate
        FROM landbrugstoette_slagtepraemie
        """).fetchone()

        validation["slagtepraemie"] = {
            "unique_cvrs": slp_stats[0] or 0,
            "total_praemie_dkk": round(slp_stats[1] or 0, 2),
            "total_dyr": slp_stats[2] or 0,
            "avg_rate_per_dyr": round(slp_stats[3] or 0, 2),
        }

        # Combined totals
        combined = self.conn.execute("""
        SELECT
            COUNT(*) AS unique_cvrs,
            SUM(national_total_dkk) AS total_national
        FROM landbrugstoette_national_cvr_summary
        """).fetchone()

        validation["combined"] = {
            "unique_cvrs": combined[0] or 0,
            "total_national_dkk": round(combined[1] or 0, 2),
        }

        self.log.info(
            f"Validation: {validation['combined']['unique_cvrs']:,} CVRs, "
            f"{validation['combined']['total_national_dkk'] / 1e6:.1f}M DKK total national"
        )

        return validation

    async def _save_gold_data(self) -> dict[str, str]:
        """Save gold tables to cloud storage."""
        self.log.info("Saving national subsidies gold data")

        output_paths = {}

        # Save de minimis
        self._save_data(
            data="landbrugstoette_deminimis",
            dataset="landbrugstoette_deminimis",
            bucket=self.config.bucket,
            stage="gold",
        )
        output_paths["deminimis"] = f"{self.config.bucket}/gold/landbrugstoette_deminimis"

        # Save slaughter premium
        self._save_data(
            data="landbrugstoette_slagtepraemie",
            dataset="landbrugstoette_slagtepraemie",
            bucket=self.config.bucket,
            stage="gold",
        )
        output_paths["slagtepraemie"] = f"{self.config.bucket}/gold/landbrugstoette_slagtepraemie"

        # Save combined summary
        self._save_data(
            data="landbrugstoette_national_cvr_summary",
            dataset="landbrugstoette_national_cvr_summary",
            bucket=self.config.bucket,
            stage="gold",
        )
        output_paths["cvr_summary"] = (
            f"{self.config.bucket}/gold/landbrugstoette_national_cvr_summary"
        )

        self.log.info(f"Saved gold data: {output_paths}")

        return output_paths
