"""
Gold layer processor for EU CAP payments (Landbrugsstøtte EU Betalinger).

Consumes silver støtteoplysninger data and produces:
- Analysis-ready payment data with standardized scheme codes
- Summary row exclusion for accurate aggregations
- CVR-level aggregations for quick lookups

Output table: landbrugstoette_eu_betalinger
"""

import os
from typing import Any

from pydantic import ConfigDict, Field

from unified_pipeline.common.base import BaseJobConfig, BaseSource, GoldJobInterface
from unified_pipeline.util.log_util import Logger
from unified_pipeline.util.timing import timed


class LandbrugstoetteEUGoldConfig(BaseJobConfig):
    """Configuration for EU CAP payments gold layer."""

    name: str = "Landbrugsstøtte EU Gold"
    dataset: str = "landbrugstoette_eu_betalinger"
    type: str = "gold"
    description: str = "EU CAP payments - EAGF (Pillar 1) and EAFRD (Pillar 2)"
    frequency: str = "yearly"
    bucket: str = os.getenv("GCS_BUCKET", "landbruget-data")

    # Silver input dataset
    silver_dataset: str = Field(
        default="stoetteoplysninger",
        description="Silver layer dataset to consume",
    )

    # Validation thresholds (ratios, not hardcoded amounts)
    min_eagf_eafrd_ratio: float = Field(
        default=5.0,
        description="Minimum expected EAGF/EAFRD ratio",
    )
    max_eagf_eafrd_ratio: float = Field(
        default=10.0,
        description="Maximum expected EAGF/EAFRD ratio",
    )
    min_cvr_count: int = Field(
        default=20000,
        description="Minimum expected unique CVRs (sanity check)",
    )

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class LandbrugstoetteEUGold(BaseSource[LandbrugstoetteEUGoldConfig], GoldJobInterface):
    """
    Gold layer processor for EU CAP payments.

    Takes silver støtteoplysninger data and produces:
    1. Detail-level payment table (with summary rows excluded)
    2. CVR-level aggregation for quick lookups
    3. Validation metrics

    CRITICAL: Always filter WHERE is_summary_row = FALSE to avoid double-counting!
    """

    def __init__(self, config: LandbrugstoetteEUGoldConfig) -> None:
        super().__init__(config)
        self.log = Logger.get_logger()

        # Configure DuckDB
        self.conn.execute("SET memory_limit = '8GB'")
        self.conn.execute("SET threads = 4")
        self.conn.execute("SET temp_directory = '/tmp'")
        self.conn.execute("SET s3_region = 'europe-west1'")

    @timed
    async def run(self, silver_data: dict[str, Any] | None = None) -> dict[str, str] | None:
        """Execute EU CAP payments gold processing."""
        self.log.info("Starting Landbrugsstøtte EU Gold processing")

        try:
            # Load silver data
            await self._load_silver_data(silver_data)

            # Create gold tables
            await self._create_detail_table()
            await self._create_cvr_summary()

            # Validate
            validation = await self._validate_data()

            # Save
            output_paths = await self._save_gold_data()

            self.log.info("Landbrugsstøtte EU Gold processing completed")

            return {
                "dataset": self.config.dataset,
                "output_paths": output_paths,
                "validation": validation,
            }

        except Exception as e:
            self.log.error(f"Error in Landbrugsstøtte EU Gold processing: {e}")
            raise

    async def _load_silver_data(self, silver_data: dict[str, Any] | None = None) -> None:
        """Load silver støtteoplysninger data."""
        self.log.info("Loading silver støtteoplysninger data")

        if silver_data and self.config.silver_dataset in silver_data:
            self.log.info("Using in-memory silver data")
            self.conn.register("stoetteoplysninger_silver", silver_data[self.config.silver_dataset])
        else:
            # Load from GCS
            pattern = f"gs://{self.config.bucket}/silver/{self.config.silver_dataset}/**/*.parquet"
            self.log.info(f"Loading silver data from: {pattern}")

            files = self.gcs_access.list_files(pattern)
            if not files:
                raise FileNotFoundError(f"No silver files found: {pattern}")

            latest_file = sorted(files)[-1]
            self.log.info(f"Loading: {latest_file}")

            with self.gcs_access._temp_download(latest_file) as temp_file:
                self.conn.execute(f"""
                    CREATE OR REPLACE TABLE stoetteoplysninger_silver AS
                    SELECT * FROM read_parquet('{temp_file}')
                """)

        count = self.conn.execute("SELECT COUNT(*) FROM stoetteoplysninger_silver").fetchone()[0]
        self.log.info(f"Loaded {count:,} rows from silver støtteoplysninger")

    async def _create_detail_table(self) -> None:
        """Create the detail-level gold table."""
        self.log.info("Creating landbrugstoette_eu_betalinger gold table")

        self.conn.execute("""
        CREATE OR REPLACE TABLE landbrugstoette_eu_betalinger AS
        SELECT
            cvr,
            regnskabsaar,
            ordning_kode,
            ordning_dansk,
            scheme_original,
            pillar,
            eagf_dkk,
            eafrd_dkk,
            medfinansiering_dkk,
            total_dkk,
            is_summary_row,

            -- Gold layer metadata
            CURRENT_TIMESTAMP AS gold_processed_at

        FROM stoetteoplysninger_silver
        WHERE cvr IS NOT NULL
        AND cvr ~ '^\\d{8}$'
        """)

        # Get counts
        total = self.conn.execute("SELECT COUNT(*) FROM landbrugstoette_eu_betalinger").fetchone()[
            0
        ]
        detail = self.conn.execute(
            "SELECT COUNT(*) FROM landbrugstoette_eu_betalinger WHERE is_summary_row = FALSE"
        ).fetchone()[0]
        summary = self.conn.execute(
            "SELECT COUNT(*) FROM landbrugstoette_eu_betalinger WHERE is_summary_row = TRUE"
        ).fetchone()[0]

        self.log.info(
            f"Created gold table: {total:,} rows ({detail:,} detail, {summary:,} summary)"
        )

    async def _create_cvr_summary(self) -> None:
        """Create CVR-level aggregation for quick lookups."""
        self.log.info("Creating CVR summary table")

        self.conn.execute("""
        CREATE OR REPLACE TABLE landbrugstoette_cvr_summary AS
        SELECT
            cvr,
            regnskabsaar,

            -- EU breakdown (EXCLUDING summary rows!)
            SUM(CASE WHEN ordning_kode = 'GRUNDBETALING' AND NOT is_summary_row THEN total_dkk ELSE 0 END) AS grundbetaling_dkk,
            SUM(CASE WHEN ordning_kode = 'GROEN_STOETTE' AND NOT is_summary_row THEN total_dkk ELSE 0 END) AS groen_stoette_dkk,
            SUM(CASE WHEN ordning_kode LIKE 'OEKOLOGISK%' AND NOT is_summary_row THEN total_dkk ELSE 0 END) AS oekologisk_dkk,
            SUM(CASE WHEN ordning_kode LIKE 'GRAESPLEJE%' AND NOT is_summary_row THEN total_dkk ELSE 0 END) AS graespleje_dkk,
            SUM(CASE WHEN ordning_kode = 'NATURA2000' AND NOT is_summary_row THEN total_dkk ELSE 0 END) AS natura2000_dkk,

            -- Pillar totals
            SUM(CASE WHEN pillar = 1 AND NOT is_summary_row THEN total_dkk ELSE 0 END) AS pillar1_total_dkk,
            SUM(CASE WHEN pillar = 2 AND NOT is_summary_row THEN total_dkk ELSE 0 END) AS pillar2_total_dkk,

            -- Grand total
            SUM(CASE WHEN NOT is_summary_row THEN total_dkk ELSE 0 END) AS eu_total_dkk,

            -- Count of schemes
            COUNT(DISTINCT CASE WHEN NOT is_summary_row THEN ordning_kode END) AS antal_ordninger,

            -- Processing timestamp
            CURRENT_TIMESTAMP AS gold_processed_at

        FROM landbrugstoette_eu_betalinger
        WHERE cvr IS NOT NULL
        GROUP BY cvr, regnskabsaar
        """)

        count = self.conn.execute("SELECT COUNT(*) FROM landbrugstoette_cvr_summary").fetchone()[0]
        self.log.info(f"Created CVR summary: {count:,} CVR-year combinations")

    async def _validate_data(self) -> dict[str, Any]:
        """Validate gold data using ratios and cross-checks."""
        self.log.info("Validating gold data")

        # Get totals (excluding summary rows)
        totals = self.conn.execute("""
        SELECT
            COUNT(DISTINCT cvr) AS unique_cvrs,
            SUM(CASE WHEN NOT is_summary_row THEN eagf_dkk ELSE 0 END) AS total_eagf,
            SUM(CASE WHEN NOT is_summary_row THEN eafrd_dkk ELSE 0 END) AS total_eafrd,
            SUM(CASE WHEN NOT is_summary_row THEN total_dkk ELSE 0 END) AS total_payments,
            COUNT(CASE WHEN is_summary_row THEN 1 END) AS summary_row_count,
            COUNT(CASE WHEN NOT is_summary_row THEN 1 END) AS detail_row_count
        FROM landbrugstoette_eu_betalinger
        """).fetchone()

        validation = {
            "unique_cvrs": totals[0],
            "total_eagf_dkk": round(totals[1], 2),
            "total_eafrd_dkk": round(totals[2], 2),
            "total_payments_dkk": round(totals[3], 2),
            "summary_rows": totals[4],
            "detail_rows": totals[5],
        }

        # Check EAGF/EAFRD ratio
        if totals[2] > 0:
            ratio = totals[1] / totals[2]
            validation["eagf_eafrd_ratio"] = round(ratio, 2)

            if self.config.min_eagf_eafrd_ratio <= ratio <= self.config.max_eagf_eafrd_ratio:
                self.log.info(f"EAGF/EAFRD ratio {ratio:.1f}:1 within expected range")
                validation["ratio_check"] = "PASS"
            else:
                self.log.warning(f"EAGF/EAFRD ratio {ratio:.1f}:1 outside expected range!")
                validation["ratio_check"] = "WARN"

        # Check CVR count
        if totals[0] >= self.config.min_cvr_count:
            self.log.info(f"CVR count {totals[0]:,} >= {self.config.min_cvr_count:,}")
            validation["cvr_check"] = "PASS"
        else:
            self.log.warning(f"CVR count {totals[0]:,} < {self.config.min_cvr_count:,}")
            validation["cvr_check"] = "WARN"

        # Check double-counting prevention
        # If working correctly, summary rows should be ~38% of total
        if totals[4] > 0 and totals[5] > 0:
            summary_pct = totals[4] / (totals[4] + totals[5])
            validation["summary_row_pct"] = round(summary_pct * 100, 1)

            # Summary should be 25-50% of rows (based on our data)
            if 0.20 <= summary_pct <= 0.55:
                self.log.info(f"Summary row percentage {summary_pct:.1%} within expected range")
                validation["summary_check"] = "PASS"
            else:
                self.log.warning(f"Summary row percentage {summary_pct:.1%} unexpected!")
                validation["summary_check"] = "WARN"

        self.log.info(
            f"Validation complete: {validation['unique_cvrs']:,} CVRs, "
            f"{validation['total_payments_dkk'] / 1e9:.2f}B DKK"
        )

        return validation

    async def _save_gold_data(self) -> dict[str, str]:
        """Save gold tables to GCS."""
        self.log.info("Saving gold data to GCS")

        output_paths = {}

        # Save detail table
        detail_path = f"gold/{self.config.dataset}"
        self._save_data(
            data="landbrugstoette_eu_betalinger",
            dataset=self.config.dataset,
            bucket=self.config.bucket,
            stage="gold",
        )
        output_paths["detail"] = f"gs://{self.config.bucket}/{detail_path}"

        # Save CVR summary
        summary_path = f"gold/{self.config.dataset}_cvr_summary"
        self._save_data(
            data="landbrugstoette_cvr_summary",
            dataset=f"{self.config.dataset}_cvr_summary",
            bucket=self.config.bucket,
            stage="gold",
        )
        output_paths["cvr_summary"] = f"gs://{self.config.bucket}/{summary_path}"

        self.log.info(f"Saved gold data: {output_paths}")

        return output_paths
