"""
Silver layer processor for De Minimis national aid data.

Source: Deminimis_stoette.parquet (cloud storage)
Data: National aid ledger entries (credits and debits)

Key transformations:
1. CVR normalization (8-digit format)
2. Ledger aggregation (sum credits and debits per CVR)
3. Net amount calculation

CRITICAL: De minimis data is in LEDGER format!
- Each row is a transaction (credit or debit)
- Must SUM per CVR to get net amount
- Negative values = debits (repayments/corrections)

Output schema:
- cvr: TEXT (8-digit normalized)
- ordning_kode: TEXT (MRDM, MREG, MIKO)
- ordning_dansk: TEXT (Danish description)
- netto_dkk: NUMERIC (net amount after ledger aggregation)
- brutto_kredit_dkk: NUMERIC (total credits)
- brutto_debit_dkk: NUMERIC (total debits)
- antal_transaktioner: INTEGER (transaction count)
"""

import os
from typing import Any

from dotenv import load_dotenv
from pydantic import ConfigDict, Field

from unified_pipeline.common.base import BaseJobConfig, BaseSource, SilverJobInterface
from unified_pipeline.util.cvr_normalizer import normalize_cvr
from unified_pipeline.util.log_util import Logger
from unified_pipeline.util.subsidy_scheme_classifier import classify_deminimis
from unified_pipeline.util.timing import timed

load_dotenv()


class DeminimisSilverConfig(BaseJobConfig):
    """Configuration for de minimis silver processing."""

    name: str = "De Minimis Silver"
    dataset: str = "deminimis"
    type: str = "transformation"
    description: str = "National de minimis aid - ledger aggregated"
    frequency: str = "yearly"
    bucket: str = (
        os.getenv("STORAGE_BUCKET")
        or os.getenv("R2_BUCKET")
        or os.getenv("GCS_BUCKET", "landbruget-data")
    )

    # Bronze source
    bronze_path: str = Field(
        default="bronze/subsidies/Deminimis_stoette.parquet",
        description="Storage path for bronze data",
    )

    # Column mappings
    cvr_column: str = Field(default="CVR", description="Source CVR column")
    amount_column: str = Field(default="Beloeb", description="Amount column (can be + or -)")
    ordning_column: str = Field(default="Ordning", description="Scheme/ordning column")

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class DeminimisSilver(BaseSource[DeminimisSilverConfig], SilverJobInterface):
    """
    Silver layer processor for de minimis national aid.

    De minimis data is in LEDGER format:
    - Each row is a transaction (credit or debit)
    - Credits are positive, debits are negative
    - Must aggregate per CVR to get net amounts
    """

    def __init__(self, config: DeminimisSilverConfig) -> None:
        super().__init__(config)
        self.log = Logger.get_logger()

    @timed
    async def run(self, bronze_data: Any | None = None) -> dict[str, Any] | None:
        """Process de minimis bronze data to silver."""
        self.log.info("Starting de minimis silver processing")

        try:
            await self._load_bronze_data(bronze_data)
            await self._transform_data()
            summary = await self._validate_and_summarize()
            output_path = await self._save_silver_data()

            self.log.info(f"De minimis silver processing complete: {output_path}")

            return {
                "dataset": self.config.dataset,
                "output_path": output_path,
                "summary": summary,
            }

        except Exception as e:
            self.log.error(f"Error processing de minimis: {e}")
            raise

    async def _load_bronze_data(self, bronze_data: Any | None = None) -> None:
        """Load bronze data from cloud storage or memory."""
        if bronze_data is not None:
            self.log.info("Using in-memory bronze data")
            self.conn.register("raw_deminimis", bronze_data)
        else:
            self.log.info(f"Loading bronze data from: {self.config.bronze_path}")
            path = f"{self.config.bucket}/{self.config.bronze_path}"

            self.conn.execute(f"""
                CREATE OR REPLACE TABLE raw_deminimis AS
                SELECT * FROM read_parquet('{path}')
            """)

        count = self.conn.execute("SELECT COUNT(*) FROM raw_deminimis").fetchone()[0]
        self.log.info(f"Loaded {count:,} ledger entries from bronze de minimis")

    async def _transform_data(self) -> None:
        """Transform ledger entries to aggregated CVR-level data."""
        self.log.info("Transforming de minimis ledger data")

        # Register CVR normalization
        self.conn.create_function("normalize_cvr_udf", normalize_cvr, [str], str)

        # Register scheme classification
        def classify_dm_code(ordning: str) -> str:
            code, _ = classify_deminimis(ordning)
            return code or "DEMINIMIS_UKENDT"

        def classify_dm_danish(ordning: str) -> str:
            _, danish = classify_deminimis(ordning)
            return danish or "De minimis - Ukendt"

        self.conn.create_function("classify_dm_code", classify_dm_code, [str], str)
        self.conn.create_function("classify_dm_danish", classify_dm_danish, [str], str)

        # Get column names
        columns = [row[0] for row in self.conn.execute("DESCRIBE raw_deminimis").fetchall()]
        self.log.info(f"Raw columns: {columns}")

        # Find amount column (might be named differently)
        amount_col = self._find_column(columns, ["Beloeb", "beloeb", "Amount", "amount", "beløb"])
        cvr_col = self._find_column(columns, ["CVR", "cvr", "CVR_NR", "cvr_nr"])
        ordning_col = self._find_column(columns, ["Ordning", "ordning", "scheme"])

        # Step 1: Normalize CVRs in raw data
        self.conn.execute(f"""
        CREATE OR REPLACE TABLE deminimis_normalized AS
        SELECT
            normalize_cvr_udf(CAST("{cvr_col}" AS VARCHAR)) AS cvr,
            classify_dm_code(CAST("{ordning_col}" AS VARCHAR)) AS ordning_kode,
            classify_dm_danish(CAST("{ordning_col}" AS VARCHAR)) AS ordning_dansk,
            "{ordning_col}" AS ordning_original,
            TRY_CAST("{amount_col}" AS DOUBLE) AS beloeb
        FROM raw_deminimis
        WHERE "{cvr_col}" IS NOT NULL
        """)

        # Step 2: Aggregate ledger entries per CVR and ordning
        # SUM(beloeb) gives net amount (credits positive, debits negative)
        self.conn.execute("""
        CREATE OR REPLACE TABLE deminimis_silver AS
        SELECT
            cvr,
            ordning_kode,
            ordning_dansk,

            -- Ledger aggregation
            SUM(beloeb) AS netto_dkk,
            SUM(CASE WHEN beloeb > 0 THEN beloeb ELSE 0 END) AS brutto_kredit_dkk,
            SUM(CASE WHEN beloeb < 0 THEN ABS(beloeb) ELSE 0 END) AS brutto_debit_dkk,
            COUNT(*) AS antal_transaktioner,

            -- Processing metadata
            CURRENT_TIMESTAMP AS processed_at

        FROM deminimis_normalized
        WHERE cvr ~ '^\\d{8}$'
        GROUP BY cvr, ordning_kode, ordning_dansk
        """)

        # Log stats
        raw_count = self.conn.execute("SELECT COUNT(*) FROM raw_deminimis").fetchone()[0]
        agg_count = self.conn.execute("SELECT COUNT(*) FROM deminimis_silver").fetchone()[0]
        unique_cvrs = self.conn.execute(
            "SELECT COUNT(DISTINCT cvr) FROM deminimis_silver"
        ).fetchone()[0]

        self.log.info(
            f"Aggregated {raw_count:,} ledger entries → {agg_count:,} CVR-ordning rows ({unique_cvrs:,} unique CVRs)"
        )

    def _find_column(self, columns: list, candidates: list) -> str:
        """Find a column from a list of candidate names."""
        for candidate in candidates:
            if candidate in columns:
                return candidate
            for col in columns:
                if col.lower() == candidate.lower():
                    return col
        raise ValueError(f"Could not find column from candidates: {candidates}")

    async def _validate_and_summarize(self) -> dict[str, Any]:
        """Validate and summarize de minimis data."""
        self.log.info("Validating de minimis silver data")

        stats = self.conn.execute("""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT cvr) AS unique_cvrs,
            SUM(netto_dkk) AS total_netto_dkk,
            SUM(brutto_kredit_dkk) AS total_kredit_dkk,
            SUM(brutto_debit_dkk) AS total_debit_dkk,
            SUM(antal_transaktioner) AS total_transactions
        FROM deminimis_silver
        """).fetchone()

        summary = {
            "total_rows": stats[0],
            "unique_cvrs": stats[1],
            "total_netto_dkk": round(stats[2] or 0, 2),
            "total_kredit_dkk": round(stats[3] or 0, 2),
            "total_debit_dkk": round(stats[4] or 0, 2),
            "total_transactions": stats[5],
        }

        # Validate net/gross ratio
        if stats[3] and stats[3] > 0:
            net_gross_ratio = (stats[2] or 0) / stats[3]
            summary["net_gross_ratio"] = round(net_gross_ratio, 2)

            # Expected: 70-90% (some debits/corrections expected)
            if 0.60 <= net_gross_ratio <= 0.95:
                self.log.info(
                    f"Net/gross ratio {net_gross_ratio:.1%} within expected range (60-95%)"
                )
            else:
                self.log.warning(f"Net/gross ratio {net_gross_ratio:.1%} outside expected range!")

        # Get ordning breakdown
        ordning_stats = self.conn.execute("""
        SELECT ordning_kode, COUNT(DISTINCT cvr) as cvrs, SUM(netto_dkk) as total_dkk
        FROM deminimis_silver
        GROUP BY ordning_kode
        ORDER BY total_dkk DESC
        """).fetchall()

        summary["ordning_breakdown"] = [
            {"ordning_kode": row[0], "cvrs": row[1], "total_dkk": round(row[2] or 0, 0)}
            for row in ordning_stats
        ]

        self.log.info(
            f"Validation: {summary['unique_cvrs']:,} CVRs, {summary['total_netto_dkk'] / 1e6:.1f}M DKK net"
        )

        return summary

    async def _save_silver_data(self) -> str:
        """Save silver data to cloud storage."""
        output_path = f"silver/{self.config.dataset}"

        self.log.info(f"Saving silver data to: {self.config.bucket}/{output_path}")

        self._save_data(
            data="deminimis_silver",
            dataset=self.config.dataset,
            bucket=self.config.bucket,
            stage="silver",
        )

        return f"{self.config.bucket}/{output_path}"
