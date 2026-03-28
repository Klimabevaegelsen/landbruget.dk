"""
Silver layer processor for Slaughter Premium (Slagtepræmie) data.

Source: SLP_2019_2023_ubetalt.parquet (cloud storage)
Data: Voluntary coupled support for cattle slaughter

Key transformations:
1. CVR normalization (8-digit format)
2. Year extraction and validation
3. Animal count and premium amount standardization

Output schema:
- cvr: TEXT (8-digit normalized)
- regnskabsaar: INTEGER (fiscal year)
- antal_dyr: INTEGER (number of animals)
- praemie_dkk: NUMERIC (premium amount)
- praemie_per_dyr_dkk: NUMERIC (calculated rate per animal)
"""

import os
from typing import Any

from dotenv import load_dotenv
from pydantic import ConfigDict, Field

from unified_pipeline.common.base import BaseJobConfig, BaseSource, SilverJobInterface
from unified_pipeline.util.cvr_normalizer import normalize_cvr
from unified_pipeline.util.log_util import Logger
from unified_pipeline.util.timing import timed

load_dotenv()


class SlagtepraemieSilverConfig(BaseJobConfig):
    """Configuration for slaughter premium silver processing."""

    name: str = "Slagtepræmie Silver"
    dataset: str = "slagtepraemie"
    type: str = "transformation"
    description: str = "Slaughter premium - voluntary coupled support for cattle"
    frequency: str = "yearly"
    bucket: str = (
        os.getenv("STORAGE_BUCKET")
        or os.getenv("R2_BUCKET")
        or os.getenv("GCS_BUCKET", "landbruget-data")
    )

    # Bronze source
    bronze_path: str = Field(
        default="bronze/subsidies/SLP_2019_2023_ubetalt.parquet",
        description="Storage path for bronze data",
    )

    # Column mappings (will auto-detect)
    cvr_column: str = Field(default="CVR", description="Source CVR column")
    year_column: str = Field(default="Aar", description="Year column")
    animals_column: str = Field(default="Antal", description="Animal count column")
    amount_column: str = Field(default="Praemie", description="Premium amount column")

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class SlagtepraemieSilver(BaseSource[SlagtepraemieSilverConfig], SilverJobInterface):
    """
    Silver layer processor for slaughter premium data.

    Slaughter premium is a voluntary coupled support scheme for cattle farmers.
    It provides a per-animal payment for cattle sent to slaughter.
    """

    def __init__(self, config: SlagtepraemieSilverConfig) -> None:
        super().__init__(config)
        self.log = Logger.get_logger()

    @timed
    async def run(self, bronze_data: Any | None = None) -> dict[str, Any] | None:
        """Process slaughter premium bronze data to silver."""
        self.log.info("Starting slagtepræmie silver processing")

        try:
            await self._load_bronze_data(bronze_data)
            await self._transform_data()
            summary = await self._validate_and_summarize()
            output_path = await self._save_silver_data()

            self.log.info(f"Slagtepræmie silver processing complete: {output_path}")

            return {
                "dataset": self.config.dataset,
                "output_path": output_path,
                "summary": summary,
            }

        except Exception as e:
            self.log.error(f"Error processing slagtepræmie: {e}")
            raise

    async def _load_bronze_data(self, bronze_data: Any | None = None) -> None:
        """Load bronze data from cloud storage or memory."""
        if bronze_data is not None:
            self.log.info("Using in-memory bronze data")
            self.conn.register("raw_slagtepraemie", bronze_data)
        else:
            self.log.info(f"Loading bronze data from: {self.config.bronze_path}")
            path = f"{self.config.bucket}/{self.config.bronze_path}"

            self.conn.execute(f"""
                CREATE OR REPLACE TABLE raw_slagtepraemie AS
                SELECT * FROM read_parquet('{path}')
            """)

        count = self.conn.execute("SELECT COUNT(*) FROM raw_slagtepraemie").fetchone()[0]
        self.log.info(f"Loaded {count:,} rows from bronze slagtepræmie")

    async def _transform_data(self) -> None:
        """Transform raw slaughter premium data to silver schema."""
        self.log.info("Transforming slagtepræmie data")

        # Register CVR normalization
        self.conn.create_function("normalize_cvr_udf", normalize_cvr, [str], str)

        # Get column names
        columns = [row[0] for row in self.conn.execute("DESCRIBE raw_slagtepraemie").fetchall()]
        self.log.info(f"Raw columns: {columns}")

        # Find columns (handle variations)
        cvr_col = self._find_column(columns, ["CVR", "cvr", "CVR_NR", "cvr_nr"])
        year_col = self._find_column(columns, ["Aar", "aar", "Year", "year", "År"])
        animals_col = self._find_column(
            columns, ["Antal", "antal", "antal_dyr", "Animals", "count"]
        )
        amount_col = self._find_column(
            columns, ["Praemie", "praemie", "Præmie", "præmie", "Amount", "amount", "beloeb"]
        )

        self.log.info(
            f"Using columns: cvr={cvr_col}, year={year_col}, animals={animals_col}, amount={amount_col}"
        )

        # Create silver table
        self.conn.execute(f"""
        CREATE OR REPLACE TABLE slagtepraemie_silver AS
        SELECT
            normalize_cvr_udf(CAST("{cvr_col}" AS VARCHAR)) AS cvr,
            CAST("{year_col}" AS INTEGER) AS regnskabsaar,
            CAST("{animals_col}" AS INTEGER) AS antal_dyr,
            TRY_CAST("{amount_col}" AS DOUBLE) AS praemie_dkk,

            -- Calculated rate per animal
            CASE
                WHEN CAST("{animals_col}" AS INTEGER) > 0
                THEN TRY_CAST("{amount_col}" AS DOUBLE) / CAST("{animals_col}" AS INTEGER)
                ELSE NULL
            END AS praemie_per_dyr_dkk,

            -- Ordning info
            'SLAGTEPRAEMIE' AS ordning_kode,
            'Slagtepræmie (koblet støtte)' AS ordning_dansk,

            -- Processing metadata
            CURRENT_TIMESTAMP AS processed_at

        FROM raw_slagtepraemie
        WHERE "{cvr_col}" IS NOT NULL
        """)

        # Filter to valid CVRs
        self.conn.execute("""
        DELETE FROM slagtepraemie_silver
        WHERE cvr IS NULL OR cvr !~ '^\\d{8}$'
        """)

        # Log stats
        count = self.conn.execute("SELECT COUNT(*) FROM slagtepraemie_silver").fetchone()[0]
        unique_cvrs = self.conn.execute(
            "SELECT COUNT(DISTINCT cvr) FROM slagtepraemie_silver"
        ).fetchone()[0]

        self.log.info(f"Transformed {count:,} rows ({unique_cvrs:,} unique CVRs)")

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
        """Validate and summarize slaughter premium data."""
        self.log.info("Validating slagtepræmie silver data")

        stats = self.conn.execute("""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT cvr) AS unique_cvrs,
            SUM(praemie_dkk) AS total_praemie_dkk,
            SUM(antal_dyr) AS total_animals,
            MIN(regnskabsaar) AS min_year,
            MAX(regnskabsaar) AS max_year,
            AVG(praemie_per_dyr_dkk) AS avg_rate_per_animal
        FROM slagtepraemie_silver
        """).fetchone()

        summary = {
            "total_rows": stats[0],
            "unique_cvrs": stats[1],
            "total_praemie_dkk": round(stats[2] or 0, 2),
            "total_animals": stats[3],
            "year_range": f"{stats[4]}-{stats[5]}",
            "avg_rate_per_animal": round(stats[6] or 0, 2),
        }

        # Get yearly breakdown
        yearly_stats = self.conn.execute("""
        SELECT
            regnskabsaar,
            COUNT(DISTINCT cvr) AS cvrs,
            SUM(antal_dyr) AS animals,
            SUM(praemie_dkk) AS total_dkk
        FROM slagtepraemie_silver
        GROUP BY regnskabsaar
        ORDER BY regnskabsaar
        """).fetchall()

        summary["yearly_breakdown"] = [
            {
                "year": row[0],
                "cvrs": row[1],
                "animals": row[2],
                "total_dkk": round(row[3] or 0, 0),
            }
            for row in yearly_stats
        ]

        self.log.info(
            f"Validation: {summary['unique_cvrs']:,} CVRs, "
            f"{summary['total_praemie_dkk'] / 1e9:.2f}B DKK total, "
            f"{summary['total_animals']:,} animals"
        )

        return summary

    async def _save_silver_data(self) -> str:
        """Save silver data to cloud storage."""
        output_path = f"silver/{self.config.dataset}"

        self.log.info(f"Saving silver data to: {self.config.bucket}/{output_path}")

        self._save_data(
            data="slagtepraemie_silver",
            dataset=self.config.dataset,
            bucket=self.config.bucket,
            stage="silver",
        )

        return f"{self.config.bucket}/{output_path}"
