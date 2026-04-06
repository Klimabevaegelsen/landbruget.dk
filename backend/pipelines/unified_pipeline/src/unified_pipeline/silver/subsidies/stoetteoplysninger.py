"""
Silver layer processor for EU CAP payment data (støtteoplysninger).

Source: støtteoplysninger.naturerhverv.dk
Data: EU payment records with CVR, scheme, amounts (EAGF/EAFRD)

Key transformations:
1. CVR normalization (8-digit format)
2. Scheme classification to standardized codes
3. Summary row flagging (to prevent double-counting)
4. Financial year extraction

Output schema:
- cvr: TEXT (8-digit normalized)
- regnskabsaar: INTEGER (fiscal year)
- ordning_kode: TEXT (GRUNDBETALING, GROEN_STOETTE, etc.)
- ordning_dansk: TEXT (Danish scheme name)
- scheme_original: TEXT (original scheme name)
- eagf_dkk: NUMERIC (Pillar 1 amount)
- eafrd_dkk: NUMERIC (Pillar 2 amount)
- medfinansiering_dkk: NUMERIC (national co-financing)
- is_summary_row: BOOLEAN (TRUE for "Total for beneficiary" rows)
"""

import os
from typing import Any

from dotenv import find_dotenv, load_dotenv
from pydantic import ConfigDict, Field

from unified_pipeline.common.base import BaseJobConfig, BaseSource, SilverJobInterface
from unified_pipeline.util.cvr_normalizer import normalize_cvr
from unified_pipeline.util.log_util import Logger
from unified_pipeline.util.subsidy_scheme_classifier import (
    classify_eu_scheme,
    get_pillar,
    is_summary_row,
)
from unified_pipeline.util.timing import timed

load_dotenv(find_dotenv(usecwd=True))


class StoetteoplysningerSilverConfig(BaseJobConfig):
    """Configuration for støtteoplysninger silver processing."""

    name: str = "Støtteoplysninger Silver"
    dataset: str = "stoetteoplysninger"
    type: str = "transformation"
    description: str = "EU CAP payment data - EAGF and EAFRD funds"
    frequency: str = "yearly"
    bucket: str = (
        os.getenv("STORAGE_BUCKET")
        or os.getenv("R2_BUCKET")
        or os.getenv("GCS_BUCKET", "landbruget-data")
    )

    # Bronze source
    bronze_path: str = Field(
        default="bronze/subsidies/stoetteoplysninger.naturerhverv.dk_*.parquet",
        description="Storage path pattern for bronze data",
    )

    # Column mappings (source → standardized)
    # The CVR column in støtteoplysninger is "vat_or_tax_identification_number"
    cvr_source_column: str = Field(
        default="vat_or_tax_identification_number", description="Source CVR column name"
    )

    scheme_column: str = Field(default="Scheme", description="Scheme name column")

    # Amount columns
    eagf_column: str = Field(
        default="Amounts (€) EAGF (Pillar I)", description="EAGF amount column (EUR)"
    )
    eafrd_column: str = Field(
        default="Amounts (€) EAFRD (Pillar II)", description="EAFRD amount column (EUR)"
    )
    national_column: str = Field(
        default="Amounts (€) National financing", description="National co-financing column (EUR)"
    )

    # Fiscal year column
    year_column: str = Field(default="Year", description="Fiscal year column")

    # EUR to DKK conversion rate (approximate 2023 average)
    eur_to_dkk: float = Field(default=7.45, description="EUR to DKK conversion rate")

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class StoetteoplysningerSilver(BaseSource[StoetteoplysningerSilverConfig], SilverJobInterface):
    """
    Silver layer processor for EU CAP payment data.

    Transforms raw støtteoplysninger data into analysis-ready format:
    - Normalizes CVR numbers to 8-digit format
    - Classifies schemes to standardized codes
    - Flags summary rows to prevent double-counting
    - Converts EUR amounts to DKK
    """

    def __init__(self, config: StoetteoplysningerSilverConfig) -> None:
        super().__init__(config)
        self.log = Logger.get_logger()

    @timed
    async def run(self, bronze_data: Any | None = None) -> dict[str, Any] | None:
        """
        Process støtteoplysninger bronze data to silver.

        Args:
            bronze_data: Optional in-memory bronze data (dict or DataFrame)

        Returns:
            Dictionary with processing summary and output path
        """
        self.log.info("Starting støtteoplysninger silver processing")

        try:
            # Load bronze data
            await self._load_bronze_data(bronze_data)

            # Transform data
            await self._transform_data()

            # Validate results
            summary = await self._validate_and_summarize()

            # Save to silver layer
            output_path = await self._save_silver_data()

            self.log.info(f"Støtteoplysninger silver processing complete: {output_path}")

            return {
                "dataset": self.config.dataset,
                "output_path": output_path,
                "summary": summary,
            }

        except Exception as e:
            self.log.error(f"Error processing støtteoplysninger: {e}")
            raise

    async def _load_bronze_data(self, bronze_data: Any | None = None) -> None:
        """Load bronze data from cloud storage or memory."""
        if bronze_data is not None:
            self.log.info("Using in-memory bronze data")
            if hasattr(bronze_data, "to_pandas"):
                # DuckDB relation
                self.conn.register("raw_stoetteoplysninger", bronze_data)
            else:
                # DataFrame or dict
                self.conn.register("raw_stoetteoplysninger", bronze_data)
        else:
            # Load from cloud storage
            self.log.info(f"Loading bronze data from: {self.config.bronze_path}")
            pattern = f"{self.config.bucket}/{self.config.bronze_path}"

            # Find latest file
            files = self.storage.list_files(pattern)
            if not files:
                raise FileNotFoundError(f"No bronze files found matching: {pattern}")

            latest_file = sorted(files)[-1]  # Most recent by name
            self.log.info(f"Loading: {latest_file}")

            self.conn.execute(f"""
                CREATE OR REPLACE TABLE raw_stoetteoplysninger AS
                SELECT * FROM read_parquet('{latest_file}')
            """)

        # Log row count
        count = self.conn.execute("SELECT COUNT(*) FROM raw_stoetteoplysninger").fetchone()[0]
        self.log.info(f"Loaded {count:,} rows from bronze støtteoplysninger")

    async def _transform_data(self) -> None:
        """Transform raw data to silver schema."""
        self.log.info("Transforming støtteoplysninger data")

        # Register CVR normalization function
        self.conn.create_function("normalize_cvr_udf", normalize_cvr, [str], str)

        # Register scheme classification functions
        def classify_scheme_code(scheme: str) -> str:
            code, _ = classify_eu_scheme(scheme)
            return code or "UKENDT"

        def classify_scheme_danish(scheme: str) -> str:
            _, danish = classify_eu_scheme(scheme)
            return danish or "Ukendt ordning"

        def detect_summary_row(scheme: str) -> bool:
            return is_summary_row(scheme)

        def get_scheme_pillar(code: str) -> int:
            pillar = get_pillar(code)
            return pillar if pillar else 0

        self.conn.create_function("classify_scheme_code", classify_scheme_code, [str], str)
        self.conn.create_function("classify_scheme_danish", classify_scheme_danish, [str], str)
        self.conn.create_function("detect_summary_row", detect_summary_row, [str], bool)
        self.conn.create_function("get_scheme_pillar", get_scheme_pillar, [str], int)

        # Get column names from raw table
        columns = [
            row[0] for row in self.conn.execute("DESCRIBE raw_stoetteoplysninger").fetchall()
        ]
        self.log.info(f"Raw columns: {columns[:10]}...")

        # Build transformation SQL
        # Handle column name variations
        cvr_col = self._find_column(
            columns, ["vat_or_tax_identification_number", "CVR", "cvr_number"]
        )
        scheme_col = self._find_column(columns, ["Scheme", "scheme", "ordning"])
        year_col = self._find_column(columns, ["Year", "year", "regnskabsaar"])
        eagf_col = self._find_column(columns, ["Amounts (€) EAGF (Pillar I)", "EAGF", "eagf"])
        eafrd_col = self._find_column(columns, ["Amounts (€) EAFRD (Pillar II)", "EAFRD", "eafrd"])
        national_col = self._find_column(
            columns, ["Amounts (€) National financing", "national", "medfinansiering"]
        )

        self.log.info(f"Using columns: cvr={cvr_col}, scheme={scheme_col}, year={year_col}")

        # Create silver table with transformations
        transform_sql = f"""
        CREATE OR REPLACE TABLE stoetteoplysninger_silver AS
        SELECT
            -- Normalized CVR (8-digit)
            normalize_cvr_udf(CAST("{cvr_col}" AS VARCHAR)) AS cvr,

            -- Fiscal year
            CAST("{year_col}" AS INTEGER) AS regnskabsaar,

            -- Scheme classification
            classify_scheme_code("{scheme_col}") AS ordning_kode,
            classify_scheme_danish("{scheme_col}") AS ordning_dansk,
            "{scheme_col}" AS scheme_original,

            -- Pillar (1 = EAGF, 2 = EAFRD)
            get_scheme_pillar(classify_scheme_code("{scheme_col}")) AS pillar,

            -- Amounts in DKK (convert from EUR)
            COALESCE(TRY_CAST("{eagf_col}" AS DOUBLE), 0) * {self.config.eur_to_dkk} AS eagf_dkk,
            COALESCE(TRY_CAST("{eafrd_col}" AS DOUBLE), 0) * {self.config.eur_to_dkk} AS eafrd_dkk,
            COALESCE(TRY_CAST("{national_col}" AS DOUBLE), 0) * {self.config.eur_to_dkk} AS medfinansiering_dkk,

            -- Summary row flag (CRITICAL: filter these out for aggregations!)
            detect_summary_row("{scheme_col}") AS is_summary_row,

            -- Total payment (for convenience)
            (
                COALESCE(TRY_CAST("{eagf_col}" AS DOUBLE), 0) +
                COALESCE(TRY_CAST("{eafrd_col}" AS DOUBLE), 0) +
                COALESCE(TRY_CAST("{national_col}" AS DOUBLE), 0)
            ) * {self.config.eur_to_dkk} AS total_dkk,

            -- Processing metadata
            CURRENT_TIMESTAMP AS processed_at

        FROM raw_stoetteoplysninger
        WHERE "{cvr_col}" IS NOT NULL
        """

        self.conn.execute(transform_sql)

        # Log transformation stats
        total = self.conn.execute("SELECT COUNT(*) FROM stoetteoplysninger_silver").fetchone()[0]
        valid_cvr = self.conn.execute(
            "SELECT COUNT(*) FROM stoetteoplysninger_silver WHERE cvr ~ '^\\d{8}$'"
        ).fetchone()[0]
        summary_rows = self.conn.execute(
            "SELECT COUNT(*) FROM stoetteoplysninger_silver WHERE is_summary_row = TRUE"
        ).fetchone()[0]

        self.log.info(
            f"Transformed {total:,} rows: {valid_cvr:,} valid CVRs, {summary_rows:,} summary rows"
        )

    def _find_column(self, columns: list, candidates: list) -> str:
        """Find a column from a list of candidate names."""
        for candidate in candidates:
            if candidate in columns:
                return candidate
            # Case-insensitive search
            for col in columns:
                if col.lower() == candidate.lower():
                    return col
        raise ValueError(f"Could not find column from candidates: {candidates}")

    async def _validate_and_summarize(self) -> dict[str, Any]:
        """Validate transformed data and generate summary."""
        self.log.info("Validating støtteoplysninger silver data")

        # Get summary statistics
        stats = self.conn.execute("""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT cvr) AS unique_cvrs,
            COUNT(CASE WHEN is_summary_row = FALSE THEN 1 END) AS detail_rows,
            COUNT(CASE WHEN is_summary_row = TRUE THEN 1 END) AS summary_rows,
            SUM(CASE WHEN is_summary_row = FALSE THEN eagf_dkk ELSE 0 END) AS total_eagf_dkk,
            SUM(CASE WHEN is_summary_row = FALSE THEN eafrd_dkk ELSE 0 END) AS total_eafrd_dkk,
            SUM(CASE WHEN is_summary_row = FALSE THEN total_dkk ELSE 0 END) AS total_payments_dkk,
            MIN(regnskabsaar) AS min_year,
            MAX(regnskabsaar) AS max_year
        FROM stoetteoplysninger_silver
        WHERE cvr ~ '^\\d{8}$'
        """).fetchone()

        summary = {
            "total_rows": stats[0],
            "unique_cvrs": stats[1],
            "detail_rows": stats[2],
            "summary_rows": stats[3],
            "total_eagf_dkk": round(stats[4], 2),
            "total_eafrd_dkk": round(stats[5], 2),
            "total_payments_dkk": round(stats[6], 2),
            "year_range": f"{stats[7]}-{stats[8]}",
        }

        # Validate ratios
        eagf_eafrd_ratio = stats[4] / stats[5] if stats[5] > 0 else 0
        summary["eagf_eafrd_ratio"] = round(eagf_eafrd_ratio, 2)

        # Expected ratio is ~7.3:1 (Pillar 1 vs Pillar 2)
        if 5.0 <= eagf_eafrd_ratio <= 10.0:
            self.log.info(
                f"EAGF/EAFRD ratio {eagf_eafrd_ratio:.1f}:1 within expected range (5-10:1)"
            )
        else:
            self.log.warning(f"EAGF/EAFRD ratio {eagf_eafrd_ratio:.1f}:1 outside expected range!")

        # Get scheme breakdown
        scheme_counts = self.conn.execute("""
        SELECT ordning_kode, COUNT(*) as count, SUM(total_dkk) as total_dkk
        FROM stoetteoplysninger_silver
        WHERE is_summary_row = FALSE
        GROUP BY ordning_kode
        ORDER BY total_dkk DESC
        LIMIT 10
        """).fetchall()

        summary["top_schemes"] = [
            {"ordning_kode": row[0], "count": row[1], "total_dkk": round(row[2], 0)}
            for row in scheme_counts
        ]

        self.log.info(
            f"Validation summary: {summary['unique_cvrs']:,} unique CVRs, "
            f"{summary['total_payments_dkk'] / 1e9:.2f}B DKK total payments"
        )

        return summary

    async def _save_silver_data(self) -> str:
        """Save silver data to cloud storage."""
        output_path = f"silver/{self.config.dataset}"

        self.log.info(f"Saving silver data to: {self.config.bucket}/{output_path}")

        # Export to parquet
        self._save_data(
            data="stoetteoplysninger_silver",
            dataset=self.config.dataset,
            bucket=self.config.bucket,
            stage="silver",
        )

        return f"{self.config.bucket}/{output_path}"
