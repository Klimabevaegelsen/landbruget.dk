"""
Gold layer processor for Spatial Subsidies (Tilsagn Arealer).

Unifies FVM organic, grassland, and environmental subsidies into a single
analysis-ready table with expected payment calculations.

Sources (from FVM WFS silver):
- fvm_organic_subsidies: Organic farming commitments (~46K rows)
- fvm_grassland_subsidies: Grassland management commitments (~30K rows)
- fvm_environmental_subsidies: Environmental commitments (~52 rows)

Output table: landbrugstoette_tilsagn_arealer
"""

import os
from typing import Any

from pydantic import ConfigDict, Field

from unified_pipeline.common.base import BaseJobConfig, BaseSource, GoldJobInterface
from unified_pipeline.util.cvr_normalizer import normalize_cvr
from unified_pipeline.util.log_util import Logger
from unified_pipeline.util.subsidy_scheme_classifier import DERIVED_PAYMENT_RATES
from unified_pipeline.util.timing import timed


class TilsagnArealerGoldConfig(BaseJobConfig):
    """Configuration for spatial subsidies gold layer."""

    name: str = "Tilsagn Arealer Gold"
    dataset: str = "landbrugstoette_tilsagn_arealer"
    type: str = "gold"
    description: str = "Unified spatial subsidies - organic, grassland, environmental"
    frequency: str = "yearly"
    bucket: str = (
        os.getenv("STORAGE_BUCKET")
        or os.getenv("R2_BUCKET")
        or os.getenv("GCS_BUCKET", "landbruget-data")
    )

    # Silver input datasets (from FVM WFS processor)
    organic_dataset: str = Field(
        default="fvm_organic_subsidies",
        description="FVM organic subsidies silver dataset",
    )
    grassland_dataset: str = Field(
        default="fvm_grassland_subsidies",
        description="FVM grassland subsidies silver dataset",
    )
    environmental_dataset: str = Field(
        default="fvm_environmental_subsidies",
        description="FVM environmental subsidies silver dataset",
    )

    # Target year (optional - if not set, processes all years)
    target_year: int | None = Field(
        default=None,
        description="Specific year to process (if None, processes all)",
    )

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class TilsagnArealerGold(BaseSource[TilsagnArealerGoldConfig], GoldJobInterface):
    """
    Gold layer processor for spatial subsidies.

    Unifies three FVM subsidy datasets:
    - Organic (økologisk arealtilskud)
    - Grassland (pleje af græs- og naturarealer)
    - Environmental (miljøvenlig drift)

    Adds expected payment calculations using data-derived rates.
    """

    def __init__(self, config: TilsagnArealerGoldConfig) -> None:
        super().__init__(config)
        self.log = Logger.get_logger()

        self.conn.execute("SET memory_limit = '8GB'")
        self.conn.execute("SET threads = 4")
        self.conn.execute("SET temp_directory = '/tmp'")
        self.conn.execute("SET s3_region = 'europe-west1'")

    @timed
    async def run(self, silver_data: dict[str, Any] | None = None) -> dict[str, str] | None:
        """Execute spatial subsidies gold processing."""
        self.log.info("Starting Tilsagn Arealer Gold processing")

        try:
            # Load all three subsidy types
            await self._load_silver_data(silver_data)

            # Unify into single table
            await self._create_unified_table()

            # Create CVR summary
            await self._create_cvr_summary()

            # Validate
            validation = await self._validate_data()

            # Save
            output_paths = await self._save_gold_data()

            self.log.info("Tilsagn Arealer Gold processing completed")

            return {
                "dataset": self.config.dataset,
                "output_paths": output_paths,
                "validation": validation,
            }

        except Exception as e:
            self.log.error(f"Error in Tilsagn Arealer Gold: {e}")
            raise

    async def _load_silver_data(self, silver_data: dict[str, Any] | None = None) -> None:
        """Load silver FVM subsidy data."""
        self.log.info("Loading silver FVM subsidy data")

        # Register CVR normalization function
        self.conn.create_function("normalize_cvr_udf", normalize_cvr, [str], str)

        # Load each subsidy type
        await self._load_fvm_dataset("organic_silver", self.config.organic_dataset, silver_data)
        await self._load_fvm_dataset("grassland_silver", self.config.grassland_dataset, silver_data)
        await self._load_fvm_dataset(
            "environmental_silver", self.config.environmental_dataset, silver_data
        )

    async def _load_fvm_dataset(
        self, table_name: str, dataset: str, silver_data: dict[str, Any] | None
    ) -> None:
        """Load a single FVM silver dataset."""
        if silver_data and dataset in silver_data:
            self.log.info(f"Using in-memory silver data for {dataset}")
            self.conn.register(table_name, silver_data[dataset])
        else:
            # Load from GCS - FVM data is organized by year
            pattern = f"{self.config.bucket}/silver/{dataset}/**/*.parquet"
            self.log.info(f"Loading {dataset} from: {pattern}")

            files = self.storage.list_files(pattern)
            if not files:
                self.log.warning(f"No files found for {dataset}, creating empty table")
                self.conn.execute(f"""
                    CREATE OR REPLACE TABLE {table_name} AS
                    SELECT
                        NULL::VARCHAR AS cvr_number,
                        NULL::VARCHAR AS field_id,
                        NULL::DOUBLE AS area_ha,
                        NULL::VARCHAR AS subsidy_measure,
                        NULL::VARCHAR AS subsidy_type_code,
                        NULL::VARCHAR AS start_date,
                        NULL::VARCHAR AS end_date,
                        NULL::INTEGER AS year
                    WHERE FALSE
                """)
                return

            # Load all parquet files (or filter by year if specified)
            if self.config.target_year:
                files = [f for f in files if str(self.config.target_year) in f]

            if not files:
                self.log.warning(f"No files for {dataset} in target year {self.config.target_year}")
                return

            # Load files
            latest_file = sorted(files)[-1]
            self.log.info(f"Loading: {latest_file}")

            with self.storage._temp_download(latest_file) as temp_file:
                self.conn.execute(f"""
                    CREATE OR REPLACE TABLE {table_name} AS
                    SELECT * FROM read_parquet('{temp_file}')
                """)

        try:
            count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            self.log.info(f"Loaded {count:,} rows for {dataset}")
        except Exception:
            self.log.warning(f"Could not count rows for {table_name}")

    async def _create_unified_table(self) -> None:
        """Create unified tilsagn_arealer table from all three sources."""
        self.log.info("Creating unified tilsagn_arealer table")

        # Extract tilsagn code from subsidy_measure (e.g., "37 Basistilskud" -> "37")
        def extract_tilsagn_code(measure: str) -> str:
            if not measure:
                return None
            # Try to extract leading number (e.g., "37", "66", "67")
            parts = str(measure).strip().split()
            if parts and parts[0].isdigit():
                return parts[0]
            return None

        self.conn.create_function("extract_tilsagn_code", extract_tilsagn_code, [str], str)

        # Get derived rates for lookup
        rates = DERIVED_PAYMENT_RATES

        # Create rate lookup function
        def get_rate(tilsagn_type: str, tilsagn_code: str) -> float:
            if tilsagn_type == "OEKOLOGISK":
                key = f"OEKOLOGISK_{tilsagn_code}" if tilsagn_code else "OEKOLOGISK_36"
                return rates.get(key, 868)  # Default to 36 rate
            if tilsagn_type == "GRAESPLEJE":
                key = f"GRAESPLEJE_{tilsagn_code}" if tilsagn_code else "GRAESPLEJE_67"
                return rates.get(key, 1650)  # Default to 67 rate
            if tilsagn_type == "MILJOE":
                key = f"MILJOE_{tilsagn_code}" if tilsagn_code else "MILJOE_70"
                return rates.get(key, 1200)
            return 0

        self.conn.create_function("get_rate", get_rate, [str, str], float)

        # Unify all three sources
        self.conn.execute("""
        CREATE OR REPLACE TABLE landbrugstoette_tilsagn_arealer AS

        -- Organic subsidies
        SELECT
            normalize_cvr_udf(cvr_number) AS cvr,
            field_id,
            'OEKOLOGISK' AS tilsagnstype,
            extract_tilsagn_code(subsidy_measure) AS tilsagn_kode,
            subsidy_measure AS tilsagn_beskrivelse,
            subsidy_type_code AS tilsagn_type_original,
            CAST(area_ha AS DOUBLE) AS areal_ha,
            get_rate('OEKOLOGISK', extract_tilsagn_code(subsidy_measure)) AS forventet_sats_kr_ha,
            CAST(area_ha AS DOUBLE) * get_rate('OEKOLOGISK', extract_tilsagn_code(subsidy_measure)) AS forventet_betaling_dkk,
            start_date AS tilsagn_start,
            end_date AS tilsagn_slut,
            CAST(year AS INTEGER) AS dataar,
            geometry,
            'fvm_organic_subsidies' AS kilde,
            CURRENT_TIMESTAMP AS gold_processed_at
        FROM organic_silver
        WHERE cvr_number IS NOT NULL

        UNION ALL

        -- Grassland subsidies
        SELECT
            normalize_cvr_udf(cvr_number) AS cvr,
            field_id,
            'GRAESPLEJE' AS tilsagnstype,
            extract_tilsagn_code(subsidy_measure) AS tilsagn_kode,
            subsidy_measure AS tilsagn_beskrivelse,
            subsidy_type_code AS tilsagn_type_original,
            CAST(area_ha AS DOUBLE) AS areal_ha,
            get_rate('GRAESPLEJE', extract_tilsagn_code(subsidy_measure)) AS forventet_sats_kr_ha,
            CAST(area_ha AS DOUBLE) * get_rate('GRAESPLEJE', extract_tilsagn_code(subsidy_measure)) AS forventet_betaling_dkk,
            start_date AS tilsagn_start,
            end_date AS tilsagn_slut,
            CAST(year AS INTEGER) AS dataar,
            geometry,
            'fvm_grassland_subsidies' AS kilde,
            CURRENT_TIMESTAMP AS gold_processed_at
        FROM grassland_silver
        WHERE cvr_number IS NOT NULL

        UNION ALL

        -- Environmental subsidies
        SELECT
            normalize_cvr_udf(cvr_number) AS cvr,
            field_id,
            'MILJOE' AS tilsagnstype,
            subsidy_type_code AS tilsagn_kode,  -- Already numeric for environmental
            subsidy_measure AS tilsagn_beskrivelse,
            subsidy_type_code AS tilsagn_type_original,
            CAST(area_ha AS DOUBLE) AS areal_ha,
            get_rate('MILJOE', subsidy_type_code) AS forventet_sats_kr_ha,
            CAST(area_ha AS DOUBLE) * get_rate('MILJOE', subsidy_type_code) AS forventet_betaling_dkk,
            start_date AS tilsagn_start,
            end_date AS tilsagn_slut,
            CAST(year AS INTEGER) AS dataar,
            geometry,
            'fvm_environmental_subsidies' AS kilde,
            CURRENT_TIMESTAMP AS gold_processed_at
        FROM environmental_silver
        WHERE cvr_number IS NOT NULL
        """)

        # Filter to valid CVRs
        self.conn.execute("""
        DELETE FROM landbrugstoette_tilsagn_arealer
        WHERE cvr IS NULL OR cvr !~ '^\\d{8}$'
        """)

        # Get counts by type
        counts = self.conn.execute("""
        SELECT tilsagnstype, COUNT(*) as cnt, SUM(areal_ha) as total_ha
        FROM landbrugstoette_tilsagn_arealer
        GROUP BY tilsagnstype
        """).fetchall()

        for row in counts:
            self.log.info(f"  {row[0]}: {row[1]:,} rows, {row[2]:,.0f} ha")

        total = self.conn.execute(
            "SELECT COUNT(*) FROM landbrugstoette_tilsagn_arealer"
        ).fetchone()[0]
        self.log.info(f"Created unified table: {total:,} total rows")

    async def _create_cvr_summary(self) -> None:
        """Create CVR-level summary of spatial subsidies."""
        self.log.info("Creating CVR summary for spatial subsidies")

        self.conn.execute("""
        CREATE OR REPLACE TABLE landbrugstoette_tilsagn_cvr_summary AS
        SELECT
            cvr,
            dataar,

            -- Organic
            SUM(CASE WHEN tilsagnstype = 'OEKOLOGISK' THEN areal_ha ELSE 0 END) AS oekologisk_areal_ha,
            SUM(CASE WHEN tilsagnstype = 'OEKOLOGISK' THEN forventet_betaling_dkk ELSE 0 END) AS oekologisk_forventet_dkk,
            COUNT(CASE WHEN tilsagnstype = 'OEKOLOGISK' THEN 1 END) AS oekologisk_antal_marker,

            -- Grassland
            SUM(CASE WHEN tilsagnstype = 'GRAESPLEJE' THEN areal_ha ELSE 0 END) AS graespleje_areal_ha,
            SUM(CASE WHEN tilsagnstype = 'GRAESPLEJE' THEN forventet_betaling_dkk ELSE 0 END) AS graespleje_forventet_dkk,
            COUNT(CASE WHEN tilsagnstype = 'GRAESPLEJE' THEN 1 END) AS graespleje_antal_marker,

            -- Environmental
            SUM(CASE WHEN tilsagnstype = 'MILJOE' THEN areal_ha ELSE 0 END) AS miljoe_areal_ha,
            SUM(CASE WHEN tilsagnstype = 'MILJOE' THEN forventet_betaling_dkk ELSE 0 END) AS miljoe_forventet_dkk,
            COUNT(CASE WHEN tilsagnstype = 'MILJOE' THEN 1 END) AS miljoe_antal_marker,

            -- Totals
            SUM(areal_ha) AS total_tilsagn_areal_ha,
            SUM(forventet_betaling_dkk) AS total_forventet_dkk,
            COUNT(*) AS total_antal_marker,

            CURRENT_TIMESTAMP AS gold_processed_at

        FROM landbrugstoette_tilsagn_arealer
        GROUP BY cvr, dataar
        """)

        count = self.conn.execute(
            "SELECT COUNT(*) FROM landbrugstoette_tilsagn_cvr_summary"
        ).fetchone()[0]
        self.log.info(f"Created CVR summary: {count:,} CVR-year combinations")

    async def _validate_data(self) -> dict[str, Any]:
        """Validate spatial subsidies data."""
        self.log.info("Validating spatial subsidies data")

        # Overall stats
        stats = self.conn.execute("""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT cvr) AS unique_cvrs,
            SUM(areal_ha) AS total_areal_ha,
            SUM(forventet_betaling_dkk) AS total_forventet_dkk,
            COUNT(DISTINCT tilsagnstype) AS tilsagn_types,
            MIN(dataar) AS min_year,
            MAX(dataar) AS max_year
        FROM landbrugstoette_tilsagn_arealer
        """).fetchone()

        validation = {
            "total_rows": stats[0] or 0,
            "unique_cvrs": stats[1] or 0,
            "total_areal_ha": round(stats[2] or 0, 2),
            "total_forventet_dkk": round(stats[3] or 0, 2),
            "tilsagn_types": stats[4] or 0,
            "year_range": f"{stats[5]}-{stats[6]}" if stats[5] else "N/A",
        }

        # Breakdown by type
        type_stats = self.conn.execute("""
        SELECT
            tilsagnstype,
            COUNT(DISTINCT cvr) AS unique_cvrs,
            COUNT(*) AS rows,
            SUM(areal_ha) AS total_ha,
            SUM(forventet_betaling_dkk) AS forventet_dkk,
            AVG(forventet_sats_kr_ha) AS avg_sats
        FROM landbrugstoette_tilsagn_arealer
        GROUP BY tilsagnstype
        ORDER BY total_ha DESC
        """).fetchall()

        validation["by_type"] = [
            {
                "tilsagnstype": row[0],
                "unique_cvrs": row[1],
                "rows": row[2],
                "total_ha": round(row[3] or 0, 0),
                "forventet_dkk": round(row[4] or 0, 0),
                "avg_sats_kr_ha": round(row[5] or 0, 0),
            }
            for row in type_stats
        ]

        # Log summary
        self.log.info(
            f"Validation: {validation['unique_cvrs']:,} CVRs, "
            f"{validation['total_areal_ha']:,.0f} ha, "
            f"{validation['total_forventet_dkk'] / 1e6:.1f}M DKK expected"
        )

        for t in validation["by_type"]:
            self.log.info(
                f"  {t['tilsagnstype']}: {t['unique_cvrs']:,} CVRs, "
                f"{t['total_ha']:,.0f} ha, {t['avg_sats_kr_ha']} kr/ha avg"
            )

        return validation

    async def _save_gold_data(self) -> dict[str, str]:
        """Save gold tables to GCS."""
        self.log.info("Saving spatial subsidies gold data")

        output_paths = {}

        # Save detail table (without geometry for now - geometry is large)
        self._save_data(
            data="landbrugstoette_tilsagn_arealer",
            dataset=self.config.dataset,
            bucket=self.config.bucket,
            stage="gold",
        )
        output_paths["detail"] = f"{self.config.bucket}/gold/{self.config.dataset}"

        # Save CVR summary
        self._save_data(
            data="landbrugstoette_tilsagn_cvr_summary",
            dataset=f"{self.config.dataset}_cvr_summary",
            bucket=self.config.bucket,
            stage="gold",
        )
        output_paths["cvr_summary"] = f"{self.config.bucket}/gold/{self.config.dataset}_cvr_summary"

        self.log.info(f"Saved gold data: {output_paths}")

        return output_paths
