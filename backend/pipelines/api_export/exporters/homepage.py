"""Homepage exporter — generates statistics and rankings JSON files.

Replaces:
- Supabase edge function: homepage-rankings (1711 lines, 25 ranking tables)
- Supabase edge function: homepage-statistics

Output files:
- homepage/statistics.json
- homepage/rankings/all.json
- homepage/rankings/{category}.json (financial, field, environment, worker)

Actual R2 data sources:
- silver/cvr_companies/data.parquet — company master data (61k rows)
- gold/cvr_enrichment_financial_statements/*/financial_statements.parquet — financials (5.6k rows)
- gold/field_production/latest/data.parquet — field/crop data (10.8M rows, 2008-2025)
- gold/pesticide_disaggregation_2023_2024/*/pesticide_disaggregation_2023_2024.parquet — pesticides (1.5M rows)
- gold/worker_safety/*/worker_safety_clean.parquet — worker safety (1.2k rows)
- gold/work_permits/*/work_permits.parquet — work permits (8.7k rows)
"""

import os
from collections.abc import Callable
from datetime import UTC, datetime

from common.logging_utils import get_pipeline_logger

from exporters.base import BaseExporter

logger = get_pipeline_logger("api_export.homepage")

BUCKET = os.getenv("R2_BUCKET") or os.getenv("GCS_BUCKET") or "landbruget-data"


def _r2_path(path: str) -> str:
    """Build r2:// path for DuckDB."""
    return f"r2://{BUCKET}/{path}"


class HomepageExporter(BaseExporter):
    """Generate homepage statistics and ranking tables."""

    def export(self) -> dict:
        stats = {}

        self._load_base_tables()

        # Generate statistics
        self._export_statistics()
        stats["statistics"] = 1

        # Generate rankings by category
        all_rankings = []

        financial = self._financial_rankings()
        all_rankings.extend(financial)
        self.write_json(self._wrap_rankings(financial), "homepage/rankings/financial.json")
        stats["financial"] = len(financial)

        field = self._field_rankings()
        all_rankings.extend(field)
        self.write_json(self._wrap_rankings(field), "homepage/rankings/field.json")
        stats["field"] = len(field)

        environment = self._environment_rankings()
        all_rankings.extend(environment)
        self.write_json(self._wrap_rankings(environment), "homepage/rankings/environment.json")
        stats["environment"] = len(environment)

        worker = self._worker_rankings()
        all_rankings.extend(worker)
        self.write_json(self._wrap_rankings(worker), "homepage/rankings/worker.json")
        stats["worker"] = len(worker)

        # Animal category placeholder (no animal-specific datasets on R2 yet)
        self.write_json(self._wrap_rankings([]), "homepage/rankings/animal.json")

        # Write combined file
        self.write_json(self._wrap_rankings(all_rankings), "homepage/rankings/all.json")
        stats["total_rankings"] = len(all_rankings)
        stats["files_written"] = 7  # statistics + 5 categories + all

        return stats

    def _wrap_rankings(self, rankings: list[dict]) -> dict:
        return {
            "rankings": rankings,
            "metadata": {
                "generated_at": datetime.now(UTC).isoformat(),
                "total_tables": len(rankings),
            },
        }

    def _load_base_tables(self) -> None:
        """Load actual R2 parquet datasets into DuckDB tables."""
        tables = {
            "companies": _r2_path("gold/cvr_enrichment_companies/data.parquet"),
            "financials": _r2_path(
                "gold/cvr_enrichment_financial_statements/20260314_083906/financial_statements.parquet"
            ),
            "field_production": _r2_path("gold/field_production/latest/data.parquet"),
            "pesticides": _r2_path(
                "gold/pesticide_disaggregation_2023_2024/20260317_074432/pesticide_disaggregation_2023_2024.parquet"
            ),
            "worker_safety": _r2_path(
                "gold/worker_safety/20260317_074628/worker_safety_clean.parquet"
            ),
            "work_permits": _r2_path("gold/work_permits/20260317_074636/work_permits.parquet"),
        }

        for name, path in tables.items():
            try:
                self.load_parquet_table(path, name)
            except Exception:
                logger.warning(
                    f"Could not load {name} from {path} — rankings using it will be skipped"
                )

    def _table_exists(self, name: str) -> bool:
        try:
            self.conn.execute(f"SELECT 1 FROM {name} LIMIT 0")
            return True
        except Exception:
            return False

    def _export_statistics(self) -> None:
        """Generate homepage/statistics.json."""
        stats = {
            "total_companies": 0,
            "total_data_points": 0,
            "last_updated": datetime.now(UTC).isoformat(),
        }

        if self._table_exists("companies"):
            stats["total_companies"] = self.conn.execute(
                "SELECT count(*) FROM companies"
            ).fetchone()[0]

        # Count total data points across all loaded tables
        total = 0
        for table in [
            "companies",
            "financials",
            "field_production",
            "pesticides",
            "worker_safety",
            "work_permits",
        ]:
            if self._table_exists(table):
                total += self.conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        stats["total_data_points"] = total

        stats["formatted"] = {
            "companies": f"{stats['total_companies']:,}".replace(",", "."),
            "data_points": _format_danish_number(stats["total_data_points"]),
        }

        self.write_json(stats, "homepage/statistics.json")

    # -------------------------------------------------------------------------
    # FINANCIAL RANKINGS
    # -------------------------------------------------------------------------
    def _financial_rankings(self) -> list[dict]:
        rankings = []
        if not self._table_exists("financials") or not self._table_exists("companies"):
            logger.warning("Skipping financial rankings — missing tables")
            return rankings

        # Join financials with companies for name/municipality
        # Use field_production as agricultural filter (is_agricultural_company flag is too restrictive: only 346 of 61k)
        # Companies with field data = actually farming = 133k unique CVRs
        self.conn.execute("""
            CREATE OR REPLACE VIEW financial_with_company AS
            SELECT
                f.cvr_number,
                c.company_name,
                c.current_municipality_name AS municipality,
                f.net_profit_loss,
                f.total_assets,
                f.average_number_of_employees,
                f.total_equity,
                f.equity_ratio,
                f.return_on_assets
            FROM financials f
            JOIN companies c ON f.cvr_number = c.cvr_number
            WHERE f.cvr_number::VARCHAR IN (
                SELECT DISTINCT cvr_number FROM field_production
            )
        """)

        # 1. Highest Profit
        rankings.append(
            self._ranking_from_sql(
                sql="""
                SELECT cvr_number, company_name, municipality,
                       net_profit_loss AS value
                FROM financial_with_company
                WHERE net_profit_loss IS NOT NULL AND net_profit_loss > 0
                ORDER BY net_profit_loss DESC LIMIT 50
            """,
                count_sql="""
                SELECT count(*) FROM financial_with_company
                WHERE net_profit_loss IS NOT NULL AND net_profit_loss > 0
            """,
                id="highest_profit",
                title="Højest Overskud",
                category="financial",
                description="Landbrugsvirksomheder med det højeste nettoresultat",
                unit="DKK",
                format_fn=lambda v: f"{v / 1_000_000:.1f}M kr",
            )
        )

        # 2. Largest Assets
        rankings.append(
            self._ranking_from_sql(
                sql="""
                SELECT cvr_number, company_name, municipality,
                       total_assets AS value
                FROM financial_with_company
                WHERE total_assets IS NOT NULL AND total_assets > 0
                ORDER BY total_assets DESC LIMIT 50
            """,
                count_sql="""
                SELECT count(*) FROM financial_with_company
                WHERE total_assets IS NOT NULL AND total_assets > 0
            """,
                id="largest_assets",
                title="Størst Aktiver",
                category="financial",
                description="Landbrugsvirksomheder med de største samlede aktiver",
                unit="DKK",
                format_fn=lambda v: f"{v / 1_000_000:.1f}M kr",
            )
        )

        # 3. Most Employees (Financial)
        rankings.append(
            self._ranking_from_sql(
                sql="""
                SELECT cvr_number, company_name, municipality,
                       average_number_of_employees AS value
                FROM financial_with_company
                WHERE average_number_of_employees IS NOT NULL AND average_number_of_employees > 0
                ORDER BY average_number_of_employees DESC LIMIT 50
            """,
                count_sql="""
                SELECT count(*) FROM financial_with_company
                WHERE average_number_of_employees IS NOT NULL AND average_number_of_employees > 0
            """,
                id="most_employees_financial",
                title="Flest Ansatte",
                category="financial",
                description="Landbrugsvirksomheder med flest ansatte ifølge regnskabsdata",
                unit="ansatte",
                format_fn=lambda v: f"{int(v)} ansatte",
            )
        )

        return [r for r in rankings if r is not None]

    # -------------------------------------------------------------------------
    # FIELD RANKINGS
    # -------------------------------------------------------------------------
    def _field_rankings(self) -> list[dict]:
        rankings = []
        if not self._table_exists("field_production") or not self._table_exists("companies"):
            logger.warning("Skipping field rankings — missing tables")
            return rankings

        # field_production has: cvr_number (VARCHAR), year, area_ha, crop_type, organic_farming, kommune_name
        # Aggregate per company for latest year
        self.conn.execute("""
            CREATE OR REPLACE TABLE land_use_summary AS
            SELECT
                fp.cvr_number,
                c.company_name,
                fp.kommune_name AS municipality,
                fp.year,
                ROUND(SUM(fp.area_ha), 1) AS total_area_ha,
                ROUND(SUM(CASE WHEN fp.organic_farming THEN fp.area_ha ELSE 0 END), 1) AS organic_area_ha,
                ROUND(100.0 * SUM(CASE WHEN fp.organic_farming THEN fp.area_ha ELSE 0 END)
                    / NULLIF(SUM(fp.area_ha), 0), 1) AS organic_percentage,
                COUNT(*) AS total_fields,
                COUNT(DISTINCT fp.crop_type) AS unique_crops
            FROM field_production fp
            JOIN companies c ON fp.cvr_number = c.cvr_number::VARCHAR
            WHERE fp.year = 2024
            GROUP BY fp.cvr_number, c.company_name, fp.kommune_name, fp.year
        """)

        # 4. Largest Agricultural Area
        rankings.append(
            self._ranking_from_sql(
                sql="SELECT cvr_number, company_name, municipality, total_area_ha AS value FROM land_use_summary ORDER BY total_area_ha DESC LIMIT 50",
                count_sql="SELECT count(*) FROM land_use_summary WHERE total_area_ha > 0",
                id="largest_land_area",
                title="Størst Landbrugsareal",
                category="field",
                description="Virksomheder med det største samlede landbrugsareal i 2024",
                unit="hektar",
                format_fn=lambda v: f"{v:.1f} ha",
            )
        )

        # 5. Largest Organic Area
        rankings.append(
            self._ranking_from_sql(
                sql="SELECT cvr_number, company_name, municipality, organic_area_ha AS value FROM land_use_summary WHERE organic_area_ha > 0 ORDER BY organic_area_ha DESC LIMIT 50",
                count_sql="SELECT count(*) FROM land_use_summary WHERE organic_area_ha > 0",
                id="largest_organic_area",
                title="Størst Økologisk Areal",
                category="field",
                description="Virksomheder med det største økologiske landbrugsareal i 2024",
                unit="hektar",
                format_fn=lambda v: f"{v:.1f} ha",
            )
        )

        # 6. Highest Organic Percentage (min 50ha)
        rankings.append(
            self._ranking_from_sql(
                sql="SELECT cvr_number, company_name, municipality, organic_percentage AS value FROM land_use_summary WHERE total_area_ha > 50 AND organic_percentage > 0 ORDER BY organic_percentage DESC, total_area_ha DESC LIMIT 50",
                count_sql="SELECT count(*) FROM land_use_summary WHERE total_area_ha > 50 AND organic_percentage > 0",
                id="highest_organic_percentage",
                title="Højest Økologisk Andel",
                category="field",
                description="Virksomheder med den højeste andel økologisk landbrug (min. 50 ha) i 2024",
                unit="procent",
                format_fn=lambda v: f"{v:.1f}%",
            )
        )

        # 7. Most Fields
        rankings.append(
            self._ranking_from_sql(
                sql="SELECT cvr_number, company_name, municipality, total_fields AS value FROM land_use_summary WHERE total_fields > 0 ORDER BY total_fields DESC LIMIT 50",
                count_sql="SELECT count(*) FROM land_use_summary WHERE total_fields > 0",
                id="most_fields",
                title="Flest Marker",
                category="field",
                description="Virksomheder med det største antal individuelle marker i 2024",
                unit="marker",
                format_fn=lambda v: f"{int(v)} marker",
            )
        )

        return [r for r in rankings if r is not None]

    # -------------------------------------------------------------------------
    # ENVIRONMENT RANKINGS (pesticide data)
    # -------------------------------------------------------------------------
    def _environment_rankings(self) -> list[dict]:
        rankings = []
        if not self._table_exists("pesticides") or not self._table_exists("companies"):
            logger.warning("Skipping environment rankings — missing tables")
            return rankings

        # pesticides has: cvr_number (VARCHAR), PesticideName, DosageQuantity, AllocatedArea, municipality
        # Aggregate per company
        self.conn.execute("""
            CREATE OR REPLACE TABLE pesticide_summary AS
            SELECT
                p.cvr_number,
                c.company_name,
                p.municipality,
                COUNT(*) AS total_applications,
                ROUND(SUM(p.DosageQuantity), 2) AS total_dosage,
                ROUND(SUM(p.AllocatedArea), 1) AS total_treated_area_ha,
                COUNT(DISTINCT p.PesticideName) AS unique_pesticides
            FROM pesticides p
            JOIN companies c ON p.cvr_number = c.cvr_number::VARCHAR
            GROUP BY p.cvr_number, c.company_name, p.municipality
        """)

        # 8. Most Pesticide Applications
        rankings.append(
            self._ranking_from_sql(
                sql="SELECT cvr_number, company_name, municipality, total_applications AS value FROM pesticide_summary ORDER BY total_applications DESC LIMIT 50",
                count_sql="SELECT count(*) FROM pesticide_summary WHERE total_applications > 0",
                id="most_pesticide_applications",
                title="Flest Pesticidanvendelser",
                category="environment",
                description="Virksomheder med flest pesticidanvendelser 2023/2024",
                unit="anvendelser",
                format_fn=lambda v: f"{int(v):,}".replace(",", "."),
            )
        )

        # 9. Largest Treated Area
        rankings.append(
            self._ranking_from_sql(
                sql="SELECT cvr_number, company_name, municipality, total_treated_area_ha AS value FROM pesticide_summary WHERE total_treated_area_ha > 0 ORDER BY total_treated_area_ha DESC LIMIT 50",
                count_sql="SELECT count(*) FROM pesticide_summary WHERE total_treated_area_ha > 0",
                id="largest_treated_area",
                title="Størst Behandlet Areal",
                category="environment",
                description="Virksomheder med det største pesticidbehandlede areal 2023/2024",
                unit="hektar",
                format_fn=lambda v: f"{v:.1f} ha",
            )
        )

        # 10. Most Unique Pesticides
        rankings.append(
            self._ranking_from_sql(
                sql="SELECT cvr_number, company_name, municipality, unique_pesticides AS value FROM pesticide_summary WHERE unique_pesticides > 0 ORDER BY unique_pesticides DESC LIMIT 50",
                count_sql="SELECT count(*) FROM pesticide_summary WHERE unique_pesticides > 0",
                id="most_unique_pesticides",
                title="Flest Forskellige Pesticider",
                category="environment",
                description="Virksomheder der bruger flest forskellige pesticidprodukter 2023/2024",
                unit="produkter",
                format_fn=lambda v: f"{int(v)} produkter",
            )
        )

        return [r for r in rankings if r is not None]

    # -------------------------------------------------------------------------
    # WORKER RANKINGS
    # -------------------------------------------------------------------------
    def _worker_rankings(self) -> list[dict]:
        rankings = []

        # Work permits: company_id (VARCHAR), year, nationality, first_permits_count
        if self._table_exists("work_permits") and self._table_exists("companies"):
            self.conn.execute("""
                CREATE OR REPLACE TABLE work_permit_summary AS
                SELECT
                    wp.company_id AS cvr_number,
                    c.company_name,
                    c.current_municipality_name AS municipality,
                    SUM(wp.first_permits_count) AS total_permits,
                    COUNT(DISTINCT wp.nationality) AS unique_nationalities
                FROM work_permits wp
                JOIN companies c ON wp.company_id = c.cvr_number::VARCHAR
                WHERE wp.year = 2024 AND wp.first_permits_count > 0
                GROUP BY wp.company_id, c.company_name, c.current_municipality_name
            """)

            rankings.append(
                self._ranking_from_sql(
                    sql="SELECT cvr_number, company_name, municipality, total_permits AS value FROM work_permit_summary ORDER BY total_permits DESC LIMIT 50",
                    count_sql="SELECT count(*) FROM work_permit_summary WHERE total_permits > 0",
                    id="most_foreign_workers",
                    title="Flest Arbejdstilladelser",
                    category="worker",
                    description="Virksomheder med flest førstegangsarbejdstilladelser i 2024",
                    unit="tilladelser",
                    format_fn=lambda v: f"{int(v)} tilladelser",
                )
            )

        # Worker safety: cvr_number (BIGINT), year, injury_type, injury_count
        if self._table_exists("worker_safety") and self._table_exists("companies"):
            self.conn.execute("""
                CREATE OR REPLACE TABLE worker_safety_summary AS
                SELECT
                    ws.cvr_number::VARCHAR AS cvr_number,
                    c.company_name,
                    c.current_municipality_name AS municipality,
                    SUM(TRY_CAST(ws.injury_count AS INTEGER)) AS total_injuries
                FROM worker_safety ws
                JOIN companies c ON ws.cvr_number = c.cvr_number
                GROUP BY ws.cvr_number, c.company_name, c.current_municipality_name
            """)

            rankings.append(
                self._ranking_from_sql(
                    sql="SELECT cvr_number, company_name, municipality, total_injuries AS value FROM worker_safety_summary WHERE total_injuries > 0 ORDER BY total_injuries DESC LIMIT 50",
                    count_sql="SELECT count(*) FROM worker_safety_summary WHERE total_injuries > 0",
                    id="most_work_injuries",
                    title="Flest Arbejdsulykker",
                    category="worker",
                    description="Virksomheder med flest rapporterede arbejdsulykker",
                    unit="ulykker",
                    format_fn=lambda v: f"{int(v)} ulykker",
                )
            )

        return [r for r in rankings if r is not None]

    # -------------------------------------------------------------------------
    # HELPERS
    # -------------------------------------------------------------------------
    def _ranking_from_sql(
        self,
        sql: str,
        count_sql: str,
        id: str,
        title: str,
        category: str,
        description: str,
        unit: str,
        format_fn: Callable,
    ) -> dict | None:
        """Generate a ranking from SQL that returns cvr_number, company_name, municipality, value."""
        try:
            rows = self.query_to_dicts(sql)
            if not rows:
                return None
            total = self.conn.execute(count_sql).fetchone()[0]
            return {
                "id": id,
                "title": title,
                "category": category,
                "description": description,
                "unit": unit,
                "company_count": total,
                "items": [
                    {
                        "cvr_number": str(row.get("cvr_number", "")),
                        "company_name": row.get("company_name", "Ukendt virksomhed"),
                        "municipality": row.get("municipality", "Ukendt kommune"),
                        "rank": idx + 1,
                        "value": row["value"],
                        "formatted_value": format_fn(row["value"]),
                    }
                    for idx, row in enumerate(rows)
                ],
            }
        except Exception:
            logger.exception(f"Failed to generate ranking: {id}")
            return None


def _format_danish_number(n: int) -> str:
    """Format number with Danish-style dots: 29.104.178"""
    return f"{n:,}".replace(",", ".")
