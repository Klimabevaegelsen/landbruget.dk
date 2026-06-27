"""Homepage exporter — generates statistics and rankings JSON files.

Replaces:
- Supabase edge function: homepage-rankings (1711 lines, 25 ranking tables)
- Supabase edge function: homepage-statistics

Output files:
- homepage/statistics.json
- homepage/rankings/all.json
- homepage/rankings/{category}.json

Actual R2 data sources:
- silver/cvr_companies/data.parquet — company master data (61k rows)
- gold/cvr_enrichment_financial_statements/*/financial_statements.parquet — financials (5.6k rows)
- gold/field_production/latest/data.parquet — field/crop data (10.8M rows, 2008-2025)
- gold/pesticide_disaggregation_2023_2024/*/pesticide_disaggregation_2023_2024.parquet — pesticides (1.5M rows)
- gold/worker_safety/*/worker_safety_clean.parquet — worker safety (1.2k rows)
- gold/work_permits/*/work_permits.parquet — work permits (8.7k rows)
- silver/chr/*/(properties|property_users|property_owners).parquet — animal sites (derived)
- silver/chr/*/herd_sizes.parquet — animal capacity snapshots
- silver/chr/*/antibiotic_usage.parquet — animal medicine usage
"""

from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal

from common.logging_utils import get_pipeline_logger

from exporters.animal import (
    create_company_animal_summary,
    create_company_municipality_lookup,
    create_company_transport_summaries,
    create_production_sites,
    site_yearly_summary_count_query,
)
from exporters.base import BaseExporter
from exporters.governance import leadership_count_query, owner_count_query
from exporters.homepage_stats_sources import HOMEPAGE_STATS_SOURCES, HomepageStatisticsSource
from exporters.parity import get_manifest_version
from exporters.worker import worker_yearly_summary_count_query

logger = get_pipeline_logger("api_export.homepage")

PIG_SPECIES_CODES = ("15",)
CATTLE_SPECIES_CODES = ("12",)


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

        animal = self._animal_rankings()
        all_rankings.extend(animal)
        self.write_json(self._wrap_rankings(animal), "homepage/rankings/animal.json")
        stats["animal"] = len(animal)

        # Write combined file
        self.write_json(self._wrap_rankings(all_rankings), "homepage/rankings/all.json")
        stats["total_rankings"] = len(all_rankings)
        stats["files_written"] = 7  # statistics + 5 categories + all

        return stats

    def _wrap_rankings(self, rankings: list[dict]) -> dict:
        invalid_rankings = []
        empty_ranking_ids = []

        for index, ranking in enumerate(rankings):
            ranking_id = ranking.get("id") if isinstance(ranking, dict) else None
            if not isinstance(ranking, dict) or not isinstance(ranking.get("items"), list):
                label = f"index {index}"
                if ranking_id:
                    label = f"{label} ({ranking_id})"
                invalid_rankings.append(label)
                continue

            if not ranking["items"] or ranking.get("status") in {"missing", "empty", "error"}:
                empty_ranking_ids.append(ranking.get("id", f"index {index}"))

        if invalid_rankings:
            raise ValueError(
                "Homepage export received invalid rankings: " + ", ".join(invalid_rankings)
            )

        if empty_ranking_ids:
            logger.warning(
                "Homepage export: %s rankings have no data: %s",
                len(empty_ranking_ids),
                empty_ranking_ids,
            )

        return {
            "rankings": rankings,
            "metadata": {
                "generated_at": datetime.now(UTC).isoformat(),
                "total_tables": len(rankings),
                "parity_manifest_version": get_manifest_version(),
            },
        }

    def _load_base_tables(self) -> None:
        """Load actual R2 parquet datasets into DuckDB tables."""
        tables = {
            "companies": self.latest_r2_parquet("gold/cvr_enrichment_companies"),
            "financials": self.latest_r2_parquet(
                "gold/cvr_enrichment_financial_statements", "financial_statements.parquet"
            ),
            "field_production": self.r2_uri("gold/field_production/latest/data.parquet"),
            "pesticides": self.latest_r2_parquet(
                "gold/pesticide_disaggregation_2023_2024",
                "pesticide_disaggregation_2023_2024.parquet",
            ),
            "worker_safety": self.latest_r2_parquet(
                "gold/worker_safety", "worker_safety_clean.parquet"
            ),
            "employment": self.latest_r2_parquet("silver/cvr_employment"),
            "inspections": self.latest_r2_parquet(
                "silver/arbejdstilsynet_inspections", "workplace_inspections.parquet"
            ),
            "work_permits": self.latest_r2_parquet("gold/work_permits", "work_permits.parquet"),
            "bmd_products": self.latest_r2_match("silver/bmd/*/pesticide_products.parquet"),
            "bnbo_fields": self.latest_r2_nested_parquet(
                "gold/field_analysis_field_bnbo_intersections_*"
            ),
            "wetland_fields": self.latest_r2_nested_parquet(
                "gold/field_analysis_field_wetland_intersections_*"
            ),
            "production_sites": self.latest_r2_parquet("gold/chr", "production_sites.parquet"),
            "chr_properties": self.latest_r2_match("silver/chr/*/properties.parquet"),
            "chr_property_users": self.latest_r2_match("silver/chr/*/property_users.parquet"),
            "chr_property_owners": self.latest_r2_match("silver/chr/*/property_owners.parquet"),
            "herd_sizes": self.latest_r2_match("silver/chr/*/herd_sizes*.parquet"),
            "herds": self.latest_r2_match("silver/chr/*/herds*.parquet"),
            "antibiotic_usage": self.latest_r2_match("silver/chr/*/antibiotic_usage.parquet"),
            "transportation_analysis": self.latest_r2_parquet(
                "gold/chr_transportation_analysis",
                "chr_transportation_analysis.parquet",
            ),
            "cattle_movements": self.latest_r2_match(
                "silver/chr/*/chr_dyr_movement_summaries*.parquet"
            ),
            "pig_movements": self.latest_r2_match("silver/svineflytning/*/movements*.parquet"),
        }

        for name, path in tables.items():
            if not path:
                logger.warning(f"Could not resolve latest R2 parquet for {name}")
                continue
            try:
                self.load_parquet_table(path, name)
            except Exception:
                logger.warning(
                    f"Could not load {name} from {path} — rankings using it will be skipped"
                )

        create_production_sites(self.conn)

    def _table_exists(self, name: str) -> bool:
        try:
            self.conn.execute(f"SELECT 1 FROM {name} LIMIT 0")
            return True
        except Exception:
            return False

    def _table_columns(self, name: str) -> set[str]:
        if not self._table_exists(name):
            return set()
        return {row[0] for row in self.conn.execute(f"DESCRIBE {name}").fetchall()}

    def _export_statistics(self) -> None:
        """Generate homepage/statistics.json with audited legacy source coverage."""
        stats = {
            "total_companies": 0,
            "total_data_points": 0,
            "last_updated": datetime.now(UTC).isoformat(),
        }

        if self._table_exists("companies"):
            stats["total_companies"] = self.conn.execute(
                "SELECT count(*) FROM companies"
            ).fetchone()[0]

        sources = []
        for source in HOMEPAGE_STATS_SOURCES:
            try:
                sources.append(self._count_statistics_source(source))
            except Exception as exc:
                logger.exception(f"Skipping statistics source {source.legacy_table}: {exc}")
                sources.append(
                    {
                        "legacy_table": source.legacy_table,
                        "status": "error",
                        "count_mode": source.count_mode,
                        "row_count": 0,
                        "source_path": None,
                        "source_paths": [],
                        "note": (f"{source.note} Query failed: {type(exc).__name__}: {exc}"),
                    }
                )
        stats["total_data_points"] = sum(source["row_count"] for source in sources)

        stats["formatted"] = {
            "companies": f"{stats['total_companies']:,}".replace(",", "."),
            "data_points": _format_danish_number(stats["total_data_points"]),
        }
        stats["metadata"] = {
            "datapoints_definition": "legacy_21_table_parity_best_effort",
            "company_definition": "current_cvr_enrichment_companies_row_count",
            "parity_manifest_version": get_manifest_version(),
            "legacy_source_slots": len(HOMEPAGE_STATS_SOURCES),
            "exact_sources": sum(1 for source in sources if source["status"] == "exact"),
            "approximate_sources": sum(
                1 for source in sources if source["status"] == "approximate"
            ),
            "missing_sources": sum(1 for source in sources if source["status"] == "missing"),
        }
        stats["sources"] = sources

        self.write_json(stats, "homepage/statistics.json")

    def _count_statistics_source(self, source: HomepageStatisticsSource) -> dict:
        if source.resolver == "derived":
            return self._count_derived_statistics_source(source)

        paths = self._resolve_statistics_paths(source)
        if not paths:
            return {
                "legacy_table": source.legacy_table,
                "status": "missing",
                "count_mode": source.count_mode,
                "row_count": 0,
                "source_path": None,
                "source_paths": [],
                "note": f"{source.note} Source could not be resolved from R2.",
            }

        return {
            "legacy_table": source.legacy_table,
            "status": source.status,
            "count_mode": source.count_mode,
            "row_count": self.count_parquet_rows(paths),
            "source_path": paths[0] if len(paths) == 1 else f"{len(paths)} parquet files",
            "source_paths": paths,
            "note": source.note,
        }

    def _resolve_statistics_paths(self, source: HomepageStatisticsSource) -> list[str]:
        if source.resolver == "fixed" and source.fixed_path:
            return [self.r2_uri(source.fixed_path)]
        if source.resolver == "latest" and source.dataset_prefix:
            path = self.latest_r2_parquet(source.dataset_prefix, source.filename)
            return [path] if path else []
        if source.resolver == "nested_latest" and source.dataset_prefix:
            path = self.latest_r2_nested_parquet(source.dataset_prefix, source.filename)
            return [path] if path else []
        if source.resolver == "latest_match" and source.object_glob:
            path = self.latest_r2_match(source.object_glob)
            return [path] if path else []
        if source.resolver == "glob" and source.object_glob:
            return self.r2_glob(source.object_glob)
        return []

    def _count_derived_statistics_source(self, source: HomepageStatisticsSource) -> dict:
        paths: list[str] = []
        query = None

        if source.derived_kind == "worker_yearly_summary":
            employment = self.latest_r2_parquet("silver/cvr_employment")
            work_permits = self.latest_r2_parquet("gold/work_permits", "work_permits.parquet")
            incidents = self.latest_r2_parquet(
                "silver/arbejdstilsynet_inspections", "workplace_inspections.parquet"
            )
            paths = [path for path in [employment, work_permits, incidents] if path]
            if len(paths) == 3:
                query = worker_yearly_summary_count_query(
                    self._relation_sql([employment]),
                    self._relation_sql([work_permits]),
                    self._relation_sql([incidents]),
                )
        elif source.derived_kind == "company_leadership":
            persons = self.latest_r2_parquet("silver/cvr_persons")
            paths = [path for path in [persons] if path]
            if persons:
                query = leadership_count_query(self._relation_sql([persons]))
        elif source.derived_kind == "company_owners":
            persons = self.latest_r2_parquet("silver/cvr_persons")
            paths = [path for path in [persons] if path]
            if persons:
                query = owner_count_query(self._relation_sql([persons]))
        elif source.derived_kind == "animal_transport_records":
            cattle_movements = self.latest_r2_match(
                "silver/chr/*/chr_dyr_movement_summaries*.parquet"
            )
            pig_movements = self.latest_r2_match("silver/svineflytning/*/movements*.parquet")
            paths = [path for path in [cattle_movements, pig_movements] if path]
            if paths:
                query = f"SELECT {self.count_parquet_rows(paths)}"
        elif source.derived_kind == "production_sites":
            paths = [
                path
                for path in [
                    self.latest_r2_match("silver/chr/*/properties.parquet"),
                    self.latest_r2_match("silver/chr/*/property_users.parquet"),
                    self.latest_r2_match("silver/chr/*/property_owners.parquet"),
                    self.latest_r2_match("silver/chr/*/herd_sizes*.parquet"),
                    self.latest_r2_match("silver/chr/*/herds*.parquet"),
                ]
                if path
            ]
            if self._table_exists("production_sites"):
                query = "SELECT COUNT(*) FROM production_sites"
            elif paths:
                temp_tables = {
                    "chr_properties": self.latest_r2_match("silver/chr/*/properties.parquet"),
                    "chr_property_users": self.latest_r2_match(
                        "silver/chr/*/property_users.parquet"
                    ),
                    "chr_property_owners": self.latest_r2_match(
                        "silver/chr/*/property_owners.parquet"
                    ),
                    "herd_sizes": self.latest_r2_match("silver/chr/*/herd_sizes*.parquet"),
                    "herds": self.latest_r2_match("silver/chr/*/herds*.parquet"),
                }
                for table_name, parquet_path in temp_tables.items():
                    if parquet_path and not self._table_exists(table_name):
                        self.load_parquet_table(parquet_path, table_name)
                if create_production_sites(self.conn):
                    query = "SELECT COUNT(*) FROM production_sites"
        elif source.derived_kind == "site_yearly_summary":
            antibiotic_usage = self.latest_r2_match("silver/chr/*/antibiotic_usage.parquet")
            herd_sizes = self.latest_r2_match("silver/chr/*/herd_sizes*.parquet")
            chr_properties = self.latest_r2_match("silver/chr/*/properties.parquet")
            chr_property_users = self.latest_r2_match("silver/chr/*/property_users.parquet")
            chr_property_owners = self.latest_r2_match("silver/chr/*/property_owners.parquet")
            herds = self.latest_r2_match("silver/chr/*/herds*.parquet")
            paths = [
                path
                for path in [
                    chr_properties,
                    chr_property_users,
                    chr_property_owners,
                    herd_sizes,
                    herds,
                    antibiotic_usage,
                ]
                if path
            ]
            if (
                self._table_exists("production_sites")
                and self._table_exists("antibiotic_usage")
                and self._table_exists("herd_sizes")
            ):
                query = site_yearly_summary_count_query(
                    "production_sites",
                    "antibiotic_usage",
                    "herd_sizes",
                )
            elif antibiotic_usage and herd_sizes and chr_properties:
                temp_tables = {
                    "chr_properties": chr_properties,
                    "chr_property_users": chr_property_users,
                    "chr_property_owners": chr_property_owners,
                    "herd_sizes": herd_sizes,
                    "herds": herds,
                }
                for table_name, parquet_path in temp_tables.items():
                    if parquet_path and not self._table_exists(table_name):
                        self.load_parquet_table(parquet_path, table_name)
                if not self._table_exists("production_sites"):
                    create_production_sites(self.conn)
                if self._table_exists("production_sites") and not self._table_exists(
                    "antibiotic_usage"
                ):
                    self.load_parquet_table(antibiotic_usage, "antibiotic_usage")
                if (
                    self._table_exists("production_sites")
                    and self._table_exists("antibiotic_usage")
                    and self._table_exists("herd_sizes")
                ):
                    query = site_yearly_summary_count_query(
                        "production_sites",
                        "antibiotic_usage",
                        "herd_sizes",
                    )

        if not query:
            return {
                "legacy_table": source.legacy_table,
                "status": "missing",
                "count_mode": source.count_mode,
                "row_count": 0,
                "source_path": None,
                "source_paths": paths,
                "note": f"{source.note} Derived inputs could not be resolved.",
            }

        return {
            "legacy_table": source.legacy_table,
            "status": source.status,
            "count_mode": source.count_mode,
            "row_count": int(self.conn.execute(query).fetchone()[0]),
            "source_path": f"derived://{source.derived_kind}",
            "source_paths": paths,
            "note": source.note,
        }

    def _relation_sql(self, paths: list[str]) -> str:
        return f"read_parquet({paths!r})"

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
        pesticide_ready = self._prepare_pesticide_summary()
        bnbo_ready = self._prepare_bnbo_summary()
        wetland_ready = self._prepare_wetland_summary()

        rankings.append(
            self._ranking_from_sql(
                sql="""
                    SELECT cvr_number, company_name, municipality, total_pesticide_burden AS value
                    FROM pesticide_summary
                    WHERE total_pesticide_burden > 0
                    ORDER BY total_pesticide_burden DESC
                    LIMIT 50
                """,
                count_sql="""
                    SELECT count(*) FROM pesticide_summary WHERE total_pesticide_burden > 0
                """,
                id="highest_pesticide_burden",
                title="Højest Pesticidbelastning",
                category="environment",
                description="Virksomheder med den højeste samlede pesticidbelastning i 2024",
                unit="B/ha",
                format_fn=lambda v: f"{v:.2f} B/ha",
            )
            if pesticide_ready
            else None
        )
        rankings.append(
            self._ranking_from_sql(
                sql="""
                    SELECT cvr_number, company_name, municipality, total_pfas_burden AS value
                    FROM pesticide_summary
                    WHERE total_pfas_burden > 0
                    ORDER BY total_pfas_burden DESC
                    LIMIT 50
                """,
                count_sql="SELECT count(*) FROM pesticide_summary WHERE total_pfas_burden > 0",
                id="most_pfas_usage",
                title="Højest PFAS-forbrug",
                category="environment",
                description="Virksomheder med det højeste forbrug af PFAS-holdige pesticider i 2024",
                unit="B/ha",
                format_fn=lambda v: f"{v:.2f} B/ha",
            )
            if pesticide_ready
            else None
        )
        rankings.append(
            self._ranking_from_sql(
                sql="""
                    SELECT cvr_number, company_name, municipality, total_glyphosate_burden AS value
                    FROM pesticide_summary
                    WHERE total_glyphosate_burden > 0
                    ORDER BY total_glyphosate_burden DESC
                    LIMIT 50
                """,
                count_sql="""
                    SELECT count(*) FROM pesticide_summary WHERE total_glyphosate_burden > 0
                """,
                id="most_glyphosate_usage",
                title="Højest Glyphosatforbrug",
                category="environment",
                description="Virksomheder med det højeste glyphosatforbrug i 2024",
                unit="B/ha",
                format_fn=lambda v: f"{v:.2f} B/ha",
            )
            if pesticide_ready
            else None
        )
        rankings.append(
            self._ranking_from_sql(
                sql="""
                    SELECT cvr_number, company_name, municipality, total_diquat_burden AS value
                    FROM pesticide_summary
                    WHERE total_diquat_burden > 0
                    ORDER BY total_diquat_burden DESC
                    LIMIT 50
                """,
                count_sql="SELECT count(*) FROM pesticide_summary WHERE total_diquat_burden > 0",
                id="most_diquat_usage",
                title="Højest Diquatforbrug",
                category="environment",
                description="Virksomheder med det højeste diquatforbrug i 2024",
                unit="B/ha",
                format_fn=lambda v: f"{v:.2f} B/ha",
            )
            if pesticide_ready
            else None
        )
        rankings.append(
            self._ranking_from_sql(
                sql="""
                    SELECT cvr_number, company_name, municipality, bnbo_not_dealt_with_ha AS value
                    FROM bnbo_summary
                    WHERE bnbo_not_dealt_with_ha > 0
                    ORDER BY bnbo_not_dealt_with_ha DESC
                    LIMIT 50
                """,
                count_sql="SELECT count(*) FROM bnbo_summary WHERE bnbo_not_dealt_with_ha > 0",
                id="most_bnbo_not_dealt_with",
                title="Mest BNBO-areal Ikke Håndteret",
                category="environment",
                description="Virksomheder med mest BNBO-areal der kræver handling i 2025",
                unit="ha",
                format_fn=lambda v: f"{v:.1f} ha",
            )
            if bnbo_ready
            else None
        )
        rankings.append(
            self._ranking_from_sql(
                sql="""
                    SELECT cvr_number, company_name, municipality, bnbo_dealt_with_ha AS value
                    FROM bnbo_summary
                    WHERE bnbo_dealt_with_ha > 0
                    ORDER BY bnbo_dealt_with_ha DESC
                    LIMIT 50
                """,
                count_sql="SELECT count(*) FROM bnbo_summary WHERE bnbo_dealt_with_ha > 0",
                id="most_bnbo_dealt_with",
                title="Mest BNBO-areal Håndteret",
                category="environment",
                description="Virksomheder med mest BNBO-areal der er håndteret i 2025",
                unit="ha",
                format_fn=lambda v: f"{v:.1f} ha",
            )
            if bnbo_ready
            else None
        )
        rankings.append(
            self._ranking_from_sql(
                sql="""
                    SELECT cvr_number, company_name, municipality, wetland_not_restored_ha AS value
                    FROM wetland_summary
                    WHERE wetland_not_restored_ha > 0
                    ORDER BY wetland_not_restored_ha DESC
                    LIMIT 50
                """,
                count_sql="""
                    SELECT count(*) FROM wetland_summary WHERE wetland_not_restored_ha > 0
                """,
                id="most_wetland_not_restored",
                title="Mest Lavbundsjorde Ikke Genoprettet",
                category="environment",
                description="Virksomheder med mest lavbundsjorde-areal der har behov for genopretning i 2024",
                unit="ha",
                format_fn=lambda v: f"{v:.1f} ha",
            )
            if wetland_ready
            else None
        )
        rankings.append(
            self._ranking_from_sql(
                sql="""
                    SELECT cvr_number, company_name, municipality, wetland_restored_ha AS value
                    FROM wetland_summary
                    WHERE wetland_restored_ha > 0
                    ORDER BY wetland_restored_ha DESC
                    LIMIT 50
                """,
                count_sql="SELECT count(*) FROM wetland_summary WHERE wetland_restored_ha > 0",
                id="most_wetland_restored",
                title="Mest Lavbundsjorde Genoprettet",
                category="environment",
                description="Virksomheder med mest lavbundsjorde-areal der er helt eller delvist genoprettet i 2024",
                unit="ha",
                format_fn=lambda v: f"{v:.1f} ha",
            )
            if wetland_ready
            else None
        )

        fallbacks = [
            (
                "highest_pesticide_burden",
                "Højest Pesticidbelastning",
                "Virksomheder med den højeste samlede pesticidbelastning i 2024",
                "B/ha",
                "Pesticide burden rankings require pesticide disaggregation and BMD product metadata.",
            ),
            (
                "most_pfas_usage",
                "Højest PFAS-forbrug",
                "Virksomheder med det højeste forbrug af PFAS-holdige pesticider i 2024",
                "B/ha",
                "PFAS ranking requires BMD product classifications joined to pesticide applications.",
            ),
            (
                "most_glyphosate_usage",
                "Højest Glyphosatforbrug",
                "Virksomheder med det højeste glyphosatforbrug i 2024",
                "B/ha",
                "Glyphosate ranking requires BMD product classifications joined to pesticide applications.",
            ),
            (
                "most_diquat_usage",
                "Højest Diquatforbrug",
                "Virksomheder med det højeste diquatforbrug i 2024",
                "B/ha",
                "Diquat ranking requires BMD product classifications joined to pesticide applications.",
            ),
            (
                "most_bnbo_not_dealt_with",
                "Mest BNBO-areal Ikke Håndteret",
                "Virksomheder med mest BNBO-areal der kræver handling i 2025",
                "ha",
                "BNBO parity needs field-level company mapping and status hectares in the latest export.",
            ),
            (
                "most_bnbo_dealt_with",
                "Mest BNBO-areal Håndteret",
                "Virksomheder med mest BNBO-areal der er håndteret i 2025",
                "ha",
                "BNBO parity needs field-level company mapping and status hectares in the latest export.",
            ),
            (
                "most_wetland_not_restored",
                "Mest Lavbundsjorde Ikke Genoprettet",
                "Virksomheder med mest lavbundsjorde-areal der har behov for genopretning i 2024",
                "ha",
                "Wetland parity needs field-level company mapping and restoration status hectares.",
            ),
            (
                "most_wetland_restored",
                "Mest Lavbundsjorde Genoprettet",
                "Virksomheder med mest lavbundsjorde-areal der er helt eller delvist genoprettet i 2024",
                "ha",
                "Wetland parity needs field-level company mapping and restoration status hectares.",
            ),
        ]

        ranking_by_id = {ranking["id"]: ranking for ranking in rankings if ranking is not None}
        return [
            ranking_by_id.get(ranking_id)
            or self._empty_ranking(
                id=ranking_id,
                title=title,
                category="environment",
                description=description,
                unit=unit,
                note=note,
            )
            for ranking_id, title, description, unit, note in fallbacks
        ]

    # -------------------------------------------------------------------------
    # WORKER RANKINGS
    # -------------------------------------------------------------------------
    def _worker_rankings(self) -> list[dict]:
        rankings = []
        employment_ready = False
        inspections_ready = False

        if self._table_exists("employment") and self._table_exists("companies"):
            employment_columns = self._table_columns("employment")
            if "month_year" in employment_columns and "company_id" in employment_columns:
                join_year_expr = "EXTRACT(year FROM e.month_year)::INTEGER"
                cvr_expr = "CAST(e.company_id AS VARCHAR)"
                year_filter_col = "month_year"
            else:
                join_year_expr = "EXTRACT(year FROM TRY_CAST(e.period_start AS DATE))::INTEGER"
                cvr_expr = "CAST(e.cvr_number AS VARCHAR)"
                year_filter_col = "period_start"

            try:
                self.conn.execute(f"""
                    CREATE OR REPLACE TABLE employee_summary AS
                    WITH employment_yearly AS (
                        SELECT
                            {cvr_expr} AS cvr_number,
                            {join_year_expr} AS year,
                            COALESCE(e.employee_count, 0) AS employee_count
                        FROM employment e
                        WHERE e.{year_filter_col} IS NOT NULL
                          AND {cvr_expr} IS NOT NULL
                    ),
                    latest_year AS (
                        SELECT MAX(year) AS year FROM employment_yearly
                    )
                    SELECT
                        ey.cvr_number,
                        c.company_name,
                        c.current_municipality_name AS municipality,
                        MAX(ey.employee_count) AS max_employees
                    FROM employment_yearly ey
                    JOIN latest_year ly ON ey.year = ly.year
                    JOIN companies c
                      ON ey.cvr_number = c.cvr_number::VARCHAR
                    GROUP BY 1, 2, 3
                """)
                employment_ready = True
            except Exception:
                logger.exception(
                    "Failed to build employee_summary; employment-based rankings will be empty"
                )

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
                WHERE wp.year = (SELECT MAX(year) FROM work_permits)
                  AND wp.first_permits_count > 0
                GROUP BY wp.company_id, c.company_name, c.current_municipality_name
            """)

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

        if self._table_exists("inspections") and self._table_exists("companies"):
            inspection_columns = self._table_columns("inspections")
            decision_expr = (
                "LOWER(CAST(i.decision AS VARCHAR))" if "decision" in inspection_columns else "''"
            )
            company_key_expr = (
                "COALESCE(NULLIF(CAST(i.cvr_number AS VARCHAR), ''), NULLIF(CAST(i.company_id AS VARCHAR), ''))"
                if "cvr_number" in inspection_columns and "company_id" in inspection_columns
                else "CAST(i.cvr_number AS VARCHAR)"
                if "cvr_number" in inspection_columns
                else "CAST(i.company_id AS VARCHAR)"
            )
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE inspection_summary AS
                WITH latest_year AS (
                    SELECT MAX(EXTRACT(year FROM date))::INTEGER AS year
                    FROM inspections
                    WHERE date IS NOT NULL
                )
                SELECT
                    {company_key_expr} AS cvr_number,
                    c.company_name,
                    c.current_municipality_name AS municipality,
                    COUNT(*) AS inspection_count,
                    SUM(CASE WHEN {decision_expr} LIKE '%straks%' THEN 1 ELSE 0 END)
                        AS urgent_violations
                FROM inspections i
                JOIN latest_year ly
                  ON EXTRACT(year FROM i.date) = ly.year
                JOIN companies c
                  ON {company_key_expr} = c.cvr_number::VARCHAR
                WHERE {company_key_expr} IS NOT NULL
                GROUP BY 1, 2, 3
            """)
            inspections_ready = True

        rankings.append(
            self._ranking_from_sql(
                sql="""
                    SELECT cvr_number, company_name, municipality, max_employees AS value
                    FROM employee_summary
                    WHERE max_employees > 0
                    ORDER BY max_employees DESC
                    LIMIT 50
                """,
                count_sql="SELECT count(*) FROM employee_summary WHERE max_employees > 0",
                id="most_employees_worker",
                title="Flest Ansatte (Arbejdsmarkedsdata)",
                category="worker",
                description="Virksomheder med flest ansatte ifølge arbejdsmarkedsdata 2025",
                unit="ansatte",
                format_fn=lambda v: f"{int(v)} ansatte",
            )
            if employment_ready
            else self._empty_ranking(
                id="most_employees_worker",
                title="Flest Ansatte (Arbejdsmarkedsdata)",
                category="worker",
                description="Virksomheder med flest ansatte ifølge arbejdsmarkedsdata 2025",
                unit="ansatte",
                note="Employment ranking requires current CVR employment parquet.",
            )
        )
        rankings.append(
            self._ranking_from_sql(
                sql="""
                    SELECT cvr_number, company_name, municipality, total_permits AS value
                    FROM work_permit_summary
                    ORDER BY total_permits DESC
                    LIMIT 50
                """,
                count_sql="SELECT count(*) FROM work_permit_summary WHERE total_permits > 0",
                id="most_foreign_workers",
                title="Førstegangsvisum Ansøgninger",
                category="worker",
                description="Virksomheder med flest førstegangsvisum ansøgninger i 2024",
                unit="tilladelser",
                format_fn=lambda v: f"{int(v)} tilladelser",
            )
            if self._table_exists("work_permit_summary")
            else self._empty_ranking(
                id="most_foreign_workers",
                title="Førstegangsvisum Ansøgninger",
                category="worker",
                description="Virksomheder med flest førstegangsvisum ansøgninger i 2024",
                unit="tilladelser",
                note="Foreign worker ranking requires current work permits parquet.",
            )
        )
        rankings.append(
            self._ranking_from_sql(
                sql="""
                    SELECT cvr_number, company_name, municipality, total_injuries AS value
                    FROM worker_safety_summary
                    WHERE total_injuries > 0
                    ORDER BY total_injuries DESC
                    LIMIT 50
                """,
                count_sql="SELECT count(*) FROM worker_safety_summary WHERE total_injuries > 0",
                id="most_work_injuries",
                title="Flest Arbejdsulykker",
                category="worker",
                description="Virksomheder med flest rapporterede arbejdsulykker i 2024",
                unit="ulykker",
                format_fn=lambda v: f"{int(v)} ulykker",
            )
            if self._table_exists("worker_safety_summary")
            else self._empty_ranking(
                id="most_work_injuries",
                title="Flest Arbejdsulykker",
                category="worker",
                description="Virksomheder med flest rapporterede arbejdsulykker i 2024",
                unit="ulykker",
                note="Work injury ranking requires worker safety gold export.",
            )
        )
        rankings.append(
            self._ranking_from_sql(
                sql="""
                    SELECT cvr_number, company_name, municipality, inspection_count AS value
                    FROM inspection_summary
                    WHERE inspection_count > 0
                    ORDER BY inspection_count DESC
                    LIMIT 50
                """,
                count_sql="SELECT count(*) FROM inspection_summary WHERE inspection_count > 0",
                id="most_workplace_inspections",
                title="Flest Arbejdstilsynsinspektioner",
                category="worker",
                description="Virksomheder med flest arbejdstilsynsinspektioner i 2025",
                unit="inspektioner",
                format_fn=lambda v: f"{int(v)} inspektioner",
            )
            if inspections_ready
            else self._empty_ranking(
                id="most_workplace_inspections",
                title="Flest Arbejdstilsynsinspektioner",
                category="worker",
                description="Virksomheder med flest arbejdstilsynsinspektioner i 2025",
                unit="inspektioner",
                note="Inspection ranking requires workplace inspections gold export with company mapping.",
            )
        )
        rankings.append(
            self._ranking_from_sql(
                sql="""
                    SELECT cvr_number, company_name, municipality, urgent_violations AS value
                    FROM inspection_summary
                    WHERE urgent_violations > 0
                    ORDER BY urgent_violations DESC
                    LIMIT 50
                """,
                count_sql="SELECT count(*) FROM inspection_summary WHERE urgent_violations > 0",
                id="most_urgent_violations",
                title="Flest Strakspåbud",
                category="worker",
                description="Virksomheder med flest strakspåbud fra Arbejdstilsynet i 2025",
                unit="påbud",
                format_fn=lambda v: f"{int(v)} påbud",
            )
            if inspections_ready
            else self._empty_ranking(
                id="most_urgent_violations",
                title="Flest Strakspåbud",
                category="worker",
                description="Virksomheder med flest strakspåbud fra Arbejdstilsynet i 2025",
                unit="påbud",
                note="Urgent violation ranking requires inspection decisions in the workplace inspections export.",
            )
        )

        return rankings

    # -------------------------------------------------------------------------
    # ANIMAL RANKINGS
    # -------------------------------------------------------------------------
    def _animal_rankings(self) -> list[dict]:
        rankings = []
        animal_ready = create_company_animal_summary(self.conn)
        species_ready = self._prepare_animal_species_summary()
        transport_ready = create_company_transport_summaries(self.conn)

        rankings.append(
            self._ranking_from_sql(
                sql=f"""
                    SELECT cvr_number, company_name, municipality, production_capacity AS value
                    FROM animal_species_summary
                    WHERE species_code IN {PIG_SPECIES_CODES}
                      AND production_capacity > 0
                    ORDER BY production_capacity DESC
                    LIMIT 50
                """,
                count_sql=f"""
                    SELECT COUNT(*)
                    FROM animal_species_summary
                    WHERE species_code IN {PIG_SPECIES_CODES}
                      AND production_capacity > 0
                """,
                id="largest_pig_production",
                title="Størst Svineproduktionskapacitet",
                category="animal",
                description="Produktionssteder med den største svineproduktionskapacitet i 2025",
                unit="kapacitet",
                format_fn=lambda v: f"{int(v):,}".replace(",", "."),
            )
            if species_ready
            else self._empty_ranking(
                id="largest_pig_production",
                title="Størst Svineproduktionskapacitet",
                category="animal",
                description="Produktionssteder med den største svineproduktionskapacitet i 2025",
                unit="kapacitet",
                note="Pig production ranking requires CHR production sites and species mapping.",
            )
        )
        rankings.append(
            self._ranking_from_sql(
                sql=f"""
                    SELECT cvr_number, company_name, municipality, production_capacity AS value
                    FROM animal_species_summary
                    WHERE species_code IN {CATTLE_SPECIES_CODES}
                      AND production_capacity > 0
                    ORDER BY production_capacity DESC
                    LIMIT 50
                """,
                count_sql=f"""
                    SELECT COUNT(*)
                    FROM animal_species_summary
                    WHERE species_code IN {CATTLE_SPECIES_CODES}
                      AND production_capacity > 0
                """,
                id="largest_cattle_production",
                title="Størst Kvægproduktionskapacitet",
                category="animal",
                description="Produktionssteder med den største kvægproduktionskapacitet i 2025",
                unit="kapacitet",
                format_fn=lambda v: f"{int(v):,}".replace(",", "."),
            )
            if species_ready
            else self._empty_ranking(
                id="largest_cattle_production",
                title="Størst Kvægproduktionskapacitet",
                category="animal",
                description="Produktionssteder med den største kvægproduktionskapacitet i 2025",
                unit="kapacitet",
                note="Cattle production ranking requires CHR production sites and species mapping.",
            )
        )
        rankings.append(
            self._ranking_from_sql(
                sql="""
                    SELECT cvr_number, company_name, municipality, total_animal_doses AS value
                    FROM company_animal_summary
                    WHERE total_animal_doses > 0
                    ORDER BY total_animal_doses DESC
                    LIMIT 50
                """,
                count_sql="""
                    SELECT COUNT(*)
                    FROM company_animal_summary
                    WHERE total_animal_doses > 0
                """,
                id="highest_antibiotic_usage",
                title="Højest Antibiotikaforbrug",
                category="animal",
                description="Virksomheder med det højeste antibiotikaforbrug i 2025",
                unit="doser",
                format_fn=lambda v: f"{v:,.0f}".replace(",", "."),
            )
            if animal_ready
            else self._empty_ranking(
                id="highest_antibiotic_usage",
                title="Højest Antibiotikaforbrug",
                category="animal",
                description="Virksomheder med det højeste antibiotikaforbrug i 2025",
                unit="doser",
                note="Antibiotic ranking requires CHR antibiotic usage joined to production sites.",
            )
        )
        rankings.append(
            self._ranking_from_sql(
                sql="""
                    SELECT cvr_number, company_name, municipality, production_site_count AS value
                    FROM company_animal_summary
                    WHERE production_site_count > 0
                    ORDER BY production_site_count DESC, total_capacity DESC
                    LIMIT 50
                """,
                count_sql="""
                    SELECT COUNT(*)
                    FROM company_animal_summary
                    WHERE production_site_count > 0
                """,
                id="most_production_sites",
                title="Flest Produktionssteder",
                category="animal",
                description="Virksomheder med flest dyreproduktionssteder i 2025",
                unit="steder",
                format_fn=lambda v: f"{int(v)} steder",
            )
            if animal_ready
            else self._empty_ranking(
                id="most_production_sites",
                title="Flest Produktionssteder",
                category="animal",
                description="Virksomheder med flest dyreproduktionssteder i 2025",
                unit="steder",
                note="Production site ranking requires CHR production sites export.",
            )
        )
        rankings.append(
            self._ranking_from_sql(
                sql="""
                    WITH latest_year AS (
                        SELECT MAX(year) AS year
                        FROM company_transport_species_yearly
                        WHERE species_code = '15'
                    )
                    SELECT cvr_number, company_name, municipality, transported_animals AS value
                    FROM company_transport_species_yearly
                    WHERE species_code = '15'
                      AND year = (SELECT year FROM latest_year)
                      AND transported_animals > 0
                    ORDER BY transported_animals DESC
                    LIMIT 50
                """,
                count_sql="""
                    WITH latest_year AS (
                        SELECT MAX(year) AS year
                        FROM company_transport_species_yearly
                        WHERE species_code = '15'
                    )
                    SELECT COUNT(*)
                    FROM company_transport_species_yearly
                    WHERE species_code = '15'
                      AND year = (SELECT year FROM latest_year)
                      AND transported_animals > 0
                """,
                id="most_transported_pigs",
                title="Flest Transporterede Svin",
                category="animal",
                description="Virksomheder med flest transporterede svin i 2024",
                unit="svin",
                format_fn=lambda v: f"{int(v):,}".replace(",", "."),
            )
            if transport_ready
            else self._empty_ranking(
                id="most_transported_pigs",
                title="Flest Transporterede Svin",
                category="animal",
                description="Virksomheder med flest transporterede svin i 2024",
                unit="svin",
                note="Pig transport ranking requires transport movements with sender CHR mapping.",
            )
        )
        rankings.append(
            self._ranking_from_sql(
                sql="""
                    WITH latest_year AS (
                        SELECT MAX(year) AS year
                        FROM company_transport_species_yearly
                        WHERE species_code = '12'
                    )
                    SELECT cvr_number, company_name, municipality, transported_animals AS value
                    FROM company_transport_species_yearly
                    WHERE species_code = '12'
                      AND year = (SELECT year FROM latest_year)
                      AND transported_animals > 0
                    ORDER BY transported_animals DESC
                    LIMIT 50
                """,
                count_sql="""
                    WITH latest_year AS (
                        SELECT MAX(year) AS year
                        FROM company_transport_species_yearly
                        WHERE species_code = '12'
                    )
                    SELECT COUNT(*)
                    FROM company_transport_species_yearly
                    WHERE species_code = '12'
                      AND year = (SELECT year FROM latest_year)
                      AND transported_animals > 0
                """,
                id="most_transported_cattle",
                title="Flest Transporterede Kvæg",
                category="animal",
                description="Virksomheder med flest transporterede kvæg i 2024",
                unit="kvæg",
                format_fn=lambda v: f"{int(v):,}".replace(",", "."),
            )
            if transport_ready
            else self._empty_ranking(
                id="most_transported_cattle",
                title="Flest Transporterede Kvæg",
                category="animal",
                description="Virksomheder med flest transporterede kvæg i 2024",
                unit="kvæg",
                note="Cattle transport ranking requires CHR movement summaries or transportation analysis export.",
            )
        )

        return rankings

    # -------------------------------------------------------------------------
    # HELPERS
    # -------------------------------------------------------------------------
    def _prepare_pesticide_summary(self) -> bool:
        if not self._table_exists("pesticides") or not self._table_exists("companies"):
            logger.warning("Skipping pesticide burden rankings — missing pesticides or companies")
            return False

        pesticide_columns = self._table_columns("pesticides")
        company_key = "cvr_number" if "cvr_number" in pesticide_columns else None
        if (
            not company_key
            or "DosageQuantity" not in pesticide_columns
            or "AllocatedArea" not in pesticide_columns
        ):
            logger.warning(
                "Skipping pesticide burden rankings — pesticide schema is missing key columns"
            )
            return False

        if (
            self._table_exists("bmd_products")
            and "PesticideRegistrationNumber" in pesticide_columns
        ):
            self.conn.execute("""
                CREATE OR REPLACE VIEW pesticides_enriched AS
                SELECT
                    p.*,
                    COALESCE(b.contains_pfas, false) AS contains_pfas,
                    COALESCE(b.contains_diquat, false) AS contains_diquat,
                    COALESCE(b.contains_glyphosate, false) AS contains_glyphosate,
                    COALESCE(b.samlet_belastning, 0.0) AS samlet_belastning
                FROM pesticides p
                LEFT JOIN bmd_products b
                  ON CAST(p.PesticideRegistrationNumber AS VARCHAR) = CAST(b.registrerings_nr AS VARCHAR)
            """)
        else:
            self.conn.execute("""
                CREATE OR REPLACE VIEW pesticides_enriched AS
                SELECT
                    p.*,
                    false AS contains_pfas,
                    false AS contains_diquat,
                    false AS contains_glyphosate,
                    0.0 AS samlet_belastning
                FROM pesticides p
            """)

        municipality_expr = (
            "COALESCE(NULLIF(TRIM(CAST(p.municipality AS VARCHAR)), ''), c.current_municipality_name)"
            if "municipality" in pesticide_columns
            else "c.current_municipality_name"
        )
        pesticide_name_expr = "p.PesticideName" if "PesticideName" in pesticide_columns else "NULL"

        self.conn.execute(f"""
            CREATE OR REPLACE TABLE pesticide_summary AS
            SELECT
                CAST(p.{company_key} AS VARCHAR) AS cvr_number,
                c.company_name,
                {municipality_expr} AS municipality,
                COUNT(*) AS total_applications,
                ROUND(SUM(COALESCE(p.DosageQuantity, 0)), 2) AS total_dosage,
                ROUND(SUM(COALESCE(p.AllocatedArea, 0)), 1) AS total_treated_area_ha,
                COUNT(DISTINCT {pesticide_name_expr}) AS unique_pesticides,
                CASE
                    WHEN SUM(COALESCE(p.AllocatedArea, 0)) > 0
                    THEN SUM(COALESCE(p.DosageQuantity, 0) * COALESCE(p.samlet_belastning, 0))
                        / SUM(COALESCE(p.AllocatedArea, 0))
                    ELSE 0
                END AS total_pesticide_burden,
                CASE
                    WHEN SUM(COALESCE(p.AllocatedArea, 0)) > 0
                    THEN SUM(
                        CASE WHEN COALESCE(p.contains_pfas, false)
                            THEN COALESCE(p.DosageQuantity, 0) * COALESCE(p.samlet_belastning, 0)
                            ELSE 0
                        END
                    ) / SUM(COALESCE(p.AllocatedArea, 0))
                    ELSE 0
                END AS total_pfas_burden,
                CASE
                    WHEN SUM(COALESCE(p.AllocatedArea, 0)) > 0
                    THEN SUM(
                        CASE WHEN COALESCE(p.contains_diquat, false)
                            THEN COALESCE(p.DosageQuantity, 0) * COALESCE(p.samlet_belastning, 0)
                            ELSE 0
                        END
                    ) / SUM(COALESCE(p.AllocatedArea, 0))
                    ELSE 0
                END AS total_diquat_burden,
                CASE
                    WHEN SUM(COALESCE(p.AllocatedArea, 0)) > 0
                    THEN SUM(
                        CASE WHEN COALESCE(p.contains_glyphosate, false)
                            THEN COALESCE(p.DosageQuantity, 0) * COALESCE(p.samlet_belastning, 0)
                            ELSE 0
                        END
                    ) / SUM(COALESCE(p.AllocatedArea, 0))
                    ELSE 0
                END AS total_glyphosate_burden
            FROM pesticides_enriched p
            JOIN companies c
              ON CAST(p.{company_key} AS VARCHAR) = c.cvr_number::VARCHAR
            GROUP BY 1, 2, 3
        """)
        return True

    def _prepare_bnbo_summary(self) -> bool:
        if not self._table_exists("bnbo_fields") or not self._table_exists("companies"):
            return False

        columns = self._table_columns("bnbo_fields")
        company_key = next(
            (column for column in ("cvr_number", "cvr", "company_id") if column in columns),
            None,
        )
        if not company_key:
            return False

        municipality_expr = (
            "COALESCE(NULLIF(TRIM(CAST(b.municipality AS VARCHAR)), ''), c.current_municipality_name)"
            if "municipality" in columns
            else "c.current_municipality_name"
        )
        if {"bnbo_action_required_hectares", "bnbo_completed_hectares"}.issubset(columns):
            not_dealt_expr = "COALESCE(b.bnbo_action_required_hectares, 0)"
            dealt_expr = "COALESCE(b.bnbo_completed_hectares, 0)"
        elif "bnbo_status" in columns and "area_ha" in columns:
            not_dealt_expr = """
                CASE
                    WHEN LOWER(CAST(b.bnbo_status AS VARCHAR))
                        IN ('not_dealt_with', 'kræver handling', 'action_required')
                    THEN COALESCE(b.area_ha, 0)
                    ELSE 0
                END
            """
            dealt_expr = """
                CASE
                    WHEN LOWER(CAST(b.bnbo_status AS VARCHAR))
                        IN ('dealt_with', 'gennemført', 'completed')
                    THEN COALESCE(b.area_ha, 0)
                    ELSE 0
                END
            """
        else:
            return False

        self.conn.execute(f"""
            CREATE OR REPLACE TABLE bnbo_summary AS
            SELECT
                CAST(b.{company_key} AS VARCHAR) AS cvr_number,
                c.company_name,
                {municipality_expr} AS municipality,
                SUM({not_dealt_expr}) AS bnbo_not_dealt_with_ha,
                SUM({dealt_expr}) AS bnbo_dealt_with_ha
            FROM bnbo_fields b
            JOIN companies c
              ON CAST(b.{company_key} AS VARCHAR) = c.cvr_number::VARCHAR
            GROUP BY 1, 2, 3
        """)
        return True

    def _prepare_wetland_summary(self) -> bool:
        if not self._table_exists("wetland_fields") or not self._table_exists("companies"):
            return False

        columns = self._table_columns("wetland_fields")
        company_key = next(
            (column for column in ("cvr_number", "cvr", "company_id") if column in columns),
            None,
        )
        if not company_key:
            return False

        municipality_expr = (
            "COALESCE(NULLIF(TRIM(CAST(w.municipality AS VARCHAR)), ''), c.current_municipality_name)"
            if "municipality" in columns
            else "c.current_municipality_name"
        )
        if {
            "property_wetland_water_uncovered_m2",
            "property_wetland_water_covered_m2",
        }.issubset(columns):
            not_restored_expr = "COALESCE(w.property_wetland_water_uncovered_m2, 0) / 10000.0"
            restored_expr = "COALESCE(w.property_wetland_water_covered_m2, 0) / 10000.0"
        elif "wetlands_status" in columns and "area_ha" in columns:
            if "water_covered_hectares" in columns:
                not_restored_expr = """
                    CASE
                        WHEN LOWER(CAST(w.wetlands_status AS VARCHAR))
                                IN ('not_restored', 'present')
                            AND COALESCE(w.water_covered_hectares, 0) = 0
                        THEN COALESCE(w.area_ha, 0)
                        ELSE 0
                    END
                """
                restored_expr = """
                    CASE
                        WHEN LOWER(CAST(w.wetlands_status AS VARCHAR))
                                IN ('restored', 'completed')
                        THEN COALESCE(w.area_ha, 0)
                        WHEN LOWER(CAST(w.wetlands_status AS VARCHAR)) = 'present'
                            AND COALESCE(w.water_covered_hectares, 0) > 0
                        THEN COALESCE(w.water_covered_hectares, 0)
                        ELSE 0
                    END
                """
            else:
                not_restored_expr = """
                    CASE
                        WHEN LOWER(CAST(w.wetlands_status AS VARCHAR)) IN ('not_restored', 'present')
                        THEN COALESCE(w.area_ha, 0)
                        ELSE 0
                    END
                """
                restored_expr = """
                    CASE
                        WHEN LOWER(CAST(w.wetlands_status AS VARCHAR)) IN ('restored', 'completed')
                        THEN COALESCE(w.area_ha, 0)
                        ELSE 0
                    END
                """
        else:
            return False

        self.conn.execute(f"""
            CREATE OR REPLACE TABLE wetland_summary AS
            SELECT
                CAST(w.{company_key} AS VARCHAR) AS cvr_number,
                c.company_name,
                {municipality_expr} AS municipality,
                SUM({not_restored_expr}) AS wetland_not_restored_ha,
                SUM({restored_expr}) AS wetland_restored_ha
            FROM wetland_fields w
            JOIN companies c
              ON CAST(w.{company_key} AS VARCHAR) = c.cvr_number::VARCHAR
            GROUP BY 1, 2, 3
        """)
        return True

    def _prepare_animal_species_summary(self) -> bool:
        if not self._table_exists("production_sites") or not self._table_exists("companies"):
            return False

        production_columns = self._table_columns("production_sites")
        if "company_id" not in production_columns:
            return False

        if not self._table_exists("company_municipality"):
            create_company_municipality_lookup(self.conn)

        combined_municipality_join = ""
        site_municipality_join = ""
        combined_municipality_expr = (
            "COALESCE(NULLIF(TRIM(c.current_municipality_name), ''), 'Ukendt kommune')"
        )
        site_municipality_expr = combined_municipality_expr
        if self._table_exists("company_municipality"):
            combined_municipality_join = (
                "LEFT JOIN company_municipality cm ON cm.cvr_number = combined.cvr_number"
            )
            site_municipality_join = (
                "LEFT JOIN company_municipality cm ON cm.cvr_number = ps.company_id"
            )
            combined_municipality_expr = (
                "COALESCE(NULLIF(TRIM(c.current_municipality_name), ''), "
                "cm.municipality, 'Ukendt kommune')"
            )
            site_municipality_expr = combined_municipality_expr

        if self._table_exists("herd_sizes") and "main_species_code" in production_columns:
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE animal_species_summary AS
                WITH site_capacity AS (
                    SELECT
                        ps.company_id AS cvr_number,
                        CAST(ps.main_species_code AS VARCHAR) AS species_code,
                        SUM(COALESCE(ps.capacity, 0)) AS site_capacity
                    FROM production_sites ps
                    WHERE ps.company_id IS NOT NULL
                      AND ps.main_species_code IS NOT NULL
                    GROUP BY 1, 2
                ),
                herd_capacity AS (
                    SELECT
                        ps.company_id AS cvr_number,
                        CAST(hs.species_code AS VARCHAR) AS species_code,
                        SUM(COALESCE(hs.count, 0)) AS registered_animals
                    FROM production_sites ps
                    JOIN herd_sizes hs
                      ON TRY_CAST(ps.chr AS BIGINT) = hs.chr
                    WHERE ps.company_id IS NOT NULL
                      AND hs.species_code IS NOT NULL
                    GROUP BY 1, 2
                ),
                combined AS (
                    SELECT
                        COALESCE(sc.cvr_number, hc.cvr_number) AS cvr_number,
                        COALESCE(sc.species_code, hc.species_code) AS species_code,
                        COALESCE(sc.site_capacity, 0) AS site_capacity,
                        COALESCE(hc.registered_animals, 0) AS registered_animals
                    FROM site_capacity sc
                    FULL OUTER JOIN herd_capacity hc
                      ON sc.cvr_number = hc.cvr_number
                     AND sc.species_code = hc.species_code
                )
                SELECT
                    combined.cvr_number,
                    c.company_name,
                    {combined_municipality_expr} AS municipality,
                    combined.species_code,
                    combined.site_capacity,
                    combined.registered_animals,
                    CASE
                        WHEN combined.site_capacity > 0 THEN combined.site_capacity
                        ELSE combined.registered_animals
                    END AS production_capacity
                FROM combined
                JOIN companies c
                  ON combined.cvr_number = c.cvr_number::VARCHAR
                {combined_municipality_join}
            """)
            return True

        if "main_species_code" in production_columns:
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE animal_species_summary AS
                SELECT
                    ps.company_id AS cvr_number,
                    c.company_name,
                    {site_municipality_expr} AS municipality,
                    CAST(ps.main_species_code AS VARCHAR) AS species_code,
                    SUM(COALESCE(ps.capacity, 0)) AS site_capacity,
                    0 AS registered_animals,
                    SUM(COALESCE(ps.capacity, 0)) AS production_capacity
                FROM production_sites ps
                JOIN companies c
                  ON ps.company_id = c.cvr_number::VARCHAR
                {site_municipality_join}
                WHERE ps.main_species_code IS NOT NULL
                GROUP BY 1, 2, 3, 4
            """)
            return True

        return False

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
    ) -> dict:
        """Generate a ranking from SQL that returns cvr_number, company_name, municipality, value."""
        try:
            rows = self.query_to_dicts(sql)
            if not rows:
                return self._empty_ranking(
                    id=id,
                    title=title,
                    category=category,
                    description=description,
                    unit=unit,
                    note="No rows returned for this ranking.",
                )
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
                        "value": _json_number(row["value"]),
                        "formatted_value": format_fn(_json_number(row["value"])),
                    }
                    for idx, row in enumerate(rows)
                ],
            }
        except Exception:
            logger.exception(f"Failed to generate ranking: {id}")
            return self._empty_ranking(
                id=id,
                title=title,
                category=category,
                description=description,
                unit=unit,
                note="Ranking query failed.",
            )

    def _empty_ranking(
        self,
        *,
        id: str,
        title: str,
        category: str,
        description: str,
        unit: str,
        note: str,
    ) -> dict:
        return {
            "id": id,
            "title": title,
            "category": category,
            "description": description,
            "unit": unit,
            "company_count": 0,
            "items": [],
            "status": "missing",
            "note": note,
        }


def _format_danish_number(n: int) -> str:
    """Format number with Danish-style dots: 29.104.178"""
    return f"{n:,}".replace(",", ".")


def _json_number(value):
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    return value
