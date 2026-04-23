"""Municipality rankings and details exporter.

Replaces:
- Supabase edge function municipality-rankings
- municipality-details endpoint

Output:
- municipalities/rankings/all.json
- municipalities/rankings/{category}.json
- municipalities/details/{municipality}_{category}.json
"""

from datetime import UTC, datetime
from urllib.parse import quote

from common.logging_utils import get_pipeline_logger

from exporters.animal import create_production_sites
from exporters.base import BaseExporter

logger = get_pipeline_logger("api_export.municipalities")


class MunicipalitiesExporter(BaseExporter):
    """Generate municipality-level rankings and details."""

    def export(self) -> dict:
        stats = {"files_written": 0}

        self._load_tables()

        response = {
            "rankings": {},
            "metadata": {
                "year": 2024,
                "total_municipalities": 0,
                "generated_at": datetime.now(UTC).isoformat(),
                "categories_included": [],
            },
        }

        categories = [
            ("land_use", self._land_use_rankings),
            ("organic_farming", self._organic_farming_rankings),
            ("production", self._production_rankings),
            ("pesticide_burden", self._pesticide_burden_rankings),
            ("pesticide_pfas", self._pesticide_pfas_rankings),
            ("pesticide_glyphosate", self._pesticide_glyphosate_rankings),
            ("antibiotic_usage", self._antibiotic_usage_rankings),
            ("environmental", self._environmental_rankings),
            ("worker_safety", self._worker_safety_rankings),
            ("incidents", self._incident_rankings),
        ]

        for category, builder in categories:
            rankings = builder(2024)
            if not rankings:
                continue

            response["rankings"][category] = rankings
            response["metadata"]["categories_included"].append(category)
            response["metadata"]["total_municipalities"] = max(
                response["metadata"]["total_municipalities"],
                len(rankings),
            )
            self.write_json(
                {
                    "rankings": {category: rankings},
                    "metadata": {
                        **response["metadata"],
                        "categories_included": [category],
                        "total_municipalities": len(rankings),
                    },
                },
                f"municipalities/rankings/{category}.json",
            )
            stats["files_written"] += 1

        self.write_json(response, "municipalities/rankings/all.json")
        stats["files_written"] += 1

        details_count = self._generate_details(2024, response["rankings"])
        stats["details_files"] = details_count
        stats["files_written"] += details_count

        return stats

    def _load_tables(self) -> None:
        tables = {
            "field_production": self.r2_uri("gold/field_production/latest/data.parquet"),
            "companies": self.latest_r2_parquet("gold/cvr_enrichment_companies"),
            "pesticides": self.latest_r2_parquet(
                "gold/pesticide_disaggregation_2023_2024",
                "pesticide_disaggregation_2023_2024.parquet",
            ),
            "bmd_products": self.latest_r2_match("silver/bmd/*/pesticide_products.parquet"),
            "production_sites": self.latest_r2_parquet("gold/chr", "production_sites.parquet"),
            "chr_properties": self.latest_r2_match("silver/chr/*/properties.parquet"),
            "chr_property_users": self.latest_r2_match("silver/chr/*/property_users.parquet"),
            "chr_property_owners": self.latest_r2_match("silver/chr/*/property_owners.parquet"),
            "antibiotic_usage": self.latest_r2_match("silver/chr/*/antibiotic_usage.parquet"),
            "herd_sizes": self.latest_r2_match("silver/chr/*/herd_sizes*.parquet"),
            "herds": self.latest_r2_match("silver/chr/*/herds*.parquet"),
            "worker_safety": self.latest_r2_parquet(
                "gold/worker_safety", "worker_safety_clean.parquet"
            ),
            "inspections": self.latest_r2_parquet(
                "silver/arbejdstilsynet_inspections", "workplace_inspections.parquet"
            ),
            "env_fields": self.latest_r2_nested_parquet(
                "gold/field_environmental_analysis_fields_*"
            ),
            "nitrogen": self.latest_r2_parquet("gold/nles5_nitrogen_estimation_nitrogen_estimates"),
        }

        for name, path in tables.items():
            if not path:
                logger.warning(f"Could not resolve latest R2 parquet for {name}")
                continue
            try:
                self.load_parquet_table(path, name)
            except Exception:
                logger.warning(f"Could not load {name} from {path}")

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

    def _land_use_rankings(self, year: int) -> list[dict] | None:
        if not self._prepare_land_use_summary(year):
            return None
        rows = self.query_to_dicts("""
            SELECT *
            FROM municipality_land_use_summary
            ORDER BY total_area_ha DESC, municipality
        """)
        return [
            {
                "municipality": row["municipality"],
                "rank": idx + 1,
                "value": row["total_area_ha"],
                "metric": "total_agricultural_area_ha",
                "additional_data": {
                    "total_fields": row["total_fields"],
                    "avg_field_size": row["avg_field_size"],
                    "organic_percentage": row["organic_percentage"] or 0,
                    "unique_companies": row["unique_companies"],
                    "unique_crops": row["unique_crops"],
                },
            }
            for idx, row in enumerate(rows)
        ]

    def _organic_farming_rankings(self, year: int) -> list[dict] | None:
        if not self._prepare_land_use_summary(year):
            return None
        rows = self.query_to_dicts("""
            SELECT *
            FROM municipality_land_use_summary
            WHERE organic_area_ha > 0
            ORDER BY organic_percentage DESC, organic_area_ha DESC, municipality
        """)
        return [
            {
                "municipality": row["municipality"],
                "rank": idx + 1,
                "value": row["organic_percentage"],
                "metric": "organic_farming_percentage",
                "additional_data": {
                    "organic_area_ha": row["organic_area_ha"],
                    "total_area_ha": row["total_area_ha"],
                    "unique_companies": row["unique_companies"],
                },
            }
            for idx, row in enumerate(rows)
        ]

    def _production_rankings(self, _year: int) -> list[dict] | None:
        if not self._prepare_production_summary():
            return None
        rows = self.query_to_dicts("""
            SELECT *
            FROM municipality_production_summary
            WHERE total_animal_capacity > 0
            ORDER BY total_animal_capacity DESC, municipality
        """)
        return [
            {
                "municipality": row["municipality"],
                "rank": idx + 1,
                "value": row["total_animal_capacity"],
                "metric": "total_animal_capacity",
                "additional_data": {
                    "total_production_sites": row["total_production_sites"],
                    "unique_companies": row["unique_companies"],
                },
            }
            for idx, row in enumerate(rows)
        ]

    def _pesticide_burden_rankings(self, _year: int) -> list[dict] | None:
        if not self._prepare_pesticide_summary():
            return None
        rows = self.query_to_dicts("""
            SELECT *
            FROM municipality_pesticide_summary
            WHERE total_pesticide_burden > 0
            ORDER BY total_pesticide_burden DESC, municipality
        """)
        return [
            {
                "municipality": row["municipality"],
                "rank": idx + 1,
                "value": row["total_pesticide_burden"],
                "metric": "total_pesticide_burden",
                "additional_data": {
                    "total_applications": row["total_applications"],
                    "total_treated_area_ha": row["total_treated_area_ha"],
                    "unique_companies": row["unique_companies"],
                },
            }
            for idx, row in enumerate(rows)
        ]

    def _pesticide_pfas_rankings(self, _year: int) -> list[dict] | None:
        if not self._prepare_pesticide_summary():
            return None
        rows = self.query_to_dicts("""
            SELECT *
            FROM municipality_pesticide_summary
            WHERE pfas_pesticide_burden > 0
            ORDER BY pfas_pesticide_burden DESC, municipality
        """)
        return [
            {
                "municipality": row["municipality"],
                "rank": idx + 1,
                "value": row["pfas_pesticide_burden"],
                "metric": "pfas_pesticide_burden",
                "additional_data": {
                    "total_applications": row["pfas_applications"],
                    "total_treated_area_ha": row["total_treated_area_ha"],
                    "unique_companies": row["unique_companies"],
                },
            }
            for idx, row in enumerate(rows)
        ]

    def _pesticide_glyphosate_rankings(self, _year: int) -> list[dict] | None:
        if not self._prepare_pesticide_summary():
            return None
        rows = self.query_to_dicts("""
            SELECT *
            FROM municipality_pesticide_summary
            WHERE glyphosate_pesticide_burden > 0
            ORDER BY glyphosate_pesticide_burden DESC, municipality
        """)
        return [
            {
                "municipality": row["municipality"],
                "rank": idx + 1,
                "value": row["glyphosate_pesticide_burden"],
                "metric": "glyphosate_pesticide_burden",
                "additional_data": {
                    "total_applications": row["glyphosate_applications"],
                    "total_treated_area_ha": row["total_treated_area_ha"],
                    "unique_companies": row["unique_companies"],
                },
            }
            for idx, row in enumerate(rows)
        ]

    def _antibiotic_usage_rankings(self, _year: int) -> list[dict] | None:
        if not self._prepare_antibiotic_summary():
            return None
        rows = self.query_to_dicts("""
            SELECT *
            FROM municipality_antibiotic_summary
            WHERE total_antibiotic_ddd_usage > 0
            ORDER BY total_antibiotic_ddd_usage DESC, municipality
        """)
        return [
            {
                "municipality": row["municipality"],
                "rank": idx + 1,
                "value": row["total_antibiotic_ddd_usage"],
                "metric": "total_antibiotic_ddd_usage",
                "additional_data": {
                    "total_production_sites": row["total_production_sites"],
                    "sites_with_antibiotics": row["sites_with_antibiotics"],
                    "unique_companies": row["unique_companies"],
                },
            }
            for idx, row in enumerate(rows)
        ]

    def _environmental_rankings(self, year: int) -> list[dict] | None:
        if not self._prepare_nitrogen_summary(year):
            return None
        rows = self.query_to_dicts("""
            SELECT *
            FROM municipality_environmental_summary
            WHERE avg_nitrogen_leaching_kg > 0
            ORDER BY avg_nitrogen_leaching_kg DESC, municipality
        """)
        return [
            {
                "municipality": row["municipality"],
                "rank": idx + 1,
                "value": row["avg_nitrogen_leaching_kg"],
                "metric": "avg_nitrogen_leaching_kg",
                "additional_data": {
                    "total_area_ha": row["total_area_ha"],
                    "unique_companies": row["unique_companies"],
                },
            }
            for idx, row in enumerate(rows)
        ]

    def _worker_safety_rankings(self, _year: int) -> list[dict] | None:
        if not self._prepare_worker_safety_summary():
            return None
        rows = self.query_to_dicts("""
            SELECT *
            FROM municipality_worker_safety_summary
            WHERE total_workplace_incidents > 0
            ORDER BY total_workplace_incidents DESC, municipality
        """)
        return [
            {
                "municipality": row["municipality"],
                "rank": idx + 1,
                "value": row["total_workplace_incidents"],
                "metric": "total_workplace_incidents",
                "additional_data": {
                    "unique_companies": row["unique_companies"],
                },
            }
            for idx, row in enumerate(rows)
        ]

    def _incident_rankings(self, _year: int) -> list[dict] | None:
        if not self._prepare_incident_summary():
            return None
        rows = self.query_to_dicts("""
            SELECT *
            FROM municipality_incident_summary
            WHERE total_incidents > 0
            ORDER BY total_incidents DESC, municipality
        """)
        return [
            {
                "municipality": row["municipality"],
                "rank": idx + 1,
                "value": row["total_incidents"],
                "metric": "total_incidents",
                "additional_data": {
                    "companies_with_incidents": row["companies_with_incidents"],
                    "incident_types_count": row["incident_types_count"],
                },
            }
            for idx, row in enumerate(rows)
        ]

    def _prepare_land_use_summary(self, year: int) -> bool:
        if not (self._table_exists("field_production") and self._table_exists("companies")):
            return False
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE municipality_land_use_summary AS
            SELECT
                c.current_municipality_name AS municipality,
                {year} AS year,
                ROUND(SUM(fp.area_ha), 1) AS total_area_ha,
                ROUND(SUM(CASE WHEN fp.organic_farming THEN fp.area_ha ELSE 0 END), 1)
                    AS organic_area_ha,
                COUNT(*) AS total_fields,
                ROUND(AVG(fp.area_ha), 2) AS avg_field_size,
                ROUND(
                    100.0 * SUM(CASE WHEN fp.organic_farming THEN fp.area_ha ELSE 0 END)
                    / NULLIF(SUM(fp.area_ha), 0),
                    1
                ) AS organic_percentage,
                COUNT(DISTINCT fp.cvr_number) AS unique_companies,
                COUNT(DISTINCT fp.crop_type) AS unique_crops
            FROM field_production fp
            JOIN companies c ON fp.cvr_number = c.cvr_number::VARCHAR
            WHERE fp.year = {year}
              AND c.current_municipality_name IS NOT NULL
            GROUP BY c.current_municipality_name
        """)
        return True

    def _prepare_production_summary(self) -> bool:
        if not self._table_exists("production_sites"):
            return False
        columns = self._table_columns("production_sites")
        municipality_expr = (
            "NULLIF(TRIM(CAST(ps.municipality AS VARCHAR)), '')"
            if "municipality" in columns
            else "NULL"
        )
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE municipality_production_summary AS
            SELECT
                COALESCE({municipality_expr}, c.current_municipality_name) AS municipality,
                COUNT(*) AS total_production_sites,
                SUM(COALESCE(ps.capacity, 0)) AS total_animal_capacity,
                COUNT(DISTINCT ps.company_id) AS unique_companies
            FROM production_sites ps
            LEFT JOIN companies c ON ps.company_id = c.cvr_number::VARCHAR
            WHERE COALESCE({municipality_expr}, c.current_municipality_name) IS NOT NULL
              AND COALESCE(ps.capacity, 0) > 0
            GROUP BY 1
        """)
        return True

    def _prepare_pesticide_summary(self) -> bool:
        if not (
            self._table_exists("pesticides")
            and self._table_exists("bmd_products")
            and self._table_exists("companies")
        ):
            return False
        self.conn.execute("""
            CREATE OR REPLACE TABLE municipality_pesticide_summary AS
            SELECT
                COALESCE(NULLIF(TRIM(CAST(p.municipality AS VARCHAR)), ''), c.current_municipality_name)
                    AS municipality,
                SUM(COALESCE(p.DosageQuantity, 0) * COALESCE(b.samlet_belastning, 0)) AS total_pesticide_burden,
                SUM(
                    CASE WHEN COALESCE(b.contains_pfas, false)
                        THEN COALESCE(p.DosageQuantity, 0) * COALESCE(b.samlet_belastning, 0)
                        ELSE 0
                    END
                ) AS pfas_pesticide_burden,
                SUM(
                    CASE WHEN COALESCE(b.contains_glyphosate, false)
                        THEN COALESCE(p.DosageQuantity, 0) * COALESCE(b.samlet_belastning, 0)
                        ELSE 0
                    END
                ) AS glyphosate_pesticide_burden,
                COUNT(*) AS total_applications,
                SUM(CASE WHEN COALESCE(b.contains_pfas, false) THEN 1 ELSE 0 END) AS pfas_applications,
                SUM(CASE WHEN COALESCE(b.contains_glyphosate, false) THEN 1 ELSE 0 END)
                    AS glyphosate_applications,
                ROUND(SUM(COALESCE(p.AllocatedArea, 0)), 1) AS total_treated_area_ha,
                COUNT(DISTINCT p.cvr_number) AS unique_companies
            FROM pesticides p
            JOIN companies c
              ON CAST(p.cvr_number AS VARCHAR) = c.cvr_number::VARCHAR
            LEFT JOIN bmd_products b
              ON CAST(p.PesticideRegistrationNumber AS VARCHAR) = CAST(b.registrerings_nr AS VARCHAR)
            WHERE COALESCE(NULLIF(TRIM(CAST(p.municipality AS VARCHAR)), ''), c.current_municipality_name)
                  IS NOT NULL
            GROUP BY 1
        """)
        return True

    def _prepare_antibiotic_summary(self) -> bool:
        if not (self._table_exists("antibiotic_usage") and self._table_exists("production_sites")):
            return False
        production_columns = self._table_columns("production_sites")
        municipality_expr = (
            "NULLIF(TRIM(CAST(ps.municipality AS VARCHAR)), '')"
            if "municipality" in production_columns
            else "NULL"
        )
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE municipality_antibiotic_summary AS
            SELECT
                COALESCE({municipality_expr}, c.current_municipality_name) AS municipality,
                SUM(COALESCE(au.animal_doses, 0)) AS total_antibiotic_ddd_usage,
                COUNT(DISTINCT ps.chr) AS total_production_sites,
                COUNT(DISTINCT CASE WHEN COALESCE(au.animal_doses, 0) > 0 THEN ps.chr END)
                    AS sites_with_antibiotics,
                COUNT(DISTINCT ps.company_id) AS unique_companies
            FROM antibiotic_usage au
            JOIN production_sites ps
              ON TRY_CAST(ps.chr AS BIGINT) = au.chr
            LEFT JOIN companies c
              ON ps.company_id = c.cvr_number::VARCHAR
            WHERE COALESCE({municipality_expr}, c.current_municipality_name) IS NOT NULL
            GROUP BY 1
        """)
        return True

    def _prepare_nitrogen_summary(self, year: int) -> bool:
        if not (self._table_exists("nitrogen") and self._table_exists("companies")):
            return False
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE municipality_environmental_summary AS
            SELECT
                c.current_municipality_name AS municipality,
                ROUND(
                    SUM(COALESCE(n.nitrogen_washout_kg_ha, 0) * COALESCE(n.area_ha, 0))
                    / NULLIF(SUM(COALESCE(n.area_ha, 0)), 0),
                    2
                ) AS avg_nitrogen_leaching_kg,
                ROUND(SUM(COALESCE(n.area_ha, 0)), 1) AS total_area_ha,
                COUNT(DISTINCT n.cvr_number) AS unique_companies
            FROM nitrogen n
            JOIN companies c
              ON CAST(n.cvr_number AS VARCHAR) = c.cvr_number::VARCHAR
            WHERE c.current_municipality_name IS NOT NULL
              AND n.year = {year}
            GROUP BY c.current_municipality_name
        """)
        return True

    def _prepare_worker_safety_summary(self) -> bool:
        if not (self._table_exists("worker_safety") and self._table_exists("companies")):
            return False
        self.conn.execute("""
            CREATE OR REPLACE TABLE municipality_worker_safety_summary AS
            SELECT
                c.current_municipality_name AS municipality,
                SUM(COALESCE(TRY_CAST(ws.injury_count AS INTEGER), 0)) AS total_workplace_incidents,
                COUNT(DISTINCT ws.cvr_number) AS unique_companies
            FROM worker_safety ws
            JOIN companies c
              ON CAST(ws.cvr_number AS VARCHAR) = c.cvr_number::VARCHAR
            WHERE c.current_municipality_name IS NOT NULL
            GROUP BY c.current_municipality_name
        """)
        return True

    def _prepare_incident_summary(self) -> bool:
        if not (self._table_exists("inspections") and self._table_exists("companies")):
            return False
        columns = self._table_columns("inspections")
        company_key_expr = (
            "COALESCE(NULLIF(CAST(i.cvr_number AS VARCHAR), ''), NULLIF(CAST(i.company_id AS VARCHAR), ''))"
            if "cvr_number" in columns and "company_id" in columns
            else "CAST(i.cvr_number AS VARCHAR)"
            if "cvr_number" in columns
            else "CAST(i.company_id AS VARCHAR)"
        )
        decision_expr = "CAST(i.decision AS VARCHAR)" if "decision" in columns else "NULL"
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE municipality_incident_summary AS
            SELECT
                c.current_municipality_name AS municipality,
                COUNT(*) AS total_incidents,
                COUNT(DISTINCT {company_key_expr}) AS companies_with_incidents,
                COUNT(DISTINCT {decision_expr}) AS incident_types_count
            FROM inspections i
            JOIN companies c
              ON {company_key_expr} = c.cvr_number::VARCHAR
            WHERE c.current_municipality_name IS NOT NULL
            GROUP BY c.current_municipality_name
        """)
        return True

    def _generate_details(self, year: int, rankings: dict[str, list[dict]]) -> int:
        files_written = 0
        ranking_lookup: dict[str, dict[str, int]] = {}

        for category, items in rankings.items():
            for item in items:
                ranking_lookup.setdefault(item["municipality"], {})[category] = item["rank"]

        detail_specs = [
            (
                "land_use",
                self._land_use_details,
                ["field_count", "organic_percentage"],
                "total_agricultural_area_ha",
            ),
            (
                "organic_farming",
                self._organic_farming_details,
                ["organic_area_ha", "organic_percentage"],
                "organic_farming_percentage",
            ),
            (
                "production",
                self._production_details,
                ["total_production_sites"],
                "total_animal_capacity",
            ),
            (
                "pesticide_burden",
                self._pesticide_burden_details,
                ["total_applications", "treated_area_ha"],
                "total_pesticide_burden",
            ),
            (
                "pesticide_pfas",
                self._pesticide_pfas_details,
                ["application_count", "treated_area_ha"],
                "pfas_pesticide_burden",
            ),
            (
                "pesticide_glyphosate",
                self._pesticide_glyphosate_details,
                ["application_count", "treated_area_ha"],
                "glyphosate_pesticide_burden",
            ),
            (
                "antibiotic_usage",
                self._antibiotic_usage_details,
                ["site_count"],
                "total_antibiotic_ddd_usage",
            ),
            (
                "environmental",
                self._environmental_details,
                ["total_area_ha", "total_n_leached_kg"],
                "avg_nitrogen_leaching_kg",
            ),
            ("worker_safety", self._worker_safety_details, [], "total_workplace_incidents"),
            ("incidents", self._incident_details, ["incident_types_count"], "total_incidents"),
        ]

        for category, query_fn, additional_keys, metric in detail_specs:
            if category not in rankings:
                continue
            try:
                rows = query_fn(year)
                files_written += self._write_details_files(
                    rows,
                    category,
                    year,
                    ranking_lookup,
                    additional_keys=additional_keys,
                    metric=metric,
                )
            except Exception:
                logger.exception(f"Failed to generate {category} details")

        logger.info(f"Generated {files_written} municipality detail files")
        return files_written

    def _land_use_details(self, year: int) -> list[dict]:
        return self.query_to_dicts(f"""
            WITH company_areas AS (
                SELECT
                    c.current_municipality_name AS municipality,
                    fp.cvr_number,
                    c.company_name,
                    ROUND(SUM(fp.area_ha), 1) AS value,
                    COUNT(*) AS field_count,
                    ROUND(
                        100.0 * SUM(CASE WHEN fp.organic_farming THEN fp.area_ha ELSE 0 END)
                        / NULLIF(SUM(fp.area_ha), 0),
                        1
                    ) AS organic_percentage
                FROM field_production fp
                JOIN companies c ON fp.cvr_number = c.cvr_number::VARCHAR
                WHERE fp.year = {year}
                  AND c.current_municipality_name IS NOT NULL
                GROUP BY 1, 2, 3
            ),
            municipality_totals AS (
                SELECT municipality, SUM(value) AS total_value
                FROM company_areas
                GROUP BY municipality
            )
            SELECT
                ca.*,
                mt.total_value,
                ROUND(100.0 * ca.value / NULLIF(mt.total_value, 0), 1) AS percentage_of_municipality,
                ROW_NUMBER() OVER (PARTITION BY ca.municipality ORDER BY ca.value DESC) AS rank_in_municipality
            FROM company_areas ca
            JOIN municipality_totals mt ON ca.municipality = mt.municipality
            QUALIFY rank_in_municipality <= 20
            ORDER BY municipality, rank_in_municipality
        """)

    def _organic_farming_details(self, year: int) -> list[dict]:
        return self.query_to_dicts(f"""
            WITH company_organic AS (
                SELECT
                    c.current_municipality_name AS municipality,
                    fp.cvr_number,
                    c.company_name,
                    SUM(CASE WHEN fp.organic_farming THEN fp.area_ha ELSE 0 END) AS organic_area_ha,
                    SUM(fp.area_ha) AS company_area_ha,
                    ROUND(
                        100.0 * SUM(CASE WHEN fp.organic_farming THEN fp.area_ha ELSE 0 END)
                        / NULLIF(SUM(fp.area_ha), 0),
                        1
                    ) AS organic_percentage
                FROM field_production fp
                JOIN companies c ON fp.cvr_number = c.cvr_number::VARCHAR
                WHERE fp.year = {year}
                  AND c.current_municipality_name IS NOT NULL
                GROUP BY 1, 2, 3
            ),
            municipality_totals AS (
                SELECT
                    municipality,
                    SUM(company_area_ha) AS municipality_area_ha,
                    ROUND(100.0 * SUM(organic_area_ha) / NULLIF(SUM(company_area_ha), 0), 1)
                        AS total_value
                FROM company_organic
                GROUP BY municipality
            )
            SELECT
                co.municipality,
                co.cvr_number,
                co.company_name,
                ROUND(100.0 * co.organic_area_ha / NULLIF(mt.municipality_area_ha, 0), 2) AS value,
                co.organic_area_ha,
                co.organic_percentage,
                mt.total_value,
                ROUND(
                    100.0 * (100.0 * co.organic_area_ha / NULLIF(mt.municipality_area_ha, 0))
                    / NULLIF(mt.total_value, 0),
                    1
                ) AS percentage_of_municipality,
                ROW_NUMBER() OVER (
                    PARTITION BY co.municipality
                    ORDER BY co.organic_area_ha DESC, co.company_name
                ) AS rank_in_municipality
            FROM company_organic co
            JOIN municipality_totals mt ON co.municipality = mt.municipality
            WHERE co.organic_area_ha > 0
            QUALIFY rank_in_municipality <= 20
            ORDER BY municipality, rank_in_municipality
        """)

    def _production_details(self, _year: int) -> list[dict]:
        return self.query_to_dicts("""
            WITH company_capacity AS (
                SELECT
                    COALESCE(NULLIF(TRIM(CAST(ps.municipality AS VARCHAR)), ''), c.current_municipality_name)
                        AS municipality,
                    ps.company_id AS cvr_number,
                    c.company_name,
                    SUM(COALESCE(ps.capacity, 0)) AS value,
                    COUNT(*) AS total_production_sites
                FROM production_sites ps
                LEFT JOIN companies c ON ps.company_id = c.cvr_number::VARCHAR
                WHERE COALESCE(NULLIF(TRIM(CAST(ps.municipality AS VARCHAR)), ''), c.current_municipality_name)
                      IS NOT NULL
                  AND COALESCE(ps.capacity, 0) > 0
                GROUP BY 1, 2, 3
            ),
            municipality_totals AS (
                SELECT municipality, SUM(value) AS total_value
                FROM company_capacity
                GROUP BY municipality
            )
            SELECT
                cc.*,
                mt.total_value,
                ROUND(100.0 * cc.value / NULLIF(mt.total_value, 0), 1) AS percentage_of_municipality,
                ROW_NUMBER() OVER (PARTITION BY cc.municipality ORDER BY cc.value DESC) AS rank_in_municipality
            FROM company_capacity cc
            JOIN municipality_totals mt ON cc.municipality = mt.municipality
            QUALIFY rank_in_municipality <= 20
            ORDER BY municipality, rank_in_municipality
        """)

    def _pesticide_burden_details(self, _year: int) -> list[dict]:
        return self._pesticide_details()

    def _pesticide_pfas_details(self, _year: int) -> list[dict]:
        return self._pesticide_details("COALESCE(b.contains_pfas, false)")

    def _pesticide_glyphosate_details(self, _year: int) -> list[dict]:
        return self._pesticide_details("COALESCE(b.contains_glyphosate, false)")

    def _pesticide_details(
        self,
        predicate: str | None = None,
    ) -> list[dict]:
        predicate_clause = f"AND {predicate}" if predicate else ""
        return self.query_to_dicts(f"""
            WITH company_pesticides AS (
                SELECT
                    COALESCE(NULLIF(TRIM(CAST(p.municipality AS VARCHAR)), ''), c.current_municipality_name)
                        AS municipality,
                    p.cvr_number,
                    c.company_name,
                    SUM(COALESCE(p.DosageQuantity, 0) * COALESCE(b.samlet_belastning, 0)) AS value,
                    COUNT(*) AS application_count,
                    ROUND(SUM(COALESCE(p.AllocatedArea, 0)), 1) AS treated_area_ha
                FROM pesticides p
                JOIN companies c ON CAST(p.cvr_number AS VARCHAR) = c.cvr_number::VARCHAR
                LEFT JOIN bmd_products b
                  ON CAST(p.PesticideRegistrationNumber AS VARCHAR) = CAST(b.registrerings_nr AS VARCHAR)
                WHERE COALESCE(NULLIF(TRIM(CAST(p.municipality AS VARCHAR)), ''), c.current_municipality_name)
                      IS NOT NULL
                  {predicate_clause}
                GROUP BY 1, 2, 3
            ),
            municipality_totals AS (
                SELECT municipality, SUM(value) AS total_value
                FROM company_pesticides
                GROUP BY municipality
            )
            SELECT
                cp.*,
                mt.total_value,
                ROUND(100.0 * cp.value / NULLIF(mt.total_value, 0), 1) AS percentage_of_municipality,
                ROW_NUMBER() OVER (PARTITION BY cp.municipality ORDER BY cp.value DESC) AS rank_in_municipality
            FROM company_pesticides cp
            JOIN municipality_totals mt ON cp.municipality = mt.municipality
            WHERE cp.value > 0
            QUALIFY rank_in_municipality <= 20
            ORDER BY municipality, rank_in_municipality
        """)

    def _antibiotic_usage_details(self, _year: int) -> list[dict]:
        return self.query_to_dicts("""
            WITH company_antibiotics AS (
                SELECT
                    COALESCE(NULLIF(TRIM(CAST(ps.municipality AS VARCHAR)), ''), c.current_municipality_name)
                        AS municipality,
                    ps.company_id AS cvr_number,
                    c.company_name,
                    SUM(COALESCE(au.animal_doses, 0)) AS value,
                    COUNT(DISTINCT ps.chr) AS site_count
                FROM antibiotic_usage au
                JOIN production_sites ps ON TRY_CAST(ps.chr AS BIGINT) = au.chr
                LEFT JOIN companies c ON ps.company_id = c.cvr_number::VARCHAR
                WHERE COALESCE(NULLIF(TRIM(CAST(ps.municipality AS VARCHAR)), ''), c.current_municipality_name)
                      IS NOT NULL
                GROUP BY 1, 2, 3
            ),
            municipality_totals AS (
                SELECT municipality, SUM(value) AS total_value
                FROM company_antibiotics
                GROUP BY municipality
            )
            SELECT
                ca.*,
                mt.total_value,
                ROUND(100.0 * ca.value / NULLIF(mt.total_value, 0), 1) AS percentage_of_municipality,
                ROW_NUMBER() OVER (PARTITION BY ca.municipality ORDER BY ca.value DESC) AS rank_in_municipality
            FROM company_antibiotics ca
            JOIN municipality_totals mt ON ca.municipality = mt.municipality
            WHERE ca.value > 0
            QUALIFY rank_in_municipality <= 20
            ORDER BY municipality, rank_in_municipality
        """)

    def _environmental_details(self, year: int) -> list[dict]:
        return self.query_to_dicts(f"""
            WITH company_nitrogen AS (
                SELECT
                    c.current_municipality_name AS municipality,
                    n.cvr_number,
                    c.company_name,
                    SUM(COALESCE(n.nitrogen_washout_kg_ha, 0) * COALESCE(n.area_ha, 0))
                        AS total_n_leached_kg,
                    SUM(COALESCE(n.area_ha, 0)) AS total_area_ha
                FROM nitrogen n
                JOIN companies c ON CAST(n.cvr_number AS VARCHAR) = c.cvr_number::VARCHAR
                WHERE c.current_municipality_name IS NOT NULL
                  AND n.year = {year}
                GROUP BY 1, 2, 3
            ),
            municipality_totals AS (
                SELECT municipality, SUM(total_area_ha) AS municipality_area_ha
                FROM company_nitrogen
                GROUP BY municipality
            ),
            municipality_values AS (
                SELECT
                    cn.municipality,
                    ROUND(
                        SUM(cn.total_n_leached_kg) / NULLIF(SUM(cn.total_area_ha), 0),
                        2
                    ) AS total_value
                FROM company_nitrogen cn
                GROUP BY cn.municipality
            )
            SELECT
                cn.municipality,
                cn.cvr_number,
                cn.company_name,
                ROUND(cn.total_n_leached_kg / NULLIF(mt.municipality_area_ha, 0), 3) AS value,
                ROUND(cn.total_area_ha, 1) AS total_area_ha,
                ROUND(cn.total_n_leached_kg, 1) AS total_n_leached_kg,
                mv.total_value,
                ROUND(
                    100.0 * (cn.total_n_leached_kg / NULLIF(mt.municipality_area_ha, 0))
                    / NULLIF(mv.total_value, 0),
                    1
                ) AS percentage_of_municipality,
                ROW_NUMBER() OVER (
                    PARTITION BY cn.municipality
                    ORDER BY cn.total_n_leached_kg DESC
                ) AS rank_in_municipality
            FROM company_nitrogen cn
            JOIN municipality_totals mt ON cn.municipality = mt.municipality
            JOIN municipality_values mv ON cn.municipality = mv.municipality
            WHERE cn.total_n_leached_kg > 0
            QUALIFY rank_in_municipality <= 20
            ORDER BY municipality, rank_in_municipality
        """)

    def _worker_safety_details(self, _year: int) -> list[dict]:
        return self.query_to_dicts("""
            WITH company_safety AS (
                SELECT
                    c.current_municipality_name AS municipality,
                    CAST(ws.cvr_number AS VARCHAR) AS cvr_number,
                    c.company_name,
                    SUM(COALESCE(TRY_CAST(ws.injury_count AS INTEGER), 0)) AS value
                FROM worker_safety ws
                JOIN companies c ON CAST(ws.cvr_number AS VARCHAR) = c.cvr_number::VARCHAR
                WHERE c.current_municipality_name IS NOT NULL
                GROUP BY 1, 2, 3
            ),
            municipality_totals AS (
                SELECT municipality, SUM(value) AS total_value
                FROM company_safety
                GROUP BY municipality
            )
            SELECT
                cs.*,
                mt.total_value,
                ROUND(100.0 * cs.value / NULLIF(mt.total_value, 0), 1) AS percentage_of_municipality,
                ROW_NUMBER() OVER (PARTITION BY cs.municipality ORDER BY cs.value DESC) AS rank_in_municipality
            FROM company_safety cs
            JOIN municipality_totals mt ON cs.municipality = mt.municipality
            WHERE cs.value > 0
            QUALIFY rank_in_municipality <= 20
            ORDER BY municipality, rank_in_municipality
        """)

    def _incident_details(self, _year: int) -> list[dict]:
        decision_expr = (
            "CAST(i.decision AS VARCHAR)"
            if "decision" in self._table_columns("inspections")
            else "'inspection'"
        )
        return self.query_to_dicts(
            """
            WITH company_incidents AS (
                SELECT
                    c.current_municipality_name AS municipality,
                    COALESCE(NULLIF(CAST(i.cvr_number AS VARCHAR), ''), NULLIF(CAST(i.company_id AS VARCHAR), ''))
                        AS cvr_number,
                    c.company_name,
                    COUNT(*) AS value,
                    COUNT(DISTINCT __DECISION_EXPR__) AS incident_types_count
                FROM inspections i
                JOIN companies c
                  ON COALESCE(NULLIF(CAST(i.cvr_number AS VARCHAR), ''), NULLIF(CAST(i.company_id AS VARCHAR), ''))
                     = c.cvr_number::VARCHAR
                WHERE c.current_municipality_name IS NOT NULL
                GROUP BY 1, 2, 3
            ),
            municipality_totals AS (
                SELECT municipality, SUM(value) AS total_value
                FROM company_incidents
                GROUP BY municipality
            )
            SELECT
                ci.*,
                mt.total_value,
                ROUND(100.0 * ci.value / NULLIF(mt.total_value, 0), 1) AS percentage_of_municipality,
                ROW_NUMBER() OVER (PARTITION BY ci.municipality ORDER BY ci.value DESC) AS rank_in_municipality
            FROM company_incidents ci
            JOIN municipality_totals mt ON ci.municipality = mt.municipality
            WHERE ci.value > 0
            QUALIFY rank_in_municipality <= 20
            ORDER BY municipality, rank_in_municipality
        """.replace("__DECISION_EXPR__", decision_expr)
        )

    def _write_details_files(
        self,
        rows: list[dict],
        category: str,
        year: int,
        ranking_lookup: dict[str, dict[str, int]],
        additional_keys: list[str],
        metric: str,
    ) -> int:
        files_written = 0
        municipalities: dict[str, list[dict]] = {}
        totals: dict[str, float] = {}

        for row in rows:
            muni = row["municipality"]
            totals[muni] = row["total_value"]
            municipalities.setdefault(muni, []).append(
                {
                    "company_id": str(row["cvr_number"]),
                    "company_name": row["company_name"] or f"CVR {row['cvr_number']}",
                    "cvr_number": str(row["cvr_number"]),
                    "value": row["value"],
                    "percentage_of_municipality": row["percentage_of_municipality"],
                    "rank_in_municipality": row["rank_in_municipality"],
                    "additional_data": {
                        key: row.get(key) for key in additional_keys if row.get(key) is not None
                    },
                }
            )

        for muni, companies in municipalities.items():
            detail = {
                "municipality": muni,
                "category": category,
                "metric": metric,
                "year": year,
                "total_municipality_value": totals.get(muni, 0),
                "municipality_rank": ranking_lookup.get(muni, {}).get(category),
                "companies": companies,
                "metadata": {
                    "total_companies": len(companies),
                    "generated_at": datetime.now(UTC).isoformat(),
                },
            }

            safe_name = quote(muni, safe="")
            self.write_json(detail, f"municipalities/details/{safe_name}_{category}.json")
            files_written += 1

        return files_written
