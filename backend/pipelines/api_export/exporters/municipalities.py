"""Municipality rankings and details exporter.

Replaces: Supabase edge function municipality-rankings (254 lines)
         + municipality-details endpoint

Output:
- municipalities/rankings/all.json
- municipalities/rankings/land_use.json
- municipalities/rankings/production.json
- municipalities/details/{municipality}_{category}.json (per-municipality company breakdowns)

Data sources (R2):
- gold/field_production/latest/data.parquet (columns: cvr_number, year, area_ha, crop_type, organic_farming, kommune_name)
- gold/pesticide_disaggregation_2023_2024/*/pesticide_disaggregation_2023_2024.parquet (columns: cvr_number, municipality, AllocatedArea)
"""

import logging
import os
from datetime import UTC, datetime
from urllib.parse import quote

from exporters.base import BaseExporter

logger = logging.getLogger("api_export.municipalities")

BUCKET = os.getenv("R2_BUCKET") or os.getenv("GCS_BUCKET") or "landbruget-data"


class MunicipalitiesExporter(BaseExporter):
    """Generate municipality-level rankings."""

    def export(self) -> dict:
        stats = {"files_written": 0}

        # Load tables using actual R2 paths and column names
        tables = {
            "field_production": f"r2://{BUCKET}/gold/field_production/latest/data.parquet",
            "companies": f"r2://{BUCKET}/gold/cvr_enrichment_companies/data.parquet",
        }
        for name, path in tables.items():
            try:
                self.load_parquet_table(path, name)
            except Exception:
                logger.warning(f"Could not load {name} from {path}")

        response = {
            "rankings": {},
            "metadata": {
                "year": 2024,
                "total_municipalities": 0,
                "generated_at": datetime.now(UTC).isoformat(),
                "categories_included": [],
            },
        }

        # Land Use Rankings (from field_production)
        land_use = self._land_use_rankings(2024)
        if land_use:
            response["rankings"]["land_use"] = land_use
            response["metadata"]["categories_included"].append("land_use")
            response["metadata"]["total_municipalities"] = len(land_use)
            self.write_json(
                {**response, "rankings": {"land_use": land_use}},
                "municipalities/rankings/land_use.json",
            )
            stats["files_written"] += 1

        # Production Rankings (from field_production aggregate)
        production = self._production_rankings(2024)
        if production:
            response["rankings"]["production"] = production
            response["metadata"]["categories_included"].append("production")
            self.write_json(
                {**response, "rankings": {"production": production}},
                "municipalities/rankings/production.json",
            )
            stats["files_written"] += 1

        # Write combined
        self.write_json(response, "municipalities/rankings/all.json")
        stats["files_written"] += 1

        # Generate per-municipality company details
        details_count = self._generate_details(2024, response.get("rankings", {}))
        stats["files_written"] += details_count
        stats["details_files"] = details_count

        return stats

    def _land_use_rankings(self, year: int) -> list[dict] | None:
        """Rank municipalities by agricultural land use."""
        try:
            # kommune_name in field_production is all 'Unknown', so join with companies for municipality
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE municipality_land_use_summary AS
                SELECT
                    c.current_municipality_name AS municipality,
                    {year} AS year,
                    ROUND(SUM(fp.area_ha), 1) AS total_area_ha,
                    COUNT(*) AS total_fields,
                    ROUND(AVG(fp.area_ha), 2) AS avg_field_size,
                    ROUND(100.0 * SUM(CASE WHEN fp.organic_farming THEN fp.area_ha ELSE 0 END)
                        / NULLIF(SUM(fp.area_ha), 0), 1) AS organic_percentage,
                    COUNT(DISTINCT fp.cvr_number) AS unique_companies,
                    COUNT(DISTINCT fp.crop_type) AS unique_crops
                FROM field_production fp
                JOIN companies c ON fp.cvr_number = c.cvr_number::VARCHAR
                WHERE fp.year = {year} AND c.current_municipality_name IS NOT NULL
                GROUP BY c.current_municipality_name
                ORDER BY total_area_ha DESC
            """)

            rows = self.query_to_dicts("SELECT * FROM municipality_land_use_summary")
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
        except Exception:
            logger.exception("Failed to generate land use rankings")
            return None

    def _production_rankings(self, year: int) -> list[dict] | None:
        """Rank municipalities by total agricultural production."""
        try:
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE municipality_production_summary AS
                SELECT
                    c.current_municipality_name AS municipality,
                    ROUND(SUM(fp.production_estimate_hkg), 0) AS total_production_hkg,
                    ROUND(SUM(fp.area_ha), 1) AS total_area_ha,
                    COUNT(DISTINCT fp.cvr_number) AS unique_companies,
                    COUNT(*) AS total_fields,
                    ROUND(SUM(fp.production_estimate_hkg) / NULLIF(SUM(fp.area_ha), 0), 1) AS yield_per_ha
                FROM field_production fp
                JOIN companies c ON fp.cvr_number = c.cvr_number::VARCHAR
                WHERE fp.year = {year}
                    AND c.current_municipality_name IS NOT NULL
                    AND fp.production_estimate_hkg IS NOT NULL
                GROUP BY c.current_municipality_name
                ORDER BY total_production_hkg DESC
            """)

            rows = self.query_to_dicts("SELECT * FROM municipality_production_summary")
            return [
                {
                    "municipality": row["municipality"],
                    "rank": idx + 1,
                    "value": row["total_production_hkg"],
                    "metric": "total_production_hkg",
                    "additional_data": {
                        "total_area_ha": row["total_area_ha"],
                        "unique_companies": row["unique_companies"],
                        "total_fields": row["total_fields"],
                        "yield_per_ha": row["yield_per_ha"] or 0,
                    },
                }
                for idx, row in enumerate(rows)
            ]
        except Exception:
            logger.exception("Failed to generate production rankings")
            return None

    def _generate_details(self, year: int, rankings: dict) -> int:
        """Generate per-municipality company breakdown files.

        For each municipality in each ranking category, produces a JSON file
        listing the top companies contributing to that municipality's value.

        Returns:
            Number of files written.
        """
        files_written = 0

        # Build ranking lookup: municipality → rank per category
        ranking_lookup: dict[str, dict[str, int]] = {}
        for category, items in rankings.items():
            for item in items:
                muni = item["municipality"]
                if muni not in ranking_lookup:
                    ranking_lookup[muni] = {}
                ranking_lookup[muni][category] = item["rank"]

        # Land use details: top companies by area per municipality
        try:
            land_use_details = self.query_to_dicts(f"""
                WITH company_areas AS (
                    SELECT
                        c.current_municipality_name AS municipality,
                        fp.cvr_number AS cvr_number,
                        c.company_name,
                        ROUND(SUM(fp.area_ha), 1) AS value,
                        COUNT(*) AS field_count,
                        ROUND(100.0 * SUM(CASE WHEN fp.organic_farming THEN fp.area_ha ELSE 0 END)
                            / NULLIF(SUM(fp.area_ha), 0), 1) AS organic_percentage
                    FROM field_production fp
                    JOIN companies c ON fp.cvr_number = c.cvr_number::VARCHAR
                    WHERE fp.year = {year} AND c.current_municipality_name IS NOT NULL
                    GROUP BY c.current_municipality_name, fp.cvr_number, c.company_name
                ),
                municipality_totals AS (
                    SELECT municipality, SUM(value) AS total_value
                    FROM company_areas
                    GROUP BY municipality
                ),
                ranked AS (
                    SELECT
                        ca.*,
                        mt.total_value,
                        ROUND(100.0 * ca.value / NULLIF(mt.total_value, 0), 1) AS percentage_of_municipality,
                        ROW_NUMBER() OVER (PARTITION BY ca.municipality ORDER BY ca.value DESC) AS rank_in_municipality
                    FROM company_areas ca
                    JOIN municipality_totals mt ON ca.municipality = mt.municipality
                )
                SELECT * FROM ranked
                WHERE rank_in_municipality <= 20
                ORDER BY municipality, rank_in_municipality
            """)

            files_written += self._write_details_files(
                land_use_details,
                "land_use",
                year,
                ranking_lookup,
                additional_keys=["field_count", "organic_percentage"],
            )
        except Exception:
            logger.exception("Failed to generate land_use details")

        # Production details: top companies by production per municipality
        try:
            production_details = self.query_to_dicts(f"""
                WITH company_production AS (
                    SELECT
                        c.current_municipality_name AS municipality,
                        fp.cvr_number AS cvr_number,
                        c.company_name,
                        ROUND(SUM(fp.production_estimate_hkg), 0) AS value,
                        ROUND(SUM(fp.area_ha), 1) AS total_area_ha,
                        COUNT(*) AS field_count
                    FROM field_production fp
                    JOIN companies c ON fp.cvr_number = c.cvr_number::VARCHAR
                    WHERE fp.year = {year}
                        AND c.current_municipality_name IS NOT NULL
                        AND fp.production_estimate_hkg IS NOT NULL
                    GROUP BY c.current_municipality_name, fp.cvr_number, c.company_name
                ),
                municipality_totals AS (
                    SELECT municipality, SUM(value) AS total_value
                    FROM company_production
                    GROUP BY municipality
                ),
                ranked AS (
                    SELECT
                        cp.*,
                        mt.total_value,
                        ROUND(100.0 * cp.value / NULLIF(mt.total_value, 0), 1) AS percentage_of_municipality,
                        ROW_NUMBER() OVER (PARTITION BY cp.municipality ORDER BY cp.value DESC) AS rank_in_municipality
                    FROM company_production cp
                    JOIN municipality_totals mt ON cp.municipality = mt.municipality
                )
                SELECT * FROM ranked
                WHERE rank_in_municipality <= 20
                ORDER BY municipality, rank_in_municipality
            """)

            files_written += self._write_details_files(
                production_details,
                "production",
                year,
                ranking_lookup,
                additional_keys=["total_area_ha", "field_count"],
            )
        except Exception:
            logger.exception("Failed to generate production details")

        logger.info(f"Generated {files_written} municipality detail files")
        return files_written

    def _write_details_files(
        self,
        rows: list[dict],
        category: str,
        year: int,
        ranking_lookup: dict[str, dict[str, int]],
        additional_keys: list[str] | None = None,
    ) -> int:
        """Group rows by municipality and write one JSON file per municipality."""
        files_written = 0
        municipalities: dict[str, list[dict]] = {}

        for row in rows:
            muni = row["municipality"]
            if muni not in municipalities:
                municipalities[muni] = []
            company = {
                "company_id": str(row["cvr_number"]),
                "company_name": row["company_name"] or f"CVR {row['cvr_number']}",
                "cvr_number": str(row["cvr_number"]),
                "value": row["value"],
                "percentage_of_municipality": row["percentage_of_municipality"],
                "rank_in_municipality": row["rank_in_municipality"],
            }
            if additional_keys:
                company["additional_data"] = {
                    k: row.get(k) for k in additional_keys if row.get(k) is not None
                }
            municipalities[muni] = [*municipalities[muni], company]

        for muni, companies in municipalities.items():
            total_value = rows[0]["total_value"] if rows else 0
            for row in rows:
                if row["municipality"] == muni:
                    total_value = row["total_value"]
                    break

            detail = {
                "municipality": muni,
                "category": category,
                "year": year,
                "total_municipality_value": total_value,
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
