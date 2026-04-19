"""Pesticide analysis exporter.

Replaces: Supabase edge functions pesticide-analysis + pesticide-company-details

Output:
- pesticides/analysis/index.json (national aggregate)
- pesticides/analysis/{municipality}.json (per-municipality, 98 files)
- pesticides/companies/{cvr}.json (per-company pesticide details)
- pesticides/burden-histogram-{year}.json (per-year burden distribution)

Data source: gold/pesticide_disaggregation_{year}_{year+1}/*/pesticide_disaggregation_{year}_{year+1}.parquet
Columns: cvr_number, PesticideName, DosageQuantity, DosageUnit, AllocatedArea, municipality
"""

import os
import re
from datetime import UTC, datetime

from common.logging_utils import get_pipeline_logger

from exporters.base import BaseExporter

logger = get_pipeline_logger("api_export.pesticides")

BUCKET = os.getenv("R2_BUCKET") or os.getenv("GCS_BUCKET") or "landbruget-data"


class PesticidesExporter(BaseExporter):
    """Generate pesticide analysis JSON files."""

    def export(self) -> dict:
        stats = {"files_written": 0}

        # Discover all available disaggregation years
        year_paths = self._discover_disaggregation_years()
        if not year_paths:
            logger.warning("No pesticide disaggregation data found on R2")
            return stats

        latest_year = max(year_paths)
        logger.info(f"Found disaggregation data for years: {sorted(year_paths.keys())}")

        # Load latest year for national/municipality/company exports
        # Discover latest timestamped CVR enrichment file
        cvr_pattern = f"{self._r2_bucket}/gold/cvr_enrichment_companies/*/data.parquet"
        try:
            cvr_files = sorted(self.r2_fs.glob(cvr_pattern))
            companies_path = f"r2://{cvr_files[-1]}" if cvr_files else None
        except Exception:
            logger.warning("Could not discover CVR enrichment files")
            companies_path = None

        try:
            self.load_parquet_table(year_paths[latest_year], "pesticides")
            if companies_path:
                self.load_parquet_table(companies_path, "companies")
            else:
                logger.warning("No CVR enrichment data found, proceeding without company names")
                self.conn.execute(
                    "CREATE TABLE companies (cvr_number VARCHAR, company_name VARCHAR, "
                    "current_municipality_name VARCHAR)"
                )
        except Exception:
            logger.exception("Failed to load pesticide data")
            return stats

        period = f"{latest_year}-{latest_year + 1}"

        # National aggregate
        national = self._national_aggregate(period)
        if national:
            self.write_json(national, "pesticides/analysis/index.json")
            stats["files_written"] += 1

        # Per-municipality
        municipalities = self._per_municipality(period)
        for muni_name, muni_data in municipalities.items():
            safe_name = muni_name.replace("/", "_")
            self.write_json(muni_data, f"pesticides/analysis/{safe_name}.json")
            stats["files_written"] += 1

        # Per-company
        company_count = self._per_company(period)
        stats["files_written"] += company_count
        stats["company_files"] = company_count

        # Burden histograms for all available years
        bmd_path = f"r2://{BUCKET}/silver/bmd/20260301_042330/pesticide_products.parquet"
        try:
            self.load_parquet_table(bmd_path, "bmd_products")
            histogram_count = self._burden_histograms(year_paths)
            stats["files_written"] += histogram_count
            stats["histogram_years"] = sorted(year_paths.keys())
        except Exception:
            logger.warning("Could not load BMD data for burden histogram")

        return stats

    def _discover_disaggregation_years(self) -> dict[int, str]:
        """Find all available pesticide disaggregation parquets on R2.

        Returns:
            Dict mapping year (start of season) to the latest parquet path.
        """
        pattern = f"{self._r2_bucket}/gold/pesticide_disaggregation_*/*/*.parquet"
        try:
            files = self.r2_fs.glob(pattern)
        except Exception:
            logger.exception("Failed to list disaggregation files on R2")
            return {}

        year_paths: dict[int, str] = {}
        for f in files:
            match = re.search(r"pesticide_disaggregation_(\d{4})_(\d{4})", f)
            if match:
                year = int(match.group(1))
                # Keep latest timestamp per year (lexicographic sort works for timestamps)
                if year not in year_paths or f > year_paths[year]:
                    year_paths[year] = f"r2://{f}"

        return year_paths

    def _burden_histograms(self, year_paths: dict[int, str]) -> int:
        """Generate burden histogram JSON for each available year.

        Loads each year's disaggregation data into a temp table, computes the
        histogram, and writes pesticides/burden-histogram-{year}.json.
        """
        count = 0
        for year in sorted(year_paths):
            try:
                table_name = f"pesticides_{year}"
                self.load_parquet_table(year_paths[year], table_name)

                rows = self.query_to_dicts(f"""
                    WITH field_burdens AS (
                        SELECT
                            p.field_uuid,
                            CASE
                                WHEN MAX(p.AllocatedArea) > 0
                                THEN SUM(COALESCE(p.DosageQuantity * b.samlet_belastning, 0))
                                     / MAX(p.AllocatedArea)
                                ELSE 0
                            END AS burden_per_ha
                        FROM {table_name} p
                        LEFT JOIN bmd_products b
                            ON p.PesticideRegistrationNumber = b.registrerings_nr
                        GROUP BY p.field_uuid
                    )
                    SELECT
                        LEAST(FLOOR(burden_per_ha / 0.5) * 0.5, 12.0) AS bin_start,
                        COUNT(*)::BIGINT AS field_count
                    FROM field_burdens
                    WHERE burden_per_ha >= 0
                    GROUP BY LEAST(FLOOR(burden_per_ha / 0.5) * 0.5, 12.0)
                    ORDER BY bin_start
                """)

                histogram = [
                    {"bin_start": float(r["bin_start"]), "field_count": int(r["field_count"])}
                    for r in rows
                ]

                self.write_json(histogram, f"pesticides/burden-histogram-{year}.json")
                count += 1
                logger.info(
                    f"Generated burden histogram for {year}: {len(histogram)} bins, "
                    f"{sum(r['field_count'] for r in histogram):,} fields"
                )

                # Clean up temp table
                self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")

            except Exception:
                logger.exception(f"Failed to generate burden histogram for year {year}")

        return count

    def _national_aggregate(self, period: str) -> dict | None:
        try:
            summary = self.query_to_dicts("""
                SELECT
                    COUNT(*) AS total_applications,
                    COUNT(DISTINCT cvr_number) AS total_companies,
                    COUNT(DISTINCT PesticideName) AS unique_pesticides,
                    COUNT(DISTINCT municipality) AS total_municipalities,
                    ROUND(SUM(AllocatedArea), 1) AS total_treated_area_ha,
                    ROUND(SUM(DosageQuantity), 2) AS total_dosage
                FROM pesticides
            """)
            if not summary:
                return None

            top_pesticides = self.query_to_dicts("""
                SELECT
                    PesticideName AS name,
                    COUNT(*) AS application_count,
                    ROUND(SUM(DosageQuantity), 2) AS total_dosage,
                    ROUND(SUM(AllocatedArea), 1) AS total_area_ha
                FROM pesticides
                WHERE PesticideName IS NOT NULL
                GROUP BY PesticideName
                ORDER BY application_count DESC
                LIMIT 20
            """)

            return {
                "summary": summary[0],
                "top_pesticides": top_pesticides,
                "metadata": {
                    "generated_at": datetime.now(UTC).isoformat(),
                    "period": period,
                },
            }
        except Exception:
            logger.exception("Failed to generate national aggregate")
            return None

    def _per_municipality(self, period: str) -> dict:
        results = {}
        try:
            municipalities = self.query_to_dicts("""
                SELECT DISTINCT municipality
                FROM pesticides
                WHERE municipality IS NOT NULL
                ORDER BY municipality
            """)

            for row in municipalities:
                muni = row["municipality"]
                summary = self.query_to_dicts(f"""
                    SELECT
                        COUNT(*) AS total_applications,
                        COUNT(DISTINCT cvr_number) AS total_companies,
                        COUNT(DISTINCT PesticideName) AS unique_pesticides,
                        ROUND(SUM(AllocatedArea), 1) AS total_treated_area_ha,
                        ROUND(SUM(DosageQuantity), 2) AS total_dosage
                    FROM pesticides
                    WHERE municipality = '{muni.replace("'", "''")}'
                """)

                top = self.query_to_dicts(f"""
                    SELECT
                        PesticideName AS name,
                        COUNT(*) AS application_count,
                        ROUND(SUM(DosageQuantity), 2) AS total_dosage
                    FROM pesticides
                    WHERE municipality = '{muni.replace("'", "''")}'
                        AND PesticideName IS NOT NULL
                    GROUP BY PesticideName
                    ORDER BY application_count DESC
                    LIMIT 10
                """)

                results[muni] = {
                    "municipality": muni,
                    "summary": summary[0] if summary else {},
                    "top_pesticides": top,
                    "metadata": {
                        "generated_at": datetime.now(UTC).isoformat(),
                        "period": period,
                    },
                }

            logger.info(f"Generated {len(results)} municipality pesticide files")
        except Exception:
            logger.exception("Failed to generate per-municipality data")

        return results

    def _per_company(self, period: str) -> int:
        """Generate per-company pesticide detail files. Returns count of files written."""
        count = 0
        try:
            # Get all companies with pesticide data
            companies = self.query_to_dicts("""
                SELECT DISTINCT p.cvr_number, c.company_name, c.current_municipality_name AS municipality
                FROM pesticides p
                LEFT JOIN companies c ON p.cvr_number = c.cvr_number::VARCHAR
                ORDER BY p.cvr_number
            """)

            for comp in companies:
                cvr = comp["cvr_number"]
                details = self.query_to_dicts(f"""
                    SELECT
                        PesticideName AS pesticide_name,
                        DosageQuantity AS dosage_quantity,
                        DosageUnit AS dosage_unit,
                        AllocatedArea AS allocated_area_ha,
                        AllocationMethod AS allocation_method,
                        municipality
                    FROM pesticides
                    WHERE cvr_number = '{cvr}'
                    ORDER BY PesticideName
                """)

                summary = self.query_to_dicts(f"""
                    SELECT
                        COUNT(*) AS total_applications,
                        COUNT(DISTINCT PesticideName) AS unique_pesticides,
                        ROUND(SUM(AllocatedArea), 1) AS total_treated_area_ha,
                        ROUND(SUM(DosageQuantity), 2) AS total_dosage
                    FROM pesticides
                    WHERE cvr_number = '{cvr}'
                """)

                self.write_json(
                    {
                        "cvr_number": cvr,
                        "company_name": comp.get("company_name"),
                        "municipality": comp.get("municipality"),
                        "summary": summary[0] if summary else {},
                        "applications": details,
                        "metadata": {
                            "generated_at": datetime.now(UTC).isoformat(),
                            "period": period,
                        },
                    },
                    f"pesticides/companies/{cvr}.json",
                )
                count += 1

            logger.info(f"Generated {count} company pesticide files")
        except Exception:
            logger.exception("Failed to generate per-company pesticide data")

        return count
