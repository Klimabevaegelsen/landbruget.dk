"""Pesticide analysis exporter.

Replaces: Supabase edge functions pesticide-analysis + pesticide-company-details

Output:
- pesticides/analysis/index.json (national aggregate)
- pesticides/analysis/{municipality}.json (per-municipality, 98 files)
- pesticides/companies/{cvr}.json (per-company pesticide details)

Data source: gold/pesticide_disaggregation_2023_2024/*/pesticide_disaggregation_2023_2024.parquet
Columns: cvr_number, PesticideName, DosageQuantity, DosageUnit, AllocatedArea, municipality
"""

import logging
import os
from datetime import UTC, datetime

from exporters.base import BaseExporter

logger = logging.getLogger("api_export.pesticides")

BUCKET = os.getenv("R2_BUCKET") or os.getenv("GCS_BUCKET") or "landbruget-data"


class PesticidesExporter(BaseExporter):
    """Generate pesticide analysis JSON files."""

    def export(self) -> dict:
        stats = {"files_written": 0}

        # Load pesticide data
        pesticide_path = f"r2://{BUCKET}/gold/pesticide_disaggregation_2023_2024/20260317_074432/pesticide_disaggregation_2023_2024.parquet"
        companies_path = f"r2://{BUCKET}/gold/cvr_enrichment_companies/data.parquet"

        try:
            self.load_parquet_table(pesticide_path, "pesticides")
            self.load_parquet_table(companies_path, "companies")
        except Exception:
            logger.exception("Failed to load pesticide data")
            return stats

        # National aggregate
        national = self._national_aggregate()
        if national:
            self.write_json(national, "pesticides/analysis/index.json")
            stats["files_written"] += 1

        # Per-municipality
        municipalities = self._per_municipality()
        for muni_name, muni_data in municipalities.items():
            safe_name = muni_name.replace("/", "_")
            self.write_json(muni_data, f"pesticides/analysis/{safe_name}.json")
            stats["files_written"] += 1

        # Per-company
        company_count = self._per_company()
        stats["files_written"] += company_count
        stats["company_files"] = company_count

        # Burden histogram (replaces Supabase RPC get_burden_histogram)
        bmd_path = f"r2://{BUCKET}/silver/bmd/20260301_042330/pesticide_products.parquet"
        try:
            self.load_parquet_table(bmd_path, "bmd_products")
            histogram_count = self._burden_histogram()
            stats["files_written"] += histogram_count
        except Exception:
            logger.warning("Could not load BMD data for burden histogram")

        return stats

    def _national_aggregate(self) -> dict | None:
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
                    "period": "2023-2024",
                },
            }
        except Exception:
            logger.exception("Failed to generate national aggregate")
            return None

    def _per_municipality(self) -> dict:
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
                        "period": "2023-2024",
                    },
                }

            logger.info(f"Generated {len(results)} municipality pesticide files")
        except Exception:
            logger.exception("Failed to generate per-municipality data")

        return results

    def _per_company(self) -> int:
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
                            "period": "2023-2024",
                        },
                    },
                    f"pesticides/companies/{cvr}.json",
                )
                count += 1

            logger.info(f"Generated {count} company pesticide files")
        except Exception:
            logger.exception("Failed to generate per-company pesticide data")

        return count

    def _burden_histogram(self) -> int:
        """Generate burden histogram JSON files.

        Replaces Supabase RPC function get_burden_histogram.
        Joins pesticide applications with BMD burden scores (samlet_belastning)
        and bins fields into 0.5 B/ha increments.

        Returns:
            Number of files written.
        """
        count = 0
        try:
            rows = self.query_to_dicts("""
                WITH field_burdens AS (
                    SELECT
                        p.field_uuid,
                        CASE
                            WHEN MAX(p.AllocatedArea) > 0
                            THEN SUM(COALESCE(p.DosageQuantity * b.samlet_belastning, 0))
                                 / MAX(p.AllocatedArea)
                            ELSE 0
                        END AS burden_per_ha
                    FROM pesticides p
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

            # Write for year 2024 (the disaggregation covers 2023-2024 season)
            self.write_json(histogram, "pesticides/burden-histogram-2024.json")
            count += 1
            logger.info(
                f"Generated burden histogram: {len(histogram)} bins, "
                f"{sum(r['field_count'] for r in histogram):,} fields"
            )

        except Exception:
            logger.exception("Failed to generate burden histogram")

        return count
